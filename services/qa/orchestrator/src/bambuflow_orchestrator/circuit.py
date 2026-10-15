"""LLM 调用的熔断器（orchestrator 详设 8.5，总体设计 11.6）。

按模型统计：1 分钟窗口内错误率超过 20% 打开，30 秒后半开，半开期间连续 10 次成功才关闭，
其间任何一次失败重新打开。打开期间不向网关发请求，调用方按各自的降级规则处理
（生成返回 evidence_only，规划退回原问题）。

设计没有给出打开前的最少请求数；这里取 10：窗口里只有一两次调用时，一次失败就是 50% 以上，
不能据此判断网关不可用。半开时最多同时放行 10 个探测（正好是关闭所需的成功次数），
其余调用仍按打开处理，避免网关刚恢复就被积压的请求压垮。
"""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Callable
from enum import Enum

from bambuflow_orchestrator import metrics as bf_metrics

_log = logging.getLogger(__name__)

WINDOW_SECONDS = 60.0
ERROR_RATE = 0.2
MIN_CALLS = 10
OPEN_SECONDS = 30.0
CLOSE_AFTER = 10  # 半开时连续成功这么多次后关闭；也是半开时同时放行的探测数


class State(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class Breaker:
    """一个模型的熔断器。asyncio 单线程使用，不加锁。

    调用方先 allow()，拿到许可后必须以 record(ok) 或 release() 结束这次调用：
    record 计入统计；release 用于既不算成功也不算失败的结束（如客户端断开），只归还半开的探测名额。
    """

    def __init__(self, model: str, clock: Callable[[], float] = time.monotonic) -> None:
        self.model = model
        self._clock = clock
        self.state = State.CLOSED
        self._calls: deque[tuple[float, bool]] = deque()  # (时间, 是否失败)，只在关闭时统计
        self._errors = 0
        self._opened_at = 0.0
        self._probes = 0  # 半开时在途的探测数
        self._successes = 0  # 半开时连续成功的次数

    def allow(self) -> bool:
        if self.state is State.OPEN:
            if self._clock() - self._opened_at < OPEN_SECONDS:
                return False
            self._transition(State.HALF_OPEN)
            self._probes = self._successes = 0
        if self.state is State.HALF_OPEN:
            if self._probes >= CLOSE_AFTER:
                return False
            self._probes += 1
        return True

    def record(self, ok: bool) -> None:
        if self.state is State.HALF_OPEN:
            self._probes = max(0, self._probes - 1)
            if not ok:
                self._open()
                return
            self._successes += 1
            if self._successes >= CLOSE_AFTER:
                self._calls.clear()
                self._errors = 0
                self._transition(State.CLOSED)
            return
        if self.state is State.OPEN:
            # 打开之前已经放行的调用陆续结束，不再计入
            return
        now = self._clock()
        self._calls.append((now, not ok))
        self._errors += 0 if ok else 1
        self._expire(now)
        total = len(self._calls)
        if total >= MIN_CALLS and self._errors / total > ERROR_RATE:
            self._open()

    def release(self) -> None:
        if self.state is State.HALF_OPEN:
            self._probes = max(0, self._probes - 1)

    def _expire(self, now: float) -> None:
        while self._calls and now - self._calls[0][0] > WINDOW_SECONDS:
            _, failed = self._calls.popleft()
            self._errors -= 1 if failed else 0

    def _open(self) -> None:
        self._opened_at = self._clock()
        self._calls.clear()
        self._errors = 0
        self._transition(State.OPEN)

    def _transition(self, state: State) -> None:
        if state is self.state:
            return
        # 熔断打开要通知 AI Infra（详设 13.4），日志里的这一行是告警的来源
        log = _log.warning if state is State.OPEN else _log.info
        if state is State.OPEN:
            bf_metrics.circuit_open.add(1, {"model": self.model})
        elif self.state is State.OPEN:
            bf_metrics.circuit_open.add(-1, {"model": self.model})
        log(
            "LLM 熔断状态变化",
            extra={"model": self.model, "from": self.state.value, "to": state.value},
        )
        self.state = state
