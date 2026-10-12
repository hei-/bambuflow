"""一次请求的时间预算（orchestrator 详设第 4 节、11.1）。

gateway 调用前不知道问题走哪条路径，统一按多跳的 10 s 设 deadline；意图路由之后，
单跳把截止时间收紧到 4 s，agent 路径用满。各环节的超时都从这里倒推，不超过请求本身的 deadline。
"""

from __future__ import annotations

import time
from collections.abc import Callable

SINGLE_HOP_SECONDS = 4.0
# 书面同意放宽 SLO（全文 ≤ 5 秒，技术设计文档 6.7）、开启单跳 LLM 重排的租户的单跳预算
RELAXED_SINGLE_HOP_SECONDS = 5.0
AGENT_SECONDS = 10.0
# 生成与校验的预留（详设 11.1，评估报告 K10）：剩余时间不超过它就不再开新一轮检索或做 LLM 重排
GENERATION_RESERVE = 3.0


class Deadline:
    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._end = clock() + seconds

    def remaining(self) -> float:
        return max(0.0, self._end - self._clock())

    def cap(self, budget: float, reserve: float = 0.0) -> float:
        """某个环节的超时：不超过 budget，且给后面的环节留出 reserve 秒。可能为 0。"""
        return max(0.0, min(budget, self.remaining() - reserve))
