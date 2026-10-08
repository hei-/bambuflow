"""过载保护（orchestrator 详设 11.4）：每副本并发上限，超过后按调用目的的优先级排队。

优先级 answer > mcp > batch；空出的名额总是先给优先级最高、排队最早的请求。
排队超过上限的请求以 Overloaded 结束，服务入口返回 RESOURCE_EXHAUSTED：
batch 排队 500 ms（由 qa/batch 稍后重试），answer 排队 1 s（由 gateway 转成对外错误）。
设计没有给 mcp 的排队上限；mcp 是 agent 代用户同步调用，与 answer 一样取 1 s。
"""

from __future__ import annotations

import asyncio
import heapq
import itertools

DEFAULT_LIMIT = 30
PRIORITY = {"answer": 0, "mcp": 1, "batch": 2}
MAX_WAIT = {"answer": 1.0, "mcp": 1.0, "batch": 0.5}


class Overloaded(Exception):
    pass


class Admission:
    """asyncio 单线程使用。acquire() 成功后必须 release()。"""

    def __init__(self, limit: int = DEFAULT_LIMIT, max_wait: dict[str, float] = MAX_WAIT) -> None:
        if limit < 1:
            raise ValueError("并发上限至少为 1")
        self.limit, self._max_wait = limit, max_wait
        self.active = 0
        # (优先级, 到达顺序, 等待名额的 future)；超时离开的留在堆里，取出时跳过
        self._waiters: list[tuple[int, int, asyncio.Future[None]]] = []
        self._seq = itertools.count()

    @property
    def queued(self) -> int:
        return sum(1 for _, _, f in self._waiters if not f.done())

    async def acquire(self, purpose: str) -> None:
        if self.active < self.limit and not self.queued:
            self.active += 1
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(self._waiters, (PRIORITY[purpose], next(self._seq), fut))
        try:
            await asyncio.wait_for(asyncio.shield(fut), self._max_wait[purpose])
        except TimeoutError:
            if fut.done() and not fut.cancelled():
                # 名额恰好在超时的同一时刻交过来：照常使用
                return
            fut.cancel()
            raise Overloaded(purpose) from None
        except asyncio.CancelledError:
            # 调用方在排队时被取消（客户端断开）：名额已交过来就还回去
            if fut.done() and not fut.cancelled():
                self.release()
            else:
                fut.cancel()
            raise

    def release(self) -> None:
        """名额直接交给排在最前的请求（active 不变）；没有人排队时才减少。"""
        while self._waiters:
            _, _, fut = heapq.heappop(self._waiters)
            if not fut.done():
                fut.set_result(None)
                return
        self.active -= 1
