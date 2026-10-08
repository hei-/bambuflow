"""过载保护：并发上限、按调用目的的优先级排队、排队超时拒绝。"""

import asyncio

import pytest

from bambuflow_orchestrator.admission import Admission, Overloaded

WAIT = {"answer": 0.3, "mcp": 0.3, "batch": 0.1}


def test_within_limit_no_queueing() -> None:
    async def go() -> int:
        a = Admission(2, WAIT)
        await a.acquire("batch")
        await a.acquire("batch")
        return a.active

    assert asyncio.run(go()) == 2


def test_freed_slot_goes_to_highest_priority_then_earliest() -> None:
    async def go() -> list[str]:
        a = Admission(1, {"answer": 5, "mcp": 5, "batch": 5})
        await a.acquire("answer")
        order: list[str] = []

        async def wait(name: str, purpose: str) -> None:
            await a.acquire(purpose)
            order.append(name)
            a.release()

        tasks = []
        for name, purpose in [("b1", "batch"), ("m1", "mcp"), ("a1", "answer"), ("a2", "answer")]:
            tasks.append(asyncio.create_task(wait(name, purpose)))
            await asyncio.sleep(0)  # 按这个顺序排队
        a.release()
        await asyncio.gather(*tasks)
        assert a.active == 0
        return order

    assert asyncio.run(go()) == ["a1", "a2", "m1", "b1"]


@pytest.mark.parametrize(("purpose", "limit"), [("batch", 0.1), ("answer", 0.3)])
def test_queueing_too_long_is_rejected(purpose: str, limit: float) -> None:
    async def go() -> float:
        a = Admission(1, WAIT)
        await a.acquire("answer")
        loop = asyncio.get_running_loop()
        start = loop.time()
        with pytest.raises(Overloaded):
            await a.acquire(purpose)
        waited = loop.time() - start
        # 超时离开的请求不占名额：释放后名额回到空闲
        a.release()
        assert a.active == 0 and a.queued == 0
        return waited

    waited = asyncio.run(go())
    assert limit <= waited < limit + 0.2


def test_batch_times_out_while_answer_keeps_waiting() -> None:
    async def go() -> tuple[bool, int]:
        a = Admission(1, WAIT)
        await a.acquire("answer")
        batch = asyncio.create_task(a.acquire("batch"))
        answer = asyncio.create_task(a.acquire("answer"))
        await asyncio.sleep(0.2)  # batch 已超时，answer 还在排队
        a.release()
        await answer
        (outcome,) = await asyncio.gather(batch, return_exceptions=True)
        return isinstance(outcome, Overloaded), a.active

    assert asyncio.run(go()) == (True, 1)


def test_cancelled_waiter_gives_up_its_place() -> None:
    async def go() -> int:
        a = Admission(1, {"answer": 5, "mcp": 5, "batch": 5})
        await a.acquire("answer")
        waiter = asyncio.create_task(a.acquire("answer"))
        await asyncio.sleep(0)
        waiter.cancel()  # 客户端在排队时断开
        await asyncio.gather(waiter, return_exceptions=True)
        a.release()
        return a.active

    assert asyncio.run(go()) == 0
