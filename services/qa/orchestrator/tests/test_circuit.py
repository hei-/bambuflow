"""熔断器：1 分钟窗口错误率超过 20% 打开，30 秒后半开，连续 10 次成功关闭。"""

from bambuflow_orchestrator import circuit
from bambuflow_orchestrator.circuit import Breaker, State


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def state(b: Breaker) -> State:
    # 经函数读取：直接比较 b.state 会被类型检查按上一次比较的结果收窄
    return b.state


def calls(b: Breaker, ok: int, failed: int) -> None:
    for _ in range(ok):
        assert b.allow()
        b.record(True)
    for _ in range(failed):
        assert b.allow()
        b.record(False)


def test_opens_when_error_rate_exceeds_threshold() -> None:
    b = Breaker("m", Clock())
    calls(b, ok=8, failed=2)  # 20% 不算超过
    assert state(b) is State.CLOSED
    calls(b, ok=0, failed=1)  # 3 / 11 ≈ 27%
    assert state(b) is State.OPEN
    assert not b.allow()


def test_needs_minimum_calls_before_opening() -> None:
    b = Breaker("m", Clock())
    calls(b, ok=0, failed=circuit.MIN_CALLS - 1)
    assert state(b) is State.CLOSED, "调用太少时一两次失败不能说明网关不可用"
    calls(b, ok=0, failed=1)
    assert state(b) is State.OPEN


def test_old_calls_leave_the_window() -> None:
    clock = Clock()
    b = Breaker("m", clock)
    calls(b, ok=0, failed=5)
    clock.now += circuit.WINDOW_SECONDS + 1
    calls(b, ok=9, failed=1)  # 窗口里只剩 1 / 10
    assert state(b) is State.CLOSED


def test_half_open_after_cool_down_and_closes_after_consecutive_successes() -> None:
    clock = Clock()
    b = Breaker("m", clock)
    calls(b, ok=0, failed=circuit.MIN_CALLS)
    clock.now += circuit.OPEN_SECONDS - 1
    assert not b.allow()
    clock.now += 1
    assert b.allow()
    assert state(b) is State.HALF_OPEN
    b.record(True)
    for _ in range(circuit.CLOSE_AFTER - 2):
        assert b.allow()
        b.record(True)
    assert state(b) is State.HALF_OPEN
    assert b.allow()
    b.record(True)
    assert state(b) is State.CLOSED
    # 关闭后从头统计：此前的失败不再计入
    calls(b, ok=0, failed=1)
    assert state(b) is State.CLOSED


def test_failure_while_half_open_reopens() -> None:
    clock = Clock()
    b = Breaker("m", clock)
    calls(b, ok=0, failed=circuit.MIN_CALLS)
    clock.now += circuit.OPEN_SECONDS
    calls(b, ok=5, failed=1)
    assert state(b) is State.OPEN
    assert not b.allow(), "重新打开后再等一个冷却期"
    clock.now += circuit.OPEN_SECONDS
    assert b.allow()


def test_half_open_limits_concurrent_probes() -> None:
    clock = Clock()
    b = Breaker("m", clock)
    calls(b, ok=0, failed=circuit.MIN_CALLS)
    clock.now += circuit.OPEN_SECONDS
    for _ in range(circuit.CLOSE_AFTER):
        assert b.allow()
    assert not b.allow(), "探测名额用完时其余调用仍按打开处理"
    b.release()  # 一次探测既不算成功也不算失败地结束（如客户端断开），名额归还
    assert b.allow()
