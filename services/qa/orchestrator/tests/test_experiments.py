import asyncio

import grpc
import pytest

from bambuflow.answer.v1 import answer_pb2
from bambuflow.config.v1 import config_pb2
from bambuflow_orchestrator import experiments
from bambuflow_orchestrator.experiments import Experiments, assign, bucket


def exp(traffic: int = 20, threshold: float | None = 0.4) -> config_pb2.RunningExperiment:
    e = config_pb2.RunningExperiment(exp_id="exp_1", tenant="credit", traffic_percent=traffic)
    if threshold is not None:
        e.support_threshold = threshold
    return e


def test_assignment_is_stable_and_follows_traffic() -> None:
    users = [f"u{i}" for i in range(4000)]
    first = [assign(exp(), u).arm for u in users]
    assert first == [assign(exp(), u).arm for u in users]
    share = sum(a == answer_pb2.EXPERIMENT_ARM_TREATMENT for a in first) / len(users)
    assert 0.17 < share < 0.23
    # 推进流量只会把对照组的用户移进实验组，已在实验组的不会出来
    wider = [assign(exp(50), u).arm for u in users]
    assert all(
        w == answer_pb2.EXPERIMENT_ARM_TREATMENT
        for a, w in zip(first, wider, strict=True)
        if a == answer_pb2.EXPERIMENT_ARM_TREATMENT
    )
    # 不同实验的分组互相独立
    assert bucket("exp_1", "u1") != bucket("exp_2", "u1") or bucket("exp_1", "u2") != bucket(
        "exp_2", "u2"
    )


def test_treatment_carries_only_given_overrides() -> None:
    user = next(u for u in (f"u{i}" for i in range(1000)) if bucket("exp_1", u) < 20)
    a = assign(exp(), user)
    assert a.treatment and a.support_threshold == 0.4 and a.min_coverage is None
    other = next(u for u in (f"u{i}" for i in range(1000)) if bucket("exp_1", u) >= 20)
    b = assign(exp(), other)
    assert not b.treatment and b.support_threshold is None


def test_experiments_by_tenant() -> None:
    xs = Experiments([exp()])
    assert xs.assignment("risk", "u1") is None
    a = xs.assignment("credit", "u1")
    assert a is not None and a.exp_id == "exp_1"
    xs.replace([])
    assert xs.assignment("credit", "u1") is None


class FailingCall(grpc.RpcError):
    pass


def test_refresh_replaces_and_keeps_last_on_failure() -> None:
    xs = Experiments([exp()])
    calls = 0

    async def fetch() -> config_pb2.GetSnapshotResponse:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FailingCall()
        if calls == 2:
            return config_pb2.GetSnapshotResponse(snapshot_version="v2", experiments=[exp(50)])
        raise asyncio.CancelledError

    async def go() -> None:
        await experiments.refresh(xs, fetch, every=0)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(go())
    assert calls == 3
    assert xs.assignment("credit", "u1") == assign(exp(50), "u1")
