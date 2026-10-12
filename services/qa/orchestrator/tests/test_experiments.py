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


def test_only_treatment_carries_the_variant() -> None:
    user = next(u for u in (f"u{i}" for i in range(1000)) if bucket("exp_1", u) < 20)
    a = assign(exp(), user)
    assert a.treatment and a.variant is not None and a.variant.support_threshold == 0.4
    other = next(u for u in (f"u{i}" for i in range(1000)) if bucket("exp_1", u) >= 20)
    b = assign(exp(), other)
    assert not b.treatment and b.variant is None


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


def test_refresh_keeps_last_on_invalid_signature_and_acks() -> None:
    from bambuflow_platform.snapshot import InvalidSnapshotError

    xs = Experiments([exp()])
    acks: list[tuple[str, str]] = []
    calls = 0

    async def fetch() -> config_pb2.GetSnapshotResponse:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InvalidSnapshotError("签名不对")
        if calls == 2:
            return config_pb2.GetSnapshotResponse(snapshot_version="v2", experiments=[exp(50)])
        raise asyncio.CancelledError

    async def ack(version: str, reason: str) -> None:
        acks.append((version, reason))

    async def go() -> None:
        await experiments.refresh(xs, fetch, every=0, wake=asyncio.Event(), ack=ack)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(go())
    assert acks == [("", "signature_invalid"), ("v2", "")]


def test_refresh_replaces_vocabulary_from_same_snapshot() -> None:
    from bambuflow_vocabulary import Vocabularies

    got: list[Vocabularies] = []
    calls = 0

    async def fetch() -> config_pb2.GetSnapshotResponse:
        nonlocal calls
        calls += 1
        if calls == 1:
            snap = config_pb2.GetSnapshotResponse(snapshot_version="v2")
            for name in ("doc_type", "dept", "topic"):
                v = snap.vocabularies.add(name=name)
                v.terms.add(code="X-" + name.upper().replace("_", "-"), name=name)
            return snap
        if calls == 2:
            # 缺了词表的快照不用
            return config_pb2.GetSnapshotResponse(snapshot_version="v3")
        raise asyncio.CancelledError

    async def go() -> None:
        await experiments.refresh(Experiments(), fetch, every=0, on_vocab=got.append)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(go())
    assert len(got) == 1 and got[0].version == "v2"
