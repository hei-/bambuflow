import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any

import grpc
import pytest

from bambuflow.evaluation.v1 import gate_pb2, gate_pb2_grpc
from bambuflow_evaluation import audit, gaterun, gates
from bambuflow_evaluation.golden import Asker, GoldenSet, Question
from bambuflow_evaluation.metrics import Outcome
from bambuflow_evaluation.runner import Config

if TYPE_CHECKING:
    from bambuflow.evaluation.v1.gate_pb2_grpc import EvalServiceAsyncStub as Stub

ASKER = {t: Asker(t + "_user", "u", t, "部门", 2, ("g",)) for t in ("credit", "ops")}


def question(i: int, tenant: str) -> Question:
    return Question(
        f"q_{tenant}_{i:03d}", "single_hop", ASKER[tenant], "问", "answered", (), ("src_a:d#c",), ()
    )


SET = GoldenSet("demo", "v1", {}, [question(i, t) for t in ("credit", "ops") for i in range(40)])


class MemStore:
    def __init__(self) -> None:
        self.rows: dict[str, gates.GateRun] = {}
        self.keys: dict[str, str] = {}
        self.index: dict[str, str] = {}
        self.config: dict[str, str] = {}

    async def start(
        self, g: gates.GateRun, key: str, index_version: str, config_version: str
    ) -> str:
        if key and key in self.keys:
            return self.keys[key]
        self.rows[g.gate_id] = g
        self.index[g.gate_id] = index_version
        self.config[g.gate_id] = config_version
        if key:
            self.keys[key] = g.gate_id
        return g.gate_id

    async def finish(
        self, gate_id: str, conclusion: str, summary: dict[str, Any], report_md: str
    ) -> None:
        g = self.rows[gate_id]
        self.rows[gate_id] = gates.GateRun(
            g.gate_id,
            g.trigger,
            g.golden_set,
            g.golden_version,
            g.tenants,
            conclusion,
            summary,
            report_md,
            g.created_at,
        )

    async def get(self, gate_id: str) -> gates.GateRun | None:
        return self.rows.get(gate_id)


class FakeRunner:
    """线上版本每题都引用到 must_cite；候选版本按 broken 决定是否丢掉引用。"""

    def __init__(
        self, index_version: str, broken: bool, delay: float, seen: list[tuple[str, int]]
    ) -> None:
        self.index_version, self.broken, self.delay, self.seen = index_version, broken, delay, seen
        self.judge_model = "nli"
        self.llm_judge = ""
        self.config = Config()

    async def run(self, questions: Sequence[Question]) -> list[Outcome]:
        self.seen.append((self.index_version, len(questions)))
        await asyncio.sleep(self.delay)
        bad = self.broken and self.index_version != ""
        return [
            Outcome(
                q.id,
                q.asker.tenant,
                q.bucket,
                q.expected_status,
                "answered",
                q.must_cite,
                () if bad else q.must_cite,
                () if bad else q.must_cite,
                (True,),
                (not bad,),
                False,
                (),
                100.0,
            )
            for q in questions
        ]


def run(service: gaterun.EvalService, body: "Callable[[Stub], Awaitable[None]]") -> None:
    async def main() -> None:
        server = grpc.aio.server()
        gate_pb2_grpc.add_EvalServiceServicer_to_server(service, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
                await body(gate_pb2_grpc.EvalServiceStub(ch))
        finally:
            await server.stop(None)

    asyncio.run(main())


async def wait(stub: "Stub", gate_id: str) -> gate_pb2.GetGateResponse:
    for _ in range(100):
        r = await stub.GetGate(gate_pb2.GetGateRequest(gate_id=gate_id), timeout=5)
        if r.verdict != gate_pb2.VERDICT_RUNNING:
            return r
        await asyncio.sleep(0.02)
    raise AssertionError("门禁一直没有结论")


class MemAudit:
    def __init__(self, fail: bool = False) -> None:
        self.bodies: list[bytes] = []
        self.fail = fail

    def put(self, key: str, body: bytes) -> None:
        if self.fail:
            raise OSError("审计桶不可用")
        self.bodies.append(body)


def service(
    store: MemStore, broken: bool = False, delay: float = 0.0, trail: MemAudit | None = None
) -> tuple[gaterun.EvalService, list[tuple[str, int]]]:
    seen: list[tuple[str, int]] = []
    return gaterun.EvalService(
        store,
        lambda: [SET],
        # 候选（索引版本或候选配置）一侧按 broken 变差
        lambda v, c, g: FakeRunner(v or c, broken, delay, seen),
        audit.Writer(trail or MemAudit()),
    ), seen


def test_index_publish_gate_compares_candidate_with_live_on_smoke_set() -> None:
    store = MemStore()
    trail = MemAudit()
    svc, seen = service(store, trail=trail)

    async def body(stub: "Stub") -> None:
        req = gate_pb2.RunGateRequest(
            trigger="index_publish",
            candidate=gate_pb2.EvalConfig(index_version="ix-2"),
            tenants=["credit"],
            idempotency_key="ix-2",
        )
        first = await stub.RunGate(req, timeout=5)
        again = await stub.RunGate(req, timeout=5)
        assert first.gate_id == again.gate_id and first.gate_id.startswith("gate_")
        done = await wait(stub, first.gate_id)
        assert done.verdict == gate_pb2.VERDICT_PASSED and done.report_uri.endswith(first.gate_id)

    run(svc, body)
    # 冒烟集每租户 30 题；先跑线上版本，再跑候选版本
    assert seen == [("", 30), ("ix-2", 30)]
    # 分阶段接入的情况按租户的全部题算（40 道没有审核人的题，一道不计）
    (g,) = store.rows.values()
    assert [(r["tenant"], r["stage"], r["unreviewed"]) for r in g.summary["readiness"]] == [
        ("credit", "none", 40)
    ]
    # 门禁结论写变更审计
    (rec,) = [json.loads(b) for b in trail.bodies]
    assert (rec["action"], rec["object_id"], rec["detail"]["conclusion"]) == (
        "gate.conclude",
        g.gate_id,
        "passed",
    )


def test_gate_is_indeterminate_when_audit_unavailable() -> None:
    store = MemStore()
    svc, _ = service(store, trail=MemAudit(fail=True))

    async def body(stub: "Stub") -> None:
        req = gate_pb2.RunGateRequest(
            trigger="index_publish", candidate=gate_pb2.EvalConfig(index_version="ix-9")
        )
        done = await wait(stub, (await stub.RunGate(req, timeout=5)).gate_id)
        # 结论写不进审计不能算通过
        assert done.verdict == gate_pb2.VERDICT_INDETERMINATE

    run(svc, body)


class UnauditedRunner(FakeRunner):
    """回放的请求写不进审计。"""

    async def run(self, questions: Sequence[Question]) -> list[Outcome]:
        raise audit.AuditUnavailableError("审计桶不可用")


def test_gate_is_indeterminate_when_replay_cannot_be_audited() -> None:
    store = MemStore()
    seen: list[tuple[str, int]] = []
    svc = gaterun.EvalService(
        store,
        lambda: [SET],
        lambda v, c, g: UnauditedRunner(v or c, False, 0.0, seen),
        audit.Writer(MemAudit()),
    )

    async def body(stub: "Stub") -> None:
        req = gate_pb2.RunGateRequest(
            trigger="index_publish", candidate=gate_pb2.EvalConfig(index_version="ix-9")
        )
        done = await wait(stub, (await stub.RunGate(req, timeout=5)).gate_id)
        assert done.verdict == gate_pb2.VERDICT_INDETERMINATE
        assert "评测回放写审计失败" in done.violations

    run(svc, body)


def test_regression_fails_and_timeout_fails() -> None:
    store = MemStore()
    svc, _ = service(store, broken=True)

    async def body(stub: "Stub") -> None:
        r = await stub.RunGate(
            gate_pb2.RunGateRequest(
                trigger="index_publish", candidate=gate_pb2.EvalConfig(index_version="ix-3")
            ),
            timeout=5,
        )
        done = await wait(stub, r.gate_id)
        assert done.verdict == gate_pb2.VERDICT_FAILED and done.violations

    run(svc, body)

    slow, _ = service(MemStore(), delay=1.0)

    async def timeout_body(stub: "Stub") -> None:
        r = await stub.RunGate(
            gate_pb2.RunGateRequest(trigger="ci", deadline={"nanos": 100_000_000}), timeout=5
        )
        done = await wait(stub, r.gate_id)
        assert done.verdict == gate_pb2.VERDICT_FAILED and "时间预算" in done.violations[0]

    run(slow, timeout_body)


def test_bad_requests() -> None:
    svc, _ = service(MemStore())

    async def body(stub: "Stub") -> None:
        for req, code in (
            (gate_pb2.RunGateRequest(trigger="manual"), grpc.StatusCode.INVALID_ARGUMENT),
            (
                gate_pb2.RunGateRequest(
                    trigger="ci", candidate=gate_pb2.EvalConfig(index_version="a b")
                ),
                grpc.StatusCode.INVALID_ARGUMENT,
            ),
            (
                gate_pb2.RunGateRequest(
                    trigger="config_release", candidate=gate_pb2.EvalConfig(config_version="rel_x")
                ),
                grpc.StatusCode.INVALID_ARGUMENT,
            ),
            (
                gate_pb2.RunGateRequest(trigger="ci", ruleset="quick"),
                grpc.StatusCode.INVALID_ARGUMENT,
            ),
            (
                gate_pb2.RunGateRequest(trigger="ci", tenants=["nobody"]),
                grpc.StatusCode.FAILED_PRECONDITION,
            ),
        ):
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await stub.RunGate(req, timeout=5)
            assert e.value.code() == code
        with pytest.raises(grpc.aio.AioRpcError) as e:
            await stub.GetGate(gate_pb2.GetGateRequest(gate_id="gate_x"), timeout=5)
        assert e.value.code() == grpc.StatusCode.NOT_FOUND

    run(svc, body)


class NoLiveRunner(FakeRunner):
    """还没有线上版本：不指定版本的检索直接失败（retrieval 返回 no_live_index），
    与真实情况一样抛出。"""

    async def run(self, questions: Sequence[Question]) -> list[Outcome]:
        if not self.index_version:
            raise RuntimeError("检索暂不可用")
        return await super().run(questions)


def test_first_publish_has_no_baseline_and_only_checks_violations() -> None:
    store = MemStore()
    seen: list[tuple[str, int]] = []
    svc = gaterun.EvalService(
        store,
        lambda: [SET],
        lambda v, c, g: NoLiveRunner(v or c, False, 0.0, seen),
        audit.Writer(MemAudit()),
    )

    async def body(stub: "Stub") -> None:
        req = gate_pb2.RunGateRequest(
            trigger="index_publish",
            candidate=gate_pb2.EvalConfig(index_version="ix-1"),
            first_release=True,
        )
        r = await stub.RunGate(req, timeout=5)
        assert (await wait(stub, r.gate_id)).verdict == gate_pb2.VERDICT_PASSED
        # 没声明第一次发布时，基线跑不出来就无法判定，不放宽成只查越权
        req.first_release, req.idempotency_key = False, "again"
        r = await stub.RunGate(req, timeout=5)
        assert (await wait(stub, r.gate_id)).verdict == gate_pb2.VERDICT_INDETERMINATE

    run(svc, body)


def test_config_release_gate_replays_the_candidate_config() -> None:
    """配置发布的门禁：候选一侧带上候选配置版本，线上一侧用生效的配置；策略包只跑权限题。"""
    store = MemStore()
    guarded = Question(
        "q_credit_900", "permission", ASKER["credit"], "问", "refused", (), (), ("src_b:x#c",)
    )
    seen: list[tuple[str, int]] = []
    svc = gaterun.EvalService(
        store,
        lambda: [GoldenSet("demo", "v1", {}, [*SET.questions, guarded])],
        lambda v, c, g: FakeRunner(v or c, False, 0.0, seen),
        audit.Writer(MemAudit()),
    )
    version = "cfv_" + "1" * 24

    async def body(stub: "Stub") -> None:
        got = await stub.RunGate(
            gate_pb2.RunGateRequest(
                trigger="config_release",
                candidate=gate_pb2.EvalConfig(config_version=version),
                tenants=["credit"],
                ruleset="permission",
                idempotency_key="rel_" + "a" * 24,
            ),
            timeout=5,
        )
        assert (await wait(stub, got.gate_id)).verdict == gate_pb2.VERDICT_PASSED
        assert store.config[got.gate_id] == version and store.index[got.gate_id] == ""

    run(svc, body)
    assert seen == [("", 1), (version, 1)]
