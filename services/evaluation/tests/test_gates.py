import asyncio
import datetime as dt
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import grpc
import pytest

from bambuflow.admin.v1 import eval_gate_admin_pb2, eval_gate_admin_pb2_grpc
from bambuflow.identity.v1 import principal_pb2
from bambuflow_evaluation import gates
from bambuflow_platform.grpc_server import AuthInterceptor
from bambuflow_platform.principal import JWKS, Verifier

if TYPE_CHECKING:
    from bambuflow.admin.v1.eval_gate_admin_pb2_grpc import EvalGateAdminServiceAsyncStub as Stub


class FakeVerifier(Verifier):
    def __init__(self) -> None:
        super().__init__(JWKS("http://unused"), operator=True)

    def verify(self, raw: str) -> principal_pb2.Principal:
        return principal_pb2.Principal(sub=raw, roles=[raw])


class MemStore:
    def __init__(self) -> None:
        self.rows: list[gates.GateRun] = []

    async def put(self, g: gates.GateRun) -> None:
        self.rows.append(g)

    async def list(self, tenant: str, trigger: str, offset: int, limit: int) -> list[gates.GateRun]:
        rows = [
            g
            for g in sorted(self.rows, key=lambda g: g.created_at, reverse=True)
            if (not tenant or tenant in g.tenants) and (not trigger or g.trigger == trigger)
        ]
        return [
            g.__class__(**{**g.__dict__, "report_md": ""}) for g in rows[offset : offset + limit]
        ]

    async def get(self, gate_id: str) -> gates.GateRun | None:
        return next((g for g in self.rows if g.gate_id == gate_id), None)


REPORT = {
    "golden": {"name": "wiki-demo", "version": "v3", "provenance": {}},
    "config": {"snapshot_version": ["vocab-1"], "index_version": ["ix-1"]},
    "repeats": 3,
    "conclusion": "failed",
    "reasons": ["credit 租户 groundedness 下降 3 个点"],
    "unstable": ["q_1"],
    "metrics": {"groundedness": 0.91, "latency_p95_ms": None},
}


def run(store: MemStore, body: "Callable[[Stub], Awaitable[None]]") -> None:
    async def main() -> None:
        server = grpc.aio.server(interceptors=[AuthInterceptor(FakeVerifier())])
        eval_gate_admin_pb2_grpc.add_EvalGateAdminServiceServicer_to_server(
            gates.GateService(store), server
        )
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
                await body(eval_gate_admin_pb2_grpc.EvalGateAdminServiceStub(ch))
        finally:
            await server.stop(None)

    asyncio.run(main())


def as_(role: str) -> dict[str, Any]:
    return {"metadata": (("authorization", "Bearer " + role),), "timeout": 5}


def test_new_gate_takes_conclusion_and_summary_from_report() -> None:
    g = gates.new_gate("ci", ["ops", "credit"], REPORT, "# 报告")
    assert g.gate_id.startswith("gate_") and g.tenants == ("credit", "ops")
    assert (g.golden_set, g.golden_version, g.conclusion) == ("wiki-demo", "v3", "failed")
    assert g.summary["metrics"]["groundedness"] == 0.91 and "golden" not in g.summary


def test_list_and_get_gates() -> None:
    store = MemStore()
    t0 = dt.datetime(2026, 10, 9, tzinfo=dt.UTC)
    a = gates.new_gate("ci", ["credit"], REPORT, "# A")
    b = gates.new_gate("scheduled", ["ops"], REPORT | {"conclusion": "passed"}, "# B")
    store.rows = [
        a.__class__(**{**a.__dict__, "created_at": t0}),
        b.__class__(**{**b.__dict__, "created_at": t0 + dt.timedelta(hours=1)}),
    ]
    ops = as_("platform_operator")

    async def body(stub: "Stub") -> None:
        page = await stub.ListGates(eval_gate_admin_pb2.ListGatesRequest(page_size=1), **ops)
        assert [g.gate_id for g in page.gates] == [b.gate_id] and page.next_page_token
        rest = await stub.ListGates(
            eval_gate_admin_pb2.ListGatesRequest(page_size=1, page_token=page.next_page_token),
            **ops,
        )
        assert [g.gate_id for g in rest.gates] == [a.gate_id] and not rest.next_page_token
        credit = await stub.ListGates(eval_gate_admin_pb2.ListGatesRequest(tenant="credit"), **ops)
        (g,) = credit.gates
        assert g.conclusion == "failed" and list(g.reasons) == REPORT["reasons"]
        metrics = {m.name: m for m in g.metrics}
        assert metrics["groundedness"].value == 0.91
        assert not metrics["latency_p95_ms"].HasField("value")
        got = await stub.GetGate(eval_gate_admin_pb2.GetGateRequest(gate_id=a.gate_id), **ops)
        assert got.report_markdown == "# A" and got.gate.repeats == 3
        for bad, code in (
            (
                stub.GetGate(eval_gate_admin_pb2.GetGateRequest(gate_id="gate_x"), **ops),
                grpc.StatusCode.NOT_FOUND,
            ),
            (
                stub.ListGates(eval_gate_admin_pb2.ListGatesRequest(trigger="nightly"), **ops),
                grpc.StatusCode.INVALID_ARGUMENT,
            ),
            (
                stub.ListGates(eval_gate_admin_pb2.ListGatesRequest(), **as_("auditor")),
                grpc.StatusCode.PERMISSION_DENIED,
            ),
        ):
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await bad
            assert e.value.code() == code

    run(store, body)
