import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import grpc
import pytest
import yaml

from bambuflow.admin.v1 import golden_admin_pb2, golden_admin_pb2_grpc
from bambuflow.identity.v1 import principal_pb2
from bambuflow_evaluation import candidates
from bambuflow_evaluation.candidates import Candidate, ReviewRefusedError, review
from bambuflow_platform.grpc_server import AuthInterceptor
from bambuflow_platform.principal import JWKS, Verifier

if TYPE_CHECKING:
    from bambuflow.admin.v1.golden_admin_pb2_grpc import GoldenSetAdminServiceAsyncStub as Stub

GOLDEN = Path(__file__).resolve().parents[3] / "golden"


class FakeVerifier(Verifier):
    """令牌写成 “用户名:角色1,角色2”。"""

    def __init__(self) -> None:
        super().__init__(JWKS("http://unused"), operator=True)

    def verify(self, raw: str) -> principal_pb2.Principal:
        sub, roles = raw.split(":")
        return principal_pb2.Principal(sub=sub, roles=roles.split(","))


class MemStore:
    def __init__(self) -> None:
        self.rows: dict[str, Candidate] = {}

    async def put(self, c: Candidate) -> None:
        self.rows[c.cand_id] = c

    async def list(self, tenant: str) -> list[Candidate]:
        return [c for c in self.rows.values() if c.tenant == tenant]

    async def update(
        self, cand_id: str, change: Callable[[Candidate], Candidate]
    ) -> Candidate | None:
        if cand_id not in self.rows:
            return None
        self.rows[cand_id] = change(self.rows[cand_id])
        return self.rows[cand_id]


def run(store: MemStore, body: "Callable[[Stub], Awaitable[None]]") -> None:
    sets = candidates.golden_sets(GOLDEN)

    async def main() -> None:
        server = grpc.aio.server(interceptors=[AuthInterceptor(FakeVerifier())])
        golden_admin_pb2_grpc.add_GoldenSetAdminServiceServicer_to_server(
            candidates.GoldenService(store, lambda: sets), server
        )
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
                await body(golden_admin_pb2_grpc.GoldenSetAdminServiceStub(ch))
        finally:
            await server.stop(None)

    asyncio.run(main())


def as_(token: str) -> dict[str, Any]:
    return {"metadata": (("authorization", "Bearer " + token),), "timeout": 5}


DANA, RICK, PAUL, PETE = (
    "dana:data_owner",
    "rick:data_owner",
    "paul:platform_admin",
    "pete:platform_admin",
)


def candidate(**kw: Any) -> Candidate:
    base = Candidate(
        "cand_" + "0" * 24,
        "credit",
        "wiki-demo",
        "single_hop",
        "credit_l2",
        "问",
        {"status": "answered"},
        "feedback",
        "dana",
    )
    return replace(base, **kw)


def test_review_needs_an_expert_and_a_platform_seat() -> None:
    c = candidate()
    with pytest.raises(ReviewRefusedError) as e:
        review(c, "dana", {"data_owner"}, True, "")
    assert e.value.reason == "self_approval_forbidden"
    c = review(c, "rick", {"data_owner"}, True, "")
    assert (c.status, c.expert_reviewer) == ("pending", "rick")
    # 专家席已满：另一位数据属主不能再复核
    with pytest.raises(ReviewRefusedError) as e:
        review(c, "ivy", {"data_owner"}, True, "")
    assert e.value.reason == "conflict"
    # 同一人不能占两席
    with pytest.raises(ReviewRefusedError):
        review(c, "rick", {"data_owner", "platform_admin"}, True, "")
    c = review(c, "paul", {"platform_admin"}, True, "")
    assert (c.status, c.platform_reviewer) == ("accepted", "paul")
    rejected = review(candidate(), "paul", {"platform_admin"}, False, "标准答案不对")
    assert (rejected.status, rejected.reject_reason) == ("rejected", "标准答案不对")


def test_list_submit_review_and_export() -> None:
    store = MemStore()

    async def body(stub: "Stub") -> None:
        listed = await stub.ListQuestions(
            golden_admin_pb2.ListQuestionsRequest(tenant="credit"), **as_(PAUL)
        )
        assert listed.questions and all(q.status == "in_set" for q in listed.questions)
        assert listed.golden_versions[0].startswith("wiki-demo@")
        req = golden_admin_pb2.SubmitCandidateRequest(
            tenant="credit",
            golden_set="wiki-demo",
            bucket="single_hop",
            asker="credit_l2",
            question="个人消费贷款最长期限是多少？",
            expected=golden_admin_pb2.GoldenExpected(status="answered", key_points=["最长 5 年"]),
            origin="feedback",
        )
        # 提问身份必须属于这个租户
        with pytest.raises(grpc.aio.AioRpcError) as e:
            await stub.SubmitCandidate(
                golden_admin_pb2.SubmitCandidateRequest(
                    tenant="credit",
                    golden_set="wiki-demo",
                    bucket="single_hop",
                    asker="ops_staff",
                    question="x",
                    expected=golden_admin_pb2.GoldenExpected(status="answered"),
                    origin="feedback",
                ),
                **as_(DANA),
            )
        assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        with pytest.raises(grpc.aio.AioRpcError) as e:
            await stub.SubmitCandidate(req, **as_("olga:platform_operator"))
        assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED
        sub = await stub.SubmitCandidate(req, **as_(DANA))
        qid = sub.question.question_id
        assert qid.startswith("cand_") and sub.question.status == "pending"

        rv = golden_admin_pb2.ReviewCandidateRequest(
            tenant="credit", question_id=qid, approved=True
        )
        with pytest.raises(grpc.aio.AioRpcError) as e:
            await stub.ReviewCandidate(rv, **as_(DANA))
        assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED
        # 错误码以 google.rpc.ErrorInfo 附在状态详情里（gateway 的 grpcmw.ReasonOf 读取）
        trailers = repr(e.value.trailing_metadata())
        assert "grpc-status-details-bin" in trailers
        assert "google.rpc.ErrorInfo" in trailers and "self_approval_forbidden" in trailers

        await stub.ReviewCandidate(rv, **as_(RICK))
        done = await stub.ReviewCandidate(rv, **as_(PETE))
        assert done.question.status == "accepted"
        with pytest.raises(grpc.aio.AioRpcError) as e:
            await stub.ReviewCandidate(
                golden_admin_pb2.ReviewCandidateRequest(
                    tenant="credit", question_id=qid, approved=False
                ),
                **as_(PAUL),
            )
        assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT  # 驳回要写原因
        all_ = await stub.ListQuestions(
            golden_admin_pb2.ListQuestionsRequest(tenant="credit", include_candidates=True),
            **as_(DANA),
        )
        assert any(q.question_id == qid and q.status == "accepted" for q in all_.questions)

    run(store, body)
    (item,) = yaml.safe_load(candidates.export_yaml(list(store.rows.values())))
    assert item["asker"] == "credit_l2" and item["provenance"]["reviewed_by"] == ["rick", "pete"]
