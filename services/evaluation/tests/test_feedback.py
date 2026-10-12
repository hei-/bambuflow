import asyncio
import datetime as dt
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import grpc
import pytest

from bambuflow.admin.v1 import golden_admin_pb2, golden_admin_pb2_grpc
from bambuflow.answer.v1 import answer_pb2, records_pb2
from bambuflow.identity.v1 import principal_pb2
from bambuflow_evaluation import candidates, feedback
from bambuflow_evaluation.candidates import Candidate
from bambuflow_platform.grpc_server import AuthInterceptor
from bambuflow_platform.principal import JWKS, Verifier

DAY = dt.datetime(2026, 10, 9, 8, tzinfo=dt.UTC)


class FakeVerifier(Verifier):
    """令牌写成 “用户名:角色”。"""

    def __init__(self) -> None:
        super().__init__(JWKS("http://unused"), operator=True)

    def verify(self, raw: str) -> principal_pb2.Principal:
        sub, roles = raw.split(":")
        return principal_pb2.Principal(sub=sub, roles=roles.split(","))


class NoCandidates:
    async def put(self, c: Candidate) -> None: ...

    async def list(self, tenant: str) -> list[Candidate]:
        return []

    async def update(self, cand_id: str, change: Callable[[Candidate], Candidate]) -> None:
        return None


def as_(token: str) -> dict[str, Any]:
    return {"metadata": (("authorization", "Bearer " + token),), "timeout": 5}


def record(
    answer_id: str, ratings: list[str], question: str = "额度上限是多少？"
) -> records_pb2.AnswerRecord:
    r = records_pb2.AnswerRecord(
        answer_id=answer_id,
        status=answer_pb2.STATUS_ANSWERED,
        feedback=ratings,
        masked_question=question,
        feedback_comments=["引用的是旧版"] if "bad_citation" in ratings else [],
    )
    r.created_at.FromDatetime(DAY)
    r.answer.citations.add(doc_id="credit-loan", title="个人消费贷款管理办法")
    r.answer.citations.add(doc_id="credit-loan", title="个人消费贷款管理办法")
    return r


def test_counts_and_negative_cases() -> None:
    recs = [
        record("ans_1", ["useful"]),
        record("ans_2", ["wrong", "bad_citation"]),
        record("ans_3", []),
        record("ans_4", ["bad_citation"], question=""),
    ]
    assert feedback.count(recs) == feedback.Counts(answers=4, useful=1, wrong=1, bad_citation=2)
    (c,) = feedback.cases("credit", recs)  # 没有掩码问题的记录不登记
    assert (c.answer_id, c.ratings) == ("ans_2", ("wrong", "bad_citation"))
    assert c.comments == ("引用的是旧版",)
    assert (c.cited_doc_ids, c.cited_titles, c.answer_status) == (
        ("credit-loan",),
        ("个人消费贷款管理办法",),
        "answered",
    )


class MemCases:
    def __init__(self) -> None:
        self.rows: dict[str, feedback.Case] = {}

    async def upsert(self, cases: list[feedback.Case]) -> None:
        for c in cases:
            old = self.rows.get(c.answer_id)
            self.rows[c.answer_id] = c if old is None else old

    async def list(self, tenant: str, status: str) -> list[feedback.Case]:
        return [c for c in self.rows.values() if c.tenant == tenant and c.status == status]

    async def resolve(
        self, tenant: str, answer_id: str, resolution: str, by: str, note: str
    ) -> feedback.Case | None:
        c = self.rows.get(answer_id)
        if c is None or c.tenant != tenant or c.status != "open":
            return None
        self.rows[answer_id] = replace(
            c, status=resolution, resolved_by=by, resolution_note=note, resolved_at=DAY
        )
        return self.rows[answer_id]


def test_feedback_case_service() -> None:
    cases = MemCases()
    asyncio.run(cases.upsert(feedback.cases("credit", [record("ans_2", ["wrong"])])))

    async def main() -> None:
        server = grpc.aio.server(interceptors=[AuthInterceptor(FakeVerifier())])
        golden_admin_pb2_grpc.add_GoldenSetAdminServiceServicer_to_server(
            candidates.GoldenService(NoCandidates(), list, cases), server
        )
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
                stub = golden_admin_pb2_grpc.GoldenSetAdminServiceStub(ch)
                listed = await stub.ListFeedbackCases(
                    golden_admin_pb2.ListFeedbackCasesRequest(tenant="credit"),
                    **as_("olga:platform_operator"),
                )
                assert [c.answer_id for c in listed.cases] == ["ans_2"]
                assert listed.cases[0].masked_question == "额度上限是多少？"
                req = golden_admin_pb2.ResolveFeedbackCaseRequest(
                    tenant="credit", answer_id="ans_2", resolution="converted", note="cand_0123"
                )
                with pytest.raises(grpc.aio.AioRpcError) as e:
                    await stub.ResolveFeedbackCase(req, **as_("olga:platform_operator"))
                assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED
                bad = golden_admin_pb2.ResolveFeedbackCaseRequest(
                    tenant="credit", answer_id="ans_2", resolution="done", note="x"
                )
                with pytest.raises(grpc.aio.AioRpcError) as e:
                    await stub.ResolveFeedbackCase(bad, **as_("dana:data_owner"))
                assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
                done = await stub.ResolveFeedbackCase(req, **as_("dana:data_owner"))
                assert (done.case.status, done.case.resolved_by) == ("converted", "dana")
                with pytest.raises(grpc.aio.AioRpcError) as e:
                    await stub.ResolveFeedbackCase(req, **as_("dana:data_owner"))
                assert e.value.code() == grpc.StatusCode.NOT_FOUND
                left = await stub.ListFeedbackCases(
                    golden_admin_pb2.ListFeedbackCasesRequest(tenant="credit"),
                    **as_("dana:data_owner"),
                )
                assert not left.cases
        finally:
            await server.stop(None)

    asyncio.run(main())
