"""AnswerService 的回放与反馈：谁能看、复核权限、幂等；以及回答记录写入失败不影响回答。"""

import asyncio
import dataclasses
import datetime as dt
import os
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from typing import Any

import grpc
import pytest

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.identity.v1 import principal_pb2
from bambuflow.retrieval.v1 import retrieval_pb2, retrieval_pb2_grpc
from bambuflow_orchestrator import flow, records, server
from bambuflow_orchestrator.admission import Admission
from bambuflow_orchestrator.session import Session
from bambuflow_platform import envelope, grpc_server


class Retrieval(retrieval_pb2_grpc.RetrievalServiceServicer):
    """只放行 visible 里的块与文档；fail 为真时复核报错。"""

    def __init__(self, visible: set[str], fail: bool = False) -> None:
        self.visible, self.fail = visible, fail
        self.purposes: list[str] = []

    async def CheckAccess(self, request: Any, context: Any) -> retrieval_pb2.CheckAccessResponse:
        self.purposes.append(dict(context.invocation_metadata()).get("x-call-purpose", ""))
        if self.fail:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "down")
        return retrieval_pb2.CheckAccessResponse(
            results=[
                retrieval_pb2.AccessResult(chunk_id=c, allowed=c in self.visible)
                for c in request.chunk_ids
            ]
        )

    async def CheckDocumentAccess(
        self, request: Any, context: Any
    ) -> retrieval_pb2.CheckDocumentAccessResponse:
        return retrieval_pb2.CheckDocumentAccessResponse(
            results=[
                retrieval_pb2.DocumentAccessResult(doc_id=d, allowed=d in self.visible)
                for d in request.doc_ids
            ]
        )

    # 回放不调用以下接口
    async def Search(self, request: Any, context: Any) -> retrieval_pb2.SearchResponse:
        raise NotImplementedError

    async def CountDocuments(
        self, request: Any, context: Any
    ) -> retrieval_pb2.CountDocumentsResponse:
        raise NotImplementedError

    async def ListDocuments(
        self, request: Any, context: Any
    ) -> retrieval_pb2.ListDocumentsResponse:
        raise NotImplementedError

    async def GetExcerpt(self, request: Any, context: Any) -> retrieval_pb2.GetExcerptResponse:
        raise NotImplementedError


class FakeRecords:
    def __init__(self, fail: bool = False) -> None:
        self.rows: dict[str, records.Record] = {}
        self.feedback: dict[tuple[str, str, str], records.Feedback] = {}
        self.fail = fail

    async def put(self, rec: records.Record) -> None:
        if self.fail:
            raise records.RecordsUnavailableError("down")
        self.rows[rec.answer_id] = dataclasses.replace(rec, created_at=dt.datetime.now(dt.UTC))

    async def get(self, answer_id: str) -> records.Record | None:
        if self.fail:
            raise records.RecordsUnavailableError("down")
        return self.rows.get(answer_id)

    async def add_feedback(
        self, tenant: str, submitter: str, fb: records.FeedbackInput
    ) -> records.Feedback:
        key = (tenant, submitter, fb.idempotency_key)
        if key not in self.feedback:
            self.feedback[key] = records.Feedback(
                records.new_feedback_id(),
                fb.answer_id,
                fb.rating,
                fb.sentence_index,
                dt.datetime.now(dt.UTC),
                fb.digest,
            )
        got = self.feedback[key]
        if got.digest != fb.digest:
            raise records.FeedbackConflictError
        return got


class Aborted(Exception):
    def __init__(self, code: grpc.StatusCode) -> None:
        self.code = code


class Ctx:
    def invocation_metadata(self) -> tuple[tuple[str, str], ...]:
        return (("x-call-purpose", "answer"), ("x-answer-id", "ans_1"))

    def time_remaining(self) -> float:
        return 2.0

    async def abort(self, code: grpc.StatusCode, details: str) -> None:
        raise Aborted(code)


def _answer() -> answer_pb2.AnswerResponse:
    a = answer_pb2.AnswerResponse(
        answer_id="ans_1",
        trace_id="t1",
        status=answer_pb2.STATUS_ANSWERED,
        answer="甲[S1]。乙[S2]。",
    )
    a.sentences.add(text="甲[S1]。", start=0, end=6, ref_ids=[1], block_id="p")
    a.sentences.add(text="乙[S2]。", start=6, end=12, ref_ids=[2], block_id="p")
    a.blocks.add(block_id="p", sentence_indexes=[0, 1])
    a.citations.add(ref_id=1, chunk_id="c1")
    a.citations.add(ref_id=2, chunk_id="c2")
    return a


def _record(store: FakeRecords) -> None:
    rec = records.build(
        answer_id="ans_1",
        tenant="credit",
        asker="alice",
        actor="",
        purpose="answer",
        question="问题",
        resp=_answer(),
        listed_restricted=False,
    )
    store.rows["ans_1"] = dataclasses.replace(rec, created_at=dt.datetime.now(dt.UTC))


@asynccontextmanager
async def service(
    retrieval: Retrieval,
    store: FakeRecords,
    sub: str = "alice",
    tenant: str = "credit",
    keys: Any = None,
) -> AsyncIterator[server.AnswerService]:
    srv = grpc.aio.server()
    retrieval_pb2_grpc.add_RetrievalServiceServicer_to_server(retrieval, srv)
    port = srv.add_insecure_port("127.0.0.1:0")
    await srv.start()
    token = grpc_server._caller.set(
        grpc_server._Caller(principal_pb2.Principal(sub=sub, tenant=tenant), "raw")
    )
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
            stub = retrieval_pb2_grpc.RetrievalServiceStub(ch)
            none: Any = None
            yield server.AnswerService(stub, none, none, none, none, store, Admission(), keys=keys)
    finally:
        grpc_server._caller.reset(token)
        await srv.stop(None)


def _run[T](coro: Coroutine[Any, Any, T]) -> T:
    return asyncio.run(coro)


def _replay(
    visible: set[str],
    sub: str = "alice",
    tenant: str = "credit",
    auditor: bool = False,
    answer_id: str = "ans_1",
    fail: bool = False,
) -> answer_pb2.GetAnswerResponse:
    async def go() -> answer_pb2.GetAnswerResponse:
        store = FakeRecords()
        _record(store)
        async with service(Retrieval(visible, fail), store, sub, tenant) as svc:
            req = answer_pb2.GetAnswerRequest(answer_id=answer_id, as_auditor=auditor)
            ctx: Any = Ctx()
            return await svc.GetAnswer(req, ctx)

    return _run(go())


def test_asker_replays_with_current_permissions() -> None:
    out = _replay({"c1"})
    assert out.access == answer_pb2.REPLAY_ACCESS_ASKER
    assert out.question == "问题" and out.answer.answer_id == "ans_1"
    assert out.answer.answer == "甲[S1]。" and [c.chunk_id for c in out.answer.citations] == ["c1"]
    assert out.HasField("created_at")


def test_auditor_of_same_tenant() -> None:
    out = _replay({"c1", "c2"}, sub="bob", auditor=True)
    assert out.access == answer_pb2.REPLAY_ACCESS_AUDITOR and len(out.answer.sentences) == 2


@pytest.mark.parametrize(
    ("sub", "tenant", "auditor", "answer_id"),
    [
        ("bob", "credit", False, "ans_1"),  # 别人
        ("bob", "risk", True, "ans_1"),  # 其他租户的审计岗
        ("alice", "risk", False, "ans_1"),  # 同名用户在其他租户
        ("alice", "credit", False, "ans_2"),  # 不存在
        ("alice", "credit", False, "bat_1"),  # 格式不对
    ],
)
def test_others_get_not_found(sub: str, tenant: str, auditor: bool, answer_id: str) -> None:
    with pytest.raises(Aborted) as e:
        _replay({"c1", "c2"}, sub, tenant, auditor, answer_id)
    assert e.value.code == grpc.StatusCode.NOT_FOUND


def test_recheck_failure_is_fail_closed() -> None:
    with pytest.raises(Aborted) as e:
        _replay({"c1", "c2"}, fail=True)
    assert e.value.code == grpc.StatusCode.UNAVAILABLE


def _feedback(
    reqs: list[answer_pb2.CreateFeedbackRequest], sub: str = "alice"
) -> list[answer_pb2.CreateFeedbackResponse | grpc.StatusCode]:
    async def go() -> list[answer_pb2.CreateFeedbackResponse | grpc.StatusCode]:
        store = FakeRecords()
        _record(store)
        out: list[answer_pb2.CreateFeedbackResponse | grpc.StatusCode] = []
        async with service(Retrieval(set()), store, sub) as svc:
            for r in reqs:
                ctx: Any = Ctx()
                try:
                    out.append(await svc.CreateFeedback(r, ctx))
                except Aborted as e:
                    out.append(e.code)
        return out

    return _run(go())


def _fb(
    key: str = "k1", comment: str = "", index: int | None = None
) -> answer_pb2.CreateFeedbackRequest:
    req = answer_pb2.CreateFeedbackRequest(
        answer_id="ans_1",
        rating=answer_pb2.FEEDBACK_RATING_BAD_CITATION,
        idempotency_key=key,
        comment=comment,
    )
    if index is not None:
        req.sentence_index = index
    return req


def test_feedback_idempotent() -> None:
    first, again, other, conflict = _feedback(
        [_fb(index=1), _fb(index=1), _fb("k2"), _fb(comment="不同内容", index=1)]
    )
    assert isinstance(first, answer_pb2.CreateFeedbackResponse)
    assert isinstance(again, answer_pb2.CreateFeedbackResponse)
    assert isinstance(other, answer_pb2.CreateFeedbackResponse)
    assert first.feedback.feedback_id.startswith("fb_")
    assert first.feedback.sentence_index == 1
    assert first.feedback.rating == answer_pb2.FEEDBACK_RATING_BAD_CITATION
    assert again.feedback.feedback_id == first.feedback.feedback_id
    assert other.feedback.feedback_id != first.feedback.feedback_id
    assert conflict == grpc.StatusCode.ALREADY_EXISTS


def test_feedback_only_by_asker_and_valid() -> None:
    assert _feedback([_fb()], sub="bob") == [grpc.StatusCode.NOT_FOUND]
    assert _feedback([_fb(index=2), _fb(key="")]) == [grpc.StatusCode.INVALID_ARGUMENT] * 2


def test_record_failure_does_not_fail_answer() -> None:
    async def go(store: FakeRecords) -> answer_pb2.AnswerResponse:
        async with service(Retrieval(set()), store) as svc:
            call = server._Call(
                "ans_9",
                flow.Request("你好", None, 8),
                "credit",
                Session("conv_1", "o"),
                answer_pb2.INTENT_OUT_OF_SCOPE,
                "answer",
                "alice",
                "",
            )
            ctx: Any = Ctx()
            return await svc._keep(call, flow.Refusal(), "t9", ctx)

    ok, broken = FakeRecords(), FakeRecords(fail=True)
    resp = _run(go(ok))
    assert resp.answer_id == "ans_9" and resp.conversation_id == "conv_1"
    assert ok.rows["ans_9"].asker == "alice" and ok.rows["ans_9"].answer.status == resp.status
    assert _run(go(broken)).status == answer_pb2.STATUS_REFUSED


class FakeKeys:
    key_id = "bambuflow-answers"

    async def data_key(self) -> envelope.DataKey:
        dek = os.urandom(32)
        return envelope.DataKey(dek, dek.hex(), self.key_id)

    async def unwrap(self, wrapped: str) -> bytes:
        return bytes.fromhex(wrapped)


def test_restricted_answer_replays_from_encrypted_snapshot() -> None:
    restricted = _answer()
    for c in restricted.citations:
        c.classification = types_pb2.CLASSIFICATION_RESTRICTED

    async def go(keys: Any) -> answer_pb2.GetAnswerResponse:
        store = FakeRecords()
        rec = records.build(
            answer_id="ans_1",
            tenant="credit",
            asker="alice",
            actor="",
            purpose="answer",
            question="问题",
            resp=restricted,
            listed_restricted=False,
        )
        if keys is not None:
            rec = await records.seal(rec, "问题", restricted, keys)
        store.rows["ans_1"] = dataclasses.replace(rec, created_at=dt.datetime.now(dt.UTC))
        async with service(Retrieval({"c1"}), store, keys=keys) as svc:
            ctx: Any = Ctx()
            return await svc.GetAnswer(answer_pb2.GetAnswerRequest(answer_id="ans_1"), ctx)

    sealed = _run(go(FakeKeys()))
    # 解开快照后照常按现在的权限复核：只看得到 c1 支撑的句子
    assert not sealed.body_withheld and sealed.question == "问题"
    assert sealed.answer.answer == "甲[S1]。" and [c.chunk_id for c in sealed.answer.citations] == [
        "c1"
    ]
    # 没有加密快照：只有标识与版本
    plain = _run(go(None))
    assert plain.body_withheld and plain.answer.answer == "" and plain.question == ""
