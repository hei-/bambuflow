import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import grpc
import httpx
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow.answer.v1 import answer_pb2, answer_pb2_grpc
from bambuflow.retrieval.v1 import retrieval_pb2, retrieval_pb2_grpc
from bambuflow_evaluation import audit
from bambuflow_evaluation.golden import Asker, Question
from bambuflow_evaluation.runner import Runner
from bambuflow_evaluation.signer import Signer
from bambuflow_grounding import KServeNLI

ASKER = Asker("credit_user", "eval-credit-1", "credit", "信贷部", 2, ("g",))


def question(qid: str) -> Question:
    return Question(qid, "single", ASKER, "额度是多少？", "answered", (), (), ())


class Retrieval(retrieval_pb2_grpc.RetrievalServiceServicer):
    async def Search(self, request: Any, context: Any) -> retrieval_pb2.SearchResponse:
        return retrieval_pb2.SearchResponse(hits=[retrieval_pb2.Hit(chunk_id="src_wiki:loan#a1")])

    # 这几个接口在这个用例里用不到（题目没有 must_cite，不调 CheckAccess）
    async def CheckAccess(self, request: Any, context: Any) -> retrieval_pb2.CheckAccessResponse:
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

    async def CheckDocumentAccess(
        self, request: Any, context: Any
    ) -> retrieval_pb2.CheckDocumentAccessResponse:
        raise NotImplementedError


class Orchestrator(answer_pb2_grpc.AnswerServiceServicer):
    def __init__(self) -> None:
        self.calls = 0

    async def Answer(self, request: Any, context: Any) -> answer_pb2.AnswerResponse:
        self.calls += 1
        if self.calls == 2:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "down")
        resp = answer_pb2.AnswerResponse(status=answer_pb2.STATUS_INSUFFICIENT_EVIDENCE)
        resp.why.index_version = "ix-2"
        resp.audit.context_chunk_ids.append("src_wiki:loan#a1")
        resp.audit.shown_chunk_ids.append("src_wiki:loan#a1")
        return resp

    # 这几个接口在这个用例里用不到
    async def StreamAnswer(
        self, request: Any, context: Any
    ) -> AsyncIterator[answer_pb2.StreamAnswerResponse]:
        raise NotImplementedError
        yield answer_pb2.StreamAnswerResponse()  # 让这个函数成为异步生成器

    async def GetAnswer(self, request: Any, context: Any) -> answer_pb2.GetAnswerResponse:
        raise NotImplementedError

    async def CreateFeedback(self, request: Any, context: Any) -> answer_pb2.CreateFeedbackResponse:
        raise NotImplementedError

    async def DeleteConversation(
        self, request: Any, context: Any
    ) -> answer_pb2.DeleteConversationResponse:
        raise NotImplementedError


class Bucket:
    def __init__(self) -> None:
        self.objs: dict[str, bytes] = {}

    def put(self, key: str, body: bytes) -> None:
        self.objs[key] = body


async def replay(bucket: Bucket, questions: list[Question]) -> None:
    srv = grpc.aio.server()
    retrieval_pb2_grpc.add_RetrievalServiceServicer_to_server(Retrieval(), srv)
    answer_pb2_grpc.add_AnswerServiceServicer_to_server(Orchestrator(), srv)
    port = srv.add_insecure_port("127.0.0.1:0")
    await srv.start()
    try:
        async with (
            grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch,
            httpx.AsyncClient() as http,
        ):
            r = Runner(
                retrieval_pb2_grpc.RetrievalServiceStub(ch),
                answer_pb2_grpc.AnswerServiceStub(ch),
                Signer(ec.generate_private_key(ec.SECP256R1())),
                KServeNLI("http://nli", http),
                "ix-2",
                trail=audit.Writer(bucket),
                gate_id="gate_1",
            )
            await r.run(questions)
    finally:
        await srv.stop(None)


def test_replayed_requests_are_audited_with_eval_mark() -> None:
    bucket = Bucket()
    asyncio.run(replay(bucket, [question("q1"), question("q2")]))
    ((key, body),) = bucket.objs.items()
    assert key.startswith(audit.REPLAY_PREFIX) and key.endswith(".jsonl")
    ok, failed = [json.loads(x) for x in body.splitlines()]
    assert ok["eval"] is True and ok["gate_id"] == "gate_1" and ok["endpoint"] == "eval-replay"
    assert (ok["sub"], ok["tenant"], ok["status"]) == ("eval-credit-1", "credit", 200)
    assert (
        ok["shown_chunk_ids"] == ["src_wiki:loan#a1"] and ok["versions"]["index_version"] == "ix-2"
    )
    assert ok["answer_status"] == "insufficient_evidence" and ok["trace_id"] == ok["answer_id"]
    # 调用失败的题也记下
    assert (failed["status"], failed["reason"]) == (500, "unavailable")
