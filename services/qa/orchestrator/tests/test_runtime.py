"""服务入口的运行时保护：一次回答的 token 用量之和、过载时排队与拒绝、熔断时只返回证据。

在进程内起一个假的 retrieval；LLM 网关与 NLI 用 httpx.MockTransport 顶替。
"""

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import grpc
import httpx
import pytest
from bambuflow_vocabulary import Vocabularies, Vocabulary

from bambuflow.answer.v1 import answer_pb2
from bambuflow.config.v1 import config_pb2
from bambuflow.identity.v1 import principal_pb2
from bambuflow.retrieval.v1 import retrieval_pb2, retrieval_pb2_grpc
from bambuflow_grounding import KServeNLI
from bambuflow_orchestrator import circuit, records
from bambuflow_orchestrator.admission import Admission
from bambuflow_orchestrator.config import CandidateUnavailableError
from bambuflow_orchestrator.experiments import Experiments
from bambuflow_orchestrator.llm import ChatClient, Router
from bambuflow_orchestrator.plans import Plans
from bambuflow_orchestrator.server import AnswerService
from bambuflow_platform import grpc_server
from bambuflow_platform.grpc_server import EVAL_CONFIG_HEADER

LOAN = "第五条 个人消费贷款单户额度原则上不超过 30 万元。"
HIT = retrieval_pb2.Hit(
    chunk_id="wiki:loan#a", doc_id="loan", doc_version=1, text=LOAN, title="贷款"
)
ANSWER_LINE = json.dumps(
    {
        "block_id": "b1",
        "block_type": "paragraph",
        "text": LOAN + "[S1]",
        "refs": [1],
        "quote": [LOAN],
        "claim_type": "quote",
    },
    ensure_ascii=False,
)
# 网关在最后一个数据块返回的用量：规划与生成各不相同，便于核对求和
PLAN_USAGE = {"prompt_tokens": 100, "completion_tokens": 10}
GENERATE_USAGE = {"prompt_tokens": 200, "completion_tokens": 20}


class Retrieval(retrieval_pb2_grpc.RetrievalServiceServicer):
    def __init__(self) -> None:
        self.searches: list[retrieval_pb2.SearchRequest] = []

    async def Search(self, request: Any, context: Any) -> retrieval_pb2.SearchResponse:
        self.searches.append(request)
        return retrieval_pb2.SearchResponse(
            hits=[HIT], index_version="ix-1", plan_version=request.plan_version
        )

    async def CheckAccess(self, request: Any, context: Any) -> retrieval_pb2.CheckAccessResponse:
        return retrieval_pb2.CheckAccessResponse(
            results=[
                retrieval_pb2.AccessResult(chunk_id=c, allowed=True) for c in request.chunk_ids
            ]
        )

    # 这几个接口在这些用例里用不到
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


def sse(content: str, usage: dict[str, int]) -> bytes:
    chunks = [
        {"model": "chat@1", "choices": [{"index": 0, "delta": {"content": content}}]},
        {"model": "chat@1", "choices": [], "usage": usage},
    ]
    body = "".join(f"data: {json.dumps(c, ensure_ascii=False)}\n\n" for c in chunks)
    return (body + "data: [DONE]\n\n").encode()


class Gateway:
    """LLM 网关与 NLI：生成（带证据区）与规划分别给出不同的输出和用量。"""

    def __init__(self) -> None:
        self.llm_calls = 0

    def __call__(self, req: httpx.Request) -> httpx.Response:
        if req.url.path.endswith("/infer"):
            n = json.loads(req.content)["inputs"][0]["shape"][0]
            outputs = [
                {"name": "support", "data": [0.99] * n},
                {"name": "contradiction", "data": [0.01] * n},
            ]
            body = {"model_name": "nli", "model_version": "t", "outputs": outputs}
            return httpx.Response(200, json=body)
        self.llm_calls += 1
        user = json.loads(req.content)["messages"][-1]["content"]
        if "<evidence" in user:
            return httpx.Response(200, content=sse(ANSWER_LINE, GENERATE_USAGE))
        return httpx.Response(200, content=sse('{"id":"q1","text":"额度"}\n', PLAN_USAGE))


class MemStore:
    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}

    async def get(self, key: str) -> bytes | None:
        return self.data.get(key)

    async def set(self, key: str, value: bytes, ttl: int) -> None:
        self.data[key] = value

    async def delete(self, key: str) -> None:
        self.data.pop(key, None)


class MemRecords:
    def __init__(self) -> None:
        self.rows: dict[str, records.Record] = {}

    async def put(self, rec: records.Record) -> None:
        self.rows[rec.answer_id] = rec

    async def get(self, answer_id: str) -> records.Record | None:
        return self.rows.get(answer_id)

    async def add_feedback(
        self, tenant: str, submitter: str, fb: records.FeedbackInput
    ) -> records.Feedback:
        raise NotImplementedError


class Aborted(Exception):
    def __init__(self, code: grpc.StatusCode) -> None:
        super().__init__(code.name)
        self.code = code


class Inbound:
    def __init__(self, purpose: str = "answer", *extra: tuple[str, str]) -> None:
        self._md = (("x-call-purpose", purpose), ("x-answer-id", "ans_1"), *extra)

    def invocation_metadata(self) -> tuple[tuple[str, str], ...]:
        return self._md

    def time_remaining(self) -> float:
        return 10.0

    async def abort(self, code: grpc.StatusCode, details: str) -> None:
        raise Aborted(code)


@asynccontextmanager
async def service(
    gateway: Gateway,
    admission: Admission | None = None,
    kept: MemRecords | None = None,
    plans: Plans | None = None,
    candidates: Callable[..., Awaitable[config_pb2.Canary]] | None = None,
    retrieval: Retrieval | None = None,
    experiments: Experiments | None = None,
) -> AsyncIterator[tuple[AnswerService, ChatClient]]:
    srv = grpc.aio.server()
    retrieval_pb2_grpc.add_RetrievalServiceServicer_to_server(retrieval or Retrieval(), srv)
    port = srv.add_insecure_port("127.0.0.1:0")
    await srv.start()
    token = grpc_server._caller.set(
        grpc_server._Caller(principal_pb2.Principal(sub="alice", tenant="credit"), "raw")
    )
    try:
        async with (
            grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch,
            httpx.AsyncClient(transport=httpx.MockTransport(gateway)) as http,
        ):
            llm = ChatClient("http://llm", "chat@1", http)
            empty = Vocabulary([])
            yield (
                AnswerService(
                    retrieval_pb2_grpc.RetrievalServiceStub(ch),
                    Router(llm),
                    KServeNLI("http://nli", http),
                    Vocabularies("v", empty, empty, empty),
                    MemStore(),
                    kept or MemRecords(),
                    admission or Admission(),
                    plans=plans,
                    candidates=candidates,
                    experiments=experiments,
                ),
                llm,
            )
    finally:
        grpc_server._caller.reset(token)
        await srv.stop(None)


def request(question: str) -> answer_pb2.AnswerRequest:
    return answer_pb2.AnswerRequest(question=question)


def test_usage_sums_planning_and_generation() -> None:
    kept = MemRecords()

    async def go() -> answer_pb2.AnswerResponse:
        async with service(Gateway(), kept=kept) as (svc, _):
            ctx: Any = Inbound()
            # 多跳问题走 agent：一次规划 + 一次生成
            return await svc.Answer(request("个人消费贷款的额度和期限分别是多少？"), ctx)

    resp = asyncio.run(go())
    assert resp.status == answer_pb2.STATUS_ANSWERED
    assert resp.why.intent == answer_pb2.INTENT_MULTI_HOP
    assert (resp.usage.prompt_tokens, resp.usage.completion_tokens) == (300, 30)
    # 用量只交给 gateway 计配额，不进回答记录：回放不应再计一次
    assert not kept.rows["ans_1"].answer.HasField("usage")


def test_record_carries_rollouts_of_the_asker() -> None:
    """灰度观察按提问人所在的灰度分组：任何路径都记。

    这里是 policy 灰度，候选包只在 retrieval 生效。"""
    kept = MemRecords()
    canary = config_pb2.Canary(release_id="rel_c", tenant="credit", percent=100, object="policy")

    async def go() -> answer_pb2.AnswerResponse:
        async with service(Gateway(), kept=kept, plans=Plans(canaries=[canary])) as (svc, _):
            ctx: Any = Inbound()
            return await svc.Answer(request("个人消费贷款的额度上限是多少？"), ctx)

    resp = asyncio.run(go())
    assert list(resp.why.canary_releases) == ["rel_c"]
    rec = kept.rows["ans_1"]
    assert list(rec.answer.why.canary_releases) == ["rel_c"] and rec.access_denied == 0


def test_eval_candidate_replaces_config_for_the_request() -> None:
    """配置发布的门禁：评测副本按请求头取候选，why 里记候选版本；取不到就拒绝这道题。"""
    version = "cfv_" + "1" * 24
    asked: list[tuple[str, Any]] = []

    async def candidates(v: str, **call: Any) -> config_pb2.Canary:
        asked.append((v, call["metadata"]))
        if v != version:
            raise CandidateUnavailableError("NotFound")
        return config_pb2.Canary(
            object="routing", version_id=v, routing=config_pb2.RoutingConfig(version_id=v)
        )

    async def go(v: str) -> answer_pb2.AnswerResponse:
        async with service(Gateway(), candidates=candidates) as (svc, _):
            ctx: Any = Inbound("batch", (EVAL_CONFIG_HEADER, v))
            return await svc.Answer(request("个人消费贷款的额度上限是多少？"), ctx)

    resp = asyncio.run(go(version))
    assert resp.why.routing_version == version
    # 转发这道题的身份去取（governance 只对评测身份开放），并把候选头带给 retrieval
    assert ("authorization", "Bearer raw") in asked[0][1]
    assert (EVAL_CONFIG_HEADER, version) in asked[0][1]
    with pytest.raises(Aborted) as e:
        asyncio.run(go("cfv_" + "2" * 24))
    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION
    with pytest.raises(Aborted) as e:
        asyncio.run(go("rel_x"))
    assert e.value.code == grpc.StatusCode.INVALID_ARGUMENT


def test_treatment_asks_retrieval_for_the_variant_plan() -> None:
    """实验组替换了 pipeline：检索请求带上变体的版本，why 记实验组与变体版本。"""
    variant = config_pb2.PipelineConfig(tenant="credit", version_id="cfv_" + "3" * 24)
    exp = config_pb2.RunningExperiment(
        exp_id="exp_1", tenant="credit", traffic_percent=100, pipeline=variant
    )
    retrieval = Retrieval()

    async def go() -> answer_pb2.AnswerResponse:
        async with service(Gateway(), retrieval=retrieval, experiments=Experiments([exp])) as (
            svc,
            _,
        ):
            ctx: Any = Inbound()
            return await svc.Answer(request("个人消费贷款的额度上限是多少？"), ctx)

    resp = asyncio.run(go())
    assert [s.plan_version for s in retrieval.searches] == [variant.version_id]
    assert resp.why.experiment_id == "exp_1"
    assert resp.why.experiment_arm == answer_pb2.EXPERIMENT_ARM_TREATMENT
    assert resp.why.pipeline_version == variant.version_id
    assert not [d for d in resp.why.degraded if d.stage == "pipeline_version"]


def test_stream_final_carries_usage() -> None:
    async def go() -> list[answer_pb2.StreamAnswerResponse]:
        async with service(Gateway()) as (svc, _):
            ctx: Any = Inbound()
            req = answer_pb2.StreamAnswerRequest(request=request("个人消费贷款的额度上限是多少？"))
            return [e async for e in svc.StreamAnswer(req, ctx)]

    events = asyncio.run(go())
    final = events[-1].final
    assert final.status == answer_pb2.STATUS_ANSWERED
    assert (final.usage.prompt_tokens, final.usage.completion_tokens) == (200, 20)


def test_open_circuit_returns_evidence_only_without_calling_gateway() -> None:
    gateway = Gateway()

    async def go() -> answer_pb2.AnswerResponse:
        async with service(gateway) as (svc, llm):
            for _ in range(circuit.MIN_CALLS):
                llm.breaker.record(False)
            ctx: Any = Inbound()
            return await svc.Answer(request("个人消费贷款的额度上限是多少？"), ctx)

    resp = asyncio.run(go())
    assert resp.status == answer_pb2.STATUS_EVIDENCE_ONLY
    assert [c.chunk_id for c in resp.citations] == [HIT.chunk_id]
    assert ("generation", "circuit_open") in [(d.stage, d.reason) for d in resp.why.degraded]
    assert gateway.llm_calls == 0
    assert (resp.usage.prompt_tokens, resp.usage.completion_tokens) == (0, 0)


def test_open_circuit_on_agent_path_skips_planning_too() -> None:
    gateway = Gateway()

    async def go() -> answer_pb2.AnswerResponse:
        async with service(gateway) as (svc, llm):
            for _ in range(circuit.MIN_CALLS):
                llm.breaker.record(False)
            ctx: Any = Inbound()
            return await svc.Answer(request("个人消费贷款的额度和期限分别是多少？"), ctx)

    resp = asyncio.run(go())
    degraded = [(d.stage, d.reason) for d in resp.why.degraded]
    # 规划退回原问题照常检索，生成只返回证据
    assert ("plan", "circuit_open") in degraded and ("generation", "circuit_open") in degraded
    assert resp.status == answer_pb2.STATUS_EVIDENCE_ONLY and resp.citations
    assert gateway.llm_calls == 0


@pytest.mark.parametrize("purpose", ["answer", "batch"])
def test_overloaded_request_is_rejected_after_queueing(purpose: str) -> None:
    admission = Admission(1, {"answer": 0.05, "mcp": 0.05, "batch": 0.05})

    async def go() -> None:
        async with service(Gateway(), admission) as (svc, _):
            await admission.acquire("answer")  # 占满唯一的名额
            ctx: Any = Inbound(purpose)
            await svc.Answer(request("个人消费贷款的额度上限是多少？"), ctx)

    with pytest.raises(Aborted) as e:
        asyncio.run(go())
    assert e.value.code == grpc.StatusCode.RESOURCE_EXHAUSTED


def test_slot_is_released_after_answer() -> None:
    admission = Admission(1)

    async def go() -> int:
        async with service(Gateway(), admission) as (svc, _):
            ctx: Any = Inbound()
            await svc.Answer(request("个人消费贷款的额度上限是多少？"), ctx)
            stream = answer_pb2.StreamAnswerRequest(
                request=request("个人消费贷款的额度上限是多少？")
            )
            _ = [e async for e in svc.StreamAnswer(stream, ctx)]
            return admission.active

    assert asyncio.run(go()) == 0


def test_turn_is_remembered_with_summary_and_can_be_deleted() -> None:
    async def go() -> tuple[str, dict[str, Any], int]:
        async with service(Gateway()) as (svc, _):
            ctx: Any = Inbound()
            resp = await svc.Answer(request("个人消费贷款的额度上限是多少？"), ctx)
            await asyncio.gather(*svc._background)
            store: Any = svc._store
            (raw,) = store.data.values()
            saved = json.loads(raw)
            await svc.DeleteConversation(
                answer_pb2.DeleteConversationRequest(conversation_id=resp.conversation_id), ctx
            )
            return resp.conversation_id, saved, len(store.data)

    conv, saved, left = asyncio.run(go())
    assert conv.startswith("conv_") and saved["turn_count"] == 1
    assert saved["recent_questions"] == ["个人消费贷款的额度上限是多少？"]
    # 摘要在回答之后异步生成；上一轮引用带所属文档，下一轮按当前权限取标题
    assert saved["summary"] and not saved["restricted"]
    assert all(set(c) == {"chunk_id", "doc_id"} for c in saved["last_citations"])
    assert left == 0
