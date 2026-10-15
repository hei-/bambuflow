"""AnswerService 的 gRPC 入口。

校验请求与元数据，读会话、做意图路由，把下游绑定到本次请求（Principal、时间预算），映射错误。
路由（orchestrator 详设 5.3）：超出范围直接拒答；主题汇总、枚举与计数走清单路径；
多跳与比较走 agent 路径；其余（含关系类）走单跳。
每次回答在返回前写回答记录（records.py），回放与反馈以它为准。

运行时保护：每副本的并发上限与按调用目的的排队（admission.py，详设 11.4）；LLM 熔断在 llm.py。
一次回答里全部 LLM 调用的 token 用量之和写进响应（非流式）或 final（流式），由 gateway 计入配额。
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import logging
import re
import time
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Sequence
from contextlib import aclosing, asynccontextmanager
from typing import TYPE_CHECKING, Any, NoReturn, Protocol

import grpc
import httpx
from bambuflow_vocabulary import Vocabularies
from opentelemetry import metrics, trace

from bambuflow.answer.v1 import answer_pb2, answer_pb2_grpc
from bambuflow.common.v1 import types_pb2
from bambuflow.config.v1 import config_pb2
from bambuflow.identity.v1 import principal_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_grounding import Grounder, GroundingUnavailableError, KServeNLI
from bambuflow_orchestrator import (
    agent,
    flow,
    intent,
    listing,
    llm_rerank,
    llmtrace,
    records,
    replay,
    structured,
    summary,
    topics,
    understand,
)
from bambuflow_orchestrator import metrics as bf_metrics
from bambuflow_orchestrator.admission import Admission, Overloaded
from bambuflow_orchestrator.budget import (
    AGENT_SECONDS,
    RELAXED_SINGLE_HOP_SECONDS,
    SINGLE_HOP_SECONDS,
    Deadline,
)
from bambuflow_orchestrator.config import CandidateUnavailableError
from bambuflow_orchestrator.experiments import Experiments
from bambuflow_orchestrator.llm import (
    ChatGeneration,
    LLMUnavailableError,
    RefusedGeneration,
    Router,
)
from bambuflow_orchestrator.plans import Plans
from bambuflow_orchestrator.session import (
    AgentStates,
    Cited,
    Session,
    Sessions,
    Store,
    StoreUnavailableError,
    owner_of,
)
from bambuflow_platform import envelope
from bambuflow_platform.grpc_server import EVAL_CONFIG_HEADER, current_principal, downstream

if TYPE_CHECKING:
    # 异步桩的类型只在生成的 .pyi 里；运行时用 RetrievalServiceStub(aio 通道) 得到的就是它
    from bambuflow.retrieval.v1.retrieval_pb2_grpc import RetrievalServiceAsyncStub

_log = logging.getLogger(__name__)
_meter = metrics.get_meter("bambuflow_orchestrator")
_record_failures = _meter.create_counter(
    "orchestrator.answer_record.write_failures",
    description="回答记录写入失败的次数（回答照常返回，这次回答之后无法回放与反馈）",
)
_trace_failures = _meter.create_counter(
    "orchestrator.llm_trace.write_failures",
    description="LLM 追踪记录写入失败的次数（回答照常返回，这次回答无法在追踪库里排查）",
)
_feedback = _meter.create_counter(
    "orchestrator.feedback", description="收到的反馈，按取值分（技术设计文档 10.2 的在线指标）"
)

MAX_QUESTION_CHARS = 2000
DEFAULT_MAX_CITATIONS = 8
MAX_CITATIONS = 20
PURPOSES = ("answer", "mcp", "batch")
# 各环节的时间上限（秒），实际取它与本次请求剩余时间中较小的一个（orchestrator 详设第 4 节时间预算）
SEARCH_BUDGET = 1.5
GENERATION_BUDGET = 2.5
CHECK_ACCESS_BUDGET = 0.5
# 生成之后至少给校验与收尾留出的时间
RESERVE_AFTER_GENERATION = 0.4
TOP_K = 20
# 内部调用之间也校验格式；格式不对按新会话处理
CONVERSATION_ID = re.compile(r"^conv_[A-Za-z0-9]{1,64}$")
# 配置发布门禁的候选版本（评测副本才接受，governance 详设 4.1 第 ③④ 步）；
# 取候选只在每个版本第一次用到时问 governance，之后走缓存
CANDIDATE_VERSION = re.compile(r"^cfv_[0-9a-f]{24}$")
CANDIDATE_BUDGET = 1.0
# 回放与反馈的 answer_id；格式不对按不存在处理
ANSWER_ID = re.compile(r"^ans_[A-Za-z0-9]{1,64}$")
# 回答记录的写入上限：正常几毫秒，超过就放弃，不拖慢回答
RECORD_WRITE_BUDGET = 0.5
# 回放、反馈读写回答记录的上限（gateway 给回放 2 s、反馈 1 s）
RECORD_READ_BUDGET = 0.5
# 回放复核权限的上限；CheckAccess、CheckDocumentAccess 一次最多 50 个
RECHECK_BUDGET = 1.0
RECHECK_BATCH = 50

# retrieval 的这些错误原样返回给 gateway；其余（含 UNAVAILABLE）一律按不可用处理，不尝试无检索生成
PASS_THROUGH = {
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.UNAUTHENTICATED,
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.DEADLINE_EXCEEDED,
}


def _metadata(context: grpc.aio.ServicerContext[Any, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in context.invocation_metadata() or ():
        if isinstance(v, str):
            out[k] = v
    return out


# type 语句惰性求值：运行时的 ServicerContext 不能带类型参数
type Ctx = grpc.aio.ServicerContext[Any, Any]


def _purpose(ctx: Ctx) -> str:
    """调用 retrieval 时沿用入口的调用目的（answer、mcp、batch，已在 _request 校验），
    retrieval 据此审计并分级准入：批量过载时最先被拒（retrieval 详设 9.2）。"""
    return _metadata(ctx)["x-call-purpose"]


class _Search:
    def __init__(
        self,
        stub: RetrievalServiceAsyncStub,
        ctx: Ctx,
        deadline: Deadline,
        intent: retrieval_pb2.SearchIntent = retrieval_pb2.SEARCH_INTENT_UNSPECIFIED,
        plan_version: str = "",
    ) -> None:
        self._stub, self._ctx, self._deadline, self._intent = stub, ctx, deadline, intent
        # 实验组替换了 pipeline 时要 retrieval 用的检索计划（plans.Settings.plan_version）
        self._plan_version = plan_version

    async def search(
        self, question: str, filters: types_pb2.Filters | None, variants: Sequence[str] = ()
    ) -> retrieval_pb2.SearchResponse:
        kw = downstream(self._ctx, budget=self._deadline.cap(SEARCH_BUDGET))
        kw["metadata"] += (("x-call-purpose", _purpose(self._ctx)),)
        # 回答生成总要精排（orchestrator 详设第 5 节第 4 步）：
        # 不依赖按调用目的的默认值，mcp 的默认是不精排
        # 同义扩展的说法作为后续子查询（retrieval 按子查询分别召回后合并，最多 4 个）
        texts = [question, *variants][:4]
        req = retrieval_pb2.SearchRequest(
            queries=[retrieval_pb2.SubQuery(id=f"q{i}", text=t) for i, t in enumerate(texts, 1)],
            filters=filters,
            top_k=TOP_K,
            intent=self._intent,
            rerank=True,
            # 各路分数写进回答的 why（retrieval 详设 7.5）
            include_score_details=True,
            plan_version=self._plan_version,
        )
        resp: retrieval_pb2.SearchResponse = await self._stub.Search(req, **kw)
        return resp


class _AgentSearch:
    """agent 路径的 doc_search：调用目的 agent_round，带轮次（orchestrator 详设 3.4、6.3）。"""

    def __init__(self, stub: RetrievalServiceAsyncStub, ctx: Ctx, plan_version: str = "") -> None:
        self._stub, self._ctx, self._plan_version = stub, ctx, plan_version

    async def search(
        self, query: str, filters: types_pb2.Filters | None, round_no: int, timeout: float
    ) -> retrieval_pb2.SearchResponse:
        kw = downstream(self._ctx, budget=timeout)
        kw["metadata"] += (("x-call-purpose", "agent_round"), ("x-agent-round", str(round_no)))
        req = retrieval_pb2.SearchRequest(
            queries=[retrieval_pb2.SubQuery(id="q1", text=query)],
            filters=filters,
            top_k=agent.TOP_K,
            include_score_details=True,
            plan_version=self._plan_version,
        )
        resp: retrieval_pb2.SearchResponse = await self._stub.Search(req, **kw)
        return resp


class _Access:
    """grounding 的权限复核：以调用者本人的 Principal 调 CheckAccess。

    local 是本次请求里 sql_query 以调用者身份查出的证据（不在索引里，CheckAccess 不认识）：
    它们的权限已由数据库按调用者的角色判定，直接放行；其余一律经 retrieval 复核。
    复核拦下的块记进 denied（一次回答共用），回答记录据此记越权拦截数
    （灰度观察，governance 详设 4.1）。"""

    def __init__(
        self,
        stub: RetrievalServiceAsyncStub,
        ctx: Ctx,
        deadline: Deadline,
        denied: set[str],
        local: set[str] | None = None,
    ) -> None:
        self._stub, self._ctx, self._deadline, self._denied = stub, ctx, deadline, denied
        self._local = local if local is not None else set()

    async def allowed(self, chunk_ids: Sequence[str]) -> dict[str, bool]:
        out = {c: True for c in chunk_ids if c in self._local}
        rest = [c for c in chunk_ids if c not in self._local]
        if rest:
            out.update(await self._check(rest))
        return out

    async def _check(self, chunk_ids: Sequence[str]) -> dict[str, bool]:
        kw = downstream(self._ctx, budget=self._deadline.cap(CHECK_ACCESS_BUDGET))
        kw["metadata"] += (("x-call-purpose", _purpose(self._ctx)),)
        try:
            resp = await self._stub.CheckAccess(
                retrieval_pb2.CheckAccessRequest(chunk_ids=chunk_ids), **kw
            )
        except grpc.aio.AioRpcError as e:
            raise GroundingUnavailableError(f"CheckAccess: {e.code().name}") from e
        got = {r.chunk_id: r.allowed for r in resp.results}
        # 不在结果里的按不可见处理（Grounder 同样如此）
        self._denied.update(c for c in chunk_ids if not got.get(c, False))
        return got


class _Sql:
    """agent 的 sql_query 工具：以调用者的权限 token 执行。

    查出的证据登记到 local，供权限复核放行。"""

    def __init__(
        self,
        catalog: structured.Catalog,
        service: structured.QueryService,
        principal: principal_pb2.Principal,
        local: set[str],
    ) -> None:
        self.catalog, self._service, self._local = catalog, service, local
        self._tokens = structured.tokens_of(principal)

    async def query(self, args: dict[str, Any], timeout: float) -> retrieval_pb2.Hit:
        q = structured.parse(args, self.catalog)
        hit = structured.evidence(q, await self._service.run(q, self._tokens, timeout))
        self._local.add(hit.chunk_id)
        return hit


class _Tokens:
    """一次回答里的全部 LLM 调用。每次调用的用量在读取结束后才确定，所以先记下调用，用时再求和。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ChatGeneration]] = []

    def total(self) -> answer_pb2.TokenUsage:
        out = answer_pb2.TokenUsage()
        for _, g in self.calls:
            out.prompt_tokens += g.usage.prompt_tokens
            out.completion_tokens += g.usage.completion_tokens
        return out

    def log(self, tenant: str, purpose: str) -> None:
        """按租户、调用目的和用途（规划、生成）记账（详设 8.5、13.1）。

        客户端断开或调用出错时用量无法随响应交给 gateway，至少在这里留下记录。"""
        if not self.calls:
            return
        by_use: dict[str, list[int]] = {}
        for use, g in self.calls:
            u = by_use.setdefault(use, [0, 0])
            u[0] += g.usage.prompt_tokens
            u[1] += g.usage.completion_tokens
        _log.info(
            "LLM 用量",
            extra={
                "tenant": tenant,
                "purpose": purpose,
                "model": self.calls[-1][1].model,
                "usage": by_use,
                "estimated": any(g.usage.estimated for _, g in self.calls),
            },
        )


class _LLM:
    def __init__(self, router: Router, deadline: Deadline, tokens: _Tokens) -> None:
        self._router, self._deadline, self._tokens = router, deadline, tokens

    def generate(
        self,
        system: str,
        user: str,
        timeout: float | None = None,
        level: int = types_pb2.CLASSIFICATION_INTERNAL,
        model: str = "",
    ) -> ChatGeneration | RefusedGeneration:
        """timeout 由调用方给出（agent 的规划）时照用；
        不给时是回答生成，按生成预算并给校验留出时间。
        level 是这次输入里的最高密级，按它选允许的模型（总体设计 8.6）；model 是 routing 指定的
        生成模型（空为默认模型），不在部署允许的模型里时同样不调用。"""
        use = "plan" if timeout is not None else "generate"
        client = self._router.choose(level, model)
        if client is None:
            _log.warning(
                "没有允许处理该密级内容的模型，不调用模型", extra={"level": level, "model": model}
            )
            refused = RefusedGeneration(level)
            llmtrace.note_call(use, level, refused)
            return refused
        remaining = self._deadline.remaining()
        if timeout is None:
            timeout = min(GENERATION_BUDGET, remaining - RESERVE_AFTER_GENERATION)
        g = client.generate(system, user, max(0.1, min(timeout, remaining)))
        self._tokens.calls.append((use, g))
        llmtrace.note_call(use, level, g)
        return g


class _Rewriter:
    """查询改写的模型调用（understand.Rewriter）：读完全部输出；用量随 _LLM 计入这次回答。"""

    def __init__(self, llm: _LLM, model: str) -> None:
        self._llm, self.model = llm, model

    async def complete(self, system: str, user: str, timeout: float) -> str:
        return "\n".join([line async for line in self._llm.generate(system, user, timeout)])


# 计数与列表各自的时间预算（K2 详设第 5 节：各约 200 ms，留出余量）
CATALOG_BUDGET = 0.5
# 问题指代上一轮的文档时，最多取几份文档的标题给查询改写（详设 10.1）
MAX_REFERENCES = 3
# 枚举与计数接口这些错误时退回普通检索流程（K2 详设第 9 节）；权限、参数错误照常返回
_FALLBACK = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
}


class _Catalog:
    def __init__(self, stub: RetrievalServiceAsyncStub, ctx: Ctx, deadline: Deadline) -> None:
        self._stub, self._ctx, self._deadline = stub, ctx, deadline

    async def count(
        self, filters: types_pb2.Filters, group_by: retrieval_pb2.CountGroupBy
    ) -> retrieval_pb2.CountDocumentsResponse:
        kw = downstream(self._ctx, budget=self._deadline.cap(CATALOG_BUDGET))
        kw["metadata"] += (("x-call-purpose", _purpose(self._ctx)),)
        req = retrieval_pb2.CountDocumentsRequest(filters=filters, group_by=group_by)
        resp: retrieval_pb2.CountDocumentsResponse = await self._stub.CountDocuments(req, **kw)
        return resp

    async def list(
        self, filters: types_pb2.Filters, page_size: int, page_token: str = ""
    ) -> retrieval_pb2.ListDocumentsResponse:
        kw = downstream(self._ctx, budget=self._deadline.cap(CATALOG_BUDGET))
        kw["metadata"] += (("x-call-purpose", _purpose(self._ctx)),)
        req = retrieval_pb2.ListDocumentsRequest(
            filters=filters,
            page_size=page_size,
            page_token=page_token,
            order_by=retrieval_pb2.DOCUMENT_ORDER_EFFECTIVE_DATE_DESC,
        )
        resp: retrieval_pb2.ListDocumentsResponse = await self._stub.ListDocuments(req, **kw)
        return resp


class _Excerpts:
    """主题汇总取摘要块：以调用者本人的身份调 GetExcerpt，先判权限再读正文；
    无权与不存在都返回 None。"""

    def __init__(self, stub: RetrievalServiceAsyncStub, ctx: Ctx, deadline: Deadline) -> None:
        self._stub, self._ctx, self._deadline = stub, ctx, deadline

    async def excerpt(self, doc_id: str, chunk_id: str) -> retrieval_pb2.GetExcerptResponse | None:
        kw = downstream(self._ctx, budget=self._deadline.cap(CATALOG_BUDGET))
        kw["metadata"] += (("x-call-purpose", _purpose(self._ctx)),)
        try:
            resp: retrieval_pb2.GetExcerptResponse = await self._stub.GetExcerpt(
                retrieval_pb2.GetExcerptRequest(doc_id=doc_id, chunk_id=chunk_id), **kw
            )
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise
        return resp


class _AgentState:
    """agent 中间状态绑定到本次回答（租户 + answer_id）。"""

    def __init__(self, states: AgentStates, tenant: str, answer_id: str) -> None:
        self._states, self._tenant, self._answer_id = states, tenant, answer_id

    async def save(self, state: dict[str, Any]) -> None:
        await self._states.save(self._tenant, self._answer_id, state)

    async def drop(self) -> None:
        await self._states.drop(self._tenant, self._answer_id)


class _Run(Protocol):
    """一条处理路径的一次回答：events() 产生流式事件，结束后 response() 给出完整契约。"""

    why: answer_pb2.Why

    def events(self) -> AsyncGenerator[answer_pb2.StreamAnswerResponse, None]: ...

    def response(self) -> answer_pb2.AnswerResponse: ...


class _Computed(Protocol):
    why: answer_pb2.Why
    restricted: bool

    def response(self) -> answer_pb2.AnswerResponse: ...


def _context_level(run: _Run, resp: answer_pb2.AnswerResponse) -> int:
    """本轮上下文的最高密级（会话按它决定能否存摘要，详设 10.1）：
    单跳与 agent 看送进模型的全部证据，清单路径看列出的文档与引用。"""
    if isinstance(run, _Replay) and run.restricted:
        return types_pb2.CLASSIFICATION_RESTRICTED
    levels = [c.classification for c in resp.citations]
    if isinstance(run, flow.Answer):
        levels += [h.classification for _, h in run.evidence]
    return max(levels, default=types_pb2.CLASSIFICATION_INTERNAL)


class _Replay:
    """已经全部算完的回答（清单路径：枚举、计数、主题汇总）：按原顺序重放事件。"""

    def __init__(self, run: _Computed, events: list[answer_pb2.StreamAnswerResponse]) -> None:
        self.why = run.why
        self.restricted = run.restricted
        self._run, self._events = run, events

    async def events(self) -> AsyncGenerator[answer_pb2.StreamAnswerResponse, None]:
        for e in self._events:
            yield e

    def response(self) -> answer_pb2.AnswerResponse:
        return self._run.response()


@dataclasses.dataclass
class _Call:
    answer_id: str
    req: flow.Request
    tenant: str
    session: Session
    intent: answer_pb2.Intent
    purpose: str
    asker: str
    actor: str
    tokens: _Tokens = dataclasses.field(default_factory=_Tokens)
    # 收到请求的时刻，回答记录据此记耗时（A/B 护栏 p95）
    started: float = dataclasses.field(default_factory=time.monotonic)
    # 返回前复核拦下的块（灰度观察的越权拦截数）
    denied: set[str] = dataclasses.field(default_factory=set)
    # 这次回答的模型调用与校验结果（LLM 追踪库）
    trace: llmtrace.Collector = dataclasses.field(default_factory=llmtrace.Collector)


class AnswerService(answer_pb2_grpc.AnswerServiceServicer):
    def __init__(
        self,
        retrieval: RetrievalServiceAsyncStub,
        llm: Router,
        nli: KServeNLI,
        vocab: Vocabularies,
        store: Store,
        answer_records: records.Records,
        admission: Admission,
        experiments: Experiments | None = None,
        sql: tuple[structured.Catalog, structured.QueryService] | None = None,
        keys: envelope.AsyncKeyService | None = None,
        plans: Plans | None = None,
        candidates: Callable[..., Awaitable[config_pb2.Canary]] | None = None,
        traces: llmtrace.Traces | None = None,
    ) -> None:
        self._retrieval, self._llm, self._nli, self._vocab = retrieval, llm, nli, vocab
        # 回答记录的信封加密（问题原文、受限级回答快照）；没有配置时不加密另存，
        # 受限级回答不能回放正文
        self._keys = keys
        self._records = answer_records
        # LLM 追踪库；没有配置密钥服务（_keys）时不写
        self._traces = traces
        self._admission = admission
        self._experiments = experiments if experiments is not None else Experiments()
        # 租户 pipeline 与全局 routing（随配置快照更新）；没有时全部用默认值
        self._plans = plans if plans is not None else Plans()
        # 评测副本按版本取候选配置（config.ConfigClient.candidate）；线上实例为 None
        self._candidates = candidates
        # 数据平台的只读查询服务（sql_query）；没有配置时结构化问题按普通问题处理
        self._sql = sql
        self._sessions, self._agent_states = Sessions(store), AgentStates(store)
        self._store = store
        # 会话写入在回答返回后进行；保留任务的引用，避免执行完之前被回收
        self._background: set[asyncio.Task[None]] = set()

    def set_vocab(self, vocab: Vocabularies) -> None:
        """换成新的词表快照（词条新增、停用后随配置快照下发，governance 详设 6.1）；
        进行中的请求不受影响。"""
        self._vocab = vocab

    def _listing(self, req: flow.Request) -> tuple[listing.Conditions, types_pb2.Filters] | None:
        cond = listing.understand(req.question, self._vocab, dt.date.today())
        return (cond, listing.merge(req.filters, cond.filters)) if cond else None

    async def _request(
        self, request: answer_pb2.AnswerRequest, context: Ctx
    ) -> tuple[str, flow.Request]:
        """校验元数据与参数，不通过时 abort。一元与流式的规则相同。"""
        md = _metadata(context)
        answer_id = md.get("x-answer-id", "")
        if md.get("x-call-purpose") not in PURPOSES or not answer_id.startswith("ans_"):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "需要 x-call-purpose 与 ans_ 前缀的 x-answer-id"
            )
        question = request.question.strip()
        max_citations = request.max_citations or DEFAULT_MAX_CITATIONS
        if (
            not question
            or len(request.question) > MAX_QUESTION_CHARS
            or not 1 <= max_citations <= MAX_CITATIONS
        ):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "问题为空或过长，或 max_citations 越界"
            )
        return answer_id, flow.Request(
            question=question,
            filters=request.filters if request.HasField("filters") else None,
            max_citations=max_citations,
        )

    @asynccontextmanager
    async def _admitted(self, context: Ctx) -> AsyncIterator[None]:
        """占用一个并发名额直到回答结束（流式为整个流）；排队超时返回 RESOURCE_EXHAUSTED。"""
        purpose = _purpose(context)
        try:
            await self._admission.acquire(purpose)
        except Overloaded:
            _log.warning("并发已满，排队超时", extra={"purpose": purpose})
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "orchestrator 繁忙，请稍后重试")
        try:
            yield
        finally:
            self._admission.release()

    async def _prepare(
        self, request: answer_pb2.AnswerRequest, context: Ctx, answer_id: str, req: flow.Request
    ) -> _Call:
        principal = current_principal()
        if principal is None:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "缺少 Principal")
        conv = request.conversation_id if CONVERSATION_ID.match(request.conversation_id) else ""
        session = await self._sessions.open(
            principal.tenant, owner_of(principal.tenant, principal.sub), conv
        )
        candidate = await self._candidate(context)
        # 分组按提问人（user_id）算，代用户调用的业务系统与 agent 不影响分组（详设 12.3）
        exp = self._experiments.assignment(principal.tenant, principal.sub)
        variant = None
        if exp is not None and exp.variant is not None:
            variant = self._plans.settings(principal.tenant, principal.sub, candidate, exp.variant)
        req = dataclasses.replace(
            req,
            experiment=exp,
            settings=self._plans.settings(principal.tenant, principal.sub, candidate),
            variant=variant,
            summary=session.summary,
        )
        if session.degraded:
            req = dataclasses.replace(
                req,
                degraded=(
                    *req.degraded,
                    types_pb2.Degradation(stage="session", reason="unavailable"),
                ),
            )
        call = _Call(
            answer_id,
            req,
            principal.tenant,
            session,
            intent.classify(req.question),
            _purpose(context),
            principal.sub,
            principal.actor,
        )
        llmtrace.bind(call.trace)
        return call

    async def _candidate(self, context: Ctx) -> config_pb2.Canary | None:
        """配置发布的门禁指定的候选（governance 详设 4.1 第 ③④ 步）。

        线上实例收到这个头已被 AuthInterceptor 拒绝。

        取不到就拒绝这道题：不能拿生效的配置冒充候选去评（fail-closed）。"""
        version = _metadata(context).get(EVAL_CONFIG_HEADER, "")
        if not version:
            return None
        if self._candidates is None or not CANDIDATE_VERSION.match(version):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "候选配置版本不合法")
        try:
            return await self._candidates(version, **downstream(context, budget=CANDIDATE_BUDGET))
        except CandidateUnavailableError as e:
            _log.warning("取候选配置失败", extra={"version_id": version, "err": str(e)})
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "候选配置不可用")

    def _deadline(self, context: Ctx, seconds: float) -> Deadline:
        remaining = context.time_remaining()
        return Deadline(seconds if remaining is None else min(remaining, seconds))

    def _single_hop(
        self,
        req: flow.Request,
        context: Ctx,
        call: _Call,
        kind: answer_pb2.Intent = answer_pb2.INTENT_SINGLE_HOP,
    ) -> flow.Answer:
        # 取证据一侧的开关：实验组从一开始就用变体（flow.Answer）
        pre = req.variant or req.settings
        relaxed = pre.single_hop_llm_rerank
        deadline = self._deadline(
            context, RELAXED_SINGLE_HOP_SECONDS if relaxed else SINGLE_HOP_SECONDS
        )
        llm = _LLM(self._llm, deadline, call.tokens)

        async def rerank(
            question: str, hits: Sequence[retrieval_pb2.Hit]
        ) -> tuple[list[retrieval_pb2.Hit], str, str]:
            r = await llm_rerank.rerank(llm, question, (), hits, deadline.remaining)
            return r.hits, r.version, r.degraded

        # 查询理解（详设 5.1、5.2）：追问改写用会话里最近几轮问题，结果缓存在会话所在的 Redis
        understander = understand.Understander(
            self._vocab,
            dt.date.today(),
            call.session.recent_questions,
            _Rewriter(llm, self._llm.model),
            self._store,
            call.tenant,
            deadline.remaining,
            call.session.summary,
            lambda: self._titles(call, context, deadline),
        )
        # 关系类问题也走单跳，只是让 retrieval 提高图召回的权重（详设 5.3）
        hint = (
            retrieval_pb2.SEARCH_INTENT_RELATION
            if kind == answer_pb2.INTENT_RELATION
            else retrieval_pb2.SEARCH_INTENT_UNSPECIFIED
        )
        return flow.Answer(
            req,
            _Search(self._retrieval, context, deadline, hint, pre.plan_version),
            llm,
            Grounder(self._nli, _Access(self._retrieval, context, deadline, call.denied)),
            understander,
            # 单跳 LLM 重排只对书面同意放宽 SLO 的租户开放（pipeline，技术设计文档 6.7）
            rerank if relaxed else None,
        )

    def _structured(self, call: _Call) -> bool:
        """租户开放了 sql_query，且问题在问白名单视图的业务数据（总体设计 6.2）。"""
        return (
            self._sql is not None
            and self._sql[0].enabled(call.tenant)
            and self._sql[0].matches(call.req.question)
        )

    def _agent(
        self, call: _Call, context: Ctx, structured_query: bool = False
    ) -> agent.AgentAnswer:
        deadline = self._deadline(context, AGENT_SECONDS)
        llm = _LLM(self._llm, deadline, call.tokens)
        local: set[str] = set()
        sql: _Sql | None = None
        principal = current_principal()
        if structured_query and self._sql is not None and principal is not None:
            sql = _Sql(*self._sql, principal, local)
        pre = call.req.variant or call.req.settings
        run = agent.Agent(
            call.req.question,
            call.req.filters,
            call.session.recent_questions,
            _AgentSearch(self._retrieval, context, pre.plan_version),
            llm,
            deadline,
            _AgentState(self._agent_states, call.tenant, call.answer_id),
            sql=sql,
            summary=call.session.summary,
            max_rounds=pre.agent_max_rounds,
            max_sub_questions=pre.agent_max_sub_questions,
        )
        grounder = Grounder(
            self._nli, _Access(self._retrieval, context, deadline, call.denied, local)
        )
        return agent.AgentAnswer(call.req, run, llm, grounder)

    async def _select(self, call: _Call, context: Ctx) -> _Run:
        """选择处理路径，并把实验分组写进 why（详设 12.3）。"""
        run = await self._route(call, context)
        # 所有路径都记提问人所在的灰度：灰度观察按它分组，不受处理路径影响（governance 详设 4.1）
        run.why.canary_releases.extend(call.req.settings.canary_releases)
        exp = call.req.experiment
        if exp is not None:
            run.why.experiment_id = exp.exp_id
            # 清单路径已经算完：列出的文档里有受限级时同样退回基线、不计入分析
            restricted = isinstance(run, _Replay) and run.restricted
            if run.why.experiment_arm != answer_pb2.EXPERIMENT_ARM_EXCLUDED:
                run.why.experiment_arm = (
                    answer_pb2.EXPERIMENT_ARM_EXCLUDED if restricted and exp.treatment else exp.arm
                )
        return run

    async def _route(self, call: _Call, context: Ctx) -> _Run:
        """按意图选择处理路径。清单路径在这里就算完：失败时还没有推送任何内容，可以整体退回单跳。"""
        req, kind = call.req, call.intent
        run: _Run
        if kind == answer_pb2.INTENT_OUT_OF_SCOPE:
            run = flow.Refusal()
            run.why.degraded.extend(req.degraded)
        elif self._structured(call):
            # 先于清单路径判断：“贷款余额合计多少”问的是业务数据，不是文档数量
            kind = answer_pb2.INTENT_STRUCTURED
            run = self._agent(call, context, structured_query=True)
        elif summary := topics.understand(req.question, self._vocab, dt.date.today()):
            # 主题汇总与多跳同一档预算（K2 详设第 9 节：10 s）
            kind = answer_pb2.INTENT_LISTING
            deadline = self._deadline(context, AGENT_SECONDS)
            ts = topics.TopicSummary(
                req.question,
                summary,
                listing.merge(req.filters, summary.filters),
                _Catalog(self._retrieval, context, deadline),
                _Excerpts(self._retrieval, context, deadline),
                _LLM(self._llm, deadline, call.tokens),
                Grounder(self._nli, _Access(self._retrieval, context, deadline, call.denied)),
            )
            ts.why.intent = kind
            ts.why.degraded.extend(req.degraded)
            try:
                ready = [e async for e in ts.events()]
            except grpc.aio.AioRpcError as e:
                run = self._single_hop(await _fall_back(e, req, context), context, call)
            else:
                return _Replay(ts, ready)
        elif found := self._listing(req):
            kind = answer_pb2.INTENT_LISTING
            deadline = self._deadline(context, SINGLE_HOP_SECONDS)
            lst = listing.Listing(*found, _Catalog(self._retrieval, context, deadline))
            lst.why.intent = kind
            lst.why.degraded.extend(req.degraded)
            try:
                ready = [e async for e in lst.events()]
            except grpc.aio.AioRpcError as e:
                run = self._single_hop(await _fall_back(e, req, context), context, call)
            else:
                return _Replay(lst, ready)
        elif kind == answer_pb2.INTENT_MULTI_HOP:
            run = self._agent(call, context)
        else:
            run = self._single_hop(req, context, call, kind)
        run.why.intent = kind
        return run

    def _remember(self, call: _Call, resp: answer_pb2.AnswerResponse, run: _Run) -> None:
        """回答结束后异步写会话并更新摘要，不占用响应时间（详设 10.2）。
        会话里存改写后的问题（详设 10.1），没有改写时存原问题。"""
        cited = [Cited(c.chunk_id, c.doc_id) for c in resp.citations]
        rewritten = run.rewritten if isinstance(run, flow.Answer) else ""
        task = asyncio.create_task(
            self._remember_turn(
                call,
                rewritten or call.req.question,
                cited,
                _context_level(run, resp),
                # 只在回答了的轮次更新摘要：拒答的问题（超出范围、疑似注入）不该送进模型，
                # 没有证据或生成不可用的轮次也不为摘要单独调一次模型
                summarize=resp.status == answer_pb2.STATUS_ANSWERED,
            )
        )
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _remember_turn(
        self, call: _Call, question: str, cited: list[Cited], level: int, summarize: bool
    ) -> None:
        turn = await self._sessions.record(call.tenant, call.session, question, cited, level)
        # 受限级会话不存摘要（详设 10.1）；摘要的输入只有用户的问题，按本轮上下文的密级选模型
        restricted = call.session.restricted or level >= types_pb2.CLASSIFICATION_RESTRICTED
        if turn is None or restricted or not summarize:
            return
        client = self._llm.choose(level)
        if client is None:
            return
        try:
            text = await asyncio.wait_for(
                summary.update(client, call.session.summary, question), summary.SECONDS
            )
        except (LLMUnavailableError, TimeoutError) as e:
            # 摘要更新失败不影响回答，下一轮退回只用最近的问题（详设 10.2）
            _log.warning("会话摘要更新失败", extra={"err": type(e).__name__})
            return
        await self._sessions.update_summary(
            call.tenant, call.session.conversation_id, call.session.owner, turn, text
        )

    async def _titles(self, call: _Call, context: Ctx, deadline: Deadline) -> list[str]:
        """上一轮所引文档的标题（最多 3 份），以调用者当前的权限经 GetExcerpt 取：
        这期间被撤权的文档不给（详设 10.1）。取不到只是不提供，不影响回答。"""
        docs: dict[str, Cited] = {}
        for c in call.session.last_citations:
            if c.doc_id not in docs and len(docs) < MAX_REFERENCES:
                docs[c.doc_id] = c
        excerpts = _Excerpts(self._retrieval, context, deadline)
        got = await asyncio.gather(
            *(excerpts.excerpt(c.doc_id, c.chunk_id) for c in docs.values()),
            return_exceptions=True,
        )
        titles: list[str] = []
        for r in got:
            if isinstance(r, retrieval_pb2.GetExcerptResponse) and r.title:
                titles.append(r.title)
            elif isinstance(r, Exception):
                _log.warning("取上一轮引用的标题失败", extra={"err": type(r).__name__})
        return titles

    async def _keep(
        self, call: _Call, run: _Run, trace_id: str, context: Ctx
    ) -> answer_pb2.AnswerResponse:
        """取完整回答并写回答记录，返回带 answer_id 等标识的回答。

        在返回之前写，而不是像会话那样放到后台：调用方拿到回答（流式收到 final）后
        可能马上回放或反馈，后台写入会让这时查不到。写入失败或超时只记日志与指标，回答照常返回：
        记录只服务于事后的回放与反馈，为它拒绝一个已经校验通过的回答得不偿失；
        审计（三类块）由 gateway 另行写入，不受影响。
        """
        resp = run.response()
        resp.answer_id, resp.trace_id = call.answer_id, trace_id
        resp.conversation_id = call.session.conversation_id
        bf_metrics.observe(
            call.tenant,
            call.purpose,
            resp,
            call.tokens.total(),
            time.monotonic() - call.started,
            len(call.denied),
        )
        rec = records.build(
            answer_id=call.answer_id,
            tenant=call.tenant,
            asker=call.asker,
            actor=call.actor,
            purpose=call.purpose,
            question=call.req.question,
            resp=resp,
            listed_restricted=isinstance(run, _Replay) and run.restricted,
            latency_ms=int((time.monotonic() - call.started) * 1000),
            usage=call.tokens.total(),
            access_denied=len(call.denied),
        )
        if self._keys is not None:
            try:
                rec = await asyncio.wait_for(
                    records.seal(rec, call.req.question, resp, self._keys),
                    _cap(context, RECORD_WRITE_BUDGET),
                )
            except (httpx.HTTPError, KeyError, ValueError, TimeoutError) as e:
                # 密钥服务不可用：不另存原文与快照（宁可不存），记录照常写入
                _record_failures.add(1, {"reason": "seal_" + type(e).__name__})
                _log.warning(
                    "回答记录加密失败，不另存原文",
                    extra={"answer_id": call.answer_id, "err": type(e).__name__},
                )
        try:
            await asyncio.wait_for(self._records.put(rec), _cap(context, RECORD_WRITE_BUDGET))
        except (records.RecordsUnavailableError, TimeoutError) as e:
            _record_failures.add(1, {"reason": type(e).__name__})
            _log.warning(
                "回答记录写入失败，回答照常返回",
                extra={"answer_id": call.answer_id, "err": type(e).__name__},
            )
        # LLM 追踪库：受限级（引用或任何一次模型输入里有）不写；放到后台，不拖慢回答
        traces, keys = self._traces, self._keys
        if (
            traces is not None
            and keys is not None
            and not rec.body_withheld
            and not call.trace.restricted()
        ):
            task = asyncio.create_task(self._write_trace(traces, keys, call, trace_id, resp))
            self._background.add(task)
            task.add_done_callback(self._background.discard)
        return resp

    async def _write_trace(
        self,
        traces: llmtrace.Traces,
        keys: envelope.AsyncKeyService,
        call: _Call,
        trace_id: str,
        resp: answer_pb2.AnswerResponse,
    ) -> None:
        t = llmtrace.build(trace_id, call.answer_id, call.tenant, call.trace)
        try:
            sealed = await asyncio.wait_for(
                llmtrace.seal(t, llmtrace.docs_of(resp), keys), RECORD_WRITE_BUDGET
            )
            await asyncio.wait_for(traces.put(sealed), RECORD_WRITE_BUDGET)
        except (
            llmtrace.TracesUnavailableError,
            httpx.HTTPError,
            KeyError,
            ValueError,
            TimeoutError,
        ) as e:
            # 追踪记录只服务于排查，写不进去不影响回答
            _trace_failures.add(1, {"reason": type(e).__name__})
            _log.warning(
                "LLM 追踪记录写入失败", extra={"trace_id": trace_id, "err": type(e).__name__}
            )

    async def Answer(
        self, request: answer_pb2.AnswerRequest, context: Ctx
    ) -> answer_pb2.AnswerResponse:
        answer_id, req = await self._request(request, context)
        async with self._admitted(context):
            call = await self._prepare(request, context, answer_id, req)
            try:
                run = await self._select(call, context)
                async for _ in run.events():
                    pass
            except grpc.aio.AioRpcError as e:
                await _abort_search(e, context)
            finally:
                call.tokens.log(call.tenant, call.purpose)
            resp = await self._keep(call, run, _trace_id(), context)
        # 用量只交给 gateway 计配额，不进回答记录（回放不应再计一次）
        resp.usage.CopyFrom(call.tokens.total())
        self._remember(call, resp, run)
        return resp

    async def StreamAnswer(
        self,
        request: answer_pb2.StreamAnswerRequest,
        context: Ctx,
    ) -> AsyncIterator[answer_pb2.StreamAnswerResponse]:
        answer_id, req = await self._request(request.request, context)
        async with self._admitted(context):
            call = await self._prepare(request.request, context, answer_id, req)
            trace_id = _trace_id()
            yield answer_pb2.StreamAnswerResponse(
                meta=answer_pb2.AnswerMeta(
                    answer_id=call.answer_id,
                    trace_id=trace_id,
                    conversation_id=call.session.conversation_id,
                )
            )
            resp: answer_pb2.AnswerResponse | None = None
            try:
                run = await self._select(call, context)
                # 调用被取消（客户端断开、deadline 到期）时立即关闭流程，连带关掉与 LLM 网关的连接；
                # 这时已产生的用量无法随 final 交给 gateway，只留在日志里。
                # 中途断开的回答不写记录：没有完整结果可回放
                async with aclosing(run.events()) as events:
                    first = True
                    async for event in events:
                        if first and event.HasField("sentence"):
                            first = False
                            bf_metrics.first_sentence.record(
                                time.monotonic() - call.started, {"tenant": call.tenant}
                            )
                        if event.HasField("final"):
                            # final 总是最后一个事件，这时回答已经完整、所有 LLM 调用都已结束：
                            # 先写记录再发 final，收到 final 的调用方马上回放也能查到
                            resp = await self._keep(call, run, trace_id, context)
                            event.final.usage.CopyFrom(call.tokens.total())
                        yield event
            except grpc.aio.AioRpcError as e:
                await _abort_search(e, context)
            finally:
                call.tokens.log(call.tenant, call.purpose)
        self._remember(call, resp if resp is not None else run.response(), run)

    async def GetAnswer(
        self, request: answer_pb2.GetAnswerRequest, context: Ctx
    ) -> answer_pb2.GetAnswerResponse:
        """回放：提问人本人或同一租户的审计岗；其他情况一律 NOT_FOUND，不区分无权与不存在。"""
        principal = await _principal(context)
        rec = await self._record(request.answer_id, context)
        if rec is None or rec.tenant != principal.tenant:
            await context.abort(grpc.StatusCode.NOT_FOUND, "回答不存在")
        if rec.asker == principal.sub:
            access = answer_pb2.REPLAY_ACCESS_ASKER
        elif request.as_auditor:
            access = answer_pb2.REPLAY_ACCESS_AUDITOR
        else:
            await context.abort(grpc.StatusCode.NOT_FOUND, "回答不存在")
        if rec.body_withheld and rec.sealed_answer and self._keys is not None:
            try:
                # 受限级回答：解开加密的审计快照，下面照常按调用者现在的权限复核（部署配置清单 P3）
                rec = await asyncio.wait_for(
                    records.restore(rec, self._keys), _cap(context, RECORD_READ_BUDGET)
                )
            except (httpx.HTTPError, KeyError, ValueError, TimeoutError) as e:
                # 解不开就按没有快照处理：只返回标识与版本
                _log.warning(
                    "解开回答快照失败", extra={"answer_id": rec.answer_id, "err": type(e).__name__}
                )
        chunk_ids = [c.chunk_id for c in rec.answer.citations]
        doc_ids = [s.document.doc_id for s in rec.answer.sentences if s.HasField("document")]
        try:
            chunks = await self._recheck(chunk_ids, context, self._chunk_access)
            docs = await self._recheck(doc_ids, context, self._doc_access)
            answer = replay.prune(rec.answer, chunks, docs)
        except grpc.aio.AioRpcError as e:
            # 复核不了就不返回（fail-closed）
            _log.warning("回放复核权限失败", extra={"code": e.code().name})
            await context.abort(grpc.StatusCode.UNAVAILABLE, "暂时不可用")
        except replay.MalformedError as e:
            _log.error("回答记录无法按权限调整", extra={"answer_id": rec.answer_id, "err": str(e)})
            await context.abort(grpc.StatusCode.UNAVAILABLE, "暂时不可用")
        out = answer_pb2.GetAnswerResponse(
            answer=answer, question=rec.question, body_withheld=rec.body_withheld, access=access
        )
        if rec.created_at is not None:
            out.created_at.FromDatetime(rec.created_at)
        return out

    async def CreateFeedback(
        self, request: answer_pb2.CreateFeedbackRequest, context: Ctx
    ) -> answer_pb2.CreateFeedbackResponse:
        """反馈：只有提问人本人；其他人与不存在的回答一律 NOT_FOUND。"""
        principal = await _principal(context)
        fb = records.feedback_input(request)
        if fb is None:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "rating、comment、sentence_index 或幂等键不合法"
            )
        rec = await self._record(request.answer_id, context)
        if rec is None or rec.tenant != principal.tenant or rec.asker != principal.sub:
            await context.abort(grpc.StatusCode.NOT_FOUND, "回答不存在")
        if fb.sentence_index is not None and fb.sentence_index >= rec.sentence_count:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "sentence_index 越界")
        try:
            saved = await asyncio.wait_for(
                self._records.add_feedback(principal.tenant, principal.sub, fb),
                _cap(context, RECORD_READ_BUDGET),
            )
        except records.FeedbackConflictError:
            await context.abort(grpc.StatusCode.ALREADY_EXISTS, "同一幂等键对应了不同的反馈内容")
        except (records.RecordsUnavailableError, TimeoutError) as e:
            _log.warning("反馈写入失败", extra={"err": type(e).__name__})
            await context.abort(grpc.StatusCode.UNAVAILABLE, "暂时不可用")
        _feedback.add(1, {"rating": saved.rating})
        return answer_pb2.CreateFeedbackResponse(feedback=saved.proto())

    async def DeleteConversation(
        self, request: answer_pb2.DeleteConversationRequest, context: Ctx
    ) -> answer_pb2.DeleteConversationResponse:
        """删除调用者自己的会话（详设 10.4）；不存在或不属于调用者时同样成功，不暴露别人的会话。"""
        principal = await _principal(context)
        if not CONVERSATION_ID.match(request.conversation_id):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "conversation_id 不合法")
        try:
            await self._sessions.delete(
                principal.tenant,
                owner_of(principal.tenant, principal.sub),
                request.conversation_id,
            )
        except StoreUnavailableError as e:
            _log.warning("会话删除失败", extra={"err": str(e)})
            await context.abort(grpc.StatusCode.UNAVAILABLE, "暂时不可用")
        return answer_pb2.DeleteConversationResponse()

    async def _record(self, answer_id: str, context: Ctx) -> records.Record | None:
        if not ANSWER_ID.match(answer_id):
            return None
        try:
            return await asyncio.wait_for(
                self._records.get(answer_id), _cap(context, RECORD_READ_BUDGET)
            )
        except (records.RecordsUnavailableError, TimeoutError) as e:
            _log.warning("读取回答记录失败", extra={"err": type(e).__name__})
            await context.abort(grpc.StatusCode.UNAVAILABLE, "暂时不可用")

    async def _recheck(
        self,
        ids: Sequence[str],
        context: Ctx,
        check: Callable[[Sequence[str], dict[str, Any]], Awaitable[dict[str, bool]]],
    ) -> dict[str, bool]:
        """以调用者本人的身份复核，去重后每 50 个一批；不在结果里的 ID 按不可见处理。"""
        allowed: dict[str, bool] = {}
        uniq = sorted(set(ids))
        for start in range(0, len(uniq), RECHECK_BATCH):
            kw = downstream(context, budget=RECHECK_BUDGET)
            # 回放属于问答这一用途；复核不经 retrieval 的入口准入，调用目的只用于审计归类
            kw["metadata"] += (("x-call-purpose", "answer"),)
            for k, v in (await check(uniq[start : start + RECHECK_BATCH], kw)).items():
                allowed[k] = allowed.get(k, False) or v
        return allowed

    async def _chunk_access(self, ids: Sequence[str], kw: dict[str, Any]) -> dict[str, bool]:
        resp = await self._retrieval.CheckAccess(
            retrieval_pb2.CheckAccessRequest(chunk_ids=ids), **kw
        )
        return {r.chunk_id: r.allowed for r in resp.results}

    async def _doc_access(self, ids: Sequence[str], kw: dict[str, Any]) -> dict[str, bool]:
        resp = await self._retrieval.CheckDocumentAccess(
            retrieval_pb2.CheckDocumentAccessRequest(doc_ids=ids), **kw
        )
        return {r.doc_id: r.allowed for r in resp.results}


async def _principal(context: Ctx) -> principal_pb2.Principal:
    principal = current_principal()
    if principal is None:
        await context.abort(grpc.StatusCode.UNAUTHENTICATED, "缺少 Principal")
    return principal


def _cap(context: Ctx, budget: float) -> float:
    remaining = context.time_remaining()
    return budget if remaining is None else max(0.0, min(budget, remaining))


async def _fall_back(e: grpc.aio.AioRpcError, req: flow.Request, context: Ctx) -> flow.Request:
    """枚举与计数接口不可用：退回普通检索流程，why 里记降级。

    回答末尾的统计口径说明声明不完整、不给总数（K2 详设第 9 节）。"""
    if e.code() not in _FALLBACK:
        await _abort_search(e, context)
    _log.warning("枚举与计数不可用，退回普通检索", extra={"code": e.code().name})
    return dataclasses.replace(
        req,
        degraded=(
            *req.degraded,
            types_pb2.Degradation(stage="listing", reason=e.code().name.lower()),
        ),
    )


def _trace_id() -> str:
    span = trace.get_current_span().get_span_context()
    return format(span.trace_id, "032x") if span.is_valid else ""


async def _abort_search(e: grpc.aio.AioRpcError, context: Ctx) -> NoReturn:
    code = e.code() if e.code() in PASS_THROUGH else grpc.StatusCode.UNAVAILABLE
    _log.warning("检索失败", extra={"code": e.code().name})
    await context.abort(code, "检索失败")
