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
from bambuflow_vocabulary import Vocabularies
from opentelemetry import metrics, trace

from bambuflow.answer.v1 import answer_pb2, answer_pb2_grpc
from bambuflow.common.v1 import types_pb2
from bambuflow.identity.v1 import principal_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_grounding import Grounder, GroundingUnavailableError, KServeNLI
from bambuflow_orchestrator import (
    agent,
    flow,
    intent,
    listing,
    records,
    replay,
    structured,
    topics,
)
from bambuflow_orchestrator.admission import Admission, Overloaded
from bambuflow_orchestrator.budget import AGENT_SECONDS, SINGLE_HOP_SECONDS, Deadline
from bambuflow_orchestrator.experiments import Experiments
from bambuflow_orchestrator.llm import ChatClient, ChatGeneration
from bambuflow_orchestrator.session import AgentStates, Session, Sessions, Store, owner_of
from bambuflow_platform.grpc_server import current_principal, downstream

if TYPE_CHECKING:
    # 异步桩的类型只在生成的 .pyi 里；运行时用 RetrievalServiceStub(aio 通道) 得到的就是它
    from bambuflow.retrieval.v1.retrieval_pb2_grpc import RetrievalServiceAsyncStub

_log = logging.getLogger(__name__)
_meter = metrics.get_meter("bambuflow_orchestrator")
_record_failures = _meter.create_counter(
    "orchestrator.answer_record.write_failures",
    description="回答记录写入失败的次数（回答照常返回，这次回答之后无法回放与反馈）",
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
    ) -> None:
        self._stub, self._ctx, self._deadline, self._intent = stub, ctx, deadline, intent

    async def search(
        self, question: str, filters: types_pb2.Filters | None
    ) -> retrieval_pb2.SearchResponse:
        kw = downstream(self._ctx, budget=self._deadline.cap(SEARCH_BUDGET))
        kw["metadata"] += (("x-call-purpose", _purpose(self._ctx)),)
        # 回答生成总要精排（orchestrator 详设第 5 节第 4 步）：
        # 不依赖按调用目的的默认值，mcp 的默认是不精排
        req = retrieval_pb2.SearchRequest(
            queries=[retrieval_pb2.SubQuery(id="q1", text=question)],
            filters=filters,
            top_k=TOP_K,
            intent=self._intent,
            rerank=True,
        )
        resp: retrieval_pb2.SearchResponse = await self._stub.Search(req, **kw)
        return resp


class _AgentSearch:
    """agent 路径的 doc_search：调用目的 agent_round，带轮次（orchestrator 详设 3.4、6.3）。"""

    def __init__(self, stub: RetrievalServiceAsyncStub, ctx: Ctx) -> None:
        self._stub, self._ctx = stub, ctx

    async def search(
        self, query: str, filters: types_pb2.Filters | None, round_no: int, timeout: float
    ) -> retrieval_pb2.SearchResponse:
        kw = downstream(self._ctx, budget=timeout)
        kw["metadata"] += (("x-call-purpose", "agent_round"), ("x-agent-round", str(round_no)))
        req = retrieval_pb2.SearchRequest(
            queries=[retrieval_pb2.SubQuery(id="q1", text=query)],
            filters=filters,
            top_k=agent.TOP_K,
        )
        resp: retrieval_pb2.SearchResponse = await self._stub.Search(req, **kw)
        return resp


class _Access:
    """grounding 的权限复核：以调用者本人的 Principal 调 CheckAccess。

    local 是本次请求里 sql_query 以调用者身份查出的证据（不在索引里，CheckAccess 不认识）：
    它们的权限已由数据库按调用者的角色判定，直接放行；其余一律经 retrieval 复核。"""

    def __init__(
        self,
        stub: RetrievalServiceAsyncStub,
        ctx: Ctx,
        deadline: Deadline,
        local: set[str] | None = None,
    ) -> None:
        self._stub, self._ctx, self._deadline = stub, ctx, deadline
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
        return {r.chunk_id: r.allowed for r in resp.results}


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
    def __init__(self, client: ChatClient, deadline: Deadline, tokens: _Tokens) -> None:
        self._client, self._deadline, self._tokens = client, deadline, tokens

    def generate(self, system: str, user: str, timeout: float | None = None) -> ChatGeneration:
        """timeout 由调用方给出（agent 的规划）时照用；
        不给时是回答生成，按生成预算并给校验留出时间。"""
        remaining = self._deadline.remaining()
        use = "plan" if timeout is not None else "generate"
        if timeout is None:
            timeout = min(GENERATION_BUDGET, remaining - RESERVE_AFTER_GENERATION)
        g = self._client.generate(system, user, max(0.1, min(timeout, remaining)))
        self._tokens.calls.append((use, g))
        return g


# 计数与列表各自的时间预算（K2 详设第 5 节：各约 200 ms，留出余量）
CATALOG_BUDGET = 0.5
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


class AnswerService(answer_pb2_grpc.AnswerServiceServicer):
    def __init__(
        self,
        retrieval: RetrievalServiceAsyncStub,
        llm: ChatClient,
        nli: KServeNLI,
        vocab: Vocabularies,
        store: Store,
        answer_records: records.Records,
        admission: Admission,
        experiments: Experiments | None = None,
        sql: tuple[structured.Catalog, structured.QueryService] | None = None,
    ) -> None:
        self._retrieval, self._llm, self._nli, self._vocab = retrieval, llm, nli, vocab
        self._records = answer_records
        self._admission = admission
        self._experiments = experiments if experiments is not None else Experiments()
        # 数据平台的只读查询服务（sql_query）；没有配置时结构化问题按普通问题处理
        self._sql = sql
        self._sessions, self._agent_states = Sessions(store), AgentStates(store)
        # 会话写入在回答返回后进行；保留任务的引用，避免执行完之前被回收
        self._background: set[asyncio.Task[None]] = set()

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
        # 分组按提问人（user_id）算，代用户调用的业务系统与 agent 不影响分组（详设 12.3）
        req = dataclasses.replace(
            req, experiment=self._experiments.assignment(principal.tenant, principal.sub)
        )
        if session.degraded:
            req = dataclasses.replace(
                req,
                degraded=(
                    *req.degraded,
                    types_pb2.Degradation(stage="session", reason="unavailable"),
                ),
            )
        return _Call(
            answer_id,
            req,
            principal.tenant,
            session,
            intent.classify(req.question),
            _purpose(context),
            principal.sub,
            principal.actor,
        )

    def _deadline(self, context: Ctx, seconds: float) -> Deadline:
        remaining = context.time_remaining()
        return Deadline(seconds if remaining is None else min(remaining, seconds))

    def _single_hop(
        self,
        req: flow.Request,
        context: Ctx,
        tokens: _Tokens,
        kind: answer_pb2.Intent = answer_pb2.INTENT_SINGLE_HOP,
    ) -> flow.Answer:
        deadline = self._deadline(context, SINGLE_HOP_SECONDS)
        # 关系类问题也走单跳，只是让 retrieval 提高图召回的权重（详设 5.3）
        hint = (
            retrieval_pb2.SEARCH_INTENT_RELATION
            if kind == answer_pb2.INTENT_RELATION
            else retrieval_pb2.SEARCH_INTENT_UNSPECIFIED
        )
        return flow.Answer(
            req,
            _Search(self._retrieval, context, deadline, hint),
            _LLM(self._llm, deadline, tokens),
            Grounder(self._nli, _Access(self._retrieval, context, deadline)),
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
        run = agent.Agent(
            call.req.question,
            call.req.filters,
            call.session.recent_questions,
            _AgentSearch(self._retrieval, context),
            llm,
            deadline,
            _AgentState(self._agent_states, call.tenant, call.answer_id),
            sql=sql,
        )
        grounder = Grounder(self._nli, _Access(self._retrieval, context, deadline, local))
        return agent.AgentAnswer(call.req, run, llm, grounder)

    async def _select(self, call: _Call, context: Ctx) -> _Run:
        """选择处理路径，并把实验分组写进 why（详设 12.3）。"""
        run = await self._route(call, context)
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
                Grounder(self._nli, _Access(self._retrieval, context, deadline)),
            )
            ts.why.intent = kind
            ts.why.degraded.extend(req.degraded)
            try:
                ready = [e async for e in ts.events()]
            except grpc.aio.AioRpcError as e:
                run = self._single_hop(await _fall_back(e, req, context), context, call.tokens)
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
                run = self._single_hop(await _fall_back(e, req, context), context, call.tokens)
            else:
                return _Replay(lst, ready)
        elif kind == answer_pb2.INTENT_MULTI_HOP:
            run = self._agent(call, context)
        else:
            run = self._single_hop(req, context, call.tokens, kind)
        run.why.intent = kind
        return run

    def _remember(self, call: _Call, resp: answer_pb2.AnswerResponse) -> None:
        """回答结束后异步写会话，不占用响应时间（详设 10.2）。"""
        cited = [c.chunk_id for c in resp.citations]
        task = asyncio.create_task(
            self._sessions.record(call.tenant, call.session, call.req.question, cited)
        )
        self._background.add(task)
        task.add_done_callback(self._background.discard)

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
        )
        try:
            await asyncio.wait_for(self._records.put(rec), _cap(context, RECORD_WRITE_BUDGET))
        except (records.RecordsUnavailableError, TimeoutError) as e:
            _record_failures.add(1, {"reason": type(e).__name__})
            _log.warning(
                "回答记录写入失败，回答照常返回",
                extra={"answer_id": call.answer_id, "err": type(e).__name__},
            )
        return resp

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
        self._remember(call, resp)
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
                    async for event in events:
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
        self._remember(call, resp if resp is not None else run.response())

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
