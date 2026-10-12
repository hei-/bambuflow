"""Agentic 多步检索（orchestrator 详设第 6 节）：多跳与比较类问题走一个有硬预算的状态机。

PLAN（拆子问题）→ RETRIEVE（每个子问题一次 doc_search，并行）→ ASSESS（逐个判断证据是否充分），
未全部覆盖且预算允许时回到 PLAN，只为未覆盖的子问题换检索表述；最多 3 轮、每轮最多 4 次调用。
之后进入 ANSWER：合并后的证据走与单跳相同的上下文构建、生成和逐句校验（flow.Answer），
一个子问题都没覆盖时 GIVE_UP，不调用生成，返回 insufficient_evidence。

每次检索都以调用者本人的 Principal 发出（由检索适配器转发），规划器无法在轮次之间提权；
规划器只看到证据的标题和前 200 字，并标为不可信（详设 6.4）。

与设计的差异：
- 充分性判断只用规则：处于 0.2–0.5 之间的子问题按“未覆盖”继续规划，但算作有证据（不 GIVE_UP）；
  详设里这一段再用一次小模型判定，等意图与判定小模型部署后补上。
- 工具：doc_search；租户开放结构化查询时另有只读 sql_query（structured 模块，P3-Q），
  规划器看到白名单视图的目录，可以给子问题指定 sql_query 与参数。sql_query 的子问题不再改写重查：
  参数不合法、无权或服务不可用都直接算未覆盖。
- 可选的 LLM 重排（详设 6.5）未做：多跳上下文按子问题轮流取精排靠前的块，已能保证各方都有证据。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import grpc

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_grounding import Grounder
from bambuflow_orchestrator import flow, llm_rerank, prompt, structured
from bambuflow_orchestrator.budget import GENERATION_RESERVE, Deadline
from bambuflow_orchestrator.llm import LLMUnavailableError
from bambuflow_orchestrator.parse import load_json_line
from bambuflow_orchestrator.session import StoreUnavailableError

_log = logging.getLogger(__name__)

MAX_ROUNDS = 3
MAX_SUB_QUESTIONS = 4  # 也是每轮的工具调用上限
MAX_EVIDENCE = 30  # 跨轮合并后的候选证据上限
AGENT_PHASE = 8.0  # agent 阶段的上限，从进入 agent 路径起算
PLAN_BUDGET = 1.5
# 每次 doc_search 的上限。详设为 400 ms；开发环境的检索与精排跑在 CPU 上，与单跳一样放宽到 1.5 s
SEARCH_BUDGET = 1.5
MIN_CALL_SECONDS = 0.1  # 剩余预算不够一次调用时不发起
TOP_K = 10
# 充分性判断（详设 6.2）：精排分达到 COVERED 算覆盖，没有一条高于 RELEVANT 算无证据
# 多跳路径的候选超过这么多条时做 LLM 重排（详设 6.5，llm_rerank.py）
RERANK_MIN = 20
COVERED_SCORE = 0.5
RELEVANT_SCORE = 0.2
UNRESOLVED_NOTE = "部分子问题没有检索到依据，以上回答可能不完整。"
TOOL = "doc_search"
SQL_BUDGET = 1.0  # sql_query 单次上限（详设 6.3）
# 后续轮次的检索遇到这些错误时停止多轮，用已有证据作答；其余错误（权限、参数）照常返回
_STOP_ROUNDS = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
}


class Search(Protocol):
    async def search(
        self, query: str, filters: types_pb2.Filters | None, round_no: int, timeout: float
    ) -> retrieval_pb2.SearchResponse:
        """调用目的 agent_round、带 x-agent-round。错误以 grpc.aio.AioRpcError 抛出。"""
        ...


class PlanLLM(Protocol):
    def generate(
        self,
        system: str,
        user: str,
        timeout: float | None = None,
        *,
        level: int = types_pb2.CLASSIFICATION_INTERNAL,
    ) -> flow.Generation: ...


class SqlTool(Protocol):
    """只读 sql_query（structured 模块）：以调用者身份执行，结果是一条 source_type = db 的证据。

    参数不合法抛 structured.QueryError，无权抛 QueryDenied，服务不可用抛 QueryUnavailable。"""

    catalog: structured.Catalog

    async def query(self, args: dict[str, Any], timeout: float) -> retrieval_pb2.Hit: ...


class StateSink(Protocol):
    """中间状态的存放处（Redis）。不可用时抛 StoreUnavailableError，这里只记降级。"""

    async def save(self, state: dict[str, Any]) -> None: ...

    async def drop(self) -> None: ...


@dataclass(frozen=True)
class _SqlFailure:
    """sql_query 没有结果的原因（invalid、denied、unavailable），记进降级项，子问题算未覆盖。"""

    reason: str


@dataclass
class SubQuestion:
    id: str
    text: str  # 拆出来的子问题，回答时据此组织
    query: str  # 本轮实际检索用的表述，第 2 轮起由规划器改写
    hits: list[retrieval_pb2.Hit] = field(default_factory=list)
    best: float = 0.0
    covered: bool = False
    relevant: bool = False  # 有过高于 RELEVANT_SCORE 的证据
    tool: str = TOOL
    args: dict[str, Any] = field(default_factory=dict)  # sql_query 的参数


@dataclass
class Gathered:
    """agent 阶段的结果。hits 已按子问题轮流排好，直接交给上下文构建。"""

    found: retrieval_pb2.SearchResponse
    sub_questions: list[SubQuestion]
    rounds: list[answer_pb2.AgentRound]
    gave_up: bool  # 没有一个子问题有证据：found.hits 为空，不调用生成
    # 做了 LLM 重排时的模型与提示词版本（写进 why.llm_rerank）；没做时为空
    llm_rerank: str = ""

    @property
    def unresolved(self) -> list[str]:
        """没有任何相关证据的子问题。只有弱证据的不算：交给模型与逐句校验去判断。"""
        return [s.text for s in self.sub_questions if not (s.covered or s.relevant)]


class Agent:
    def __init__(
        self,
        question: str,
        filters: types_pb2.Filters | None,
        history: Sequence[str],
        search: Search,
        llm: PlanLLM,
        deadline: Deadline,
        state: StateSink,
        clock: Callable[[], float] = time.monotonic,
        sql: SqlTool | None = None,
        summary: str = "",
        max_rounds: int | None = None,
        max_sub_questions: int | None = None,
    ) -> None:
        self._question, self._filters, self._history = question, filters, list(history)
        # agent 预算（pipeline 可在平台上限内调小，详设第 6 节）
        self._max_rounds = min(max_rounds or MAX_ROUNDS, MAX_ROUNDS)
        self._max_subs = min(max_sub_questions or MAX_SUB_QUESTIONS, MAX_SUB_QUESTIONS)
        # 会话摘要：与之前的问题一样只给规划器理解指代（详设 10.2）
        self._summary = summary
        self._sql = sql
        self._search, self._llm, self._deadline, self._state = search, llm, deadline, state
        self._clock = clock
        self._phase = Deadline(min(AGENT_PHASE, deadline.remaining()), clock)
        self._subs: list[SubQuestion] = []
        self._rounds: list[answer_pb2.AgentRound] = []
        self._degraded: list[types_pb2.Degradation] = []
        self._responses: list[retrieval_pb2.SearchResponse] = []
        # LLM 重排后的证据顺序；没做重排时为 None，按子问题轮流的顺序
        self._reranked: list[retrieval_pb2.Hit] | None = None
        self._llm_rerank = ""

    @property
    def uses_sql(self) -> bool:
        return self._sql is not None

    def _budget(self, budget: float) -> float:
        """一次调用的超时：给生成留出预留，也不超过 agent 阶段的上限。"""
        return min(self._deadline.cap(budget, GENERATION_RESERVE), self._phase.cap(budget))

    def _degrade(self, stage: str, reason: str) -> None:
        if not any(d.stage == stage and d.reason == reason for d in self._degraded):
            self._degraded.append(types_pb2.Degradation(stage=stage, reason=reason))

    async def run(self) -> Gathered:
        for round_no in range(1, self._max_rounds + 1):
            # sql_query 的子问题不改写重查：参数由规划器一次给定，失败多半是无权或参数不对
            pending = [s for s in self._subs if not s.covered and s.tool == TOOL]
            if round_no > 1 and (not pending or self._budget(SEARCH_BUDGET) < MIN_CALL_SECONDS):
                break
            planned = await (self._plan() if round_no == 1 else self._replan(pending))
            if not planned:
                break
            proceed = await self._retrieve(round_no, planned)
            await self._save(round_no)
            if not proceed:
                break
        await self._drop()
        await self._rerank()
        return self._gathered()

    # ---- PLAN ----

    async def _plan(self) -> list[SubQuestion]:
        """第 1 轮：拆子问题。规划失败时退回把原问题当作唯一的子问题（与单跳相同的检索）。"""
        if self._sql is not None:
            system = prompt.PLAN_SQL_SYSTEM
            user = prompt.render_plan(
                self._question, self._history, self._sql.catalog.describe(), self._summary
            )
        else:
            system = prompt.PLAN_SYSTEM
            user = prompt.render_plan(self._question, self._history, summary=self._summary)
        lines = await self._ask(system, user)
        subs: list[SubQuestion] = []
        for obj in lines or []:
            text = obj.get("text")
            if isinstance(text, str) and text.strip() and len(subs) < self._max_subs:
                sid = f"q{len(subs) + 1}"
                sub = SubQuestion(sid, text.strip(), text.strip())
                args = obj.get("args")
                if self._sql is not None and obj.get("tool") == structured.TOOL:
                    # 规划器给的参数原样交给 sql_query 校验；不是对象时按空参数，校验时拒绝
                    sub.tool, sub.args = structured.TOOL, args if isinstance(args, dict) else {}
                subs.append(sub)
        if not subs:
            if lines is not None:
                self._degrade("plan", "unparseable")
            subs = [SubQuestion("q1", self._question, self._question)]
        self._subs = subs
        return subs

    async def _replan(self, pending: list[SubQuestion]) -> list[SubQuestion]:
        """第 2 轮起：只为未覆盖的子问题换检索表述。规划失败或给不出新表述时结束多轮。"""
        # 带可疑指令标记的证据不进规划器（orchestrator 详设 6.4）：
        # 规划器是间接注入最可能影响行为的地方
        seen = [h for h in self._merged() if not h.injection_flag][:MAX_EVIDENCE]
        user = prompt.render_replan(self._question, [(s.id, s.text) for s in pending], seen)
        by_id = {s.id: s for s in pending}
        planned: list[SubQuestion] = []
        for obj in await self._ask(prompt.REPLAN_SYSTEM, user) or []:
            sub, text = by_id.pop(str(obj.get("id", "")), None), obj.get("text")
            # 换汤不换药的表述只会检索到同样的结果，不再花一轮
            if sub is None or not isinstance(text, str) or text.strip() in ("", sub.query):
                continue
            sub.query = text.strip()
            planned.append(sub)
        return planned

    async def _ask(self, system: str, user: str) -> list[dict[str, Any]] | None:
        """调用规划器，返回解析出的 JSON 行；模型不可用或时间不足时返回 None 并记降级。"""
        timeout = self._budget(PLAN_BUDGET)
        if timeout < MIN_CALL_SECONDS:
            self._degrade("plan", "no_time")
            return None
        out: list[dict[str, Any]] = []
        try:
            # 规划器看到已取得证据的标题与摘要：按其中的最高密级选模型（orchestrator 详设 8.1）
            level = flow.level_of(self._merged())
            async for raw in self._llm.generate(system, user, timeout, level=level):
                if (obj := load_json_line(raw)) is not None:
                    out.append(obj)
        except LLMUnavailableError as e:
            # 熔断打开时同样按规划失败处理：第 1 轮退回原问题，之后结束多轮
            _log.warning("规划器不可用", extra={"err": str(e)})
            self._degrade("plan", flow.unavailable_reason(e))
            return None
        return out

    # ---- RETRIEVE + ASSESS ----

    async def _retrieve(self, round_no: int, planned: list[SubQuestion]) -> bool:
        """并行检索本轮的子问题并判断是否覆盖；返回 False 表示不再继续下一轮。"""
        timeout = self._budget(SEARCH_BUDGET)
        if timeout < MIN_CALL_SECONDS:
            self._degrade("agent_round", "no_time")
            return False
        results = await asyncio.gather(
            *(self._call(round_no, s, timeout) for s in planned), return_exceptions=True
        )
        record = answer_pb2.AgentRound(round=round_no)
        ok = 0
        first_error: grpc.aio.AioRpcError | None = None
        for sub, result in zip(planned, results, strict=True):
            call = answer_pb2.AgentCall(
                sub_question_id=sub.id, sub_question=sub.query, tool=sub.tool
            )
            if isinstance(result, _SqlFailure):
                # 工具给出了结论（只是没有结果），不影响其他子问题继续多轮
                ok += 1
                self._degrade(structured.TOOL, result.reason)
            elif isinstance(result, grpc.aio.AioRpcError):
                if result.code() not in _STOP_ROUNDS:
                    raise result
                first_error = first_error or result
                self._degrade("agent_round", result.code().name.lower())
            elif isinstance(result, BaseException):
                raise result
            else:
                resp, elapsed = result
                ok += 1
                self._responses.append(resp)
                self._assess(sub, resp)
                call.chunk_ids.extend(h.chunk_id for h in resp.hits)
                call.elapsed_ms = elapsed
            call.covered = sub.covered
            record.calls.append(call)
        self._rounds.append(record)
        if first_error is not None and not self._responses:
            # 第 1 轮一次都没成功：与单跳一样按检索不可用处理，不尝试无检索生成
            raise first_error
        return ok == len(planned)

    async def _call(
        self, round_no: int, sub: SubQuestion, timeout: float
    ) -> tuple[retrieval_pb2.SearchResponse, int] | _SqlFailure:
        start = self._clock()
        if sub.tool == structured.TOOL and self._sql is not None:
            try:
                hit = await self._sql.query(sub.args, min(timeout, SQL_BUDGET))
            except structured.QueryError as e:
                _log.info("sql_query 参数不合法", extra={"err": str(e)})
                return _SqlFailure("invalid")
            except structured.QueryDenied:
                return _SqlFailure("denied")
            except structured.QueryUnavailable as e:
                _log.warning("sql_query 不可用", extra={"err": str(e)})
                return _SqlFailure("unavailable")
            resp = retrieval_pb2.SearchResponse(hits=[hit])
        else:
            resp = await self._search.search(sub.query, self._filters, round_no, timeout)
        return resp, int((self._clock() - start) * 1000)

    @staticmethod
    def _assess(sub: SubQuestion, resp: retrieval_pb2.SearchResponse) -> None:
        """按规则判断子问题是否已有足够证据（详设 6.2）。

        未精排时分数是融合分，量纲不同，有命中就算覆盖（与单跳不套用精排分下限的做法一致）。"""
        known = {h.chunk_id for h in sub.hits}
        sub.hits.extend(h for h in resp.hits if h.chunk_id not in known)
        sub.hits.sort(key=lambda h: -h.score)
        if not resp.hits:
            return
        sub.best = max(sub.best, max(h.score for h in resp.hits))
        if resp.rerank_model:
            sub.covered = sub.covered or sub.best >= COVERED_SCORE
            sub.relevant = sub.relevant or sub.best > RELEVANT_SCORE
        else:
            sub.covered = sub.relevant = True

    # ---- 合并 ----

    def _merged(self) -> list[retrieval_pb2.Hit]:
        """按子问题轮流取各自排名靠前的块，跨轮、跨子问题按 chunk_id 去重，最多 MAX_EVIDENCE 条。

        比较类问题若只按分数排，上下文可能全是其中一方；轮流取保证每个子问题都有证据进上下文。"""
        out: list[retrieval_pb2.Hit] = []
        seen: set[str] = set()
        queues = [list(s.hits) for s in self._subs]
        while len(out) < MAX_EVIDENCE and any(queues):
            for q in queues:
                while q and q[0].chunk_id in seen:
                    q.pop(0)
                if q and len(out) < MAX_EVIDENCE:
                    h = q.pop(0)
                    seen.add(h.chunk_id)
                    out.append(h)
        return out

    def _gathered(self) -> Gathered:
        found = retrieval_pb2.SearchResponse()
        if self._responses:
            # sql_query 的结果不带索引与模型版本，版本号取第一次文档检索的
            first = next((r for r in self._responses if r.index_version), self._responses[0])
            found.index_version = first.index_version
            found.policy_version = first.policy_version
            found.embedding_model = first.embedding_model
            found.rerank_model = first.rerank_model
            found.plan_version = first.plan_version
            for r in self._responses:
                for d in r.degraded:
                    self._degrade(d.stage, d.reason)
                # 轮次之间切换了索引或策略版本：why 只能记一个，另一个记进降级，回放时能看出来
                if r.index_version != first.index_version:
                    self._degrade("index_version", r.index_version)
                if r.policy_version != first.policy_version:
                    self._degrade("policy_version", r.policy_version)
        relevant = any(s.covered or s.relevant for s in self._subs)
        if relevant:
            found.hits.extend(self._merged() if self._reranked is None else self._reranked)
        found.degraded.extend(self._degraded)
        return Gathered(
            found, self._subs, self._rounds, gave_up=not relevant, llm_rerank=self._llm_rerank
        )

    # ---- LLM 重排 ----

    async def _rerank(self) -> None:
        merged = self._merged()
        if len(merged) <= RERANK_MIN:
            return
        r = await llm_rerank.rerank(
            self._llm,
            self._question,
            [s.text for s in self._subs],
            merged,
            self._deadline.remaining,
        )
        if r.degraded:
            self._degrade("llm_rerank", r.degraded)
        if r.degraded != "skipped_deadline":
            self._reranked = r.hits
        self._llm_rerank = r.version

    # ---- 中间状态 ----

    async def _save(self, round_no: int) -> None:
        state = {
            "round": round_no,
            "sub_questions": [
                {"id": s.id, "text": s.text, "query": s.query, "covered": s.covered}
                for s in self._subs
            ],
            "evidence": [{"chunk_id": h.chunk_id, "score": h.score} for h in self._merged()],
        }
        try:
            await self._state.save(state)
        except StoreUnavailableError as e:
            _log.warning("agent 中间状态写入失败", extra={"err": str(e)})
            self._degrade("session", "unavailable")

    async def _drop(self) -> None:
        try:
            await self._state.drop()
        except StoreUnavailableError as e:
            # 删不掉的状态 10 分钟后自然过期
            _log.warning("agent 中间状态删除失败", extra={"err": str(e)})


class _Evidence:
    """把 agent 阶段包装成 flow 的“取证据”一步。"""

    def __init__(self, agent: Agent) -> None:
        self._agent = agent
        self.result: Gathered | None = None

    async def search(
        self, question: str, filters: types_pb2.Filters | None, variants: Sequence[str] = ()
    ) -> retrieval_pb2.SearchResponse:
        self.result = await self._agent.run()
        return self.result.found


class AgentAnswer(flow.Answer):
    """agent 路径的一次回答：取证据换成多轮检索，提示词带子问题；生成与逐句校验与单跳相同。"""

    template_scope = "agent-answer"
    builtin = prompt.AGENT

    def __init__(self, req: flow.Request, agent: Agent, llm: flow.LLM, grounder: Grounder) -> None:
        self._evidence_source = _Evidence(agent)
        super().__init__(req, self._evidence_source, llm, grounder)
        if agent.uses_sql:
            # 规划器换成带视图目录的模板，版本号随之区分（回放据此还原）
            self._prompt_suffix = "+" + prompt.SQL_PLAN_VERSION
            self._use_template()

    @property
    def _gathered(self) -> Gathered:
        g = self._evidence_source.result
        if g is None:
            raise RuntimeError("agent 阶段尚未完成")
        return g

    async def _gather(self) -> retrieval_pb2.SearchResponse:
        found = await super()._gather()
        self.why.agent_rounds.extend(self._gathered.rounds)
        self.why.llm_rerank = self._gathered.llm_rerank
        return found

    def _render(self, evidence: list[tuple[int, retrieval_pb2.Hit]]) -> tuple[str, str]:
        g = self._gathered
        return self._fill(
            {
                "evidence": prompt.render_evidence(evidence),
                "subquestions": prompt.render_subquestions([s.text for s in g.sub_questions]),
                "unresolved": prompt.render_unresolved(g.unresolved),
            }
        )

    def _notes(self) -> list[tuple[str, answer_pb2.BlockType, str]]:
        notes = super()._notes()
        if self._gathered.unresolved:
            notes.append(("unresolved", answer_pb2.BLOCK_TYPE_NOTE, UNRESOLVED_NOTE))
        return notes
