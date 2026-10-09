"""单跳问答流程（orchestrator 详设第 4 节、9.3–9.5）：检索 → 上下文 → 生成 → 逐句校验 → 输出。

流式与非流式共用同一个处理过程（Answer）：模型每输出一行就切句校验，通过的句子按生成顺序
先发它首次用到的 citation、再发 sentence；非流式把同样的事件收拢成完整的回答契约（详设 9.5）。
未通过校验的句子两种方式下都不出现。单跳不做查询改写（问题原样作为唯一子查询）。
agent 路径（agent.py）只替换取证据与提示词两步，生成与逐句校验沿用这里的实现。
下游通过参数传入的接口访问，便于单元测试和回放。
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncGenerator
from contextlib import aclosing
from dataclasses import dataclass
from typing import Protocol

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_grounding import (
    CALCULATOR_VERSION,
    Evidence,
    Grounder,
    GroundingUnavailableError,
    Verdict,
)
from bambuflow_orchestrator import confidence, context, prompt, redact
from bambuflow_orchestrator.experiments import Assignment
from bambuflow_orchestrator.llm import CircuitOpenError, LLMUnavailableError
from bambuflow_orchestrator.parse import Parser

_log = logging.getLogger(__name__)

# 引用覆盖率达到这个值才算 answered（orchestrator 详设 9.4）
MIN_COVERAGE = 0.8
QUOTE_CHARS = 200  # 仅证据返回时，引文取块正文的前 200 字
RETRIEVERS = ("dense", "keyword")  # retrieval 的召回路；降级的路不计入 why
# 图召回只在问题链接到实体时才执行：有命中带图路径时才计入 why
GRAPH = "graph"
# 精排分低于它的块不进上下文（orchestrator 详设 7.5）；未精排时分数是融合分，量纲不同，不套用
MIN_RERANK_SCORE = 0.2
# 不带引用的标题行只是版式，不是陈述：不校验、不输出、不计入覆盖率（详设 9.4）
LAYOUT_ONLY = frozenset({"heading"})

# 问的是一个集合（K2 详设第 2 节）：检索只给最相关的几块，回答很可能只覆盖集合的一部分。
# 完整的枚举与计数路径要等入库侧补齐文档类型、生效日期等元数据；在那之前按兜底规则
# 在末尾加统计口径说明；
# 拿不到按条件的完整计数时不给总数（K2 详设第 9 节的硬性规则）。
# “多少”“几”只在后面跟量词时算问集合（“多少份制度”），“额度是多少”问的是一个值
ENUMERATION = re.compile(r"哪些|所有|全部|列出|清单|(?:多少|几)(?:份|个|项|篇|种|家|条)")
SCOPE_NOTE = "以上根据检索到的相关文档作答，未能完整统计符合条件的全部文档，可能有遗漏。"

BLOCK_TYPES = {
    "heading": answer_pb2.BLOCK_TYPE_HEADING,
    "paragraph": answer_pb2.BLOCK_TYPE_PARAGRAPH,
    "list_item": answer_pb2.BLOCK_TYPE_LIST_ITEM,
    "step": answer_pb2.BLOCK_TYPE_STEP,
    "table": answer_pb2.BLOCK_TYPE_TABLE,
    "note": answer_pb2.BLOCK_TYPE_NOTE,
}
CLAIM_TYPES = {
    "quote": answer_pb2.CLAIM_TYPE_QUOTE,
    "synthesis": answer_pb2.CLAIM_TYPE_SYNTHESIS,
    "calculation": answer_pb2.CLAIM_TYPE_CALCULATION,
    "list_item": answer_pb2.CLAIM_TYPE_LIST_ITEM,
    "none": answer_pb2.CLAIM_TYPE_NONE,
}


class Retrieval(Protocol):
    async def search(
        self, question: str, filters: types_pb2.Filters | None
    ) -> retrieval_pb2.SearchResponse:
        """权限、参数类错误以 grpc.aio.AioRpcError 原样抛出，由服务入口映射。"""
        ...


class Generation(Protocol):
    """一次生成：async for 按行读取输出；model 是网关实际使用的模型。

    网关不可用或超时时，在读取过程中抛 LLMUnavailableError。
    """

    model: str

    def __aiter__(self) -> AsyncGenerator[str, None]: ...


class LLM(Protocol):
    def generate(self, system: str, user: str) -> Generation: ...


@dataclass(frozen=True)
class Request:
    question: str
    filters: types_pb2.Filters | None
    max_citations: int
    # 走到普通流程之前已经发生的降级（如枚举与计数接口不可用），写进 why
    degraded: tuple[types_pb2.Degradation, ...] = ()
    # 提问人在租户运行中实验里的分组（详设 12.3）；没有实验时为 None
    experiment: Assignment | None = None


def _masked(h: retrieval_pb2.Hit) -> retrieval_pb2.Hit:
    """送入 LLM 前再做一次 PII 掩码（技术设计文档 8.2 第二道防线）。

    引文核对与引用都以掩码后的正文为准；图路径上的实体名称同样会写进 why，一并掩码。
    """
    out = retrieval_pb2.Hit()
    out.CopyFrom(h)
    out.text = redact.mask(h.text)
    for e in out.graph_path:
        e.from_entity = redact.mask(e.from_entity)
        e.to_entity = redact.mask(e.to_entity)
    return out


def unavailable_reason(e: LLMUnavailableError) -> str:
    """LLM 调用失败的降级原因：熔断打开为 circuit_open，其余为 unavailable。"""
    return "circuit_open" if isinstance(e, CircuitOpenError) else "unavailable"


def _degrade(why: answer_pb2.Why, stage: str, reason: str) -> None:
    why.degraded.append(types_pb2.Degradation(stage=stage, reason=reason))


def citation(ref: int, h: retrieval_pb2.Hit, quote: str) -> answer_pb2.Citation:
    return answer_pb2.Citation(
        ref_id=ref,
        chunk_id=h.chunk_id,
        doc_id=h.doc_id,
        doc_version=h.doc_version,
        title=h.title,
        quote=quote,
        span=h.span,
        source_uri=h.source_uri,
        owner=h.owner,
        classification=h.classification,
        source_type=h.source_type,
        injection_flag=h.injection_flag,
    )


Event = answer_pb2.StreamAnswerResponse


class Answer:
    """一次回答的处理过程。events() 产生 citation、sentence 与最后的 final；状态同时记在实例上，
    非流式据此组装完整的回答契约（response()）。

    子类可以替换取证据（_gather）、提示词（_render）与回答末尾程序写的说明（_notes）。
    """

    prompt_version = prompt.PROMPT_VERSION

    def __init__(self, req: Request, retrieval: Retrieval, llm: LLM, grounder: Grounder) -> None:
        self._req, self._retrieval, self._llm, self._grounder = req, retrieval, llm, grounder
        self.status: answer_pb2.Status = answer_pb2.STATUS_UNSPECIFIED
        self.why = answer_pb2.Why()
        self.evidence: list[tuple[int, retrieval_pb2.Hit]] = []
        # 已通过校验、已发送的内容；start、end 按已发送的句子累加（详设 9.3）
        self.text = ""
        self.sentences: list[answer_pb2.Sentence] = []
        self.blocks: dict[str, answer_pb2.Block] = {}
        self.citations: dict[int, answer_pb2.Citation] = {}
        self._hits: dict[int, retrieval_pb2.Hit] = {}
        self._candidates: list[str] = []
        # 回答整体的置信度（详设 7.8）：有陈述经过校验、回答为 answered 或证据不足时才给
        self.confidence: answer_pb2.Confidence | None = None

    async def _gather(self) -> retrieval_pb2.SearchResponse:
        """取证据：单跳是一次检索。"""
        return await self._retrieval.search(self._req.question, self._req.filters)

    def _render(self, evidence: list[tuple[int, retrieval_pb2.Hit]]) -> tuple[str, str]:
        """系统提示词与用户消息。"""
        return prompt.SYSTEM, prompt.render(self._req.question, evidence)

    def _notes(self) -> list[tuple[str, answer_pb2.BlockType, str]]:
        """回答末尾程序写的固定说明（块 ID、块类型、文字）。只在有通过校验的句子时调用。"""
        if self.status == answer_pb2.STATUS_ANSWERED and ENUMERATION.search(self._req.question):
            return [("scope", answer_pb2.BLOCK_TYPE_SCOPE, SCOPE_NOTE)]
        return []

    async def events(self) -> AsyncGenerator[Event, None]:
        req, why = self._req, self.why
        found = await self._gather()
        self._candidates = [h.chunk_id for h in found.hits]
        failed = {d.stage for d in found.degraded}
        why.retrievers.extend(r for r in RETRIEVERS if r not in failed)
        if any(h.graph_path for h in found.hits):
            why.retrievers.append(GRAPH)
        why.rerank_model = found.rerank_model
        why.index_version = found.index_version
        why.policy_version = found.policy_version
        why.embedding_model = found.embedding_model
        why.prompt_version = self.prompt_version
        why.degraded.extend(found.degraded)
        why.degraded.extend(req.degraded)
        if not found.hits:
            # 没有证据就不调用模型（orchestrator 详设 7.5）
            self.status = answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
            yield self._final()
            return

        floor = MIN_RERANK_SCORE if found.rerank_model else None
        self.evidence = [
            (ref, _masked(h)) for ref, h in context.build(found.hits, req.max_citations, floor)
        ]
        if not self.evidence:
            # 精排后没有一块够相关：不调用模型，按证据不足返回（详设 7.5）
            self.status = answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
            yield self._final()
            return
        self._hits = dict(self.evidence)
        min_coverage = self._apply_experiment()
        by_ref = {
            ref: Evidence(ref, h.chunk_id, h.text, h.injection_flag) for ref, h in self.evidence
        }
        # 置信度的特征：最高精排分（没有精排时为 None）与每句的支持度
        top_rerank = max(h.score for _, h in self.evidence) if found.rerank_model else None
        supports: list[float] = []
        parser = Parser()
        passed = 0
        dropped: list[str] = []
        received = False
        failure: tuple[str, str] | None = None
        generation = self._llm.generate(*self._render(self.evidence))
        try:
            # 读取与校验交替进行；校验期间模型在网关一侧继续生成（详设 9.3 第 3 步），
            # 句子按读到的顺序处理，天然保序。提前结束时 aclosing 关掉与网关的连接
            async with aclosing(aiter(generation)) as lines:
                async for raw in lines:
                    received = True
                    claims = [
                        c for c in parser.feed(raw) if c.refs or c.block_type not in LAYOUT_ONLY
                    ]
                    if parser.first_bad:
                        # P0 不重新生成：单跳预算里留给第二次生成的时间不足（详设 8.4 的后一种情况）
                        failure = ("generation", "unparseable")
                        break
                    if not claims:
                        continue
                    result = await self._grounder.verify(claims, by_ref)
                    why.nli_model = why.nli_model or result.nli_model
                    why.support_threshold = result.support_threshold
                    for v in result.verdicts:
                        supports.append(v.support)
                        if not v.passed:
                            dropped.append(v.reason)
                            continue
                        passed += 1
                        for event in self._accept(v):
                            yield event
        except LLMUnavailableError as e:
            # 熔断打开时没有发出请求，与网关不可用同样只返回证据（详设 8.5、11.2）
            _log.warning("LLM 网关不可用", extra={"err": str(e)})
            failure = ("generation", unavailable_reason(e))
        except GroundingUnavailableError as e:
            # 校验不可用时不放行任何句子（fail-closed）；已发送的句子都是此前校验通过的
            _log.warning("逐句校验不可用", extra={"err": str(e)})
            failure = ("grounding", "unavailable")
        if received or failure is None:
            why.generation_model = generation.model
        if dropped:
            _log.info("丢弃未通过校验的句子", extra={"count": len(dropped), "reasons": dropped})
        if failure is None and parser.unusable:
            _log.warning("模型输出无法解析", extra={"lines": parser.lines, "bad": parser.bad})
            failure = ("generation", "unparseable")
        if failure is not None:
            for event in self._evidence_only(*failure):
                yield event
            return

        total = passed + len(dropped)
        coverage = passed / total if total else 0.0
        # 覆盖率不足时已发送的句子不撤回，由 status 告知回答可能不完整（详设 9.4）
        self.status = (
            answer_pb2.STATUS_ANSWERED
            if passed and coverage >= min_coverage
            else answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
        )
        if supports:
            cited = {c.doc_id for c in self.citations.values()}
            self.confidence = confidence.compute(passed, supports, top_rerank, len(cited))
        if passed:
            for block_id, block_type, text in self._notes():
                yield self._note(block_id, block_type, text)
        yield self._final()

    def _apply_experiment(self) -> float:
        """上下文构建之后应用实验组的阈值，返回本次的覆盖率门槛（详设 12.3）。

        受限级内容不进实验：已分到实验组的请求退回基线，分组记为 excluded，不计入 A/B 分析。"""
        exp = self._req.experiment
        if exp is None or not exp.treatment:
            return MIN_COVERAGE
        if any(h.classification == types_pb2.CLASSIFICATION_RESTRICTED for _, h in self.evidence):
            self.why.experiment_arm = answer_pb2.EXPERIMENT_ARM_EXCLUDED
            return MIN_COVERAGE
        if exp.support_threshold is not None:
            self._grounder.use_threshold(exp.support_threshold)
        return exp.min_coverage if exp.min_coverage is not None else MIN_COVERAGE

    def _accept(self, v: Verdict) -> list[Event]:
        """记下一句通过校验的陈述，返回要发送的事件：先是它首次用到的 citation，再是 sentence。"""
        events: list[Event] = []
        block_id = v.claim.block_id
        if block_id not in self.blocks:
            if self.blocks:
                self.text += "\n"  # 段落之间换行
            self.blocks[block_id] = answer_pb2.Block(
                block_id=block_id,
                block_type=BLOCK_TYPES.get(v.claim.block_type, answer_pb2.BLOCK_TYPE_PARAGRAPH),
                order=len(self.blocks),
            )
        block = self.blocks[block_id]
        block.sentence_indexes.append(len(self.sentences))
        for ref in v.refs:
            if ref not in self.citations:
                self.citations[ref] = citation(ref, self._hits[ref], v.quotes[ref])
                events.append(Event(citation=self.citations[ref]))
        start = len(self.text)
        self.text += v.text
        sentence = answer_pb2.Sentence(
            text=v.text,
            start=start,
            end=len(self.text),
            claim_type=CLAIM_TYPES.get(v.claim.claim_type, answer_pb2.CLAIM_TYPE_UNSPECIFIED),
            ref_ids=v.refs,
            support=v.support,
            block_id=block_id,
            quotes=[answer_pb2.RefQuote(ref_id=r, quote=v.quotes[r]) for r in v.refs],
            calc=_calculation(v),
        )
        if v.claim.calc is not None:
            self.why.calculator_version = CALCULATOR_VERSION
        self.sentences.append(sentence)
        events.append(
            Event(sentence=answer_pb2.SentenceEvent(sentence=sentence, block_type=block.block_type))
        )
        return events

    def _note(self, block_id: str, block_type: answer_pb2.BlockType, text: str) -> Event:
        """程序写的固定说明（如统计口径），不是模型的陈述：
        不带引用、不做 NLI（K2 详设第 7、8 节）。"""
        block = answer_pb2.Block(block_id=block_id, block_type=block_type, order=len(self.blocks))
        self.blocks[block.block_id] = block
        block.sentence_indexes.append(len(self.sentences))
        self.text += "\n"
        start = len(self.text)
        self.text += text
        sentence = answer_pb2.Sentence(
            text=text,
            start=start,
            end=len(self.text),
            claim_type=answer_pb2.CLAIM_TYPE_NONE,
            block_id=block.block_id,
        )
        self.sentences.append(sentence)
        return Event(
            sentence=answer_pb2.SentenceEvent(sentence=sentence, block_type=block.block_type)
        )

    def _evidence_only(self, stage: str, reason: str) -> list[Event]:
        """生成或校验不可用：只返回证据。流式时还没发过的证据补发为 citation。"""
        _degrade(self.why, stage, reason)
        self.status = answer_pb2.STATUS_EVIDENCE_ONLY
        events: list[Event] = []
        for ref, h in self.evidence:
            if ref not in self.citations:
                self.citations[ref] = citation(ref, h, h.text[:QUOTE_CHARS])
                events.append(Event(citation=self.citations[ref]))
        events.append(self._final())
        return events

    def _graph_paths(self) -> None:
        """被引用的块由图召回找到时，把遍历路径写进 why（技术设计文档 6.3），按 ref_id 排序。"""
        del self.why.graph_path[:]
        for ref in sorted(self.citations):
            h = self._hits.get(ref)
            if h is not None and h.graph_path:
                self.why.graph_path.append(
                    answer_pb2.GraphPath(
                        ref_id=ref,
                        chunk_id=h.chunk_id,
                        edges=[
                            answer_pb2.GraphEdge(
                                from_entity=e.from_entity,
                                from_type=e.from_type,
                                relation_type=e.relation_type,
                                to_entity=e.to_entity,
                                to_type=e.to_type,
                            )
                            for e in h.graph_path
                        ],
                    )
                )

    def _final(self) -> Event:
        # 非流式的 response() 也读 self.why：final 总在它之前产生
        self._graph_paths()
        final = answer_pb2.AnswerFinal(
            status=self.status,
            why=self.why,
            audit=self.audit(),
            answer_kind=answer_pb2.ANSWER_KIND_FACT,
        )
        if self.confidence is not None:
            final.confidence.CopyFrom(self.confidence)
        return Event(final=final)

    def audit(self) -> answer_pb2.AuditBlocks:
        """审计用的三类块：候选、上下文、展示（被引用的块）。"""
        return answer_pb2.AuditBlocks(
            candidate_chunk_ids=self._candidates,
            context_chunk_ids=[h.chunk_id for _, h in self.evidence],
            shown_chunk_ids=[self.citations[r].chunk_id for r in sorted(self.citations)],
        )

    def response(self) -> answer_pb2.AnswerResponse:
        """非流式的完整回答契约。"""
        out = answer_pb2.AnswerResponse(
            status=self.status,
            why=self.why,
            audit=self.audit(),
            answer_kind=answer_pb2.ANSWER_KIND_FACT,
        )
        if self.status == answer_pb2.STATUS_EVIDENCE_ONLY:
            # 非流式的 evidence_only 只给证据，不带中途已校验的句子，引文统一取块正文开头
            out.citations.extend(citation(r, h, h.text[:QUOTE_CHARS]) for r, h in self.evidence)
            return out
        if self.confidence is not None:
            out.confidence.CopyFrom(self.confidence)
        out.answer = self.text
        out.sentences.extend(self.sentences)
        out.blocks.extend(self.blocks.values())
        out.citations.extend(self.citations[ref] for ref in sorted(self.citations))
        return out


async def answer(
    req: Request, retrieval: Retrieval, llm: LLM, grounder: Grounder
) -> answer_pb2.AnswerResponse:
    """非流式：走同一套按句校验流程，全部处理完后一次性返回（详设 9.5）。"""
    run = Answer(req, retrieval, llm, grounder)
    async for _ in run.events():
        pass
    return run.response()


def stream(
    req: Request, retrieval: Retrieval, llm: LLM, grounder: Grounder
) -> AsyncGenerator[Event, None]:
    """流式：citation、sentence 按校验通过的顺序产生，最后是 final（meta 由服务入口先发）。"""
    return Answer(req, retrieval, llm, grounder).events()


def _calculation(v: Verdict) -> answer_pb2.Calculation | None:
    """计算句的算式与结果，前端在引用处展示（K7 详设 3.4）。"""
    calc = v.claim.calc
    if calc is None:
        return None
    return answer_pb2.Calculation(
        inputs=[
            answer_pb2.CalcInput(var=i.var, ref_id=i.ref, value=f"{i.value:f}", unit=i.unit)
            for i in calc.inputs
        ],
        results=[
            answer_pb2.CalcResult(name=name, expr=calc.exprs[name], value=value)
            for name, value in sorted(v.results.items())
        ],
    )


# 超出范围时的固定说明：程序写的，不是模型的陈述，不带引用
REFUSAL_NOTE = "这个问题不在知识库问答的范围内，可以换一个与公司制度、流程或业务知识相关的问题。"


class Refusal:
    """超出范围的问题（orchestrator 详设 5.3）：不检索、不调用模型，直接 refused。"""

    def __init__(self) -> None:
        self.why = answer_pb2.Why()
        self._sentence = answer_pb2.Sentence(
            text=REFUSAL_NOTE,
            start=0,
            end=len(REFUSAL_NOTE),
            claim_type=answer_pb2.CLAIM_TYPE_NONE,
            block_id="refusal",
        )

    async def events(self) -> AsyncGenerator[Event, None]:
        yield Event(
            sentence=answer_pb2.SentenceEvent(
                sentence=self._sentence, block_type=answer_pb2.BLOCK_TYPE_NOTE
            )
        )
        yield Event(
            final=answer_pb2.AnswerFinal(
                status=answer_pb2.STATUS_REFUSED,
                why=self.why,
                audit=answer_pb2.AuditBlocks(),
                answer_kind=answer_pb2.ANSWER_KIND_FACT,
            )
        )

    def response(self) -> answer_pb2.AnswerResponse:
        return answer_pb2.AnswerResponse(
            status=answer_pb2.STATUS_REFUSED,
            answer=REFUSAL_NOTE,
            sentences=[self._sentence],
            blocks=[
                answer_pb2.Block(
                    block_id="refusal",
                    block_type=answer_pb2.BLOCK_TYPE_NOTE,
                    sentence_indexes=[0],
                )
            ],
            why=self.why,
            audit=answer_pb2.AuditBlocks(),
            answer_kind=answer_pb2.ANSWER_KIND_FACT,
        )
