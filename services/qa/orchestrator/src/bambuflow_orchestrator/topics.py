"""主题汇总类问题（枚举与汇总类问题详设第 6 节）：问一批文档讲了什么，而不是某一份文档里的事实。

先用元数据圈定集合（与枚举共用条件抽取，最多 MAX_DOCS 份），再取每份文档入库时生成的摘要块，
每 BATCH 份一组并行让模型归纳出带引用的要点，要点与所引摘要逐句校验（详设第 8 节，规则同普通回答），
多组时再让模型把各组要点合并成不超过 MAX_GROUPS 个分组。合并只重新分组、不改写要点，
所以校验过的要点与引用原样保留；合并失败时按详设第 9 节退回文档清单，不做归纳。

输出是小标题 + 列表项，末尾是统计口径块；why.based_on_summaries 标明结论基于摘要归纳
（详设第 6 节“已知局限”）。
小标题是版式（与普通回答的 heading 一样不带引用），程序截短到 MAX_TITLE_CHARS，不承载事实。

“主要变化”类问题（详设第 6 节按替代关系逐对比较）不在这里：按原有路由处理。
没有摘要块的文档（尚未生成或生成失败）不纳入归纳，在口径块里写明数量；
详设第 9 节的“用前 3 个分块代替摘要”需要按文档取块的接口，尚未提供。
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import re
from collections.abc import AsyncGenerator, Sequence
from contextlib import aclosing
from dataclasses import dataclass, field
from typing import Protocol

from bambuflow_vocabulary import Vocabularies

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_grounding import Evidence, Grounder, GroundingUnavailableError, Verdict
from bambuflow_orchestrator import listing, prompt, redact
from bambuflow_orchestrator.flow import LLM, citation, level_of
from bambuflow_orchestrator.llm import LLMUnavailableError
from bambuflow_orchestrator.parse import Parser, load_json_line

_log = logging.getLogger(__name__)

# 详设第 6 节的建议值
MAX_DOCS = 200
BATCH = 20
MAX_GROUPS = 7
PAGE_SIZE = 50
# 并行取摘要的上限：一次最多 200 份，不一下子压给 retrieval
EXCERPT_CONCURRENCY = 10
MAX_TITLE_CHARS = 20
DEFAULT_TITLE = "主要内容"
OTHER_TITLE = "其他"

_SUMMARY = re.compile(r"总结|归纳|概括|主要内容|主要讲|主要规定|有哪几方面|有哪些方面|哪几个方面")
# 问的是一批文档：“这些制度”“全部通知”“2025 年的制度”。只点名一份文档（《…》）时是普通问答
_COLLECTION = re.compile(
    r"这些|所有|全部|各[类项份个]|相关|有关|哪些|哪几|一批|近期|今年|去年|\d{4}\s*年"
)


def understand(question: str, vocab: Vocabularies, today: dt.date) -> listing.Conditions | None:
    """识别主题汇总类问题并抽取条件；不是时返回 None。要同时有“总结 / 主要内容”这类问法、
    “这些 / 所有 / 某年的”这类集合说法，以及一类文档（词表里的文档类型，或“文件”“文档”）。"""
    if (
        "《" in question
        or not _SUMMARY.search(question)
        or not _COLLECTION.search(question)
        or not (vocab.doc_type.contained(question) or listing.GENERIC.search(question))
    ):
        return None
    f, text = listing.conditions(question, vocab, today)
    return listing.Conditions(answer_pb2.ANSWER_KIND_SUMMARY, f, text)


class Catalog(Protocol):
    async def count(
        self, filters: types_pb2.Filters, group_by: retrieval_pb2.CountGroupBy
    ) -> retrieval_pb2.CountDocumentsResponse: ...

    async def list(
        self, filters: types_pb2.Filters, page_size: int, page_token: str = ""
    ) -> retrieval_pb2.ListDocumentsResponse: ...


class Excerpts(Protocol):
    async def excerpt(self, doc_id: str, chunk_id: str) -> retrieval_pb2.GetExcerptResponse | None:
        """按调用者权限取一块正文；不可见或不存在时返回 None。不可用时抛 grpc.aio.AioRpcError。"""
        ...


@dataclass
class _Group:
    title: str
    points: list[Verdict] = field(default_factory=list)


def _hit(e: retrieval_pb2.GetExcerptResponse) -> retrieval_pb2.Hit:
    """摘要块当作一条证据：正文送入 LLM 前再做一次 PII 掩码（技术设计文档 8.2 第二道防线）。"""
    return retrieval_pb2.Hit(
        chunk_id=e.chunk_id,
        doc_id=e.doc_id,
        doc_version=e.doc_version,
        text=redact.mask(e.text),
        span=e.span,
        title=e.title,
        heading_path=e.heading_path,
        source_uri=e.source_uri,
        source_type=e.source_type,
        owner=e.owner,
        classification=e.classification,
        injection_flag=e.injection_flag,
    )


@dataclass
class TopicSummary:
    """一次主题汇总。events() 在服务入口里一次算完再重放（与清单路径相同），失败时可以整体退回。"""

    question: str
    cond: listing.Conditions
    filters: types_pb2.Filters
    catalog: Catalog
    excerpts: Excerpts
    llm: LLM
    grounder: Grounder
    status: answer_pb2.Status = answer_pb2.STATUS_ANSWERED
    why: answer_pb2.Why = field(default_factory=answer_pb2.Why)
    text: str = ""
    sentences: list[answer_pb2.Sentence] = field(default_factory=list)
    blocks: dict[str, answer_pb2.Block] = field(default_factory=dict)
    citations: dict[int, answer_pb2.Citation] = field(default_factory=dict)
    restricted: bool = False
    _hits: dict[int, retrieval_pb2.Hit] = field(default_factory=dict)
    _listed: list[str] = field(default_factory=list)
    _total: int = 0
    kind: answer_pb2.AnswerKind = answer_pb2.ANSWER_KIND_SUMMARY

    async def events(self) -> AsyncGenerator[answer_pb2.StreamAnswerResponse, None]:
        counted = await self.catalog.count(self.filters, retrieval_pb2.COUNT_GROUP_BY_UNSPECIFIED)
        self._total = counted.total_count
        self.why.inferred_filters.CopyFrom(self.cond.filters)
        self.why.index_version = counted.index_version
        self.why.policy_version = counted.policy_version
        self.why.prompt_version = prompt.TOPIC_PROMPT_VERSION
        docs = await self._documents() if counted.total_count else []
        summaries = await self._summaries(docs)
        missing = len(docs) - len(summaries)
        groups: list[_Group] | None = None
        if summaries:
            self.why.based_on_summaries = True
            self._hits = dict(enumerate(summaries, start=1))
            groups = await self._induce()
        if groups is None and summaries:
            # 归纳整体失败（模型或校验不可用、合并失败）：退回文档清单，不做归纳（详设第 9 节）
            self.kind = answer_pb2.ANSWER_KIND_ENUMERATION
            self.why.based_on_summaries = False
            for e in self._list_items(docs):
                yield e
        elif groups:
            for e in self._emit(groups):
                yield e
        if not self.sentences:
            self.status = answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
        scope = answer_pb2.Scope(
            filters_text=self.cond.text,
            total_count=counted.total_count,
            total_is_exact=counted.total_is_exact,
            shown=len(self._listed)
            if self.kind == answer_pb2.ANSWER_KIND_ENUMERATION
            else len(summaries),
            metadata_gaps=listing.gaps_text(counted.metadata_gaps),
            basis=listing.BASIS,
        )
        s = self._add("scope", answer_pb2.BLOCK_TYPE_SCOPE, self._scope_note(scope, missing), 1000)
        self.blocks["scope"].scope.CopyFrom(scope)
        ev = answer_pb2.SentenceEvent(
            sentence=s, block_type=answer_pb2.BLOCK_TYPE_SCOPE, scope=scope
        )
        yield answer_pb2.StreamAnswerResponse(sentence=ev)
        yield answer_pb2.StreamAnswerResponse(
            final=answer_pb2.AnswerFinal(
                status=self.status, why=self.why, audit=self.audit(), answer_kind=self.kind
            )
        )

    async def _documents(self) -> list[retrieval_pb2.DocumentSummary]:
        """圈定集合：按生效日期倒序取前 MAX_DOCS 份（详设第 6 节第 1 步取“最相关或最新”的）。"""
        out: list[retrieval_pb2.DocumentSummary] = []
        token = ""
        while len(out) < MAX_DOCS:
            page = await self.catalog.list(self.filters, PAGE_SIZE, token)
            out.extend(page.documents)
            token = page.next_page_token
            if not token:
                break
        docs = out[:MAX_DOCS]
        # 受限级文档的摘要也是受限内容：回答记录据此不保存正文（技术设计文档 8.6）
        self.restricted = any(d.classification == types_pb2.CLASSIFICATION_RESTRICTED for d in docs)
        return docs

    async def _summaries(
        self, docs: Sequence[retrieval_pb2.DocumentSummary]
    ) -> list[retrieval_pb2.Hit]:
        sem = asyncio.Semaphore(EXCERPT_CONCURRENCY)

        async def one(d: retrieval_pb2.DocumentSummary) -> retrieval_pb2.GetExcerptResponse | None:
            if not d.summary_chunk_id:
                return None
            async with sem:
                return await self.excerpts.excerpt(d.doc_id, d.summary_chunk_id)

        got = await asyncio.gather(*(one(d) for d in docs))
        return [_hit(e) for e in got if e is not None]

    async def _induce(self) -> list[_Group] | None:
        """分批归纳（第 2 步）、逐句校验（第 4 步）、合并（第 3 步）。
        没有一组成功或合并失败时返回 None。"""
        refs = sorted(self._hits)
        batches = [refs[i : i + BATCH] for i in range(0, len(refs), BATCH)]
        outputs = await asyncio.gather(*(self._generate(b) for b in batches))
        evidence = {
            r: Evidence(r, h.chunk_id, h.text, h.injection_flag) for r, h in self._hits.items()
        }
        per_batch: list[list[_Group]] = []
        failed = 0
        for i, lines in enumerate(outputs):
            if lines is None:
                failed += 1
                continue
            try:
                groups = await self._verify(lines, evidence)
            except GroundingUnavailableError as e:
                # 校验不可用时不放行任何要点（fail-closed）
                _log.warning("主题汇总的要点校验不可用", extra={"err": str(e)})
                self._degrade("grounding", "unavailable")
                return None
            if groups:
                per_batch.append(groups)
            else:
                _log.info("一组摘要没有通过校验的要点", extra={"batch": i})
        if failed:
            self._degrade("summary_batch", "partial")
        groups = [g for b in per_batch for g in b]
        if not groups:
            # 模型都没有回应：退回清单；有回应但没有要点通过校验：证据不足
            return None if failed == len(batches) else []
        if len(per_batch) == 1 and len(groups) <= MAX_GROUPS:
            return groups
        return await self._merge(groups)

    async def _generate(self, refs: Sequence[int]) -> list[str] | None:
        evidence = [(r, self._hits[r]) for r in refs]
        g = self.llm.generate(
            prompt.TOPIC_SYSTEM,
            prompt.render(self.question, evidence),
            level=level_of([h for _, h in evidence]),
        )
        try:
            async with aclosing(aiter(g)) as lines:
                out = [line async for line in lines]
        except LLMUnavailableError as e:
            _log.warning("主题汇总的一组归纳失败", extra={"err": str(e)})
            return None
        self.why.generation_model = self.why.generation_model or g.model
        return out

    async def _verify(self, lines: Sequence[str], evidence: dict[int, Evidence]) -> list[_Group]:
        """校验一组输出里的要点。小标题行（heading、不带引用）只决定其后要点的分组，不是陈述。"""
        parser = Parser()
        claims = []
        title_of: dict[str, str] = {}
        title = DEFAULT_TITLE
        for raw in lines:
            for c in parser.feed(raw):
                if c.block_type == "heading" and not c.refs:
                    title = _title(c.text)
                    continue
                title_of.setdefault(c.block_id, title)
                claims.append(c)
        if parser.unusable or not claims:
            return []
        result = await self.grounder.verify(claims, evidence)
        self.why.nli_model = self.why.nli_model or result.nli_model
        self.why.support_threshold = result.support_threshold
        groups: dict[str, _Group] = {}
        for v in result.verdicts:
            if v.passed:
                t = title_of.get(v.claim.block_id, DEFAULT_TITLE)
                groups.setdefault(t, _Group(t)).points.append(v)
        return list(groups.values())

    async def _merge(self, groups: list[_Group]) -> list[_Group] | None:
        """把各组要点合并成不超过 MAX_GROUPS 个分组：模型只给出分组标题和要点编号，要点原样保留。"""
        points = {f"p{i}": v for i, v in enumerate((v for g in groups for v in g.points), 1)}
        ids = iter(points)
        listing_text = [(g.title, [(next(ids), v.claim.text) for v in g.points]) for g in groups]
        g = self.llm.generate(
            prompt.MERGE_SYSTEM,
            prompt.render_groups(self.question, listing_text),
            level=level_of(list(self._hits.values())),
        )
        merged: list[_Group] = []
        used: set[str] = set()
        try:
            async with aclosing(aiter(g)) as lines:
                async for raw in lines:
                    obj = load_json_line(raw.strip()) if raw.strip() else None
                    if obj is None or not isinstance(obj.get("points"), list):
                        continue
                    title = obj.get("title")
                    picked = [
                        p
                        for p in obj["points"]
                        if isinstance(p, str) and p in points and p not in used
                    ]
                    if not isinstance(title, str) or not picked or len(merged) == MAX_GROUPS:
                        continue
                    used.update(picked)
                    merged.append(_Group(_title(title), [points[p] for p in picked]))
        except LLMUnavailableError as e:
            _log.warning("主题汇总的合并失败", extra={"err": str(e)})
            self._degrade("summary_merge", "unavailable")
            return None
        if not merged:
            self._degrade("summary_merge", "unparseable")
            return None
        # 模型漏掉的要点不丢弃：校验已经通过，放进最后的“其他”组
        rest = [v for p, v in points.items() if p not in used]
        if rest:
            if len(merged) == MAX_GROUPS:
                merged[-1].points.extend(rest)
            else:
                merged.append(_Group(OTHER_TITLE, rest))
        return merged

    def _emit(self, groups: Sequence[_Group]) -> list[answer_pb2.StreamAnswerResponse]:
        events: list[answer_pb2.StreamAnswerResponse] = []
        for gi, g in enumerate(groups, 1):
            s = self._add(f"g{gi}", answer_pb2.BLOCK_TYPE_HEADING, g.title)
            events.append(_sentence(s, answer_pb2.BLOCK_TYPE_HEADING))
            for v in g.points:
                for ref in v.refs:
                    if ref not in self.citations:
                        self.citations[ref] = citation(ref, self._hits[ref], v.quotes[ref])
                        events.append(answer_pb2.StreamAnswerResponse(citation=self.citations[ref]))
                s = self._add(f"g{gi}-items", answer_pb2.BLOCK_TYPE_LIST_ITEM, v.text, verdict=v)
                events.append(_sentence(s, answer_pb2.BLOCK_TYPE_LIST_ITEM))
        return events

    def _list_items(
        self, docs: Sequence[retrieval_pb2.DocumentSummary]
    ) -> list[answer_pb2.StreamAnswerResponse]:
        out = []
        s = self._add("fallback", answer_pb2.BLOCK_TYPE_NOTE, "暂时无法归纳，以下为相关文档。")
        out.append(_sentence(s, answer_pb2.BLOCK_TYPE_NOTE))
        for d in docs[: listing.MAX_LISTED]:
            s = self._add("list", answer_pb2.BLOCK_TYPE_LIST_ITEM, listing.item_text(d), doc=d)
            self._listed.append(d.doc_id)
            out.append(_sentence(s, answer_pb2.BLOCK_TYPE_LIST_ITEM))
        return out

    def _scope_note(self, scope: answer_pb2.Scope, missing: int) -> str:
        n, exact = scope.total_count, scope.total_is_exact
        if n == 0:
            return "没有找到符合条件的文档；可以放宽条件后再试。"
        total = f"{n} 份" if exact else f"超过 {n} 份"
        if self.kind == answer_pb2.ANSWER_KIND_ENUMERATION:
            return f"符合条件的文档共 {total}，以下列出最新的 {scope.shown} 份（{scope.basis}）。"
        note = f"符合条件的文档共 {total}，基于其中 {scope.shown} 份的摘要归纳（{scope.basis}）。"
        if n > MAX_DOCS:
            note += "范围较大，建议缩小条件后再问。"
        if missing:
            note += f"另有 {missing} 份文档尚未生成摘要，未纳入归纳。"
        return note

    def _add(
        self,
        block_id: str,
        block_type: answer_pb2.BlockType,
        text: str,
        order: int | None = None,
        *,
        verdict: Verdict | None = None,
        doc: retrieval_pb2.DocumentSummary | None = None,
    ) -> answer_pb2.Sentence:
        if block_id not in self.blocks:
            n = len([b for b in self.blocks if b != "scope"])
            self.blocks[block_id] = answer_pb2.Block(
                block_id=block_id, block_type=block_type, order=n if order is None else order
            )
        if self.text:
            self.text += "\n"
        start = len(self.text)
        self.text += text
        s = answer_pb2.Sentence(text=text, start=start, end=len(self.text), block_id=block_id)
        s.claim_type = answer_pb2.CLAIM_TYPE_NONE
        if verdict is not None:
            s.claim_type = answer_pb2.CLAIM_TYPE_QUOTE
            s.ref_ids.extend(verdict.refs)
            s.support = verdict.support
            s.quotes.extend(
                answer_pb2.RefQuote(ref_id=r, quote=verdict.quotes[r]) for r in verdict.refs
            )
        if doc is not None:
            s.claim_type = answer_pb2.CLAIM_TYPE_LIST_ITEM
            s.document.CopyFrom(listing.listed_document(doc))
        self.blocks[block_id].sentence_indexes.append(len(self.sentences))
        self.sentences.append(s)
        return s

    def _degrade(self, stage: str, reason: str) -> None:
        self.why.degraded.append(types_pb2.Degradation(stage=stage, reason=reason))

    def audit(self) -> answer_pb2.AuditBlocks:
        hits = [self._hits[r] for r in sorted(self._hits)]
        return answer_pb2.AuditBlocks(
            candidate_chunk_ids=[h.chunk_id for h in hits],
            context_chunk_ids=[h.chunk_id for h in hits],
            shown_chunk_ids=[self.citations[r].chunk_id for r in sorted(self.citations)],
            listed_doc_ids=self._listed,
            total_count=self._total,
        )

    def response(self) -> answer_pb2.AnswerResponse:
        out = answer_pb2.AnswerResponse(
            status=self.status,
            answer=self.text,
            why=self.why,
            audit=self.audit(),
            answer_kind=self.kind,
        )
        out.sentences.extend(self.sentences)
        out.blocks.extend(sorted(self.blocks.values(), key=lambda b: b.order))
        out.citations.extend(self.citations[r] for r in sorted(self.citations))
        return out


def _title(t: str) -> str:
    t = " ".join(t.split()).strip("#：: ")
    return t[:MAX_TITLE_CHARS] or DEFAULT_TITLE


def _sentence(
    s: answer_pb2.Sentence, block_type: answer_pb2.BlockType
) -> answer_pb2.StreamAnswerResponse:
    return answer_pb2.StreamAnswerResponse(
        sentence=answer_pb2.SentenceEvent(sentence=s, block_type=block_type)
    )
