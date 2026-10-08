"""枚举与计数类问题（枚举与汇总类问题详设第 2、5、7、8、9 节）。

先用元数据圈定集合、拿到总数，再决定全列还是列一部分；列表项和数字都由程序从 ListDocuments、
CountDocuments 的结果填入，不经大模型，所以也不需要逐句 NLI
（详设第 8 节：列表项的依据是“文档存在且符合条件”）。
每个回答都带统计口径块，说清楚统计了什么、有多少、有没有缺口。
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import AsyncGenerator, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from bambuflow_vocabulary import Vocabularies

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2

# N 不超过它时全部列出，超过时列前这么多份（详设第 5 节建议值）
MAX_LISTED = 30
BASIS = "仅统计你有权查看的文档"

_COUNT = re.compile(r"(?:多少|几)(?:份|个|项|篇|种|条|部)|数量")
_LIST = re.compile(r"哪些|列出|清单|所有|全部")
# 问的是比较（“新旧两版有哪些区别”）：交给多跳与比较路径，不是列举
_COMPARE = re.compile(r"区别|差异|不同|相比|对比|比较|变化")
# 没有具体类型的集合对象：“哪些文件”“所有文档”。“材料”“资料”多指办事要准备的东西，不算
GENERIC = re.compile(r"文件|文档")
# 问办事要求（“需要哪些材料”“应提交哪些文件”）：是事实问答，不是列举库里的文档
_REQUIREMENT = re.compile(r"需要|应当|应提交|须提交|要求|准备")
_YEAR = re.compile(r"(\d{4})\s*年\s*(以来|之后|以后|起)?")
_EFFECTIVE = re.compile(r"现行|有效|在用|生效中")
_REPEALED = re.compile(r"已废止|废止的|已作废|失效")
_PENDING = re.compile(r"待生效|即将生效|尚未生效")

_VALIDITY_TEXT: dict[types_pb2.ValidityStatus, str] = {
    types_pb2.VALIDITY_STATUS_EFFECTIVE: "现行",
    types_pb2.VALIDITY_STATUS_PENDING: "待生效",
    types_pb2.VALIDITY_STATUS_REPEALED: "已废止",
}
_FIELD_TEXT = {
    "doc_type": "文档类型",
    "dept": "发文部门",
    "topic_codes": "主题",
    "effective_date": "生效日期",
    "validity_status": "效力状态",
}


@dataclass(frozen=True)
class Conditions:
    kind: answer_pb2.AnswerKind  # 枚举、计数或主题汇总
    filters: types_pb2.Filters
    text: str  # 筛选条件的文字描述


def understand(question: str, vocab: Vocabularies, today: dt.date) -> Conditions | None:
    """识别问集合的问题并抽取条件（详设第 2 节、第 5 节第 1 步）。不是这类问题时返回 None。

    要有“哪些 / 多少份”这类问法，并且问的是一类文档（词表里的文档类型，或“文件”“文档”）；
    “哪些情况下可以提前还款”这类问题不走这条路径。"""
    kind: answer_pb2.AnswerKind | None = (
        answer_pb2.ANSWER_KIND_COUNT
        if _COUNT.search(question)
        else answer_pb2.ANSWER_KIND_ENUMERATION
        if _LIST.search(question)
        else None
    )
    if (
        kind is None
        or _COMPARE.search(question)
        or _REQUIREMENT.search(question)
        or not (vocab.doc_type.contained(question) or GENERIC.search(question))
    ):
        return None
    f, text = conditions(question, vocab, today)
    return Conditions(kind, f, text)


def conditions(question: str, vocab: Vocabularies, today: dt.date) -> tuple[types_pb2.Filters, str]:
    """从问题抽取筛选条件与它的文字描述（详设第 5 节第 1 步），枚举、计数与主题汇总共用。
    上级编码展开到下级（词表只在调用方）。"""
    doc_types = vocab.doc_type.contained(question)
    f = types_pb2.Filters()
    parts: list[str] = []
    expanded = [c for code in doc_types for c in vocab.doc_type.with_descendants(code)]
    if expanded:
        f.doc_types.extend(dict.fromkeys(expanded))
        parts.append("文档类型为" + "或".join(vocab.doc_type.name(c) for c in doc_types))
    if depts := vocab.dept.contained(question):
        f.depts.extend(depts)
        parts.append("发文部门为" + "或".join(vocab.dept.name(c) for c in depts))
    # 部门、文档类型的说法里可能含主题词（“信贷部”里的“信贷”），先去掉再找主题
    rest = vocab.dept.without_names(vocab.doc_type.without_names(question))
    if topics := vocab.topic.contained(rest):
        f.topic_codes.extend(
            dict.fromkeys(c for t in topics for c in vocab.topic.with_descendants(t))
        )
        parts.append("主题为" + "或".join(vocab.topic.name(c) for c in topics))
    if m := _YEAR.search(question):
        year = int(m.group(1))
        f.effective_from = f"{year}-01-01"
        if m.group(2):
            parts.append(f"{year} 年起生效")
        else:
            f.effective_to = f"{year}-12-31"
            parts.append(f"{year} 年生效")
    elif "今年" in question or "去年" in question:
        year = today.year - (1 if "去年" in question else 0)
        f.effective_from, f.effective_to = f"{year}-01-01", f"{year}-12-31"
        parts.append(f"{year} 年生效")
    for pattern, status in (
        (_REPEALED, types_pb2.VALIDITY_STATUS_REPEALED),
        (_PENDING, types_pb2.VALIDITY_STATUS_PENDING),
        (_EFFECTIVE, types_pb2.VALIDITY_STATUS_EFFECTIVE),
    ):
        if pattern.search(question):
            f.validity_statuses.append(status)
            parts.append("效力状态为" + _VALIDITY_TEXT[status])
            break
    return f, "、".join(parts) or "全部文档"


def merge(caller: types_pb2.Filters | None, inferred: types_pb2.Filters) -> types_pb2.Filters:
    """调用方明确给出的条件优先；调用方没给的字段用推断的条件补上。两者都只能收窄范围。"""
    out = types_pb2.Filters()
    out.CopyFrom(inferred)
    if caller is None:
        return out
    out.source_ids.extend(caller.source_ids)
    if caller.doc_types:
        del out.doc_types[:]
        out.doc_types.extend(caller.doc_types)
    if caller.depts:
        del out.depts[:]
        out.depts.extend(caller.depts)
    if caller.topic_codes:
        del out.topic_codes[:]
        out.topic_codes.extend(caller.topic_codes)
    if caller.validity_statuses:
        del out.validity_statuses[:]
        out.validity_statuses.extend(caller.validity_statuses)
    out.effective_from = caller.effective_from or out.effective_from
    out.effective_to = caller.effective_to or out.effective_to
    return out


class Catalog(Protocol):
    """retrieval 的 CountDocuments、ListDocuments。不可用或超时抛 grpc.aio.AioRpcError。"""

    async def count(
        self, filters: types_pb2.Filters, group_by: retrieval_pb2.CountGroupBy
    ) -> retrieval_pb2.CountDocumentsResponse: ...

    async def list(
        self, filters: types_pb2.Filters, page_size: int, page_token: str = ""
    ) -> retrieval_pb2.ListDocumentsResponse: ...


@dataclass
class Listing:
    """一次枚举或计数回答。events() 先推口径块，再推导语与列表项（或分组），最后是 final。"""

    cond: Conditions
    filters: types_pb2.Filters
    catalog: Catalog
    status: answer_pb2.Status = answer_pb2.STATUS_ANSWERED
    why: answer_pb2.Why = field(default_factory=answer_pb2.Why)
    text: str = ""
    sentences: list[answer_pb2.Sentence] = field(default_factory=list)
    blocks: dict[str, answer_pb2.Block] = field(default_factory=dict)
    listed: list[str] = field(default_factory=list)
    total: int = 0
    restricted: bool = False

    async def events(self) -> AsyncGenerator[answer_pb2.StreamAnswerResponse, None]:
        group_by = (
            retrieval_pb2.COUNT_GROUP_BY_YEAR
            if self.filters.doc_types
            else retrieval_pb2.COUNT_GROUP_BY_DOC_TYPE
        )
        if self.cond.kind != answer_pb2.ANSWER_KIND_COUNT:
            group_by = retrieval_pb2.COUNT_GROUP_BY_UNSPECIFIED
        counted = await self.catalog.count(self.filters, group_by)
        docs: Sequence[retrieval_pb2.DocumentSummary] = []
        if self.cond.kind == answer_pb2.ANSWER_KIND_ENUMERATION and counted.total_count > 0:
            listed = await self.catalog.list(self.filters, MAX_LISTED)
            docs = listed.documents[:MAX_LISTED]
        self.total = counted.total_count
        self.why.inferred_filters.CopyFrom(self.cond.filters)
        self.why.index_version = counted.index_version
        self.why.policy_version = counted.policy_version

        n, exact = counted.total_count, counted.total_is_exact
        # 口径块先推，版式上排在最后（详设第 7 节）
        scope = answer_pb2.Scope(
            filters_text=self.cond.text,
            total_count=n,
            total_is_exact=exact,
            shown=len(docs),
            metadata_gaps=gaps_text(counted.metadata_gaps),
            basis=BASIS,
        )
        if n == 0:
            self.status = answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
        # 受限级文档的标题也是受限内容：回答记录据此不保存正文（技术设计文档 8.6）
        self.restricted = any(d.classification == types_pb2.CLASSIFICATION_RESTRICTED for d in docs)
        note = scope_note(scope, self.cond.kind)
        scope_sentence = self._add("scope", answer_pb2.BLOCK_TYPE_SCOPE, note, order_last=True)
        self.blocks["scope"].scope.CopyFrom(scope)
        yield _sentence_event(scope_sentence, answer_pb2.BLOCK_TYPE_SCOPE, scope)

        for g in counted.groups:
            label = g.value or "未标注"
            s = self._add("groups", answer_pb2.BLOCK_TYPE_TABLE, f"{label}：{g.count} 份")
            yield _sentence_event(s, answer_pb2.BLOCK_TYPE_TABLE)
        for d in docs:
            s = self._add("list", answer_pb2.BLOCK_TYPE_LIST_ITEM, item_text(d), d)
            self.listed.append(d.doc_id)
            yield _sentence_event(s, answer_pb2.BLOCK_TYPE_LIST_ITEM)
        yield answer_pb2.StreamAnswerResponse(
            final=answer_pb2.AnswerFinal(
                status=self.status, why=self.why, audit=self.audit(), answer_kind=self.cond.kind
            )
        )

    def _add(
        self,
        block_id: str,
        block_type: answer_pb2.BlockType,
        text: str,
        doc: retrieval_pb2.DocumentSummary | None = None,
        order_last: bool = False,
    ) -> answer_pb2.Sentence:
        if block_id not in self.blocks:
            # 口径块固定排在最后：其他块的序号从 0 起，口径块取一个足够大的序号
            order = 1000 if order_last else len([b for b in self.blocks if b != "scope"])
            self.blocks[block_id] = answer_pb2.Block(
                block_id=block_id, block_type=block_type, order=order
            )
        if self.text:
            self.text += "\n"
        start = len(self.text)
        self.text += text
        s = answer_pb2.Sentence(
            text=text,
            start=start,
            end=len(self.text),
            claim_type=answer_pb2.CLAIM_TYPE_LIST_ITEM if doc else answer_pb2.CLAIM_TYPE_NONE,
            block_id=block_id,
        )
        if doc is not None:
            s.document.CopyFrom(listed_document(doc))
        self.blocks[block_id].sentence_indexes.append(len(self.sentences))
        self.sentences.append(s)
        return s

    def audit(self) -> answer_pb2.AuditBlocks:
        return answer_pb2.AuditBlocks(listed_doc_ids=self.listed, total_count=self.total)

    def response(self) -> answer_pb2.AnswerResponse:
        out = answer_pb2.AnswerResponse(
            status=self.status,
            answer=self.text,
            why=self.why,
            audit=self.audit(),
            answer_kind=self.cond.kind,
        )
        out.sentences.extend(self.sentences)
        out.blocks.extend(sorted(self.blocks.values(), key=lambda b: b.order))
        return out


async def answer(
    cond: Conditions, filters: types_pb2.Filters, catalog: Catalog
) -> answer_pb2.AnswerResponse:
    run = Listing(cond, filters, catalog)
    async for _ in run.events():
        pass
    return run.response()


def scope_note(scope: answer_pb2.Scope, kind: answer_pb2.AnswerKind) -> str:
    """口径句的措辞。回放时按当前权限去掉列表项后也用它重写（replay.py）；
    措辞与 qa/batch 的 listed.go 相同。"""
    n, exact = scope.total_count, scope.total_is_exact
    if n == 0:
        # 只说没有找到，不说“另有若干份你无权查看”（详设第 8 节）
        return "没有找到符合条件的文档；可以放宽条件后再试。"
    total_text = f"{n} 份" if exact else f"超过 {n} 份"
    if kind == answer_pb2.ANSWER_KIND_COUNT:
        return f"符合条件的文档共 {total_text}（{scope.basis}）。"
    shown = (
        f"以下列出全部 {scope.shown} 份" if scope.shown == n else f"以下列出最新的 {scope.shown} 份"
    )
    return f"符合条件的文档共 {total_text}，{shown}（{scope.basis}）。"


def _sentence_event(
    s: answer_pb2.Sentence,
    block_type: answer_pb2.BlockType,
    scope: answer_pb2.Scope | None = None,
) -> answer_pb2.StreamAnswerResponse:
    ev = answer_pb2.SentenceEvent(sentence=s, block_type=block_type)
    if scope is not None:
        ev.scope.CopyFrom(scope)
    return answer_pb2.StreamAnswerResponse(sentence=ev)


def listed_document(doc: retrieval_pb2.DocumentSummary) -> answer_pb2.ListedDocument:
    return answer_pb2.ListedDocument(
        doc_id=doc.doc_id,
        doc_version=doc.doc_version,
        title=doc.title,
        source_uri=doc.source_uri,
        metadata=doc.metadata,
    )


def item_text(d: retrieval_pb2.DocumentSummary) -> str:
    """列表项由字段直接拼出（详设第 5 节第 5 步）。"""
    extras = []
    if d.metadata.effective_date:
        extras.append(f"{d.metadata.effective_date} 起生效")
    if status := _VALIDITY_TEXT.get(d.metadata.validity_status):
        extras.append(status)
    return f"《{d.title}》" + (f"（{'，'.join(extras)}）" if extras else "")


def gaps_text(gaps: Sequence[retrieval_pb2.MetadataGap]) -> str:
    return "；".join(
        f"另有 {g.count} 份文档未标注{_FIELD_TEXT.get(g.field, g.field)}，可能也符合条件"
        for g in gaps
    )
