"""文档级受控元数据与效力信息（入库管道详设第 7 节，枚举与汇总类问题详设第 4 节）。

来源优先级：源系统的结构化字段 → 标题与正文规则 → 模型抽取
（MetadataModel 端口，只补前两者没取到的字段）。
取到的说法按受控词表（governance 下发的快照）归一成编码，归一不上或取不到的字段记入 missing，不猜：
枚举与计数时由 metadata_gaps 披露，而不是把文档悄悄算进或漏出某一类。

模型抽取默认不接：与摘要模型一样，实现方经 LLM 网关按文档密级路由（受限级用独立实例）、
用量计入配额。
模型给出的每个字段带置信度，低于 MIN_CONFIDENCE 的不用（效力信息：置信度不足时留空，不猜）。
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from bambuflow_vocabulary import Vocabularies

from bambuflow.common.v1 import types_pb2
from bambuflow_processing.chunker import Chunk

FIELDS = ("doc_type", "dept", "topic_codes", "effective_date", "validity_status")
# 模型抽取的字段置信度下限（入库管道详设第 7 节：置信度不足时留空）
MIN_CONFIDENCE = 0.8


@dataclass(frozen=True)
class Guess:
    """模型抽取的结果：各字段是原文里的说法（还没按词表归一），
    confidence 是字段名到 0–1 的置信度。"""

    doc_type: str = ""
    dept: str = ""
    topics: tuple[str, ...] = ()
    effective_date: str = ""
    # effective / pending / repealed
    validity_status: str = ""
    confidence: Mapping[str, float] = field(default_factory=dict)


class MetadataModel(Protocol):
    """受控元数据的模型抽取端口：实现方经 LLM 网关调用，按 classification 路由；
    只见到掩码后的块。"""

    def guess(self, title: str, chunks: Sequence[Chunk], classification: int) -> Guess: ...


_STATUS = {
    "effective": types_pb2.VALIDITY_STATUS_EFFECTIVE,
    "pending": types_pb2.VALIDITY_STATUS_PENDING,
    "repealed": types_pb2.VALIDITY_STATUS_REPEALED,
}
# 标题末尾的版本、年份括注：“个人消费贷款管理办法（2025 版）”
_TITLE_SUFFIX = re.compile(r"[（(][^）)]*[）)]\s*$")
_EFFECTIVE = re.compile(
    r"自\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*起\s*(?:施行|实施|执行|生效)"
)
# 发文部门只认明确的表述，不从正文里随便出现的部门名推断（“由风险管理部审批”不是发文部门）
_DEPT = re.compile(r"(?:发文部门|归口部门|归口管理部门|制定部门)\s*[:：为是]\s*([^\s，。；,;]+)")
_REPEALED_TITLE = re.compile(r"已废止|已作废|（废止）")
# 替代关系：“《××管理办法（2020 版）》同时废止”。只认书名号里的标题紧跟“同时废止”一类说法，
# 不从正文里随便出现的书名推断
_REPEALS = re.compile(
    r"《([^《》]{2,80})》(?:[、和及与]《[^《》]{2,80}》)*"
    r"\s*(?:同时|一并|即行)?\s*(?:废止|停止执行|不再执行)"
)
_TITLE_IN = re.compile(r"《([^《》]{2,80})》")
# 一份文档最多声明替代的文档数（与 indexing 一致）
MAX_SUPERSEDES = 50


def extract(
    title: str, text: str, source: Mapping[str, Any] | None, vocab: Vocabularies, today: dt.date
) -> types_pb2.DocMetadata:
    """抽取一份文档的元数据。source 是文档包里的 source_fields（源系统字段），没有时为 None。"""
    src = source or {}
    clean_title = _TITLE_SUFFIX.sub("", title).strip()
    md = types_pb2.DocMetadata(vocabulary_version=vocab.version)

    md.doc_type = (
        vocab.doc_type.exact(src["doc_type"]) if src.get("doc_type") else ""
    ) or vocab.doc_type.suffix(clean_title)
    dept = src.get("dept") or ""
    if not dept and (m := _DEPT.search(text)):
        dept = m.group(1)
    md.dept = vocab.dept.exact(dept) if dept else ""
    topics = [c for t in src.get("topics") or [] if (c := vocab.topic.exact(t))]
    md.topic_codes.extend(topics or vocab.topic.contained(clean_title))

    effective = src.get("effective_date") or _effective_date(text)
    if effective:
        md.effective_date = effective
    status = src.get("status") or ""
    if status:
        md.validity_status = _STATUS[status]
    elif _REPEALED_TITLE.search(title):
        md.validity_status = types_pb2.VALIDITY_STATUS_REPEALED
    elif effective and dt.date.fromisoformat(effective) > today:
        # 生效日期在将来：待生效。过去的日期不能推出“现行”——它可能已被后来的文档废止
        md.validity_status = types_pb2.VALIDITY_STATUS_PENDING

    md.supersedes.extend(supersedes(text, src.get("supersedes") or []))

    values = {
        "doc_type": md.doc_type,
        "dept": md.dept,
        "topic_codes": list(md.topic_codes),
        "effective_date": md.effective_date,
        "validity_status": md.validity_status,
    }
    md.missing.extend(f for f in FIELDS if not values[f])
    return md


def fill_missing(md: types_pb2.DocMetadata, g: Guess, vocab: Vocabularies) -> types_pb2.DocMetadata:
    """用模型抽取的结果补齐 missing 里的字段：只补缺的，置信度不足或按词表归一不上的仍记为缺失。"""
    out = types_pb2.DocMetadata()
    out.CopyFrom(md)

    def sure(f: str) -> bool:
        return f in md.missing and g.confidence.get(f, 0.0) >= MIN_CONFIDENCE

    if sure("doc_type") and g.doc_type:
        out.doc_type = vocab.doc_type.exact(g.doc_type)
    if sure("dept") and g.dept:
        out.dept = vocab.dept.exact(g.dept)
    if sure("topic_codes"):
        out.topic_codes.extend(c for t in g.topics if (c := vocab.topic.exact(t)))
    if sure("effective_date") and g.effective_date:
        try:
            out.effective_date = dt.date.fromisoformat(g.effective_date).isoformat()
        except ValueError:
            pass
    if sure("validity_status") and g.validity_status in _STATUS:
        out.validity_status = _STATUS[g.validity_status]
    values = {
        "doc_type": out.doc_type,
        "dept": out.dept,
        "topic_codes": list(out.topic_codes),
        "effective_date": out.effective_date,
        "validity_status": out.validity_status,
    }
    del out.missing[:]
    out.missing.extend(f for f in FIELDS if not values[f])
    return out


def title_key(title: str) -> str:
    """规范化标题：去掉末尾的版本、年份括注与首尾空白（与 indexing 的 TitleKey 一致）。"""
    return _TITLE_SUFFIX.sub("", title.strip()).strip()


def supersedes(text: str, source: list[str]) -> list[str]:
    """这份文档声明替代的文档（入库管道详设第 7 节效力信息）：源系统字段给的 doc_id 优先，
    再加正文里“《…》同时废止”的标题；indexing 在同一数据源里按它们关联被替代的文档。"""
    out = [f"doc:{d}" for d in source]
    for m in _REPEALS.finditer(text):
        for t in _TITLE_IN.findall(m.group(0)):
            out.append(f"title:{title_key(t)}")
    seen: list[str] = []
    for t in out:
        if t not in seen:
            seen.append(t)
    return seen[:MAX_SUPERSEDES]


def _effective_date(text: str) -> str:
    m = _EFFECTIVE.search(text)
    if not m:
        return ""
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
    except ValueError:
        return ""
