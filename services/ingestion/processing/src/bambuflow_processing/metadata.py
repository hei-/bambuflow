"""文档级受控元数据与效力信息（入库管道详设第 7 节，枚举与汇总类问题详设第 4 节）。

来源优先级：源系统的结构化字段 → 标题与正文规则；模型抽取在后续步骤加入。取到的说法按受控词表
（governance 下发的快照）归一成编码，归一不上或取不到的字段记入 missing，不猜：
枚举与计数时由 metadata_gaps 披露，而不是把文档悄悄算进或漏出某一类。
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping
from typing import Any

from bambuflow_vocabulary import Vocabularies

from bambuflow.common.v1 import types_pb2

FIELDS = ("doc_type", "dept", "topic_codes", "effective_date", "validity_status")

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

    values = {
        "doc_type": md.doc_type,
        "dept": md.dept,
        "topic_codes": list(md.topic_codes),
        "effective_date": md.effective_date,
        "validity_status": md.validity_status,
    }
    md.missing.extend(f for f in FIELDS if not values[f])
    return md


def _effective_date(text: str) -> str:
    m = _EFFECTIVE.search(text)
    if not m:
        return ""
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
    except ValueError:
        return ""
