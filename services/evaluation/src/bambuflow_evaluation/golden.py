"""读取评测集（golden/<集合>/questions.yaml 与 askers.yaml，题目结构见评测详设 3.2）。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Asker:
    """提问身份模板：以它签发 Principal，不依赖某个真实员工的当前属性（评测详设 3.3）。"""

    name: str
    sub: str
    tenant: str
    dept: str
    clearance: int
    groups: tuple[str, ...]


@dataclass(frozen=True)
class Question:
    id: str
    bucket: str
    asker: Asker
    question: str
    expected_status: str
    key_points: tuple[str, ...]
    must_cite: tuple[str, ...]
    must_not_cite: tuple[str, ...]
    notes: str = ""


@dataclass(frozen=True)
class GoldenSet:
    name: str
    version: str
    provenance: dict[str, Any]
    questions: list[Question]


def _strs(v: Any) -> tuple[str, ...]:
    if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
        raise ValueError(f"应为字符串列表: {v!r}")
    return tuple(v)


def load(path: Path) -> GoldenSet:
    askers_raw = yaml.safe_load((path / "askers.yaml").read_text(encoding="utf-8"))
    askers = {
        name: Asker(name, a["sub"], a["tenant"], a["dept"], int(a["clearance"]), _strs(a["groups"]))
        for name, a in askers_raw.items()
    }
    doc = yaml.safe_load((path / "questions.yaml").read_text(encoding="utf-8"))
    questions = []
    seen: set[str] = set()
    for q in doc["questions"]:
        if q["id"] in seen:
            raise ValueError(f"题目 id 重复: {q['id']}")
        seen.add(q["id"])
        exp = q["expected"]
        questions.append(
            Question(
                id=q["id"],
                bucket=q["bucket"],
                asker=askers[q["asker"]],
                question=q["question"],
                expected_status=exp["status"],
                key_points=_strs(exp.get("key_points", [])),
                must_cite=_strs(exp.get("must_cite", [])),
                must_not_cite=_strs(exp.get("must_not_cite", [])),
                notes=str(q.get("notes", "")),
            )
        )
    return GoldenSet(doc["tenant_set"], str(doc["version"]), doc["provenance_default"], questions)


def _doc(chunk_id: str) -> str:
    """chunk_id 的形式是 {数据源}:{doc_id}#{锚点}，取出 doc_id。"""
    return chunk_id.split(":", 1)[1].split("#", 1)[0]


def review_sheet(gs: GoldenSet) -> str:
    """审核清单：业务人员逐题确认问题、期望状态、要点与必引来源（技术设计文档 10.4 标注流程）。"""
    out = [
        f"# 评测集审核清单：{gs.name} {gs.version}",
        "",
        "逐题确认：问题是否像真实员工会问的；期望状态是否正确；要点是否完整且能在必引文档里找到；",
        "应拒答题的理由是否成立。有问题的在“意见”列写明，通过的勾选。",
        "",
        "| 审核 | 题目 | 提问身份 | 问题 | 期望 | 要点 | 必引文档 | 不得出现 | 备注 | 意见 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for q in gs.questions:
        cite = ", ".join(sorted({_doc(c) for c in q.must_cite})) or "—"
        avoid = ", ".join(sorted({_doc(c) for c in q.must_not_cite})) or "—"
        out.append(
            f"| [ ] | {q.id} | {q.asker.name} | {q.question} | {q.expected_status} | "
            f"{'；'.join(q.key_points) or '—'} | {cite} | {avoid} | {q.notes or '—'} |  |"
        )
    return "\n".join(out) + "\n"
