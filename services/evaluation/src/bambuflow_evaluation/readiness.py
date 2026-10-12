"""租户评测集是否满足分阶段接入的要求（评测详设 3.1，评估报告 K13）。

- 试点：≥ 100 题，应拒答 ≥ 15%，合成题 ≤ 50%；
- 全量：≥ 300 题，应拒答 ≥ 15%，合成题 ≤ 50%，双人标注题 ≥ 150。

只计已审核的题：合成题（LLM 生成）要有一人审核，其余来源要有审核人；
没有审核人的题（如样例评测集里 llm_generated_unreviewed 的题）只用于回归比较，不计入题量。
“双人标注”指有两位不同审核人的非合成题。
切到全量时 governance 要求门禁里这个租户的阶段已是 general
（题量与占比都是建议值，按首批租户的标注速度复核）。
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from bambuflow_evaluation.golden import Question

SYNTHETIC = frozenset({"llm_generated", "llm_generated_unreviewed"})
PILOT_MIN, GENERAL_MIN, DOUBLE_MIN = 100, 300, 150
REFUSE_MIN, SYNTHETIC_MAX = 0.15, 0.5


@dataclass(frozen=True)
class Readiness:
    tenant: str
    # none / pilot / general：满足的最高阶段
    stage: str
    counted: int
    unreviewed: int
    refuse_share: float
    synthetic_share: float
    double_reviewed: int
    # 达到下一个阶段还差什么
    gaps: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        return {
            "tenant": self.tenant,
            "stage": self.stage,
            "counted": self.counted,
            "unreviewed": self.unreviewed,
            "refuse_share": round(self.refuse_share, 3),
            "synthetic_share": round(self.synthetic_share, 3),
            "double_reviewed": self.double_reviewed,
            "gaps": list(self.gaps),
        }


def _reviewed(q: Question) -> bool:
    return len(q.reviewers) >= 1


def assess(tenant: str, questions: Iterable[Question]) -> Readiness:
    mine = [q for q in questions if q.asker.tenant == tenant]
    counted = [q for q in mine if _reviewed(q)]
    n = len(counted)
    refuse = sum(q.expected_status != "answered" for q in counted) / n if n else 0.0
    synthetic = sum(q.origin in SYNTHETIC for q in counted) / n if n else 0.0
    double = sum(q.origin not in SYNTHETIC and len(q.reviewers) >= 2 for q in counted)
    shares_ok = refuse >= REFUSE_MIN and synthetic <= SYNTHETIC_MAX

    def gaps(minimum: int, need_double: bool) -> list[str]:
        out = []
        if n < minimum:
            out.append(f"已审核题 {n} 道，至少 {minimum} 道")
        if refuse < REFUSE_MIN:
            out.append(f"应拒答占比 {refuse:.0%}，至少 {REFUSE_MIN:.0%}")
        if synthetic > SYNTHETIC_MAX:
            out.append(f"合成题占比 {synthetic:.0%}，至多 {SYNTHETIC_MAX:.0%}")
        if need_double and double < DOUBLE_MIN:
            out.append(f"双人标注题 {double} 道，至少 {DOUBLE_MIN} 道")
        return out

    if n >= GENERAL_MIN and double >= DOUBLE_MIN and shares_ok:
        stage, missing = "general", []
    elif n >= PILOT_MIN and shares_ok:
        stage, missing = "pilot", gaps(GENERAL_MIN, True)
    else:
        stage, missing = "none", gaps(PILOT_MIN, False)
    return Readiness(
        tenant=tenant,
        stage=stage,
        counted=n,
        unreviewed=len(mine) - n,
        refuse_share=refuse,
        synthetic_share=synthetic,
        double_reviewed=double,
        gaps=tuple(missing),
    )
