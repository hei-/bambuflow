"""指标与门禁结论（技术设计文档 10.2，评测详设 5.2、6.1）。只用规则计算，不调用模型。"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

# 核心指标下降超过 2 个点即不通过（技术设计文档 10.3）
MAX_DROP = 0.02
# 待复核题超过 10% 时结论为无法判定（评测详设 3.3）
MAX_PENDING = 0.10
CORE = ("recall_at_10", "citation_coverage", "groundedness", "status_accuracy")
# 设计门槛（技术设计文档 10.2），报告里对照展示；评测集审核前不作为门禁
TARGETS = {"groundedness": 0.9, "citation_coverage": 0.95, "refusal_accuracy": 0.9}


@dataclass(frozen=True)
class Outcome:
    """一道题一次运行的结果。"""

    qid: str
    tenant: str  # 提问身份所属租户：门禁按租户对照基线（评测详设 5.1）
    bucket: str
    expected_status: str
    status: str
    must_cite: tuple[str, ...]
    top10: tuple[str, ...]
    cited: tuple[str, ...]
    # 每句是否带引用、是否有支撑
    sentence_refs: tuple[bool, ...]
    sentence_grounded: tuple[bool, ...]
    pending: bool
    violations: tuple[str, ...]
    latency_ms: float

    def signature(self) -> tuple[object, ...]:
        """不含延迟的结果指纹：同一评测配置重跑时应完全相同。"""
        return (
            self.status,
            tuple(sorted(self.cited)),
            self.top10,
            self.sentence_refs,
            self.sentence_grounded,
        )


def _ratio(num: int, den: int) -> float | None:
    return num / den if den else None


def _percentile(xs: Sequence[float], p: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    return s[max(0, math.ceil(p * len(s)) - 1)]


def compute(outcomes: Sequence[Outcome]) -> dict[str, float | None]:
    live = [o for o in outcomes if not o.pending]
    with_cite = [o for o in live if o.must_cite]
    recall = (
        sum(len(set(o.must_cite) & set(o.top10)) / len(o.must_cite) for o in with_cite)
        / len(with_cite)
        if with_cite
        else None
    )
    sentences = sum(len(o.sentence_refs) for o in live)
    refuse = [o for o in live if o.expected_status != "answered"]
    answer = [o for o in live if o.expected_status == "answered"]
    latencies = [o.latency_ms for o in outcomes]
    return {
        "recall_at_10": recall,
        "citation_coverage": _ratio(sum(sum(o.sentence_refs) for o in live), sentences),
        "groundedness": _ratio(sum(sum(o.sentence_grounded) for o in live), sentences),
        "status_accuracy": _ratio(sum(o.status == o.expected_status for o in live), len(live)),
        "refusal_accuracy": _ratio(sum(o.status == o.expected_status for o in refuse), len(refuse)),
        "false_refusal_rate": _ratio(sum(o.status != "answered" for o in answer), len(answer)),
        "latency_p50_ms": _percentile(latencies, 0.5),
        "latency_p95_ms": _percentile(latencies, 0.95),
    }


@dataclass(frozen=True)
class Verdict:
    conclusion: str  # passed / failed / indeterminate
    reasons: tuple[str, ...]
    # 三次运行间结果不同的题（真实 LLM 的随机性）：只作分析参考，不影响结论（evaluation 详设 4.3）
    unstable: tuple[str, ...] = ()


def average(runs: Sequence[Sequence[Outcome]]) -> dict[str, float | None]:
    """各次运行的指标取平均；延迟按全部运行的样本计算分位数。"""
    per = [compute(r) for r in runs]
    out: dict[str, float | None] = {}
    for k in per[0]:
        vals = [v for m in per if (v := m[k]) is not None]
        out[k] = sum(vals) / len(vals) if vals else None
    out |= {
        k: v for k, v in compute([o for r in runs for o in r]).items() if k.startswith("latency")
    }
    return out


def _regressions(
    metrics: dict[str, float | None], baseline: dict[str, float | None] | None
) -> list[str]:
    if not baseline:
        return []
    out = []
    for k in CORE:
        cur, base = metrics.get(k), baseline.get(k)
        if cur is not None and base is not None and base - cur > MAX_DROP:
            out.append(f"{k} 从基线 {base:.3f} 降到 {cur:.3f}，超过 2 个点")
    return out


def by_tenant(runs: Sequence[Sequence[Outcome]]) -> dict[str, list[list[Outcome]]]:
    """按租户拆开每次运行的结果。"""
    out: dict[str, list[list[Outcome]]] = {}
    for t in sorted({o.tenant for o in runs[0]}):
        out[t] = [[o for o in run if o.tenant == t] for run in runs]
    return out


def tenant_metrics(runs: Sequence[Sequence[Outcome]]) -> dict[str, dict[str, float | None]]:
    return {t: average(sub) for t, sub in by_tenant(runs).items()}


def _run_regressions(
    run: Sequence[Outcome],
    baseline: dict[str, float | None] | None,
    tenant_baseline: dict[str, dict[str, float | None]] | None,
) -> list[str]:
    """一次运行对照基线的退步。基线带租户指标时逐租户比较（各租户的评测集各自是基线），
    否则比较整体；基线里没有的租户（新加入）不比较。"""
    if not tenant_baseline:
        return _regressions(compute(run), baseline)
    out = []
    for t, sub in by_tenant([run]).items():
        out += [f"{t}：{r}" for r in _regressions(compute(sub[0]), tenant_baseline.get(t))]
    return out


def judge(
    runs: Sequence[Sequence[Outcome]],
    baseline: dict[str, float | None] | None,
    tenant_baseline: dict[str, dict[str, float | None]] | None = None,
) -> Verdict:
    """每次运行各自对照基线下结论，三次结论一致才算可复现（evaluation 详设 4.3）。
    越权在任何一次出现都是失败，权限题不按多数取舍；待复核题超过 10% 时无法判定。
    核心指标按租户对照基线，任一租户下降超过 2 个点即不通过（评测详设 5.1）。"""
    first = runs[0]
    unstable = tuple(
        o.qid
        for i, o in enumerate(first)
        if any(run[i].signature() != o.signature() for run in runs[1:])
    )
    violations = sorted({v for run in runs for o in run for v in o.violations})
    if violations:
        return Verdict("failed", (f"越权 {len(violations)} 块：{', '.join(violations)}",), unstable)
    pending = sum(o.pending for o in first)
    if first and pending / len(first) > MAX_PENDING:
        return Verdict("indeterminate", (f"待复核题 {pending}/{len(first)} 超过 10%",), unstable)
    per_run = [_run_regressions(r, baseline, tenant_baseline) for r in runs]
    outcomes = {bool(r) for r in per_run}
    if len(outcomes) > 1:
        failing = sum(bool(r) for r in per_run)
        return Verdict(
            "indeterminate", (f"{len(runs)} 次运行中 {failing} 次低于基线，结论不一致",), unstable
        )
    if per_run[0]:
        return Verdict("failed", tuple(per_run[0]), unstable)
    return Verdict("passed", (), unstable)
