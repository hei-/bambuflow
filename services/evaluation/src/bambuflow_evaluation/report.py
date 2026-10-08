"""评测报告（评测详设 5.2）：评测配置、结论、指标对照、分桶与逐题明细。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from bambuflow_evaluation.golden import GoldenSet
from bambuflow_evaluation.metrics import CORE, TARGETS, Outcome, Verdict, tenant_metrics

LABELS = {
    "recall_at_10": "Recall@10",
    "citation_coverage": "引用覆盖率",
    "groundedness": "Groundedness",
    "status_accuracy": "状态正确率",
    "refusal_accuracy": "应拒答题正确拒答率",
    "false_refusal_rate": "误拒率",
    "latency_p50_ms": "Answer 延迟 p50（ms）",
    "latency_p95_ms": "Answer 延迟 p95（ms）",
}


def _fmt(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:.0f}" if v > 10 else f"{v:.3f}"


def as_json(
    golden: GoldenSet,
    config: dict[str, list[str]],
    judge_model: str,
    runs: Sequence[Sequence[Outcome]],
    metrics: dict[str, float | None],
    verdict: Verdict,
) -> dict[str, Any]:
    return {
        "golden": {"name": golden.name, "version": golden.version, "provenance": golden.provenance},
        "config": config,
        "judge": {"nli_model": judge_model},
        "repeats": len(runs),
        "conclusion": verdict.conclusion,
        "reasons": list(verdict.reasons),
        "unstable": list(verdict.unstable),
        "metrics": metrics,
        "tenant_metrics": tenant_metrics(runs),
    }


def markdown(
    golden: GoldenSet,
    config: dict[str, list[str]],
    judge_model: str,
    runs: Sequence[Sequence[Outcome]],
    metrics: dict[str, float | None],
    baseline: dict[str, float | None] | None,
    verdict: Verdict,
    tenant_baseline: dict[str, dict[str, float | None]] | None = None,
) -> str:
    first = runs[0]
    out = [
        f"# 评测报告：{golden.name} {golden.version}",
        "",
        f"**结论：{verdict.conclusion}**"
        + ("" if not verdict.reasons else "（" + "；".join(verdict.reasons) + "）"),
        "",
        "## 评测配置",
        "",
        f"- 评测集：`golden/{golden.name}` 版本 {golden.version}，{len(first)} 题；"
        f"来源 {golden.provenance.get('source')}，"
        f"审核人 {golden.provenance.get('reviewed_by') or '无（待审核）'}",
    ]
    for k, vs in sorted(config.items()):
        out.append(f"- {k}：{', '.join(sorted(vs))}")
    out += [
        f"- 判定器：NLI {judge_model or '—'}，支持度阈值 0.5",
        f"- 同一配置连跑 {len(runs)} 次，指标取平均；每次各自对照基线下结论，结论一致才算可复现",
        "",
        "## 指标",
        "",
        "| 指标 | 本次 | 基线 | 设计门槛 |",
        "| --- | --- | --- | --- |",
    ]
    for k, label in LABELS.items():
        base = _fmt(baseline.get(k)) if baseline else "—"
        target = f"≥ {TARGETS[k]}" if k in TARGETS else ("不低于基线" if k in CORE else "—")
        out.append(f"| {label} | {_fmt(metrics.get(k))} | {base} | {target} |")
    if verdict.unstable:
        out += [
            "",
            f"三次运行间结果不同的题（{len(verdict.unstable)} 道，只作分析参考）："
            + "、".join(verdict.unstable),
        ]
    tenant_base = tenant_baseline or {}
    out += ["", "## 按租户", "", "| 租户 | 题数 | " + " | ".join(LABELS[k] for k in CORE) + " |"]
    out.append("| --- | --- | " + " | ".join("---" for _ in CORE) + " |")
    for t, tm in tenant_metrics(runs).items():
        tb = tenant_base.get(t) or {}
        cells = [f"{_fmt(tm.get(k))}（基线 {_fmt(tb.get(k)) if tb else '—'}）" for k in CORE]
        n = sum(o.tenant == t for o in first)
        out.append(f"| {t} | {n} | " + " | ".join(cells) + " |")
    out += ["", "## 分桶", "", "| 分桶 | 题数 | 状态正确 |", "| --- | --- | --- |"]
    for bucket in sorted({o.bucket for o in first}):
        sub = [o for o in first if o.bucket == bucket]
        ok = sum(o.status == o.expected_status for o in sub)
        out.append(f"| {bucket} | {len(sub)} | {ok} |")
    out += [
        "",
        "## 逐题",
        "",
        "| 题目 | 期望 | 实际 | 必引进前 10 | 引用块数 | 有支撑句 / 总句 | 备注 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for o in first:
        hit = f"{len(set(o.must_cite) & set(o.top10))}/{len(o.must_cite)}" if o.must_cite else "—"
        note = (
            "待复核" if o.pending else ("越权：" + ", ".join(o.violations) if o.violations else "")
        )
        out.append(
            f"| {o.qid} | {o.expected_status} | {o.status} | {hit} | {len(set(o.cited))} | "
            f"{sum(o.sentence_grounded)}/{len(o.sentence_grounded)} | {note} |"
        )
    out.append("")
    return "\n".join(out)
