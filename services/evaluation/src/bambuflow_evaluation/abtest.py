"""A/B 分析（evaluation 详设 7.2，技术设计文档 10.5）。

输入是 orchestrator 导出的回答记录（AnswerRecordService），按用户聚合后比较两组：
同一用户的多次问答不独立，不能按请求算显著性。达到样本量之前只给进度、不给结论，避免提前偏看；
护栏（拒答率、p95 耗时、单次成本）每天检查，任一触发即请 governance 停止实验。
越权不在这里看：线上回答都过了返回前复核，越权由离线权限评测集保证。

纯计算，不做 I/O，便于单元测试。
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from bambuflow.admin.v1 import experiment_admin_pb2, experiment_analysis_pb2
from bambuflow.answer.v1 import answer_pb2, records_pb2

USEFUL = experiment_admin_pb2.PRIMARY_METRIC_USEFUL_RATE
INSUFFICIENT = experiment_admin_pb2.PRIMARY_METRIC_INSUFFICIENT_EVIDENCE_RATE

# 护栏的允许范围（建议值，部署配置清单）：实验组拒答率不高于对照组 2 个点，
# p95 耗时与单次成本不高于对照组 20%
REFUSED_MARGIN = 0.02
LATENCY_RATIO = 1.2
COST_RATIO = 1.2
# 两组回答数都达到这个数才判定护栏，样本太少时波动会误停实验
GUARD_MIN_ANSWERS = 100
Z_95 = 1.959964


@dataclass
class _Arm:
    answers: int = 0
    refused: int = 0
    tokens: int = 0

    def __post_init__(self) -> None:
        self.latencies: list[int] = []
        # 用户 → 主要指标的（分子, 分母）
        self.per_user: dict[str, list[int]] = defaultdict(lambda: [0, 0])


def p95(values: list[int]) -> int:
    if not values:
        return 0
    s = sorted(values)
    return s[min(len(s) - 1, math.ceil(0.95 * len(s)) - 1)]


def _user_values(arm: _Arm) -> list[float]:
    return [num / den for num, den in arm.per_user.values() if den > 0]


def _mean_var(xs: list[float]) -> tuple[float, float]:
    if not xs:
        return 0.0, 0.0
    m = sum(xs) / len(xs)
    if len(xs) < 2:
        return m, 0.0
    return m, sum((x - m) ** 2 for x in xs) / (len(xs) - 1)


def _summary(name: str, arm: _Arm) -> experiment_analysis_pb2.ArmSummary:
    values = _user_values(arm)
    mean, _ = _mean_var(values)
    return experiment_analysis_pb2.ArmSummary(
        arm=name,
        users=len(arm.per_user),
        answers=arm.answers,
        metric_users=len(values),
        metric_mean=mean,
        refused_rate=arm.refused / arm.answers if arm.answers else 0.0,
        p95_latency_ms=p95(arm.latencies),
        mean_tokens=arm.tokens / arm.answers if arm.answers else 0.0,
    )


def guardrails(
    control: experiment_analysis_pb2.ArmSummary, treatment: experiment_analysis_pb2.ArmSummary
) -> list[experiment_analysis_pb2.GuardrailCheck]:
    enough = min(control.answers, treatment.answers) >= GUARD_MIN_ANSWERS
    checks = [
        (
            "refused_rate",
            control.refused_rate,
            treatment.refused_rate,
            treatment.refused_rate > control.refused_rate + REFUSED_MARGIN,
        ),
        (
            "p95_latency_ms",
            float(control.p95_latency_ms),
            float(treatment.p95_latency_ms),
            treatment.p95_latency_ms > control.p95_latency_ms * LATENCY_RATIO,
        ),
        (
            "mean_tokens",
            control.mean_tokens,
            treatment.mean_tokens,
            treatment.mean_tokens > control.mean_tokens * COST_RATIO,
        ),
    ]
    return [
        experiment_analysis_pb2.GuardrailCheck(
            name=n, control=c, treatment=t, breached=enough and bad
        )
        for n, c, t, bad in checks
    ]


def _conclusion(
    metric: experiment_admin_pb2.PrimaryMetric, control: _Arm, treatment: _Arm
) -> experiment_analysis_pb2.Conclusion:
    mc, vc = _mean_var(_user_values(control))
    mt, vt = _mean_var(_user_values(treatment))
    nc, nt = len(_user_values(control)), len(_user_values(treatment))
    delta = mt - mc
    se = math.sqrt(vc / nc + vt / nt)
    if se == 0:
        p = 1.0 if delta == 0 else 0.0
    else:
        # 每组上千用户，Welch 检验的 t 分布与正态分布几乎一致，用正态近似
        p = math.erfc(abs(delta) / se / math.sqrt(2))
    return experiment_analysis_pb2.Conclusion(
        delta=delta,
        ci_low=delta - Z_95 * se,
        ci_high=delta + Z_95 * se,
        p_value=p,
        significant=p < 0.05,
        improved=delta > 0 if metric == USEFUL else delta < 0,
    )


def analyze(
    exp: experiment_admin_pb2.Experiment, records: Iterable[records_pb2.AnswerRecord]
) -> experiment_analysis_pb2.ExperimentAnalysis:
    arms = {answer_pb2.EXPERIMENT_ARM_CONTROL: _Arm(), answer_pb2.EXPERIMENT_ARM_TREATMENT: _Arm()}
    for r in records:
        arm = arms.get(r.experiment_arm)
        if arm is None or r.experiment_id != exp.exp_id:
            # excluded（受限级内容退回基线）与其他实验的记录不计入
            continue
        arm.answers += 1
        arm.refused += r.status == answer_pb2.STATUS_REFUSED
        arm.tokens += r.usage.prompt_tokens + r.usage.completion_tokens
        arm.latencies.append(r.latency_ms)
        user = arm.per_user[r.user_key]
        if exp.primary_metric == USEFUL:
            # 只看有反馈的回答：标过“有用”的算成功
            if r.feedback:
                user[0] += "useful" in r.feedback
                user[1] += 1
        else:
            user[0] += r.status == answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
            user[1] += 1
    control = _summary("control", arms[answer_pb2.EXPERIMENT_ARM_CONTROL])
    treatment = _summary("treatment", arms[answer_pb2.EXPERIMENT_ARM_TREATMENT])
    reached = min(control.metric_users, treatment.metric_users)
    out = experiment_analysis_pb2.ExperimentAnalysis(
        experiment=exp,
        control=control,
        treatment=treatment,
        progress=min(1.0, reached / exp.sample_size_per_arm) if exp.sample_size_per_arm else 0.0,
        guardrails=guardrails(control, treatment),
    )
    if exp.sample_size_per_arm and reached >= exp.sample_size_per_arm:
        out.conclusion.CopyFrom(
            _conclusion(
                exp.primary_metric,
                arms[answer_pb2.EXPERIMENT_ARM_CONTROL],
                arms[answer_pb2.EXPERIMENT_ARM_TREATMENT],
            )
        )
    return out


def breached(analysis: experiment_analysis_pb2.ExperimentAnalysis) -> list[str]:
    """触发的护栏，写进停止原因。"""
    return [
        f"{g.name} 实验组 {g.treatment:.4g}，对照组 {g.control:.4g}"
        for g in analysis.guardrails
        if g.breached
    ]
