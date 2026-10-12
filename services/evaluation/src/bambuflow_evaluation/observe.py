"""灰度观察（governance 详设 4.1 第 ⑨ 步）：比较灰度的候选组与对照组，给出继续、全量或终止的结论。

候选组是提问人落在这次灰度里的回答（why.canary_releases，按用户分桶、与处理路径无关）。
对照组：按比例分流的灰度（租户对象，该租户 10% 的用户）取同一时段、同一租户里不在灰度的回答；
整个租户都在灰度里（全局对象对试点租户）时没有同时段的对照，取灰度开始前 24 小时的回答。

比较引用覆盖率、拒答率、越权拦截（返回前复核拦下证据的回答占比）与 p95 耗时。
两组都够 GUARD_MIN 次即开始判定，超阈值立即终止，不等观察期满
（宁可误停，也不让有问题的配置多影响用户）；全量要观察满 30 分钟、
候选组至少 MIN_ANSWERS 次（低流量租户按请求数而不是时间，governance 详设第 12 节待决问题 1）。

纯计算，不做 I/O，便于单元测试。
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass, field

from bambuflow.answer.v1 import answer_pb2, records_pb2
from bambuflow.config.v1 import rollout_control_pb2
from bambuflow_evaluation import abtest

# 建议值（governance 详设 4.1、第 12 节）
OBSERVE = dt.timedelta(minutes=30)
MIN_ANSWERS = 500
# 两组都达到这个数才判定是否超阈值，样本太少时波动会误停
GUARD_MIN = 100
# 整个租户都在灰度里时，对照组取灰度开始前这么长时间的回答
BASELINE = dt.timedelta(hours=24)
# 候选组覆盖率比对照组低 5 个点以上、越权拦截占比高 1 个点以上算超阈值；拒答率与 p95 与 A/B 护栏相同
COVERAGE_DROP = 0.05
DENIED_MARGIN = 0.01

CONTINUE = rollout_control_pb2.VERDICT_CONTINUE
PROMOTE = rollout_control_pb2.VERDICT_PROMOTE
ABORT = rollout_control_pb2.VERDICT_ABORT


@dataclass
class Arm:
    answers: int = 0
    refused: int = 0
    answered: int = 0
    coverage_sum: float = 0.0
    denied: int = 0
    latencies: list[int] = field(default_factory=list)

    @classmethod
    def of(cls, records: Iterable[records_pb2.AnswerRecord]) -> Arm:
        a = cls()
        for r in records:
            a.answers += 1
            a.refused += r.status == answer_pb2.STATUS_REFUSED
            # 覆盖率只对给出了回答的有意义
            if r.status == answer_pb2.STATUS_ANSWERED:
                a.answered += 1
                a.coverage_sum += r.coverage
            a.denied += r.access_denied > 0
            a.latencies.append(r.latency_ms)
        return a

    @property
    def refused_rate(self) -> float:
        return self.refused / self.answers if self.answers else 0.0

    @property
    def coverage(self) -> float:
        return self.coverage_sum / self.answered if self.answered else 0.0

    @property
    def denied_rate(self) -> float:
        return self.denied / self.answers if self.answers else 0.0

    @property
    def p95(self) -> int:
        return abtest.p95(self.latencies)


def windows(
    ro: rollout_control_pb2.ActiveRollout, now: dt.datetime
) -> tuple[tuple[dt.datetime, dt.datetime], tuple[dt.datetime, dt.datetime] | None]:
    """候选组读取的时段，以及单独的对照时段（只在整个租户都在灰度里时有）。"""
    start = ro.started_at.ToDatetime(dt.UTC)
    baseline = (start - BASELINE, start) if ro.percent >= 100 else None
    return (start, now), baseline


def split(
    ro: rollout_control_pb2.ActiveRollout,
    during: Iterable[records_pb2.AnswerRecord],
    baseline: Iterable[records_pb2.AnswerRecord] | None,
) -> tuple[Arm, Arm]:
    treated: list[records_pb2.AnswerRecord] = []
    others: list[records_pb2.AnswerRecord] = []
    for r in during:
        (treated if ro.release_id in r.canary_releases else others).append(r)
    return Arm.of(treated), Arm.of(others if baseline is None else baseline)


@dataclass(frozen=True)
class Observation:
    verdict: rollout_control_pb2.Verdict
    # 两组样本数与各项指标（终止时前面加上超阈值的项），governance 记在灰度上并写进审计
    summary: str


def _breaches(t: Arm, c: Arm) -> list[str]:
    out: list[str] = []
    if t.answered and c.answered and t.coverage < c.coverage - COVERAGE_DROP:
        out.append(f"引用覆盖率 {t.coverage:.2f} 低于对照 {c.coverage:.2f}")
    if t.refused_rate > c.refused_rate + abtest.REFUSED_MARGIN:
        out.append(f"拒答率 {t.refused_rate:.3f} 高于对照 {c.refused_rate:.3f}")
    if t.denied_rate > c.denied_rate + DENIED_MARGIN:
        out.append(f"越权拦截占比 {t.denied_rate:.3f} 高于对照 {c.denied_rate:.3f}")
    if t.p95 > c.p95 * abtest.LATENCY_RATIO:
        out.append(f"p95 耗时 {t.p95} ms 高于对照 {c.p95} ms")
    return out


def judge(ro: rollout_control_pb2.ActiveRollout, t: Arm, c: Arm, now: dt.datetime) -> Observation:
    summary = (
        f"候选 {t.answers} 次、对照 {c.answers} 次；覆盖率 {t.coverage:.2f}/{c.coverage:.2f}；"
        f"拒答率 {t.refused_rate:.3f}/{c.refused_rate:.3f}；"
        f"越权拦截 {t.denied_rate:.3f}/{c.denied_rate:.3f}；p95 {t.p95}/{c.p95} ms"
    )
    if min(t.answers, c.answers) < GUARD_MIN:
        return Observation(CONTINUE, summary)
    if breaches := _breaches(t, c):
        return Observation(ABORT, "；".join(breaches) + "（" + summary + "）")
    elapsed = now - ro.started_at.ToDatetime(dt.UTC)
    if elapsed >= OBSERVE and t.answers >= MIN_ANSWERS:
        return Observation(PROMOTE, summary)
    return Observation(CONTINUE, summary)
