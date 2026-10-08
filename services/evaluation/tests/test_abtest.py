import asyncio
import datetime as dt
import uuid

from bambuflow.admin.v1 import experiment_admin_pb2
from bambuflow.answer.v1 import answer_pb2, records_pb2
from bambuflow.config.v1 import experiment_control_pb2
from bambuflow_evaluation import abtest, service

CONTROL = answer_pb2.EXPERIMENT_ARM_CONTROL
TREATMENT = answer_pb2.EXPERIMENT_ARM_TREATMENT


def experiment(
    metric: experiment_admin_pb2.PrimaryMetric = abtest.USEFUL, sample: int = 300
) -> experiment_admin_pb2.Experiment:
    e = experiment_admin_pb2.Experiment(
        exp_id="exp_1", tenant="credit", primary_metric=metric, sample_size_per_arm=sample
    )
    e.created_at.FromDatetime(dt.datetime(2026, 10, 1, tzinfo=dt.UTC))
    return e


def rec(
    user: str,
    arm: answer_pb2.ExperimentArm,
    useful: bool | None = None,
    status: answer_pb2.Status = answer_pb2.STATUS_ANSWERED,
    latency: int = 1000,
    tokens: int = 500,
    exp_id: str = "exp_1",
) -> records_pb2.AnswerRecord:
    r = records_pb2.AnswerRecord(
        answer_id=f"ans_{uuid.uuid4().hex}",
        user_key=user,
        experiment_id=exp_id,
        experiment_arm=arm,
        status=status,
        latency_ms=latency,
        usage=answer_pb2.TokenUsage(prompt_tokens=tokens),
    )
    if useful is not None:
        r.feedback.append("useful" if useful else "wrong")
    return r


def population(
    n: int, arm: answer_pb2.ExperimentArm, useful_rate: float, seed: int
) -> list[records_pb2.AnswerRecord]:
    out: list[records_pb2.AnswerRecord] = []
    for i in range(n):
        # 确定的“伪随机”：(i * 37 + seed) % 100 在 0–99 上均匀分布
        useful = (i * 37 + seed) % 100 < useful_rate * 100
        # 每个用户问两次：显著性按用户算，不按请求算
        for _ in range(2):
            out.append(rec(f"{arm}-u{i}", arm, useful))
    return out


def test_no_conclusion_before_sample_size() -> None:
    a = abtest.analyze(
        experiment(sample=300),
        population(100, CONTROL, 0.6, 1) + population(100, TREATMENT, 0.8, 2),
    )
    assert 0.3 < a.progress < 0.35
    assert not a.HasField("conclusion")
    assert a.control.users == 100 and a.control.answers == 200


def test_conclusion_detects_real_improvement() -> None:
    a = abtest.analyze(
        experiment(sample=300),
        population(400, CONTROL, 0.6, 1) + population(400, TREATMENT, 0.75, 2),
    )
    assert a.progress == 1.0
    c = a.conclusion
    assert c.significant and c.improved and c.p_value < 0.01
    assert c.ci_low < c.delta < c.ci_high and 0.05 < c.delta < 0.25


def test_no_difference_is_not_significant() -> None:
    a = abtest.analyze(
        experiment(sample=300),
        population(400, CONTROL, 0.6, 3) + population(400, TREATMENT, 0.6, 4),
    )
    assert not a.conclusion.significant


def test_insufficient_rate_lower_is_better_and_excluded_ignored() -> None:
    records = [
        rec(
            f"c{i}",
            CONTROL,
            status=answer_pb2.STATUS_INSUFFICIENT_EVIDENCE if i % 2 else answer_pb2.STATUS_ANSWERED,
        )
        for i in range(400)
    ] + [
        rec(
            f"t{i}",
            TREATMENT,
            status=answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
            if i % 5 == 0
            else answer_pb2.STATUS_ANSWERED,
        )
        for i in range(400)
    ]
    records += [rec("x", answer_pb2.EXPERIMENT_ARM_EXCLUDED), rec("y", TREATMENT, exp_id="exp_2")]
    a = abtest.analyze(experiment(abtest.INSUFFICIENT, sample=300), records)
    assert a.treatment.answers == 400 and a.control.metric_mean == 0.5
    assert a.conclusion.delta < 0 and a.conclusion.improved


def test_guardrails_need_enough_answers() -> None:
    slow = [rec(f"t{i}", TREATMENT, latency=3000, tokens=900) for i in range(150)]
    refused = [rec(f"r{i}", TREATMENT, status=answer_pb2.STATUS_REFUSED) for i in range(20)]
    base = [rec(f"c{i}", CONTROL) for i in range(150)]
    a = abtest.analyze(experiment(), base + slow + refused)
    names = {g.name for g in a.guardrails if g.breached}
    assert names == {"refused_rate", "p95_latency_ms", "mean_tokens"}
    assert abtest.breached(a) and "refused_rate" in abtest.breached(a)[0]
    few = abtest.analyze(experiment(), base[:50] + slow[:50])
    assert not any(g.breached for g in few.guardrails)


class FakeControl:
    def __init__(self, running: list[experiment_admin_pb2.Experiment]) -> None:
        self.running = running
        self.stopped: list[experiment_control_pb2.StopForGuardrailRequest] = []

    async def GetExperiment(
        self, request: experiment_control_pb2.GetExperimentRequest, *, timeout: float
    ) -> experiment_control_pb2.GetExperimentResponse:
        return experiment_control_pb2.GetExperimentResponse(experiment=self.running[0])

    async def ListRunningExperiments(
        self, request: experiment_control_pb2.ListRunningExperimentsRequest, *, timeout: float
    ) -> experiment_control_pb2.ListRunningExperimentsResponse:
        return experiment_control_pb2.ListRunningExperimentsResponse(experiments=self.running)

    async def StopForGuardrail(
        self, request: experiment_control_pb2.StopForGuardrailRequest, *, timeout: float
    ) -> experiment_control_pb2.StopForGuardrailResponse:
        self.stopped.append(request)
        return experiment_control_pb2.StopForGuardrailResponse()


class FakeRecords:
    """按页返回固定的记录，记下每次请求的时间窗口。"""

    def __init__(self, records: list[records_pb2.AnswerRecord], page: int = 100) -> None:
        self.records, self.page = records, page
        self.windows: list[tuple[dt.datetime, dt.datetime]] = []

    async def ListAnswerRecords(
        self, request: records_pb2.ListAnswerRecordsRequest, *, timeout: float
    ) -> records_pb2.ListAnswerRecordsResponse:
        window = (request.since.ToDatetime(dt.UTC), request.until.ToDatetime(dt.UTC))
        if window not in self.windows:
            self.windows.append(window)
        if len(self.windows) > 1:
            return records_pb2.ListAnswerRecordsResponse()
        start = int(request.page_token or 0)
        out = records_pb2.ListAnswerRecordsResponse(records=self.records[start : start + self.page])
        if start + self.page < len(self.records):
            out.next_page_token = str(start + self.page)
        return out


def test_guard_stops_breached_experiments_and_reads_all_pages_and_windows() -> None:
    records = [rec(f"c{i}", CONTROL) for i in range(150)] + [
        rec(f"t{i}", TREATMENT, latency=5000) for i in range(150)
    ]
    control, store = FakeControl([experiment()]), FakeRecords(records)
    now = dt.datetime(2026, 11, 20, tzinfo=dt.UTC)  # 实验已跑 50 天：分两个 31 天窗口读
    stopped = asyncio.run(service.guard(control, store, now))
    assert stopped == ["exp_1"] and "p95_latency_ms" in control.stopped[0].reason
    assert len(store.windows) == 2 and store.windows[-1][1] == now
    assert store.windows[0][1] - store.windows[0][0] == dt.timedelta(days=31)


def test_guard_leaves_healthy_experiments_running() -> None:
    records = [rec(f"c{i}", CONTROL) for i in range(150)] + [
        rec(f"t{i}", TREATMENT) for i in range(150)
    ]
    control = FakeControl([experiment()])
    assert asyncio.run(service.guard(control, FakeRecords(records), dt.datetime.now(dt.UTC))) == []
    assert control.stopped == []
