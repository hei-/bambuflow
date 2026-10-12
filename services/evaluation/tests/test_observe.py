import asyncio
import datetime as dt
import uuid

import grpc

from bambuflow.answer.v1 import answer_pb2, records_pb2
from bambuflow.config.v1 import rollout_control_pb2
from bambuflow_evaluation import observe, service

START = dt.datetime(2026, 10, 11, 8, 0, tzinfo=dt.UTC)
LATER = START + dt.timedelta(minutes=40)
ANSWERED = answer_pb2.STATUS_ANSWERED
REFUSED = answer_pb2.STATUS_REFUSED


def rollout(percent: int = 10, release_id: str = "rel_a") -> rollout_control_pb2.ActiveRollout:
    ro = rollout_control_pb2.ActiveRollout(
        release_id=release_id,
        object="pipeline",
        scope="credit",
        version_id="cfv_" + "1" * 24,
        tenant="credit",
        percent=percent,
    )
    ro.started_at.FromDatetime(START)
    return ro


def many(
    n: int,
    canary: bool,
    status: answer_pb2.Status = ANSWERED,
    coverage: float = 0.9,
    latency: int = 1000,
    denied: int = 0,
) -> list[records_pb2.AnswerRecord]:
    return [
        records_pb2.AnswerRecord(
            answer_id=f"ans_{uuid.uuid4().hex}",
            status=status,
            coverage=coverage,
            latency_ms=latency,
            access_denied=denied,
            canary_releases=["rel_a"] if canary else ["rel_other"],
        )
        for _ in range(n)
    ]


def judge(records: list[records_pb2.AnswerRecord], now: dt.datetime) -> observe.Observation:
    ro = rollout()
    return observe.judge(ro, *observe.split(ro, records, None), now)


def test_promote_needs_full_window_and_enough_answers() -> None:
    healthy = many(600, True) + many(3000, False)
    # 样本够但观察期未满
    assert judge(healthy, START + dt.timedelta(minutes=20)).verdict == observe.CONTINUE
    got = judge(healthy, LATER)
    assert got.verdict == observe.PROMOTE and got.summary.startswith("候选 600 次、对照 3000 次")
    # 观察期满但候选组不到 500 次（低流量租户按请求数）
    assert judge(many(300, True) + many(3000, False), LATER).verdict == observe.CONTINUE


def test_breaches_abort_without_waiting() -> None:
    early = START + dt.timedelta(minutes=10)
    cases = {
        "引用覆盖率": many(150, True, coverage=0.7),
        "拒答率": many(130, True) + many(20, True, status=REFUSED),
        "越权拦截": many(140, True) + many(10, True, denied=1),
        "p95": many(150, True, latency=2000),
    }
    for name, treated in cases.items():
        got = judge(treated + many(1000, False), early)
        assert got.verdict == observe.ABORT and got.summary.startswith(name), name
        assert len(got.summary) <= 500
    # 两组太少不判定：波动会误停
    assert judge(many(50, True, latency=9000) + many(1000, False), early).verdict == (
        observe.CONTINUE
    )


def test_coverage_only_counts_answered() -> None:
    # 拒答的回答没有覆盖率，不应拉低候选组的覆盖率
    treated = many(500, True) + many(5, True, status=REFUSED, coverage=0.0)
    assert judge(treated + many(1000, False), LATER).verdict == observe.PROMOTE


def test_whole_tenant_rollout_compares_with_the_day_before() -> None:
    ro = rollout(percent=100)
    (since, until), baseline = observe.windows(ro, LATER)
    assert (since, until) == (START, LATER)
    assert baseline == (START - dt.timedelta(hours=24), START)
    # 整个租户都在灰度里：对照组取灰度开始前的回答，不用同时段（同时段的都在灰度里）
    t, c = observe.split(ro, many(600, True), many(2000, False, latency=500))
    assert (t.answers, c.answers) == (600, 2000)
    assert observe.judge(ro, t, c, LATER).verdict == observe.ABORT
    assert observe.windows(rollout(), LATER)[1] is None


class FakeControl:
    def __init__(self, rollouts: list[rollout_control_pb2.ActiveRollout]) -> None:
        self.rollouts = rollouts
        self.reports: list[rollout_control_pb2.ReportObservationRequest] = []
        self.gone: set[str] = set()

    async def ListRollouts(
        self, request: rollout_control_pb2.ListRolloutsRequest, *, timeout: float
    ) -> rollout_control_pb2.ListRolloutsResponse:
        return rollout_control_pb2.ListRolloutsResponse(rollouts=self.rollouts)

    async def ReportObservation(
        self, request: rollout_control_pb2.ReportObservationRequest, *, timeout: float
    ) -> rollout_control_pb2.ReportObservationResponse:
        if request.release_id in self.gone:
            raise grpc.aio.AioRpcError(
                grpc.StatusCode.FAILED_PRECONDITION,
                grpc.aio.Metadata(),
                grpc.aio.Metadata(),
                "发布不在灰度中",
            )
        self.reports.append(request)
        return rollout_control_pb2.ReportObservationResponse()


class FakeRecords:
    """按时段返回记录：灰度开始后的与开始前的。"""

    def __init__(
        self,
        during: list[records_pb2.AnswerRecord],
        before: list[records_pb2.AnswerRecord] | None = None,
    ) -> None:
        self.during, self.before = during, before or []
        self.requests: list[records_pb2.ListAnswerRecordsRequest] = []

    async def ListAnswerRecords(
        self, request: records_pb2.ListAnswerRecordsRequest, *, timeout: float
    ) -> records_pb2.ListAnswerRecordsResponse:
        self.requests.append(request)
        got = self.during if request.since.ToDatetime(dt.UTC) >= START else self.before
        return records_pb2.ListAnswerRecordsResponse(records=got)


def test_observe_reports_each_rollout_and_skips_ended_ones() -> None:
    pl, gl = rollout(), rollout(percent=100, release_id="rel_b")
    control = FakeControl([pl, gl])
    records = FakeRecords(many(600, True) + many(3000, False))
    concluded = asyncio.run(service.observe_rollouts(control, records, LATER))
    # rel_a 健康、观察期满：全量；rel_b 整个租户都在灰度里，开始前没有回答：对照组为空，继续观察
    assert concluded == ["rel_a"]
    verdicts = {r.release_id: r.verdict for r in control.reports}
    assert verdicts == {"rel_a": observe.PROMOTE, "rel_b": observe.CONTINUE}
    assert all(r.version_id == pl.version_id for r in control.reports)
    assert {r.tenant for r in records.requests} == {"credit"}
    assert not any(r.experiment_id for r in records.requests)
    # 这期间有人全量或终止了：跳过，继续下一个
    control = FakeControl([pl, gl])
    control.gone = {"rel_a"}
    assert asyncio.run(service.observe_rollouts(control, records, LATER)) == []
    assert [r.release_id for r in control.reports] == ["rel_b"]
