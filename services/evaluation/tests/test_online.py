import asyncio
import datetime as dt
from collections.abc import Sequence

from bambuflow.answer.v1 import answer_pb2, records_pb2
from bambuflow_evaluation import online
from bambuflow_evaluation.online import Daily

DAY = dt.date(2026, 10, 6)


def answer(sentences: list[tuple[str, list[str]]]) -> answer_pb2.AnswerResponse:
    a = answer_pb2.AnswerResponse(status=answer_pb2.STATUS_ANSWERED)
    for text, quotes in sentences:
        s = a.sentences.add(text=text, claim_type=answer_pb2.CLAIM_TYPE_QUOTE)
        for i, q in enumerate(quotes, 1):
            s.quotes.add(ref_id=i, quote=q)
    return a


def record(
    i: int,
    ans: answer_pb2.AnswerResponse | None,
    status: answer_pb2.Status = answer_pb2.STATUS_ANSWERED,
) -> records_pb2.AnswerRecord:
    r = records_pb2.AnswerRecord(answer_id=f"ans_{i}", status=status, body_withheld=ans is None)
    if ans is not None:
        r.answer.CopyFrom(ans)
    return r


class OverlapNLI:
    """支持度 = 句子里出现在引文中的字的比例。"""

    async def support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        return [sum(ch in p for ch in h) / len(h) for p, h in pairs], "judge@t"


def test_sample_skips_withheld_and_unanswered_and_is_stable() -> None:
    good = answer([("额度不超过 30 万元。[S1]", ["单户额度不超过 30 万元。"])])
    records = [record(i, good) for i in range(10)] + [
        record(100, None),
        record(101, good, answer_pb2.STATUS_INSUFFICIENT_EVIDENCE),
        record(102, answer([("没有引文的句子。", [])])),
    ]
    a = online.sample(DAY, records, n=5)
    assert len(a) == 5 and online.sample(DAY, list(reversed(records)), n=5) == a
    assert len(online.sample(DAY, records)) == 10


def test_judge_groundedness_and_citation_precision() -> None:
    a = answer(
        [
            ("额度不超过 30 万元。[S1]", ["单户额度不超过 30 万元。", "请假须提前申请。"]),
            ("所有客户免息三年。[S1]", ["单户额度不超过 30 万元。"]),
        ]
    )
    d = asyncio.run(online.judge("credit", DAY, [a], OverlapNLI()))
    assert d.sampled == 1 and d.sentences == 2 and d.judge_model == "judge@t"
    # 第一句有一条引文支持；第二句没有
    assert d.groundedness == 0.5
    # 三条引文里只有一条支持所在句子
    assert abs(d.citation_precision - 1 / 3) < 1e-9


def daily(day: dt.date, g: float) -> Daily:
    return Daily("credit", day, 10, 20, g, g, "judge@t")


def test_trend_flags_decline_of_more_than_three_points() -> None:
    today = DAY + dt.timedelta(days=1)
    days = [daily(DAY - dt.timedelta(days=i), 0.95) for i in range(7, 30)]
    days += [daily(DAY - dt.timedelta(days=i), 0.88) for i in range(7)]
    t = online.trend(days, today)
    assert abs(t.avg_7d - 0.88) < 1e-9 and t.declining
    steady = online.trend([daily(DAY - dt.timedelta(days=i), 0.9) for i in range(30)], today)
    assert not steady.declining
    # 数据不足 8 天不告警
    assert not online.trend(days[-7:], today).declining


class FakeRecords:
    def __init__(self, records: list[records_pb2.AnswerRecord]) -> None:
        self.records = records
        self.requests: list[records_pb2.ListAnswerRecordsRequest] = []

    async def ListAnswerRecords(
        self, request: records_pb2.ListAnswerRecordsRequest, *, timeout: float
    ) -> records_pb2.ListAnswerRecordsResponse:
        self.requests.append(request)
        return records_pb2.ListAnswerRecordsResponse(records=self.records)


class MemStore:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, dt.date], Daily] = {}

    async def put(self, d: Daily) -> None:
        self.rows[(d.tenant, d.day)] = d

    async def days(self, tenant: str, since: dt.date) -> list[Daily]:
        return [d for (t, day), d in self.rows.items() if t == tenant and day >= since]


def test_run_day_writes_each_tenant_and_reads_the_whole_day() -> None:
    good = answer([("额度不超过 30 万元。[S1]", ["单户额度不超过 30 万元。"])])
    store, stub = MemStore(), FakeRecords([record(1, good)])
    out = asyncio.run(online.run_day(stub, OverlapNLI(), store, ["credit", "risk"], DAY))
    assert [d.tenant for d in out] == ["credit", "risk"] and out[0].groundedness == 1.0
    req = stub.requests[0]
    assert req.include_answer and req.since.ToDatetime(dt.UTC).date() == DAY
    assert (req.until.ToDatetime(dt.UTC) - req.since.ToDatetime(dt.UTC)) == dt.timedelta(days=1)
    assert set(store.rows) == {("credit", DAY), ("risk", DAY)}
