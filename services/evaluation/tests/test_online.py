import asyncio
import datetime as dt
import json
from collections.abc import Sequence

import grpc
import pytest

from bambuflow.admin.v1 import quality_trend_pb2, quality_trend_pb2_grpc
from bambuflow.answer.v1 import answer_pb2, records_pb2
from bambuflow.identity.v1 import principal_pb2
from bambuflow_evaluation import audit, llm_judge, online
from bambuflow_evaluation.online import Daily
from bambuflow_platform.grpc_server import AuthInterceptor
from bambuflow_platform.principal import JWKS, Verifier

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


class MemAudit:
    def __init__(self, fail: bool = False) -> None:
        self.keys: list[str] = []
        self.bodies: list[bytes] = []
        self.fail = fail

    def put(self, key: str, body: bytes) -> None:
        if self.fail:
            raise OSError("审计桶不可用")
        self.keys.append(key)
        self.bodies.append(body)


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
    logs = MemAudit()
    out = asyncio.run(
        online.run_day(
            stub,
            OverlapNLI(),
            store,
            ["credit", "risk"],
            DAY,
            audit.Writer(logs, now=lambda: dt.datetime(2026, 10, 7, 1, tzinfo=dt.UTC)),
        )
    )
    assert [d.tenant for d in out] == ["credit", "risk"] and out[0].groundedness == 1.0
    req = stub.requests[0]
    assert req.include_answer and req.since.ToDatetime(dt.UTC).date() == DAY
    assert (req.until.ToDatetime(dt.UTC) - req.since.ToDatetime(dt.UTC)) == dt.timedelta(days=1)
    assert set(store.rows) == {("credit", DAY), ("risk", DAY)}
    # 读每个租户的回答原文之前各写一条访问审计
    rows = [json.loads(b) for b in logs.bodies]
    assert [(r["actor"], r["action"], r["object_id"]) for r in rows] == [
        ("service:evaluation", "answer_records.read", "credit"),
        ("service:evaluation", "answer_records.read", "risk"),
    ]
    assert all(k.startswith("evaluation/changes/2026/10/07/") for k in logs.keys)


def test_run_day_skips_tenant_when_audit_unavailable() -> None:
    good = answer([("额度不超过 30 万元。[S1]", ["单户额度不超过 30 万元。"])])
    store, stub = MemStore(), FakeRecords([record(1, good)])
    trail = audit.Writer(MemAudit(fail=True))
    out = asyncio.run(online.run_day(stub, OverlapNLI(), store, ["credit"], DAY, trail))
    # 审计写不进去就不读回答原文
    assert out == [] and stub.requests == [] and store.rows == {}


class RoleVerifier(Verifier):
    """令牌原文就是调用者的角色。"""

    def __init__(self) -> None:
        super().__init__(JWKS("http://unused"), operator=True)

    def verify(self, raw: str) -> principal_pb2.Principal:
        return principal_pb2.Principal(sub="u", roles=[raw])


def test_trends_owner_sees_only_own_tenant() -> None:
    async def main() -> None:
        server = grpc.aio.server(interceptors=[AuthInterceptor(RoleVerifier())])
        quality_trend_pb2_grpc.add_QualityTrendServiceServicer_to_server(
            online.TrendService(MemStore()), server
        )
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
                stub = quality_trend_pb2_grpc.QualityTrendServiceStub(ch)

                async def call(
                    role: str, tenant: str
                ) -> quality_trend_pb2.GetQualityTrendsResponse:
                    req = quality_trend_pb2.GetQualityTrendsRequest(tenant=tenant)
                    md = (("authorization", "Bearer " + role),)
                    resp: quality_trend_pb2.GetQualityTrendsResponse = await stub.GetQualityTrends(
                        req, metadata=md, timeout=5
                    )
                    return resp

                assert (await call("tenant_owner:credit", "credit")).tenant == "credit"
                for role, tenant in (("tenant_owner:credit", "ops"), ("auditor", "credit")):
                    with pytest.raises(grpc.aio.AioRpcError) as e:
                        await call(role, tenant)
                    assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED
        finally:
            await server.stop(None)

    asyncio.run(main())


class SometimesJudge:
    """第二条回答评审失败；其余按回答里有没有“不”判忠实与否。"""

    version = "judge-x+judge@1"

    def __init__(self) -> None:
        self.n = 0

    async def judge(
        self, question: str, answer: str, quotes: Sequence[str], key_points: Sequence[str]
    ) -> llm_judge.Judgment:
        self.n += 1
        if self.n == 2:
            raise llm_judge.JudgeError("评审模型不可用")
        return llm_judge.Judgment("不" not in answer)


def test_online_faithfulness_counts_only_judged_answers() -> None:
    answers = [
        answer([("额度 30 万元。[S1]", ["额度 30 万元"])]),
        answer([("期限 5 年。[S1]", ["期限 5 年"])]),
        answer([("不需要担保。[S1]", ["需要担保"])]),
    ]
    for a in answers:
        a.answer = "".join(s.text for s in a.sentences)
    d = online.Daily("credit", DAY, 3, 3, 1.0, 1.0, "nli")
    got = asyncio.run(online.faithfulness(d, answers, SometimesJudge()))
    assert (
        got.faithfulness_judged == 2
        and got.faithfulness == 0.5
        and got.llm_judge == "judge-x+judge@1"
    )
