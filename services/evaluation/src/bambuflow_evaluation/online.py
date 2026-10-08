"""在线抽样评审（evaluation 详设 7.1）：每天按租户从回答记录中抽样，算 Groundedness 与引用精确率。

- 抽样：前一天（UTC）的 answered 回答，每租户至多 200 条（建议值），
  按 answer_id 与日期的哈希排序取前若干条，同一天重跑抽到的是同一批。
  涉及受限级数据、正文未保存的回答导出时就没有正文，不会被抽到。
- 判定：每句与它自己的引文做 NLI，判定模型与线上校验器不同（evaluation 详设 6.1）；
  没有标准答案，不算正确率。Faithfulness 要 LLM 评审，本期不算。
- 趋势：7 天均值比 30 天均值低 3 个点以上算下滑，记告警日志（按日志告警），
  查询接口同样返回 declining。

结果存在 evaluation 自有的 schema（evaluation.online_quality），按租户与日期覆盖写入。
"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

import grpc
import psycopg

from bambuflow.admin.v1 import quality_trend_pb2, quality_trend_pb2_grpc
from bambuflow.answer.v1 import answer_pb2, records_pb2
from bambuflow_evaluation.service import CALL_TIMEOUT, PLATFORM_ADMIN, Ctx, Records, abort
from bambuflow_grounding import GroundingUnavailableError
from bambuflow_platform.grpc_server import current_principal

_log = logging.getLogger(__name__)

SAMPLE_SIZE = 200
SUPPORT_THRESHOLD = 0.5
DECLINE_POINTS = 0.03
MARKER = re.compile(r"\[S\d+\]")

SCHEMA = """
CREATE SCHEMA IF NOT EXISTS evaluation;
CREATE TABLE IF NOT EXISTS evaluation.online_quality (
    tenant             text             NOT NULL,
    day                date             NOT NULL,
    sampled            integer          NOT NULL,
    sentences          integer          NOT NULL,
    groundedness       double precision NOT NULL,
    citation_precision double precision NOT NULL,
    judge_model        text             NOT NULL,
    computed_at        timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant, day)
);
"""


class NLI(Protocol):
    async def support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]: ...


@dataclass(frozen=True)
class Daily:
    tenant: str
    day: dt.date
    sampled: int
    sentences: int
    groundedness: float
    citation_precision: float
    judge_model: str
    computed_at: dt.datetime | None = None


def _rank(day: dt.date, answer_id: str) -> str:
    return hashlib.sha256(f"{day.isoformat()}\x00{answer_id}".encode()).hexdigest()


def sample(
    day: dt.date, records: Sequence[records_pb2.AnswerRecord], n: int = SAMPLE_SIZE
) -> list[answer_pb2.AnswerResponse]:
    """可评审的回答（有正文、answered、有带引文的陈述），按稳定的哈希顺序取前 n 条。"""
    usable = [
        r
        for r in records
        if r.HasField("answer")
        and r.status == answer_pb2.STATUS_ANSWERED
        and any(s.quotes for s in r.answer.sentences)
    ]
    usable.sort(key=lambda r: _rank(day, r.answer_id))
    return [r.answer for r in usable[:n]]


async def judge(
    tenant: str, day: dt.date, answers: Sequence[answer_pb2.AnswerResponse], nli: NLI
) -> Daily:
    """逐句与自己的引文做 NLI。

    句子取各引文支持度的最大值（Groundedness），引文逐条计（引用精确率）。"""
    pairs: list[tuple[str, str]] = []
    owners: list[int] = []
    sentences = 0
    for a in answers:
        for s in a.sentences:
            if s.claim_type == answer_pb2.CLAIM_TYPE_NONE or not s.quotes:
                continue
            text = MARKER.sub("", s.text).strip()
            for q in s.quotes:
                pairs.append((q.quote, text))
                owners.append(sentences)
            sentences += 1
    model = ""
    best = [0.0] * sentences
    supported = 0
    if pairs:
        scores, model = await nli.support(pairs)
        for i, sc in zip(owners, scores, strict=True):
            best[i] = max(best[i], sc)
            supported += sc >= SUPPORT_THRESHOLD
    return Daily(
        tenant=tenant,
        day=day,
        sampled=len(answers),
        sentences=sentences,
        groundedness=sum(b >= SUPPORT_THRESHOLD for b in best) / sentences if sentences else 0.0,
        citation_precision=supported / len(pairs) if pairs else 0.0,
        judge_model=model,
    )


async def fetch_day(stub: Records, tenant: str, day: dt.date) -> list[records_pb2.AnswerRecord]:
    start = dt.datetime.combine(day, dt.time(), dt.UTC)
    out: list[records_pb2.AnswerRecord] = []
    token = ""
    while True:
        req = records_pb2.ListAnswerRecordsRequest(
            tenant=tenant, include_answer=True, page_token=token
        )
        req.since.FromDatetime(start)
        req.until.FromDatetime(start + dt.timedelta(days=1))
        resp = await stub.ListAnswerRecords(req, timeout=CALL_TIMEOUT)
        out.extend(resp.records)
        token = resp.next_page_token
        if not token:
            return out


@dataclass(frozen=True)
class Trend:
    days: list[Daily]
    avg_7d: float
    avg_30d: float

    @property
    def declining(self) -> bool:
        # 两段都要有数据才比较：刚开始抽样的头几天不告警
        return len(self.days) >= 8 and self.avg_7d < self.avg_30d - DECLINE_POINTS


def trend(days: Sequence[Daily], today: dt.date) -> Trend:
    def avg(n: int) -> float:
        xs = [d.groundedness for d in days if today - dt.timedelta(days=n) <= d.day < today]
        return sum(xs) / len(xs) if xs else 0.0

    return Trend(sorted(days, key=lambda d: d.day), avg(7), avg(30))


class Store(Protocol):
    async def put(self, d: Daily) -> None: ...

    async def days(self, tenant: str, since: dt.date) -> list[Daily]: ...


class PGStore:
    """evaluation 自有 schema 的读写；每次操作单独连接（每天一轮写入，查询量很小）。"""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    async def migrate(self) -> None:
        async with await psycopg.AsyncConnection.connect(self._dsn, autocommit=True) as conn:
            await conn.execute(SCHEMA.encode())

    async def put(self, d: Daily) -> None:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            await conn.execute(
                """INSERT INTO evaluation.online_quality
                     (tenant, day, sampled, sentences, groundedness, citation_precision,
                      judge_model)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (tenant, day) DO UPDATE SET
                     sampled = EXCLUDED.sampled, sentences = EXCLUDED.sentences,
                     groundedness = EXCLUDED.groundedness,
                     citation_precision = EXCLUDED.citation_precision,
                     judge_model = EXCLUDED.judge_model, computed_at = now()""",
                (
                    d.tenant,
                    d.day,
                    d.sampled,
                    d.sentences,
                    d.groundedness,
                    d.citation_precision,
                    d.judge_model,
                ),
            )

    async def days(self, tenant: str, since: dt.date) -> list[Daily]:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            cur = await conn.execute(
                """SELECT tenant, day, sampled, sentences, groundedness, citation_precision,
                          judge_model, computed_at
                     FROM evaluation.online_quality WHERE tenant = %s AND day >= %s
                    ORDER BY day""",
                (tenant, since),
            )
            return [Daily(*row) for row in await cur.fetchall()]


async def run_day(
    stub: Records, nli: NLI, store: Store, tenants: Sequence[str], day: dt.date
) -> list[Daily]:
    """评审一天的抽样并写入；返回各租户结果。某个租户失败只记日志，继续下一个。"""
    out: list[Daily] = []
    for tenant in tenants:
        try:
            records = await fetch_day(stub, tenant, day)
            d = await judge(tenant, day, sample(day, records), nli)
            await store.put(d)
            days = await store.days(tenant, day - dt.timedelta(days=29))
        except (grpc.aio.AioRpcError, psycopg.Error, GroundingUnavailableError) as e:
            _log.warning("在线抽样评审失败", extra={"tenant": tenant, "err": type(e).__name__})
            continue
        out.append(d)
        t = trend(days, day + dt.timedelta(days=1))
        if t.declining:
            _log.warning(
                "在线质量趋势下滑",
                extra={
                    "tenant": tenant,
                    "groundedness_7d": t.avg_7d,
                    "groundedness_30d": t.avg_30d,
                },
            )
    return out


TENANT = re.compile(r"^[a-z][a-z0-9-]{0,62}$")


class TrendService(quality_trend_pb2_grpc.QualityTrendServiceServicer):
    def __init__(self, store: Store) -> None:
        self._store = store

    async def GetQualityTrends(
        self, request: quality_trend_pb2.GetQualityTrendsRequest, context: Ctx
    ) -> quality_trend_pb2.GetQualityTrendsResponse:
        p = current_principal()
        if p is None or PLATFORM_ADMIN not in p.roles:
            await abort(context, grpc.StatusCode.PERMISSION_DENIED, "需要平台管理员")
        n = request.days or 30
        if not TENANT.match(request.tenant) or not 1 <= n <= 90:
            await abort(context, grpc.StatusCode.INVALID_ARGUMENT, "tenant 或 days 不合法")
        today = dt.datetime.now(dt.UTC).date()
        try:
            days = await self._store.days(request.tenant, today - dt.timedelta(days=max(n, 30)))
        except psycopg.Error as e:
            _log.warning("读取在线质量趋势失败", extra={"err": type(e).__name__})
            await abort(context, grpc.StatusCode.UNAVAILABLE, "暂时不可用")
        t = trend(days, today + dt.timedelta(days=1))
        out = quality_trend_pb2.GetQualityTrendsResponse(
            tenant=request.tenant,
            groundedness_7d=t.avg_7d,
            groundedness_30d=t.avg_30d,
            declining=t.declining,
        )
        for d in t.days:
            if d.day < today - dt.timedelta(days=n - 1):
                continue
            q = out.days.add(
                day=d.day.isoformat(),
                sampled=d.sampled,
                sentences=d.sentences,
                groundedness=d.groundedness,
                citation_precision=d.citation_precision,
                judge_model=d.judge_model,
            )
            if d.computed_at is not None:
                q.computed_at.FromDatetime(d.computed_at)
        return out
