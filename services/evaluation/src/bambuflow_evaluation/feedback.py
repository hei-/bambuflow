"""用户反馈回流评估（技术设计文档 F8，evaluation 详设 3.4、7.1）。

- 聚合：每天按租户统计全部回答数与收到“有用”“有误”“引用不对”的回答数，
  写进在线质量的日记录（online.py）。
- 负反馈案例：被标为“有误”或“引用不对”的回答登记为待处理案例，
  只存掩码后的问题、反馈与当时引用的文档，不存用户身份；
  业务专家据此出候选题（origin=feedback，走候选题复核）后标为已转题，或驳回。
  同一回答重复登记时只更新反馈，不改处理状态；处理过的案例不会因为又收到反馈而重新打开。

案例存在 evaluation 自有的 schema（evaluation.feedback_cases）。
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

import psycopg

from bambuflow.answer.v1 import answer_pb2, records_pb2

NEGATIVE = frozenset({"wrong", "bad_citation"})
RESOLUTIONS = ("converted", "dismissed")
STATUSES = ("open", *RESOLUTIONS)
LIST_LIMIT = 200

SCHEMA = """
CREATE SCHEMA IF NOT EXISTS evaluation;
CREATE TABLE IF NOT EXISTS evaluation.feedback_cases (
    answer_id       text        PRIMARY KEY,
    tenant          text        NOT NULL,
    answered_at     timestamptz NOT NULL,
    masked_question text        NOT NULL,
    answer_status   text        NOT NULL,
    ratings         text[]      NOT NULL,
    comments        text[]      NOT NULL,
    cited_doc_ids   text[]      NOT NULL,
    cited_titles    text[]      NOT NULL,
    status          text        NOT NULL DEFAULT 'open'
                                CHECK (status IN ('open', 'converted', 'dismissed')),
    resolved_by     text        NOT NULL DEFAULT '',
    resolution_note text        NOT NULL DEFAULT '',
    resolved_at     timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS feedback_cases_tenant
    ON evaluation.feedback_cases (tenant, status, answered_at DESC);
"""

_STATUS = {
    answer_pb2.STATUS_ANSWERED: "answered",
    answer_pb2.STATUS_EVIDENCE_ONLY: "evidence_only",
    answer_pb2.STATUS_INSUFFICIENT_EVIDENCE: "insufficient_evidence",
    answer_pb2.STATUS_REFUSED: "refused",
}


@dataclass(frozen=True)
class Counts:
    answers: int = 0
    useful: int = 0
    wrong: int = 0
    bad_citation: int = 0


def count(records: Iterable[records_pb2.AnswerRecord]) -> Counts:
    answers = useful = wrong = bad = 0
    for r in records:
        answers += 1
        fb = set(r.feedback)
        useful += "useful" in fb
        wrong += "wrong" in fb
        bad += "bad_citation" in fb
    return Counts(answers, useful, wrong, bad)


@dataclass(frozen=True)
class Case:
    answer_id: str
    tenant: str
    answered_at: dt.datetime
    masked_question: str
    answer_status: str
    ratings: tuple[str, ...]
    comments: tuple[str, ...] = ()
    cited_doc_ids: tuple[str, ...] = ()
    cited_titles: tuple[str, ...] = ()
    status: str = "open"
    resolved_by: str = ""
    resolution_note: str = ""
    resolved_at: dt.datetime | None = None


def cases(tenant: str, records: Iterable[records_pb2.AnswerRecord]) -> list[Case]:
    """收到负反馈的回答。没有掩码问题的记录（导出时没带 include_answer）不登记。"""
    out = []
    for r in records:
        ratings = tuple(x for x in r.feedback if x in NEGATIVE)
        if not ratings or not r.masked_question:
            continue
        docs: dict[str, str] = {}
        for c in r.answer.citations:
            docs.setdefault(c.doc_id, c.title)
        out.append(
            Case(
                answer_id=r.answer_id,
                tenant=tenant,
                answered_at=r.created_at.ToDatetime(dt.UTC),
                masked_question=r.masked_question,
                answer_status=_STATUS.get(r.status, "unspecified"),
                ratings=ratings,
                comments=tuple(r.feedback_comments),
                cited_doc_ids=tuple(docs),
                cited_titles=tuple(docs.values()),
            )
        )
    return out


class Store(Protocol):
    async def upsert(self, cases: list[Case]) -> None: ...

    async def list(self, tenant: str, status: str) -> list[Case]: ...

    async def resolve(
        self, tenant: str, answer_id: str, resolution: str, by: str, note: str
    ) -> Case | None: ...


def _case(r: tuple[object, ...]) -> Case:
    def strs(v: object) -> tuple[str, ...]:
        return tuple(str(x) for x in v) if isinstance(v, list) else ()

    resolved = r[12]
    answered = r[2]
    if not isinstance(answered, dt.datetime):
        raise TypeError("answered_at 应为时间")
    return Case(
        answer_id=str(r[0]),
        tenant=str(r[1]),
        answered_at=answered,
        masked_question=str(r[3]),
        answer_status=str(r[4]),
        ratings=strs(r[5]),
        comments=strs(r[6]),
        cited_doc_ids=strs(r[7]),
        cited_titles=strs(r[8]),
        status=str(r[9]),
        resolved_by=str(r[10]),
        resolution_note=str(r[11]),
        resolved_at=resolved if isinstance(resolved, dt.datetime) else None,
    )


class PGStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    async def migrate(self) -> None:
        async with await psycopg.AsyncConnection.connect(self._dsn, autocommit=True) as conn:
            await conn.execute(SCHEMA.encode())

    async def upsert(self, cases: list[Case]) -> None:
        if not cases:
            return
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            for c in cases:
                await conn.execute(
                    """INSERT INTO evaluation.feedback_cases
                         (answer_id, tenant, answered_at, masked_question, answer_status, ratings,
                          comments, cited_doc_ids, cited_titles)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (answer_id) DO UPDATE SET
                         ratings = EXCLUDED.ratings, comments = EXCLUDED.comments""",
                    (
                        c.answer_id,
                        c.tenant,
                        c.answered_at,
                        c.masked_question,
                        c.answer_status,
                        list(c.ratings),
                        list(c.comments),
                        list(c.cited_doc_ids),
                        list(c.cited_titles),
                    ),
                )

    async def list(self, tenant: str, status: str) -> list[Case]:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            cur = await conn.execute(
                """SELECT answer_id, tenant, answered_at, masked_question, answer_status, ratings,
                          comments, cited_doc_ids, cited_titles, status, resolved_by,
                          resolution_note, resolved_at
                     FROM evaluation.feedback_cases WHERE tenant = %s AND status = %s
                    ORDER BY answered_at DESC LIMIT %s""",
                (tenant, status, LIST_LIMIT),
            )
            return [_case(r) for r in await cur.fetchall()]

    async def resolve(
        self, tenant: str, answer_id: str, resolution: str, by: str, note: str
    ) -> Case | None:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            cur = await conn.execute(
                """UPDATE evaluation.feedback_cases
                      SET status = %s, resolved_by = %s, resolution_note = %s, resolved_at = now()
                    WHERE tenant = %s AND answer_id = %s AND status = 'open'
                RETURNING answer_id, tenant, answered_at, masked_question, answer_status, ratings,
                          comments, cited_doc_ids, cited_titles, status, resolved_by,
                          resolution_note, resolved_at""",
                (resolution, by, note, tenant, answer_id),
            )
            row = await cur.fetchone()
            return _case(row) if row else None
