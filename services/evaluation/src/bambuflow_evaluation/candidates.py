"""评测集题目的查看与候选题复核（evaluation 详设 3.4、8.2）。

评测集以仓库 golden/ 为准，这里只读镜像里那一版（BF_GOLDEN_DIR）。新题先作为候选题存进
evaluation.golden_candidates，业务专家（数据属主）与平台（平台管理员）各一人复核通过后为 accepted，
再由 python -m bambuflow_evaluation candidates 导出成 YAML 走合并请求加进 golden/：
评测集的变更与被评测的变更分开评审（evaluation 详设 3.4），服务不直接改仓库。
"""

from __future__ import annotations

import datetime as dt
import json
import re
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Protocol

import grpc
import psycopg
import yaml
from google.protobuf.any_pb2 import Any as Any_
from google.protobuf.timestamp_pb2 import Timestamp

from bambuflow.admin.v1 import golden_admin_pb2, golden_admin_pb2_grpc
from bambuflow_evaluation import golden
from bambuflow_evaluation.service import Ctx, abort
from bambuflow_platform.grpc_server import current_principal

PLATFORM_ADMIN, PLATFORM_OPERATOR, DATA_OWNER = "platform_admin", "platform_operator", "data_owner"
STATUSES = ("answered", "insufficient_evidence", "refused")
ORIGINS = ("expert", "feedback", "online_sample", "llm_generated")
TENANT = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
NAME = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
CANDIDATE_ID = re.compile(r"^cand_[0-9a-f]{24}$")
CHUNK_ID = re.compile(r"^src_[A-Za-z0-9_]+:[^#\s]{1,200}#[0-9a-f]{1,64}$")

SCHEMA = """
CREATE SCHEMA IF NOT EXISTS evaluation;
CREATE TABLE IF NOT EXISTS evaluation.golden_candidates (
    cand_id           text        PRIMARY KEY,
    tenant            text        NOT NULL,
    golden_set        text        NOT NULL,
    bucket            text        NOT NULL,
    asker             text        NOT NULL,
    question          text        NOT NULL,
    expected          jsonb       NOT NULL,
    origin            text        NOT NULL,
    status            text        NOT NULL CHECK (status IN ('pending', 'accepted', 'rejected')),
    submitted_by      text        NOT NULL,
    submitted_at      timestamptz NOT NULL,
    expert_reviewer   text        NOT NULL DEFAULT '',
    platform_reviewer text        NOT NULL DEFAULT '',
    reject_reason     text        NOT NULL DEFAULT '',
    CHECK (status <> 'accepted' OR (expert_reviewer <> '' AND platform_reviewer <> ''
        AND expert_reviewer <> platform_reviewer))
);
CREATE INDEX IF NOT EXISTS golden_candidates_tenant
    ON evaluation.golden_candidates (tenant, submitted_at);
"""


@dataclass(frozen=True)
class Candidate:
    cand_id: str
    tenant: str
    golden_set: str
    bucket: str
    asker: str
    question: str
    # status、key_points、must_cite、must_not_cite
    expected: dict[str, Any]
    origin: str
    submitted_by: str
    status: str = "pending"
    submitted_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.UTC))
    expert_reviewer: str = ""
    platform_reviewer: str = ""
    reject_reason: str = ""


class ReviewRefusedError(Exception):
    """复核不允许：reason 是对外的错误码（self_approval_forbidden、conflict）。"""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


def review(c: Candidate, who: str, roles: set[str], approved: bool, reason: str) -> Candidate:
    """一次复核：业务专家（数据属主）与平台（平台管理员）各占一席。

    同一人不能占两席，提交人不能复核。

    任一席驳回即 rejected；两席都通过为 accepted。"""
    if c.status != "pending":
        raise ReviewRefusedError("conflict", "候选题已经复核完")
    if who == c.submitted_by:
        raise ReviewRefusedError("self_approval_forbidden", "不能复核自己提交的题")
    if who in (c.expert_reviewer, c.platform_reviewer):
        raise ReviewRefusedError("self_approval_forbidden", "同一人不能占两个复核席位")
    if DATA_OWNER in roles and not c.expert_reviewer:
        c = replace(c, expert_reviewer=who)
    elif PLATFORM_ADMIN in roles and not c.platform_reviewer:
        c = replace(c, platform_reviewer=who)
    else:
        raise ReviewRefusedError("conflict", "你所在的复核席位已有人复核")
    if not approved:
        return replace(c, status="rejected", reject_reason=reason)
    if c.expert_reviewer and c.platform_reviewer:
        return replace(c, status="accepted")
    return c


class Store(Protocol):
    async def put(self, c: Candidate) -> None: ...

    async def list(self, tenant: str) -> list[Candidate]: ...

    async def update(
        self, cand_id: str, change: Callable[[Candidate], Candidate]
    ) -> Candidate | None: ...


def _row(r: tuple[Any, ...]) -> Candidate:
    return Candidate(
        cand_id=r[0],
        tenant=r[1],
        golden_set=r[2],
        bucket=r[3],
        asker=r[4],
        question=r[5],
        expected=dict(r[6]),
        origin=r[7],
        submitted_by=r[8],
        status=r[9],
        submitted_at=r[10],
        expert_reviewer=r[11],
        platform_reviewer=r[12],
        reject_reason=r[13],
    )


class PGStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    async def migrate(self) -> None:
        async with await psycopg.AsyncConnection.connect(self._dsn, autocommit=True) as conn:
            await conn.execute(SCHEMA.encode())

    async def put(self, c: Candidate) -> None:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            await conn.execute(
                """INSERT INTO evaluation.golden_candidates
                     (cand_id, tenant, golden_set, bucket, asker, question, expected, origin,
                      submitted_by, status, submitted_at,
                     expert_reviewer, platform_reviewer, reject_reason)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    c.cand_id,
                    c.tenant,
                    c.golden_set,
                    c.bucket,
                    c.asker,
                    c.question,
                    json.dumps(c.expected, ensure_ascii=False),
                    c.origin,
                    c.submitted_by,
                    c.status,
                    c.submitted_at,
                    c.expert_reviewer,
                    c.platform_reviewer,
                    c.reject_reason,
                ),
            )

    async def list(self, tenant: str) -> list[Candidate]:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            cur = await conn.execute(
                """SELECT cand_id, tenant, golden_set, bucket, asker, question, expected, origin,
                          submitted_by, status, submitted_at, expert_reviewer, platform_reviewer,
                          reject_reason
                     FROM evaluation.golden_candidates WHERE tenant = %s
                    ORDER BY submitted_at, cand_id""",
                (tenant,),
            )
            return [_row(r) for r in await cur.fetchall()]

    async def update(
        self, cand_id: str, change: Callable[[Candidate], Candidate]
    ) -> Candidate | None:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn, conn.transaction():
            cur = await conn.execute(
                """SELECT cand_id, tenant, golden_set, bucket, asker, question, expected, origin,
                          submitted_by, status, submitted_at, expert_reviewer, platform_reviewer,
                          reject_reason
                     FROM evaluation.golden_candidates WHERE cand_id = %s FOR UPDATE""",
                (cand_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            c = change(_row(row))
            await conn.execute(
                """UPDATE evaluation.golden_candidates SET status = %s, expert_reviewer = %s,
                     platform_reviewer = %s, reject_reason = %s WHERE cand_id = %s""",
                (c.status, c.expert_reviewer, c.platform_reviewer, c.reject_reason, cand_id),
            )
            return c


def golden_sets(root: Path) -> list[golden.GoldenSet]:
    """root 下的每个评测集（含 questions.yaml 与 askers.yaml 的目录）。"""
    return [
        golden.load(d)
        for d in sorted(root.iterdir())
        if (d / "questions.yaml").exists() and (d / "askers.yaml").exists()
    ]


def _ts(t: dt.datetime) -> Timestamp:
    out = Timestamp()
    out.FromDatetime(t)
    return out


def _expected(e: dict[str, Any]) -> golden_admin_pb2.GoldenExpected:
    return golden_admin_pb2.GoldenExpected(
        status=e.get("status", ""),
        key_points=e.get("key_points", []),
        must_cite=e.get("must_cite", []),
        must_not_cite=e.get("must_not_cite", []),
    )


def _candidate_proto(c: Candidate) -> golden_admin_pb2.GoldenQuestion:
    return golden_admin_pb2.GoldenQuestion(
        question_id=c.cand_id,
        golden_set=c.golden_set,
        bucket=c.bucket,
        asker=c.asker,
        question=c.question,
        expected=_expected(c.expected),
        status=c.status,
        origin=c.origin,
        submitted_by=c.submitted_by,
        submitted_at=_ts(c.submitted_at),
        expert_reviewer=c.expert_reviewer,
        platform_reviewer=c.platform_reviewer,
        reject_reason=c.reject_reason,
    )


def _strs(v: Any, n: int, size: int, pattern: re.Pattern[str] | None = None) -> bool:
    return (
        isinstance(v, list)
        and len(v) <= n
        and all(
            isinstance(x, str) and 0 < len(x) <= size and (pattern is None or pattern.match(x))
            for x in v
        )
    )


class GoldenService(golden_admin_pb2_grpc.GoldenSetAdminServiceServicer):
    def __init__(self, store: Store, sets: Callable[[], list[golden.GoldenSet]]) -> None:
        self._store, self._sets = store, sets

    async def _roles(self, context: Ctx, allowed: set[str]) -> tuple[str, set[str]]:
        p = current_principal()
        roles = set(p.roles) if p else set()
        if p is None or not roles & allowed:
            await abort(context, grpc.StatusCode.PERMISSION_DENIED, "没有权限")
        return p.sub, roles

    async def _tenant(self, context: Ctx, tenant: str) -> None:
        if not TENANT.match(tenant):
            await abort(context, grpc.StatusCode.INVALID_ARGUMENT, "tenant 不合法")

    async def ListQuestions(
        self, request: golden_admin_pb2.ListQuestionsRequest, context: Ctx
    ) -> golden_admin_pb2.ListQuestionsResponse:
        await self._roles(context, {PLATFORM_ADMIN, PLATFORM_OPERATOR, DATA_OWNER})
        await self._tenant(context, request.tenant)
        out = golden_admin_pb2.ListQuestionsResponse()
        for gs in self._sets():
            mine = [q for q in gs.questions if q.asker.tenant == request.tenant]
            if mine:
                out.golden_versions.append(f"{gs.name}@{gs.version}")
            for q in mine:
                out.questions.append(
                    golden_admin_pb2.GoldenQuestion(
                        question_id=q.id,
                        golden_set=gs.name,
                        bucket=q.bucket,
                        asker=q.asker.name,
                        question=q.question,
                        expected=_expected(
                            {
                                "status": q.expected_status,
                                "key_points": list(q.key_points),
                                "must_cite": list(q.must_cite),
                                "must_not_cite": list(q.must_not_cite),
                            }
                        ),
                        status="in_set",
                        origin=str(gs.provenance.get("source", "")),
                    )
                )
        if request.include_candidates:
            out.questions.extend(
                _candidate_proto(c) for c in await self._store.list(request.tenant)
            )
        return out

    async def SubmitCandidate(
        self, request: golden_admin_pb2.SubmitCandidateRequest, context: Ctx
    ) -> golden_admin_pb2.SubmitCandidateResponse:
        who, _ = await self._roles(context, {PLATFORM_ADMIN, DATA_OWNER})
        await self._tenant(context, request.tenant)
        sets = {gs.name: gs for gs in self._sets()}
        gs = sets.get(request.golden_set)
        asker = (
            next((q.asker for q in gs.questions if q.asker.name == request.asker), None)
            if gs
            else None
        )
        e = request.expected
        ok = (
            asker is not None
            and asker.tenant == request.tenant
            and NAME.match(request.bucket) is not None
            and 0 < len(request.question.strip()) <= 500
            and e.status in STATUSES
            and request.origin in ORIGINS
            and _strs(list(e.key_points), 20, 200)
            and _strs(list(e.must_cite), 20, 300, CHUNK_ID)
            and _strs(list(e.must_not_cite), 20, 300, CHUNK_ID)
        )
        if not ok:
            # 提问身份必须是该评测集里已有、且属于这个租户的模板
            await abort(
                context,
                grpc.StatusCode.INVALID_ARGUMENT,
                "golden_set、asker、bucket、question、expected 或 origin 不合法",
            )
        c = Candidate(
            cand_id="cand_" + secrets.token_hex(12),
            tenant=request.tenant,
            golden_set=request.golden_set,
            bucket=request.bucket,
            asker=request.asker,
            question=request.question.strip(),
            expected={
                "status": e.status,
                "key_points": list(e.key_points),
                "must_cite": list(e.must_cite),
                "must_not_cite": list(e.must_not_cite),
            },
            origin=request.origin,
            submitted_by=who,
        )
        await self._store.put(c)
        return golden_admin_pb2.SubmitCandidateResponse(question=_candidate_proto(c))

    async def ReviewCandidate(
        self, request: golden_admin_pb2.ReviewCandidateRequest, context: Ctx
    ) -> golden_admin_pb2.ReviewCandidateResponse:
        who, roles = await self._roles(context, {PLATFORM_ADMIN, DATA_OWNER})
        await self._tenant(context, request.tenant)
        reason = request.reason.strip()
        if (
            not CANDIDATE_ID.match(request.question_id)
            or len(reason) > 500
            or (not request.approved and not reason)
        ):
            await abort(context, grpc.StatusCode.INVALID_ARGUMENT, "question_id 或 reason 不合法")
        refused: list[ReviewRefusedError] = []

        def change(c: Candidate) -> Candidate:
            try:
                return review(c, who, roles, request.approved, reason)
            except ReviewRefusedError as err:
                refused.append(err)
                return c

        c = await self._store.update(request.question_id, change)
        if c is None or c.tenant != request.tenant:
            await abort(context, grpc.StatusCode.NOT_FOUND, "候选题不存在")
        if refused:
            err = refused[0]
            forbidden = err.reason == "self_approval_forbidden"
            code = (
                grpc.StatusCode.PERMISSION_DENIED
                if forbidden
                else grpc.StatusCode.FAILED_PRECONDITION
            )
            await context.abort(
                code, str(err), trailing_metadata=reason_metadata(code, str(err), err.reason)
            )
        return golden_admin_pb2.ReviewCandidateResponse(question=_candidate_proto(c))


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b, n = n & 0x7F, n >> 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _field(num: int, data: bytes) -> bytes:
    """长度前缀字段（wire type 2）。"""
    return _varint(num << 3 | 2) + _varint(len(data)) + data


def reason_metadata(code: grpc.StatusCode, msg: str, reason: str) -> tuple[tuple[str, bytes], ...]:
    """业务错误码按 google.rpc.Status 的 ErrorInfo 附在 grpc-status-details-bin 里，
    与 libs/platform-go/grpcmw 的 Reason 一致，gateway 据此转成对外的 problem type。

    googleapis-common-protos 没有类型声明（加的 stub 包与 types-grpcio 冲突），
    两个消息都很小，这里按 protobuf 编码直接拼：
    ErrorInfo{1: reason, 2: domain}，Status{1: code, 2: message, 3: details}。"""
    info = _field(1, reason.encode()) + _field(2, b"bambuflow")
    detail = Any_(type_url="type.googleapis.com/google.rpc.ErrorInfo", value=info)
    status = (
        _varint(1 << 3)
        + _varint(code.value[0])
        + _field(2, msg.encode())
        + _field(3, detail.SerializeToString())
    )
    return (("grpc-status-details-bin", status),)


def export_yaml(cands: list[Candidate]) -> str:
    """已通过复核的候选题，导出成 questions.yaml 的条目，走合并请求加进评测集。"""
    items = [
        {
            "id": "q_" + c.cand_id.removeprefix("cand_")[:12],
            "bucket": c.bucket,
            "asker": c.asker,
            "question": c.question,
            "expected": c.expected,
            "provenance": {
                "source": c.origin,
                "author": c.submitted_by,
                "reviewed_by": [c.expert_reviewer, c.platform_reviewer],
            },
        }
        for c in cands
        if c.status == "accepted"
    ]
    return yaml.safe_dump(items, allow_unicode=True, sort_keys=False)
