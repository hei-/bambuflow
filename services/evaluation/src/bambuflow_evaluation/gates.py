"""评测门禁的记录与查询（evaluation 详设 5.2、8.2）。

每次评测运行（python -m bambuflow_evaluation run）配置了数据库时，把结论、指标、评测配置与完整报告
写进 evaluation 自有的表 evaluation.gate_runs；
管理接口 /v1/admin/eval-gates 经 EvalGateAdminService 只读。
报告设计上存对象存储保留 1 年，这里先存在表里（报告是几十 KB 的 Markdown）。
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import re
import secrets
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import grpc
import psycopg
from google.protobuf.timestamp_pb2 import Timestamp

from bambuflow.admin.v1 import eval_gate_admin_pb2, eval_gate_admin_pb2_grpc
from bambuflow_evaluation.service import Ctx, abort, owned_tenants
from bambuflow_platform.grpc_server import current_principal

TRIGGERS = ("ci", "config_release", "index_publish", "index_rebuild", "scheduled", "manual")
CONCLUSIONS = ("running", "passed", "failed", "indeterminate")
ROLES = frozenset({"platform_admin", "platform_operator"})
DEFAULT_PAGE, MAX_PAGE = 50, 200
GATE_ID = re.compile(r"^gate_[0-9a-f]{24}$")
TENANT = re.compile(r"^[a-z][a-z0-9-]{0,62}$")

SCHEMA = """
CREATE SCHEMA IF NOT EXISTS evaluation;
CREATE TABLE IF NOT EXISTS evaluation.gate_runs (
    gate_id        text        PRIMARY KEY,
    trigger        text        NOT NULL,
    golden_set     text        NOT NULL,
    golden_version text        NOT NULL,
    tenants        text[]      NOT NULL,
    conclusion     text        NOT NULL CHECK (conclusion IN ('passed', 'failed', 'indeterminate')),
    summary        jsonb       NOT NULL,
    report_md      text        NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS gate_runs_created ON evaluation.gate_runs (created_at DESC);
-- 异步门禁（EvalService.RunGate）：先以 running 记下，跑完改成结论；同一个幂等键返回同一个门禁
ALTER TABLE evaluation.gate_runs ADD COLUMN IF NOT EXISTS idempotency_key text;
ALTER TABLE evaluation.gate_runs ADD COLUMN IF NOT EXISTS index_version text NOT NULL DEFAULT '';
-- 配置发布的门禁评测的候选配置版本（governance 详设 4.1 第 ③④ 步）
ALTER TABLE evaluation.gate_runs ADD COLUMN IF NOT EXISTS config_version text NOT NULL DEFAULT '';
CREATE UNIQUE INDEX IF NOT EXISTS gate_runs_idempotency ON evaluation.gate_runs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
ALTER TABLE evaluation.gate_runs DROP CONSTRAINT IF EXISTS gate_runs_conclusion_check;
ALTER TABLE evaluation.gate_runs ADD CONSTRAINT gate_runs_conclusion_check
    CHECK (conclusion IN ('running', 'passed', 'failed', 'indeterminate'));
"""


@dataclass(frozen=True)
class GateRun:
    gate_id: str
    trigger: str
    golden_set: str
    golden_version: str
    tenants: tuple[str, ...]
    conclusion: str
    # report.as_json 的结果：reasons、unstable、metrics、config、repeats
    summary: dict[str, Any]
    report_md: str = ""
    created_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.UTC))


def new_gate(trigger: str, tenants: list[str], data: dict[str, Any], report_md: str) -> GateRun:
    """由一次评测运行的报告（report.as_json）生成门禁记录。"""
    return GateRun(
        gate_id="gate_" + secrets.token_hex(12),
        trigger=trigger,
        golden_set=str(data["golden"]["name"]),
        golden_version=str(data["golden"]["version"]),
        tenants=tuple(sorted(tenants)),
        conclusion=str(data["conclusion"]),
        summary={
            k: data[k]
            for k in ("reasons", "unstable", "metrics", "config", "repeats", "readiness")
            if k in data
        },
        report_md=report_md,
    )


class Store(Protocol):
    async def put(self, g: GateRun) -> None: ...

    async def list(
        self, tenants: Sequence[str], trigger: str, offset: int, limit: int
    ) -> list[GateRun]:
        """覆盖 tenants 里任一租户的门禁（为空不限），新的在前。"""
        ...

    async def get(self, gate_id: str) -> GateRun | None: ...


class PGStore:
    """每次操作单独连接（门禁每天几十次，查询量很小）。"""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    async def migrate(self) -> None:
        async with await psycopg.AsyncConnection.connect(self._dsn, autocommit=True) as conn:
            await conn.execute(SCHEMA.encode())

    async def put(self, g: GateRun) -> None:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            await conn.execute(
                """INSERT INTO evaluation.gate_runs (gate_id, trigger, golden_set, golden_version,
                     tenants, conclusion, summary, report_md, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    g.gate_id,
                    g.trigger,
                    g.golden_set,
                    g.golden_version,
                    list(g.tenants),
                    g.conclusion,
                    json.dumps(g.summary, ensure_ascii=False),
                    g.report_md,
                    g.created_at,
                ),
            )

    async def list(
        self, tenants: Sequence[str], trigger: str, offset: int, limit: int
    ) -> list[GateRun]:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            cur = await conn.execute(
                """SELECT gate_id, trigger, golden_set, golden_version, tenants, conclusion,
                          summary, '', created_at
                     FROM evaluation.gate_runs
                    WHERE (cardinality(%s::text[]) = 0 OR tenants && %s::text[])
                      AND (%s = '' OR trigger = %s)
                    ORDER BY created_at DESC, gate_id DESC OFFSET %s LIMIT %s""",
                (list(tenants), list(tenants), trigger, trigger, offset, limit),
            )
            return [_row(r) for r in await cur.fetchall()]

    async def start(self, g: GateRun, key: str, index_version: str, config_version: str) -> str:
        """记下进行中的门禁；同一幂等键已有门禁时返回它的 gate_id（不再新起）。"""
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            cur = await conn.execute(
                """INSERT INTO evaluation.gate_runs (gate_id, trigger, golden_set, golden_version,
                     tenants, conclusion, summary, report_md, created_at, idempotency_key,
                     index_version, config_version)
                   VALUES (%s, %s, %s, %s, %s, 'running', '{}', '', %s, NULLIF(%s, ''), %s, %s)
                   ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                   RETURNING gate_id""",
                (
                    g.gate_id,
                    g.trigger,
                    g.golden_set,
                    g.golden_version,
                    list(g.tenants),
                    g.created_at,
                    key,
                    index_version,
                    config_version,
                ),
            )
            if await cur.fetchone():
                return g.gate_id
            cur = await conn.execute(
                "SELECT gate_id FROM evaluation.gate_runs WHERE idempotency_key = %s", (key,)
            )
            row = await cur.fetchone()
            if row is None:
                # 插入冲突却查不到：同一幂等键的门禁在两条语句之间被删了（不会发生，按无法记录处理）
                raise RuntimeError("门禁记录不一致")
            return str(row[0])

    async def finish(
        self, gate_id: str, conclusion: str, summary: dict[str, Any], report_md: str
    ) -> None:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            await conn.execute(
                """UPDATE evaluation.gate_runs SET conclusion = %s, summary = %s, report_md = %s
                    WHERE gate_id = %s AND conclusion = 'running'""",
                (conclusion, json.dumps(summary, ensure_ascii=False), report_md, gate_id),
            )

    async def abandon_running(self) -> int:
        """服务重启时还在 running 的门禁不会再有结论：按不通过结束。

        调用方超时本来也按不通过处理。"""
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            cur = await conn.execute(
                """UPDATE evaluation.gate_runs SET conclusion = 'failed',
                          summary = '{"reasons": ["评测服务重启，门禁中断"]}'
                    WHERE conclusion = 'running'"""
            )
            return cur.rowcount

    async def get(self, gate_id: str) -> GateRun | None:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            cur = await conn.execute(
                """SELECT gate_id, trigger, golden_set, golden_version, tenants, conclusion,
                          summary, report_md, created_at
                     FROM evaluation.gate_runs WHERE gate_id = %s""",
                (gate_id,),
            )
            row = await cur.fetchone()
            return _row(row) if row else None


def _row(r: tuple[Any, ...]) -> GateRun:
    return GateRun(r[0], r[1], r[2], r[3], tuple(r[4]), r[5], dict(r[6]), r[7], r[8])


def _proto(g: GateRun) -> eval_gate_admin_pb2.Gate:
    created = Timestamp()
    created.FromDatetime(g.created_at)
    s = g.summary
    return eval_gate_admin_pb2.Gate(
        gate_id=g.gate_id,
        trigger=g.trigger,
        golden_set=g.golden_set,
        golden_version=g.golden_version,
        tenants=list(g.tenants),
        conclusion=g.conclusion,
        reasons=[str(x) for x in s.get("reasons", [])],
        unstable=[str(x) for x in s.get("unstable", [])],
        metrics=[
            eval_gate_admin_pb2.GateMetric(name=k, value=float(v))
            if v is not None
            else eval_gate_admin_pb2.GateMetric(name=k)
            for k, v in sorted(s.get("metrics", {}).items())
        ],
        config=[
            eval_gate_admin_pb2.GateConfigVersion(name=k, values=[str(x) for x in v])
            for k, v in sorted(s.get("config", {}).items())
        ],
        repeats=int(s.get("repeats", 0)),
        created_at=created,
        readiness=[
            eval_gate_admin_pb2.TenantReadiness(
                tenant=str(r["tenant"]),
                stage=str(r["stage"]),
                counted=int(r["counted"]),
                unreviewed=int(r["unreviewed"]),
                refuse_share=float(r["refuse_share"]),
                synthetic_share=float(r["synthetic_share"]),
                double_reviewed=int(r["double_reviewed"]),
                gaps=[str(x) for x in r.get("gaps", [])],
            )
            for r in s.get("readiness", [])
        ],
    )


def _page(size: int, token: str) -> tuple[int, int] | None:
    n = size or DEFAULT_PAGE
    if not 0 < n <= MAX_PAGE:
        return None
    if not token:
        return n, 0
    try:
        raw = base64.urlsafe_b64decode(token.encode()).decode()
        offset = int(raw.removeprefix("o:")) if raw.startswith("o:") else -1
    except ValueError:
        return None
    return (n, offset) if offset >= 0 else None


class GateService(eval_gate_admin_pb2_grpc.EvalGateAdminServiceServicer):
    def __init__(self, store: Store) -> None:
        self._store = store

    async def _scope(self, context: Ctx) -> frozenset[str] | None:
        """平台管理员、平台运维看全部（None）；租户负责人只看覆盖自己负责的租户的门禁。"""
        p = current_principal()
        if p is not None and ROLES.intersection(p.roles):
            return None
        owned = owned_tenants(p.roles) if p is not None else frozenset()
        if not owned:
            await abort(
                context, grpc.StatusCode.PERMISSION_DENIED, "需要平台管理员、平台运维或租户负责人"
            )
        return owned

    async def ListGates(
        self, request: eval_gate_admin_pb2.ListGatesRequest, context: Ctx
    ) -> eval_gate_admin_pb2.ListGatesResponse:
        scope = await self._scope(context)
        page = _page(request.page_size, request.page_token)
        if (
            page is None
            or (request.tenant and not TENANT.match(request.tenant))
            or (request.trigger and request.trigger not in TRIGGERS)
        ):
            await abort(context, grpc.StatusCode.INVALID_ARGUMENT, "参数不合法")
        if scope is not None and request.tenant and request.tenant not in scope:
            await abort(context, grpc.StatusCode.PERMISSION_DENIED, "只能查看自己负责的租户")
        tenants = [request.tenant] if request.tenant else sorted(scope or ())
        size, offset = page
        gates = await self._store.list(tenants, request.trigger, offset, size + 1)
        out = eval_gate_admin_pb2.ListGatesResponse(gates=[_proto(g) for g in gates[:size]])
        if len(gates) > size:
            out.next_page_token = base64.urlsafe_b64encode(f"o:{offset + size}".encode()).decode()
        return out

    async def GetGate(
        self, request: eval_gate_admin_pb2.GetGateRequest, context: Ctx
    ) -> eval_gate_admin_pb2.GetGateResponse:
        scope = await self._scope(context)
        g = await self._store.get(request.gate_id) if GATE_ID.match(request.gate_id) else None
        # 不覆盖自己负责的租户的门禁与不存在的一样处理
        if g is None or (scope is not None and not scope.intersection(g.tenants)):
            await abort(context, grpc.StatusCode.NOT_FOUND, "门禁不存在")
        return eval_gate_admin_pb2.GetGateResponse(gate=_proto(g), report_markdown=g.report_md)
