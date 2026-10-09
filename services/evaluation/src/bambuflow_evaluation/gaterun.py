"""异步评测门禁 EvalService.RunGate / GetGate（evaluation 详设 4、5、8.1）。

RunGate 立即返回 gate_id，门禁在后台跑：同一次运行里先在线上版本、再在候选索引版本上跑同一批题，
按题配对比较（evaluation 详设 4.3：外部模型波动等共同因素同时影响两边），
候选不低于线上（按租户的核心指标）且零越权才通过；超过时间预算按不通过。
结论写进 evaluation.gate_runs，管理接口 /v1/admin/eval-gates 可看报告。

题目来自镜像里的评测集（golden/）：index_publish 用每租户前 30 题（按题号）的冒烟集，
其余触发用全部题。
同时运行的门禁最多 4 个（evaluation 详设第 9 节），排不上的在后台等待。
"""

from __future__ import annotations

import asyncio
import logging
import re
import secrets
from collections.abc import Callable, Sequence
from dataclasses import replace
from typing import Any, Protocol

import grpc

from bambuflow.evaluation.v1 import gate_pb2, gate_pb2_grpc
from bambuflow_evaluation import gates, report
from bambuflow_evaluation.golden import GoldenSet, Question
from bambuflow_evaluation.metrics import Outcome, average, judge, tenant_metrics
from bambuflow_evaluation.runner import Config
from bambuflow_evaluation.service import Ctx, abort

_log = logging.getLogger(__name__)

SMOKE_PER_TENANT = 30
MAX_PARALLEL = 4
# 时间预算（evaluation 详设 5.1）
BUDGET = {"index_publish": 300.0, "index_rebuild": 4 * 3600.0}
DEFAULT_BUDGET = 1800.0
GATE_TRIGGERS = ("ci", "config_release", "index_publish", "index_rebuild", "scheduled")
VERSION = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
TENANT = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
VERDICTS = {
    "running": gate_pb2.VERDICT_RUNNING,
    "passed": gate_pb2.VERDICT_PASSED,
    "failed": gate_pb2.VERDICT_FAILED,
    "indeterminate": gate_pb2.VERDICT_INDETERMINATE,
}


class GateRunner(Protocol):
    """runner.Runner 中门禁用到的部分。"""

    judge_model: str
    config: Config

    async def run(self, questions: Sequence[Question]) -> list[Outcome]: ...


class Store(Protocol):
    async def start(self, g: gates.GateRun, key: str, index_version: str) -> str: ...

    async def finish(
        self, gate_id: str, conclusion: str, summary: dict[str, Any], report_md: str
    ) -> None: ...

    async def get(self, gate_id: str) -> gates.GateRun | None: ...


def select(sets: Sequence[GoldenSet], tenants: set[str], smoke: bool) -> GoldenSet:
    """挑出要跑的题，合成一个评测集（报告里记各集合的版本）。"""
    picked: list[Question] = []
    for gs in sets:
        mine = [q for q in gs.questions if not tenants or q.asker.tenant in tenants]
        if smoke:
            by: dict[str, list[Question]] = {}
            for q in sorted(mine, key=lambda q: q.id):
                by.setdefault(q.asker.tenant, []).append(q)
            mine = [q for qs in by.values() for q in qs[:SMOKE_PER_TENANT]]
        picked.extend(mine)
    return GoldenSet(
        name="+".join(gs.name for gs in sets),
        version="+".join(gs.version for gs in sets),
        provenance={},
        questions=picked,
    )


class EvalService(gate_pb2_grpc.EvalServiceServicer):
    def __init__(
        self,
        store: Store,
        sets: Callable[[], Sequence[GoldenSet]],
        runner: Callable[[str], GateRunner],
    ) -> None:
        self._store, self._sets, self._runner = store, sets, runner
        self._slots = asyncio.Semaphore(MAX_PARALLEL)
        self._tasks: set[asyncio.Task[None]] = set()

    async def RunGate(
        self, request: gate_pb2.RunGateRequest, context: Ctx
    ) -> gate_pb2.RunGateResponse:
        version = request.candidate.index_version
        tenants = set(request.tenants)
        if (
            request.trigger not in GATE_TRIGGERS
            or (version and not VERSION.match(version))
            or not all(TENANT.match(t) for t in tenants)
            or len(request.idempotency_key) > 128
        ):
            await abort(
                context, grpc.StatusCode.INVALID_ARGUMENT, "trigger、索引版本、租户或幂等键不合法"
            )
        smoke = (
            request.ruleset or ("smoke" if request.trigger == "index_publish" else "full")
        ) == "smoke"
        gs = select(self._sets(), tenants, smoke)
        if not gs.questions:
            await abort(context, grpc.StatusCode.FAILED_PRECONDITION, "评测集里没有这些租户的题")
        budget = (
            request.deadline.ToTimedelta().total_seconds()
            if request.HasField("deadline")
            else BUDGET.get(request.trigger, DEFAULT_BUDGET)
        )
        g = gates.GateRun(
            gate_id="gate_" + secrets.token_hex(12),
            trigger=request.trigger,
            golden_set=gs.name,
            golden_version=gs.version,
            tenants=tuple(sorted(tenants or {q.asker.tenant for q in gs.questions})),
            conclusion="running",
            summary={},
        )
        gate_id = await self._store.start(g, request.idempotency_key, version)
        if gate_id == g.gate_id:
            task = asyncio.create_task(self._run(replace(g, gate_id=gate_id), gs, version, budget))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        _log.info(
            "门禁开始",
            extra={
                "gate_id": gate_id,
                "trigger": request.trigger,
                "index_version": version,
                "questions": len(gs.questions),
            },
        )
        return gate_pb2.RunGateResponse(gate_id=gate_id)

    async def GetGate(
        self, request: gate_pb2.GetGateRequest, context: Ctx
    ) -> gate_pb2.GetGateResponse:
        g = await self._store.get(request.gate_id) if gates.GATE_ID.match(request.gate_id) else None
        if g is None:
            await abort(context, grpc.StatusCode.NOT_FOUND, "门禁不存在")
        violations = (
            []
            if g.conclusion in ("running", "passed")
            else [str(r) for r in g.summary.get("reasons", [])]
        )
        return gate_pb2.GetGateResponse(
            gate_id=g.gate_id,
            verdict=VERDICTS[g.conclusion],
            report_uri=f"/v1/admin/eval-gates/{g.gate_id}",
            violations=violations,
        )

    async def _run(self, g: gates.GateRun, gs: GoldenSet, version: str, budget: float) -> None:
        try:
            async with asyncio.timeout(budget):
                async with self._slots:
                    conclusion, summary, md = await self._evaluate(gs, version)
        except TimeoutError:
            conclusion, summary, md = "failed", {"reasons": [f"超过时间预算 {budget:.0f} 秒"]}, ""
        except (grpc.aio.AioRpcError, OSError, ValueError, KeyError, RuntimeError) as e:
            # 落成“无法判定”，否则门禁永远停在 running；调用方按不通过处理
            _log.exception("门禁执行失败", extra={"gate_id": g.gate_id})
            conclusion, summary, md = (
                "indeterminate",
                {"reasons": [f"评测执行失败：{type(e).__name__}"]},
                "",
            )
        await self._store.finish(g.gate_id, conclusion, summary, md)
        _log.info("门禁结束", extra={"gate_id": g.gate_id, "conclusion": conclusion})

    async def _evaluate(self, gs: GoldenSet, version: str) -> tuple[str, dict[str, Any], str]:
        # 成对对比：线上版本作基线、候选版本作被测，同一批题同一次运行（evaluation 详设 4.3）
        live = self._runner("")
        candidate = self._runner(version)
        base_run = await live.run(gs.questions)
        cand_run = await candidate.run(gs.questions)
        baseline: dict[str, float | None] | None = average([base_run])
        tenant_baseline: dict[str, dict[str, float | None]] | None = tenant_metrics([base_run])
        if all(o.status.startswith("error:") for o in base_run):
            # 还没有线上版本（第一次发布）：没有可对照的基线，只检查越权与待复核
            baseline = tenant_baseline = None
        metrics = average([cand_run])
        verdict = judge([cand_run], baseline, tenant_baseline)
        config = {k: sorted(v) for k, v in candidate.config.versions.items()}
        data = report.as_json(gs, config, candidate.judge_model, [cand_run], metrics, verdict)
        md = report.markdown(
            gs,
            config,
            candidate.judge_model,
            [cand_run],
            metrics,
            baseline,
            verdict,
            tenant_baseline,
        )
        summary = {k: data[k] for k in ("reasons", "unstable", "metrics", "config", "repeats")}
        return verdict.conclusion, summary, md
