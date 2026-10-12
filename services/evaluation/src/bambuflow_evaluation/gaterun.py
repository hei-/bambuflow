"""异步评测门禁 EvalService.RunGate / GetGate（evaluation 详设 4、5、8.1）。

RunGate 立即返回 gate_id，门禁在后台跑：同一次运行里先在线上（生效的索引与配置）、
再在候选（待发布的索引版本，或 governance 配置发布给的候选配置版本）上跑同一批题，
按题配对比较（evaluation 详设 4.3：外部模型波动等共同因素同时影响两边），
候选不低于线上（按租户的核心指标）且零越权才通过；超过时间预算按不通过。
结论写进 evaluation.gate_runs，管理接口 /v1/admin/eval-gates 可看报告。

题目来自镜像里的评测集（golden/）：index_publish 用每租户前 30 题（按题号）的冒烟集，
其余触发用全部题；规则集 permission 只跑权限题（带 must_not_cite 的题，策略包的发布用它）。
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
from bambuflow_evaluation import audit, gates, readiness, report
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
CONFIG_VERSION = re.compile(r"^cfv_[0-9a-f]{24}$")
TENANT = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
RULESETS = ("smoke", "full", "permission")
VERDICTS = {
    "running": gate_pb2.VERDICT_RUNNING,
    "passed": gate_pb2.VERDICT_PASSED,
    "failed": gate_pb2.VERDICT_FAILED,
    "indeterminate": gate_pb2.VERDICT_INDETERMINATE,
}


class GateRunner(Protocol):
    """runner.Runner 中门禁用到的部分。"""

    judge_model: str
    llm_judge: str
    config: Config

    async def run(self, questions: Sequence[Question]) -> list[Outcome]: ...


class Store(Protocol):
    async def start(
        self, g: gates.GateRun, key: str, index_version: str, config_version: str
    ) -> str: ...

    async def finish(
        self, gate_id: str, conclusion: str, summary: dict[str, Any], report_md: str
    ) -> None: ...

    async def get(self, gate_id: str) -> gates.GateRun | None: ...


def select(sets: Sequence[GoldenSet], tenants: set[str], ruleset: str) -> GoldenSet:
    """挑出要跑的题，合成一个评测集（报告里记各集合的版本）。"""
    picked: list[Question] = []
    for gs in sets:
        mine = [q for q in gs.questions if not tenants or q.asker.tenant in tenants]
        if ruleset == "permission":
            mine = [q for q in mine if q.must_not_cite]
        if ruleset == "smoke":
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
        # 按（索引版本，候选配置版本，门禁）给出回放器，版本为空是线上
        runner: Callable[[str, str, str], GateRunner],
        trail: audit.Writer,
    ) -> None:
        self._store, self._sets, self._runner = store, sets, runner
        # 门禁结论写变更审计（技术设计文档 8.3：不经 gateway 的后台操作同样审计）
        self._trail = trail
        self._slots = asyncio.Semaphore(MAX_PARALLEL)
        self._tasks: set[asyncio.Task[None]] = set()

    async def RunGate(
        self, request: gate_pb2.RunGateRequest, context: Ctx
    ) -> gate_pb2.RunGateResponse:
        version = request.candidate.index_version
        config_version = request.candidate.config_version
        tenants = set(request.tenants)
        ruleset = request.ruleset or ("smoke" if request.trigger == "index_publish" else "full")
        if (
            request.trigger not in GATE_TRIGGERS
            or (version and not VERSION.match(version))
            or (config_version and not CONFIG_VERSION.match(config_version))
            or ruleset not in RULESETS
            or not all(TENANT.match(t) for t in tenants)
            or len(request.idempotency_key) > 128
        ):
            await abort(
                context,
                grpc.StatusCode.INVALID_ARGUMENT,
                "trigger、索引版本、配置版本、规则集、租户或幂等键不合法",
            )
        sets = self._sets()
        gs = select(sets, tenants, ruleset)
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
        gate_id = await self._store.start(g, request.idempotency_key, version, config_version)
        if gate_id == g.gate_id:
            # 分阶段接入的要求按租户的全部题算（冒烟集只是其中一部分）
            ready = [
                readiness.assess(t, [q for s in sets for q in s.questions]).as_dict()
                for t in g.tenants
            ]
            task = asyncio.create_task(
                self._run(
                    replace(g, gate_id=gate_id),
                    gs,
                    (version, config_version),
                    budget,
                    request.first_release,
                    ready,
                )
            )
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        _log.info(
            "门禁开始",
            extra={
                "gate_id": gate_id,
                "trigger": request.trigger,
                "index_version": version,
                "config_version": config_version,
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

    async def _run(
        self,
        g: gates.GateRun,
        gs: GoldenSet,
        candidate: tuple[str, str],
        budget: float,
        first: bool,
        ready: list[dict[str, Any]],
    ) -> None:
        try:
            async with asyncio.timeout(budget):
                async with self._slots:
                    conclusion, summary, md = await self._evaluate(gs, candidate, first, g.gate_id)
        except TimeoutError:
            conclusion, summary, md = "failed", {"reasons": [f"超过时间预算 {budget:.0f} 秒"]}, ""
        except audit.AuditUnavailableError:
            # 回放的请求写不进审计（evaluation 详设 4.2）：结论不能算数，按无法判定
            _log.exception("评测回放写审计失败", extra={"gate_id": g.gate_id})
            conclusion, summary, md = "indeterminate", {"reasons": ["评测回放写审计失败"]}, ""
        except (grpc.aio.AioRpcError, OSError, ValueError, KeyError, RuntimeError) as e:
            # 落成“无法判定”，否则门禁永远停在 running；调用方按不通过处理
            _log.exception("门禁执行失败", extra={"gate_id": g.gate_id})
            conclusion, summary, md = (
                "indeterminate",
                {"reasons": [f"评测执行失败：{type(e).__name__}"]},
                "",
            )
        try:
            await self._trail.write(
                "gate.conclude",
                g.gate_id,
                {
                    "conclusion": conclusion,
                    "trigger": g.trigger,
                    "tenants": list(g.tenants),
                    "index_version": candidate[0],
                    "config_version": candidate[1],
                    "golden_set": gs.name,
                },
            )
        except audit.AuditUnavailableError:
            # 审计写不进去，结论不能算通过：落成“无法判定”，调用方按不通过处理
            _log.exception("门禁结论写审计失败", extra={"gate_id": g.gate_id})
            conclusion, summary = "indeterminate", {"reasons": ["门禁结论写审计失败"]}
        await self._store.finish(g.gate_id, conclusion, {**summary, "readiness": ready}, md)
        _log.info("门禁结束", extra={"gate_id": g.gate_id, "conclusion": conclusion})

    async def _evaluate(
        self, gs: GoldenSet, version: tuple[str, str], first: bool, gate_id: str
    ) -> tuple[str, dict[str, Any], str]:
        # 成对对比：线上作基线、候选作被测，同一批题同一次运行（evaluation 详设 4.3）。
        # 第一次发布没有线上版本：没有可对照的基线，只检查越权与待复核
        candidate = self._runner(*version, gate_id)
        baseline: dict[str, float | None] | None = None
        tenant_baseline: dict[str, dict[str, float | None]] | None = None
        if not first:
            base_run = await self._runner("", "", gate_id).run(gs.questions)
            baseline, tenant_baseline = average([base_run]), tenant_metrics([base_run])
        cand_run = await candidate.run(gs.questions)
        metrics = average([cand_run])
        verdict = judge([cand_run], baseline, tenant_baseline)
        config = {k: sorted(v) for k, v in candidate.config.versions.items()}
        data = report.as_json(
            gs, config, candidate.judge_model, [cand_run], metrics, verdict, candidate.llm_judge
        )
        md = report.markdown(
            gs,
            config,
            candidate.judge_model,
            [cand_run],
            metrics,
            baseline,
            verdict,
            tenant_baseline,
            candidate.llm_judge,
        )
        summary = {k: data[k] for k in ("reasons", "unstable", "metrics", "config", "repeats")}
        return verdict.conclusion, summary, md
