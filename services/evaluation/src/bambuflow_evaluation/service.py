"""evaluation 常驻服务的 A/B 部分（evaluation 详设 2、7.2、8.2）：

分析接口与每天的护栏检查。

实验本身从 governance 的 ExperimentControlService 读，
回答记录从 orchestrator 的 AnswerRecordService 读，两者都是服务间接口；
分析接口经 gateway 调用，要求平台管理员的管理 Principal。
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any, NoReturn, Protocol

import grpc

from bambuflow.admin.v1 import (
    experiment_admin_pb2,
    experiment_analysis_pb2,
    experiment_analysis_pb2_grpc,
)
from bambuflow.answer.v1 import records_pb2
from bambuflow.config.v1 import experiment_control_pb2
from bambuflow_evaluation import abtest
from bambuflow_platform.grpc_server import current_principal

if TYPE_CHECKING:
    Ctx = grpc.aio.ServicerContext[Any, Any]
else:
    Ctx = Any

_log = logging.getLogger(__name__)

PLATFORM_ADMIN = "platform_admin"
# 导出接口单次最多 31 天，按窗口分段读
WINDOW = dt.timedelta(days=31)
CALL_TIMEOUT = 10.0


class Control(Protocol):
    """governance 的 ExperimentControlService（生成的异步存根，或测试里的替身）。"""

    @property
    def GetExperiment(
        self,
    ) -> Callable[..., Awaitable[experiment_control_pb2.GetExperimentResponse]]: ...

    @property
    def ListRunningExperiments(
        self,
    ) -> Callable[..., Awaitable[experiment_control_pb2.ListRunningExperimentsResponse]]: ...

    @property
    def StopForGuardrail(
        self,
    ) -> Callable[..., Awaitable[experiment_control_pb2.StopForGuardrailResponse]]: ...


class Records(Protocol):
    """orchestrator 的 AnswerRecordService。"""

    @property
    def ListAnswerRecords(
        self,
    ) -> Callable[..., Awaitable[records_pb2.ListAnswerRecordsResponse]]: ...


def _window(
    exp: experiment_admin_pb2.Experiment, now: dt.datetime
) -> tuple[dt.datetime, dt.datetime]:
    since = exp.created_at.ToDatetime(dt.UTC)
    until = exp.stopped_at.ToDatetime(dt.UTC) if exp.HasField("stopped_at") else now
    return since, until


async def records_of(
    stub: Records, exp: experiment_admin_pb2.Experiment, since: dt.datetime, until: dt.datetime
) -> AsyncIterator[records_pb2.AnswerRecord]:
    """实验期间的全部回答记录，按 31 天一段、每段分页读取。"""
    start = since
    while start < until:
        end = min(until, start + WINDOW)
        token = ""
        while True:
            req = records_pb2.ListAnswerRecordsRequest(
                tenant=exp.tenant, experiment_id=exp.exp_id, page_token=token
            )
            req.since.FromDatetime(start)
            req.until.FromDatetime(end)
            resp = await stub.ListAnswerRecords(req, timeout=CALL_TIMEOUT)
            for r in resp.records:
                yield r
            token = resp.next_page_token
            if not token:
                break
        start = end


async def analyze(
    records: Records, exp: experiment_admin_pb2.Experiment, now: dt.datetime
) -> experiment_analysis_pb2.ExperimentAnalysis:
    since, until = _window(exp, now)
    out = abtest.analyze(exp, [r async for r in records_of(records, exp, since, until)])
    out.data_since.FromDatetime(since)
    out.data_until.FromDatetime(until)
    return out


async def abort(context: Ctx, code: grpc.StatusCode, msg: str) -> NoReturn:
    await context.abort(code, msg)
    raise AssertionError("unreachable")


class AnalysisService(experiment_analysis_pb2_grpc.ExperimentAnalysisServiceServicer):
    def __init__(self, control: Control, records: Records) -> None:
        self._control, self._records = control, records

    async def GetExperimentAnalysis(
        self, request: experiment_analysis_pb2.GetExperimentAnalysisRequest, context: Ctx
    ) -> experiment_analysis_pb2.GetExperimentAnalysisResponse:
        p = current_principal()
        if p is None or PLATFORM_ADMIN not in p.roles:
            await abort(context, grpc.StatusCode.PERMISSION_DENIED, "需要平台管理员")
        try:
            got = await self._control.GetExperiment(
                experiment_control_pb2.GetExperimentRequest(exp_id=request.exp_id),
                timeout=CALL_TIMEOUT,
            )
            analysis = await analyze(self._records, got.experiment, dt.datetime.now(dt.UTC))
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                await abort(context, grpc.StatusCode.NOT_FOUND, "实验不存在")
            _log.warning("A/B 分析取数失败", extra={"err": e.code().name})
            await abort(context, grpc.StatusCode.UNAVAILABLE, "暂时不可用")
        return experiment_analysis_pb2.GetExperimentAnalysisResponse(analysis=analysis)


async def guard(control: Control, records: Records, now: dt.datetime) -> list[str]:
    """检查所有运行中实验的护栏，触发的请 governance 停止（技术设计文档 10.5 第 5 步）。

    返回被停止的实验。

    单个实验取数失败只记日志、继续检查其他实验；governance 不可用时整次失败，由调度方重试。"""
    running = await control.ListRunningExperiments(
        experiment_control_pb2.ListRunningExperimentsRequest(), timeout=CALL_TIMEOUT
    )
    stopped: list[str] = []
    for exp in running.experiments:
        try:
            analysis = await analyze(records, exp, now)
        except grpc.aio.AioRpcError as e:
            _log.warning("护栏检查取数失败", extra={"exp_id": exp.exp_id, "err": e.code().name})
            continue
        reasons = abtest.breached(analysis)
        if not reasons:
            continue
        await control.StopForGuardrail(
            experiment_control_pb2.StopForGuardrailRequest(
                exp_id=exp.exp_id, reason="；".join(reasons)
            ),
            timeout=CALL_TIMEOUT,
        )
        _log.warning("护栏触发，已停止实验", extra={"exp_id": exp.exp_id, "reasons": reasons})
        stopped.append(exp.exp_id)
    return stopped
