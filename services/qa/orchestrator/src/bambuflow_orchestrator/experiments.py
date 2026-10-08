"""A/B 实验分组（orchestrator 详设 12.3，技术设计文档 10.5）。

运行中的实验来自 governance 的配置快照，每个租户至多一个；每 30 秒重新拉取，
流量调整与停止 1 分钟内生效（governance 详设 4.2）。拉取失败时沿用上一份：
实验配置只在允许范围内调阈值，旧配置多用几十秒没有安全影响。

分组按 user_id 与实验 id 的哈希计算，同一用户在同一实验中始终在同一组；
实验组只替换快照里给出的阈值，上下文里有受限级内容时退回基线（由回答流程判断，见 flow.Answer）。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import grpc

from bambuflow.answer.v1 import answer_pb2
from bambuflow.config.v1 import config_pb2

_log = logging.getLogger(__name__)

REFRESH_SECONDS = 30.0


@dataclass(frozen=True)
class Assignment:
    exp_id: str
    arm: answer_pb2.ExperimentArm
    # 实验组替换的阈值；对照组与未替换的项为 None
    support_threshold: float | None = None
    min_coverage: float | None = None

    @property
    def treatment(self) -> bool:
        return self.arm == answer_pb2.EXPERIMENT_ARM_TREATMENT


def bucket(exp_id: str, user: str) -> int:
    """0–99 的稳定分桶。"""
    digest = hashlib.sha256(f"{exp_id}\x00{user}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % 100


def assign(exp: config_pb2.RunningExperiment, user: str) -> Assignment:
    if bucket(exp.exp_id, user) < exp.traffic_percent:
        return Assignment(
            exp.exp_id,
            answer_pb2.EXPERIMENT_ARM_TREATMENT,
            exp.support_threshold if exp.HasField("support_threshold") else None,
            exp.min_coverage if exp.HasField("min_coverage") else None,
        )
    return Assignment(exp.exp_id, answer_pb2.EXPERIMENT_ARM_CONTROL)


class Experiments:
    """当前运行中的实验，按租户索引。"""

    def __init__(self, running: list[config_pb2.RunningExperiment] | None = None) -> None:
        self._by_tenant = {e.tenant: e for e in running or []}

    def replace(self, running: list[config_pb2.RunningExperiment]) -> None:
        self._by_tenant = {e.tenant: e for e in running}

    def assignment(self, tenant: str, user: str) -> Assignment | None:
        exp = self._by_tenant.get(tenant)
        return assign(exp, user) if exp is not None else None


async def refresh(
    experiments: Experiments,
    fetch: Callable[[], Awaitable[config_pb2.GetSnapshotResponse]],
    every: float = REFRESH_SECONDS,
) -> None:
    """定期拉取快照并替换运行中的实验，直到被取消。"""
    version = ""
    while True:
        await asyncio.sleep(every)
        try:
            snap = await fetch()
        except (grpc.RpcError, TimeoutError, OSError) as e:
            _log.warning("拉取实验配置失败，沿用上一份", extra={"err": type(e).__name__})
            continue
        experiments.replace(list(snap.experiments))
        if snap.snapshot_version != version:
            version = snap.snapshot_version
            _log.info(
                "已更新实验配置",
                extra={"snapshot_version": version, "running": len(snap.experiments)},
            )
