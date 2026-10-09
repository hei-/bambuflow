"""A/B 实验分组（orchestrator 详设 12.3，技术设计文档 10.5）。

运行中的实验来自 governance 的配置快照，每个租户至多一个；每 30 秒重新拉取，
流量调整与停止 1 分钟内生效（governance 详设 4.2）。拉取失败时沿用上一份：
实验配置只在允许范围内调阈值，旧配置多用几十秒没有安全影响。

分组按 user_id 与实验 id 的哈希计算，同一用户在同一实验中始终在同一组；
实验组只替换快照里给出的阈值，上下文里有受限级内容时退回基线（由回答流程判断，见 flow.Answer）。
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import grpc
from bambuflow_vocabulary import Vocabularies

from bambuflow.answer.v1 import answer_pb2
from bambuflow.config.v1 import config_pb2
from bambuflow_platform.snapshot import InvalidSnapshotError

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
    wake: asyncio.Event | None = None,
    ack: Callable[[str, str], Awaitable[None]] | None = None,
    on_vocab: Callable[[Vocabularies], None] | None = None,
) -> None:
    """拉取快照并替换运行中的实验与受控词表（on_vocab），直到被取消。

    收到版本变化的通知（wake）立即拉，否则每 every 秒兜底拉一次。

    fetch 返回的是已验签的快照；验签不过时保留上一份，
    经 ack 上报失败原因（governance 详设 8.1）。"""
    version = ""
    while True:
        if wake is None:
            await asyncio.sleep(every)
        else:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wake.wait(), every)
            wake.clear()
        try:
            snap = await fetch()
        except InvalidSnapshotError as e:
            _log.error("配置快照验签失败，沿用上一份", extra={"err": str(e)})
            if ack is not None:
                await ack("", "signature_invalid")
            continue
        except (grpc.RpcError, TimeoutError, OSError) as e:
            _log.warning("拉取实验配置失败，沿用上一份", extra={"err": type(e).__name__})
            continue
        try:
            vocab = Vocabularies.from_snapshot(snap) if on_vocab is not None else None
        except ValueError as e:
            # 词表不完整：整份快照都不用，沿用上一份
            _log.error("配置快照的词表不完整，沿用上一份", extra={"err": str(e)})
            if ack is not None:
                await ack(snap.snapshot_version, "invalid_vocabulary")
            continue
        experiments.replace(list(snap.experiments))
        if on_vocab is not None and vocab is not None:
            on_vocab(vocab)
        if ack is not None:
            await ack(snap.snapshot_version, "")
        if snap.snapshot_version != version:
            version = snap.snapshot_version
            _log.info(
                "已更新实验配置",
                extra={"snapshot_version": version, "running": len(snap.experiments)},
            )
