"""与 governance ConfigService 的交互（governance 详设 7.1、8.1、9.2）。

取快照并验签、订阅版本变化的通知、上报加载结果；加载成功（ack 无失败原因）的快照写本地磁盘，
启动时 governance 不可用就先用盘上的一份（governance 详设 8.1）。

评测副本另按请求取候选配置（candidate，governance 详设 4.1 第 ③④ 步）：
配置发布的门禁指定一个候选版本，转发这道题的评测身份向 governance 按版本取（只对评测身份开放），
验签后按版本缓存 CANDIDATE_TTL。
候选版本不可变，但它和哪些生效版本放在一起会变，所以缓存不长留。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterable, Awaitable, Callable
from typing import Any, Protocol

import grpc
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow.config.v1 import config_pb2
from bambuflow_platform.snapshot import (
    InvalidSnapshotError,
    NoCachedSnapshotError,
    SnapshotCache,
    open_snapshot,
    parse_keys,
    public_key_der,
)

_log = logging.getLogger(__name__)

SERVICE = "orchestrator"
CALL_TIMEOUT = 10.0
MAX_BACKOFF = 30.0
CANDIDATE_TTL = 600.0
# 同时在评的候选不多（门禁最多并行 4 个）；超过就整份清掉重取
MAX_CANDIDATES = 32


class CandidateUnavailableError(Exception):
    """取不到候选配置（版本不存在、governance 不可用、验签不过或内容与版本不符）。"""


class Stub(Protocol):
    """governance 的 ConfigService（生成的异步存根，或测试里的替身）。"""

    @property
    def GetSnapshot(self) -> Callable[..., Awaitable[config_pb2.GetSnapshotResponse]]: ...

    @property
    def GetSigningKeys(self) -> Callable[..., Awaitable[config_pb2.GetSigningKeysResponse]]: ...

    @property
    def Ack(self) -> Callable[..., Awaitable[config_pb2.AckResponse]]: ...

    @property
    def Watch(self) -> Callable[..., AsyncIterable[config_pb2.WatchResponse]]: ...

    @property
    def GetCandidate(self) -> Callable[..., Awaitable[config_pb2.GetCandidateResponse]]: ...


class ConfigClient:
    def __init__(self, stub: Stub, instance: str, cache: SnapshotCache | None = None) -> None:
        self._stub, self._instance, self._cache = stub, instance, cache
        self._keys: dict[str, ec.EllipticCurvePublicKey] = {}
        self._der: dict[str, bytes] = {}
        # 最近取到的已验签快照（版本 → 签名的原样），加载成功后写盘
        self._fetched: tuple[str, config_pb2.GetSnapshotResponse] | None = None
        # 评测副本取到的候选：版本 → （取到的时刻，候选）
        self._candidates: dict[str, tuple[float, config_pb2.Canary]] = {}

    async def _load_keys(self) -> None:
        resp = await self._stub.GetSigningKeys(
            config_pb2.GetSigningKeysRequest(), timeout=CALL_TIMEOUT
        )
        self._keys, self._der = parse_keys(resp), public_key_der(resp)

    async def snapshot(self) -> config_pb2.GetSnapshotResponse:
        """取快照并验签；公钥不认识时重新拉一次公钥（密钥轮换）。

        验签不过抛 InvalidSnapshotError。"""
        signed = await self._stub.GetSnapshot(
            config_pb2.GetSnapshotRequest(service=SERVICE), timeout=CALL_TIMEOUT
        )
        if signed.signing_key_id not in self._keys:
            await self._load_keys()
        snap = open_snapshot(signed, self._keys)
        self._fetched = (snap.snapshot_version, signed)
        return snap

    async def candidate(self, version: str, **call: Any) -> config_pb2.Canary:
        """评测副本按版本取候选（call 是转发评测身份的 metadata 与 timeout，见 downstream）。

        取不到抛 CandidateUnavailableError。"""
        now = time.monotonic()
        hit = self._candidates.get(version)
        if hit is not None and now - hit[0] <= CANDIDATE_TTL:
            return hit[1]
        try:
            resp = await self._stub.GetCandidate(
                config_pb2.GetCandidateRequest(service=SERVICE, version_id=version), **call
            )
            if resp.snapshot.signing_key_id not in self._keys:
                await self._load_keys()
            snap = open_snapshot(resp.snapshot, self._keys)
        except (grpc.RpcError, InvalidSnapshotError) as e:
            raise CandidateUnavailableError(type(e).__name__) from e
        if len(snap.canaries) != 1 or snap.canaries[0].version_id != version:
            raise CandidateUnavailableError("候选快照与请求的版本不符")
        if len(self._candidates) >= MAX_CANDIDATES:
            self._candidates.clear()
        self._candidates[version] = (now, snap.canaries[0])
        _log.info(
            "已加载候选配置",
            extra={"version_id": version, "object": snap.canaries[0].object},
        )
        return snap.canaries[0]

    def cached(self) -> config_pb2.GetSnapshotResponse:
        """盘上的快照（启动时 governance 不可用）；没有时抛 NoCachedSnapshotError。"""
        if self._cache is None:
            raise NoCachedSnapshotError(SERVICE)
        snap, confirmed = self._cache.load()
        _log.warning(
            "governance 不可用，先用本地磁盘上的配置快照启动",
            extra={
                "snapshot_version": snap.snapshot_version,
                "confirmed_at": confirmed.isoformat(),
            },
        )
        return snap

    def _persist(self, version: str) -> None:
        if self._cache is None or self._fetched is None or self._fetched[0] != version:
            return
        signed = self._fetched[1]
        try:
            self._cache.save(signed, self._der[signed.signing_key_id])
        except (OSError, KeyError) as e:
            _log.warning("快照写本地磁盘失败", extra={"err": type(e).__name__})

    async def ack(self, version: str, reason: str) -> None:
        if not reason:
            self._persist(version)
        try:
            await self._stub.Ack(
                config_pb2.AckRequest(
                    service=SERVICE,
                    instance_id=self._instance,
                    snapshot_version=version,
                    loaded=not reason,
                    reason=reason,
                ),
                timeout=CALL_TIMEOUT,
            )
        except grpc.RpcError as e:
            _log.warning("上报快照加载结果失败", extra={"err": type(e).__name__})

    async def watch(self, loaded: Callable[[], str], wake: asyncio.Event) -> None:
        """订阅版本变化的通知，每收到一条就唤醒刷新；断线后按退避重连（上限 30 秒），直到被取消。"""
        backoff = 1.0
        while True:
            try:
                call = self._stub.Watch(
                    config_pb2.WatchRequest(
                        service=SERVICE, instance_id=self._instance, loaded=loaded()
                    )
                )
                async for _ in call:
                    backoff = 1.0
                    wake.set()
            except grpc.RpcError as e:
                _log.warning("快照通知断开，稍后重连", extra={"err": type(e).__name__})
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
