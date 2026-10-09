"""与 governance ConfigService 的交互（governance 详设 7.1、8.1、9.2）。

取快照并验签、订阅版本变化的通知、上报加载结果。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

import grpc
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow.config.v1 import config_pb2
from bambuflow_platform.snapshot import open_snapshot, parse_keys

if TYPE_CHECKING:
    from bambuflow.config.v1.config_pb2_grpc import ConfigServiceAsyncStub

_log = logging.getLogger(__name__)

SERVICE = "orchestrator"
CALL_TIMEOUT = 10.0
MAX_BACKOFF = 30.0


class ConfigClient:
    def __init__(self, stub: ConfigServiceAsyncStub, instance: str) -> None:
        self._stub, self._instance = stub, instance
        self._keys: dict[str, ec.EllipticCurvePublicKey] = {}

    async def _load_keys(self) -> None:
        resp = await self._stub.GetSigningKeys(
            config_pb2.GetSigningKeysRequest(), timeout=CALL_TIMEOUT
        )
        self._keys = parse_keys(resp)

    async def snapshot(self) -> config_pb2.GetSnapshotResponse:
        """取快照并验签；公钥不认识时重新拉一次公钥（密钥轮换）。

        验签不过抛 InvalidSnapshotError。"""
        signed = await self._stub.GetSnapshot(
            config_pb2.GetSnapshotRequest(service=SERVICE), timeout=CALL_TIMEOUT
        )
        if signed.signing_key_id not in self._keys:
            await self._load_keys()
        return open_snapshot(signed, self._keys)

    async def ack(self, version: str, reason: str) -> None:
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
