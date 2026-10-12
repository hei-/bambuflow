import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow.config.v1 import config_pb2
from bambuflow_orchestrator.config import CANDIDATE_TTL, CandidateUnavailableError, ConfigClient
from bambuflow_platform.snapshot import NoCachedSnapshotError, SnapshotCache


class FakeStub:
    def __init__(self, key: ec.EllipticCurvePrivateKey) -> None:
        self.key, self.version, self.acks = key, "v1", list[config_pb2.AckRequest]()
        self.candidate_calls: list[tuple[config_pb2.GetCandidateRequest, Any]] = []

    def _signed(self, body: config_pb2.GetSnapshotResponse) -> config_pb2.GetSnapshotResponse:
        payload = body.SerializeToString()
        sig = self.key.sign(payload, ec.ECDSA(hashes.SHA256()))
        return config_pb2.GetSnapshotResponse(
            signed_payload=payload, signature=sig, signing_key_id="k1"
        )

    async def GetSnapshot(self, req: Any, timeout: float) -> config_pb2.GetSnapshotResponse:
        return self._signed(config_pb2.GetSnapshotResponse(snapshot_version=self.version))

    async def GetCandidate(
        self, req: config_pb2.GetCandidateRequest, metadata: Any, timeout: float
    ) -> config_pb2.GetCandidateResponse:
        self.candidate_calls.append((req, metadata))
        # cfv_…9 当作候选快照里的版本与请求不符
        got = req.version_id if not req.version_id.endswith("9") else "cfv_" + "0" * 24
        canary = config_pb2.Canary(object="routing", version_id=got, percent=100)
        return config_pb2.GetCandidateResponse(
            snapshot=self._signed(config_pb2.GetSnapshotResponse(canaries=[canary]))
        )

    async def GetSigningKeys(self, req: Any, timeout: float) -> config_pb2.GetSigningKeysResponse:
        der = self.key.public_key().public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return config_pb2.GetSigningKeysResponse(
            keys=[config_pb2.SigningKey(key_id="k1", public_key=der)]
        )

    async def Ack(self, req: config_pb2.AckRequest, timeout: float) -> config_pb2.AckResponse:
        self.acks.append(req)
        return config_pb2.AckResponse()

    async def Watch(self, req: Any) -> AsyncIterator[config_pb2.WatchResponse]:
        yield config_pb2.WatchResponse(snapshot_version=self.version)


def test_loaded_snapshot_is_cached_for_restart(tmp_path: Path) -> None:
    stub = FakeStub(ec.generate_private_key(ec.SECP256R1()))

    async def go() -> None:
        client = ConfigClient(stub, "orc-1", SnapshotCache(str(tmp_path), "orchestrator"))
        with pytest.raises(NoCachedSnapshotError):
            client.cached()
        snap = await client.snapshot()
        # 加载失败的不写盘
        await client.ack(snap.snapshot_version, "invalid_vocabulary")
        with pytest.raises(NoCachedSnapshotError):
            client.cached()
        await client.ack(snap.snapshot_version, "")
        # 重启后 governance 不可用：用盘上的一份
        restarted = ConfigClient(stub, "orc-1", SnapshotCache(str(tmp_path), "orchestrator"))
        assert restarted.cached().snapshot_version == "v1"

    asyncio.run(go())


def test_candidate_forwards_identity_and_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = FakeStub(ec.generate_private_key(ec.SECP256R1()))
    clock = [100.0]
    monkeypatch.setattr("bambuflow_orchestrator.config.time.monotonic", lambda: clock[0])
    md = (("authorization", "Bearer eval"),)
    version = "cfv_" + "1" * 24

    async def go() -> None:
        client = ConfigClient(stub, "orc-eval-1")
        got = await client.candidate(version, metadata=md, timeout=1.0)
        assert got.version_id == version
        req, sent = stub.candidate_calls[0]
        # 转发这道题的评测身份：governance 只对评测身份开放候选
        assert (req.service, sent) == ("orchestrator", md)
        await client.candidate(version, metadata=md, timeout=1.0)
        assert len(stub.candidate_calls) == 1
        clock[0] += CANDIDATE_TTL + 1
        await client.candidate(version, metadata=md, timeout=1.0)
        assert len(stub.candidate_calls) == 2
        with pytest.raises(CandidateUnavailableError):
            await client.candidate("cfv_" + "9" * 24, metadata=md, timeout=1.0)

    asyncio.run(go())
