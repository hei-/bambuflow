from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow.config.v1 import config_pb2
from bambuflow_platform.snapshot import (
    InvalidSnapshotError,
    NoCachedSnapshotError,
    SnapshotCache,
    open_snapshot,
    parse_keys,
)


def sign(
    key: ec.EllipticCurvePrivateKey, kid: str, body: config_pb2.GetSnapshotResponse
) -> config_pb2.GetSnapshotResponse:
    payload = body.SerializeToString()
    sig = key.sign(payload, ec.ECDSA(hashes.SHA256()))
    return config_pb2.GetSnapshotResponse(
        snapshot_version="forged", signed_payload=payload, signature=sig, signing_key_id=kid
    )


def test_open_uses_signed_payload_and_rejects_tampering() -> None:
    key = ec.generate_private_key(ec.SECP256R1())
    der = key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    keys = parse_keys(
        config_pb2.GetSigningKeysResponse(keys=[config_pb2.SigningKey(key_id="k1", public_key=der)])
    )
    good = sign(key, "k1", config_pb2.GetSnapshotResponse(snapshot_version="v1"))
    assert open_snapshot(good, keys).snapshot_version == "v1"
    tampered = config_pb2.GetSnapshotResponse()
    tampered.CopyFrom(good)
    tampered.signed_payload += b"\x08\x01"
    for bad in (
        tampered,
        config_pb2.GetSnapshotResponse(snapshot_version="v1"),
        sign(key, "k9", config_pb2.GetSnapshotResponse(snapshot_version="v2")),
    ):
        with pytest.raises(InvalidSnapshotError):
            open_snapshot(bad, keys)
    other = ec.generate_private_key(ec.SECP256R1())
    with pytest.raises(InvalidSnapshotError):
        open_snapshot(
            sign(other, "k1", config_pb2.GetSnapshotResponse(snapshot_version="v2")), keys
        )


def test_cache_keeps_current_and_previous(tmp_path: Path) -> None:
    key = ec.generate_private_key(ec.SECP256R1())
    der = key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    now = [datetime(2026, 10, 10, 8, tzinfo=UTC)]
    cache = SnapshotCache(str(tmp_path), "orchestrator", now=lambda: now[0])
    with pytest.raises(NoCachedSnapshotError):
        cache.load()
    cache.save(sign(key, "k1", config_pb2.GetSnapshotResponse(snapshot_version="v1")), der)
    now[0] = now[0] + timedelta(minutes=1)
    # 同一份：只更新确认时间，不覆盖上一份
    cache.save(sign(key, "k1", config_pb2.GetSnapshotResponse(snapshot_version="v1")), der)
    cache.save(sign(key, "k1", config_pb2.GetSnapshotResponse(snapshot_version="v2")), der)
    snap, confirmed = cache.load()
    assert snap.snapshot_version == "v2"
    assert confirmed == now[0]
    # 当前一份损坏：退回上一份
    (tmp_path / "orchestrator.current.json").write_text('{"signed_payload": "AAAA"')
    assert cache.load()[0].snapshot_version == "v1"
    assert not list(tmp_path.glob("*.tmp"))
