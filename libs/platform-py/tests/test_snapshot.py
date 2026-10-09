import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow.config.v1 import config_pb2
from bambuflow_platform.snapshot import InvalidSnapshotError, open_snapshot, parse_keys


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
