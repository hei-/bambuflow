"""校验 governance 下发的配置快照（governance 详设 7.1、8.1、9.2）。

只信任签名，不信任传输通道：验签通过后从 signed_payload 解析快照本体使用，不读外层的同名字段。
签名是 ECDSA P-256（ASN.1 DER）over SHA-256(signed_payload)；公钥来自 ConfigService.GetSigningKeys。
与 Go 的 libs/platform-go/snapshot 规则相同。
"""

from __future__ import annotations

from collections.abc import Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow.config.v1 import config_pb2


class InvalidSnapshotError(Exception):
    """快照没有签名、签名不对或公钥未知。"""


def parse_keys(resp: config_pb2.GetSigningKeysResponse) -> dict[str, ec.EllipticCurvePublicKey]:
    out: dict[str, ec.EllipticCurvePublicKey] = {}
    for k in resp.keys:
        pub = serialization.load_der_public_key(k.public_key)
        if isinstance(pub, ec.EllipticCurvePublicKey):
            out[k.key_id] = pub
    return out


def open_snapshot(
    resp: config_pb2.GetSnapshotResponse, keys: Mapping[str, ec.EllipticCurvePublicKey]
) -> config_pb2.GetSnapshotResponse:
    """验签并返回快照本体；任何问题都抛 InvalidSnapshotError。"""
    if not resp.signed_payload or not resp.signature:
        raise InvalidSnapshotError("快照没有签名")
    key = keys.get(resp.signing_key_id)
    if key is None:
        raise InvalidSnapshotError(f"未知的签名公钥 {resp.signing_key_id!r}")
    try:
        key.verify(resp.signature, resp.signed_payload, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature as e:
        raise InvalidSnapshotError("签名不对") from e
    return config_pb2.GetSnapshotResponse.FromString(resp.signed_payload)
