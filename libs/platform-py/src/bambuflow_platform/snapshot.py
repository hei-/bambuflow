"""校验 governance 下发的配置快照（governance 详设 7.1、8.1、9.2）。

只信任签名，不信任传输通道：验签通过后从 signed_payload 解析快照本体使用，不读外层的同名字段。
签名是 ECDSA P-256（ASN.1 DER）over SHA-256(signed_payload)；公钥来自 ConfigService.GetSigningKeys。
与 Go 的 libs/platform-go/snapshot 规则相同。
加载成功的快照连同签名与公钥写本地磁盘（SnapshotCache），governance 不可用时实例重启仍能用上一份。
"""

from __future__ import annotations

import base64
import json
import os
import tempfile
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow.config.v1 import config_pb2


class InvalidSnapshotError(Exception):
    """快照没有签名、签名不对或公钥未知。"""


class NoCachedSnapshotError(Exception):
    """盘上没有可用的快照。"""


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


def public_key_der(resp: config_pb2.GetSigningKeysResponse) -> dict[str, bytes]:
    """公钥的 DER 编码（写磁盘缓存时与快照一起保存）。"""
    return {k.key_id: k.public_key for k in resp.keys}


class SnapshotCache:
    """快照的本地磁盘缓存（governance 详设 8.1）：保留当前与上一份，内容、签名与验签公钥一起写盘。

    实例重启时 governance 不可用就先用盘上的快照启动，再向 governance 对齐。
    盘上的公钥只用来发现文件损坏（缓存目录是实例自己的卷）。
    文件格式与 Go 的 libs/platform-go/snapshot.Cache 相同。
    """

    def __init__(
        self, directory: str, service: str, now: Callable[[], datetime] = lambda: datetime.now(UTC)
    ) -> None:
        self._dir, self._service, self._now = Path(directory), service, now

    def _path(self, slot: str) -> Path:
        return self._dir / f"{self._service}.{slot}.json"

    def save(self, signed: config_pb2.GetSnapshotResponse, der: bytes) -> None:
        """记下一份已验签、已加载的快照。

        与当前一份不同时，当前一份转为上一份；相同时只更新确认时间。"""
        body = {
            "signed_payload": base64.b64encode(signed.signed_payload).decode(),
            "signature": base64.b64encode(signed.signature).decode(),
            "signing_key_id": signed.signing_key_id,
            "public_key": base64.b64encode(der).decode(),
            "confirmed_at": self._now().isoformat(),
        }
        self._dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        current = self._path("current")
        try:
            old = json.loads(current.read_text())
        except (OSError, ValueError):
            old = None
        if isinstance(old, dict) and old.get("signed_payload") != body["signed_payload"]:
            current.replace(self._path("previous"))
        # 先写临时文件再改名，写到一半崩溃不会留下半个文件
        fd, tmp = tempfile.mkstemp(dir=self._dir, prefix=f"{self._service}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(body, f)
                f.flush()
                os.fsync(f.fileno())
            Path(tmp).replace(current)
        finally:
            Path(tmp).unlink(missing_ok=True)

    def load(self) -> tuple[config_pb2.GetSnapshotResponse, datetime]:
        """返回盘上的快照本体与它最后一次确认生效的时间；当前一份验不过（损坏）再取上一份。"""
        for slot in ("current", "previous"):
            try:
                e = json.loads(self._path(slot).read_text())
                kid = e["signing_key_id"]
                pub = serialization.load_der_public_key(base64.b64decode(e["public_key"]))
                if not isinstance(pub, ec.EllipticCurvePublicKey):
                    continue
                snap = open_snapshot(
                    config_pb2.GetSnapshotResponse(
                        signed_payload=base64.b64decode(e["signed_payload"]),
                        signature=base64.b64decode(e["signature"]),
                        signing_key_id=kid,
                    ),
                    {kid: pub},
                )
                confirmed = datetime.fromisoformat(e["confirmed_at"])
            except (OSError, ValueError, KeyError, TypeError, InvalidSnapshotError):
                continue
            if snap.snapshot_version:
                return snap, confirmed
        raise NoCachedSnapshotError(self._service)
