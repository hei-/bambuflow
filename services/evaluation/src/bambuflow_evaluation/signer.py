"""按提问身份模板签发 Principal。

设计中评测身份由评测命名空间的独立 realm 签发、只在评测副本上被信任（评测详设 4.2）。
P0 只有 compose 环境，用开发 Vault 里 gateway 的签名密钥签发（只存在于开发与 CI），
并带 actor=evaluation 便于区分；部署到评测命名空间时换成独立签发方，不改 runner 的其余部分。
"""

from __future__ import annotations

import hashlib
import secrets
import time

import httpx
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow_evaluation.golden import Asker

CLEARANCE = {
    1: "CLASSIFICATION_PUBLIC",
    2: "CLASSIFICATION_INTERNAL",
    3: "CLASSIFICATION_CONFIDENTIAL",
    4: "CLASSIFICATION_RESTRICTED",
}
LIFETIME = 300


class Signer:
    def __init__(self, vault_addr: str, vault_token: str) -> None:
        resp = httpx.get(
            vault_addr.rstrip("/") + "/v1/secret/data/gateway/principal-signing",
            headers={"X-Vault-Token": vault_token},
            timeout=10,
        )
        resp.raise_for_status()
        key = serialization.load_pem_private_key(
            resp.json()["data"]["data"]["private_key"].encode(), password=None
        )
        if not isinstance(key, ec.EllipticCurvePrivateKey):
            raise TypeError("签名密钥应为 P-256 私钥")
        self._key = key
        point = key.public_key().public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
        )
        # 与 gateway 的 kid 算法一致（services/gateway/internal/issuer）
        self._kid = hashlib.sha256(point).hexdigest()[:16]

    def sign(self, a: Asker) -> str:
        now = int(time.time())
        claims = {
            "iss": "bambuflow-gateway",
            "aud": "bambuflow-internal",
            "sub": a.sub,
            "tenant": a.tenant,
            "dept": a.dept,
            "clearance": CLEARANCE[a.clearance],
            "groups": list(a.groups),
            "actor": "evaluation",
            "auth_time": now,
            "iat": now,
            "exp": now + LIFETIME,
            "jti": secrets.token_hex(16),
        }
        return jwt.encode(claims, self._key, algorithm="ES256", headers={"kid": self._kid})
