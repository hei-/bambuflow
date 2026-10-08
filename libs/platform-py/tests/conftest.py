"""测试用的签发端：生成 P-256 密钥，签发 Principal，提供可轮换、可故障的 JWKS。"""

from __future__ import annotations

import time
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow_platform.principal import AUDIENCE, ISSUER


class FakeGateway:
    def __init__(self, *kids: str) -> None:
        self.keys: dict[str, ec.EllipticCurvePrivateKey] = {}
        self.down = False
        self.calls = 0
        for kid in kids:
            self.add_key(kid)

    def add_key(self, kid: str) -> None:
        self.keys[kid] = ec.generate_private_key(ec.SECP256R1())

    def fetch(self, url: str, timeout: float) -> dict[str, Any]:
        self.calls += 1
        if self.down:
            raise ValueError("JWKS 端点不可用")
        keys = []
        for kid, k in self.keys.items():
            jwk = jwt.algorithms.ECAlgorithm.to_jwk(k.public_key(), as_dict=True)
            keys.append({**jwk, "kid": kid, "use": "sig"})
        return {"keys": keys}

    def sign(self, kid: str, **overrides: Any) -> str:
        now = int(time.time())
        claims: dict[str, Any] = {
            "sub": "alice", "tenant": "credit", "dept": "信贷部",
            "clearance": "CLASSIFICATION_INTERNAL", "groups": ["credit-l2"],
            "iss": ISSUER, "aud": AUDIENCE, "jti": "j1", "iat": now, "exp": now + 300,
        }  # fmt: skip
        for k, v in overrides.items():
            if v is None:
                claims.pop(k, None)
            else:
                claims[k] = v
        return jwt.encode(claims, self.keys[kid], algorithm="ES256", headers={"kid": kid})


@pytest.fixture
def gateway() -> FakeGateway:
    return FakeGateway("k1")
