"""按提问身份模板签发评测身份（evaluation 详设 4.2、第 6 节）。

评测身份用 evaluation 自己的签名密钥（Vault 的 evaluation/principal-signing）
与签发方 bambuflow-evaluation 签发，不借用 gateway 的密钥：线上实例只信任 gateway 签发的 Principal，
评测身份在线上无效；评测副本（BF_EVAL_MODE=1）反过来只信任这个签发方，
公钥经 evaluation 常驻服务的内部 JWKS 端点取得（serve_jwks）。
设计里的签发方是评测命名空间的独立 realm；这里由 evaluation 直接签发同样格式的 Principal，
隔离效果相同。
"""

from __future__ import annotations

import hashlib
import json
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from jwt.algorithms import ECAlgorithm

from bambuflow_evaluation.golden import Asker
from bambuflow_platform.principal import EVAL_ISSUER

CLEARANCE = {
    1: "CLASSIFICATION_PUBLIC",
    2: "CLASSIFICATION_INTERNAL",
    3: "CLASSIFICATION_CONFIDENTIAL",
    4: "CLASSIFICATION_RESTRICTED",
}
LIFETIME = 300
KEY_PATH = "evaluation/principal-signing"
JWKS_PATH = "/internal/jwks.json"


class Signer:
    def __init__(self, key: ec.EllipticCurvePrivateKey) -> None:
        self._key = key
        point = key.public_key().public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
        )
        # 与 gateway 的 kid 算法一致（services/gateway/internal/issuer）
        self._kid = hashlib.sha256(point).hexdigest()[:16]

    @classmethod
    def from_vault(cls, vault_addr: str, vault_token: str) -> Signer:
        resp = httpx.get(
            f"{vault_addr.rstrip('/')}/v1/secret/data/{KEY_PATH}",
            headers={"X-Vault-Token": vault_token},
            timeout=10,
        )
        resp.raise_for_status()
        key = serialization.load_pem_private_key(
            resp.json()["data"]["data"]["private_key"].encode(), password=None
        )
        if not isinstance(key, ec.EllipticCurvePrivateKey):
            raise TypeError("签名密钥应为 P-256 私钥")
        return cls(key)

    def jwks(self) -> dict[str, Any]:
        """评测副本验签用的公钥（与 gateway 的内部 JWKS 同一格式）。"""
        jwk = ECAlgorithm.to_jwk(self._key.public_key(), as_dict=True)
        jwk.update({"kid": self._kid, "use": "sig", "alg": "ES256"})
        return {"keys": [jwk]}

    def sign(self, a: Asker) -> str:
        now = int(time.time())
        claims = {
            "iss": EVAL_ISSUER,
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


def serve_jwks(signer: Signer, listen: str) -> ThreadingHTTPServer:
    """在后台线程里提供 GET /internal/jwks.json（只有公钥），listen 为 host:port。

    网络策略只放行评测副本。"""
    body = json.dumps(signer.jwks()).encode()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != JWKS_PATH:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            # 访问日志不需要：只有评测副本按缓存周期来取
            return

    host, _, port = listen.rpartition(":")
    server = ThreadingHTTPServer((host, int(port)), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
