"""合成 Principal：用开发环境 Vault 里 gateway 的签名密钥签发，直接调用 retrieval。

评测要覆盖目录里不存在的身份组合（不同密级、部门、跨租户、缺属性），经 gateway 登录做不到，
所以在评测侧签发。密钥只存在于开发与 CI 的 Vault（tools/devtools/cmd/devseed）；生产的评测实例
信任单独的测试身份签发方（retrieval 详设 10.2“评测模式”），不用这个办法。
"""

import hashlib
import os
import secrets
import time
from dataclasses import dataclass
from typing import Any

import httpx
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

CLEARANCE = {
    0: "CLASSIFICATION_UNSPECIFIED",
    1: "CLASSIFICATION_PUBLIC",
    2: "CLASSIFICATION_INTERNAL",
    3: "CLASSIFICATION_CONFIDENTIAL",
    4: "CLASSIFICATION_RESTRICTED",
}


@dataclass(frozen=True)
class Template:
    name: str
    sub: str
    tenant: str
    dept: str
    clearance: int
    groups: tuple[str, ...] = ()
    # 非空时 Principal 只带引用，retrieval 从平台身份库取完整群组
    groups_ref: str = ""
    # 预期被拒绝时为 True（策略或必含条件不成立）
    denied: bool = False


class Signer:
    def __init__(self) -> None:
        resp = httpx.get(
            os.environ["VAULT_ADDR"] + "/v1/secret/data/gateway/principal-signing",
            headers={"X-Vault-Token": os.environ["VAULT_TOKEN"]},
            timeout=10,
        )
        resp.raise_for_status()
        pem = resp.json()["data"]["data"]["private_key"].encode()
        key = serialization.load_pem_private_key(pem, password=None)
        if not isinstance(key, ec.EllipticCurvePrivateKey):
            raise TypeError("签名密钥应为 P-256 私钥")
        self._key = key
        point = key.public_key().public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
        )
        # 与 gateway 的 kid 算法一致（services/gateway/internal/issuer）
        self.kid = hashlib.sha256(point).hexdigest()[:16]

    def sign(self, t: Template) -> str:
        now = int(time.time())
        claims: dict[str, Any] = {
            "iss": "bambuflow-gateway",
            "aud": "bambuflow-internal",
            "sub": t.sub,
            "tenant": t.tenant,
            "dept": t.dept,
            "clearance": CLEARANCE[t.clearance],
            "actor": "perm-eval",
            "auth_time": now,
            "iat": now,
            "exp": now + 300,
            "jti": secrets.token_hex(16),
        }
        if t.groups_ref:
            claims["groups_ref"] = t.groups_ref
        else:
            claims["groups"] = list(t.groups)
        return jwt.encode(claims, self._key, algorithm="ES256", headers={"kid": self.kid})


CREDIT = ("credit-l2", "all-staff")

# 20 个模板：真实账号的属性组合、合成账号的密级与部门边界、跨租户、群组引用，
# 以及必须拒绝的残缺身份。
TEMPLATES = [
    Template("alice 信贷", "alice", "credit", "信贷部", 2, CREDIT),
    Template("alice 信贷 受限级", "alice", "credit", "信贷部", 4, CREDIT),
    Template("alice 信贷 公开级", "alice", "credit", "信贷部", 1, CREDIT),
    Template("carl 信贷", "carl", "credit", "信贷部", 2, CREDIT),
    Template("bob 风险", "bob", "risk", "风险管理部", 3, ("risk-dept", "all-staff")),
    Template("bob 风险 公开级", "bob", "risk", "风险管理部", 1, ("risk-dept",)),
    Template("bob 跨租户到信贷", "bob", "credit", "风险管理部", 3, ("risk-dept", "all-staff")),
    Template("alice 跨租户到风险", "alice", "risk", "信贷部", 2, CREDIT),
    Template("alice 运营租户（禁止评测信贷源）", "alice", "ops", "信贷部", 2, CREDIT),
    Template("zed 只有部门", "zed", "credit", "信贷部", 1),
    Template("zed 风险 受限级", "zed", "risk", "风险管理部", 4, ("project-042",)),
    Template(
        "zed 多群组 机密级", "zed", "credit", "信贷部", 3, ("credit-l2", "risk-dept", "project-042")
    ),
    Template("合规部 全员", "yan", "credit", "合规部", 4, ("all-staff",)),
    Template("只有无关群组", "nobody", "credit", "信贷部", 4, ("ghost",)),
    Template("eve 群组引用", "eve", "ops", "运营部", 1, groups_ref="eve"),
    Template("eve 群组引用 跨租户", "eve", "credit", "运营部", 1, groups_ref="eve"),
    Template("缺部门", "alice", "credit", "", 2, CREDIT, denied=True),
    Template("缺密级", "alice", "credit", "信贷部", 0, CREDIT, denied=True),
    Template("未配置的租户", "alice", "hr", "信贷部", 2, CREDIT, denied=True),
    Template("借用他人的群组引用", "alice", "ops", "信贷部", 2, groups_ref="eve", denied=True),
]
