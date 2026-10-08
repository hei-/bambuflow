import time
from collections.abc import Callable

import jwt
import pytest

from bambuflow.common.v1 import types_pb2
from bambuflow_platform.principal import (
    JWKS,
    MAX_CACHE_AGE_SECONDS,
    MIN_REFRESH_SECONDS,
    InvalidPrincipalError,
    Verifier,
)

from .conftest import FakeGateway


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def make_verifier(gw: FakeGateway) -> tuple[Verifier, Clock]:
    clock = Clock()
    return Verifier(JWKS("http://gateway/jwks", clock=clock, fetch=gw.fetch)), clock


def test_accepts_valid_principal(gateway: FakeGateway) -> None:
    v, _ = make_verifier(gateway)
    p = v.verify(gateway.sign("k1"))
    assert (p.sub, p.tenant, p.dept) == ("alice", "credit", "信贷部")
    assert p.clearance == types_pb2.CLASSIFICATION_INTERNAL
    assert list(p.groups) == ["credit-l2"]


def _hs256() -> str:
    now = int(time.time())
    claims = {"sub": "alice", "tenant": "credit", "iat": now, "exp": now + 60}
    claims |= {"iss": "bambuflow-gateway", "aud": "bambuflow-internal"}
    key = "guessable-hmac-key-" * 3
    return jwt.encode(claims, key, algorithm="HS256", headers={"kid": "k1"})


def _unknown_kid() -> str:
    stranger = FakeGateway("k9")
    return stranger.sign("k9")


CASES = [
    "过期",
    "签发方不对",
    "受众不对",
    "有效期超过 5 分钟",
    "缺少 iat",
    "缺少 tenant",
    "数据接口带平台角色",
    "声明类型不符",
    "未知 kid",
    "签名不对",
    "HS256 冒充",
    "不是 JWT",
]


@pytest.mark.parametrize("name", CASES)
def test_rejects(gateway: FakeGateway, name: str) -> None:
    now = int(time.time())
    other = FakeGateway("k1")  # 同名 kid、不同密钥：模拟伪造签名
    tokens: dict[str, Callable[[], str]] = {
        "过期": lambda: gateway.sign("k1", iat=now - 400, exp=now - 100),
        "签发方不对": lambda: gateway.sign("k1", iss="someone-else"),
        "受众不对": lambda: gateway.sign("k1", aud="bambuflow-platform"),
        "有效期超过 5 分钟": lambda: gateway.sign("k1", exp=now + 360),
        "缺少 iat": lambda: gateway.sign("k1", iat=None),
        "缺少 tenant": lambda: gateway.sign("k1", tenant=None),
        "数据接口带平台角色": lambda: gateway.sign("k1", roles=["platform_admin"]),
        "声明类型不符": lambda: gateway.sign("k1", groups="credit-l2"),
        "未知 kid": _unknown_kid,
        "签名不对": lambda: other.sign("k1"),
        "HS256 冒充": _hs256,
        "不是 JWT": lambda: "not-a-jwt",
    }
    assert set(tokens) == set(CASES)
    v, _ = make_verifier(gateway)
    with pytest.raises(InvalidPrincipalError):
        v.verify(tokens[name]())


def test_jwks_unavailable_fails_closed(gateway: FakeGateway) -> None:
    gateway.down = True
    v, _ = make_verifier(gateway)
    with pytest.raises(InvalidPrincipalError, match="获取 JWKS 失败"):
        v.verify(gateway.sign("k1"))


def test_key_rotation(gateway: FakeGateway) -> None:
    v, clock = make_verifier(gateway)
    v.verify(gateway.sign("k1"))

    gateway.add_key("k2")
    with pytest.raises(InvalidPrincipalError):
        v.verify(gateway.sign("k2"))  # 刷新间隔内不重新拉取
    clock.t += MIN_REFRESH_SECONDS
    v.verify(gateway.sign("k2"))

    old = gateway.sign("k1")
    del gateway.keys["k1"]
    clock.t += MAX_CACHE_AGE_SECONDS + 1
    with pytest.raises(InvalidPrincipalError):
        v.verify(old)  # 旧密钥下线、缓存过期后不再被接受
    v.verify(gateway.sign("k2"))


def test_unknown_kid_does_not_hammer_jwks(gateway: FakeGateway) -> None:
    v, _ = make_verifier(gateway)
    forged = FakeGateway("forged")
    for _ in range(20):
        with pytest.raises(InvalidPrincipalError):
            v.verify(forged.sign("forged"))
    assert gateway.calls <= 1


def test_operator_verifier_accepts_only_management_principals(gateway: FakeGateway) -> None:
    """管理接口的实现方只接受管理 Principal：带平台角色、不带租户。

    与 Go 的 NewOperatorVerifier 一致。"""
    v = Verifier(JWKS("http://gateway/jwks", fetch=gateway.fetch), operator=True)
    p = v.verify(gateway.sign("k1", tenant=None, roles=["platform_admin"]))
    assert p.sub == "alice" and list(p.roles) == ["platform_admin"]
    for bad in (
        gateway.sign("k1"),
        gateway.sign("k1", roles=["platform_admin"]),
        gateway.sign("k1", tenant=None),
    ):
        with pytest.raises(InvalidPrincipalError, match="不是管理接口的 Principal"):
            v.verify(bad)
