"""平台授权服务的登录链路：平台 realm → 测试用公司 IdP（corp-idp）→ OpenLDAP。

拿到平台令牌后按 JWKS 验签，检查签名算法、受众与有效期（ADR-13：平台令牌 15 分钟、ES256）。
需要 compose 的 core profile 已经起来（make up）。
"""

import jwt
import pytest

from .auth import REALM, TEST_PASSWORD, LoginFailedError, platform_login


def test_login_via_corp_idp_issues_platform_token() -> None:
    access = platform_login("alice", TEST_PASSWORD)["access_token"]

    assert jwt.get_unverified_header(access)["alg"] == "ES256"
    key = jwt.PyJWKClient(f"{REALM}/protocol/openid-connect/certs").get_signing_key_from_jwt(access)
    claims = jwt.decode(
        access, key, algorithms=["ES256"], audience="bambuflow-platform", issuer=REALM
    )
    assert claims["exp"] - claims["iat"] == 900
    assert claims["preferred_username"] == "alice"


def test_wrong_password_is_rejected_by_upstream() -> None:
    with pytest.raises(LoginFailedError, match="没有回到应用"):
        platform_login("alice", "wrong-password")
