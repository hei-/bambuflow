"""业务系统代用户调用（gateway 详设 3.1、3.4）。

业务系统拿用户的公司 IdP 令牌，向平台授权服务做令牌交换（RFC 8693），得到 act 为自己的平台令牌；
调用 gateway 时还要出示同一业务系统的服务身份（开发环境没有入口代理，直接带证书主题请求头）。

登记做的事与 deploy/authz/register-business-client.sh 相同
（这里用 Python 调管理 API，e2e 容器里没有 jq）。
risk 租户的 bound_clients 里有 risk-portal；e2e-biz-unregistered 只建客户端、不给令牌交换权限。
"""

import json
import os
from collections.abc import Iterator

import httpx
import pytest

from .auth import KC, TEST_PASSWORD
from .test_gateway import GATEWAY, QUESTION

PORTAL = "risk-portal"
UNREGISTERED = "e2e-biz-unregistered"
CERT_HEADER = "Ssl-Client-Subject-Dn"
EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
SUBJECT_TYPE = "urn:ietf:params:oauth:token-type:access_token"


def _client(admin: httpx.Client, client_id: str) -> str:
    """建只做令牌交换的机密客户端（受众为平台、带 act）；已存在就不动。返回内部 ID。"""
    r = admin.get("/admin/realms/bambuflow/clients", params={"clientId": client_id})
    r.raise_for_status()
    if not r.json():
        r = admin.post(
            "/admin/realms/bambuflow/clients",
            json={
                "clientId": client_id,
                "enabled": True,
                "publicClient": False,
                "standardFlowEnabled": False,
                "directAccessGrantsEnabled": False,
                "serviceAccountsEnabled": False,
                "attributes": {"access.token.signed.response.alg": "ES256"},
                "protocolMappers": [
                    {
                        "name": "platform-audience",
                        "protocol": "openid-connect",
                        "protocolMapper": "oidc-audience-mapper",
                        "config": {
                            "included.custom.audience": "bambuflow-platform",
                            "access.token.claim": "true",
                            "id.token.claim": "false",
                        },
                    },
                    {
                        "name": "act",
                        "protocol": "openid-connect",
                        "protocolMapper": "oidc-hardcoded-claim-mapper",
                        "config": {
                            "claim.name": "act",
                            "claim.value": json.dumps({"sub": client_id}),
                            "jsonType.label": "JSON",
                            "access.token.claim": "true",
                            "id.token.claim": "false",
                            "userinfo.token.claim": "false",
                        },
                    },
                ],
            },
        )
        assert r.status_code in (201, 409), r.text
        r = admin.get("/admin/realms/bambuflow/clients", params={"clientId": client_id})
        r.raise_for_status()
    internal: str = r.json()[0]["id"]
    return internal


def _allow_exchange(admin: httpx.Client, client_internal_id: str, client_id: str) -> None:
    r = admin.put(
        "/admin/realms/bambuflow/identity-provider/instances/corp-idp/management/permissions",
        json={"enabled": True},
    )
    r.raise_for_status()
    perm = r.json()["scopePermissions"]["token-exchange"]
    rm = admin.get("/admin/realms/bambuflow/clients", params={"clientId": "realm-management"})
    rm.raise_for_status()
    authz = f"/admin/realms/bambuflow/clients/{rm.json()[0]['id']}/authz/resource-server"
    name = f"token-exchange:{client_id}"
    found = [
        p for p in admin.get(f"{authz}/policy", params={"name": name}).json() if p["name"] == name
    ]
    if found:
        policy = found[0]["id"]
    else:
        r = admin.post(
            f"{authz}/policy/client",
            json={"name": name, "clients": [client_internal_id], "logic": "POSITIVE"},
        )
        r.raise_for_status()
        policy = r.json()["id"]
    current = [p["id"] for p in admin.get(f"{authz}/policy/{perm}/associatedPolicies").json()]
    rep = admin.get(f"{authz}/permission/scope/{perm}").json()
    rep.update(policies=sorted({*current, policy}), decisionStrategy="AFFIRMATIVE")
    admin.put(f"{authz}/permission/scope/{perm}", json=rep).raise_for_status()


@pytest.fixture(scope="module")
def secrets() -> Iterator[dict[str, str]]:
    tok = httpx.post(
        f"{KC}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": "admin",
            "password": os.environ["KEYCLOAK_ADMIN_PASSWORD"],
        },
        timeout=30,
    )
    tok.raise_for_status()
    with httpx.Client(
        base_url=KC, headers={"Authorization": "Bearer " + tok.json()["access_token"]}, timeout=30
    ) as admin:
        out = {}
        for client_id in (PORTAL, UNREGISTERED):
            internal = _client(admin, client_id)
            if client_id == PORTAL:
                _allow_exchange(admin, internal, client_id)
            r = admin.get(f"/admin/realms/bambuflow/clients/{internal}/client-secret")
            r.raise_for_status()
            out[client_id] = r.json()["value"]
        yield out


def corp_token(user: str) -> str:
    """用户在业务系统登录后，业务系统手里的公司 IdP 令牌。"""
    r = httpx.post(
        f"{KC}/realms/corp-idp/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "e2e-business-frontend",
            "username": user,
            "password": TEST_PASSWORD,
            "scope": "openid",
        },
        timeout=30,
    )
    r.raise_for_status()
    tok: str = r.json()["access_token"]
    return tok


def exchange(client_id: str, secret: str, subject: str) -> httpx.Response:
    return httpx.post(
        f"{KC}/realms/bambuflow/protocol/openid-connect/token",
        data={
            "grant_type": EXCHANGE,
            "subject_token": subject,
            "subject_token_type": SUBJECT_TYPE,
            "subject_issuer": "corp-idp",
            "client_id": client_id,
            "client_secret": secret,
        },
        timeout=30,
    )


def search(tok: str, cert_cn: str | None) -> httpx.Response:
    headers = {"Authorization": "Bearer " + tok}
    if cert_cn is not None:
        headers[CERT_HEADER] = f"CN={cert_cn},O=Corp"
    return httpx.post(GATEWAY + "/v1/search", json=QUESTION, headers=headers, timeout=10)


def test_exchanged_token_acts_for_the_user(secrets: dict[str, str]) -> None:
    r = exchange(PORTAL, secrets[PORTAL], corp_token("ruth"))
    assert r.status_code == 200, r.text
    tok = r.json()["access_token"]
    # 绑定 risk 租户的业务系统：不用传租户请求头，以 ruth 本人的权限检索
    assert search(tok, PORTAL).status_code == 200
    # 服务身份缺失或不是同一个业务系统
    assert search(tok, None).status_code == 401
    assert search(tok, "other-app").status_code == 401


def test_exchange_only_for_registered_clients(secrets: dict[str, str]) -> None:
    r = exchange(UNREGISTERED, secrets[UNREGISTERED], corp_token("ruth"))
    assert r.status_code == 403, r.text


def test_upstream_token_is_not_accepted_directly() -> None:
    # 转发用户的原始令牌：签发方与受众都不是本平台（MCP 规范同样禁止令牌透传）
    assert search(corp_token("ruth"), PORTAL).status_code == 401
