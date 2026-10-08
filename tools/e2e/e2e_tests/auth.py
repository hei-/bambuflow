"""端到端测试共用：模拟浏览器经平台授权服务 → 测试用公司 IdP → OpenLDAP 登录，拿到平台令牌。"""

import base64
import hashlib
import html
import os
import re
import secrets
from urllib.parse import parse_qs, urljoin, urlparse

import httpx

KC = os.environ.get("KEYCLOAK_URL", "http://keycloak:8080")
REALM = f"{KC}/realms/bambuflow"
REDIRECT_URI = "http://localhost/callback"
# 测试目录的统一口令，与构建 OpenLDAP 镜像时传入的是同一个变量（deploy/compose/compose.yaml）
TEST_PASSWORD = os.environ["TEST_USER_PASSWORD"]

LOGIN_FORM = re.compile(r'<form[^>]*id="kc-form-login"[^>]*action="([^"]+)"')


class LoginFailedError(Exception):
    pass


def _follow(client: httpx.Client, resp: httpx.Response) -> httpx.Response:
    """跟随重定向，直到落在一个页面上或回到 REDIRECT_URI（不去请求回调地址本身）。"""
    for _ in range(10):
        location = resp.headers.get("location")
        if resp.status_code not in (301, 302, 303) or location is None:
            return resp
        url = urljoin(str(resp.url), location)
        if url.startswith(REDIRECT_URI):
            return resp
        resp = client.get(url)
    raise LoginFailedError("重定向次数过多")


def platform_login(username: str, password: str, client_id: str = "console") -> dict[str, str]:
    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=")
    state = secrets.token_urlsafe(16)
    with httpx.Client(follow_redirects=False, timeout=30) as client:
        resp = client.get(
            f"{REALM}/protocol/openid-connect/auth",
            params={
                "client_id": client_id,
                "response_type": "code",
                "scope": "openid",
                "redirect_uri": REDIRECT_URI,
                "state": state,
                "code_challenge": challenge.decode(),
                "code_challenge_method": "S256",
                # 直接跳到公司 IdP，省去平台登录页上的选择
                "kc_idp_hint": "corp-idp",
            },
        )
        page = _follow(client, resp)
        form = LOGIN_FORM.search(page.text)
        if form is None or "/realms/corp-idp/" not in str(page.url):
            raise LoginFailedError(f"没有到达公司 IdP 登录页：{page.url}")
        resp = client.post(
            html.unescape(form.group(1)), data={"username": username, "password": password}
        )
        final = _follow(client, resp)
        location = final.headers.get("location", "")
        if not location.startswith(REDIRECT_URI):
            raise LoginFailedError(f"登录没有回到应用，停在 {final.url}")
        query = parse_qs(urlparse(location).query)
        assert query["state"] == [state]
        token = client.post(
            f"{REALM}/protocol/openid-connect/token",
            data={
                "grant_type": "authorization_code",
                "client_id": client_id,
                "code": query["code"][0],
                "redirect_uri": REDIRECT_URI,
                "code_verifier": verifier,
            },
        )
        token.raise_for_status()
        body: dict[str, str] = token.json()
        return body
