"""console 退出（技术设计文档 8.1）：平台授权服务退出时连同公司 IdP 的会话一起退出，
最后回到 console 的回调地址。
"""

import base64
import hashlib
import html
import secrets
from urllib.parse import parse_qs, urljoin, urlparse

import httpx

from .auth import LOGIN_FORM, REALM, REDIRECT_URI, TEST_PASSWORD, _follow


def test_logout_returns_to_console() -> None:
    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=")
    with httpx.Client(follow_redirects=False, timeout=30) as c:
        page = _follow(
            c,
            c.get(
                f"{REALM}/protocol/openid-connect/auth",
                params={
                    "client_id": "console",
                    "response_type": "code",
                    "scope": "openid",
                    "redirect_uri": REDIRECT_URI,
                    "state": "s",
                    "code_challenge": challenge.decode(),
                    "code_challenge_method": "S256",
                    "kc_idp_hint": "corp-idp",
                },
            ),
        )
        form = LOGIN_FORM.search(page.text)
        assert form, page.url
        login = _follow(
            c,
            c.post(
                html.unescape(form.group(1)), data={"username": "alice", "password": TEST_PASSWORD}
            ),
        )
        code = parse_qs(urlparse(login.headers["location"]).query)["code"][0]
        tok = c.post(
            f"{REALM}/protocol/openid-connect/token",
            data={
                "grant_type": "authorization_code",
                "client_id": "console",
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "code_verifier": verifier,
            },
        ).json()
        # 退出要经过公司 IdP 再回来：公司 IdP 没登记平台的退出回跳地址时停在 Invalid redirect uri 页
        r = c.get(
            f"{REALM}/protocol/openid-connect/logout",
            params={"id_token_hint": tok["id_token"], "post_logout_redirect_uri": REDIRECT_URI},
        )
        for _ in range(10):
            location = r.headers.get("location")
            assert r.status_code in (301, 302, 303) and location, (
                f"退出停在 {r.url}：{r.status_code}"
            )
            url = urljoin(str(r.url), location)
            if url.startswith(REDIRECT_URI):
                return
            r = c.get(url)
        raise AssertionError("退出时重定向次数过多")
