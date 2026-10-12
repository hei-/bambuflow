"""校验 gateway 签发的内部身份 Principal（gateway 详设 3.2）。

规则与 libs/platform-go/principal 一致。

任何校验失败都抛 InvalidPrincipalError，按未认证处理（fail-closed）；具体原因只用于日志。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

import httpx
import jwt
from google.protobuf import json_format

from bambuflow.identity.v1 import principal_pb2

ISSUER = "bambuflow-gateway"
# evaluation 签发评测身份时使用的签发方：只有评测副本信任它（evaluation 详设 4.2）
EVAL_ISSUER = "bambuflow-evaluation"
AUDIENCE = "bambuflow-internal"
MAX_LIFETIME_SECONDS = 300

# 未知 kid 最多每 30 秒刷新一次 JWKS，防止伪造 kid 的请求把端点打满；
# 缓存超过 10 分钟在下次使用时刷新，使轮换下线的旧密钥及时失效。
MIN_REFRESH_SECONDS = 30.0
MAX_CACHE_AGE_SECONDS = 600.0


class InvalidPrincipalError(Exception):
    pass


class JWKS:
    """从 gateway 的内部 JWKS 端点获取 P-256 公钥并缓存，按 kid 查找。"""

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        fetch: Callable[[str, float], dict[str, Any]] | None = None,
    ) -> None:
        self._url = url
        self._timeout = timeout
        self._clock = clock
        self._fetch = fetch or _http_get_json
        self._lock = threading.Lock()
        self._keys: dict[str, Any] = {}
        self._fetched_at: float | None = None

    def key(self, kid: str) -> Any:
        with self._lock:
            key = self._keys.get(kid)
            age = None if self._fetched_at is None else self._clock() - self._fetched_at
            stale = age is None or age > MAX_CACHE_AGE_SECONDS
            can_refresh = age is None or age >= MIN_REFRESH_SECONDS
            if (key is None or stale) and can_refresh:
                self._fetched_at = self._clock()
                try:
                    self._keys = _parse_jwks(self._fetch(self._url, self._timeout))
                    key = self._keys.get(kid)
                except (httpx.HTTPError, ValueError, jwt.PyJWTError) as e:
                    # 刷新失败但缓存里有这个 kid 时沿用缓存（公钥，短时间沿用不扩大权限）
                    if key is None:
                        raise InvalidPrincipalError(f"获取 JWKS 失败: {e}") from e
            if key is None:
                raise InvalidPrincipalError(f"未知的 kid {kid!r}")
            return key


def _http_get_json(url: str, timeout: float) -> dict[str, Any]:
    resp = httpx.get(url, timeout=timeout)
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    return data


def _parse_jwks(data: dict[str, Any]) -> dict[str, Any]:
    keys = {}
    for k in data.get("keys", []):
        if k.get("kty") != "EC" or k.get("crv") != "P-256" or not k.get("kid"):
            continue
        if k.get("use", "sig") != "sig":
            continue
        # PyJWK 解析时会校验点在曲线上
        keys[k["kid"]] = jwt.PyJWK(k, algorithm="ES256").key
    if not keys:
        raise ValueError("JWKS 中没有可用的 P-256 公钥")
    return keys


class Verifier:
    """校验 gateway 签发的 Principal。

    operator 为 True 时只接受管理 Principal（带平台角色、不带租户，
    gateway 签发给管理接口的实现方）；
    否则只接受数据接口的 Principal（带租户、不带角色）。
    两类下游各自只接受自己那一类，与 Go 的 principal 包一致。
    issuer 默认是 gateway；评测副本只信任 evaluation 签发的评测身份
    （EVAL_ISSUER，evaluation 详设 4.2）。"""

    def __init__(self, jwks: JWKS, operator: bool = False, issuer: str = ISSUER) -> None:
        self._jwks = jwks
        self._operator = operator
        self._issuer = issuer

    def verify(self, raw: str) -> principal_pb2.Principal:
        try:
            kid = jwt.get_unverified_header(raw).get("kid")
            if not kid:
                raise InvalidPrincipalError("缺少 kid")
            claims = jwt.decode(
                raw,
                self._jwks.key(kid),
                algorithms=["ES256"],
                audience=AUDIENCE,
                issuer=self._issuer,
                options={"require": ["exp", "iat", "iss", "aud", "sub"]},
            )
        except jwt.PyJWTError as e:
            raise InvalidPrincipalError(str(e)) from e
        if claims["exp"] - claims["iat"] > MAX_LIFETIME_SECONDS:
            raise InvalidPrincipalError("有效期超过 5 分钟")
        try:
            p = json_format.ParseDict(claims, principal_pb2.Principal(), ignore_unknown_fields=True)
        except json_format.ParseError as e:
            raise InvalidPrincipalError(f"声明格式不符: {e}") from e
        if not p.sub:
            raise InvalidPrincipalError("缺少 sub")
        if self._operator and (not p.roles or p.tenant):
            raise InvalidPrincipalError("不是管理接口的 Principal")
        if not self._operator and (not p.tenant or p.roles):
            raise InvalidPrincipalError("不是数据接口的 Principal")
        return p
