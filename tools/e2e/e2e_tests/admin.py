"""管理接口用例共用：以测试账号经 console 登录，调 gateway 的 /v1/admin（gateway 详设 4.2）。

平台角色来自测试目录的群组（tools/test-upstream/gen_ldif.py、admin-roles.yaml）：
olga、oscar 平台运维；ivan 信息安全；dana 信贷部数据属主；
rick 风险管理部数据属主；paul、pete 平台管理员；audra 审计岗。
"""

import time
import uuid
from typing import Any

import httpx

from .test_gateway import GATEWAY, token

# 审批类操作要求 10 分钟内登录过（gateway 的加强认证，BF_STEP_UP_MAX_AGE），令牌用 5 分钟就重新登录
TOKEN_REUSE_SECONDS = 300
_tokens: dict[str, tuple[float, str]] = {}


def admin_token(user: str) -> str:
    cached = _tokens.get(user)
    if cached is None or time.monotonic() - cached[0] > TOKEN_REUSE_SECONDS:
        cached = (time.monotonic(), token(user))
        _tokens[user] = cached
    return cached[1]


def call(
    user: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    key: str | None = "auto",
    params: dict[str, Any] | None = None,
) -> httpx.Response:
    """调管理接口。写操作默认带一个新的 Idempotency-Key；key=None 时不带。"""
    headers = {"Authorization": "Bearer " + admin_token(user)}
    if method != "GET" and key is not None:
        headers["Idempotency-Key"] = uuid.uuid4().hex if key == "auto" else key
    return httpx.request(
        method, GATEWAY + path, json=body, params=params, headers=headers, timeout=30
    )
