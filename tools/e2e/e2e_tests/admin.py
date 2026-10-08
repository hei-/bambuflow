"""管理接口用例共用：以测试账号经 console 登录，调 gateway 的 /v1/admin（gateway 详设 4.2）。

平台角色来自测试目录的群组（tools/test-upstream/gen_ldif.py、admin-roles.yaml）：
olga、oscar 平台运维；ivan 信息安全；dana 信贷部数据属主；
rick 风险管理部数据属主；paul 平台管理员；audra 审计岗。
"""

import uuid
from functools import cache
from typing import Any

import httpx

from .test_gateway import GATEWAY, token


@cache
def admin_token(user: str) -> str:
    # 平台令牌 15 分钟有效，单个用例文件内足够；跨文件各自登录
    return token(user)


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
