"""运行时保护的端到端测试（gateway 详设 5.1、第 6 节）。

覆盖租户每日 token 配额、租户并发、每用户并发 SSE 流。开发环境的配额与并发都放得很宽
（tools/test-upstream/tenants），所以这里直接改 gateway 在 Redis 里的计数来制造“用尽 / 占满”，
测完恢复原值。计数的 key 与时区约定见 services/gateway/internal/admission。
全局自适应并发需要压出下游延迟，由单测覆盖。需要先 make ingest。
"""

import os
import socket
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from .test_gateway import GATEWAY, token

REDIS = os.environ.get("BF_REDIS_ADDR", "redis:6379")
# 与 compose 里 gateway 的 BF_QUOTA_TIMEZONE 一致（未设置时 gateway 默认 Asia/Shanghai）
QUOTA_TZ = ZoneInfo(os.environ.get("BF_QUOTA_TIMEZONE", "Asia/Shanghai"))
QUESTION = "个人消费贷款的额度上限是多少？"


def redis(*args: str) -> bytes | int | None:
    """发一条 Redis 命令（RESP 协议），只解析这里用到的回复类型。

    e2e 只需要几条命令，不为此引入 Redis 客户端依赖。
    """
    host, port = REDIS.rsplit(":", 1)
    req = f"*{len(args)}\r\n".encode() + b"".join(
        f"${len(a.encode())}\r\n".encode() + a.encode() + b"\r\n" for a in args
    )
    with socket.create_connection((host, int(port)), timeout=5) as s:
        s.sendall(req)
        f = s.makefile("rb")
        line = f.readline()
        kind, rest = line[:1], line[1:-2]
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else f.read(n + 2)[:-2]
        if kind == b"*":
            # TIME：两个整数字符串
            items = []
            for _ in range(int(rest)):
                n = int(f.readline()[1:-2])
                items.append(f.read(n + 2)[:-2])
            return b" ".join(items)
        return rest


def quota_key(tenant: str) -> str:
    return f"bf:quota:tokens:{tenant}:{datetime.now(QUOTA_TZ).date().isoformat()}"


def used(tenant: str) -> int:
    v = redis("GET", quota_key(tenant))
    return int(v) if isinstance(v, bytes) else 0


@contextmanager
def exhausted_quota(tenant: str) -> Iterator[None]:
    key = quota_key(tenant)
    before = redis("GET", key)
    redis("SET", key, str(10**15), "EX", "3600")
    try:
        yield
    finally:
        if isinstance(before, bytes):
            redis("SET", key, before.decode(), "EX", str(48 * 3600))
        else:
            redis("DEL", key)


@contextmanager
def occupied(slots: str, key: str, n: int) -> Iterator[None]:
    """往并发计数里塞 n 个一分钟后到期的租约，模拟名额已被占满。"""
    k = f"bf:slots:{slots}:{key}"
    t = redis("TIME")
    assert isinstance(t, bytes)
    sec, usec = (int(x) for x in t.split())
    expire = str(sec * 1000 + usec // 1000 + 60_000)
    members = [x for i in range(n) for x in (expire, f"e2e-{uuid.uuid4().hex}-{i}")]
    redis("ZADD", k, *members)
    try:
        yield
    finally:
        redis("DEL", k)


def post(path: str, tok: str, body: dict[str, object], tenant: str = "credit") -> httpx.Response:
    return httpx.post(
        GATEWAY + path,
        json=body,
        headers={"Authorization": "Bearer " + tok, "Bambuflow-Tenant": tenant},
        timeout=15,
    )


def test_answer_usage_is_counted_against_tenant_quota() -> None:
    before = used("credit")
    r = post("/v1/answers", token("alice"), {"question": QUESTION})
    assert r.status_code == 200, r.text
    assert "usage" not in r.json() and "prompt_tokens" not in r.text, "用量不对外返回"
    assert used("credit") > before, "问答的用量应计入租户当日配额"


def test_quota_exceeded_blocks_answers_but_not_search() -> None:
    tok = token("alice")
    with exhausted_quota("credit"):
        r = post("/v1/answers", tok, {"question": QUESTION})
        assert r.status_code == 429, r.text
        assert r.json()["type"] == "quota_exceeded"
        assert "retry-after" not in r.headers, "配额用尽当日不会恢复，不应提示重试"
        r = post("/v1/answers", tok, {"question": QUESTION, "stream": True})
        assert r.status_code == 429 and r.json()["type"] == "quota_exceeded", r.text
        r = post(
            "/v1/compat/chat/completions",
            tok,
            {"messages": [{"role": "user", "content": QUESTION}]},
        )
        assert r.status_code == 429 and r.json()["type"] == "quota_exceeded", r.text
        # 仅检索照常
        r = post("/v1/search", tok, {"question": QUESTION})
        assert r.status_code == 200, r.text
    # 恢复后问答照常
    assert post("/v1/answers", tok, {"question": QUESTION}).status_code == 200


def test_tenant_concurrency_limit() -> None:
    tok = token("alice")
    # 开发环境 credit 的并发上限为 1000（tools/test-upstream/tenants/credit）
    with occupied("tenant", "credit", 1000):
        r = post("/v1/search", tok, {"question": QUESTION})
        assert r.status_code == 429, r.text
        assert r.json()["type"] == "rate_limited" and r.headers.get("retry-after")
    assert post("/v1/search", tok, {"question": QUESTION}).status_code == 200


def test_user_stream_limit() -> None:
    tok = token("alice")
    # compose 里每用户并发流放宽到 BF_USER_STREAMS（默认 1000）
    with occupied("user-streams", "alice", int(os.environ.get("BF_USER_STREAMS", "1000"))):
        r = post("/v1/answers", tok, {"question": QUESTION, "stream": True})
        assert r.status_code == 429 and r.json()["type"] == "rate_limited", r.text
        # 非流式问答不受流数限制
        assert post("/v1/answers", tok, {"question": QUESTION}).status_code == 200
    r = post("/v1/answers", tok, {"question": QUESTION, "stream": True})
    assert r.status_code == 200 and "event: final" in r.text, r.text
