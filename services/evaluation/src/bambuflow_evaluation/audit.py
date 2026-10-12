"""evaluation 的访问审计（evaluation 详设 7.1：评审读审计原文本身也写审计）。

在线抽样评审要读回答原文，每读一个租户一天的回答之前先写一条审计，写不进去就不读（fail-closed）。
记录格式与 governance 的变更审计一致（governance 详设 9.3），写进同一个只追加的审计桶，
键前缀 evaluation/changes/，一条一个对象，按合规模式保留。
经管理接口的操作（候选题复核、反馈案例处理等）已有 gateway 的管理审计，这里不重复写。

评测回放的请求（evaluation 详设 4.2：照常写审计，带评测标记）不经 gateway，由回放器自己写：
每跑完一遍题写一个对象，键前缀 evaluation/replay/，每行一条，格式同 gateway 的入口审计。
governance 的审计索引按日期目录收录这两类对象（internal/auditquery）。
"""

from __future__ import annotations

import asyncio
import datetime as dt
import io
import json
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlparse

from minio import Minio
from minio.commonconfig import COMPLIANCE
from minio.error import MinioException
from minio.retention import Retention

PREFIX = "evaluation/changes/"
REPLAY_PREFIX = "evaluation/replay/"
# 没有管理 Principal 的操作（每天的定时任务）记为服务发起
ACTOR = "service:evaluation"


class AuditUnavailableError(Exception):
    """审计写不进去：调用方不执行要审计的操作。"""


class Store(Protocol):
    def put(self, key: str, body: bytes) -> None: ...


@dataclass
class Writer:
    store: Store
    now: Callable[[], dt.datetime] = field(default=lambda: dt.datetime.now(dt.UTC))

    async def write(self, action: str, object_id: str, detail: dict[str, Any]) -> None:
        t = self.now().astimezone(dt.UTC)
        record = {
            "time": t.isoformat().replace("+00:00", "Z"),
            "actor": ACTOR,
            "action": action,
            "object_id": object_id,
            "detail": detail,
        }
        body = json.dumps(record, ensure_ascii=False).encode() + b"\n"
        await self._put(PREFIX, t, ".json", body)

    async def replay(self, records: list[dict[str, Any]]) -> None:
        """一遍回放的请求，每条一行（格式同 gateway 的入口审计，带 eval 标记）。"""
        if not records:
            return
        body = b"".join(json.dumps(r, ensure_ascii=False).encode() + b"\n" for r in records)
        await self._put(REPLAY_PREFIX, self.now().astimezone(dt.UTC), ".jsonl", body)

    async def _put(self, prefix: str, t: dt.datetime, suffix: str, body: bytes) -> None:
        # 日期目录取写入的时刻（UTC）：审计索引每轮只列最近两天的目录
        ns = int(t.timestamp() * 1_000_000_000)
        key = f"{prefix}{t:%Y/%m/%d}/{ns:019d}-{secrets.token_hex(6)}{suffix}"
        try:
            await asyncio.to_thread(self.store.put, key, body)
        except (MinioException, OSError) as e:
            raise AuditUnavailableError(f"写访问审计失败：{type(e).__name__}") from e


class MinioStore:
    """开启对象锁的审计桶；与 Go 的 libs/platform-go/worm 一致，
    桶没开对象锁就不用（fail-closed）。"""

    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, bucket: str, retention_days: int
    ) -> None:
        u = urlparse(endpoint)
        self._c = Minio(
            u.netloc, access_key=access_key, secret_key=secret_key, secure=u.scheme == "https"
        )
        self._bucket = bucket
        self._retention = dt.timedelta(days=retention_days)
        # 桶由 gateway 与 governance 建好，这里只核对对象锁：没开对象锁时这一调用报错
        try:
            self._c.get_object_lock_config(bucket)
        except (MinioException, OSError) as e:
            raise AuditUnavailableError(f"审计桶 {bucket} 没有开启对象锁或无法访问") from e

    def put(self, key: str, body: bytes) -> None:
        until = dt.datetime.now(dt.UTC) + self._retention
        self._c.put_object(
            self._bucket,
            key,
            io.BytesIO(body),
            len(body),
            content_type="application/x-ndjson",
            retention=Retention(COMPLIANCE, until),
        )
