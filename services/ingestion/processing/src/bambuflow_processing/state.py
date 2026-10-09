"""入库状态、死信与重放（入库管道详设第 11 节、14.4），存在 Redis。

- 每份文档一条状态：最近的事件、阶段、结果、失败原因与所在的入库批次；
- 不可重试的失败直接进死信，可重试的满 MAX_ATTEMPTS 次进死信；
  向量化不进死信（模型服务恢复后自然继续）；
- 死信只记事件（原文在对象存储的引用与摘要）和错误摘要，不记文档内容；
- 重放把原事件重新放回事件流，从对象存储里的原文开始处理；
  同一文档重放 QUARANTINE_REPLAYS 次仍失败标为隔离。
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import secrets
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from bambuflow.ingestion.v1 import events_pb2

STATUS_PREFIX = "ingest:status:"
DEAD_LETTERS = "ingest:dead-letters"
ATTEMPTS = "ingest:attempts"
REPLAYS = "ingest:replays"
MAX_ATTEMPTS = 5
QUARANTINE_REPLAYS = 3
# 向量化失败只等模型服务恢复，不进死信（入库管道详设第 11 节）
NEVER_DEAD = frozenset({"embed"})
# 待发布版本的嵌入模型与本 worker 不同（全量重建进行中）：等重建发布后再写，不进死信
NEVER_DEAD_CODES = frozenset({"model_mismatch"})
MESSAGE_LIMIT = 200


@dataclass(frozen=True)
class Failure:
    stage: str
    code: str
    message: str
    retryable: bool


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


def new_batch_id() -> str:
    return "ing_" + secrets.token_hex(12)


class IngestState:
    """client 是 redis.Redis（只用到哈希命令与 XADD）。"""

    def __init__(self, client: Any, pipeline_version: str, now: Callable[[], str] = _now) -> None:
        self._r = client
        # 死信记录处理时的组件版本，修好组件后据此判断要不要重放
        self._pipeline = pipeline_version
        self._now = now

    # ---- worker ----

    def succeeded(self, evt: events_pb2.DocumentChanged, stage: str, state: str) -> None:
        key = STATUS_PREFIX + evt.doc_id
        fields = self._base(evt, stage, state) | {"error_code": "", "error": "", "attempts": 0}
        # 入库批次只属于重放的那个事件；源端来了新事件就不再挂在旧批次下
        if self._event_of(key) != evt.event_id:
            fields["batch_id"] = ""
        self._r.hset(key, mapping=fields)
        self._r.hdel(ATTEMPTS, evt.event_id)
        # 文档的最新内容已经成功（或已有更新的版本），旧的死信与重放计数都作废
        self._r.hdel(DEAD_LETTERS, evt.doc_id)
        self._r.hdel(REPLAYS, evt.doc_id)

    def failed(self, evt: events_pb2.DocumentChanged, f: Failure, fast_path: bool = False) -> bool:
        """记录一次失败，返回是否进了死信（进了死信的消息由调用方确认，不再留在挂起列表）。

        fast_path 为 True 时是权限变更或删除：必须生效，一直重试，不进死信。"""
        attempts = int(self._r.hincrby(ATTEMPTS, evt.event_id, 1))
        dead = not fast_path and (
            not f.retryable
            or (
                attempts >= MAX_ATTEMPTS
                and f.stage not in NEVER_DEAD
                and f.code not in NEVER_DEAD_CODES
            )
        )
        state = "retrying"
        if dead:
            replays = int(self._r.hget(REPLAYS, evt.doc_id) or 0)
            quarantined = replays >= QUARANTINE_REPLAYS
            state = "quarantined" if quarantined else "dead_letter"
            first = self._first_failed(evt.doc_id)
            now = self._now()
            self._r.hset(
                DEAD_LETTERS,
                evt.doc_id,
                json.dumps(
                    {
                        "doc_id": evt.doc_id,
                        "source_id": evt.source_id,
                        "event_id": evt.event_id,
                        "stage": f.stage,
                        "error_code": f.code,
                        "error": f.message[:MESSAGE_LIMIT],
                        "pipeline_version": self._pipeline,
                        "attempts": attempts,
                        "replays": replays,
                        "quarantined": quarantined,
                        "first_failed_at": first or now,
                        "last_failed_at": now,
                        "event": base64.b64encode(evt.SerializeToString()).decode(),
                    },
                    ensure_ascii=False,
                ),
            )
            self._r.hdel(ATTEMPTS, evt.event_id)
        self._r.hset(
            STATUS_PREFIX + evt.doc_id,
            mapping=self._base(evt, f.stage, state)
            | {"error_code": f.code, "error": f.message[:MESSAGE_LIMIT], "attempts": attempts},
        )
        return dead

    def _base(self, evt: events_pb2.DocumentChanged, stage: str, state: str) -> dict[str, Any]:
        return {
            "doc_id": evt.doc_id,
            "source_id": evt.source_id,
            "event_id": evt.event_id,
            "stage": stage,
            "state": state,
            "updated_at": self._now(),
        }

    def _event_of(self, key: str) -> str:
        v = self._r.hget(key, "event_id")
        return _s(v) if v is not None else ""

    def _first_failed(self, doc_id: str) -> str:
        raw = self._r.hget(DEAD_LETTERS, doc_id)
        return str(json.loads(raw)["first_failed_at"]) if raw else ""

    # ---- 管理服务 ----

    def status(self, doc_id: str) -> dict[str, str] | None:
        got = self._r.hgetall(STATUS_PREFIX + doc_id)
        return {_s(k): _s(v) for k, v in got.items()} if got else None

    def dead_letters(self) -> list[dict[str, Any]]:
        """全部死信，按 doc_id 排序。"""
        out = [json.loads(v) for v in self._r.hgetall(DEAD_LETTERS).values()]
        return sorted(out, key=lambda d: str(d["doc_id"]))

    def replay(self, letters: Iterable[dict[str, Any]], stream: str) -> tuple[str, int]:
        """把死信里的原事件放回事件流，返回入库批次与条数。

        逐条执行、没有放进事务：中途失败时已放回的会被处理，剩下的仍在死信里，重新调用即可；
        同一事件处理两次由 indexing 的幂等键挡住。"""
        batch = new_batch_id()
        n = 0
        for d in letters:
            pb = base64.b64decode(d["event"])
            self._r.xadd(stream, {"pb": pb})
            self._r.hincrby(REPLAYS, d["doc_id"], 1)
            self._r.hdel(DEAD_LETTERS, d["doc_id"])
            self._r.hset(
                STATUS_PREFIX + d["doc_id"],
                mapping={"state": "queued", "batch_id": batch, "updated_at": self._now()},
            )
            n += 1
        return (batch if n else ""), n
