"""入库状态、死信与重放（入库管道详设第 11 节、14.4），存在 Redis。

- 每份文档一条状态：最近的事件、阶段、结果、失败原因与所在的入库批次；
- 不可重试的失败直接进死信，可重试的满 MAX_ATTEMPTS 次进死信；
  向量化不进死信（模型服务恢复后自然继续）；
- 死信只记事件（原文在对象存储的引用与摘要）和错误摘要，不记文档内容；
- 重放把原事件重新放回事件流，从对象存储里的原文开始处理；
  同一文档重放 QUARANTINE_REPLAYS 次仍失败标为隔离；
- 增强（摘要）失败不阻塞入库，文档先不带摘要写入；原事件记下来，按退避时间放回事件流补做，
  最多 ENRICH_MAX_TRIES 次（入库管道详设第 7、11 节）。
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import secrets
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from bambuflow.ingestion.v1 import events_pb2

# 键的前缀；全量重建用的 worker（BF_REBUILD_WORKER）用 REBUILD_NAMESPACE，
# 与线上模型的 worker 互不覆盖
NAMESPACE = "ingest:"
REBUILD_NAMESPACE = "ingest:rebuild:"
MAX_ATTEMPTS = 5
QUARANTINE_REPLAYS = 3
# 向量化失败只等模型服务恢复，不进死信（入库管道详设第 11 节）
NEVER_DEAD = frozenset({"embed"})
# 没有与本 worker 的嵌入模型一致的待发布版本（重建上线后旧模型的 worker 还没下线，或模型配置有误）：
# 等部署改正后再写，不进死信
NEVER_DEAD_CODES = frozenset({"model_mismatch"})
MESSAGE_LIMIT = 200
# 可以按组件版本重新处理的成功状态（stale 是已有更新的版本，重新处理也会被丢弃）
SUCCEEDED_STATES = frozenset({"written", "unchanged"})
# 增强补做：30 分钟起每次翻倍，共 6 次（约 31 小时）；之后不再补做，文档照常可检索，只是没有摘要
ENRICH_RETRY_BASE = 30 * 60.0
ENRICH_MAX_TRIES = 6


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

    def __init__(
        self,
        client: Any,
        pipeline_version: str,
        now: Callable[[], str] = _now,
        namespace: str = NAMESPACE,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._r = client
        self._clock = clock
        # 增强补做：doc_id → {"n": 失败次数, "due": 下次放回事件流的时间, "event": 原事件}
        self._enrich = namespace + "enrich-retry"
        self._status = namespace + "status:"
        self._dead = namespace + "dead-letters"
        self._attempts = namespace + "attempts"
        self._replays = namespace + "replays"
        # 死信记录处理时的组件版本，修好组件后据此判断要不要重放
        self._pipeline = pipeline_version
        self._now = now

    # ---- worker ----

    def succeeded(self, evt: events_pb2.DocumentChanged, stage: str, state: str) -> None:
        key = self._status + evt.doc_id
        # 记下原事件与组件版本：修好组件后可以按版本或时间段重新处理已成功的文档
        # （入库管道详设第 11 节）
        fields = self._base(evt, stage, state) | {
            "error_code": "",
            "error": "",
            "attempts": 0,
            "pipeline_version": self._pipeline,
            "event": base64.b64encode(evt.SerializeToString()).decode(),
        }
        # 入库批次只属于重放的那个事件；源端来了新事件就不再挂在旧批次下
        if self._event_of(key) != evt.event_id:
            fields["batch_id"] = ""
        self._r.hset(key, mapping=fields)
        self._r.hdel(self._attempts, evt.event_id)
        # 文档的最新内容已经成功（或已有更新的版本），旧的死信与重放计数都作废
        self._r.hdel(self._dead, evt.doc_id)
        self._r.hdel(self._replays, evt.doc_id)

    def failed(self, evt: events_pb2.DocumentChanged, f: Failure) -> bool:
        """记录一次失败，返回是否进了死信（进了死信的消息由调用方确认，不再留在挂起列表）。"""
        attempts = int(self._r.hincrby(self._attempts, evt.event_id, 1))
        dead = not f.retryable or (
            attempts >= MAX_ATTEMPTS
            and f.stage not in NEVER_DEAD
            and f.code not in NEVER_DEAD_CODES
        )
        state = "retrying"
        if dead:
            replays = int(self._r.hget(self._replays, evt.doc_id) or 0)
            quarantined = replays >= QUARANTINE_REPLAYS
            state = "quarantined" if quarantined else "dead_letter"
            first = self._first_failed(evt.doc_id)
            now = self._now()
            self._r.hset(
                self._dead,
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
            self._r.hdel(self._attempts, evt.event_id)
        self._r.hset(
            self._status + evt.doc_id,
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
        raw = self._r.hget(self._dead, doc_id)
        return str(json.loads(raw)["first_failed_at"]) if raw else ""

    def enrich_failed(self, evt: events_pb2.DocumentChanged) -> bool:
        """记一次增强失败，返回之后还会不会补做。"""
        raw = self._r.hget(self._enrich, evt.doc_id)
        n = (json.loads(raw)["n"] if raw else 0) + 1
        if n > ENRICH_MAX_TRIES:
            self._r.hdel(self._enrich, evt.doc_id)
            return False
        item = {
            "n": n,
            "due": self._clock() + ENRICH_RETRY_BASE * 2 ** (n - 1),
            "event": base64.b64encode(evt.SerializeToString()).decode(),
        }
        self._r.hset(self._enrich, evt.doc_id, json.dumps(item))
        return True

    def enrich_ok(self, doc_id: str) -> None:
        self._r.hdel(self._enrich, doc_id)

    def requeue_enrich(self, stream: str) -> int:
        """把到期的增强补做放回事件流，返回条数。放回后把到期时间推到下一个退避点：
        补做再失败时 enrich_failed 会改写它，补做成功时 enrich_ok 删掉它。"""
        now, n = self._clock(), 0
        for doc, raw in self._r.hgetall(self._enrich).items():
            item = json.loads(raw)
            if item["due"] > now:
                continue
            self._r.xadd(stream, {"pb": base64.b64decode(item["event"])})
            item["due"] = now + ENRICH_RETRY_BASE * 2 ** (item["n"] - 1)
            self._r.hset(self._enrich, _s(doc), json.dumps(item))
            n += 1
        return n

    # ---- 管理服务 ----

    def status(self, doc_id: str) -> dict[str, str] | None:
        got = self._r.hgetall(self._status + doc_id)
        return {_s(k): _s(v) for k, v in got.items()} if got else None

    def dead_letters(self) -> list[dict[str, Any]]:
        """全部死信，按 doc_id 排序。"""
        out = [json.loads(v) for v in self._r.hgetall(self._dead).values()]
        return sorted(out, key=lambda d: str(d["doc_id"]))

    def succeeded_docs(self) -> list[dict[str, str]]:
        """已成功入库、记下了原事件的文档状态（之前的版本没记原事件的不在其中），按 doc_id 排序。"""
        out = []
        for key in self._r.scan_iter(match=self._status + "*", count=500):
            st = {_s(k): _s(v) for k, v in self._r.hgetall(key).items()}
            if st.get("event") and st.get("state") in SUCCEEDED_STATES:
                out.append(st)
        return sorted(out, key=lambda d: d["doc_id"])

    def reprocess(self, docs: Iterable[dict[str, str]], stream: str, batch: str) -> int:
        """把已成功文档的原事件放回事件流重新处理，挂在入库批次 batch 下。"""
        n = 0
        for d in docs:
            self._r.xadd(stream, {"pb": base64.b64decode(d["event"])})
            self._r.hset(
                self._status + d["doc_id"],
                mapping={"state": "queued", "batch_id": batch, "updated_at": self._now()},
            )
            n += 1
        return n

    def replay(
        self, letters: Iterable[dict[str, Any]], stream: str, batch: str = ""
    ) -> tuple[str, int]:
        """把死信里的原事件放回事件流，返回入库批次与条数。

        逐条执行、没有放进事务：中途失败时已放回的会被处理，剩下的仍在死信里，重新调用即可；
        同一事件处理两次由 indexing 的幂等键挡住。"""
        batch = batch or new_batch_id()
        n = 0
        for d in letters:
            pb = base64.b64decode(d["event"])
            self._r.xadd(stream, {"pb": pb})
            self._r.hincrby(self._replays, d["doc_id"], 1)
            self._r.hdel(self._dead, d["doc_id"])
            self._r.hset(
                self._status + d["doc_id"],
                mapping={"state": "queued", "batch_id": batch, "updated_at": self._now()},
            )
            n += 1
        return (batch if n else ""), n
