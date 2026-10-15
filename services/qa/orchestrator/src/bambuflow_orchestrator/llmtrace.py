"""LLM 追踪库（技术设计文档 14.2，orchestrator 详设 13.3）。

一次回答的全部模型调用（系统与用户提示词、模型输出原文、用量）与逐句校验结果，
按 trace_id 存进 orchestrator.llm_traces，整体信封加密（附加数据绑定 trace_id）。
普通指标、日志与 trace 只记 ID 和计数，排查某次回答的内容只能到这里查，
且只有 LLM 调试人员（llm_debugger）能看，每次查看由 gateway 写管理审计。

- 收集：请求开始时把 Collector 绑到上下文（bind），模型调用（server._LLM）与逐句校验（flow）往里记；
  上下文变量随 asyncio 任务传递，并发的规划与生成都能记到同一个 Collector。
- 不写：没有配置密钥服务（不能加密就不存）；任何一次调用的输入里有受限级内容（技术设计文档 8.6，
  受限级不进追踪库）；回答没写成回答记录（中途断开）。
- 删除：文档物理删除时（DocumentPurged）删掉 docs 里有它的记录，
  再把完成时间补记进删除凭证（部署配置清单 P2）；过了回答记录的保留期由 purge 一并删除。
"""

from __future__ import annotations

import asyncio
import contextvars
import datetime as dt
import logging
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn, Protocol, cast

import grpc
import psycopg
import redis.asyncio
from google.protobuf.timestamp_pb2 import Timestamp
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from redis.exceptions import RedisError

from bambuflow.admin.v1 import llm_trace_admin_pb2, llm_trace_admin_pb2_grpc
from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.ingestion.v1 import events_pb2, index_writer_pb2
from bambuflow_grounding import Verdict
from bambuflow_platform import envelope
from bambuflow_platform.grpc_server import current_principal

if TYPE_CHECKING:
    # 异步桩的类型只在生成的 .pyi 里；运行时用 IndexWriterServiceStub(aio 通道) 得到的就是它
    from bambuflow.ingestion.v1.index_writer_pb2_grpc import IndexWriterServiceAsyncStub

_log = logging.getLogger(__name__)

LLM_DEBUGGER = "llm_debugger"
_TRACE_ID = re.compile(r"^[0-9a-f]{32}$")


class Generation(Protocol):
    """一次模型调用（llm.ChatGeneration 或 llm.RefusedGeneration）。"""

    model: str
    output: str

    def prompts(self) -> tuple[str, str]: ...

    @property
    def usage(self) -> Any: ...


@dataclass
class _Call:
    use: str
    level: int
    generation: Generation


@dataclass
class Collector:
    """一次回答里的模型调用与校验结果。"""

    calls: list[_Call] = field(default_factory=list)
    verdicts: list[Verdict] = field(default_factory=list)

    def restricted(self) -> bool:
        return any(c.level >= types_pb2.CLASSIFICATION_RESTRICTED for c in self.calls)


_current: contextvars.ContextVar[Collector | None] = contextvars.ContextVar(
    "llm_trace", default=None
)


def bind(c: Collector) -> None:
    """把 c 绑到当前上下文：之后在这个请求里（含它创建的任务）发生的模型调用与校验都记到 c。"""
    _current.set(c)


def note_call(use: str, level: int, generation: Generation) -> None:
    if (c := _current.get()) is not None:
        c.calls.append(_Call(use, level, generation))


def note_verdicts(verdicts: Iterable[Verdict]) -> None:
    if (c := _current.get()) is not None:
        c.verdicts.extend(verdicts)


def docs_of(resp: answer_pb2.AnswerResponse) -> list[str]:
    """这次回答用到的文档：块 ID 的前半段是“数据源:文档”；清单路径列出的文档只有文档 ID。"""
    out: set[str] = set()
    for chunk_id in (*resp.audit.context_chunk_ids, *resp.audit.shown_chunk_ids):
        if "#" in chunk_id:
            out.add(chunk_id.split("#", 1)[0])
    out.update(resp.audit.listed_doc_ids)
    for c in resp.citations:
        out.add(c.doc_id)
    out.discard("")
    return sorted(out)


def build(trace_id: str, answer_id: str, tenant: str, c: Collector) -> llm_trace_admin_pb2.LlmTrace:
    t = llm_trace_admin_pb2.LlmTrace(trace_id=trace_id, answer_id=answer_id, tenant=tenant)
    for call in c.calls:
        system, user = call.generation.prompts()
        u = call.generation.usage
        t.calls.add(
            use=call.use,
            model=call.generation.model,
            level=types_pb2.Classification.ValueType(call.level),
            system_prompt=system,
            user_prompt=user,
            output=call.generation.output,
            prompt_tokens=u.prompt_tokens,
            completion_tokens=u.completion_tokens,
        )
    for v in c.verdicts:
        t.verdicts.add(
            text=v.text, passed=v.passed, reason=v.reason, support=v.support, refs=list(v.refs)
        )
    return t


@dataclass(frozen=True)
class Sealed:
    trace_id: str
    answer_id: str
    tenant: str
    docs: Sequence[str]
    blob: bytes
    wrapped_key: str
    key_id: str
    created_at: dt.datetime | None = None


async def seal(
    t: llm_trace_admin_pb2.LlmTrace, docs: Sequence[str], keys: envelope.AsyncKeyService
) -> Sealed:
    k = await keys.data_key()
    blob = envelope.seal(k.plaintext, t.SerializeToString(), t.trace_id + ":llm_trace")
    return Sealed(t.trace_id, t.answer_id, t.tenant, docs, blob, k.wrapped, k.key_id)


async def restore(s: Sealed, keys: envelope.AsyncKeyService) -> llm_trace_admin_pb2.LlmTrace:
    dek = await keys.unwrap(s.wrapped_key)
    t = llm_trace_admin_pb2.LlmTrace.FromString(
        envelope.unseal(dek, s.blob, s.trace_id + ":llm_trace")
    )
    if s.created_at is not None:
        ts = Timestamp()
        ts.FromDatetime(s.created_at)
        t.created_at.CopyFrom(ts)
    return t


class TracesUnavailableError(Exception):
    pass


class Traces(Protocol):
    async def put(self, s: Sealed) -> None: ...

    async def get(self, trace_id: str) -> Sealed | None: ...

    async def delete_doc(self, keys: Sequence[str]) -> int: ...


class PGTraces:
    def __init__(self, pool: AsyncConnectionPool[psycopg.AsyncConnection[TupleRow]]) -> None:
        self._pool = pool

    async def put(self, s: Sealed) -> None:
        try:
            async with self._pool.connection() as conn:
                await conn.execute(
                    """INSERT INTO orchestrator.llm_traces
                         (trace_id, answer_id, tenant, docs, sealed, sealed_key, key_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (trace_id) DO NOTHING""",
                    (
                        s.trace_id,
                        s.answer_id,
                        s.tenant,
                        list(s.docs),
                        s.blob,
                        s.wrapped_key,
                        s.key_id,
                    ),
                )
        except psycopg.Error as e:
            raise TracesUnavailableError(type(e).__name__) from e

    async def get(self, trace_id: str) -> Sealed | None:
        try:
            async with self._pool.connection() as conn:
                cur = await conn.execute(
                    """SELECT trace_id, answer_id, tenant, docs, sealed, sealed_key, key_id,
                              created_at
                         FROM orchestrator.llm_traces WHERE trace_id = %s""",
                    (trace_id,),
                )
                row = await cur.fetchone()
        except psycopg.Error as e:
            raise TracesUnavailableError(type(e).__name__) from e
        if row is None:
            return None
        return Sealed(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])

    async def delete_doc(self, keys: Sequence[str]) -> int:
        """删掉 docs 里有 keys 任何一个的记录，返回删除的条数。"""
        try:
            async with self._pool.connection() as conn:
                cur = await conn.execute(
                    "DELETE FROM orchestrator.llm_traces WHERE docs && %s::text[]", (list(keys),)
                )
                return cur.rowcount
        except psycopg.Error as e:
            raise TracesUnavailableError(type(e).__name__) from e


async def purge(dsn: str, before: dt.datetime) -> int:
    """删除 before 之前的追踪记录（保留期与回答记录一致）。"""
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cur = await conn.execute(
            "DELETE FROM orchestrator.llm_traces WHERE created_at < %s", (before,)
        )
        return cur.rowcount


Ctx = Any


class AdminService(llm_trace_admin_pb2_grpc.LlmTraceAdminServiceServicer):
    """GET /v1/admin/llm-traces/{trace_id}：只接受带 llm_debugger 角色的管理 Principal。
    不存在、过了保留期或因文档删除被删掉的一律 NOT_FOUND。"""

    def __init__(self, traces: Traces, keys: envelope.AsyncKeyService | None) -> None:
        self._traces, self._keys = traces, keys

    async def GetLlmTrace(
        self, request: llm_trace_admin_pb2.GetLlmTraceRequest, context: Ctx
    ) -> llm_trace_admin_pb2.GetLlmTraceResponse:
        p = current_principal()
        if p is None or LLM_DEBUGGER not in p.roles:
            await _abort(context, grpc.StatusCode.PERMISSION_DENIED, "需要 LLM 调试人员")
        if not _TRACE_ID.match(request.trace_id):
            await _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "trace_id 应为 32 位十六进制")
        try:
            sealed = await self._traces.get(request.trace_id)
        except TracesUnavailableError as e:
            _log.warning("读取 LLM 追踪记录失败", extra={"err": str(e)})
            await _abort(context, grpc.StatusCode.UNAVAILABLE, "暂时不可用")
        keys = self._keys
        if sealed is None or keys is None:
            await _abort(context, grpc.StatusCode.NOT_FOUND, "追踪记录不存在")
        try:
            trace = await restore(sealed, keys)
        except (envelope.DecryptError, OSError, KeyError, ValueError) as e:
            _log.warning("解开 LLM 追踪记录失败", extra={"err": type(e).__name__})
            await _abort(context, grpc.StatusCode.UNAVAILABLE, "暂时不可用")
        return llm_trace_admin_pb2.GetLlmTraceResponse(trace=trace)


async def _abort(context: Ctx, code: grpc.StatusCode, msg: str) -> NoReturn:
    await context.abort(code, msg)
    raise AssertionError("unreachable")


PURGE_STREAM = "ingest:document-purged"
PURGE_GROUP = "orchestrator-llm-traces"
STORE = "llm_traces"


class PurgeConsumer:
    """消费 DocumentPurged：删掉引用过这份文档的追踪记录，再补记删除凭证；都成功才确认。
    失败的消息留在待处理列表里，下一轮重读（数据库、indexing 恢复后自然补上）。"""

    def __init__(
        self,
        rdb: redis.asyncio.Redis,
        traces: Traces,
        receipts: IndexWriterServiceAsyncStub,
        consumer: str,
    ) -> None:
        self._rdb, self._traces, self._receipts, self._consumer = rdb, traces, receipts, consumer

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                await self.ensure_group()
                pending = await self.step("0")
                fresh = await self.step(">")
                backoff = 1.0 if pending or fresh else backoff
            except (RedisError, OSError) as e:
                _log.warning("消费文档删除事件失败，稍后重试", extra={"err": type(e).__name__})
                try:
                    await asyncio.wait_for(stop.wait(), backoff)
                except TimeoutError:
                    pass
                backoff = min(backoff * 2, 30.0)

    async def ensure_group(self) -> None:
        try:
            await self._rdb.xgroup_create(PURGE_STREAM, PURGE_GROUP, id="0", mkstream=True)
        except RedisError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def step(self, start: str) -> int:
        """读一批（start 为 "0" 时读本消费者没确认的，">" 时等新消息最多 5 秒），返回确认的条数。"""
        res = cast(
            list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]],
            await self._rdb.xreadgroup(
                PURGE_GROUP,
                self._consumer,
                {PURGE_STREAM: start},
                count=50,
                block=None if start == "0" else 5000,
            ),
        )
        done = 0
        for _stream, messages in res or []:
            for msg_id, values in messages:
                if await self.handle(values):
                    await self._rdb.xack(PURGE_STREAM, PURGE_GROUP, msg_id)
                    done += 1
        return done

    async def handle(self, values: dict[bytes, bytes]) -> bool:
        raw = values.get(b"pb")
        try:
            evt = events_pb2.DocumentPurged.FromString(raw or b"")
        except ValueError:
            _log.error("丢弃无法解析的文档删除事件")
            return True
        if not evt.doc_id or not evt.receipt_id:
            _log.error("文档删除事件缺少 doc_id 或 receipt_id，丢弃")
            return True
        try:
            n = await self._traces.delete_doc([f"{evt.source_id}:{evt.doc_id}", evt.doc_id])
        except TracesUnavailableError as e:
            _log.warning("删除 LLM 追踪记录失败，稍后重试", extra={"err": str(e)})
            return False
        now = Timestamp()
        now.GetCurrentTime()
        try:
            await self._receipts.RecordPurge(
                index_writer_pb2.RecordPurgeRequest(
                    receipt_id=evt.receipt_id, store=STORE, completed_at=now
                ),
                timeout=10,
            )
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                _log.error("删除凭证不存在，丢弃", extra={"receipt_id": evt.receipt_id})
                return True
            _log.warning("补记删除凭证失败，稍后重试", extra={"code": e.code().name})
            return False
        _log.info(
            "已删除引用过该文档的 LLM 追踪记录",
            extra={"doc_id": evt.doc_id, "receipt_id": evt.receipt_id, "deleted": n},
        )
        return True
