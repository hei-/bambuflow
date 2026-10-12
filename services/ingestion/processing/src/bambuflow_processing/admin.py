"""入库运维接口 IngestAdminService（入库管道详设第 11 节、14.4）：
入库状态、死信与隔离文档、按条件重放。

只经 gateway 的管理接口调用，只接受平台运维的管理 Principal。
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import datetime as dt
import logging
from typing import TYPE_CHECKING, Any, NoReturn

import grpc
import redis
from google.protobuf.timestamp_pb2 import Timestamp

from bambuflow.admin.v1 import ingest_admin_pb2, ingest_admin_pb2_grpc
from bambuflow_platform.grpc_server import current_principal
from bambuflow_processing.state import IngestState, new_batch_id
from bambuflow_processing.worker import STREAM

if TYPE_CHECKING:
    Ctx = grpc.aio.ServicerContext[Any, Any]
else:
    Ctx = Any

_log = logging.getLogger(__name__)

PLATFORM_OPERATOR = "platform_operator"
STAGES = frozenset({"fetch", "parse", "mask", "enrich", "embed", "write"})
DEFAULT_PAGE = 50
MAX_PAGE = 200
MAX_REPLAY_DOCS = 500
MAX_REASON = 500
MAX_ID = 512
MAX_VERSION = 64
# 一次最多重新处理的已成功文档：更多的应分批，或改走全量重建
MAX_REPROCESS = 10000


async def abort(context: Ctx, code: grpc.StatusCode, msg: str) -> NoReturn:
    await context.abort(code, msg)
    raise AssertionError("unreachable")


def _ts(iso: str) -> Timestamp:
    t = Timestamp()
    if iso:
        t.FromDatetime(dt.datetime.fromisoformat(iso))
    return t


def _error(code: str, message: str) -> ingest_admin_pb2.IngestError | None:
    return ingest_admin_pb2.IngestError(code=code, message=message) if code else None


def _letter(d: dict[str, Any]) -> ingest_admin_pb2.DeadLetter:
    return ingest_admin_pb2.DeadLetter(
        doc_id=d["doc_id"],
        source_id=d["source_id"],
        event_id=d["event_id"],
        stage=d["stage"],
        error=_error(d["error_code"], d["error"]),
        pipeline_version=d["pipeline_version"],
        attempts=int(d["attempts"]),
        replays=int(d["replays"]),
        quarantined=bool(d["quarantined"]),
        first_failed_at=_ts(d["first_failed_at"]),
        last_failed_at=_ts(d["last_failed_at"]),
    )


def _matches(d: dict[str, Any], source_id: str, stage: str, error_code: str) -> bool:
    return (
        (not source_id or d["source_id"] == source_id)
        and (not stage or d["stage"] == stage)
        and (not error_code or d["error_code"] == error_code)
    )


def _in_window(at: str, start: dt.datetime | None, end: dt.datetime | None) -> bool:
    """at 是状态里的 ISO 时间；没有记录时间的不算在任何时间段内。"""
    if start is None and end is None:
        return True
    if not at:
        return False
    t = dt.datetime.fromisoformat(at)
    return (start is None or t >= start) and (end is None or t <= end)


def _bound(t: Timestamp, present: bool) -> dt.datetime | None:
    return t.ToDatetime(dt.UTC) if present else None


def _token(doc_id: str) -> str:
    return base64.urlsafe_b64encode(doc_id.encode()).decode()


class IngestAdmin(ingest_admin_pb2_grpc.IngestAdminServiceServicer):
    def __init__(self, state: IngestState) -> None:
        self._state = state

    async def _operator(self, context: Ctx) -> None:
        p = current_principal()
        if p is None or PLATFORM_OPERATOR not in p.roles:
            await abort(context, grpc.StatusCode.PERMISSION_DENIED, "需要平台运维")

    async def _call(self, context: Ctx, fn: Any, *args: Any) -> Any:
        # Redis 客户端是同步的（与 worker 共用 IngestState），放到线程里不阻塞事件循环
        try:
            return await asyncio.to_thread(fn, *args)
        except redis.RedisError as e:
            _log.warning("读写入库状态失败", extra={"err": type(e).__name__})
            await abort(context, grpc.StatusCode.UNAVAILABLE, "暂时不可用")

    async def _stage(self, context: Ctx, stage: str) -> None:
        if stage and stage not in STAGES:
            await abort(context, grpc.StatusCode.INVALID_ARGUMENT, "stage 不认识")

    async def GetIngestStatus(
        self, request: ingest_admin_pb2.GetIngestStatusRequest, context: Ctx
    ) -> ingest_admin_pb2.GetIngestStatusResponse:
        await self._operator(context)
        if not request.doc_id or len(request.doc_id) > MAX_ID:
            await abort(context, grpc.StatusCode.INVALID_ARGUMENT, "doc_id 不合法")
        s = await self._call(context, self._state.status, request.doc_id)
        if s is None:
            await abort(context, grpc.StatusCode.NOT_FOUND, "没有这份文档的入库记录")
        return ingest_admin_pb2.GetIngestStatusResponse(
            doc_id=s["doc_id"],
            source_id=s.get("source_id", ""),
            event_id=s.get("event_id", ""),
            stage=s.get("stage", ""),
            state=s["state"],
            last_error=_error(s.get("error_code", ""), s.get("error", "")),
            attempts=int(s.get("attempts") or 0),
            ingest_batch_id=s.get("batch_id", ""),
            updated_at=_ts(s.get("updated_at", "")),
        )

    async def ListDeadLetters(
        self, request: ingest_admin_pb2.ListDeadLettersRequest, context: Ctx
    ) -> ingest_admin_pb2.ListDeadLettersResponse:
        await self._operator(context)
        await self._stage(context, request.stage)
        size = request.page_size or DEFAULT_PAGE
        if not 0 < size <= MAX_PAGE:
            await abort(context, grpc.StatusCode.INVALID_ARGUMENT, f"page_size 须在 1–{MAX_PAGE}")
        try:
            after = base64.urlsafe_b64decode(request.page_token).decode()
        except (binascii.Error, UnicodeDecodeError):
            await abort(context, grpc.StatusCode.INVALID_ARGUMENT, "page_token 不合法")
        letters = [
            d
            for d in await self._call(context, self._state.dead_letters)
            if _matches(d, request.source_id, request.stage, request.error_code)
            and (not request.quarantined_only or d["quarantined"])
            and str(d["doc_id"]) > after
        ]
        page = letters[:size]
        return ingest_admin_pb2.ListDeadLettersResponse(
            dead_letters=[_letter(d) for d in page],
            next_page_token=_token(page[-1]["doc_id"]) if len(letters) > size else "",
        )

    async def Replay(
        self, request: ingest_admin_pb2.ReplayRequest, context: Ctx
    ) -> ingest_admin_pb2.ReplayResponse:
        await self._operator(context)
        await self._stage(context, request.stage)
        if not request.reason.strip() or len(request.reason) > MAX_REASON:
            await abort(
                context, grpc.StatusCode.INVALID_ARGUMENT, f"reason 必填，最多 {MAX_REASON} 字"
            )
        named = set(request.doc_ids)
        if len(named) > MAX_REPLAY_DOCS:
            await abort(
                context, grpc.StatusCode.INVALID_ARGUMENT, f"doc_ids 最多 {MAX_REPLAY_DOCS} 个"
            )
        start = _bound(getattr(request, "from"), request.HasField("from"))
        end = _bound(request.to, request.HasField("to"))
        window = start is not None or end is not None
        version = request.pipeline_version
        if len(version) > MAX_VERSION or (start and end and start > end):
            await abort(
                context, grpc.StatusCode.INVALID_ARGUMENT, "pipeline_version 或时间段不合法"
            )
        if not (
            named or request.source_id or request.stage or request.error_code or window or version
        ):
            # 不给条件就是全部重放：这种操作要明确写出条件，不能靠空请求触发
            await abort(context, grpc.StatusCode.INVALID_ARGUMENT, "至少给一个条件或 doc_ids")
        if request.include_succeeded and not (version or window):
            # 重新处理已成功的文档开销大：要说清楚是哪个组件版本或哪段时间的
            await abort(
                context,
                grpc.StatusCode.INVALID_ARGUMENT,
                "重新处理已成功的文档须给组件版本或时间段",
            )
        matched = [
            d
            for d in await self._call(context, self._state.dead_letters)
            if _matches(d, request.source_id, request.stage, request.error_code)
            and (not named or d["doc_id"] in named)
            and (not version or d.get("pipeline_version") == version)
            and _in_window(str(d.get("last_failed_at", "")), start, end)
        ]
        # 隔离文档只有点名才重放（入库管道详设第 11 节：不再自动重试，等人工处理）
        chosen = [d for d in matched if not d["quarantined"] or d["doc_id"] in named]
        again: list[dict[str, str]] = []
        if request.include_succeeded and not request.stage and not request.error_code:
            dead = {d["doc_id"] for d in chosen}
            again = [
                d
                for d in await self._call(context, self._state.succeeded_docs)
                if _matches(d, request.source_id, "", "")
                and (not named or d["doc_id"] in named)
                and (not version or d.get("pipeline_version") == version)
                and _in_window(d.get("updated_at", ""), start, end)
                and d["doc_id"] not in dead
            ]
            if len(again) > MAX_REPROCESS:
                await abort(
                    context,
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"符合条件的已成功文档有 {len(again)} 份，"
                    f"超过一次 {MAX_REPROCESS} 份，请缩小条件",
                )
        batch = new_batch_id()
        _, n = await self._call(context, self._state.replay, chosen, STREAM, batch)
        m = await self._call(context, self._state.reprocess, again, STREAM, batch)
        _log.info("已重放", extra={"ingest_batch_id": batch, "replayed": n + m, "reprocessed": m})
        return ingest_admin_pb2.ReplayResponse(
            ingest_batch_id=batch if n + m else "",
            replayed=n + m,
            reprocessed=m,
            skipped_quarantined=len(matched) - len(chosen),
        )
