"""回答记录导出（evaluation 详设 7.1、7.2）：给 evaluation 做 A/B 分析与在线抽样评审。

服务间接口，不带用户 Principal：靠服务网格的 mTLS 与网络策略只放行 evaluation
（技术设计文档 12.6）。导出的提问人只是租户内稳定的假名，不导出用户 ID 与问题原文；
涉及受限级数据、正文未保存的记录不带正文。
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import logging
from typing import TYPE_CHECKING, Any, NoReturn

import grpc
import psycopg
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from bambuflow.answer.v1 import answer_pb2, records_pb2, records_pb2_grpc

if TYPE_CHECKING:
    Ctx = grpc.aio.ServicerContext[Any, Any]
else:
    Ctx = Any

_log = logging.getLogger(__name__)

SERVICE = "bambuflow.answer.v1.AnswerRecordService"
DEFAULT_PAGE = 500
MAX_PAGE = 1000
MAX_SPAN = dt.timedelta(days=31)


def user_key(tenant: str, asker: str) -> str:
    """提问人的假名：同一租户内稳定，A/B 按它聚合（evaluation 详设 7.2）。"""
    return hashlib.sha256(f"{tenant}\x00{asker}".encode()).hexdigest()[:16]


def _offset(token: str) -> int | None:
    if not token:
        return 0
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
    except ValueError:
        return None
    if not raw.startswith("o:") or not raw[2:].isdigit():
        return None
    return int(raw[2:])


def _token(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode().rstrip("=")


class RecordService(records_pb2_grpc.AnswerRecordServiceServicer):
    def __init__(self, pool: AsyncConnectionPool[psycopg.AsyncConnection[TupleRow]]) -> None:
        self._pool = pool

    async def _invalid(self, context: Ctx, msg: str) -> NoReturn:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
        raise AssertionError("unreachable")

    async def ListAnswerRecords(
        self, request: records_pb2.ListAnswerRecordsRequest, context: Ctx
    ) -> records_pb2.ListAnswerRecordsResponse:
        size = request.page_size or DEFAULT_PAGE
        offset = _offset(request.page_token)
        since, until = request.since.ToDatetime(dt.UTC), request.until.ToDatetime(dt.UTC)
        if not request.tenant or not 1 <= size <= MAX_PAGE or offset is None:
            await self._invalid(context, "tenant、page_size 或 page_token 不合法")
        if not (request.HasField("since") and request.HasField("until")):
            await self._invalid(context, "since 与 until 必填")
        if not since < until or until - since > MAX_SPAN:
            await self._invalid(context, "时间范围不合法或超过 31 天")
        sql = """SELECT r.answer_id, r.asker, r.created_at, r.answer, r.body_withheld,
                        r.latency_ms, r.prompt_tokens, r.completion_tokens,
                        COALESCE((SELECT array_agg(f.rating ORDER BY f.created_at)
                                    FROM orchestrator.answer_feedback f
                                   WHERE f.answer_id = r.answer_id), '{}'),
                        r.question,
                        COALESCE((SELECT array_agg(f.comment ORDER BY f.created_at)
                                    FROM orchestrator.answer_feedback f
                                   WHERE f.answer_id = r.answer_id AND f.comment <> ''), '{}'),
                        r.access_denied
                   FROM orchestrator.answer_records r
                  WHERE r.tenant = %s AND r.created_at >= %s AND r.created_at < %s"""
        args: list[Any] = [request.tenant, since, until]
        if request.experiment_id:
            sql += " AND r.experiment_id = %s"
            args.append(request.experiment_id)
        sql += " ORDER BY r.created_at, r.answer_id OFFSET %s LIMIT %s"
        args += [offset, size + 1]
        try:
            async with self._pool.connection() as conn:
                cur = await conn.execute(sql, args)
                rows = await cur.fetchall()
        except (psycopg.Error, PoolTimeout) as e:
            _log.warning("导出回答记录失败", extra={"err": type(e).__name__})
            await context.abort(grpc.StatusCode.UNAVAILABLE, "回答记录暂不可用")
        out = records_pb2.ListAnswerRecordsResponse()
        if len(rows) > size:
            rows, out.next_page_token = rows[:size], _token(offset + size)
        for (
            answer_id,
            asker,
            created,
            raw,
            withheld,
            latency,
            prompt,
            completion,
            fb,
            question,
            comments,
            denied,
        ) in rows:
            ans = answer_pb2.AnswerResponse.FromString(raw)
            rec = out.records.add(
                answer_id=answer_id,
                user_key=user_key(request.tenant, asker),
                status=ans.status,
                experiment_id=ans.why.experiment_id,
                experiment_arm=ans.why.experiment_arm,
                latency_ms=latency,
                usage=answer_pb2.TokenUsage(prompt_tokens=prompt, completion_tokens=completion),
                feedback=fb,
                body_withheld=withheld,
                coverage=ans.confidence.coverage,
                access_denied=denied,
                canary_releases=ans.why.canary_releases,
            )
            rec.created_at.FromDatetime(created)
            if request.include_answer:
                # 问题与反馈说明存的都是掩码后的文本（受限级的回答也存掩码问题）
                rec.masked_question = question
                rec.feedback_comments.extend(comments)
                if not withheld:
                    rec.answer.CopyFrom(ans)
        return out
