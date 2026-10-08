"""回答记录与反馈（技术设计文档 13.1、9.2、F8），存在 orchestrator 自有的 orchestrator schema。

回答记录是回放（GET /v1/answers/{answer_id}）的依据：answer_id、租户、提问人、时间、
回答契约（不含只给审计的 audit 字段）与 why。为什么由 orchestrator 存而不是 gateway：
回答在这里组装完成，回放时的权限复核也要在这里按回答的结构（句子与引用的依赖）去掉不可见的部分。

保存什么（技术设计文档 8.6 密级路由、9.2 查询与回答记录）：
- 问题原文做 PII 掩码后保存；agent 子问题由问题改写而来，同样掩码。
- 回答涉及受限级数据（任一引用或列出的文档为受限）时不保存正文：问题、句子、块、引文、标题都不存，
  只留 status、answer_kind、why 里的版本号与降级项、引用的块和文档标识。设计对受限级的回放承诺是
  “保存加密的审计快照”（部署配置清单 P3），加密快照还没有实现，在那之前宁可不存。
- 保留期：设计待合规确认（部署配置清单 N4 默认保留到年限确定为止），暂不清理。

反馈只挂在回答记录下，只有提问人本人可以提交；说明文字掩码后保存。幂等键与回答同库、同一事务判重，
不依赖 Redis：重复提交返回原结果，内容不同报冲突。
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import secrets
from dataclasses import dataclass
from typing import Protocol

import psycopg
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow_orchestrator import redact

MAX_COMMENT_CHARS = 500
MAX_IDEMPOTENCY_KEY = 128

RATINGS = {
    answer_pb2.FEEDBACK_RATING_USEFUL: "useful",
    answer_pb2.FEEDBACK_RATING_WRONG: "wrong",
    answer_pb2.FEEDBACK_RATING_BAD_CITATION: "bad_citation",
}
_RATING_OF = {v: k for k, v in RATINGS.items()}

ARMS = {
    answer_pb2.EXPERIMENT_ARM_CONTROL: "control",
    answer_pb2.EXPERIMENT_ARM_TREATMENT: "treatment",
    answer_pb2.EXPERIMENT_ARM_EXCLUDED: "excluded",
}


class RecordsUnavailableError(Exception):
    pass


class FeedbackConflictError(Exception):
    """同一提交人的同一幂等键对应了不同的反馈内容。"""


@dataclass(frozen=True)
class Record:
    answer_id: str
    tenant: str
    # 提问人（Principal.sub）与代用户调用的业务系统或 agent
    asker: str
    actor: str
    purpose: str
    # 已掩码；body_withheld 时为空
    question: str
    body_withheld: bool
    answer: answer_pb2.AnswerResponse
    sentence_count: int
    created_at: dt.datetime | None = None
    # 从收到请求到回答完整的耗时与本次全部 LLM 调用的用量（A/B 护栏，evaluation 详设 7.2）
    latency_ms: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0


def _restricted(c: types_pb2.Classification) -> bool:
    return c == types_pb2.CLASSIFICATION_RESTRICTED


def build(
    *,
    answer_id: str,
    tenant: str,
    asker: str,
    actor: str,
    purpose: str,
    question: str,
    resp: answer_pb2.AnswerResponse,
    listed_restricted: bool,
    latency_ms: int = 0,
    usage: answer_pb2.TokenUsage | None = None,
) -> Record:
    """由返回给调用方的回答生成记录。

    listed_restricted：清单路径列出的文档里有受限级（列表项本身不带密级）。"""
    ans = answer_pb2.AnswerResponse()
    ans.CopyFrom(resp)
    ans.ClearField("audit")
    for r in ans.why.agent_rounds:
        for c in r.calls:
            c.sub_question = redact.mask(c.sub_question)
    withheld = listed_restricted or any(_restricted(c.classification) for c in ans.citations)
    if withheld:
        ans = _slim(ans)
    return Record(
        answer_id=answer_id,
        tenant=tenant,
        asker=asker,
        actor=actor,
        purpose=purpose,
        question="" if withheld else redact.mask(question),
        body_withheld=withheld,
        answer=ans,
        sentence_count=len(resp.sentences),
        latency_ms=latency_ms,
        prompt_tokens=usage.prompt_tokens if usage else 0,
        completion_tokens=usage.completion_tokens if usage else 0,
    )


def _slim(a: answer_pb2.AnswerResponse) -> answer_pb2.AnswerResponse:
    """受限级回答只留标识与版本：句子、引文、标题、子问题、图路径上的实体名称都是正文。"""
    out = answer_pb2.AnswerResponse(
        answer_id=a.answer_id,
        trace_id=a.trace_id,
        status=a.status,
        answer_kind=a.answer_kind,
        conversation_id=a.conversation_id,
    )
    out.why.CopyFrom(a.why)
    out.why.ClearField("graph_path")
    for r in out.why.agent_rounds:
        for c in r.calls:
            c.sub_question = ""
    out.citations.extend(
        answer_pb2.Citation(
            ref_id=c.ref_id,
            chunk_id=c.chunk_id,
            doc_id=c.doc_id,
            doc_version=c.doc_version,
            classification=c.classification,
            source_type=c.source_type,
        )
        for c in a.citations
    )
    return out


@dataclass(frozen=True)
class FeedbackInput:
    answer_id: str
    rating: str
    sentence_index: int | None
    # 已掩码
    comment: str
    idempotency_key: str
    # 请求内容的摘要（掩码前），判断同一幂等键是否对应同样的内容
    digest: str


@dataclass(frozen=True)
class Feedback:
    feedback_id: str
    answer_id: str
    rating: str
    sentence_index: int | None
    created_at: dt.datetime
    digest: str

    def proto(self) -> answer_pb2.Feedback:
        out = answer_pb2.Feedback(
            feedback_id=self.feedback_id,
            answer_id=self.answer_id,
            rating=_RATING_OF[self.rating],
        )
        if self.sentence_index is not None:
            out.sentence_index = self.sentence_index
        out.created_at.FromDatetime(self.created_at)
        return out


def feedback_input(req: answer_pb2.CreateFeedbackRequest) -> FeedbackInput | None:
    """校验反馈请求；不合法时返回 None。句子下标是否越界要等读到回答记录后再查。"""
    rating = RATINGS.get(req.rating)
    comment = req.comment.strip()
    key = req.idempotency_key
    index = req.sentence_index if req.HasField("sentence_index") else None
    if (
        rating is None
        or len(comment) > MAX_COMMENT_CHARS
        or not key
        or len(key) > MAX_IDEMPOTENCY_KEY
        or (index is not None and index < 0)
    ):
        return None
    digest = hashlib.sha256(
        json.dumps([req.answer_id, rating, index, comment], ensure_ascii=False).encode()
    ).hexdigest()
    return FeedbackInput(req.answer_id, rating, index, redact.mask(comment), key, digest)


def new_feedback_id() -> str:
    return "fb_" + secrets.token_hex(12)


class Records(Protocol):
    """回答记录与反馈的读写；不可用时抛 RecordsUnavailableError。"""

    async def put(self, rec: Record) -> None: ...

    async def get(self, answer_id: str) -> Record | None: ...

    async def add_feedback(self, tenant: str, submitter: str, fb: FeedbackInput) -> Feedback:
        """写入反馈；同一提交人的同一幂等键已存在时返回已有反馈，
        内容不同时抛 FeedbackConflictError。"""
        ...


class PGRecords:
    """Records 的 PostgreSQL 实现。每次操作的超时由调用方按请求剩余时间控制。"""

    def __init__(self, pool: AsyncConnectionPool[psycopg.AsyncConnection[TupleRow]]) -> None:
        self._pool = pool

    async def put(self, rec: Record) -> None:
        try:
            async with self._pool.connection() as conn:
                # 同一 answer_id 只会来自同一次回答；批量条目重试时以最后一次为准，
                # 但只覆盖同一提问人的记录
                await conn.execute(
                    """INSERT INTO orchestrator.answer_records
                         (answer_id, tenant, asker, actor, purpose, question, body_withheld,
                          sentence_count, answer, experiment_id, experiment_arm, latency_ms,
                          prompt_tokens, completion_tokens)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (answer_id) DO UPDATE SET
                         question = EXCLUDED.question, body_withheld = EXCLUDED.body_withheld,
                         sentence_count = EXCLUDED.sentence_count, answer = EXCLUDED.answer,
                         experiment_id = EXCLUDED.experiment_id,
                         experiment_arm = EXCLUDED.experiment_arm,
                         latency_ms = EXCLUDED.latency_ms, prompt_tokens = EXCLUDED.prompt_tokens,
                         completion_tokens = EXCLUDED.completion_tokens
                       WHERE answer_records.tenant = EXCLUDED.tenant
                         AND answer_records.asker = EXCLUDED.asker""",
                    (
                        rec.answer_id,
                        rec.tenant,
                        rec.asker,
                        rec.actor,
                        rec.purpose,
                        rec.question,
                        rec.body_withheld,
                        rec.sentence_count,
                        rec.answer.SerializeToString(),
                        rec.answer.why.experiment_id,
                        ARMS.get(rec.answer.why.experiment_arm, ""),
                        rec.latency_ms,
                        rec.prompt_tokens,
                        rec.completion_tokens,
                    ),
                )
        except (psycopg.Error, PoolTimeout) as e:
            raise RecordsUnavailableError(type(e).__name__) from e

    async def get(self, answer_id: str) -> Record | None:
        try:
            async with self._pool.connection() as conn:
                cur = await conn.execute(
                    """SELECT tenant, asker, actor, purpose, question, body_withheld,
                              sentence_count, answer, created_at
                         FROM orchestrator.answer_records WHERE answer_id = %s""",
                    (answer_id,),
                )
                row = await cur.fetchone()
        except (psycopg.Error, PoolTimeout) as e:
            raise RecordsUnavailableError(type(e).__name__) from e
        if row is None:
            return None
        tenant, asker, actor, purpose, question, withheld, count, raw, created = row
        return Record(
            answer_id=answer_id,
            tenant=tenant,
            asker=asker,
            actor=actor,
            purpose=purpose,
            question=question,
            body_withheld=withheld,
            answer=answer_pb2.AnswerResponse.FromString(raw),
            sentence_count=count,
            created_at=created,
        )

    async def add_feedback(self, tenant: str, submitter: str, fb: FeedbackInput) -> Feedback:
        try:
            async with self._pool.connection() as conn:
                cur = await conn.execute(
                    """INSERT INTO orchestrator.answer_feedback
                          (feedback_id, answer_id, tenant, submitter, rating, sentence_index,
                           comment, idempotency_key, request_digest)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tenant, submitter, idempotency_key) DO NOTHING
                        RETURNING feedback_id, answer_id, rating, sentence_index, created_at,
                                  request_digest""",
                    (
                        new_feedback_id(),
                        fb.answer_id,
                        tenant,
                        submitter,
                        fb.rating,
                        fb.sentence_index,
                        fb.comment,
                        fb.idempotency_key,
                        fb.digest,
                    ),
                )
                row = await cur.fetchone()
                if row is None:
                    cur = await conn.execute(
                        """SELECT feedback_id, answer_id, rating, sentence_index, created_at,
                                  request_digest
                             FROM orchestrator.answer_feedback
                            WHERE tenant = %s AND submitter = %s AND idempotency_key = %s""",
                        (tenant, submitter, fb.idempotency_key),
                    )
                    row = await cur.fetchone()
        except (psycopg.Error, PoolTimeout) as e:
            raise RecordsUnavailableError(type(e).__name__) from e
        if row is None:
            # 插入因冲突跳过、随后又读不到：只可能是并发删除，按不可用处理
            raise RecordsUnavailableError("feedback vanished")
        out = Feedback(*row)
        if out.digest != fb.digest:
            raise FeedbackConflictError
        return out
