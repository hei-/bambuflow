"""回放：逐题以提问身份调用 retrieval 与 orchestrator，收集判定输入。

只经 contracts 生成的客户端调用，不 import 其他服务的代码（技术设计文档 12.2）。
"""

from __future__ import annotations

import datetime as dt
import logging
import re
import secrets
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

import grpc

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_evaluation.golden import Question
from bambuflow_evaluation.llm_judge import NOT_JUDGED, Judge, JudgeError, Judgment
from bambuflow_evaluation.metrics import Outcome
from bambuflow_evaluation.signer import Signer
from bambuflow_grounding import SUPPORT_THRESHOLD, KServeNLI
from bambuflow_platform.grpc_server import EVAL_CONFIG_HEADER, EVAL_INDEX_HEADER

if TYPE_CHECKING:
    from bambuflow.answer.v1.answer_pb2_grpc import AnswerServiceAsyncStub
    from bambuflow.retrieval.v1.retrieval_pb2_grpc import RetrievalServiceAsyncStub

_log = logging.getLogger(__name__)

MARKER = re.compile(r"\[S\d+\]")
TOP_K = 10
# 与 gateway 给单跳问答的 deadline 相同（gateway 详设），评的就是线上行为
ANSWER_DEADLINE = 4.0
CALL_DEADLINE = 5.0


class ReplayTrail(Protocol):
    """回放请求的审计（audit.Writer.replay）；写不进去抛 audit.AuditUnavailableError。"""

    async def replay(self, records: list[dict[str, Any]]) -> None: ...


@dataclass
class Config:
    """评测配置里由被测系统给出的部分（评测详设 4.1），从回答的 why 里收集。"""

    versions: dict[str, set[str]] = field(default_factory=dict)

    def add(self, why: answer_pb2.Why) -> None:
        for k in (
            "index_version",
            "policy_version",
            "pipeline_version",
            "routing_version",
            "embedding_model",
            "generation_model",
            "prompt_version",
            "nli_model",
        ):
            v = getattr(why, k)
            if v:
                self.versions.setdefault(k, set()).add(v)


def _status(s: answer_pb2.Status) -> str:
    return answer_pb2.Status.Name(s).removeprefix("STATUS_").lower()


class Runner:
    def __init__(
        self,
        retrieval: RetrievalServiceAsyncStub,
        orchestrator: AnswerServiceAsyncStub,
        signer: Signer,
        judge_nli: KServeNLI,
        index_version: str = "",
        llm: Judge | None = None,
        config_version: str = "",
        trail: ReplayTrail | None = None,
        gate_id: str = "",
    ) -> None:
        self._retrieval = retrieval
        # 门禁评测待发布版本时指定的索引版本（evaluation 详设 4.2）：
        # 经 metadata 传给评测副本，空 = 线上版本
        self._index_version = index_version
        # 配置发布的门禁评测的候选配置版本（governance 详设 4.1 第 ③④ 步）：
        # 同样经 metadata 传给评测副本，它们向 governance 按版本取候选，空 = 生效的配置
        self._config_version = config_version
        self._orchestrator = orchestrator
        self._signer = signer
        self._nli = judge_nli
        self.config = Config()
        self.judge_model = ""
        # LLM 评审（评测详设 6.2）；不配置时不算 Faithfulness 与答案正确率
        self._llm = llm
        self.llm_judge = llm.version if llm else ""
        # 评测回放的请求照常写审计、带评测标记（evaluation 详设 4.2）：评测副本的请求不经 gateway，
        # 由这里每跑完一遍写一段；没配置时（命令行的 make eval）不写
        self._trail, self._gate_id = trail, gate_id
        self._replayed: list[dict[str, Any]] = []

    async def run(self, questions: Sequence[Question]) -> list[Outcome]:
        self._replayed = []
        out = [await self.one(q) for q in questions]
        if self._trail is not None:
            await self._trail.replay(self._replayed)
        return out

    def _record(
        self,
        q: Question,
        answer_id: str,
        latency: float,
        resp: answer_pb2.AnswerResponse | None,
        error: str = "",
    ) -> None:
        """一道题的回放审计，字段与 gateway 的入口审计一致（gateway internal/audit.Record）。"""
        rec: dict[str, Any] = {
            "time": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
            "trace_id": answer_id,
            "answer_id": answer_id,
            "sub": q.asker.sub,
            "tenant": q.asker.tenant,
            "endpoint": "eval-replay",
            "status": 200 if resp is not None else 500,
            "latency_ms": int(latency),
            "eval": True,
            "gate_id": self._gate_id,
        }
        if error:
            rec["reason"] = error
        if resp is not None:
            w = resp.why
            rec |= {
                "candidate_chunk_ids": list(resp.audit.candidate_chunk_ids),
                "context_chunk_ids": list(resp.audit.context_chunk_ids),
                "shown_chunk_ids": list(resp.audit.shown_chunk_ids),
                "answer_status": _status(resp.status),
                "versions": {
                    "index_version": w.index_version,
                    "policy_version": w.policy_version,
                    "generation_model": w.generation_model,
                    "prompt_version": w.prompt_version,
                    "pipeline_version": w.pipeline_version,
                    "routing_version": w.routing_version,
                },
            }
        self._replayed.append(rec)

    async def one(self, q: Question) -> Outcome:
        token = self._signer.sign(q.asker)

        def md(purpose: str) -> tuple[tuple[str, str], ...]:
            out: tuple[tuple[str, str], ...] = (
                ("authorization", "Bearer " + token),
                ("x-call-purpose", purpose),
            )
            if self._index_version:
                out += ((EVAL_INDEX_HEADER, self._index_version),)
            if self._config_version:
                out += ((EVAL_CONFIG_HEADER, self._config_version),)
            return out

        # must_cite 在当前索引里已不存在（NOT_FOUND）的题标为待复核，不计入指标（评测详设 3.3）
        pending = False
        if q.must_cite:
            access = await self._retrieval.CheckAccess(
                retrieval_pb2.CheckAccessRequest(chunk_ids=q.must_cite),
                metadata=md("search"),
                timeout=CALL_DEADLINE,
            )
            pending = any(r.reason == retrieval_pb2.ACCESS_REASON_NOT_FOUND for r in access.results)

        found = await self._retrieval.Search(
            retrieval_pb2.SearchRequest(
                queries=[retrieval_pb2.SubQuery(id="q1", text=q.question)], top_k=TOP_K
            ),
            metadata=md("search"),
            timeout=CALL_DEADLINE,
        )
        top10 = tuple(h.chunk_id for h in found.hits)

        start = time.perf_counter()
        answer_id = "ans_eval" + secrets.token_hex(8)
        try:
            resp = await self._orchestrator.Answer(
                answer_pb2.AnswerRequest(question=q.question),
                metadata=(*md("batch"), ("x-answer-id", answer_id)),
                timeout=ANSWER_DEADLINE,
            )
        except grpc.aio.AioRpcError as e:
            latency = (time.perf_counter() - start) * 1000
            self._record(q, answer_id, latency, None, e.code().name.lower())
            return Outcome(
                q.id,
                q.asker.tenant,
                q.bucket,
                q.expected_status,
                "error:" + e.code().name.lower(),
                q.must_cite,
                top10,
                (),
                (),
                (),
                pending,
                (),
                latency,
            )
        latency = (time.perf_counter() - start) * 1000
        self._record(q, answer_id, latency, resp)
        self.config.add(resp.why)

        cited = tuple(c.chunk_id for c in resp.citations)
        # 越权不只看引用：无权块出现在候选里也说明过滤有漏洞（评测详设 6.1）
        violations = tuple(sorted(set(q.must_not_cite) & (set(cited) | set(top10))))
        grounded = await self._grounded(resp)
        judged = await self._judge(q, resp)
        return Outcome(
            qid=q.id,
            tenant=q.asker.tenant,
            bucket=q.bucket,
            expected_status=q.expected_status,
            status=_status(resp.status),
            must_cite=q.must_cite,
            top10=top10,
            cited=cited,
            sentence_refs=tuple(bool(s.ref_ids) for s in _claims(resp)),
            sentence_grounded=grounded,
            pending=pending,
            violations=violations,
            latency_ms=latency,
            faithful=judged.faithful,
            key_points_covered=judged.key_points,
            confidence_features=_features(resp),
            confidence_score=resp.confidence.score,
        )

    async def _judge(self, q: Question, resp: answer_pb2.AnswerResponse) -> Judgment:
        """LLM 评审：只评已回答的；应回答却没回答的题，要点一条都算没覆盖（不调用评审）。
        带受限级引用的回答不评：还没有按密级选模型（H22），受限级内容不能发给评审模型。"""
        if self._llm is None:
            return NOT_JUDGED
        points = q.key_points if q.expected_status == "answered" else ()
        if resp.status != answer_pb2.STATUS_ANSWERED:
            return Judgment(None, tuple(False for _ in points))
        if any(c.classification == types_pb2.CLASSIFICATION_RESTRICTED for c in resp.citations):
            return NOT_JUDGED
        quotes = [x.quote for s in resp.sentences for x in s.quotes]
        try:
            return await self._llm.judge(q.question, resp.answer, quotes, points)
        except JudgeError:
            _log.warning("LLM 评审失败，这道题不计入评审指标", extra={"qid": q.id})
            return NOT_JUDGED

    async def _grounded(self, resp: answer_pb2.AnswerResponse) -> tuple[bool, ...]:
        """Groundedness：每句与它自己的引文（Sentence.quotes）做 NLI，取最大支持度是否达到阈值。
        同一块被多句引用时各句的引文不同，不能拿 Citation.quote 一段去判所有句子。"""
        pairs: list[tuple[str, str]] = []
        owners: list[int] = []
        claims = _claims(resp)
        for i, s in enumerate(claims):
            text = MARKER.sub("", s.text).strip()
            for q in s.quotes:
                pairs.append((q.quote, text))
                owners.append(i)
        best = [0.0] * len(claims)
        if pairs:
            scores, self.judge_model = await self._nli.support(pairs)
            for i, sc in zip(owners, scores, strict=True):
                best[i] = max(best[i], sc)
        return tuple(b >= SUPPORT_THRESHOLD for b in best)


def _features(resp: answer_pb2.AnswerResponse) -> tuple[float, ...]:
    if not resp.HasField("confidence"):
        return ()
    c = resp.confidence
    return (
        c.coverage,
        c.mean_support,
        c.top_rerank,
        c.source_diversity,
        float(c.conflict),
    )


def _claims(resp: answer_pb2.AnswerResponse) -> list[answer_pb2.Sentence]:
    """要算引用覆盖率与 Groundedness 的陈述：程序写的统计口径说明（claim_type = none）不是陈述。"""
    return [s for s in resp.sentences if s.claim_type != answer_pb2.CLAIM_TYPE_NONE]
