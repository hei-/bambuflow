"""消费 DocumentChanged，解析、分块、向量化后交给 indexing 写入（入库管道详设第 4、6–8 节）。

P0 以一次性方式运行：先重试本消费者挂起未确认的旧消息，再读新消息，读空后退出。
处理失败的消息不确认，留在挂起列表里等下次重试，并以非零退出码报告；
不可重试或重试满 5 次的转入死信后确认（state 模块），权限变更与删除一直重试、不进死信。
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import logging
import socket
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import grpc
import redis
from bambuflow_vocabulary import Vocabularies
from google.protobuf.timestamp_pb2 import Timestamp

from bambuflow.common.v1 import types_pb2
from bambuflow.ingestion.v1 import events_pb2, index_writer_pb2
from bambuflow_platform import envelope
from bambuflow_processing import pii
from bambuflow_processing.blocks import ParseError, UnsupportedFormatError, assemble
from bambuflow_processing.chunker import (
    CHUNKER_VERSION,
    Chunk,
    MissingSpanError,
    chunk,
    chunk_parsed,
    text_hash,
)
from bambuflow_processing.enrich import Enricher, Enrichment, SummaryModel
from bambuflow_processing.graph import ChunkText, Extractor, RelationModel, to_proto
from bambuflow_processing.injection import Detector, InjectionClassifier
from bambuflow_processing.metadata import extract
from bambuflow_processing.ocr import OcrEngine, OcrError
from bambuflow_processing.parsers import parse
from bambuflow_processing.pdf import OcrRequiredError
from bambuflow_processing.state import Failure, IngestState

STREAM = "ingest:document-changed"
GROUP = "processing"
# 输出变化时提升：p3-1 加关系抽取（P3-D），p3-2 加摘要与关键词（P3-I1），
# p3-3 加可疑指令标记（P3-I2）
PIPELINE_VERSION = "p3-3"

_log = logging.getLogger(__name__)

_CLASSIFICATION = {
    "public": types_pb2.CLASSIFICATION_PUBLIC,
    "internal": types_pb2.CLASSIFICATION_INTERNAL,
    "confidential": types_pb2.CLASSIFICATION_CONFIDENTIAL,
    "restricted": types_pb2.CLASSIFICATION_RESTRICTED,
}
_SOURCE_TYPE = {
    "wiki": types_pb2.SOURCE_TYPE_WIKI,
    "mail": types_pb2.SOURCE_TYPE_MAIL,
    "file": types_pb2.SOURCE_TYPE_FILE,
    "image": types_pb2.SOURCE_TYPE_IMAGE,
    "table": types_pb2.SOURCE_TYPE_TABLE,
    # 日志（P3-I3）：connectors 已按 5 分钟窗口写成一节一块的正文，跳过实体关系抽取（graph 模块）
    "log": types_pb2.SOURCE_TYPE_LOG,
    # 关系型数据库视图的表结构与字段说明（P3-I4），不含行数据
    "db": types_pb2.SOURCE_TYPE_DB,
}


class RawStore(Protocol):
    def get(self, ref: str) -> bytes: ...


class EmbedderLike(Protocol):
    model_version: str

    def embed(self, texts: list[str]) -> list[list[float]]: ...


class IndexWriter(Protocol):
    def WriteDocument(
        self, request: index_writer_pb2.WriteDocumentRequest, timeout: float
    ) -> index_writer_pb2.WriteDocumentResponse: ...

    def TombstoneDocument(
        self, request: index_writer_pb2.TombstoneDocumentRequest, timeout: float
    ) -> index_writer_pb2.TombstoneDocumentResponse: ...

    def UpdateAcl(
        self, request: index_writer_pb2.UpdateAclRequest, timeout: float
    ) -> index_writer_pb2.UpdateAclResponse: ...


class Stream(Protocol):
    def read(self, pending: bool) -> list[tuple[str, bytes]]: ...
    def ack(self, msg_id: str) -> None: ...


@dataclass
class Stats:
    written: int = 0
    unchanged: int = 0
    stale: int = 0
    deleted: int = 0
    acl_updated: int = 0
    failed: list[str] = field(default_factory=list)
    # failed 中转入死信的条数
    dead_lettered: int = 0


class UnsupportedEventError(Exception):
    pass


class ContentMismatchError(ValueError):
    """文档包或原文与事件的内容摘要不一致。"""


class ModelMismatchError(RuntimeError):
    """待发布版本的嵌入模型与本 worker 的不一致：等模型切换完成再处理。"""


def classify(stage: str, e: Exception) -> Failure:
    """失败分类（技术设计文档 5.6）：格式不支持、文件损坏这类不可重试，网络与模型服务问题可重试。"""
    msg = str(e)
    if isinstance(e, grpc.Call):
        return Failure(stage, f"rpc_{e.code().name.lower()}", msg, True)
    retryable: dict[type[Exception], str] = {
        # 没配 OCR 时等配置好再处理（OcrRequiredError 是 ParseError 的子类，要先判断）
        OcrRequiredError: "ocr_unavailable",
        OcrError: "ocr_error",
        ModelMismatchError: "model_mismatch",
        OSError: "io_error",
    }
    permanent: dict[type[Exception], str] = {
        UnsupportedEventError: "unsupported_event",
        UnsupportedFormatError: "unsupported_format",
        ContentMismatchError: "content_mismatch",
        ParseError: "bad_document",
        MissingSpanError: "bad_document",
        ValueError: "bad_document",
        KeyError: "bad_document",
    }
    for kinds, ok in ((retryable, True), (permanent, False)):
        for kind, code in kinds.items():
            if isinstance(e, kind):
                return Failure(stage, code, msg, ok)
    return Failure(stage, "runtime_error", msg, True)


class Worker:
    def __init__(
        self,
        stream: Stream,
        raw: RawStore,
        embedder: EmbedderLike,
        writer: IndexWriter,
        ocr: OcrEngine | None = None,
        *,
        vocab: Vocabularies,
        today: Callable[[], dt.date] = dt.date.today,
        relation_model: RelationModel | None = None,
        summary_model: SummaryModel | None = None,
        injection_classifier: InjectionClassifier | None = None,
        pii_keys: envelope.KeyService | None = None,
        state: IngestState,
    ) -> None:
        self._stream = stream
        # 入库状态与死信（入库管道详设第 11 节）
        self._state = state
        # 当前文档处理到的阶段，失败时据此记录（fetch、parse、mask、enrich、embed、write）
        self._stage = ""
        self._raw = raw
        # PII 原值信封加密的密钥服务；没有时只掩码、不保存原值（原文绝不以明文进索引）
        self._pii_keys = pii_keys
        self._embedder = embedder
        self._writer = writer
        # 没有配置 OCR 时，含扫描页或图片的文档处理失败并留在队列里，而不是缺页入库
        self._ocr = ocr
        # 受控词表快照：元数据按它归一（入库管道详设第 7 节）
        self._vocab = vocab
        self._today = today
        # 实体关系抽取：规则与词表；模型抽取经端口接入，默认不接（graph 模块说明了取舍）
        self._graph = Extractor(vocab, relation_model)
        # 摘要与关键词：默认抽取式；模型摘要经端口接入，默认不接（enrich 模块说明了取舍）
        self._enricher = Enricher(vocab, summary_model)
        # 可疑指令：规则加分类器，分类器经端口接入、默认不接（injection 模块说明了取舍）
        self._detector = Detector(injection_classifier)

    def run_once(self) -> Stats:
        stats = Stats()
        for pending in (True, False):
            while batch := self._stream.read(pending):
                for msg_id, pb in batch:
                    self._handle_message(msg_id, pb, stats)
                if pending:
                    break  # 挂起消息只重试一轮，仍失败的留给下次
        return stats

    def _handle_message(self, msg_id: str, pb: bytes, stats: Stats) -> None:
        evt = events_pb2.DocumentChanged.FromString(pb)
        if evt.change_type == events_pb2.CHANGE_TYPE_DELETE:
            self._handle_delete(msg_id, evt, stats)
            return
        if evt.change_type == events_pb2.CHANGE_TYPE_ACL_ONLY:
            self._handle_acl(msg_id, evt, stats)
            return
        self._stage = "fetch"
        try:
            result = self.handle(evt)
        except (
            UnsupportedEventError,
            grpc.RpcError,
            OSError,
            ValueError,
            KeyError,
            RuntimeError,
        ) as e:
            self._fail(msg_id, evt, classify(self._stage, e), stats)
            return
        if result == index_writer_pb2.WRITE_RESULT_WRITTEN:
            stats.written += 1
            outcome = "written"
        elif result == index_writer_pb2.WRITE_RESULT_UNCHANGED:
            stats.unchanged += 1
            outcome = "unchanged"
        else:
            stats.stale += 1
            outcome = "stale"
        self._state.succeeded(evt, "write", outcome)
        self._stream.ack(msg_id)

    def _fail(
        self,
        msg_id: str,
        evt: events_pb2.DocumentChanged,
        f: Failure,
        stats: Stats,
        fast_path: bool = False,
    ) -> None:
        _log.error(
            "处理失败",
            extra={
                "doc_id": evt.doc_id,
                "event_id": evt.event_id,
                "stage": f.stage,
                "code": f.code,
                "reason": f.message,
            },
        )
        stats.failed.append(evt.doc_id)
        if self._state.failed(evt, f, fast_path):
            stats.dead_lettered += 1
            self._stream.ack(msg_id)

    def _handle_delete(self, msg_id: str, evt: events_pb2.DocumentChanged, stats: Stats) -> None:
        """源端删除：交给 indexing 写删除标记，对线上立即生效（入库管道详设第 9 节）。"""
        occurred = Timestamp()
        occurred.CopyFrom(evt.occurred_at)
        req = index_writer_pb2.TombstoneDocumentRequest(
            source_id=evt.source_id, doc_id=evt.doc_id, occurred_at=occurred
        )
        try:
            resp = self._writer.TombstoneDocument(req, timeout=30)
        except grpc.RpcError as e:
            self._fail(msg_id, evt, classify("delete", e), stats, fast_path=True)
            return
        if resp.result == index_writer_pb2.TOMBSTONE_RESULT_TOMBSTONED:
            stats.deleted += 1
            outcome = "deleted"
        elif resp.result == index_writer_pb2.TOMBSTONE_RESULT_STALE:
            stats.stale += 1
            outcome = "stale"
        else:
            # 重复事件或从未入库的文档：没有需要做的，确认消息
            stats.unchanged += 1
            outcome = "unchanged"
        self._state.succeeded(evt, "delete", outcome)
        self._stream.ack(msg_id)

    def _handle_acl(self, msg_id: str, evt: events_pb2.DocumentChanged, stats: Stats) -> None:
        """只有权限变化：从文档包取新的权限与密级交给 indexing，不分块、不重算向量（快速通道）。"""
        try:
            env: dict[str, Any] = json.loads(self._raw.get(evt.raw_ref))
            occurred = Timestamp()
            occurred.CopyFrom(evt.occurred_at)
            resp = self._writer.UpdateAcl(
                index_writer_pb2.UpdateAclRequest(
                    source_id=evt.source_id,
                    doc_id=evt.doc_id,
                    acl_tokens=env["acl_tokens"],
                    classification=_CLASSIFICATION[env["classification"]],
                    occurred_at=occurred,
                ),
                timeout=30,
            )
        except (grpc.RpcError, OSError, ValueError, KeyError) as e:
            self._fail(msg_id, evt, classify("acl", e), stats, fast_path=True)
            return
        if resp.result == index_writer_pb2.ACL_RESULT_UPDATED:
            stats.acl_updated += 1
            outcome = "acl_updated"
        elif resp.result == index_writer_pb2.ACL_RESULT_STALE:
            stats.stale += 1
            outcome = "stale"
        else:
            stats.unchanged += 1
            outcome = "unchanged"
        self._state.succeeded(evt, "acl", outcome)
        self._stream.ack(msg_id)

    def handle(self, evt: events_pb2.DocumentChanged) -> int:
        if evt.change_type != events_pb2.CHANGE_TYPE_UPSERT:
            # 未知的变更类型：拒绝处理而不是悄悄跳过
            raise UnsupportedEventError(f"不支持的变更类型 {evt.change_type}")
        env: dict[str, Any] = json.loads(self._raw.get(evt.raw_ref))
        if env["content_hash"] != evt.content_hash:
            raise ContentMismatchError("文档包与事件的内容摘要不一致")
        self._stage = "parse"
        parsed = self._chunks(evt, env)
        self._stage = "mask"
        chunks, sealed = self._mask(parsed)
        self._stage = "enrich"
        source_type = _SOURCE_TYPE[env["source_type"]]
        classification = _CLASSIFICATION[env["classification"]]
        flagged = self._detector.flagged(
            [ChunkText(c.chunk_id, c.text) for c in chunks], classification
        )
        enrichment = self._enrich(evt, env["title"], chunks, classification, flagged)
        inputs = [c.embedding_input() for c in chunks]
        if enrichment is not None:
            # 摘要块的向量以标题开头：主题匹配时标题本身是很强的信号
            inputs.append(f"{env['title']}\n{enrichment.summary_chunk.text}")
        self._stage = "embed"
        vectors = self._embedder.embed(inputs)
        self._stage = "write"
        occurred = Timestamp()
        occurred.CopyFrom(evt.occurred_at)
        relations = self._graph.extract(
            env["title"],
            [ChunkText(c.chunk_id, c.text) for c in chunks],
            source_type,
            classification,
        )
        req = index_writer_pb2.WriteDocumentRequest(
            idempotency_key=f"{evt.doc_id}:{evt.content_hash}:{PIPELINE_VERSION}",
            source_id=evt.source_id,
            doc_id=evt.doc_id,
            content_hash=evt.content_hash,
            occurred_at=occurred,
            title=env["title"],
            source_uri=env["source_uri"],
            source_type=source_type,
            owner=env.get("owner", ""),
            classification=classification,
            acl_tokens=env["acl_tokens"],
            embedding_model_version=self._embedder.model_version,
            chunker_version=CHUNKER_VERSION,
            pipeline_version=PIPELINE_VERSION,
            # 规则抽取用的正文：Markdown 用原文，二进制格式用解析出的各块
            metadata=extract(
                env["title"],
                env.get("content") or "\n".join(c.text for c in chunks),
                env.get("source_fields"),
                self._vocab,
                self._today(),
            ),
            chunks=[
                _chunk(c, v, c.chunk_id in flagged, sealed.get(c.chunk_id))
                for c, v in zip(chunks, vectors[: len(chunks)], strict=True)
            ],
            relations=to_proto(relations),
            enrichment=_enrichment(enrichment, vectors[-1]) if enrichment else None,
        )
        resp = self._writer.WriteDocument(req, timeout=30)
        if resp.result == index_writer_pb2.WRITE_RESULT_MODEL_MISMATCH:
            pending, model = resp.index_version, self._embedder.model_version
            raise ModelMismatchError(f"待发布版本 {pending} 的嵌入模型与 {model} 不一致")
        result: int = resp.result
        return result

    def _mask(self, chunks: list[Chunk]) -> tuple[list[Chunk], dict[str, pii.Sealed]]:
        """PII 等长掩码，原值按块加密（入库管道详设第 6 节）。

        之后的摘要、关系抽取、向量化都只见到掩码文本。"""
        key = pii.DocumentKey(self._pii_keys) if self._pii_keys is not None else None
        out: list[Chunk] = []
        sealed: dict[str, pii.Sealed] = {}
        for c in chunks:
            masked, found = pii.mask(c.text)
            if not found:
                out.append(c)
                continue
            out.append(dataclasses.replace(c, text=masked, text_hash=text_hash(masked)))
            if key is not None:
                sealed[c.chunk_id] = key.seal(c.chunk_id, found)
        return out, sealed

    def _enrich(
        self,
        evt: events_pb2.DocumentChanged,
        title: str,
        chunks: list[Chunk],
        classification: int,
        flagged: frozenset[str],
    ) -> Enrichment | None:
        try:
            # 带可疑指令的块不进摘要：摘要会成为主题汇总的证据
            return self._enricher.enrich(
                evt.source_id, evt.doc_id, title, chunks, classification, flagged
            )
        except (ValueError, RuntimeError, OSError) as e:
            # 增强失败不阻塞入库（入库管道详设第 7 节）：文档先不带摘要发布，下次重处理时补上
            _log.warning("生成摘要失败", extra={"doc_id": evt.doc_id, "reason": str(e)})
            return None

    def _chunks(self, evt: events_pb2.DocumentChanged, env: dict[str, Any]) -> list[Chunk]:
        if env["content_type"] == "text/markdown":
            return chunk(evt.source_id, evt.doc_id, env["content"])
        # 二进制原文另存一个对象（connectors 的文档包格式），取回后核对摘要再解析
        data = self._raw.get(env["content_ref"])
        if hashlib.sha256(data).hexdigest() != evt.content_hash:
            raise ContentMismatchError("原文与事件的内容摘要不一致")
        blocks = parse(env["content_type"], data, env["title"], self._ocr)
        return chunk_parsed(evt.source_id, evt.doc_id, assemble(blocks))


def _chunk(
    c: Chunk, vector: list[float], flagged: bool = False, sealed: pii.Sealed | None = None
) -> index_writer_pb2.Chunk:
    return index_writer_pb2.Chunk(
        pii=index_writer_pb2.SealedPII(
            ciphertext=sealed.ciphertext, wrapped_key=sealed.wrapped_key, key_id=sealed.key_id
        )
        if sealed
        else None,
        injection_flag=flagged,
        chunk_id=c.chunk_id,
        parent_chunk_id=c.parent_chunk_id,
        heading_path=list(c.heading_path),
        text=c.text,
        text_hash=c.text_hash,
        span=_span(c),
        embedding=vector,
    )


def _enrichment(e: Enrichment, vector: list[float]) -> index_writer_pb2.Enrichment:
    return index_writer_pb2.Enrichment(
        summary_chunk=_chunk(e.summary_chunk, vector),
        keywords=list(e.keywords),
        sections=[
            index_writer_pb2.SectionSummary(
                parent_chunk_id=sec.parent_chunk_id,
                heading_path=list(sec.heading_path),
                summary=sec.summary,
                keywords=list(sec.keywords),
            )
            for sec in e.sections
        ],
        summarizer_version=e.version,
    )


def _span(c: Chunk) -> types_pb2.Span:
    span = types_pb2.Span(
        char_start=c.start, char_end=c.end, line_start=c.line_start, line_end=c.line_end
    )
    if c.page is not None:
        span.page = c.page
    if c.bbox is not None:
        x0, y0, x1, y1 = c.bbox
        span.bbox.CopyFrom(types_pb2.BoundingBox(x0=x0, y0=y0, x1=x1, y1=y1))
    return span


class RedisStream:
    """Redis Streams 消费组；消费者名取主机名，挂起消息归属于它。"""

    def __init__(self, client: Any, count: int = 16, block_ms: int = 2000) -> None:
        self._r = client
        self._count = count
        self._block = block_ms
        self._consumer = socket.gethostname()
        try:
            self._r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            # 消费组已存在时 Redis 返回 BUSYGROUP，属于正常情况
            if "BUSYGROUP" not in str(e):
                raise

    def read(self, pending: bool) -> list[tuple[str, bytes]]:
        resp = self._r.xreadgroup(
            GROUP,
            self._consumer,
            {STREAM: "0" if pending else ">"},
            count=self._count,
            block=None if pending else self._block,
        )
        out: list[tuple[str, bytes]] = []
        for _, entries in resp or []:
            for msg_id, fields in entries:
                if fields:  # 已确认的挂起条目返回空字段
                    out.append(
                        (msg_id.decode() if isinstance(msg_id, bytes) else msg_id, fields[b"pb"])
                    )
        return out

    def ack(self, msg_id: str) -> None:
        self._r.xack(STREAM, GROUP, msg_id)
