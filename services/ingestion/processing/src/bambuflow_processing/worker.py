"""消费 DocumentChanged，解析、分块、向量化后交给 indexing 写入（入库管道详设第 4、6–8 节）。

常驻（serve）或一次性（run_once）运行。处理失败的消息不确认，记进重试表按指数退避重试（RedisStream）；
不可重试或重试满 5 次的转入死信后确认（state 模块），权限变更与删除一直重试、不进死信。
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import logging
import socket
import time
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
from bambuflow_processing.blocks import Block, ParseError, UnsupportedFormatError, assemble
from bambuflow_processing.chunker import (
    CHUNKER_VERSION,
    Chunk,
    MissingSpanError,
    chunk,
    chunk_parsed,
    text_hash,
)
from bambuflow_processing.embedder import Sparse
from bambuflow_processing.enrich import Enricher, Enrichment, SummaryModel
from bambuflow_processing.graph import ChunkText, Extractor, RelationModel, to_proto
from bambuflow_processing.injection import Detector, InjectionClassifier
from bambuflow_processing.metadata import MetadataModel, extract, fill_missing
from bambuflow_processing.ocr import OcrError
from bambuflow_processing.pdf import OcrRequiredError
from bambuflow_processing.sandbox import ParseCrashedError, ParseTimeoutError
from bambuflow_processing.state import Failure, IngestState

STREAM = "ingest:document-changed"
GROUP = "processing"
# 重试的指数退避：起点与上限（秒）；常驻模式检查到期重试的间隔
RETRY_BASE, RETRY_MAX, PENDING_SCAN = 30.0, 900.0, 5.0
# 检查要补做增强的文档的间隔（秒）
ENRICH_SCAN = 60.0
# 读了没确认、也不在重试表里的消息（处理中途进程没了）空闲多久后由任一实例认领重做：
# 大于单份文档最长的处理时长（OCR 超时 30 分钟加向量化与写入）
STALE_AFTER = 45 * 60.0
# 全量重建期间按新模型部署的 worker 用独立的消费组：每个事件两边各处理一次，
# 内容变更同时写入两个待发布版本
REBUILD_GROUP = "processing-rebuild"
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
    # 模型是否给稀疏向量（KServe 模型元数据里有名为 sparse 的输出）
    sparse: bool

    def embed_full(self, texts: list[str]) -> tuple[list[list[float]], list[Sparse] | None]: ...


@dataclass(frozen=True)
class Vector:
    """一块的向量：稠密向量与（模型给出时的）学习型稀疏向量。"""

    dense: list[float]
    sparse: Sparse | None = None


class IndexWriter(Protocol):
    def WriteDocument(
        self, request: index_writer_pb2.WriteDocumentRequest, timeout: float
    ) -> index_writer_pb2.WriteDocumentResponse: ...


class VectorCache(Protocol):
    """按块的向量缓存（入库管道详设第 8 节）：indexing 的 LookupVectors。"""

    def LookupVectors(
        self, request: index_writer_pb2.LookupVectorsRequest, timeout: float
    ) -> index_writer_pb2.LookupVectorsResponse: ...


# 一次向量缓存查询的输入摘要上限（与 indexing 一致）
_LOOKUP_BATCH = 1000


class Throttle:
    """按每秒文档数限速（入库管道详设第 10、12 节）：
    全量重建的写入不能挤占在线查询用的元数据库与索引存储。

    两份文档之间至少隔 1/rate 秒；处理本身慢于这个间隔时不再额外等待。"""

    def __init__(
        self,
        rate: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate <= 0:
            raise ValueError("限速须为正数")
        self._gap, self._clock, self._sleep = 1.0 / rate, clock, sleep
        self._next = 0.0

    def wait(self) -> None:
        now = self._clock()
        if now < self._next:
            self._sleep(self._next - now)
            now = self._next
        self._next = now + self._gap


class Stream(Protocol):
    def read(self, pending: bool) -> list[tuple[str, bytes]]: ...
    def ack(self, msg_id: str) -> None: ...
    def retry_later(self, msg_id: str) -> None: ...


@dataclass
class Stats:
    written: int = 0
    unchanged: int = 0
    stale: int = 0
    failed: list[str] = field(default_factory=list)
    # failed 中转入死信的条数
    dead_lettered: int = 0

    def merge(self, o: Stats) -> None:
        self.written += o.written
        self.unchanged += o.unchanged
        self.stale += o.stale
        self.failed += o.failed
        self.dead_lettered += o.dead_lettered


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
        ParseTimeoutError: "parse_timeout",
        ParseCrashedError: "parse_crashed",
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
        *,
        # 解析二进制原文（含 OCR）：生产为子进程里按阶段超时的 SandboxParser
        parse: Callable[[str, bytes, str], list[Block]],
        vocab: Vocabularies,
        today: Callable[[], dt.date] = dt.date.today,
        relation_model: RelationModel | None = None,
        summary_model: SummaryModel | None = None,
        injection_classifier: InjectionClassifier | None = None,
        pii_keys: envelope.KeyService | None = None,
        state: IngestState,
        vector_cache: VectorCache | None = None,
        throttle: Throttle | None = None,
        ner: pii.NerModel | None = None,
        metadata_model: MetadataModel | None = None,
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
        # 向量缓存：命中的块不再送嵌入模型；没有时全部向量化
        self._cache = vector_cache
        # 写入限速：全量重建用的 worker 配置（BF_MAX_DOCS_PER_SECOND），线上增量不限
        self._throttle = throttle
        # NER（规则之外识别人名、地址等）；不配置时 PII 只按规则识别
        self._ner = ner
        # 受控元数据的模型抽取：只补规则没取到的字段；默认不接（metadata 模块说明了取舍）
        self._metadata_model = metadata_model
        self._writer = writer
        self._parse = parse
        # 这次处理里增强（摘要）是否失败：失败的文档先不带摘要写入，之后补做
        self._enrich_failed = False
        # 受控词表快照：元数据按它归一（入库管道详设第 7 节）
        self._vocab = vocab
        self._today = today
        # 实体关系抽取：规则与词表；模型抽取经端口接入，默认不接（graph 模块说明了取舍）
        self._graph = Extractor(vocab, relation_model)
        # 摘要与关键词：默认抽取式；模型摘要经端口接入，默认不接（enrich 模块说明了取舍）
        self._enricher = Enricher(vocab, summary_model)
        # 可疑指令：规则加分类器，分类器经端口接入、默认不接（injection 模块说明了取舍）
        self._detector = Detector(injection_classifier)

    def serve(self, stop: Callable[[], bool], clock: Callable[[], float] = time.monotonic) -> None:
        """常驻：新消息即到即处理（读事件流时阻塞等待，见 RedisStream）。
        处理失败、没进死信的消息记进重试表，按指数退避重试（30 秒起每次翻倍，最长 15 分钟）；
        按现有规则失败 5 次进死信（约 7 分半钟）；向量化与模型不一致不进死信，按封顶间隔一直重试，
        等模型服务恢复或部署改正（入库管道详设第 11 节）。
        每 5 秒取一次到期的重试与别的实例没做完的消息；每分钟把到期的增强补做放回事件流。"""
        next_scan = next_enrich = 0.0
        while not stop():
            for msg_id, pb in self._stream.read(False):
                self._attempt(msg_id, pb)
            if clock() >= next_enrich:
                next_enrich = clock() + ENRICH_SCAN
                if n := self._state.requeue_enrich(STREAM):
                    _log.info("放回事件流补做增强", extra={"count": n})
            if clock() < next_scan:
                continue
            next_scan = clock() + PENDING_SCAN
            for msg_id, pb in self._stream.read(True):
                self._attempt(msg_id, pb)

    def _attempt(self, msg_id: str, pb: bytes) -> Stats:
        if self._throttle is not None:
            self._throttle.wait()
        stats = Stats()
        self._handle_message(msg_id, pb, stats)
        if stats.failed and not stats.dead_lettered:
            self._stream.retry_later(msg_id)
        return stats

    def run_once(self) -> Stats:
        """一次性：先做到期的重试与别的实例没做完的消息，再读新消息，读空后退出。"""
        stats = Stats()
        for pending in (True, False):
            while batch := self._stream.read(pending):
                for msg_id, pb in batch:
                    stats.merge(self._attempt(msg_id, pb))
                if pending:
                    break
        return stats

    def _handle_message(self, msg_id: str, pb: bytes, stats: Stats) -> None:
        evt = events_pb2.DocumentChanged.FromString(pb)
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
        if not self._enrich_failed:
            self._state.enrich_ok(evt.doc_id)
        elif not self._state.enrich_failed(evt):
            _log.error("增强多次失败，不再补做，文档没有摘要", extra={"doc_id": evt.doc_id})
        self._stream.ack(msg_id)

    def _fail(
        self,
        msg_id: str,
        evt: events_pb2.DocumentChanged,
        f: Failure,
        stats: Stats,
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
        if self._state.failed(evt, f):
            stats.dead_lettered += 1
            self._stream.ack(msg_id)

    def handle(self, evt: events_pb2.DocumentChanged) -> int:
        if evt.change_type != events_pb2.CHANGE_TYPE_UPSERT:
            # 删除与仅权限变化走 ingest:document-access，由 indexing 直接处理
            # （入库管道详设第 9 节）；
            # 出现在这里说明发错了流，拒绝处理而不是悄悄跳过
            raise UnsupportedEventError(f"内容事件流上不处理变更类型 {evt.change_type}")
        self._enrich_failed = False
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
        hashes = [text_hash(i) for i in inputs]
        vectors = self._embed(evt.doc_id, inputs, hashes)
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
            metadata=self._metadata(evt.doc_id, env, chunks, classification),
            chunks=[
                _chunk(c, v, c.chunk_id in flagged, sealed.get(c.chunk_id), h)
                for c, v, h in zip(
                    chunks, vectors[: len(chunks)], hashes[: len(chunks)], strict=True
                )
            ],
            relations=to_proto(relations),
            enrichment=_enrichment(enrichment, vectors[-1], hashes[-1]) if enrichment else None,
        )
        resp = self._writer.WriteDocument(req, timeout=30)
        if resp.result == index_writer_pb2.WRITE_RESULT_MODEL_MISMATCH:
            pending, model = resp.index_version, self._embedder.model_version
            raise ModelMismatchError(f"待发布版本 {pending} 的嵌入模型与 {model} 不一致")
        result: int = resp.result
        return result

    def _metadata(
        self, doc_id: str, env: dict[str, Any], chunks: list[Chunk], classification: int
    ) -> types_pb2.DocMetadata:
        # 规则抽取用的正文：Markdown 用原文，二进制格式用解析出的各块
        md = extract(
            env["title"],
            env.get("content") or "\n".join(c.text for c in chunks),
            env.get("source_fields"),
            self._vocab,
            self._today(),
        )
        if self._metadata_model is None or not md.missing:
            return md
        try:
            # 模型只见到掩码后的块，与摘要、向量化一样
            guess = self._metadata_model.guess(env["title"], chunks, classification)
        except (ValueError, RuntimeError, OSError) as e:
            # 增强失败不阻塞入库：取不到的字段照旧记为缺失，由 metadata_gaps 披露
            _log.warning("模型抽取元数据失败", extra={"doc_id": doc_id, "reason": str(e)})
            return md
        return fill_missing(md, guess, self._vocab)

    def _embed(self, doc_id: str, inputs: list[str], hashes: list[str]) -> list[Vector]:
        """先按（嵌入模型、输入摘要）查已入库的向量，只把未命中的送嵌入模型（入库管道详设第 8 节）。

        增量入库时一份文档通常只有改动的块未命中。缓存不可用只是少省一些计算，照常全部向量化。
        模型给稀疏向量时，缓存里没有稀疏向量的块（启用稀疏召回之前入库的）按未命中处理，补上稀疏向量。"""
        cached: dict[str, Vector] = {}
        if self._cache is not None:
            try:
                for i in range(0, len(hashes), _LOOKUP_BATCH):
                    resp = self._cache.LookupVectors(
                        index_writer_pb2.LookupVectorsRequest(
                            embedding_model_version=self._embedder.model_version,
                            input_hashes=sorted(set(hashes[i : i + _LOOKUP_BATCH])),
                        ),
                        timeout=10,
                    )
                    cached.update(
                        {
                            v.input_hash: Vector(list(v.embedding), dict(v.sparse) or None)
                            for v in resp.vectors
                        }
                    )
            except grpc.RpcError as e:
                _log.warning(
                    "向量缓存不可用，全部向量化", extra={"doc_id": doc_id, "reason": str(e)}
                )
                cached = {}

        def hit(h: str) -> bool:
            v = cached.get(h)
            return v is not None and (v.sparse is not None or not self._embedder.sparse)

        missing = [i for i, h in enumerate(hashes) if not hit(h)]
        computed: dict[int, Vector] = {}
        if missing:
            dense, sparse = self._embedder.embed_full([inputs[i] for i in missing])
            for j, i in enumerate(missing):
                computed[i] = Vector(dense[j], sparse[j] if sparse is not None else None)
        if self._cache is not None:
            _log.info(
                "向量化",
                extra={
                    "doc_id": doc_id,
                    "chunks": len(inputs),
                    "cache_hits": len(inputs) - len(missing),
                },
            )
        return [computed[i] if i in computed else cached[h] for i, h in enumerate(hashes)]

    def _mask(self, chunks: list[Chunk]) -> tuple[list[Chunk], dict[str, pii.Sealed]]:
        """PII 等长掩码，原值按块加密（入库管道详设第 6 节）。

        之后的摘要、关系抽取、向量化都只见到掩码文本。"""
        key = pii.DocumentKey(self._pii_keys) if self._pii_keys is not None else None
        out: list[Chunk] = []
        sealed: dict[str, pii.Sealed] = {}
        for c in chunks:
            masked, found = pii.mask(c.text, self._ner)
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
            # 增强失败不阻塞入库（入库管道详设第 7 节）：文档先不带摘要发布，
            # 之后补做（IngestState.enrich_failed）
            _log.warning("生成摘要失败", extra={"doc_id": evt.doc_id, "reason": str(e)})
            self._enrich_failed = True
            return None

    def _chunks(self, evt: events_pb2.DocumentChanged, env: dict[str, Any]) -> list[Chunk]:
        if env["content_type"] == "text/markdown":
            return chunk(evt.source_id, evt.doc_id, env["content"])
        # 二进制原文另存一个对象（connectors 的文档包格式），取回后核对摘要再解析
        data = self._raw.get(env["content_ref"])
        if hashlib.sha256(data).hexdigest() != evt.content_hash:
            raise ContentMismatchError("原文与事件的内容摘要不一致")
        blocks = self._parse(env["content_type"], data, env["title"])
        return chunk_parsed(evt.source_id, evt.doc_id, assemble(blocks))


def _chunk(
    c: Chunk,
    vector: Vector,
    flagged: bool = False,
    sealed: pii.Sealed | None = None,
    input_hash: str = "",
) -> index_writer_pb2.Chunk:
    return index_writer_pb2.Chunk(
        input_hash=input_hash,
        sparse=vector.sparse or {},
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
        embedding=vector.dense,
    )


def _enrichment(e: Enrichment, vector: Vector, input_hash: str = "") -> index_writer_pb2.Enrichment:
    return index_writer_pb2.Enrichment(
        summary_chunk=_chunk(e.summary_chunk, vector, input_hash=input_hash),
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
    """Redis Streams 消费组。

    消费者名取主机名，而 Pod 重建、一次性任务每次运行主机名都会变，所以挂起的消息不按“谁读的”去找：
    - 失败的消息记进重试表（次数与下次时间存在 Redis，重启不丢），到期后由任一实例用
      XCLAIM 认领重试。XCLAIM 带最小空闲时长（等于这次的退避间隔），认领会重置空闲时长，
      所以同一条只有一个实例认领得到；
    - 读了没确认、也不在重试表里的（处理中途进程没了），空闲超过 stale_after 后由任一实例认领重做。
    消费方按事件幂等，偶尔重复处理一次没有副作用。"""

    def __init__(
        self,
        client: Any,
        count: int = 16,
        block_ms: int = 2000,
        group: str = GROUP,
        stale_after: float = STALE_AFTER,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._r = client
        self._group = group
        self._count = count
        self._block = block_ms
        self._stale_ms = int(stale_after * 1000)
        self._now = now
        self._consumer = socket.gethostname()
        # 重试表：消息 ID → {"n": 已失败次数, "delay": 本次退避秒数, "due": 下次重试的时间}
        self._retry = f"processing:retry:{group}"
        try:
            self._r.xgroup_create(STREAM, group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            # 消费组已存在时 Redis 返回 BUSYGROUP，属于正常情况
            if "BUSYGROUP" not in str(e):
                raise

    def read(self, pending: bool) -> list[tuple[str, bytes]]:
        if pending:
            return self._due() + self._stale()
        resp = self._r.xreadgroup(
            self._group, self._consumer, {STREAM: ">"}, count=self._count, block=self._block
        )
        return [m for _, entries in resp or [] for m in _messages(entries)]

    def _due(self) -> list[tuple[str, bytes]]:
        now = self._now()
        out: list[tuple[str, bytes]] = []
        for key, raw in self._r.hgetall(self._retry).items():
            item = json.loads(raw)
            if item["due"] > now:
                continue
            msg_id = key.decode() if isinstance(key, bytes) else key
            claimed = self._r.xclaim(
                STREAM, self._group, self._consumer, int(item["delay"] * 1000), [msg_id]
            )
            got = _messages(claimed)
            if not got and not self._r.xpending_range(STREAM, self._group, msg_id, msg_id, 1):
                # 已经不在挂起列表里（别处确认了，或流被裁剪）：重试表里的记录也不要了
                self._r.hdel(self._retry, msg_id)
            out.extend(got)
        return out

    def _stale(self) -> list[tuple[str, bytes]]:
        out: list[tuple[str, bytes]] = []
        start = "0-0"
        while True:
            resp = self._r.xautoclaim(
                STREAM, self._group, self._consumer, self._stale_ms, start, count=100
            )
            nxt, entries = resp[0], resp[1]
            # 在重试表里的由 _due 按到期时间处理
            out.extend(m for m in _messages(entries) if not self._r.hexists(self._retry, m[0]))
            nxt = nxt.decode() if isinstance(nxt, bytes) else nxt
            if nxt in ("0-0", ""):
                return out
            start = nxt

    def retry_later(self, msg_id: str) -> None:
        raw = self._r.hget(self._retry, msg_id)
        n = (json.loads(raw)["n"] if raw else 0) + 1
        delay = min(RETRY_BASE * 2 ** (n - 1), RETRY_MAX)
        self._r.hset(
            self._retry, msg_id, json.dumps({"n": n, "delay": delay, "due": self._now() + delay})
        )

    def ack(self, msg_id: str) -> None:
        self._r.xack(STREAM, self._group, msg_id)
        self._r.hdel(self._retry, msg_id)


def _messages(entries: Any) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    for msg_id, fields in entries or []:
        if fields:  # 已确认或已删除的条目没有字段
            out.append((msg_id.decode() if isinstance(msg_id, bytes) else msg_id, fields[b"pb"]))
    return out
