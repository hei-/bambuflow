"""把合成文档写入待发布版本（make eval-perm 的第一步，随后 make ingest 发布）。

文档按序号的哈希确定地生成 ACL 与密级，分布在两个租户各自的数据源和一个不属于任何租户的数据源上，
用来和权限表上的暴力计算结果逐条比对（retrieval 详设第 13 节“随机数据 + 暴力对照”）。
内容固定，重复运行时 IndexWriter 返回 unchanged，是幂等的。

每份文档另带两条关系（图召回用，P3-D）：清算核心服务依赖本文档的下游系统；
某个运维中心负责另一份文档的下游系统。第二条让两跳路径跨越两份 ACL 不同的文档，
用来检查“证据不可见的边不参与遍历”（retrieval 详设 13 节用例 4）。
"""

import hashlib
import os
from dataclasses import dataclass

import grpc
import httpx
import psycopg
from google.protobuf.timestamp_pb2 import Timestamp

from bambuflow.common.v1 import types_pb2
from bambuflow.ingestion.v1 import index_writer_pb2, index_writer_pb2_grpc

# src_evalorphan 不在任何租户的数据源里，任何人都不应检索到
SOURCES = ("src_evalcredit", "src_evalrisk", "src_evalorphan")
TOKENS = (
    "user:alice",
    "user:bob",
    "user:carl",
    "user:zed",
    "group:credit-l2",
    "group:risk-dept",
    "group:all-staff",
    "group:project-042",
    "dept:信贷部",
    "dept:风险管理部",
    "group:ghost-unused",
)
TOPICS = ("授信额度计算", "风险限额审批", "员工请假制度", "反洗钱报告", "贷款利率调整")
HUB = "清算核心服务"
DOCS = 30
# 密级编号 1–4 对应的枚举值
CLASSES = (
    types_pb2.CLASSIFICATION_PUBLIC,
    types_pb2.CLASSIFICATION_INTERNAL,
    types_pb2.CLASSIFICATION_CONFIDENTIAL,
    types_pb2.CLASSIFICATION_RESTRICTED,
)


@dataclass(frozen=True)
class Doc:
    source: str
    doc_id: str
    text: str
    tokens: tuple[str, ...]
    classification: int


def downstream(i: int) -> str:
    return f"评测下游系统{i:02d}"


def ops_center(i: int) -> str:
    return f"评测运维中心{i % 5}"


def other(i: int) -> int:
    return (i + 7) % DOCS


def pick(i: int, salt: str, n: int) -> int:
    """按文档序号确定地取 [0, n) 里的一个数：每次运行生成同一批文档，结果可复现。"""
    return int(hashlib.sha256(f"{i}:{salt}".encode()).hexdigest(), 16) % n


def corpus() -> list[Doc]:
    out = []
    for i in range(DOCS):
        shuffled = sorted(TOKENS, key=lambda t: pick(i, t, 1 << 32))
        tokens = tuple(sorted(shuffled[: pick(i, "count", 4)]))
        topic = TOPICS[pick(i, "topic", len(TOPICS))]
        text = (
            f"评测文档 {i}：{topic}的规定与流程。{HUB}依赖{downstream(i)}。"
            f"{ops_center(i)}负责{downstream(other(i))}。"
        )
        source = SOURCES[i % len(SOURCES)]
        out.append(Doc(source, f"eval-{i:02d}", text, tokens, pick(i, "class", 4) + 1))
    return out


def live_models(dsn: str) -> tuple[str, str]:
    """待发布版本要求同一嵌入模型和分块器，取线上版本的，避免和随后的 Wiki 入库冲突。"""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            """SELECT v.embedding_model, v.chunker_version FROM idx.aliases a
               JOIN idx.index_versions v USING (version) WHERE a.alias = 'prod'"""
        ).fetchone()
    if row is None:
        raise SystemExit("还没有线上索引版本，先运行 make ingest")
    return str(row[0]), str(row[1])


def embed(texts: list[str]) -> list[list[float]]:
    resp = httpx.post(
        os.environ["BF_EMBEDDER_URL"] + "/v2/models/bge-m3/infer",
        json={
            "inputs": [{"name": "text", "shape": [len(texts)], "datatype": "BYTES", "data": texts}]
        },
        timeout=30,
    )
    resp.raise_for_status()
    out = resp.json()["outputs"][0]
    n, dim = out["shape"]
    data = out["data"]
    return [data[i * dim : (i + 1) * dim] for i in range(n)]


def request(
    d: Doc, vec: list[float], model: str, chunker: str
) -> index_writer_pb2.WriteDocumentRequest:
    ts = Timestamp()
    ts.FromJsonString("2026-10-01T00:00:00Z")
    h = hashlib.sha256(d.text.encode()).hexdigest()
    chunk_id = f"{d.source}:{d.doc_id}#{h[:16]}"
    i = int(d.doc_id.removeprefix("eval-"))
    system = types_pb2.ENTITY_TYPE_SYSTEM

    def rel(
        subject: index_writer_pb2.Entity,
        kind: types_pb2.RelationType,
        obj: index_writer_pb2.Entity,
    ) -> index_writer_pb2.Relation:
        return index_writer_pb2.Relation(
            subject=subject,
            relation_type=kind,
            object=obj,
            evidence_chunk_id=chunk_id,
            confidence=0.9,
            extractor_version="perm-eval",
        )

    relations = [
        rel(
            index_writer_pb2.Entity(type=system, name=HUB),
            types_pb2.RELATION_TYPE_DEPENDS_ON,
            index_writer_pb2.Entity(type=system, name=downstream(i)),
        ),
        rel(
            index_writer_pb2.Entity(type=types_pb2.ENTITY_TYPE_ORG, name=ops_center(i)),
            types_pb2.RELATION_TYPE_RESPONSIBLE_FOR,
            index_writer_pb2.Entity(type=system, name=downstream(other(i))),
        ),
    ]
    chunk = index_writer_pb2.Chunk(
        chunk_id=chunk_id,
        parent_chunk_id="",
        text=d.text,
        text_hash=h,
        span=types_pb2.Span(char_start=0, char_end=len(d.text), line_start=1, line_end=1),
        embedding=vec,
    )
    return index_writer_pb2.WriteDocumentRequest(
        idempotency_key=f"{d.doc_id}:{h}:perm-eval",
        source_id=d.source,
        doc_id=d.doc_id,
        content_hash=h,
        occurred_at=ts,
        title=f"评测文档 {d.doc_id}",
        source_uri=f"test://perm-eval/{d.doc_id}",
        source_type=types_pb2.SOURCE_TYPE_WIKI,
        classification=CLASSES[d.classification - 1],
        acl_tokens=list(d.tokens),
        chunks=[chunk],
        relations=relations,
        embedding_model_version=model,
        chunker_version=chunker,
        pipeline_version="perm-eval",
    )


def main() -> None:
    model, chunker = live_models(os.environ["BF_PG_DSN"])
    docs = corpus()
    vecs = embed([d.text for d in docs])
    written = 0
    with grpc.insecure_channel(os.environ["BF_INDEXING_ADDR"]) as ch:
        stub = index_writer_pb2_grpc.IndexWriterServiceStub(ch)
        for d, v in zip(docs, vecs, strict=True):
            resp = stub.WriteDocument(request(d, v, model, chunker), timeout=10)
            if resp.result not in (
                index_writer_pb2.WRITE_RESULT_WRITTEN,
                index_writer_pb2.WRITE_RESULT_UNCHANGED,
            ):
                raise SystemExit(
                    f"{d.doc_id}: 写入结果 {index_writer_pb2.WriteResult.Name(resp.result)}"
                )
            written += resp.result == index_writer_pb2.WRITE_RESULT_WRITTEN
    print(f"合成文档 {len(docs)} 份已写入待发布版本（新写入 {written} 份）")


if __name__ == "__main__":
    main()
