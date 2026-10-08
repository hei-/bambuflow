"""入库的端到端测试：Wiki 样例经 connectors → processing → indexing 写入并发布（P0 第 5 步）。

需要先 make up PROFILE=core,ingest，再 make ingest（验证幂等需要连续入库两次，CI 会这样跑）。
IndexWriter 的契约测试写入独立数据源 src_e2e 的文档，ACL 为空，任何人都检索不到。
"""

import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import grpc
import psycopg
import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from bambuflow.common.v1 import types_pb2
from bambuflow.ingestion.v1 import index_writer_pb2, index_writer_pb2_grpc

from .upstream import distractors

SOURCE = "src_wikidemo"
# 样例页面以 front matter 的 id 为 doc_id；目录在 e2e 镜像里随仓库一起（/src）。
# 干扰页面在构建 connectors 开发镜像时生成，清单取自同一个生成器。
WIKI_DIR = Path(__file__).resolve().parents[2] / "test-upstream" / "wiki"
PAGES = {
    m.group(1)
    for f in WIKI_DIR.glob("*.md")
    if (m := re.search(r"^id: (\S+)$", f.read_text(encoding="utf-8"), re.MULTILINE))
} | {p.id for p in distractors()}


@pytest.fixture(scope="module")
def db() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(os.environ["BF_PG_DSN"], autocommit=True) as conn:
        yield conn


def rows(db: psycopg.Connection[Any], sql: str, *args: Any) -> list[Any]:
    with db.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def live(db: psycopg.Connection[Any]) -> tuple[str, str, str]:
    """线上版本的版本号、嵌入模型与分块器版本。"""
    found = rows(
        db,
        """SELECT v.version, v.embedding_model, v.chunker_version FROM idx.aliases a
           JOIN idx.index_versions v USING (version)
           WHERE a.alias = 'prod' AND v.status = 'live'""",
    )
    assert found, "prod 别名应指向一个线上版本"
    version, model, chunker = found[0]
    return str(version), str(model), str(chunker)


def test_published_version_contains_all_pages(db: psycopg.Connection[Any]) -> None:
    version, model, _ = live(db)
    assert model == "bge-m3@mock-hash-1"
    docs = {
        r[0]
        for r in rows(
            db,
            """SELECT vd.doc_id FROM idx.version_docs vd JOIN idx.documents d USING (doc_id)
               WHERE vd.version = %s AND d.source_id = %s""",
            version,
            SOURCE,
        )
    }
    assert docs == PAGES


def test_chunks_have_valid_spans_and_vectors(db: psycopg.Connection[Any]) -> None:
    version, _, _ = live(db)
    chunks = rows(
        db,
        """SELECT c.chunk_id, c.text, (c.span->>'char_start')::int, (c.span->>'char_end')::int,
                  (c.span->>'line_start')::int, vector_dims(c.embedding), c.doc_id, c.heading_path
           FROM idx.chunks c
           JOIN idx.version_docs vd ON vd.doc_id = c.doc_id AND vd.doc_version = c.doc_version
           WHERE vd.version = %s AND c.chunk_id LIKE %s AND c.kind = 'content'""",
        version,
        SOURCE + ":%",
    )
    assert len(chunks) >= len(PAGES)
    for chunk_id, text, start, end, line, dims, doc_id, _ in chunks:
        assert chunk_id.startswith(f"{SOURCE}:{doc_id}#")
        assert end - start == len(text) and line >= 1, chunk_id
        assert dims == 1024
    long_section = [c for c in chunks if c[6] == "aml-report-procedure" and "报告流程" in c[7]]
    assert len(long_section) >= 2, "超过 800 字的节应按段落切成多块"


def test_acl_is_most_conservative_when_missing(db: psycopg.Connection[Any]) -> None:
    acl = {
        r[0]: (list(r[1]), r[2])
        for r in rows(
            db,
            "SELECT doc_id, acl_tokens, classification FROM idx.doc_acl WHERE source_id = %s",
            SOURCE,
        )
    }
    assert acl["credit-draft-notes"] == (["user:carl"], 2)  # 没有 ACL：只给属主
    assert acl["legacy-orphan"] == ([], 2)  # 没有 ACL 也没有属主：谁都不可见
    assert acl["risk-limit-policy"] == (["group:risk-dept"], 3)
    assert acl["aml-report-procedure"][0] == ["group:credit-l2", "group:risk-dept"]


def test_reingest_is_idempotent(db: psycopg.Connection[Any]) -> None:
    # 连续入库两次（CI 会这样跑）：第二次连接器按同步状态判断没有变化，不发事件（增量入库）
    docs = rows(db, "SELECT doc_id, doc_version FROM idx.documents WHERE source_id = %s", SOURCE)
    assert all(v == 1 for _, v in docs), f"内容未变的文档不应升版本: {docs}"
    dup = rows(
        db,
        """SELECT doc_id FROM idx.chunks WHERE chunk_id LIKE %s
           GROUP BY doc_id HAVING count(DISTINCT doc_version) > 1""",
        SOURCE + ":%",
    )
    assert dup == [], "重复入库不应产生新的块版本"


def request(
    model: str, chunker: str, doc_id: str, with_span: bool = True
) -> index_writer_pb2.WriteDocumentRequest:
    ts = Timestamp()
    ts.FromJsonString("2026-01-01T00:00:00Z")
    chunk = index_writer_pb2.Chunk(
        chunk_id=f"src_e2e:{doc_id}#a1",
        parent_chunk_id=f"src_e2e:{doc_id}#p-1",
        text="测试内容。",
        text_hash="t1",
        embedding=[0.0] * 1024,
    )
    if with_span:
        chunk.span.CopyFrom(types_pb2.Span(char_start=0, char_end=5, line_start=1, line_end=1))
    return index_writer_pb2.WriteDocumentRequest(
        idempotency_key=f"{doc_id}:h1:e2e",
        source_id="src_e2e",
        doc_id=doc_id,
        content_hash="h1",
        occurred_at=ts,
        title="e2e",
        source_uri="test://e2e",
        source_type=types_pb2.SOURCE_TYPE_WIKI,
        classification=types_pb2.CLASSIFICATION_INTERNAL,
        acl_tokens=[],
        chunks=[chunk],
        embedding_model_version=model,
        # 与线上版本一致，否则待发布版本的分块器不一致，之后的入库会被判为需要重建
        chunker_version=chunker,
        pipeline_version="e2e",
    )


def test_index_writer_is_idempotent_and_rejects_missing_span(db: psycopg.Connection[Any]) -> None:
    _, model, chunker = live(db)
    with grpc.insecure_channel(os.environ["BF_INDEXING_ADDR"]) as ch:
        stub = index_writer_pb2_grpc.IndexWriterServiceStub(ch)
        first = stub.WriteDocument(request(model, chunker, "e2e-idempotency"), timeout=10)
        second = stub.WriteDocument(request(model, chunker, "e2e-idempotency"), timeout=10)
        assert first.result in (
            index_writer_pb2.WRITE_RESULT_WRITTEN,
            index_writer_pb2.WRITE_RESULT_UNCHANGED,
        )
        assert second.result == index_writer_pb2.WRITE_RESULT_UNCHANGED
        assert second.doc_version == first.doc_version

        with pytest.raises(grpc.RpcError) as err:
            stub.WriteDocument(request(model, chunker, "e2e-no-span", with_span=False), timeout=10)
        assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert rows(db, "SELECT count(*) FROM idx.chunks WHERE doc_id = 'e2e-idempotency'")[0][0] == 1
    assert rows(db, "SELECT count(*) FROM idx.documents WHERE doc_id = 'e2e-no-span'")[0][0] == 0
