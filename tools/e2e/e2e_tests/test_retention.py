"""按数据源的保留期写删除标记（G3，技术设计文档 9.2）：源端时间早于保留期的文档写删除标记，
之后比删除更早产生的内容不能再写入；没列出的数据源与保留期内的文档不受影响。
用单独的测试数据源，不影响其他用例。
"""

import os
import uuid

import grpc
import psycopg

from bambuflow.admin.v1 import index_admin_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.ingestion.v1 import index_writer_pb2, index_writer_pb2_grpc

from .test_index_admin import admin, live_models, ts

SOURCE = "src_e2eretention"


def write(doc: str, occurred: str, model: str, chunker: str) -> int:
    text = f"保留期测试 {doc}"
    req = index_writer_pb2.WriteDocumentRequest(
        idempotency_key=f"{doc}:{occurred}",
        source_id=SOURCE,
        doc_id=doc,
        content_hash="h-" + occurred,
        occurred_at=ts(occurred),
        title="保留期测试",
        source_uri="test://" + doc,
        source_type=types_pb2.SOURCE_TYPE_MAIL,
        classification=types_pb2.CLASSIFICATION_INTERNAL,
        acl_tokens=["group:all-staff"],
        embedding_model_version=model,
        chunker_version=chunker,
        chunks=[
            index_writer_pb2.Chunk(
                chunk_id=f"{SOURCE}:{doc}#{abs(hash(occurred)) % 10**12:012d}",
                text=text,
                text_hash="t-" + occurred,
                span=types_pb2.Span(char_start=0, char_end=len(text), line_start=1, line_end=1),
                embedding=[0.01] * 1024,
            )
        ],
    )
    with grpc.insecure_channel(os.environ["BF_INDEXING_ADDR"]) as ch:
        result: int = (
            index_writer_pb2_grpc.IndexWriterServiceStub(ch).WriteDocument(req, timeout=10).result
        )
    return result


def state(doc: str) -> str:
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        row = conn.execute("SELECT state FROM idx.doc_acl WHERE doc_id = %s", (doc,)).fetchone()
    assert row is not None
    return str(row[0])


def test_expired_documents_get_tombstoned() -> None:
    _, model, chunker = live_models()
    old, fresh = "e2e-old-" + uuid.uuid4().hex[:6], "e2e-new-" + uuid.uuid4().hex[:6]
    assert (
        write(old, "2020-01-01T00:00:00Z", model, chunker) == index_writer_pb2.WRITE_RESULT_WRITTEN
    )
    assert (
        write(fresh, "2026-10-01T00:00:00Z", model, chunker)
        == index_writer_pb2.WRITE_RESULT_WRITTEN
    )

    resp = admin().ExpireDocuments(
        index_admin_pb2.ExpireDocumentsRequest(retention_days={SOURCE: 365}), timeout=30
    )
    assert resp.tombstoned[SOURCE] >= 1
    assert state(old) == "tombstoned" and state(fresh) == "active"
    # 删除优先：源端重发同一封旧邮件不能让它复活
    assert (
        write(old, "2020-01-02T00:00:00Z", model, chunker)
        == index_writer_pb2.WRITE_RESULT_STALE_VERSION
    )
    # 再跑一次没有新的到期文档
    again = admin().ExpireDocuments(
        index_admin_pb2.ExpireDocumentsRequest(retention_days={SOURCE: 365}), timeout=30
    )
    assert again.tombstoned[SOURCE] == 0


def counts(doc: str) -> tuple[int, int, str, int]:
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        chunks = conn.execute(
            "SELECT count(*) FROM idx.chunks WHERE doc_id = %s", (doc,)
        ).fetchone()
        manifests = conn.execute(
            "SELECT count(*) FROM idx.version_docs WHERE doc_id = %s", (doc,)
        ).fetchone()
        title = conn.execute("SELECT title FROM idx.documents WHERE doc_id = %s", (doc,)).fetchone()
        receipts = conn.execute(
            "SELECT count(*) FROM idx.deletion_receipts WHERE doc_id = %s", (doc,)
        ).fetchone()
    assert chunks and manifests and title and receipts
    return int(chunks[0]), int(manifests[0]), str(title[0]), int(receipts[0])


def test_purge_removes_all_index_copies_and_writes_receipt() -> None:
    _, model, chunker = live_models()
    doc = "e2e-purge-" + uuid.uuid4().hex[:6]
    assert (
        write(doc, "2020-03-01T00:00:00Z", model, chunker) == index_writer_pb2.WRITE_RESULT_WRITTEN
    )
    admin().ExpireDocuments(
        index_admin_pb2.ExpireDocumentsRequest(retention_days={SOURCE: 365}), timeout=30
    )
    assert counts(doc)[0] > 0
    # 观察期还没过：不删
    admin().PurgeDeleted(index_admin_pb2.PurgeDeletedRequest(grace_hours=24 * 365), timeout=60)
    assert counts(doc)[0] > 0
    # 删除时间是刚才 expire 时写的：往前调两小时，模拟已过了 1 小时的观察期
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        conn.execute(
            "UPDATE idx.doc_acl SET tombstoned_at = now() - interval '2 hours' WHERE doc_id = %s",
            (doc,),
        )
    resp = admin().PurgeDeleted(index_admin_pb2.PurgeDeletedRequest(grace_hours=1), timeout=60)
    assert resp.purged >= 1
    assert counts(doc) == (0, 0, "", 1)
    assert state(doc) == "tombstoned"  # 删除标记保留，删除优先照常
    assert (
        write(doc, "2020-03-02T00:00:00Z", model, chunker)
        == index_writer_pb2.WRITE_RESULT_STALE_VERSION
    )
