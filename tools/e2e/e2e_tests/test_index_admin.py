"""增量发布、删除标记与回滚（P2-T0，入库管道详设第 4、9、10 节；回滚经管理接口双人复核，P3-M3）。

经 IndexWriter 写一份带唯一词的文档、用管理接口发布，再经 gateway 检索验证：删除标记对线上立即生效，
删除优先于更早的写入，回滚不会让删除的文档复活。
"""

import os
import uuid
from typing import Any

import grpc
import httpx
import psycopg
from google.protobuf.timestamp_pb2 import Timestamp

from bambuflow.admin.v1 import index_admin_pb2, index_admin_pb2_grpc
from bambuflow.common.v1 import types_pb2
from bambuflow.ingestion.v1 import index_writer_pb2, index_writer_pb2_grpc

from .admin import call
from .test_gateway import GATEWAY, token

SOURCE = "src_evalcredit"  # credit 租户的数据源（tools/test-upstream/sources.yaml）


def ts(text: str) -> Timestamp:
    t = Timestamp()
    t.FromJsonString(text)
    return t


def live_models() -> tuple[str, str, str]:
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        row = conn.execute(
            """SELECT a.version, v.embedding_model, v.chunker_version FROM idx.aliases a
               JOIN idx.index_versions v USING (version) WHERE a.alias = 'prod'"""
        ).fetchone()
    assert row is not None
    return str(row[0]), str(row[1]), str(row[2])


def write(
    doc: str, text: str, occurred: str, model: str, chunker: str, content_hash: str = ""
) -> int:
    req = index_writer_pb2.WriteDocumentRequest(
        idempotency_key=f"{doc}:{occurred}",
        source_id=SOURCE,
        doc_id=doc,
        content_hash=content_hash or "h-" + occurred,
        occurred_at=ts(occurred),
        title="增量发布测试",
        source_uri="test://" + doc,
        source_type=types_pb2.SOURCE_TYPE_WIKI,
        classification=types_pb2.CLASSIFICATION_PUBLIC,
        acl_tokens=["group:all-staff"],
        embedding_model_version=model,
        chunker_version=chunker,
        chunks=[
            index_writer_pb2.Chunk(
                chunk_id=f"{SOURCE}:{doc}#{occurred[:4]}{abs(hash(text)) % 10**12:012d}",
                text=text,
                text_hash="t-" + occurred,
                span=types_pb2.Span(char_start=0, char_end=len(text), line_start=1, line_end=1),
                embedding=[0.01] * 1024,
            )
        ],
    )
    with grpc.insecure_channel(os.environ["BF_INDEXING_ADDR"]) as ch:
        resp = index_writer_pb2_grpc.IndexWriterServiceStub(ch).WriteDocument(req, timeout=10)
    result: int = resp.result
    return result


def tombstone(doc: str, occurred: str) -> int:
    req = index_writer_pb2.TombstoneDocumentRequest(
        source_id=SOURCE, doc_id=doc, occurred_at=ts(occurred)
    )
    with grpc.insecure_channel(os.environ["BF_INDEXING_ADDR"]) as ch:
        resp = index_writer_pb2_grpc.IndexWriterServiceStub(ch).TombstoneDocument(req, timeout=10)
    result: int = resp.result
    return result


def admin() -> Any:
    return index_admin_pb2_grpc.IndexAdminServiceStub(
        grpc.insecure_channel(os.environ["BF_INDEXING_ADDR"])
    )


def found(word: str, doc: str) -> bool:
    r = httpx.post(
        GATEWAY + "/v1/search",
        json={"question": word, "top_k": 50},
        headers={"Authorization": "Bearer " + token("alice"), "Bambuflow-Tenant": "credit"},
        timeout=10,
    )
    assert r.status_code == 200, r.text
    return any(h["doc_id"] == doc for h in r.json()["hits"])


def test_tombstone_takes_effect_on_live_and_survives_rollback() -> None:
    word = "增量测试" + uuid.uuid4().hex[:8]
    doc = "e2e-p2-" + uuid.uuid4().hex[:8]
    before, model, chunker = live_models()
    assert write(
        doc, f"{word} 是一条只用于增量发布测试的内容。", "2026-10-01T00:00:00Z", model, chunker
    ) == (index_writer_pb2.WRITE_RESULT_WRITTEN)
    pub = admin().Publish(index_admin_pb2.PublishRequest(), timeout=30)
    # 增量发布：本轮只写了一份，原线上版本的其余文档原样带入
    assert pub.carried > 0
    assert found(word, doc)

    assert tombstone(doc, "2026-10-02T00:00:00Z") == index_writer_pb2.TOMBSTONE_RESULT_TOMBSTONED
    assert not found(word, doc), "删除标记应对线上立即生效"
    # 删除优先：比删除更早产生的内容写入被丢弃
    assert write(doc, f"{word} 旧内容", "2026-10-01T12:00:00Z", model, chunker) == (
        index_writer_pb2.WRITE_RESULT_STALE_VERSION
    )

    # 回滚与切回经管理接口双人复核（test_admin_index_versions.py）：olga 申请、oscar 复核
    back = switch(before)
    assert back["live_version"] == pub.version and back["target_version"] == before
    forward = switch(pub.version)
    assert forward["live_version"] == before and forward["status"] == "executed"
    # 文档仍在这个版本的清单里，但删除标记在文档级、不随回滚复活
    assert not found(word, doc)


def switch(version: str) -> dict[str, Any]:
    """把线上别名切到 version：一位平台运维申请，另一位复核后执行。"""
    op = call(
        "olga", "POST", f"/v1/admin/index-versions/{version}/activate", {"reason": "e2e 回滚"}
    )
    assert op.status_code == 202, op.text
    done = call(
        "oscar", "POST", f"/v1/admin/index-versions/operations/{op.json()['operation_id']}/approve"
    )
    assert done.status_code == 200 and done.json()["status"] == "executed", done.text
    result: dict[str, Any] = done.json()
    return result


def update_acl(doc: str, tokens: list[str], occurred: str) -> int:
    req = index_writer_pb2.UpdateAclRequest(
        source_id=SOURCE,
        doc_id=doc,
        acl_tokens=tokens,
        classification=types_pb2.CLASSIFICATION_PUBLIC,
        occurred_at=ts(occurred),
    )
    with grpc.insecure_channel(os.environ["BF_INDEXING_ADDR"]) as ch:
        resp = index_writer_pb2_grpc.IndexWriterServiceStub(ch).UpdateAcl(req, timeout=10)
    result: int = resp.result
    return result


def test_acl_fast_path_and_deny_list_take_effect_immediately() -> None:
    word = "撤权测试" + uuid.uuid4().hex[:8]
    doc = "e2e-acl-" + uuid.uuid4().hex[:8]
    _, model, chunker = live_models()
    write(doc, f"{word} 是一条只用于撤权测试的内容。", "2026-10-01T00:00:00Z", model, chunker)
    admin().Publish(index_admin_pb2.PublishRequest(), timeout=30)
    assert found(word, doc)

    # 撤权走快速通道：不重新入库、不发布，下一次检索即不可见
    assert update_acl(doc, ["group:risk-dept"], "2026-10-02T00:00:00Z") == (
        index_writer_pb2.ACL_RESULT_UPDATED
    )
    assert not found(word, doc)
    # 乱序到达的旧权限事件不生效
    assert update_acl(doc, ["group:all-staff"], "2026-10-01T12:00:00Z") == (
        index_writer_pb2.ACL_RESULT_STALE
    )
    assert not found(word, doc)
    assert update_acl(doc, ["group:all-staff"], "2026-10-03T00:00:00Z") == (
        index_writer_pb2.ACL_RESULT_UPDATED
    )
    assert found(word, doc)

    # 紧急拒绝名单：任何人都检索不到，撤销后恢复
    added = admin().AddDenyRules(
        index_admin_pb2.AddDenyRulesRequest(doc_ids=[doc], reason="e2e 拒绝名单测试"), timeout=10
    )
    assert added.added == 1 and not found(word, doc)
    removed = admin().RemoveDenyRules(
        index_admin_pb2.RemoveDenyRulesRequest(doc_ids=[doc]), timeout=10
    )
    assert removed.removed == 1 and found(word, doc)


def test_unchanged_content_with_stale_model_is_rewritten() -> None:
    """内容没变、但已有的块是别的嵌入模型算的（换过模型）：必须重写，不能让新版本引用旧向量。"""
    word = "换模型测试" + uuid.uuid4().hex[:8]
    doc = "e2e-model-" + uuid.uuid4().hex[:8]
    _, model, chunker = live_models()
    text = f"{word} 是一条只用于换模型测试的内容。"
    first = write(doc, text, "2026-10-01T00:00:00Z", model, chunker, content_hash="same")
    assert first == index_writer_pb2.WRITE_RESULT_WRITTEN
    with psycopg.connect(os.environ["BF_PG_DSN"], autocommit=True) as conn:
        conn.execute(
            "UPDATE idx.chunks SET embedding_model = 'bge-m3@old' WHERE doc_id = %s", (doc,)
        )
    # 内容摘要相同的再次写入：以前判为“未变化”，现在要发现块的模型不对而重写
    again = write(doc, text, "2026-10-01T00:00:01Z", model, chunker, content_hash="same")
    assert again == index_writer_pb2.WRITE_RESULT_WRITTEN
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        row = conn.execute(
            """SELECT count(*) FILTER (WHERE c.embedding_model <> %s) FROM idx.index_versions v
               JOIN idx.version_docs vd ON vd.version = v.version
               JOIN idx.chunks c ON c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version
               WHERE v.status = 'pending' AND vd.doc_id = %s""",
            (model, doc),
        ).fetchone()
    assert row is not None and row[0] == 0, "待发布版本引用了旧模型的块"
