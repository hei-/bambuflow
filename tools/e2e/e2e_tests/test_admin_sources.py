"""数据源注册、审批、撤销与租户授权（P3-M5，governance 详设 6.2、7.2）：

平台管理员注册、本部门的数据属主审批（记审批人、时间、依据）后才能授权；
授权以 governance 为唯一事实来源，下发给 retrieval，授权与取消授权 1 分钟内在检索生效；
撤销先写按数据源的紧急拒绝，在线立即不可见。
每个用例注册自己的数据源，不改动测试租户原有的授权。
"""

import os
import time
import uuid
from collections.abc import Callable

import grpc
import psycopg

from bambuflow.admin.v1 import index_admin_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.ingestion.v1 import index_writer_pb2, index_writer_pb2_grpc

from .admin import call
from .test_index_admin import admin, found, live_models, ts

PATH = "/v1/admin/sources"


def wait_until(cond: Callable[[], bool], seconds: int = 60) -> bool:
    """授权变更 1 分钟内生效（retrieval 每 10 秒拉一次授权）。"""
    for _ in range(seconds):
        if cond():
            return True
        time.sleep(1)
    return cond()


def register_and_approve() -> str:
    r = call(
        "paul",
        "POST",
        PATH,
        {
            "type": "wiki",
            "owner_dept": "信贷部",
            "default_classification": "internal",
            "description": "e2e 数据源",
        },
    )
    assert r.status_code == 201, r.text
    src = r.json()
    assert src["source_id"].startswith("src_") and src["status"] == "pending"
    assert src["created_by"] == "paul"
    ok = call("dana", "POST", f"{PATH}/{src['source_id']}/approve", {"basis": "e2e 工单 DATA-1"})
    assert ok.status_code == 200, ok.text
    a = ok.json()
    assert a["status"] == "active" and a["approved_by"] == "dana"
    assert a["approval_basis"] == "e2e 工单 DATA-1" and a["approved_at"]
    return str(src["source_id"])


def write_doc(source: str, word: str) -> str:
    """往数据源里写一份带唯一词的文档并发布。"""
    doc = "e2e-src-" + uuid.uuid4().hex[:8]
    _, model, chunker = live_models()
    text = f"{word} 是一条只用于数据源授权测试的内容。"
    req = index_writer_pb2.WriteDocumentRequest(
        idempotency_key=f"{doc}:1",
        source_id=source,
        doc_id=doc,
        content_hash="h-" + doc,
        occurred_at=ts("2026-10-01T00:00:00Z"),
        title="数据源授权测试",
        source_uri="test://" + doc,
        source_type=types_pb2.SOURCE_TYPE_WIKI,
        classification=types_pb2.CLASSIFICATION_PUBLIC,
        acl_tokens=["group:all-staff"],
        embedding_model_version=model,
        chunker_version=chunker,
        chunks=[
            index_writer_pb2.Chunk(
                chunk_id=f"{source}:{doc}#a1",
                text=text,
                text_hash="t-" + doc,
                span=types_pb2.Span(char_start=0, char_end=len(text), line_start=1, line_end=1),
                embedding=[0.01] * 1024,
            )
        ],
    )
    with grpc.insecure_channel(os.environ["BF_INDEXING_ADDR"]) as ch:
        resp = index_writer_pb2_grpc.IndexWriterServiceStub(ch).WriteDocument(req, timeout=10)
    assert resp.result == index_writer_pb2.WRITE_RESULT_WRITTEN
    admin().Publish(index_admin_pb2.PublishRequest(), timeout=30)
    return doc


def test_grant_and_revoke_take_effect_in_search() -> None:
    src = register_and_approve()
    word = "授权测试" + uuid.uuid4().hex[:8]
    doc = write_doc(src, word)
    # 没有授权：已入库也检索不到
    assert not found(word, doc)

    g = call("dana", "POST", f"{PATH}/{src}/grants", {"tenant": "credit"})
    assert g.status_code == 201 and g.json()["granted_by"] == "dana", g.text
    assert call("dana", "POST", f"{PATH}/{src}/grants", {"tenant": "credit"}).status_code == 200
    assert wait_until(lambda: found(word, doc)), "授权应在 1 分钟内生效"
    listed = call("dana", "GET", f"{PATH}/{src}/grants").json()["grants"]
    assert [x["tenant"] for x in listed] == ["credit"]

    d = call("dana", "DELETE", f"{PATH}/{src}/grants", params={"tenant": "credit"})
    assert d.status_code == 200, d.text
    assert wait_until(lambda: not found(word, doc)), "取消授权应在 1 分钟内生效"
    assert (
        call("dana", "DELETE", f"{PATH}/{src}/grants", params={"tenant": "credit"}).status_code
        == 404
    )

    assert call("dana", "POST", f"{PATH}/{src}/grants", {"tenant": "credit"}).status_code == 201
    assert wait_until(lambda: found(word, doc))

    # 别的部门的属主：看不到（404），也不能按数据源紧急拒绝
    assert call("rick", "POST", f"{PATH}/{src}/revoke", {"reason": "x"}).status_code == 404
    deny = {"scope": "source", "target": src, "reason": "不是本部门"}
    assert call("rick", "POST", "/v1/admin/emergency-denials", deny).status_code == 403

    rv = call("dana", "POST", f"{PATH}/{src}/revoke", {"reason": "e2e 源系统下线"})
    assert rv.status_code == 200, rv.text
    r = rv.json()
    assert (
        r["status"] == "revoked" and r["revoked_by"] == "dana" and r["denial_id"].startswith("den_")
    )
    # 先写了按数据源的紧急拒绝：在线立即不可见
    assert wait_until(lambda: not found(word, doc), 10), "撤销后应立即不可见"
    rules = call("ivan", "GET", "/v1/admin/emergency-denials", params={"page_size": 200}).json()
    assert any(
        x["denial_id"] == r["denial_id"] and x["scope"] == "source" for x in rules["denials"]
    )
    # 撤销发出 source.revoked 事件（governance 的发件表）
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        n = conn.execute("SELECT count(*) FROM governance.outbox").fetchone()
    assert n is not None and n[0] >= 1

    # connectors 消费 source.revoked，对该源全部文档写删除标记，之后的同步都跳过
    def tombstoned() -> bool:
        with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
            row = conn.execute("SELECT state FROM idx.doc_acl WHERE doc_id = %s", (doc,)).fetchone()
        return row is not None and row[0] == "tombstoned"

    assert wait_until(tombstoned), "撤销后应在 1 分钟内给该源的文档写删除标记"
    status = call("olga", "GET", f"{PATH}/{src}/sync-status")
    assert status.status_code == 200 and status.json()["revoked_at"], status.text
    assert call("olga", "POST", f"{PATH}/{src}/sync", {"reason": "x"}).status_code == 409
    # 撤销后不能再授权、不能再撤销
    assert call("dana", "POST", f"{PATH}/{src}/grants", {"tenant": "risk"}).status_code == 409
    assert call("dana", "POST", f"{PATH}/{src}/revoke", {"reason": "x"}).status_code == 409


def test_register_and_approve_permissions() -> None:
    body = {"type": "wiki", "owner_dept": "信贷部", "default_classification": "internal"}
    # 只有平台管理员能注册
    assert call("dana", "POST", PATH, body).status_code == 403
    src = call("paul", "POST", PATH, body).json()["source_id"]
    # 列表：平台管理员看全部，数据属主只看本部门的
    assert src in [
        x["source_id"]
        for x in call("paul", "GET", PATH, params={"status": "pending"}).json()["sources"]
    ]
    assert src not in [x["source_id"] for x in call("rick", "GET", PATH).json()["sources"]]
    # 平台管理员不能审批；别的部门的属主看不到；缺依据 400
    assert call("paul", "POST", f"{PATH}/{src}/approve", {"basis": "x"}).status_code == 403
    assert call("rick", "POST", f"{PATH}/{src}/approve", {"basis": "x"}).status_code == 404
    assert call("dana", "POST", f"{PATH}/{src}/approve", {}).status_code == 400
    # 待审批的数据源不能授权
    assert call("dana", "POST", f"{PATH}/{src}/grants", {"tenant": "credit"}).status_code == 409
    assert call("dana", "POST", f"{PATH}/{src}/approve", {"basis": "x"}).status_code == 200
    assert call("dana", "POST", f"{PATH}/{src}/approve", {"basis": "x"}).status_code == 409


def test_restricted_source_needs_infosec() -> None:
    body = {
        "type": "db",
        "owner_dept": "信贷部",
        "default_classification": "restricted",
        "description": "e2e 受限级",
    }
    src = call("paul", "POST", PATH, body).json()["source_id"]
    infosec = {"basis": "SEC-1", "capacity": "infosec"}
    # 属主批过仍待信息安全审批，不能授权
    first = call("dana", "POST", f"{PATH}/{src}/approve", {"basis": "DATA-1"})
    assert first.status_code == 200 and first.json()["status"] == "pending", first.text
    assert call("dana", "POST", f"{PATH}/{src}/grants", {"tenant": "credit"}).status_code == 409
    # 属主不能以信息安全的身份审批
    assert call("dana", "POST", f"{PATH}/{src}/approve", infosec).status_code == 403
    done = call("ivan", "POST", f"{PATH}/{src}/approve", infosec)
    assert done.status_code == 200 and done.json()["status"] == "active", done.text
    assert done.json()["security_approved_by"] == "ivan"
