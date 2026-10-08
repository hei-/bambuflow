"""索引版本的管理接口（P3-M3，入库管道详设 14.4）：版本列表与清单摘要；切换或回滚需双人复核——
第一次调用只创建待复核的操作（op_），另一位平台运维复核后才执行，发起人不能自批；
目标版本清单不全时拒绝。
"""

import os
import uuid

import psycopg

from bambuflow.admin.v1 import index_admin_pb2

from .admin import call
from .test_index_admin import admin, live_models, write

PATH = "/v1/admin/index-versions"


def publish_new_version() -> tuple[str, str]:
    """写一份新文档并发布，返回（原线上版本，新线上版本）。"""
    before, model, chunker = live_models()
    doc = "e2e-ver-" + uuid.uuid4().hex[:8]
    write(doc, f"索引版本测试 {doc}", "2026-10-01T00:00:00Z", model, chunker)
    pub = admin().Publish(index_admin_pb2.PublishRequest(), timeout=30)
    return before, str(pub.version)


def test_list_versions_with_manifest_summary() -> None:
    before, live = publish_new_version()
    r = call("olga", "GET", PATH, params={"page_size": 200})
    assert r.status_code == 200, r.text
    versions = {v["version"]: v for v in r.json()["versions"]}
    assert versions[live]["status"] == "live" and versions[before]["status"] == "retired"
    v = versions[live]
    assert v["doc_count"] > 0 and v["missing_doc_count"] == 0 and v["embedding_model"]
    assert v["published_at"] and v["created_at"]
    # 只有平台运维能看
    assert call("audra", "GET", PATH).status_code == 403
    assert call("alice", "GET", PATH).status_code == 403


def test_activate_requires_second_operator() -> None:
    before, live = publish_new_version()
    op = call("olga", "POST", f"{PATH}/{before}/activate", {"reason": "e2e 回滚演练"})
    assert op.status_code == 202, op.text
    o = op.json()
    assert o["operation_id"].startswith("op_") and o["status"] == "pending"
    assert o["requested_by"] == "olga" and o["live_version"] == live and o["expires_at"]
    # 申请本身不切换
    assert live_models()[0] == live
    # 同一版本再申请：返回已有的待复核操作
    again = call("oscar", "POST", f"{PATH}/{before}/activate", {"reason": "重复申请"})
    assert again.status_code == 200 and again.json()["operation_id"] == o["operation_id"]

    approve = f"{PATH}/operations/{o['operation_id']}/approve"
    selfie = call("olga", "POST", approve)
    assert selfie.status_code == 403 and selfie.json()["type"] == "self_approval_forbidden"
    assert call("audra", "POST", approve).status_code == 403
    assert live_models()[0] == live

    done = call("oscar", "POST", approve)
    assert done.status_code == 200, done.text
    d = done.json()
    assert d["status"] == "executed" and d["approved_by"] == "oscar" and d["approved_at"]
    assert live_models()[0] == before
    # 已执行的操作不能再复核
    assert call("ivan", "POST", approve).status_code == 403
    assert call("olga", "POST", approve).status_code == 403
    again = call("rick", "POST", approve)
    assert again.status_code == 403
    assert call("oscar", "POST", approve).json()["type"] == "conflict"

    # 切回去，恢复其他用例看到的线上版本
    back = call("oscar", "POST", f"{PATH}/{live}/activate", {"reason": "e2e 切回"})
    assert back.status_code == 202, back.text
    ok = call("olga", "POST", f"{PATH}/operations/{back.json()['operation_id']}/approve")
    assert ok.status_code == 200 and live_models()[0] == live


def test_activate_refuses_incomplete_manifest_and_bad_targets() -> None:
    before, live = publish_new_version()
    # 当前线上版本、不存在的版本都不能切
    r = call("olga", "POST", f"{PATH}/{live}/activate", {"reason": "x"})
    assert r.status_code == 409 and r.json()["type"] == "activation_refused"
    r = call("olga", "POST", f"{PATH}/ix-does-not-exist/activate", {"reason": "x"})
    assert r.status_code == 409 and r.json()["type"] == "activation_refused"
    assert call("olga", "POST", f"{PATH}/{before}/activate", {}).status_code == 400

    # 制造一个清单不全的旧版本：清单里多一份块记录不存在的文档
    with psycopg.connect(os.environ["BF_PG_DSN"], autocommit=True) as conn:
        doc = conn.execute(
            "SELECT doc_id FROM idx.version_docs WHERE version = %s LIMIT 1", (before,)
        ).fetchone()
        assert doc is not None
        conn.execute(
            """INSERT INTO idx.version_docs (version, doc_id, doc_version) VALUES (%s, %s, 999999)
               ON CONFLICT (version, doc_id) DO UPDATE SET doc_version = 999999""",
            (before, doc[0]),
        )
    r = call("olga", "POST", f"{PATH}/{before}/activate", {"reason": "x"})
    assert r.status_code == 409 and r.json()["type"] == "activation_refused"
    listed = {
        v["version"]: v
        for v in call("olga", "GET", PATH, params={"page_size": 200}).json()["versions"]
    }
    assert listed[before]["missing_doc_count"] >= 1
