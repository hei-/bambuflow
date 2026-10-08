"""紧急拒绝规则（P3-M2，governance 详设 4.2、8.2；retrieval 详设 10.2）：
信息安全按文档拒绝后，检索的召回过滤与返回前复核都看不到它；
规则只能新增，同一对象只登记一次；数据属主只能按本部门的数据源拒绝；
每次新增写 governance 的变更审计（与 gateway 的管理审计同一个 trace_id）。

为了不影响其他用例，被拒绝的对象都是本用例新写的文档（规则没有删除接口）。
按数据源拒绝在 test_admin_sources.py 里对新注册的数据源验证；
按租户拒绝只有单元测试（测试租户被拒绝会影响其他用例）。
"""

import json
import time
import uuid
from collections.abc import Callable
from typing import Any

from minio import Minio

from bambuflow.admin.v1 import index_admin_pb2

from .admin import call
from .test_audit import BUCKET, s3
from .test_index_admin import admin, found, live_models, write

PATH = "/v1/admin/emergency-denials"


def wait_until(cond: Callable[[], bool], seconds: int = 60) -> bool:
    """1 分钟内生效（governance 详设 4.2）：按秒轮询。"""
    for _ in range(seconds):
        if cond():
            return True
        time.sleep(1)
    return bool(cond())


def test_document_denial_takes_effect_and_is_add_only() -> None:
    word = "紧急拒绝测试" + uuid.uuid4().hex[:8]
    doc = "e2e-deny-" + uuid.uuid4().hex[:8]
    _, model, chunker = live_models()
    write(doc, f"{word} 是一条只用于紧急拒绝测试的内容。", "2026-10-01T00:00:00Z", model, chunker)
    admin().Publish(index_admin_pb2.PublishRequest(), timeout=30)
    assert found(word, doc)

    body = {"scope": "document", "target": doc, "reason": "e2e 工单 SEC-" + doc}
    r = call("ivan", "POST", PATH, body)
    assert r.status_code == 201, r.text
    d = r.json()
    assert d["denial_id"].startswith("den_") and d["created_by"] == "ivan"
    assert d["status"] in ("active", "pending")
    assert wait_until(lambda: not found(word, doc)), "紧急拒绝应在 1 分钟内生效"

    # 同一对象再登记：返回已有的那一条
    again = call("ivan", "POST", PATH, body)
    assert again.status_code == 200 and again.json()["denial_id"] == d["denial_id"]
    listed = call("ivan", "GET", PATH, params={"page_size": 200}).json()["denials"]
    mine = [x for x in listed if x["denial_id"] == d["denial_id"]]
    assert mine and mine[0]["status"] == "active" and mine[0]["applied_at"]

    # 只能新增：没有删除接口
    assert call("ivan", "DELETE", PATH + "/" + d["denial_id"]).status_code == 404
    assert not found(word, doc)

    # 变更审计：governance 写进审计桶，带入口的 trace_id
    c = s3()
    rec = None
    for obj in c.list_objects(BUCKET, prefix="governance/changes/", recursive=True):
        x = json.loads(c.get_object(BUCKET, obj.object_name).read().decode())
        if x.get("object_id") == d["denial_id"]:
            rec = x
    assert rec is not None, "变更审计里找不到这条规则"
    assert rec["action"] == "emergency_denial.create" and rec["actor"] == "ivan"
    assert rec["roles"] == ["infosec"] and rec["trace_id"]
    # 同一个 trace_id 能在 gateway 的管理审计里找到这次请求（gateway 每 5 秒送一次审计段）
    assert wait_until(lambda: gateway_admin_record(c, rec["trace_id"]) is not None, 30)
    g = gateway_admin_record(c, rec["trace_id"])
    assert g is not None and g["sub"] == "ivan" and g["status"] == 201
    assert g["admin"]["owner"] == "governance" and g["admin"]["path"] == PATH
    assert doc in g["admin"]["request"] and g["admin"]["request_sha256"]


def gateway_admin_record(c: Minio, trace_id: str) -> dict[str, Any] | None:
    for obj in c.list_objects(BUCKET, prefix="gateway/", recursive=True):
        for line in c.get_object(BUCKET, obj.object_name).read().decode().splitlines():
            rec: dict[str, Any] = json.loads(line)
            if rec.get("trace_id") == trace_id and rec.get("admin"):
                return rec
    return None


def test_denial_permissions_and_idempotency() -> None:
    doc = "e2e-deny-perm-" + uuid.uuid4().hex[:8]
    body = {"scope": "document", "target": doc, "reason": "e2e 权限检查"}
    # 数据属主只能按本部门的数据源拒绝；平台运维、普通用户没有这个权限
    for user, b in [
        ("dana", body),
        ("dana", {"scope": "source", "target": "src_evalrisk", "reason": "不是本部门"}),
        ("olga", body),
        ("alice", body),
    ]:
        r = call(user, "POST", PATH, b)
        assert r.status_code == 403, (user, r.text)
    assert call("ivan", "POST", PATH, body, key=None).status_code == 400
    assert (
        call(
            "ivan", "POST", PATH, {"scope": "everything", "target": doc, "reason": "x"}
        ).status_code
        == 400
    )

    # 幂等键：同键同内容返回第一次的结果，同键不同内容 409
    key = "e2e-" + uuid.uuid4().hex
    first = call("ivan", "POST", PATH, body, key=key)
    assert first.status_code == 201, first.text
    replay = call("ivan", "POST", PATH, body, key=key)
    assert replay.status_code == 201 and replay.json() == first.json()
    other = call("ivan", "POST", PATH, {**body, "reason": "换了原因"}, key=key)
    assert other.status_code == 409 and other.json()["type"] == "idempotency_conflict"
