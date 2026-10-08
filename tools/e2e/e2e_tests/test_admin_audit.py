"""审计查询（P3-M4，governance 详设 1.1、9.3）：审计岗按用户、文档、时间范围查 gateway 的入口审计；
其他角色 403；查询本身既写 gateway 的管理审计，也写 governance 的变更审计。
"""

import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from .admin import call
from .test_answers import QUESTION, ask
from .test_audit import BUCKET, s3

PATH = "/v1/admin/audit"


def window() -> dict[str, str]:
    now = datetime.now(UTC)
    return {
        "from": (now - timedelta(minutes=10)).isoformat(timespec="seconds"),
        "to": (now + timedelta(minutes=1)).isoformat(timespec="seconds"),
    }


def query_all(params: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    token = ""
    for _ in range(50):
        p = {**params, "page_size": 200}
        if token:
            p["page_token"] = token
        r = call("audra", "GET", PATH, params=p)
        assert r.status_code == 200, r.text
        body = r.json()
        out += body["records"]
        token = body.get("next_page_token", "")
        if not token:
            break
    return out


def test_auditor_queries_by_user_and_document() -> None:
    a = ask("alice", "credit", QUESTION)
    assert a["citations"], a
    doc = a["citations"][0]["doc_id"]
    found: list[dict[str, Any]] = []
    # gateway 每 5 秒送一次审计段
    for _ in range(30):
        found = [
            r
            for r in query_all({**window(), "sub": "alice"})
            if r.get("answer_id") == a["answer_id"]
        ]
        if found:
            break
        time.sleep(1)
    assert found, "按用户查不到这次问答"
    rec = found[0]
    assert rec["sub"] == "alice" and rec["tenant"] == "credit" and rec["endpoint"] == "answers"
    assert set(rec["shown_chunk_ids"]) == {c["chunk_id"] for c in a["citations"]}

    by_doc = [
        r for r in query_all({**window(), "doc_id": doc}) if r.get("answer_id") == a["answer_id"]
    ]
    assert by_doc and "shown" in by_doc[0]["matched_in"], by_doc
    # 按用户再按文档：别人的记录不出现
    assert all(r["sub"] == "alice" for r in query_all({**window(), "sub": "alice", "doc_id": doc}))

    # 查询本身写 governance 的变更审计
    c = s3()
    actions = []
    for obj in c.list_objects(BUCKET, prefix="governance/changes/", recursive=True):
        x = json.loads(c.get_object(BUCKET, obj.object_name).read().decode())
        if x.get("action") == "audit.query" and x.get("actor") == "audra":
            actions.append(x["detail"])
    assert any(d.get("doc_id") == doc for d in actions), "审计查询没有写变更审计"


def test_only_auditor_can_query() -> None:
    for user in ("olga", "ivan", "alice"):
        assert call(user, "GET", PATH, params=window()).status_code == 403, user
    # 必须带时间范围，跨度不超过 31 天
    assert call("audra", "GET", PATH, params={"sub": "alice"}).status_code == 400
    wide = {"from": "2026-01-01T00:00:00Z", "to": "2026-03-01T00:00:00Z"}
    assert call("audra", "GET", PATH, params=wide).status_code == 400
