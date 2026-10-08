"""回放与反馈的端到端测试（P3-U1、U2；技术设计文档 13.1，gateway 详设 4.1）。

回答经 gateway 生成后由 orchestrator 写回答记录；回放只给提问人本人与本租户审计岗
（credit-audit 群组，测试用户 carl），引用与列表项按查看者当前的权限复核；
反馈只给提问人，带 Idempotency-Key。MCP 不提供反馈由 test_mcp.py 的工具清单用例覆盖。
需要先 make ingest。
"""

import uuid
from typing import Any

import httpx

from bambuflow.admin.v1 import index_admin_pb2

from .test_answers import QUESTION, ask, ask_stream
from .test_batches import listed
from .test_gateway import GATEWAY, token
from .test_index_admin import admin


def headers(
    user: str, tenant: str = "credit", extra: dict[str, str] | None = None
) -> dict[str, str]:
    return {"Authorization": "Bearer " + token(user), "Bambuflow-Tenant": tenant, **(extra or {})}


def replay(user: str, answer_id: str, tenant: str = "credit") -> httpx.Response:
    return httpx.get(f"{GATEWAY}/v1/answers/{answer_id}", headers=headers(user, tenant), timeout=10)


def feedback(user: str, answer_id: str, body: dict[str, Any], key: str | None) -> httpx.Response:
    extra = {"Idempotency-Key": key} if key else None
    return httpx.post(
        f"{GATEWAY}/v1/answers/{answer_id}/feedback",
        json=body,
        headers=headers(user, extra=extra),
        timeout=10,
    )


def test_asker_replays_same_answer() -> None:
    a = ask("alice", "credit", QUESTION + "（身份证 110101199003077777）")
    r = replay("alice", a["answer_id"])
    assert r.status_code == 200, r.text
    got = r.json()
    assert got["access"] == "asker" and not got["body_withheld"]
    for k in ("answer_id", "status", "answer", "sentences", "blocks", "citations", "answer_kind"):
        assert got[k] == a[k], k
    assert got["why"]["index_version"] == a["why"]["index_version"]
    # 问题按 PII 掩码后保存
    assert "110101199003077777" not in got["question"] and "110101" in got["question"]
    assert "audit" not in got


def test_stream_answer_can_be_replayed() -> None:
    events = ask_stream("alice", "credit", QUESTION)
    meta = events[0][1]
    final = events[-1][1]
    r = replay("alice", meta["answer_id"])
    assert r.status_code == 200, r.text
    assert r.json()["status"] == final["status"]
    assert r.json()["trace_id"] == meta["trace_id"]


def test_others_get_404_and_auditor_can_view() -> None:
    a = ask("alice", "credit", QUESTION)
    # 同租户的普通成员、其他租户的用户：一律 404，与不存在相同
    for user, tenant in (("eve", "ops"), ("bob", "risk")):
        r = replay(user, a["answer_id"], tenant)
        assert r.status_code == 404, (user, r.text)
    assert replay("alice", "ans_" + uuid.uuid4().hex).status_code == 404
    r = replay("carl", a["answer_id"])
    assert r.status_code == 200, r.text
    assert r.json()["access"] == "auditor"


def test_replay_rechecks_citations_with_current_permissions() -> None:
    a = ask("alice", "credit", QUESTION)
    assert a["citations"], a
    gone = a["citations"][0]["doc_id"]
    added = admin().AddDenyRules(
        index_admin_pb2.AddDenyRulesRequest(doc_ids=[gone], reason="e2e 回放复核"), timeout=10
    )
    assert added.added == 1
    try:
        r = replay("alice", a["answer_id"])
        assert r.status_code == 200, r.text
        got = r.json()
        hidden = {c["ref_id"] for c in a["citations"] if c["doc_id"] == gone}
        assert all(c["doc_id"] != gone for c in got["citations"])
        for s in got["sentences"]:
            assert not set(s["ref_ids"]) & hidden, "引用了不可见文档的句子应去掉"
            assert got["answer"][s["start"] : s["end"]] == s["text"]
        if not any(s["ref_ids"] for s in got["sentences"]):
            assert got["status"] == "insufficient_evidence"
    finally:
        admin().RemoveDenyRules(index_admin_pb2.RemoveDenyRulesRequest(doc_ids=[gone]), timeout=10)


def test_replay_rechecks_listed_documents() -> None:
    a = ask("alice", "credit", "信贷管理部有哪些制度？")
    assert a["answer_kind"] == "enumeration", a
    docs = listed(a)
    assert docs
    added = admin().AddDenyRules(
        index_admin_pb2.AddDenyRulesRequest(doc_ids=[docs[0]], reason="e2e 回放复核"), timeout=10
    )
    assert added.added == 1
    try:
        got = replay("alice", a["answer_id"]).json()
        assert listed(got) == docs[1:]
    finally:
        admin().RemoveDenyRules(
            index_admin_pb2.RemoveDenyRulesRequest(doc_ids=[docs[0]]), timeout=10
        )


def test_feedback_only_by_asker_and_idempotent() -> None:
    a = ask("alice", "credit", QUESTION)
    key = "e2e-" + uuid.uuid4().hex
    body = {"rating": "bad_citation", "sentence_index": 0, "comment": "手机 13812345678 引错了"}
    first = feedback("alice", a["answer_id"], body, key)
    assert first.status_code == 200, first.text
    fb = first.json()
    assert fb["feedback_id"].startswith("fb_") and fb["rating"] == "bad_citation"
    again = feedback("alice", a["answer_id"], body, key)
    assert again.status_code == 200 and again.json()["feedback_id"] == fb["feedback_id"]
    conflict = feedback("alice", a["answer_id"], {"rating": "useful"}, key)
    assert conflict.status_code == 409 and conflict.json()["type"] == "idempotency_conflict"

    # 审计岗能回放，但不能替提问人反馈
    assert feedback("carl", a["answer_id"], {"rating": "useful"}, "k-carl").status_code == 404
    assert feedback("alice", a["answer_id"], {"rating": "useful"}, None).status_code == 400
    bad_index = {"rating": "wrong", "sentence_index": len(a["sentences"])}
    assert feedback("alice", a["answer_id"], bad_index, "k-" + uuid.uuid4().hex).status_code == 400
