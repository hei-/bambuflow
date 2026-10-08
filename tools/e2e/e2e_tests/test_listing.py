"""枚举与计数类问答（P2-T10，K2 详设第 5、8 节）。

列表与总数和按权限暴力计算的结果一致，无权文档不出现、不影响数量。alice 的权限 token 取自
测试目录（tools/test-upstream 的 LDAP 数据，与评测集的提问身份 credit_l2 一致）。
"""

import json
import os
from pathlib import Path
from typing import Any

import psycopg

from .test_answers import ask

ALICE_TOKENS = ["user:alice", "dept:信贷部", "group:credit-l2", "group:all-staff"]
ALICE_CLEARANCE = 2
TENANTS = Path(__file__).resolve().parents[2] / "test-upstream" / "policy" / "data.json"


def visible(filters: dict[str, Any]) -> set[str]:
    """alice 在 credit 租户下可见、且满足 why.inferred_filters 的文档（直接查库，不经被测代码）。"""
    # 租户授权以 governance 为准（P3-M5），租户策略里禁止的数据源再减掉
    policy = json.loads(TENANTS.read_text())["tenants"]["credit"]
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        rows = conn.execute(
            """SELECT g.source_id FROM governance.source_grants g
               JOIN governance.sources s USING (source_id)
               WHERE g.tenant = 'credit' AND s.status = 'active'"""
        ).fetchall()
    sources = sorted({r[0] for r in rows} - set(policy.get("denied_sources", [])))
    sql = """SELECT d.doc_id FROM idx.aliases al
             JOIN idx.version_docs vd ON vd.version = al.version
             JOIN idx.documents d ON d.doc_id = vd.doc_id
             JOIN idx.doc_acl a ON a.doc_id = vd.doc_id
             WHERE al.alias = 'prod' AND a.state = 'active' AND a.classification <= %s
               AND a.acl_tokens && %s AND a.source_id = ANY(%s)
               AND NOT EXISTS (SELECT 1 FROM idx.deny_list dl WHERE dl.doc_id = d.doc_id)
               AND NOT EXISTS (SELECT 1 FROM idx.deny_scopes ds
                               WHERE ds.kind = 'source' AND ds.value = a.source_id)"""
    args: list[Any] = [ALICE_CLEARANCE, ALICE_TOKENS, sources]
    if filters.get("doc_types"):
        sql += " AND d.doc_type = ANY(%s)"
        args.append(filters["doc_types"])
    if filters.get("depts"):
        sql += " AND d.dept = ANY(%s)"
        args.append(filters["depts"])
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        return {r[0] for r in conn.execute(sql, args).fetchall()}


def scope_of(a: dict[str, Any]) -> dict[str, Any]:
    (block,) = [b for b in a["blocks"] if b["block_type"] == "scope"]
    scope: dict[str, Any] = block["scope"]
    return scope


def test_enumeration_lists_exactly_the_visible_matching_docs() -> None:
    a = ask("alice", "credit", "信贷管理部有哪些制度？")
    assert a["answer_kind"] == "enumeration" and a["status"] == "answered"
    inferred = a["why"]["inferred_filters"]
    assert "DT-POLICY" in inferred["doc_types"] and inferred["depts"] == ["DEPT-CREDIT"]
    want = visible(inferred)
    listed = {s["document"]["doc_id"] for s in a["sentences"] if s["claim_type"] == "list_item"}
    scope = scope_of(a)
    assert want and len(want) <= 30
    assert listed == want, f"多列 {sorted(listed - want)}，漏列 {sorted(want - listed)}"
    assert (
        scope["total_count"] == len(want)
        and scope["total_is_exact"]
        and scope["shown"] == len(want)
    )
    assert scope["basis"] == "仅统计你有权查看的文档" and "无权" not in a["answer"]


def test_count_matches_brute_force() -> None:
    a = ask("alice", "credit", "制度有多少份？")
    assert a["answer_kind"] == "count"
    want = visible(a["why"]["inferred_filters"])
    assert scope_of(a)["total_count"] == len(want)
    assert not [s for s in a["sentences"] if s["claim_type"] == "list_item"]


def test_ordinary_question_is_not_routed_to_listing() -> None:
    a = ask("alice", "credit", "个人消费贷款的额度上限是多少？")
    assert a["answer_kind"] == "fact" and "inferred_filters" not in a["why"]
