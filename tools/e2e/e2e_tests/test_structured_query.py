"""结构化数据实时查询 sql_query（P3-Q，总体设计 6.2，orchestrator 详设 6.3）。

视图目录见 deploy/compose/compose.yaml 的 sql_catalog（只开放给运维租户），
演示库与查询账号由 make ingest 的 dbschema-seed 建。
模拟 LLM 按问题里的 [mock-sql:{参数}] 规划成 sql_query。

- sam（sre-oncall → 报表角色 report_ops）能查贷款余额，结果作为 source_type = db 的证据被引用；
- eve 没有映射到任何报表角色：查询被拒（fail-closed），证据不足；
- 不在白名单里的视图（客户联系方式）参数校验就拒绝，根本不会发到数据库；
- 没开放 sql_query 的租户（信贷）同样的问题不走结构化路径。
"""

import json
from typing import Any

from .test_answers import ask

BALANCE = {
    "view": "bankdemo.v_loan_balance",
    "measures": [{"agg": "sum", "column": "balance"}],
    "filters": [{"column": "branch", "op": "=", "value": "演示网点甲"}],
}


def question(args: dict[str, Any]) -> str:
    return "演示网点甲的贷款余额合计是多少？[mock-sql:" + json.dumps(args, ensure_ascii=False) + "]"


def degraded(a: dict[str, Any]) -> set[tuple[str, str]]:
    return {(d["stage"], d["reason"]) for d in a["why"]["degraded"]}


def test_caller_with_report_role_gets_cited_query_result() -> None:
    a = ask("sam", "ops", question(BALANCE))
    assert a["why"]["intent"] == "structured", a["why"]
    calls = [c for r in a["why"]["agent_rounds"] for c in r["calls"]]
    assert [c["tool"] for c in calls] == ["sql_query"] and calls[0]["covered"]
    assert a["status"] == "answered", a
    (c,) = [c for c in a["citations"] if c["source_type"] == "db"]
    assert c["doc_id"] == "db:bankdemo.v_loan_balance" and c["title"] == "贷款余额"
    assert any("765432.1" in s["text"] for s in a["sentences"])


def test_caller_without_report_role_is_denied() -> None:
    a = ask("eve", "ops", question(BALANCE))
    assert a["why"]["intent"] == "structured"
    assert ("sql_query", "denied") in degraded(a)
    assert a["status"] == "insufficient_evidence"
    assert not [c for c in a["citations"] if c["source_type"] == "db"]


def test_view_outside_catalog_is_rejected_before_the_database() -> None:
    a = ask("sam", "ops", question({"view": "bankdemo.v_customer_contact", "columns": ["phone"]}))
    assert ("sql_query", "invalid") in degraded(a)
    assert not [c for c in a["citations"] if c["source_type"] == "db"]


def test_tenant_without_sql_query_takes_the_normal_path() -> None:
    a = ask("alice", "credit", question(BALANCE))
    assert a["why"].get("intent") != "structured"
    assert not [c for c in a["citations"] if c["source_type"] == "db"]
