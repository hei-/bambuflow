"""sql_query（P3-Q）：目录、参数校验、SQL 拼装、证据格式，以及 agent 怎样用它。"""

import asyncio
import datetime as dt
import json
from collections.abc import AsyncGenerator, Sequence
from pathlib import Path
from typing import Any

import pytest

from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_grounding import Grounder
from bambuflow_orchestrator import agent, flow, prompt, structured
from bambuflow_orchestrator.budget import Deadline

CATALOG = """
tenants: [ops]
roles:
  report_ops: [group:sre-oncall]
  report_finance: [group:sre-core]
views:
  - name: bankdemo.v_loan_balance
    title: 贷款余额
    description: 每笔贷款的当前余额。
    classification: internal
    terms: [贷款]
    columns:
      - {name: loan_id, label: 贷款编号, type: number}
      - {name: branch, label: 经办网点, type: text}
      - {name: balance, label: 贷款余额（元）, type: number}
      - {name: opened_on, label: 放款日期, type: date}
"""


@pytest.fixture
def catalog(tmp_path: Path) -> structured.Catalog:
    p = tmp_path / "catalog.yaml"
    p.write_text(CATALOG, encoding="utf-8")
    return structured.Catalog.load(str(p))


def test_catalog_scope(catalog: structured.Catalog) -> None:
    assert catalog.enabled("ops") and not catalog.enabled("credit")
    assert catalog.matches("演示网点甲的贷款余额合计是多少？")
    # 没有统计类词语，或没提到任何视图：不按结构化问题处理
    assert not catalog.matches("贷款余额视图是做什么的？")
    assert not catalog.matches("反洗钱培训要多少学时？")
    assert catalog.roles_for(["user:sam", "group:sre-oncall"]) == ["report_ops"]
    assert catalog.roles_for(["group:all-staff"]) == []
    assert "bankdemo.v_loan_balance：贷款余额" in catalog.describe()


def test_parse_rejects_anything_outside_the_catalog(catalog: structured.Catalog) -> None:
    ok = {"view": "bankdemo.v_loan_balance", "measures": [{"agg": "sum", "column": "balance"}]}
    assert structured.parse(ok, catalog).measures[0].alias == "sum_balance"
    bad: list[dict[str, Any]] = [
        {"view": "bankdemo.v_customer_contact", "columns": ["phone"]},
        {**ok, "measures": [{"agg": "sum", "column": "phone"}]},
        {**ok, "measures": [{"agg": "sum", "column": "branch"}]},
        {**ok, "measures": [{"agg": "median", "column": "balance"}]},
        {**ok, "filters": [{"column": "branch", "op": "like", "value": "%"}]},
        {**ok, "filters": [{"column": "balance", "op": ">", "value": "1; DROP TABLE x"}]},
        {**ok, "filters": [{"column": "opened_on", "op": ">=", "value": "去年"}]},
        {**ok, "limit": 500},
        {"view": "bankdemo.v_loan_balance"},
        {**ok, "columns": ["branch"]},
    ]
    for args in bad:
        with pytest.raises(structured.QueryError):
            structured.parse(args, catalog)


def test_build_binds_values_and_quotes_identifiers(catalog: structured.Catalog) -> None:
    q = structured.parse(
        {
            "view": "bankdemo.v_loan_balance",
            "measures": [{"agg": "count"}, {"agg": "sum", "column": "balance"}],
            "group_by": ["branch"],
            "filters": [
                {"column": "opened_on", "op": "between", "value": ["2025-01-01", "2025-12-31"]},
                {"column": "branch", "op": "in", "value": ["演示网点甲", "演示网点乙"]},
            ],
            "limit": 10,
        },
        catalog,
    )
    stmt, params = structured.build(q)
    text = stmt.as_string(None)
    assert text == (
        'SELECT "branch", count(*) AS "count", sum("balance") AS "sum_balance" '
        'FROM "bankdemo"."v_loan_balance" WHERE "opened_on" BETWEEN %s AND %s '
        'AND "branch" = ANY(%s) GROUP BY "branch" ORDER BY "count" DESC LIMIT %s'
    )
    assert params == [dt.date(2025, 1, 1), dt.date(2025, 12, 31), ["演示网点甲", "演示网点乙"], 10]


def test_evidence_states_result_first_and_cites_view(catalog: structured.Catalog) -> None:
    q = structured.parse(
        {
            "view": "bankdemo.v_loan_balance",
            "measures": [{"agg": "sum", "column": "balance"}],
            "filters": [{"column": "branch", "op": "=", "value": "演示网点甲"}],
        },
        catalog,
    )
    at = dt.datetime(2026, 10, 7, 8, 0, tzinfo=dt.UTC)
    h = structured.evidence(q, structured.Result(("sum_balance",), ((765432.1,),), at))
    first = h.text.split("\n")[0]
    assert first == "按经办网点 = 演示网点甲查询贷款余额，贷款余额（元）合计为 765432.1。"
    assert "bankdemo.v_loan_balance" in h.text and "2026-10-07 08:00:00 UTC" in h.text
    assert h.source_type == types_pb2.SOURCE_TYPE_DB and h.chunk_id.startswith("db:bankdemo.")
    assert h.score == 1.0 and h.classification == types_pb2.CLASSIFICATION_INTERNAL
    empty = structured.evidence(q, structured.Result(("sum_balance",), (), at))
    assert "没有符合条件的记录" in empty.text


class Clock:
    t = 0.0

    def __call__(self) -> float:
        return self.t


class FakeGeneration:
    model = "chat@t"

    def __init__(self, output: str) -> None:
        self.output = output

    async def __aiter__(self) -> AsyncGenerator[str, None]:
        for raw in self.output.split("\n"):
            yield raw


class PlanOnly:
    def __init__(self, output: str) -> None:
        self.output = output
        self.prompts: list[tuple[str, str]] = []

    def generate(self, system: str, user: str, timeout: float | None = None) -> FakeGeneration:
        self.prompts.append((system, user))
        return FakeGeneration(self.output)


class Search:
    async def search(
        self, query: str, filters: types_pb2.Filters | None, round_no: int, timeout: float
    ) -> retrieval_pb2.SearchResponse:
        return retrieval_pb2.SearchResponse(
            hits=[retrieval_pb2.Hit(chunk_id="wiki:x#1", text="制度正文", score=0.9)],
            index_version="ix-1",
            rerank_model="r@t",
        )


class Sink:
    async def save(self, state: dict[str, Any]) -> None:
        pass

    async def drop(self) -> None:
        pass


class FakeSql:
    def __init__(self, catalog: structured.Catalog, fail: Exception | None = None) -> None:
        self.catalog, self.fail = catalog, fail
        self.args: list[dict[str, Any]] = []

    async def query(self, args: dict[str, Any], timeout: float) -> retrieval_pb2.Hit:
        self.args.append(args)
        if self.fail is not None:
            raise self.fail
        q = structured.parse(args, self.catalog)
        r = structured.Result(("count",), ((2,),), dt.datetime(2026, 10, 7, tzinfo=dt.UTC))
        return structured.evidence(q, r)


def gather(llm: PlanOnly, sql: FakeSql | None) -> agent.Gathered:
    clock = Clock()
    a = agent.Agent(
        "贷款有多少笔？顺便说下制度",
        None,
        (),
        Search(),
        llm,
        Deadline(10, clock),
        Sink(),
        clock,
        sql=sql,
    )
    return asyncio.run(a.run())


def plan_lines(lines: Sequence[dict[str, Any]]) -> str:
    return "\n".join(json.dumps(x, ensure_ascii=False) for x in lines)


SQL_LINE = {
    "id": "q1",
    "text": "贷款有多少笔",
    "tool": "sql_query",
    "args": {"view": "bankdemo.v_loan_balance", "measures": [{"agg": "count"}]},
}


def test_agent_runs_sql_sub_questions_with_catalog_in_plan(catalog: structured.Catalog) -> None:
    llm = PlanOnly(plan_lines([SQL_LINE, {"id": "q2", "text": "贷款制度"}]))
    sql = FakeSql(catalog)
    g = gather(llm, sql)
    system, user = llm.prompts[0]
    assert system == prompt.PLAN_SQL_SYSTEM and "<views>" in user
    assert sql.args == [SQL_LINE["args"]]
    calls = {c.tool: c for c in g.rounds[0].calls}
    assert set(calls) == {"sql_query", "doc_search"} and calls["sql_query"].covered
    assert any(h.chunk_id.startswith("db:") for h in g.found.hits)
    # 版本号取文档检索的，不被 sql 结果清空
    assert g.found.index_version == "ix-1"


def test_sql_failures_leave_sub_question_uncovered_and_degraded(
    catalog: structured.Catalog,
) -> None:
    for err, reason in (
        (structured.QueryDenied(), "denied"),
        (structured.QueryUnavailable("x"), "unavailable"),
        (structured.QueryError("x"), "invalid"),
    ):
        g = gather(PlanOnly(plan_lines([SQL_LINE])), FakeSql(catalog, err))
        assert not g.rounds[0].calls[0].covered
        assert [(d.stage, d.reason) for d in g.found.degraded] == [("sql_query", reason)]
        # 只问了 sql 且失败：没有任何证据，GIVE_UP
        assert g.gave_up and len(g.rounds) == 1


def test_without_sql_tool_the_plan_ignores_sql_lines(catalog: structured.Catalog) -> None:
    llm = PlanOnly(plan_lines([SQL_LINE]))
    g = gather(llm, None)
    assert llm.prompts[0][0] == prompt.PLAN_SYSTEM
    assert [c.tool for c in g.rounds[0].calls] == ["doc_search"]


class NoNLI:
    async def support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        return [0.0] * len(pairs), "nli@t"


class AllowAll:
    async def allowed(self, chunk_ids: Sequence[str]) -> dict[str, bool]:
        return dict.fromkeys(chunk_ids, True)


def test_agent_answer_uses_sql_prompt_version(catalog: structured.Catalog) -> None:
    clock = Clock()
    req = flow.Request("q", None, 8)
    for sql, version in (
        (None, prompt.AGENT_PROMPT_VERSION),
        (FakeSql(catalog), prompt.AGENT_SQL_PROMPT_VERSION),
    ):
        a = agent.Agent(
            "q", None, (), Search(), PlanOnly(""), Deadline(10, clock), Sink(), clock, sql=sql
        )
        answer = agent.AgentAnswer(req, a, PlanOnly(""), Grounder(NoNLI(), AllowAll()))
        assert answer.prompt_version == version
