"""关系型数据库表结构（P3-I4，技术设计文档 5.2、5.5）。

dbschema-seed 在开发 Postgres 里建演示库（bankdemo schema）与只读账号，
connectors-dbschema 以只读账号读系统表，只同步白名单视图的表结构与字段说明，行数据不入索引；
权限来自视图授权（tools/test-upstream/dbschema.yaml）。
make ingest 连跑两次：表结构不变时不产生新的内容版本。需要先 make ingest。
"""

import os
from typing import Any
from urllib.parse import urlparse, urlunparse

import psycopg

from .test_answers import ask, search_hits

SOURCE = "src_dbschema"
LOAN = "bankdemo-bankdemo-v-loan-balance"
BRANCH = "bankdemo-bankdemo-v-branch-daily"


def docs() -> dict[str, dict[str, Any]]:
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        rows = conn.execute(
            """SELECT d.doc_id, d.doc_version, d.source_type, d.title, a.acl_tokens
               FROM idx.documents d JOIN idx.doc_acl a USING (doc_id)
               WHERE d.source_id = %s AND a.state = 'active'""",
            (SOURCE,),
        ).fetchall()
        texts = conn.execute(
            """SELECT c.doc_id, string_agg(c.text, '\n') FROM idx.aliases al
               JOIN idx.version_docs vd ON vd.version = al.version
               JOIN idx.chunks c ON c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version
               WHERE al.alias = 'prod' AND c.kind = 'content' AND vd.doc_id = ANY(%s)
               GROUP BY c.doc_id""",
            ([r[0] for r in rows],),
        ).fetchall()
    text: dict[str, str] = {r[0]: r[1] for r in texts}
    return {
        r[0]: {
            "version": r[1],
            "source_type": r[2],
            "title": r[3],
            "acl": r[4],
            "text": text.get(r[0], ""),
        }
        for r in rows
    }


def test_only_whitelisted_views_are_synced() -> None:
    d = docs()
    assert set(d) == {LOAN, BRANCH}, sorted(d)
    for info in d.values():
        assert info["source_type"] == 7
        # make ingest 跑了两次：表结构没变，不产生新的内容版本
        assert info["version"] == 1


def test_structure_and_comments_but_no_rows() -> None:
    loan = docs()[LOAN]
    for want in (
        "loan_id（bigint）：贷款编号",
        "balance（numeric(18,2)）：贷款余额（元）",
        "贷款余额视图",
    ):
        assert want in loan["text"], want
    # 行数据不入索引：演示表里的值一个都不出现
    for row_value in ("765432.10", "演示网点甲", "900001", "演示客户甲"):
        assert row_value not in loan["text"]
    assert loan["title"].startswith("视图 bankdemo.v_loan_balance（bankdemo 库，同步于 ")


def test_acl_comes_from_view_grants() -> None:
    d = docs()
    assert d[LOAN]["acl"] == ["group:sre-oncall"]
    assert d[BRANCH]["acl"] == ["group:sre-core", "group:sre-oncall"]


def test_reader_account_cannot_read_rows() -> None:
    u = urlparse(os.environ["BF_PG_DSN"])
    reader = u._replace(
        netloc=f"dbschema_reader:{os.environ['BF_DBSCHEMA_PASSWORD']}@{u.hostname}:{u.port}"
    )
    with psycopg.connect(urlunparse(reader)) as conn:
        try:
            conn.execute("SELECT * FROM bankdemo.v_loan_balance").fetchall()
        except psycopg.errors.InsufficientPrivilege:
            return
    raise AssertionError("只读账号只能读系统目录，不应能查视图里的行")


def test_answer_cites_the_view_with_database_and_sync_time() -> None:
    hits = search_hits("sam", "ops", "贷款余额视图有哪些字段")
    assert any(h["doc_id"] == LOAN and h["source_type"] == "db" for h in hits.values())
    a = ask("sam", "ops", "贷款余额视图有哪些字段？")
    cited = [c for c in a["citations"] if c["doc_id"] == LOAN]
    assert cited, a["citations"]
    assert cited[0]["source_type"] == "db" and "bankdemo" in cited[0]["title"]
    assert "同步于" in cited[0]["title"] and cited[0]["source_uri"].endswith(
        "bankdemo.v_loan_balance"
    )
