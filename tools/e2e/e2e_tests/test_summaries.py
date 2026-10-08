"""摘要块与主题汇总（P3-I1，入库管道详设第 7 节、K2 详设第 6 节）。

入库时每份文档生成一个摘要块（documents.summary_chunk_id），权限随所属文档、不进普通召回；
“总结一下所有制度的主要内容”这类问题基于摘要块分组归纳，要点只引用调用者可见文档的摘要。
需要先 make ingest。
"""

import os
from typing import Any

import psycopg

from .test_answers import ask, search_hits
from .test_listing import visible


def live_summaries() -> dict[str, tuple[str, str]]:
    """线上版本里每份文档的摘要块：doc_id → (chunk_id, 正文)。"""
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        rows = conn.execute(
            """SELECT c.doc_id, c.chunk_id, c.text FROM idx.aliases al
               JOIN idx.version_docs vd ON vd.version = al.version
               JOIN idx.chunks c ON c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version
               WHERE al.alias = 'prod' AND c.kind = 'summary'"""
        ).fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def test_ingested_documents_have_summary_chunks() -> None:
    sums = live_summaries()
    wiki = {d: v for d, v in sums.items() if v[0].startswith("src_wikidemo:")}
    assert len(wiki) >= 10, "Wiki 样例源的大部分文档应有摘要块"
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        rows = conn.execute(
            "SELECT doc_id, summary_chunk_id, keywords FROM idx.documents WHERE doc_id = ANY(%s)",
            (list(wiki),),
        ).fetchall()
        sections = conn.execute(
            "SELECT count(*) FROM idx.sections WHERE doc_id = ANY(%s)", (list(wiki),)
        ).fetchone()
    for doc_id, summary_id, keywords in rows:
        # 文档级字段指向最新内容版本的摘要块；没有内容更新时就是线上版本里的那一块
        assert summary_id == wiki[doc_id][0], doc_id
        assert keywords, f"{doc_id} 没有关键词"
    assert sections is not None and sections[0] > 0
    assert all(text for _, text in wiki.values())


def test_summary_chunks_are_not_in_ordinary_search() -> None:
    summary_ids = {v[0] for v in live_summaries().values()}
    hits = search_hits("alice", "credit", "个人贷款的额度和期限")
    assert hits and not set(hits) & summary_ids


def test_topic_summary_cites_only_visible_summaries() -> None:
    a = ask("alice", "credit", "总结一下所有制度的主要内容")
    assert a["why"]["intent"] == "listing"
    assert a["answer_kind"] in ("summary", "enumeration"), a["answer_kind"]
    want = visible(a["why"]["inferred_filters"])
    (scope,) = [b["scope"] for b in a["blocks"] if b["block_type"] == "scope"]
    assert scope["total_count"] == len(want) and scope["basis"] == "仅统计你有权查看的文档"
    if a["answer_kind"] == "enumeration":
        # 归纳失败时退回文档清单（K2 详设第 9 节），模拟 LLM 下不应发生
        raise AssertionError(f"主题汇总退回了文档清单: {a['why']['degraded']}")
    assert a["status"] == "answered" and a["why"]["based_on_summaries"] is True
    summaries = {v[0]: d for d, v in live_summaries().items()}
    assert a["citations"]
    for c in a["citations"]:
        assert c["chunk_id"] in summaries, f"要点引用的不是摘要块: {c['chunk_id']}"
        assert c["doc_id"] in want, f"引用了不可见或不符合条件的文档 {c['doc_id']}"
    kinds = [b["block_type"] for b in a["blocks"]]
    assert kinds[0] == "heading" and "list_item" in kinds and kinds[-1] == "scope"
    items: list[dict[str, Any]] = [
        s for s in a["sentences"] if s["claim_type"] == "quote" and s["ref_ids"]
    ]
    assert items and all(s["support"] >= 0.5 for s in items)
    assert scope["shown"] <= scope["total_count"]


def test_ops_user_summary_never_cites_invisible_documents() -> None:
    # 风险租户处于试点且没有试点群组，问答对所有成员关闭，所以用运维租户的 sam
    a = ask("sam", "ops", "总结一下所有制度的主要内容")
    assert a["citations"]
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        invisible = {
            r[0]
            for r in conn.execute(
                "SELECT doc_id FROM idx.doc_acl WHERE NOT acl_tokens && %s",
                (["group:sre-oncall", "group:all-staff", "user:sam", "dept:运维部"],),
            ).fetchall()
        }
    assert not {c["doc_id"] for c in a["citations"]} & invisible
