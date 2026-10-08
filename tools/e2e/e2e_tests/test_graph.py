"""图谱入库与图召回的端到端测试（P3-D、P3 遗留 R3）：Wiki 样例与干扰页面经 processing 的规则抽取
写入 idx.relations；图召回找到的块在 /v1/search 带图路径，被引用时路径写进回答的 why.graph_path。

需要先 make up PROFILE=core,ingest，再 make ingest。
图召回的权限另由权限评测集覆盖（tools/e2e/perm_eval）。
"""

import os
import re
from collections.abc import Iterator
from typing import Any

import httpx
import psycopg
import pytest

from bambuflow.common.v1 import types_pb2

from .test_agentic import answer
from .test_gateway import GATEWAY, token

SOURCE = "src_wikidemo"


@pytest.fixture(scope="module")
def db() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(os.environ["BF_PG_DSN"], autocommit=True) as conn:
        yield conn


def live_relations(db: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    return db.execute(
        """SELECT r.doc_id, r.relation_type, ef.canonical_name, ef.aliases, et.canonical_name,
                  et.aliases, c.text, r.extractor_version, d.title
           FROM idx.aliases al
           JOIN idx.version_docs vd ON vd.version = al.version
           JOIN idx.relations r ON r.doc_id = vd.doc_id AND r.doc_version = vd.doc_version
           JOIN idx.entities ef ON ef.entity_id = r.from_entity
           JOIN idx.entities et ON et.entity_id = r.to_entity
           LEFT JOIN idx.chunks c ON c.chunk_id = r.evidence_chunk_id
               AND c.doc_id = r.doc_id AND c.doc_version = r.doc_version
           JOIN idx.documents d ON d.doc_id = r.doc_id
           WHERE al.alias = 'prod' AND d.source_id = %s""",
        (SOURCE,),
    ).fetchall()


def test_relations_are_extracted_with_evidence(db: psycopg.Connection[Any]) -> None:
    rels = live_relations(db)
    assert rels, "线上版本里没有任何关系"
    for doc_id, _, _, _, obj, obj_aliases, text, extractor, title in rels:
        # 证据块必须是同一文档同一内容版本里的块：图召回靠它定位原文
        assert text is not None, f"{doc_id} 的关系证据块不在该文档版本里"
        assert extractor == "rules-1"
        # 规则抽取的客体一定出现在证据原文里（《》与空白不算），或者是“本办法”指代的文档本身
        plain = re.sub(r"[《》\s]", "", text)
        names = [obj, *obj_aliases]
        assert any(n in plain or n in title for n in names), f"{doc_id}: 证据里找不到 {obj}"


def test_expected_edges_from_sample_corpus(db: psycopg.Connection[Any]) -> None:
    edges = {(r[0], r[1], r[2], r[4]) for r in live_relations(db)}
    # 制度页面的适用范围（credit-personal-loan：“本办法适用于……人民币贷款”）
    assert (
        "credit-personal-loan",
        types_pb2.RELATION_TYPE_APPLIES_TO,
        "个人消费贷款管理办法",
        "人民币贷款",
    ) in edges
    # 干扰页面里的分行补充通知引用总行制度：同名的《》与制度页面标题是同一个实体
    cites = {
        e for e in edges if e[1] == types_pb2.RELATION_TYPE_CITES and e[3] == "个人消费贷款管理办法"
    }
    assert cites, "分行通知应引用总行《个人消费贷款管理办法》"


ENTITY_TYPES = {"org", "product", "policy_clause", "system", "role"}
RELATION_TYPES = {"depends_on", "belongs_to", "responsible_for", "cites", "applies_to"}
LOAN_POLICY = "个人消费贷款管理办法"


def search(user: str, question: str) -> list[dict[str, Any]]:
    r = httpx.post(
        GATEWAY + "/v1/search",
        json={"question": question, "top_k": 50},
        headers={"Authorization": "Bearer " + token(user), "Bambuflow-Tenant": "credit"},
        timeout=10,
    )
    assert r.status_code == 200, r.text
    hits: list[dict[str, Any]] = r.json()["hits"]
    return hits


def check_edges(edges: list[dict[str, Any]]) -> None:
    assert 1 <= len(edges) <= 2, edges
    for e in edges:
        assert e["from_type"] in ENTITY_TYPES and e["to_type"] in ENTITY_TYPES, e
        assert e["relation_type"] in RELATION_TYPES and e["from_entity"] and e["to_entity"], e
    # 两跳时前一条边的客体或主体与后一条边相连
    if len(edges) == 2:
        assert {edges[0]["from_entity"], edges[0]["to_entity"]} & {
            edges[1]["from_entity"],
            edges[1]["to_entity"],
        }, edges


def test_search_hits_found_by_graph_carry_path() -> None:
    hits = search("alice", LOAN_POLICY + "适用于什么范围？")
    graph_hits = [h for h in hits if "graph_path" in h]
    assert graph_hits, "问题里链接到了实体，应有图召回找到的块"
    for h in graph_hits:
        check_edges(h["graph_path"])
    # 遍历从问题里链接到的实体出发
    assert any(
        LOAN_POLICY in (e["from_entity"], e["to_entity"])
        for h in graph_hits
        for e in h["graph_path"]
    )


def test_relation_answer_records_graph_path_in_why() -> None:
    question = LOAN_POLICY + "归哪个部门管理？"
    a = answer("alice", question)
    why = a["why"]
    assert why["intent"] == "relation"
    graph_chunks = {h["chunk_id"] for h in search("alice", question) if "graph_path" in h}
    cited = {c["ref_id"]: c["chunk_id"] for c in a["citations"]}
    paths = why.get("graph_path", [])
    # 只有被引用、且由图召回找到的块才有路径；路径挂在对应的引用上
    for p in paths:
        assert cited.get(p["ref_id"]) == p["chunk_id"], (p, cited)
        check_edges(p["edges"])
    # 被引用的块凡是图召回能找到的都带路径（仅检索取 50 条，可能比问答的候选少，所以只比一个方向）
    assert set(cited.values()) & graph_chunks <= {p["chunk_id"] for p in paths}
    if paths:
        assert "graph" in why["retrievers"]
