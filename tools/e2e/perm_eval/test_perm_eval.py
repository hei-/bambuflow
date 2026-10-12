"""权限评测集 P0 版：越权必须为 0，权限相关故障必须拒绝。

由 make eval-perm 运行（先写入合成文档、再 make ingest 发布）。预期结果不调用任何被测代码：
直接读权限表（idx.doc_acl）、紧急拒绝名单（idx.deny_list、idx.deny_scopes）、
governance 的租户授权（governance.source_grants，只算已审批、未撤销的数据源）
和租户策略（tools/test-upstream/policies 的租户层里的 deny_sources），
按“数据源授权给租户且未被策略禁止、未打删除标记、未被紧急拒绝、密级不超、token 有交集”暴力计算，
与 retrieval 的 Search、CheckAccess、CheckDocumentAccess、GetExcerpt、ListDocuments、
CountDocuments 逐条比对。
"""

import os
import re
import time
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import grpc
import psycopg
import pytest

from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2, retrieval_pb2_grpc

from .principals import TEMPLATES, Signer, Template

POLICIES = Path(__file__).resolve().parents[2] / "test-upstream" / "policies"
# 最后一题能链接到图谱实体（seed 里的“清算核心服务”），三路召回都参与
GRAPH_QUESTION = "清算核心服务依赖哪些系统？"
QUESTIONS = ("授信额度怎么计算？", "风险限额由谁审批？", "员工请假需要哪些手续？", GRAPH_QUESTION)
TOP_K = 50
PER_DOC_CAP = 3  # retrieval 详设 7.3：同一文档最多保留 3 块


def denied_sources(tenant: str) -> set[str]:
    """租户层种子（tools/test-upstream/policies/tenant.<租户>.rego）里禁止的数据源。
    预期结果不经被测代码计算，所以直接读种子：
    测试数据只用 deny_sources := {"…"} 或 set() 这一种写法。"""
    path = POLICIES / f"tenant.{tenant}.rego"
    if not path.exists():
        return set()
    m = re.search(r"^deny_sources := (?:set\(\)|\{(.*)\})$", path.read_text(), re.M)
    return set(re.findall(r'"([^"]+)"', m.group(1) or "")) if m else set()


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    doc_id: str
    source: str
    tokens: frozenset[str]
    classification: int
    active: bool


@pytest.fixture(scope="module")
def db() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(os.environ["BF_PG_DSN"], autocommit=True) as conn:
        yield conn


@pytest.fixture(scope="module")
def live_chunks(db: psycopg.Connection[Any]) -> list[Chunk]:
    rows = db.execute(
        """SELECT c.chunk_id, vd.doc_id, a.source_id, a.acl_tokens, a.classification,
                  a.state = 'active'
           FROM idx.aliases al
           JOIN idx.version_docs vd ON vd.version = al.version
           JOIN idx.chunks c ON c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version
           JOIN idx.doc_acl a ON a.doc_id = vd.doc_id
           WHERE al.alias = 'prod'"""
    ).fetchall()
    chunks = [Chunk(r[0], r[1], r[2], frozenset(r[3]), r[4], r[5]) for r in rows]
    assert any(c.source == "src_evalcredit" for c in chunks), (
        "线上版本里没有合成文档，先运行 make eval-perm 的 seed 步骤"
    )
    return chunks


def groups_of(db: psycopg.Connection[Any], t: Template) -> tuple[str, ...]:
    if not t.groups_ref:
        return t.groups
    rows = db.execute(
        "SELECT grp FROM identity.identity_groups WHERE username = %s", (t.groups_ref,)
    ).fetchall()
    return tuple(r[0] for r in rows)


def granted_sources(db: psycopg.Connection[Any], tenant: str) -> set[str]:
    """租户可检索的数据源：governance 的授权（已审批、未撤销）减去策略禁止与按数据源的紧急拒绝。

    租户被紧急拒绝时为空。
    """
    rows = db.execute(
        """SELECT g.source_id FROM governance.source_grants g
           JOIN governance.sources s USING (source_id)
           WHERE g.tenant = %s AND s.status = 'active'
             AND NOT EXISTS (SELECT 1 FROM idx.deny_scopes d
                             WHERE d.kind = 'source' AND d.value = g.source_id)
             AND NOT EXISTS (SELECT 1 FROM idx.deny_scopes d
                             WHERE d.kind = 'tenant' AND d.value = g.tenant)""",
        (tenant,),
    ).fetchall()
    return {r[0] for r in rows} - denied_sources(tenant)


def denied_docs(db: psycopg.Connection[Any]) -> set[str]:
    return {r[0] for r in db.execute("SELECT doc_id FROM idx.deny_list").fetchall()}


def expected(db: psycopg.Connection[Any], chunks: list[Chunk], t: Template) -> set[str]:
    sources = granted_sources(db, t.tenant)
    denied = denied_docs(db)
    tokens = {f"user:{t.sub}", f"dept:{t.dept}"} | {f"group:{g}" for g in groups_of(db, t)}
    return {
        c.chunk_id
        for c in chunks
        if c.active
        and c.source in sources
        and c.doc_id not in denied
        and c.classification <= t.clearance
        and c.tokens & tokens
    }


@pytest.fixture(scope="module")
def signer() -> Signer:
    return Signer()


def stub(addr: str) -> Iterator[retrieval_pb2_grpc.RetrievalServiceStub]:
    with grpc.insecure_channel(addr) as ch:
        yield retrieval_pb2_grpc.RetrievalServiceStub(ch)


@pytest.fixture(scope="module")
def retrieval() -> Iterator[retrieval_pb2_grpc.RetrievalServiceStub]:
    yield from stub(os.environ["BF_RETRIEVAL_ADDR"])


def md(token: str, purpose: str = "search") -> tuple[tuple[str, str | bytes], ...]:
    return (("authorization", "Bearer " + token), ("x-call-purpose", purpose))


def search(
    s: retrieval_pb2_grpc.RetrievalServiceStub, token: str, question: str
) -> retrieval_pb2.SearchResponse:
    req = retrieval_pb2.SearchRequest(
        queries=[retrieval_pb2.SubQuery(id="q1", text=question)], top_k=TOP_K
    )
    resp: retrieval_pb2.SearchResponse = s.Search(req, metadata=md(token), timeout=10)
    return resp


ALLOWED = [t for t in TEMPLATES if not t.denied]
DENIED = [t for t in TEMPLATES if t.denied]


@pytest.mark.parametrize("t", ALLOWED, ids=lambda t: t.name)
def test_search_matches_brute_force(
    t: Template,
    db: psycopg.Connection[Any],
    live_chunks: list[Chunk],
    signer: Signer,
    retrieval: retrieval_pb2_grpc.RetrievalServiceStub,
) -> None:
    want = expected(db, live_chunks, t)
    for q in QUESTIONS:
        resp = search(retrieval, signer.sign(t), q)
        got = {h.chunk_id for h in resp.hits}
        over = got - want
        assert not over, f"越权 {len(over)} 块: {sorted(over)}"
        # 检索结果经过近重复去重、同文档限量与 MMR（retrieval 详设 7.3），条数会少于 top_k，
        # 不能再按条数判断漏召回；“过滤比权限更严”由下面 CheckAccess 的逐块全量比对负责。
        assert got or not want, "有可见块却没有任何结果"
        per_doc = Counter(h.doc_id for h in resp.hits)
        assert max(per_doc.values(), default=0) <= PER_DOC_CAP, f"同一文档超过 {PER_DOC_CAP} 块"
        assert resp.index_version and resp.policy_version
    print(f"{t.name}: 可见 {len(want)} 块，{len(QUESTIONS)} 题均无越权")


@pytest.mark.parametrize("t", ALLOWED, ids=lambda t: t.name)
def test_check_access_matches_brute_force(
    t: Template,
    db: psycopg.Connection[Any],
    live_chunks: list[Chunk],
    signer: Signer,
    retrieval: retrieval_pb2_grpc.RetrievalServiceStub,
) -> None:
    want = expected(db, live_chunks, t)
    ids = sorted(c.chunk_id for c in live_chunks) + ["src_evalcredit:no-such-doc#0"]
    allowed = set()
    for i in range(0, len(ids), 50):
        req = retrieval_pb2.CheckAccessRequest(chunk_ids=ids[i : i + 50])
        resp = retrieval.CheckAccess(req, metadata=md(signer.sign(t)), timeout=10)
        allowed |= {r.chunk_id for r in resp.results if r.allowed}
        missing = [r for r in resp.results if r.chunk_id.endswith("no-such-doc#0")]
        assert all(r.reason == retrieval_pb2.ACCESS_REASON_NOT_FOUND for r in missing)
    assert allowed == want, f"越权 {sorted(allowed - want)}，误拒 {sorted(want - allowed)}"


@pytest.mark.parametrize("t", ALLOWED, ids=lambda t: t.name)
def test_check_document_access_matches_brute_force(
    t: Template,
    db: psycopg.Connection[Any],
    live_chunks: list[Chunk],
    signer: Signer,
    retrieval: retrieval_pb2_grpc.RetrievalServiceStub,
) -> None:
    """按文档复核（批量取结果时复核清单类回答的列表项）：与按块复核同一套判定，权限在文档级。"""
    visible = expected(db, live_chunks, t)
    want = {c.doc_id for c in live_chunks if c.chunk_id in visible}
    ids = sorted({c.doc_id for c in live_chunks}) + ["no-such-doc"]
    allowed = set()
    for i in range(0, len(ids), 50):
        req = retrieval_pb2.CheckDocumentAccessRequest(doc_ids=ids[i : i + 50])
        resp = retrieval.CheckDocumentAccess(req, metadata=md(signer.sign(t), "batch"), timeout=10)
        assert [r.doc_id for r in resp.results] == ids[i : i + 50]
        allowed |= {r.doc_id for r in resp.results if r.allowed}
        missing = [r for r in resp.results if r.doc_id == "no-such-doc"]
        assert all(r.reason == retrieval_pb2.ACCESS_REASON_NOT_FOUND for r in missing)
    assert allowed == want, f"越权 {sorted(allowed - want)}，误拒 {sorted(want - allowed)}"


def excerpt(
    s: retrieval_pb2_grpc.RetrievalServiceStub, token: str, doc_id: str, chunk_id: str
) -> retrieval_pb2.GetExcerptResponse | grpc.RpcError:
    """GetExcerpt 经入口准入（租户令牌桶）；逐块全量比对时可能被限流，按退避重试，最多 5 次。"""
    req = retrieval_pb2.GetExcerptRequest(doc_id=doc_id, chunk_id=chunk_id)
    for attempt in range(5):
        try:
            resp: retrieval_pb2.GetExcerptResponse = s.GetExcerpt(
                req, metadata=md(token, "mcp"), timeout=10
            )
            return resp
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.RESOURCE_EXHAUSTED or attempt == 4:
                return e
            time.sleep(0.2 * 2**attempt)
    raise AssertionError("不可达")


@pytest.mark.parametrize("t", ALLOWED, ids=lambda t: t.name)
def test_get_excerpt_matches_brute_force(
    t: Template,
    db: psycopg.Connection[Any],
    live_chunks: list[Chunk],
    signer: Signer,
    retrieval: retrieval_pb2_grpc.RetrievalServiceStub,
) -> None:
    """MCP get_document_excerpt：可见的块返回原文；无权、不存在、块不属于所给文档一律 NOT_FOUND，
    且错误内容完全相同，调用方无法据此探测文档是否存在（gateway 详设第 7 节）。
    权限在文档级，每份文档取一块比对，避免全量逐块调用压满租户令牌桶。"""
    want = expected(db, live_chunks, t)
    token = signer.sign(t)
    one = {c.doc_id: c for c in sorted(live_chunks, key=lambda c: c.chunk_id)}
    got: set[str] = set()
    errors: set[tuple[grpc.StatusCode, str]] = set()
    for c in one.values():
        r = excerpt(retrieval, token, c.doc_id, c.chunk_id)
        if isinstance(r, grpc.RpcError):
            assert r.code() == grpc.StatusCode.NOT_FOUND, f"{c.chunk_id}: {r.code()}"
            errors.add((r.code(), r.details() or ""))
            continue
        assert r.chunk_id == c.chunk_id and r.doc_id == c.doc_id and r.text
        assert r.index_version and r.policy_version
        got.add(c.chunk_id)
    want_one = {c.chunk_id for c in one.values() if c.chunk_id in want}
    assert got == want_one, f"越权 {sorted(got - want_one)}，误拒 {sorted(want_one - got)}"
    # 不存在的文档、块不属于所给文档：与无权的错误完全一样
    docs = sorted(one)
    probes = [("no-such-doc", "src_evalcredit:no-such-doc#0")]
    if len(docs) > 1:
        probes.append((docs[0], one[docs[1]].chunk_id))
    for doc_id, chunk_id in probes:
        r = excerpt(retrieval, token, doc_id, chunk_id)
        assert isinstance(r, grpc.RpcError) and r.code() == grpc.StatusCode.NOT_FOUND
        errors.add((r.code(), r.details() or ""))
    assert len(errors) == 1, f"无权与不存在的错误不一致: {errors}"


@pytest.mark.parametrize("t", ALLOWED, ids=lambda t: t.name)
def test_count_and_list_match_brute_force(
    t: Template,
    db: psycopg.Connection[Any],
    live_chunks: list[Chunk],
    signer: Signer,
    retrieval: retrieval_pb2_grpc.RetrievalServiceStub,
) -> None:
    """枚举与计数（K2）：无权文档既不出现在列表里，也不影响数量。"""
    visible = expected(db, live_chunks, t)
    want = {c.doc_id for c in live_chunks if c.chunk_id in visible}
    m = md(signer.sign(t))
    count = retrieval.CountDocuments(retrieval_pb2.CountDocumentsRequest(), metadata=m, timeout=10)
    assert count.total_is_exact and count.total_count == len(want), (
        f"计数 {count.total_count}，可见文档 {len(want)}"
    )
    listed: set[str] = set()
    token = ""
    while True:
        req = retrieval_pb2.ListDocumentsRequest(page_size=50, page_token=token)
        resp = retrieval.ListDocuments(req, metadata=m, timeout=10)
        assert resp.total_count == len(want)
        listed |= {d.doc_id for d in resp.documents}
        if not (token := resp.next_page_token):
            break
    assert listed == want, f"越权 {sorted(listed - want)}，漏列 {sorted(want - listed)}"


@pytest.mark.parametrize("t", ALLOWED, ids=lambda t: t.name)
def test_count_by_doc_type_matches_brute_force(
    t: Template,
    db: psycopg.Connection[Any],
    live_chunks: list[Chunk],
    signer: Signer,
    retrieval: retrieval_pb2_grpc.RetrievalServiceStub,
) -> None:
    """按文档类型计数、分组与缺口（K2）：只统计可见文档，缺口里也不含无权文档。"""
    visible = expected(db, live_chunks, t)
    docs = sorted({c.doc_id for c in live_chunks if c.chunk_id in visible})
    rows = db.execute(
        "SELECT doc_id, doc_type FROM idx.documents WHERE doc_id = ANY(%s)", (docs,)
    ).fetchall()
    doc_type = dict(rows)
    by_type = Counter(doc_type.values())
    m = md(signer.sign(t))
    for dt in sorted(k for k in by_type if k) + ["DT-NO-SUCH-TYPE"]:
        req = retrieval_pb2.CountDocumentsRequest(
            filters=types_pb2.Filters(doc_types=[dt]),
            group_by=retrieval_pb2.COUNT_GROUP_BY_DOC_TYPE,
        )
        resp = retrieval.CountDocuments(req, metadata=m, timeout=10)
        assert resp.total_count == by_type.get(dt, 0), f"{dt}: 计数 {resp.total_count}"
        gaps = {g.field: g.count for g in resp.metadata_gaps}
        assert gaps.get("doc_type", 0) == by_type.get("", 0), f"{dt}: 缺口 {gaps}"
    resp = retrieval.CountDocuments(
        retrieval_pb2.CountDocumentsRequest(group_by=retrieval_pb2.COUNT_GROUP_BY_DOC_TYPE),
        metadata=m,
        timeout=10,
    )
    assert {g.value: g.count for g in resp.groups} == dict(by_type)


def visible_edges(db: psycopg.Connection[Any], visible: set[str]) -> set[tuple[str, int, str]]:
    """线上版本里证据块对该身份可见的边（实体名称，关系类型，实体名称）。"""
    rows = db.execute(
        """SELECT ef.canonical_name, r.relation_type, et.canonical_name, r.evidence_chunk_id
           FROM idx.aliases al
           JOIN idx.version_docs vd ON vd.version = al.version
           JOIN idx.relations r ON r.doc_id = vd.doc_id AND r.doc_version = vd.doc_version
           JOIN idx.entities ef ON ef.entity_id = r.from_entity
           JOIN idx.entities et ON et.entity_id = r.to_entity
           WHERE al.alias = 'prod'"""
    ).fetchall()
    return {(r[0], r[1], r[2]) for r in rows if r[3] in visible}


@pytest.mark.parametrize("t", ALLOWED, ids=lambda t: t.name)
def test_graph_paths_only_use_visible_evidence(
    t: Template,
    db: psycopg.Connection[Any],
    live_chunks: list[Chunk],
    signer: Signer,
    retrieval: retrieval_pb2_grpc.RetrievalServiceStub,
) -> None:
    """图上的权限（retrieval 详设 13 节用例 4）：关系两端实体可见、证据块不可见时，
    不返回该关系的任何信息。路径上的每条边都必须有调用者可见的证据块；不可见文档里的下游系统名称不能出现在任何路径里。"""
    want = expected(db, live_chunks, t)
    edges = visible_edges(db, want)
    resp = search(retrieval, signer.sign(t), GRAPH_QUESTION)
    pathed = [h for h in resp.hits if h.graph_path]
    for h in pathed:
        assert h.chunk_id in want, f"越权块 {h.chunk_id}"
        for e in h.graph_path:
            edge = (e.from_entity, e.relation_type, e.to_entity)
            assert edge in edges, f"{h.chunk_id} 的路径用到了证据不可见的边 {edge}"
    hub = [e for e in edges if e[0] == "清算核心服务"]
    # 有可见的起点边时图召回必须有结果，否则上面的断言是空转
    assert bool(pathed) == bool(hub), f"可见起点边 {len(hub)} 条，带路径的结果 {len(pathed)} 条"
    print(f"{t.name}: 可见边 {len(edges)} 条，带图路径的结果 {len(pathed)} 条，均无越权")


@pytest.mark.parametrize("t", DENIED, ids=lambda t: t.name)
def test_incomplete_or_foreign_principals_are_denied(
    t: Template, signer: Signer, retrieval: retrieval_pb2_grpc.RetrievalServiceStub
) -> None:
    token = signer.sign(t)
    with pytest.raises(grpc.RpcError) as err:
        search(retrieval, token, QUESTIONS[0])
    assert err.value.code() == grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(grpc.RpcError) as err:
        retrieval.CheckDocumentAccess(
            retrieval_pb2.CheckDocumentAccessRequest(doc_ids=["eval-00"]),
            metadata=md(token, "batch"),
            timeout=10,
        )
    assert err.value.code() == grpc.StatusCode.PERMISSION_DENIED
    r = excerpt(retrieval, token, "eval-00", "src_evalcredit:eval-00#x")
    assert isinstance(r, grpc.RpcError) and r.code() == grpc.StatusCode.PERMISSION_DENIED


def test_forged_or_missing_principal_is_rejected(
    signer: Signer, retrieval: retrieval_pb2_grpc.RetrievalServiceStub
) -> None:
    token = signer.sign(ALLOWED[0])
    head, payload, sig = token.split(".")
    forged = f"{head}.{payload}.{sig[:-4]}AAAA"
    for bad in (forged, ""):
        with pytest.raises(grpc.RpcError) as err:
            search(retrieval, bad, QUESTIONS[0])
        assert err.value.code() == grpc.StatusCode.UNAUTHENTICATED


# 故障注入：三个实例与 retrieval 同一镜像，分别没有策略包、连不上元数据库、拿不到租户授权
# （deploy/compose/compose.yaml）。
@pytest.mark.parametrize(
    "addr", ["retrieval-nopolicy:9090", "retrieval-nodb:9090", "retrieval-nogrants:9090"]
)
def test_faults_fail_closed(addr: str, signer: Signer) -> None:
    for s in stub(addr):
        token = signer.sign(ALLOWED[0])
        with pytest.raises(grpc.RpcError) as err:
            search(s, token, QUESTIONS[0])
        assert err.value.code() == grpc.StatusCode.UNAVAILABLE
        with pytest.raises(grpc.RpcError) as err:
            s.CheckAccess(
                retrieval_pb2.CheckAccessRequest(chunk_ids=["src_evalcredit:eval-00#x"]),
                metadata=md(token),
                timeout=10,
            )
        assert err.value.code() == grpc.StatusCode.UNAVAILABLE
        with pytest.raises(grpc.RpcError) as err:
            s.CheckDocumentAccess(
                retrieval_pb2.CheckDocumentAccessRequest(doc_ids=["eval-00"]),
                metadata=md(token, "batch"),
                timeout=10,
            )
        assert err.value.code() == grpc.StatusCode.UNAVAILABLE
        r = excerpt(s, token, "eval-00", "src_evalcredit:eval-00#x")
        assert isinstance(r, grpc.RpcError) and r.code() == grpc.StatusCode.UNAVAILABLE
