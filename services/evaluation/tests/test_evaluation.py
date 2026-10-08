from dataclasses import replace
from pathlib import Path

from bambuflow_evaluation import golden, retrieval
from bambuflow_evaluation.affected import affected
from bambuflow_evaluation.metrics import Outcome, average, compute, judge, tenant_metrics

GOLDEN = Path(__file__).resolve().parents[3] / "golden" / "wiki-demo"
BASE = Outcome(
    qid="q1",
    tenant="credit",
    bucket="single_hop",
    expected_status="answered",
    status="answered",
    must_cite=("c1", "c3"),
    top10=("c1", "c2"),
    cited=("c1",),
    sentence_refs=(True, True),
    sentence_grounded=(True, False),
    pending=False,
    violations=(),
    latency_ms=100.0,
)
REFUSE = replace(BASE, qid="q2", expected_status="insufficient_evidence", must_cite=())


def test_compute() -> None:
    m = compute([BASE, REFUSE, replace(BASE, qid="q3", pending=True, must_cite=("gone",))])
    assert m["recall_at_10"] == 0.5  # c1 进了前 10，c3 没有；待复核题不计入
    assert m["citation_coverage"] == 1.0 and m["groundedness"] == 0.5
    assert m["status_accuracy"] == 0.5 and m["refusal_accuracy"] == 0.0
    assert m["false_refusal_rate"] == 0.0 and m["latency_p95_ms"] == 100.0


def test_judge_permission_violation_fails() -> None:
    run = [replace(BASE, violations=("secret#1",))]
    v = judge([run], None)
    assert v.conclusion == "failed" and "越权" in v.reasons[0]


def test_judge_regression_against_baseline() -> None:
    run = [BASE]
    m = compute(run)
    assert judge([run], dict(m) | {"groundedness": 0.6}).conclusion == "failed"  # 低 10 个点
    assert judge([run], dict(m) | {"groundedness": 0.51}).conclusion == "passed"


def test_judge_compares_conclusions_not_individual_answers() -> None:
    a = [BASE, REFUSE]
    b = [BASE, replace(REFUSE, status="insufficient_evidence")]
    # 单题结果不同但每次都不低于基线：结论一致，通过，并列出不稳定的题
    v = judge([a, a, b], compute(a))
    assert v.conclusion == "passed" and v.unstable == ("q2",)
    # 有的运行低于基线、有的不低：结论不一致，无法判定
    v = judge([a, a, b], compute(a) | {"status_accuracy": 0.9})
    assert v.conclusion == "indeterminate"
    pending = [replace(BASE, pending=True), REFUSE]
    assert judge([pending], None).conclusion == "indeterminate"
    assert (
        average([a, b])["status_accuracy"]
        == (compute(a)["status_accuracy"] or 0) / 2 + (compute(b)["status_accuracy"] or 0) / 2
    )


def test_golden_set_is_well_formed() -> None:
    gs = golden.load(GOLDEN)
    assert len(gs.questions) >= 100  # 试点阶段 ≥ 100 题（评测详设 3.1）
    refuse = [q for q in gs.questions if q.expected_status != "answered"]
    assert len(refuse) / len(gs.questions) >= 0.15  # 应拒答 ≥ 15%（评测详设 3.1）
    for q in gs.questions:
        for c in q.must_cite + q.must_not_cite:
            assert c.startswith("src_wikidemo:") and "#" in c, (q.id, c)
        assert q.must_cite or q.expected_status != "answered", q.id


def test_review_sheet_lists_every_question() -> None:
    gs = golden.load(GOLDEN)
    sheet = golden.review_sheet(gs)
    assert sheet.count("| [ ] |") == len(gs.questions)
    assert "credit-mortgage-loan" in sheet and "已废止的 2023 版" in sheet


def test_retrieval_scores() -> None:
    rel = frozenset({"a", "b"})
    recall, ndcg, mrr = retrieval.score(["x", "a", "y", "b"], rel)
    assert recall == 1.0 and mrr == 0.5
    assert 0 < ndcg < 1  # 相关段落不在最前面
    assert retrieval.score(["a", "b"], rel) == (1.0, 1.0, 1.0)
    assert retrieval.score(["x"] * 10 + ["a"], rel) == (0.0, 0.0, 0.0)  # 第 11 名不计
    conclusion, _ = retrieval.judge(
        {"recall_at_10": 0.7, "ndcg_at_10": 0.6, "mrr_at_10": 0.6},
        {"recall_at_10": 0.75, "ndcg_at_10": 0.6, "mrr_at_10": 0.6},
    )
    assert conclusion == "failed"


def test_retrieval_set_is_well_formed() -> None:
    meta, queries = retrieval.load(GOLDEN.parent / "t2retrieval")
    assert len(queries) == meta["queries"] and all(q.relevant for q in queries)
    ids_file = GOLDEN.parent / "t2retrieval" / "corpus_ids.txt"
    if not meta.get("corpus_all"):
        ids = set(ids_file.read_text().split())
        assert len(ids) == meta["corpus"]
        assert all(q.relevant <= ids for q in queries)  # 每条查询的相关段落都在语料里


def test_judge_per_tenant_catches_regression_hidden_in_total() -> None:
    # credit 两题都变差，risk 两题都变好：整体持平，但 credit 下降超过 2 个点，必须阻断
    good, bad = BASE, replace(BASE, top10=("c9",))
    base_runs = [[good, good, replace(bad, tenant="risk"), replace(bad, tenant="risk")]]
    cur_runs = [[bad, bad, replace(good, tenant="risk"), replace(good, tenant="risk")]]
    base = tenant_metrics(base_runs)
    assert compute(base_runs[0])["recall_at_10"] == compute(cur_runs[0])["recall_at_10"]
    v = judge(cur_runs * 3, average(base_runs), base)
    assert v.conclusion == "failed" and v.reasons[0].startswith("credit：recall_at_10")
    # 只跑 risk 的题：credit 不在本次运行里，不比较
    assert (
        judge([[o for o in cur_runs[0] if o.tenant == "risk"]] * 3, None, base).conclusion
        == "passed"
    )


def test_affected_tenants_by_changed_paths() -> None:
    tenants = {"credit", "risk", "ops"}
    assert (
        affected(["docs/terminology.md", "deploy/helm/bambuflow/values.yaml", "README.md"], tenants)
        == set()
    )
    assert affected(["tools/test-upstream/tenants/risk/tenant.yaml"], tenants) == {"risk"}
    assert affected(["config/tenants/hr/tenant.yaml"], tenants) == set()  # 评测集里没有该租户
    for p in (
        "services/retrieval/internal/search/fuse.go",
        "libs/grounding/x.py",
        "golden/wiki-demo/questions.yaml",
    ):
        assert affected([p, "docs/x.md"], tenants) == tenants
