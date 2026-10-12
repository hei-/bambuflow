from pathlib import Path

from bambuflow_evaluation import golden
from bambuflow_evaluation.golden import Asker, Question
from bambuflow_evaluation.readiness import assess

RISK = Asker("risk_manager", "bob", "risk", "风险管理部", 3, ("risk-dept",))
OTHER = Asker("credit_l2", "alice", "credit", "信贷部", 2, ("credit-l2",))


def q(i: int, origin: str, reviewers: tuple[str, ...], status: str = "answered") -> Question:
    return Question(
        id=f"q{origin}{i}",
        bucket="single_hop",
        asker=RISK,
        question="问题",
        expected_status=status,
        key_points=(),
        must_cite=(),
        must_not_cite=(),
        origin=origin,
        reviewers=reviewers,
    )


def bank(expert: int, synthetic: int, unreviewed: int = 0, refuse_every: int = 5) -> list[Question]:
    out = [
        q(
            i,
            "expert",
            ("rick", "pete"),
            "insufficient_evidence" if i % refuse_every == 0 else "answered",
        )
        for i in range(expert)
    ]
    out += [q(i, "llm_generated", ("rick",)) for i in range(synthetic)]
    out += [q(i, "llm_generated_unreviewed", ()) for i in range(unreviewed)]
    return out


def test_stages() -> None:
    # 样例评测集的题都没审核：一道不计
    r = assess("risk", bank(0, 0, unreviewed=32))
    assert (r.stage, r.counted, r.unreviewed) == ("none", 0, 32)
    assert any("至少 100" in g for g in r.gaps)
    # 100 题、合成题一半、应拒答 ≥ 15%（专家题每 5 道 1 道应拒答 → 10%，不够）
    r = assess("risk", bank(50, 50))
    assert r.stage == "none" and any("应拒答" in g for g in r.gaps)
    r = assess("risk", bank(50, 50, refuse_every=2))
    assert r.stage == "pilot" and r.synthetic_share == 0.5
    assert any("至少 300" in g for g in r.gaps) and any("双人标注" in g for g in r.gaps)
    # 合成题超过一半不行
    assert assess("risk", bank(40, 60, refuse_every=2)).stage == "none"
    r = assess("risk", bank(160, 150, refuse_every=2))
    assert (r.stage, r.double_reviewed, r.gaps) == ("general", 160, ())
    # 只看这个租户的题
    other = [Question(**{**q(1, "expert", ("a", "b")).__dict__, "asker": OTHER})]
    assert assess("risk", other).counted == 0


def test_question_provenance_overrides_default(tmp_path: Path) -> None:
    (tmp_path / "askers.yaml").write_text(
        "risk_manager: {sub: bob, tenant: risk, dept: 风险管理部, clearance: 3,"
        " groups: [risk-dept]}\n",
        encoding="utf-8",
    )
    (tmp_path / "questions.yaml").write_text(
        """version: v1
tenant_set: demo
provenance_default: {source: llm_generated_unreviewed, author: claude, reviewed_by: null}
questions:
  - {id: a, bucket: single_hop, asker: risk_manager, question: 甲, expected: {status: answered}}
  - id: b
    bucket: single_hop
    asker: risk_manager
    question: 乙
    expected: {status: answered}
    provenance: {source: expert, author: rick, reviewed_by: [rick, pete, rick]}
""",
        encoding="utf-8",
    )
    gs = golden.load(tmp_path)
    a, b = gs.questions
    assert (a.origin, a.reviewers) == ("llm_generated_unreviewed", ())
    assert (b.origin, b.reviewers) == ("expert", ("pete", "rick"))
