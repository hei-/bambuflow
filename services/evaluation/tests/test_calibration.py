import math

from bambuflow_evaluation import calibration
from bambuflow_evaluation.metrics import Outcome


def outcome(
    tenant: str,
    features: tuple[float, ...],
    correct: bool,
    score: float = 0.9,
    status: str = "answered",
) -> Outcome:
    return Outcome(
        qid="q",
        tenant=tenant,
        bucket="fact",
        expected_status="answered",
        status=status,
        must_cite=(),
        top10=(),
        cited=(),
        sentence_refs=(True,),
        sentence_grounded=(correct,),
        pending=False,
        violations=(),
        latency_ms=1.0,
        confidence_features=features,
        confidence_score=score,
    )


def synthetic(n: int, tenant: str = "credit", offset: int = 0) -> list[Outcome]:
    """正确的概率随覆盖率上升、有冲突时下降；当前权重一律给 0.9（明显偏高）。
    用无理数倍数取小数部分代替随机数：结果确定，分布足够均匀。"""
    out = []
    for i in range(offset, offset + n):
        c, s, u = (i * 0.6180339887) % 1, (i * 0.4142135623) % 1, (i * 0.7548776662) % 1
        k = float(i % 5 == 0)
        p = 1 / (1 + math.exp(-(4 * c + 1 * s - 2 * k - 2.5)))
        out.append(outcome(tenant, (c, s, 0.5, 1 / 3, k), u < p))
    return out


def test_label_requires_answered_grounded_and_judged_correct() -> None:
    o = outcome("t", (1, 1, 1, 1, 0), True)
    assert calibration.label(o)
    assert not calibration.label(outcome("t", (1, 1, 1, 1, 0), False))
    assert not calibration.label(Outcome(**{**vars(o), "faithful": False}))
    assert not calibration.label(Outcome(**{**vars(o), "key_points_covered": (True, False)}))
    assert not calibration.label(Outcome(**{**vars(o), "expected_status": "refused"}))


def test_ece_is_zero_when_predictions_match_frequencies() -> None:
    assert calibration.ece([0.75] * 4, [True, True, True, False]) == 0.0
    assert abs(calibration.ece([0.95] * 4, [True, False, True, False]) - 0.45) < 1e-9


def test_fit_recovers_direction_and_meets_target() -> None:
    (g, t) = calibration.calibrate(synthetic(2000))
    assert g.scope == "global" and g.samples == 2000
    assert g.ece_current is not None and g.ece_current > 0.2  # 一律 0.9 明显偏高
    assert g.ece_fitted is not None and g.ece_fitted <= calibration.ECE_TARGET and g.passed
    # 覆盖率越高越可信；冲突项按公式的写法导出为正数（公式里是 −w5·k）
    assert g.weights["coverage"] > 1.0 and g.weights["conflict"] > 0.5
    assert g.version.startswith("confidence-")
    # 租户样本超过 300：单独拟合
    assert t.scope == "credit" and t.weights


def test_small_tenants_use_global_and_tiny_sets_are_not_fitted() -> None:
    rows = calibration.calibrate([*synthetic(120), *synthetic(30, "ops", offset=500)])
    by = {r.scope: r for r in rows}
    assert by["credit"].note.endswith("使用全局权重") and not by["credit"].weights
    assert by["global"].weights  # 150 题，够拟合
    tiny = calibration.calibrate(synthetic(20))
    assert not tiny[0].weights and "不拟合" in tiny[0].note


def test_only_answered_questions_with_confidence_are_samples() -> None:
    rows = [
        outcome("t", (1, 1, 1, 1, 0), True),
        outcome("t", (), True),
        outcome("t", (1, 1, 1, 1, 0), False, status="insufficient_evidence"),
    ]
    assert len(calibration.samples(rows)) == 1
