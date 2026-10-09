from bambuflow.answer.v1 import answer_pb2
from bambuflow_orchestrator import confidence


def test_strong_answer_is_high_and_weak_is_low() -> None:
    strong = confidence.compute(4, [0.95, 0.9, 0.92, 0.88], 0.9, 3)
    assert strong.level == answer_pb2.CONFIDENCE_LEVEL_HIGH and strong.score > 0.85
    assert strong.coverage == 1.0 and strong.source_diversity == 1.0
    weak = confidence.compute(1, [0.9, 0.2], 0.4, 1)
    assert weak.level == answer_pb2.CONFIDENCE_LEVEL_LOW
    assert weak.weights_version == confidence.DEFAULT.version


def test_missing_rerank_is_neutral_and_conflict_lowers_score() -> None:
    a = confidence.compute(3, [0.9, 0.9, 0.9], None, 2)
    assert a.top_rerank == confidence.NO_RERANK
    b = confidence.compute(3, [0.9, 0.9, 0.9], None, 2, conflict=True)
    assert b.score < a.score and b.conflict
