"""回答整体的置信度（技术设计文档 7.8）。

score = sigmoid(w1·c + w2·s̄ + w3·r + w4·d − w5·k + b)：c 引用覆盖率、s̄ 平均支持度、r 最高精排分、
d 来源多样性、k 是否有未解决的证据冲突。权重应由 evaluation 在评测集上用逻辑回归校准（ECE ≤ 0.05），
校准结果经 governance 下发；在那之前用这里的默认权重：各项全满时约 0.92，
覆盖率与支持度一半时落在低档，宁可偏低，让用户去核实原文。
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from bambuflow.answer.v1 import answer_pb2


@dataclass(frozen=True)
class Weights:
    version: str
    coverage: float
    support: float
    rerank: float
    diversity: float
    conflict: float
    bias: float


DEFAULT = Weights("confidence-default@1", 2.0, 2.0, 1.0, 0.5, 1.5, -3.0)
HIGH, MEDIUM = 0.8, 0.5
# 没有精排时 r 取中间值：不因为缺少精排而抬高或压低置信度
NO_RERANK = 0.5


def level(score: float) -> answer_pb2.ConfidenceLevel:
    if score >= HIGH:
        return answer_pb2.CONFIDENCE_LEVEL_HIGH
    if score >= MEDIUM:
        return answer_pb2.CONFIDENCE_LEVEL_MEDIUM
    return answer_pb2.CONFIDENCE_LEVEL_LOW


def compute(
    passed: int,
    supports: Sequence[float],
    top_rerank: float | None,
    cited_docs: int,
    conflict: bool = False,
    w: Weights = DEFAULT,
) -> answer_pb2.Confidence:
    """supports 是全部陈述（含未通过的）的支持度；passed 是通过校验的句数。"""
    total = len(supports)
    c = passed / total if total else 0.0
    s = sum(supports) / total if total else 0.0
    r = NO_RERANK if top_rerank is None else min(1.0, max(0.0, top_rerank))
    d = min(cited_docs, 3) / 3
    z = (
        w.coverage * c
        + w.support * s
        + w.rerank * r
        + w.diversity * d
        - w.conflict * float(conflict)
        + w.bias
    )
    score = 1 / (1 + math.exp(-z))
    return answer_pb2.Confidence(
        score=round(score, 4),
        level=level(score),
        weights_version=w.version,
        coverage=round(c, 4),
        mean_support=round(s, 4),
        top_rerank=round(r, 4),
        source_diversity=round(d, 4),
        conflict=conflict,
    )
