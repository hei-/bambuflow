"""上下文构建（orchestrator 详设第 7 节）：按分数选块、编号 [S1]…[Sn]。

P0 简化：
- 证据区预算按字符粗估（中文约 1.3 字 / token，6000 字约 4500 token），
  接真实模型后按其分词器计算；
- 不做父块扩展和首尾排序，随精排一起在 P1 加入；
- 精排分下限由调用方按是否精排决定（flow.MIN_RERANK_SCORE）：未精排时分数是 RRF 融合分，不套用。
"""

from __future__ import annotations

from collections.abc import Sequence

from bambuflow.retrieval.v1 import retrieval_pb2

EVIDENCE_CHAR_BUDGET = 6000


def build(
    hits: Sequence[retrieval_pb2.Hit], max_citations: int, min_score: float | None = None
) -> list[tuple[int, retrieval_pb2.Hit]]:
    """按 Search 返回的顺序（已按分数排序）逐条加入，直到条数或预算用完。编号从 1 开始。
    min_score 不为空时，分数低于它的块不进上下文。"""
    chosen: list[retrieval_pb2.Hit] = []
    used = 0
    for h in hits:
        if min_score is not None and h.score < min_score:
            continue
        if len(chosen) == max_citations:
            break
        if used + len(h.text) > EVIDENCE_CHAR_BUDGET:
            # 放不下的块跳过，后面更短的块仍可能放得下
            continue
        chosen.append(h)
        used += len(h.text)
    return list(enumerate(chosen, start=1))
