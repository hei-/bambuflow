"""LLM listwise 重排（orchestrator 详设 6.5，技术设计文档 6.7 的 cross_encoder + llm 策略）。

从精排分最高的 POOL 条候选里由 LLM 选 KEEP 条进入上下文，预算 BUDGET 秒；不可用、超时或输出无效时
按精排分取前 KEEP 条。多跳路径在候选超过 20 条时用；
单跳只对书面同意放宽 SLO 的租户开放（pipeline 配置）。
带可疑指令标记的证据不给模型看（与规划器相同，详设 6.4），排在选中的证据之后。
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_orchestrator import flow, prompt
from bambuflow_orchestrator.budget import GENERATION_RESERVE
from bambuflow_orchestrator.llm import LLMUnavailableError
from bambuflow_orchestrator.parse import load_json_line

_log = logging.getLogger(__name__)

POOL = 20
KEEP = 8
BUDGET = 0.8


class Chooser(Protocol):
    def generate(
        self, system: str, user: str, timeout: float | None = None, *, level: int = 2
    ) -> flow.Generation: ...


@dataclass(frozen=True)
class Reranked:
    hits: list[retrieval_pb2.Hit]
    # 做了 LLM 重排时的模型与提示词版本（why.llm_rerank）；没做或失败时为空
    version: str = ""
    # 降级原因（llm_rerank 环节）；没有降级时为空
    degraded: str = ""


async def rerank(
    llm: Chooser,
    question: str,
    sub_questions: Sequence[str],
    hits: Sequence[retrieval_pb2.Hit],
    remaining: Callable[[], float],
) -> Reranked:
    """返回重排后的全部候选：选中的在前，其余按精排分。剩余时间不够生成预留加重排预算时不做。"""
    by_score = sorted(hits, key=lambda h: h.score, reverse=True)
    if remaining() <= GENERATION_RESERVE + BUDGET:
        return Reranked(list(hits), degraded="skipped_deadline")
    pool = [h for h in by_score if not h.injection_flag][:POOL]
    chosen, version, reason = await _choose(llm, question, sub_questions, pool)
    if chosen is None:
        chosen = by_score[:KEEP]
    ids = {h.chunk_id for h in chosen}
    return Reranked(chosen + [h for h in by_score if h.chunk_id not in ids], version, reason)


async def _choose(
    llm: Chooser, question: str, sub_questions: Sequence[str], pool: list[retrieval_pb2.Hit]
) -> tuple[list[retrieval_pb2.Hit] | None, str, str]:
    system = prompt.RERANK_SYSTEM.format(keep=KEEP)
    user = prompt.render_rerank(question, sub_questions, pool)
    lines: list[str] = []
    try:
        generation = llm.generate(system, user, BUDGET, level=flow.level_of(pool))
        async for raw in generation:
            lines.append(raw)
    except LLMUnavailableError as e:
        _log.warning("LLM 重排不可用，按精排分取证据", extra={"err": str(e)})
        return None, "", flow.unavailable_reason(e)
    order: list[int] = []
    for raw in lines:
        obj = load_json_line(raw)
        got = obj.get("order") if obj is not None else None
        if isinstance(got, list):
            # 模型输出不可信：只认范围内的整数编号（bool 也是 int，排除），重复的只取第一次
            valid = [
                n
                for n in got
                if isinstance(n, int) and not isinstance(n, bool) and 1 <= n <= len(pool)
            ]
            order = list(dict.fromkeys(valid))
            break
    if not order:
        return None, "", "unparseable"
    version = f"{generation.model}+{prompt.RERANK_PROMPT_VERSION}"
    return [pool[n - 1] for n in order[:KEEP]], version, ""
