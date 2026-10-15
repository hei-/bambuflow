"""orchestrator 的指标（详设 13.1）。只记租户、调用目的、意图、模型、状态与计数，
不记问题、上下文与回答正文：这些只进加密的 LLM 追踪库。"""

from __future__ import annotations

from opentelemetry import metrics

from bambuflow.answer.v1 import answer_pb2

_meter = metrics.get_meter("bambuflow_orchestrator")

answers = _meter.create_counter(
    "orchestrator.answers", description="回答数，按租户、调用目的与状态"
)
duration = _meter.create_histogram(
    "orchestrator.answer.duration", unit="s", description="从收到请求到回答完整的时间"
)
first_sentence = _meter.create_histogram(
    "orchestrator.answer.first_sentence",
    unit="s",
    description="流式回答从收到请求到推送第一句的时间",
)
coverage = _meter.create_histogram(
    "orchestrator.answer.coverage",
    description="引用覆盖率（通过校验的句子数 / 陈述总数）",
    explicit_bucket_boundaries_advisory=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0],
)
tokens = _meter.create_counter(
    "orchestrator.tokens", description="消耗的 token，按租户、模型与方向"
)
degraded = _meter.create_counter(
    "orchestrator.degraded", description="回答里的降级标记，按环节与原因"
)
dropped_sentences = _meter.create_counter(
    "orchestrator.sentences.dropped", description="未通过校验被丢弃的句子，按原因"
)
# 正常情况下接近 0，出现即说明上游过滤或配置有问题（详设 13.4：大于 0 按安全事件处理）
security = _meter.create_counter(
    "orchestrator.security", description="安全事件：返回前权限复核拦截的块、模型路由越界"
)
circuit_open = _meter.create_up_down_counter(
    "orchestrator.llm.circuit_open", description="处于熔断打开状态的 LLM 模型数"
)


def observe(
    tenant: str,
    purpose: str,
    resp: answer_pb2.AnswerResponse,
    usage: answer_pb2.TokenUsage,
    elapsed: float,
    denied: int,
) -> None:
    """一次完整回答的指标；在写回答记录时调用（中途断开的回答不记）。"""
    status = answer_pb2.Status.Name(resp.status).removeprefix("STATUS_").lower()
    intent = answer_pb2.Intent.Name(resp.why.intent).removeprefix("INTENT_").lower()
    answers.add(1, {"tenant": tenant, "purpose": purpose, "status": status})
    duration.record(elapsed, {"tenant": tenant, "purpose": purpose, "intent": intent})
    if resp.HasField("confidence"):
        coverage.record(resp.confidence.coverage, {"tenant": tenant})
    model = resp.why.generation_model
    tokens.add(usage.prompt_tokens, {"tenant": tenant, "model": model, "direction": "input"})
    tokens.add(usage.completion_tokens, {"tenant": tenant, "model": model, "direction": "output"})
    for d in resp.why.degraded:
        degraded.add(1, {"stage": d.stage, "reason": d.reason})
    if denied:
        security.add(denied, {"tenant": tenant, "kind": "permission_recheck_blocked"})
