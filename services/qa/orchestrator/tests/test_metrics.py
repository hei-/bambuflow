from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    InMemoryMetricReader,
    NumberDataPoint,
)

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow_orchestrator import metrics as bf_metrics

# 模块里的指标在导入时经全局代理创建，装上 SDK 的 MeterProvider 后接到这个读取器
_reader = InMemoryMetricReader()
metrics.set_meter_provider(MeterProvider(metric_readers=[_reader]))


def _points(name: str) -> list[tuple[dict[str, object], float]]:
    data = _reader.get_metrics_data()
    assert data is not None
    out: list[tuple[dict[str, object], float]] = []
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                if m.name != name:
                    continue
                for p in m.data.data_points:
                    if isinstance(p, NumberDataPoint):
                        out.append((dict(p.attributes or {}), float(p.value)))
                    elif isinstance(p, HistogramDataPoint):
                        out.append((dict(p.attributes or {}), float(p.count)))
    return out


def test_observe_records_status_tokens_degradation_and_recheck_blocks() -> None:
    resp = answer_pb2.AnswerResponse(
        status=answer_pb2.STATUS_EVIDENCE_ONLY,
        why=answer_pb2.Why(
            intent=answer_pb2.INTENT_SINGLE_HOP,
            generation_model="qwen",
            degraded=[types_pb2.Degradation(stage="llm", reason="circuit_open")],
        ),
        confidence=answer_pb2.Confidence(coverage=0.5),
    )
    usage = answer_pb2.TokenUsage(prompt_tokens=120, completion_tokens=30)
    bf_metrics.observe("metrics-test", "answer", resp, usage, 1.2, denied=2)

    assert (
        {"tenant": "metrics-test", "purpose": "answer", "status": "evidence_only"},
        1.0,
    ) in _points("orchestrator.answers")
    assert (
        {"tenant": "metrics-test", "purpose": "answer", "intent": "single_hop"},
        1.0,
    ) in _points("orchestrator.answer.duration")
    # 同一进程里别的测试也会记指标：只看这个租户的
    tokens = {
        a["direction"]: v
        for a, v in _points("orchestrator.tokens")
        if a["tenant"] == "metrics-test"
    }
    assert tokens == {"input": 120.0, "output": 30.0}
    assert ({"stage": "llm", "reason": "circuit_open"}, 1.0) in _points("orchestrator.degraded")
    # 返回前复核拦下的块按安全事件计
    assert (
        {"tenant": "metrics-test", "kind": "permission_recheck_blocked"},
        2.0,
    ) in _points("orchestrator.security")
    assert ({"tenant": "metrics-test"}, 1.0) in _points("orchestrator.answer.coverage")
