import asyncio
from collections.abc import AsyncIterator
from typing import Any

import grpc
from opentelemetry import trace
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics.export import HistogramDataPoint, InMemoryMetricReader
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import REGISTRY, generate_latest

from bambuflow.answer.v1 import answer_pb2, answer_pb2_grpc
from bambuflow_platform import telemetry


class Unused(answer_pb2_grpc.AnswerServiceServicer):
    """这里只用到 Answer 与 StreamAnswer；回放、反馈与删除会话接口不涉及拦截器行为。"""

    async def GetAnswer(self, request: Any, context: Any) -> answer_pb2.GetAnswerResponse:
        raise NotImplementedError

    async def CreateFeedback(self, request: Any, context: Any) -> answer_pb2.CreateFeedbackResponse:
        raise NotImplementedError

    async def DeleteConversation(
        self, request: Any, context: Any
    ) -> answer_pb2.DeleteConversationResponse:
        raise NotImplementedError


class Stub(Unused):
    async def Answer(self, request: Any, context: Any) -> answer_pb2.AnswerResponse:
        return answer_pb2.AnswerResponse()

    async def StreamAnswer(
        self, request: Any, context: Any
    ) -> AsyncIterator[answer_pb2.StreamAnswerResponse]:
        yield answer_pb2.StreamAnswerResponse(meta=answer_pb2.AnswerMeta())


def test_trace_context_crosses_grpc() -> None:
    exporter = InMemorySpanExporter()
    provider = telemetry.install("test", SimpleSpanProcessor(exporter))

    async def scenario() -> int:
        server = grpc.aio.server(interceptors=[telemetry.aio_server_interceptor()])
        answer_pb2_grpc.add_AnswerServiceServicer_to_server(Stub(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            with trace.get_tracer("test").start_as_current_span("root") as root:
                async with grpc.aio.insecure_channel(
                    f"127.0.0.1:{port}", interceptors=telemetry.aio_client_interceptors()
                ) as ch:
                    stub = answer_pb2_grpc.AnswerServiceStub(ch)
                    await stub.Answer(answer_pb2.AnswerRequest(), timeout=5)
                    # 服务端流（StreamAnswer）同样接上调用方的 trace
                    async for _ in stub.StreamAnswer(answer_pb2.StreamAnswerRequest(), timeout=5):
                        pass
                trace_id: int = root.get_span_context().trace_id
                return trace_id
        finally:
            await server.stop(None)

    want = asyncio.run(scenario())
    provider.force_flush()
    servers = [s for s in exporter.get_finished_spans() if s.kind == trace.SpanKind.SERVER]
    assert len(servers) == 2, "一元与流式调用各应有一个服务端 span"
    assert all(s.context.trace_id == want for s in servers)
    assert servers[0].resource.attributes["service.name"] == "test"


class Failing(Unused):
    async def Answer(self, request: Any, context: Any) -> answer_pb2.AnswerResponse:
        await context.abort(grpc.StatusCode.PERMISSION_DENIED, "no")
        raise AssertionError("unreachable")

    async def StreamAnswer(
        self, request: Any, context: Any
    ) -> AsyncIterator[answer_pb2.StreamAnswerResponse]:
        yield answer_pb2.StreamAnswerResponse(meta=answer_pb2.AnswerMeta())


def test_metrics_interceptor_records_red_by_method_and_code() -> None:
    reader = InMemoryMetricReader()
    provider = telemetry.meter_provider("test", reader)
    interceptor = telemetry.MetricsInterceptor(provider.get_meter("test"))

    async def scenario() -> None:
        for servicer in (Stub(), Failing()):
            server = grpc.aio.server(interceptors=[interceptor])
            answer_pb2_grpc.add_AnswerServiceServicer_to_server(servicer, server)
            port = server.add_insecure_port("127.0.0.1:0")
            await server.start()
            try:
                async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
                    stub = answer_pb2_grpc.AnswerServiceStub(ch)
                    try:
                        await stub.Answer(answer_pb2.AnswerRequest(), timeout=5)
                    except grpc.aio.AioRpcError:
                        pass
                    async for _ in stub.StreamAnswer(answer_pb2.StreamAnswerRequest(), timeout=5):
                        pass
            finally:
                await server.stop(None)

    asyncio.run(scenario())
    data = reader.get_metrics_data()
    assert data is not None
    points = [
        p
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == "rpc.server.call.duration"
        for p in m.data.data_points
        if isinstance(p, HistogramDataPoint)
    ]
    attrs = [dict(p.attributes or {}) for p in points]
    got = {
        (a["rpc.method"], a["rpc.response.status_code"]): p.count
        for a, p in zip(attrs, points, strict=True)
    }
    svc = "bambuflow.answer.v1.AnswerService"
    assert got == {
        (f"{svc}/Answer", "OK"): 1,
        (f"{svc}/Answer", "PERMISSION_DENIED"): 1,
        (f"{svc}/StreamAnswer", "OK"): 2,
    }
    # 秒级分桶
    assert tuple(points[0].explicit_bounds) == telemetry.SECONDS_BUCKETS


def test_prometheus_names_match_go_services() -> None:
    """告警规则与仪表盘按 Prometheus 里的名字写，Python 与 Go 服务导出的名字要一致：
    单位 s 加 _seconds 后缀，计数器加 _total。"""
    reader = PrometheusMetricReader()
    provider = telemetry.meter_provider("test", reader)
    meter = provider.get_meter("test")
    meter.create_histogram("test.names.duration", unit="s").record(0.3)
    meter.create_counter("test.names.answers").add(2, {"status": "answered"})
    out = generate_latest(REGISTRY).decode()
    lines = out.splitlines()
    assert any(
        line.startswith("test_names_duration_seconds_bucket{") and 'le="0.5"' in line
        for line in lines
    )
    assert any(
        line.startswith("test_names_answers_total{") and line.endswith('status="answered"} 2.0')
        for line in lines
    )
    provider.shutdown()
