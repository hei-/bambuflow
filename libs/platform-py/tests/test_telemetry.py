import asyncio
from collections.abc import AsyncIterator
from typing import Any

import grpc
from opentelemetry import trace
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from bambuflow.answer.v1 import answer_pb2, answer_pb2_grpc
from bambuflow_platform import telemetry


class Unused(answer_pb2_grpc.AnswerServiceServicer):
    """这里只用到 Answer 与 StreamAnswer；回放与反馈接口不涉及拦截器行为。"""

    async def GetAnswer(self, request: Any, context: Any) -> answer_pb2.GetAnswerResponse:
        raise NotImplementedError

    async def CreateFeedback(self, request: Any, context: Any) -> answer_pb2.CreateFeedbackResponse:
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
