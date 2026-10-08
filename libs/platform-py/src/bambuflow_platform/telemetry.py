"""初始化 OpenTelemetry 追踪（技术设计文档 12.5、14.3），与 libs/platform-go/telemetry 对应。

trace 上下文用 W3C traceparent 传递；导出目标按 OTel 标准环境变量配置
（OTEL_EXPORTER_OTLP_ENDPOINT 等）。
span 属性只记 ID、版本和耗时，不记查询原文。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import cast

import grpc
from opentelemetry import propagate, trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation import grpc as otel_grpc
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# opentelemetry-instrumentation-grpc 没有类型标注，在这里一次性给出它们的签名
_server = cast(Callable[[], grpc.aio.ServerInterceptor], otel_grpc.aio_server_interceptor)
_clients = cast(
    Callable[[], Sequence[grpc.aio.ClientInterceptor]], otel_grpc.aio_client_interceptors
)


def aio_server_interceptor() -> grpc.aio.ServerInterceptor:
    """grpc.aio 服务端拦截器：为每个请求建 span，并接上调用方传来的 trace。"""
    return _server()


def aio_client_interceptors() -> Sequence[grpc.aio.ClientInterceptor]:
    """grpc.aio 客户端拦截器：为每次调用建 span，并把 trace 上下文带给下游。"""
    return _clients()


def install(service: str, processor: SpanProcessor) -> TracerProvider:
    """创建带服务名的 TracerProvider 并设为全局，统一用 W3C traceparent 与 baggage 传播。"""
    provider = TracerProvider(resource=Resource.create({"service.name": service}))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    propagate.set_global_textmap(
        CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
    )
    return provider


def setup(service: str) -> TracerProvider:
    """按环境变量创建 OTLP 导出器；退出前调用返回值的 shutdown()，把缓冲的 span 发完。"""
    return install(service, BatchSpanProcessor(OTLPSpanExporter()))
