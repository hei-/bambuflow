"""初始化 OpenTelemetry 追踪与指标（技术设计文档 12.5、14.2），与 libs/platform-go/telemetry 对应。

trace 上下文用 W3C traceparent 传递；导出目标按 OTel 标准环境变量配置
（OTEL_EXPORTER_OTLP_ENDPOINT 等）。
指标经 Prometheus 格式的 /metrics 导出（BF_METRICS_LISTEN，默认 :9464），名称与 Go 服务一致。
span 属性与指标维度只记 ID、计数、耗时和版本，不记查询原文与用户标识。
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any, cast

import grpc
from opentelemetry import metrics, propagate, trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.instrumentation import grpc as otel_grpc
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.metrics import Histogram, MeterProvider
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from prometheus_client import start_http_server

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


# 与 Go 服务的 telemetry.MetricsListen 同一端口；地址为空（":9464"）时同样监听所有地址
METRICS_LISTEN = "0.0.0.0:9464"
# 以秒计的耗时直方图的分桶（与 libs/platform-go 的 telemetry.SecondsBuckets 一致）：
# SDK 的默认分桶按毫秒设计，用在秒上几乎全部落进第一个桶
SECONDS_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 1.5, 2, 4, 8, 15, 30, 60)


def meter_provider(service: str, reader: Any) -> MeterProvider:
    """带服务名与秒级分桶的 MeterProvider；reader 指定导出方式（测试里用内存读取器）。"""
    return MeterProvider(
        resource=Resource.create({"service.name": service}),
        metric_readers=[reader],
        views=[
            View(
                instrument_type=Histogram,
                instrument_unit="s",
                aggregation=ExplicitBucketHistogramAggregation(SECONDS_BUCKETS),
            )
        ],
    )


def setup_metrics(service: str) -> None:
    """安装全局 MeterProvider，并在 BF_METRICS_LISTEN（默认 :9464）提供 /metrics。"""
    metrics.set_meter_provider(meter_provider(service, PrometheusMetricReader()))
    host, _, port = os.environ.get("BF_METRICS_LISTEN", METRICS_LISTEN).rpartition(":")
    start_http_server(int(port), addr=host)


class MetricsInterceptor(grpc.aio.ServerInterceptor):
    """gRPC 服务端的 RED 指标：rpc.server.call.duration（按方法、状态码），
    名称与维度和 Go 服务的 otelgrpc 一致，仪表盘与告警按同一套写。放在拦截器列表的第一个，
    鉴权失败也计入。"""

    def __init__(self, meter: metrics.Meter | None = None) -> None:
        meter = meter or metrics.get_meter("bambuflow/grpc")
        self._duration = meter.create_histogram(
            "rpc.server.call.duration", unit="s", description="gRPC 服务端处理耗时"
        )

    def _record(self, method: str, start: float, code: grpc.StatusCode) -> None:
        # 与 otelgrpc 的 RPC 语义约定一致：方法写“服务全名/方法”，状态码写名字
        self._duration.record(
            time.monotonic() - start,
            {
                "rpc.system.name": "grpc",
                "rpc.method": method.removeprefix("/"),
                "rpc.response.status_code": code.name,
            },
        )

    async def intercept_service(
        self,
        continuation: Callable[
            [grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler[Any, Any] | None]
        ],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler[Any, Any] | None:
        handler = await continuation(handler_call_details)
        if handler is None:
            return handler
        method = handler_call_details.method
        if handler.unary_unary is not None:
            unary = handler.unary_unary

            async def unary_behavior(request: Any, context: Any) -> Any:
                start = time.monotonic()
                code = grpc.StatusCode.UNKNOWN
                try:
                    response = await unary(request, context)
                    code = _code(context, grpc.StatusCode.OK)
                    return response
                except grpc.aio.AbortError:
                    code = _code(context, grpc.StatusCode.UNKNOWN)
                    raise
                except asyncio.CancelledError:
                    code = grpc.StatusCode.CANCELLED
                    raise
                finally:
                    self._record(method, start, code)

            return grpc.unary_unary_rpc_method_handler(
                unary_behavior,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if handler.unary_stream is not None:
            # 类型桩把流式处理函数写成返回同步迭代器；
            # 本平台的流式处理函数一律是异步生成器（AuthInterceptor）
            streaming: Any = handler.unary_stream

            async def stream_behavior(request: Any, context: Any) -> AsyncIterator[Any]:
                start = time.monotonic()
                code = grpc.StatusCode.UNKNOWN
                try:
                    async for response in streaming(request, context):
                        yield response
                    code = _code(context, grpc.StatusCode.OK)
                except grpc.aio.AbortError:
                    code = _code(context, grpc.StatusCode.UNKNOWN)
                    raise
                except asyncio.CancelledError:
                    # 客户端断开时流被取消
                    code = grpc.StatusCode.CANCELLED
                    raise
                finally:
                    self._record(method, start, code)

            return grpc.unary_stream_rpc_method_handler(
                stream_behavior,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        return handler


def _code(context: Any, default: grpc.StatusCode) -> grpc.StatusCode:
    code = context.code()
    return code if isinstance(code, grpc.StatusCode) else default
