// Package telemetry 初始化 OpenTelemetry 追踪（技术设计文档 12.5、14.3）。
//
// trace 上下文用 W3C traceparent 在 gRPC metadata、事件消息头之间传递。导出目标按 OTel 标准环境变量配置
// （OTEL_EXPORTER_OTLP_ENDPOINT 等）；span 属性只记 ID、版本和耗时，不记查询原文。
package telemetry

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
)

// Setup 按环境变量创建 OTLP 导出器并设为全局，返回关闭函数（退出前调用，把缓冲的 span 发完）。
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := NewTracerProvider(service, sdktrace.WithBatcher(exp))
	Install(tp)
	return tp.Shutdown, nil
}

// NewTracerProvider 创建带服务名的 TracerProvider；opts 指定导出方式（测试里用内存记录器）。
func NewTracerProvider(service string, opts ...sdktrace.TracerProviderOption) *sdktrace.TracerProvider {
	res := resource.NewSchemaless(semconv.ServiceName(service))
	return sdktrace.NewTracerProvider(append([]sdktrace.TracerProviderOption{sdktrace.WithResource(res)}, opts...)...)
}

// Install 把 tp 设为全局，并统一用 W3C traceparent 与 baggage 传播。
func Install(tp *sdktrace.TracerProvider) {
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
}

// ServerOption 让 gRPC 服务端为每个请求建 span，并接上调用方传来的 trace。
func ServerOption() grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler())
}

// DialOption 让 gRPC 客户端为每次调用建 span，并把 trace 上下文带给下游。
func DialOption() grpc.DialOption {
	return grpc.WithStatsHandler(otelgrpc.NewClientHandler())
}
