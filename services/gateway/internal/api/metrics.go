package api

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hei-/bambuflow/libs/platform-go/telemetry"
)

// gateway 的业务指标（gateway 详设 11.1）。维度只用接口、租户、状态码与错误类型，不带用户与调用方的标识。
var (
	meter = telemetry.Meter("bambuflow/gateway")
	// 按接口、租户、状态码与错误类型（problem 的 type）计数：401、403 异常升高，
	// 无用户身份的数据请求（user_identity_required）与 429、503 比例由告警规则按它算
	requests, _ = meter.Int64Counter("gateway.requests", metric.WithDescription("对外接口的请求结果"))
	// 正在进行的 SSE 流：接近单实例上限的 80% 时告警
	sseStreams, _ = meter.Int64UpDownCounter("gateway.sse.streams", metric.WithDescription("正在进行的 SSE 流"))
)

func countRequest(c *call, status int, reason string) {
	requests.Add(context.WithoutCancel(c.ctx), 1, metric.WithAttributes(
		attribute.String("endpoint", c.endpoint), attribute.String("tenant", c.tenant),
		attribute.Int("status", status), attribute.String("reason", reason)))
}
