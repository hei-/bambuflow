package search

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hei-/bambuflow/libs/platform-go/telemetry"
)

// retrieval 的业务指标（详设 11.1）。只记召回路、结果与计数，不记查询与正文。
var (
	meter = telemetry.Meter("bambuflow/retrieval")
	// 每路召回的耗时与结果：outcome 为 ok、idle（这一路无事可做）或降级原因（timeout、circuit_open 等）
	routeDuration, _ = meter.Float64Histogram("retrieval.route.duration", metric.WithUnit("s"),
		metric.WithDescription("单路召回耗时"))
	routeResults, _ = meter.Int64Counter("retrieval.route.results", metric.WithDescription("单路召回返回的候选条数"))
	// 组装阶段按权限表最新状态拦下的候选（二次过滤）：15 分钟内持续大于 0 说明索引里的 ACL 落后于元数据
	assembleDropped, _ = meter.Int64Counter("retrieval.assemble.dropped",
		metric.WithDescription("组装阶段二次过滤拦下的候选"))
	// 各层缓存的命中：layer 为 decision、vector、result
	cacheLookups, _ = meter.Int64Counter("retrieval.cache.lookups", metric.WithDescription("检索缓存的查找结果"))
)

func observeRoute(r route, d time.Duration) {
	outcome := "ok"
	switch {
	case r.reason != "":
		outcome = r.reason
	case r.idle:
		outcome = "idle"
	}
	ctx := context.Background()
	routeDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attribute.String("route", r.name), attribute.String("outcome", outcome)))
	n := 0
	for _, l := range r.lists {
		n += len(l)
	}
	routeResults.Add(ctx, int64(n), metric.WithAttributes(attribute.String("route", r.name)))
}

func countCache(ctx context.Context, layer string, hits, misses int) {
	if hits > 0 {
		cacheLookups.Add(ctx, int64(hits), metric.WithAttributes(attribute.String("layer", layer), attribute.String("result", "hit")))
	}
	if misses > 0 {
		cacheLookups.Add(ctx, int64(misses), metric.WithAttributes(attribute.String("layer", layer), attribute.String("result", "miss")))
	}
}
