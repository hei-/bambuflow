package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// MetricsListen 是指标端点的默认地址（OTel 的 Prometheus 导出器约定端口），由 BF_METRICS_LISTEN 覆盖。
const MetricsListen = ":9464"

// Meter 是各模块登记指标用的 Meter；SetupMetrics 之前登记的指标在安装后自动接上。
func Meter(name string) metric.Meter {
	return otel.GetMeterProvider().Meter(name)
}

// SecondsBuckets 是以秒计的耗时直方图的分桶（与 libs/platform-py 的 telemetry 一致）：SDK 的默认分桶按毫秒设计，
// 用在秒上几乎全部落进第一个桶。覆盖检索的百毫秒级到 agent 多跳的数十秒。
var SecondsBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 1.5, 2, 4, 8, 15, 30, 60}

var secondsView = sdkmetric.NewView(
	sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram, Unit: "s"},
	sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: SecondsBuckets}},
)

// SetupMetrics 安装全局 MeterProvider，指标经 Prometheus 格式的 /metrics 导出（技术设计文档 14.2）；
// 返回该端点的处理器。指标只带 ID、计数、耗时和版本类的维度，不带查询原文与用户标识。
func SetupMetrics(service string) (http.Handler, error) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	exp, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, err
	}
	res := resource.NewSchemaless(semconv.ServiceName(service))
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp), sdkmetric.WithResource(res),
		sdkmetric.WithView(secondsView)))
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}

// ServeMetrics 安装指标并在 BF_METRICS_LISTEN（默认 :9464）提供 /metrics，ctx 结束时关闭。
// 端口起不来只记日志：指标不可用不影响业务。
func ServeMetrics(ctx context.Context, service string, log *slog.Logger) error {
	h, err := SetupMetrics(service)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", h)
	addr := MetricsListen
	if v := os.Getenv("BF_METRICS_LISTEN"); v != "" {
		addr = v
	}
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("指标端点启动失败", "addr", addr, "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	return nil
}

// HTTPServer 为 HTTP 服务记录 RED 指标（http.server.request.duration，按路由模式、方法与状态码）。
// 路由取 ServeMux 匹配到的模式（r.Pattern），未登记的路径记为 unmatched，避免把原始路径变成高基数的维度。
func HTTPServer(next http.Handler) http.Handler {
	hist, err := Meter("bambuflow/http").Float64Histogram("http.server.request.duration",
		metric.WithUnit("s"), metric.WithDescription("HTTP 请求耗时"))
	if err != nil {
		otel.Handle(err)
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		hist.Record(r.Context(), time.Since(start).Seconds(), metric.WithAttributes(
			semconv.HTTPRoute(route), semconv.HTTPRequestMethodKey.String(r.Method),
			semconv.HTTPResponseStatusCode(sw.status)))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Flush 透传给底层：SSE 流式回答依赖逐段刷出。
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap 让 http.ResponseController 找到底层的 ResponseWriter（设置写超时等）。
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Gauge 登记一个抓取时才取值的仪表（如密钥剩余有效期、最近一次成功的时间）。登记失败只交给 OTel 的错误处理：指标不影响业务。
func Gauge(meter, name, description, unit string, value func() float64) {
	_, err := Meter(meter).Float64ObservableGauge(name, metric.WithDescription(description), metric.WithUnit(unit),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(value())
			return nil
		}))
	if err != nil {
		otel.Handle(err)
	}
}
