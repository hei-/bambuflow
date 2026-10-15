package telemetry

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	return string(body)
}

func TestMetricsRecordREDBySecondsBuckets(t *testing.T) {
	h, err := SetupMetrics("test")
	if err != nil {
		t.Fatal(err)
	}

	// gRPC 服务端：otelgrpc 的 stats handler 经全局 MeterProvider 记耗时与状态码
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer(ServerOption())
	retrievalv1.RegisterRetrievalServiceServer(s, stub{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := retrievalv1.NewRetrievalServiceClient(conn).CheckAccess(t.Context(), &retrievalv1.CheckAccessRequest{}); err != nil {
		t.Fatal(err)
	}

	// HTTP：路由记匹配到的模式，不记原始路径；未登记的路径记为 unmatched
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/answers/{answer_id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	srv := HTTPServer(mux)
	for _, path := range []string{"/v1/answers/ans_123", "/nope"} {
		srv.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	}

	Gauge("test", "test.key.remaining", "", "s", func() float64 { return 42 })

	out := scrape(t, h)
	for _, want := range []string{
		`rpc_server_call_duration_seconds_count{`,
		// otelgrpc 用新的 RPC 语义约定：方法写全名，状态码写名字（Python 的 MetricsInterceptor 与之一致）
		`rpc_method="bambuflow.retrieval.v1.RetrievalService/CheckAccess"`,
		`rpc_response_status_code="OK"`,
		`http_route="GET /v1/answers/{answer_id}"`,
		`http_response_status_code="403"`,
		`http_route="unmatched"`,
		// 秒级分桶：默认的毫秒分桶里没有 0.25
		`le="0.25"`,
		"go_goroutines",
		// 抓取时取值的仪表
		`test_key_remaining_seconds{otel_scope_name="test"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("指标输出缺少 %s，相关的行：\n%s", want, grepLines(out, "_count{"))
		}
	}
	if strings.Contains(out, "ans_123") {
		t.Error("原始路径进了指标维度")
	}
}

func TestHTTPServerKeepsFlusher(t *testing.T) {
	var flushed bool
	srv := HTTPServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// SSE 依赖逐段刷出：包装后的 ResponseWriter 仍要能 Flush
		f, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("包装后丢了 http.Flusher")
		}
		f.Flush()
		flushed = true
	}))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if !flushed || !rec.Flushed {
		t.Error("Flush 没有透传到底层")
	}
}

func grepLines(out, sub string) string {
	var lines []string
	for l := range strings.SplitSeq(out, "\n") {
		if strings.Contains(l, sub) {
			lines = append(lines, l)
		}
	}
	return strings.Join(lines, "\n")
}
