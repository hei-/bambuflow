package telemetry

import (
	"context"
	"net"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

type stub struct {
	retrievalv1.UnimplementedRetrievalServiceServer
}

func (stub) CheckAccess(context.Context, *retrievalv1.CheckAccessRequest) (*retrievalv1.CheckAccessResponse, error) {
	return &retrievalv1.CheckAccessResponse{}, nil
}

func TestTraceContextCrossesGRPC(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := NewTracerProvider("test", sdktrace.WithSpanProcessor(rec))
	Install(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer(ServerOption())
	retrievalv1.RegisterRetrievalServiceServer(s, stub{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet", DialOption(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, root := tp.Tracer("test").Start(context.Background(), "root")
	if _, err := retrievalv1.NewRetrievalServiceClient(conn).CheckAccess(ctx, &retrievalv1.CheckAccessRequest{}); err != nil {
		t.Fatal(err)
	}
	root.End()

	want := root.SpanContext().TraceID()
	var server sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.SpanKind().String() == "server" {
			server = s
		}
	}
	if server == nil {
		t.Fatalf("没有记录到服务端 span: %d 个 span", len(rec.Ended()))
	}
	if server.SpanContext().TraceID() != want {
		t.Errorf("服务端 span 的 trace %s 应与调用方 %s 一致", server.SpanContext().TraceID(), want)
	}
	if server.Resource().String() == "" {
		t.Error("span 应带服务名资源")
	}
}
