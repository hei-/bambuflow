package grpcmw

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/hei-/bambuflow/libs/platform-go/logging"
	"github.com/hei-/bambuflow/libs/platform-go/principal"
)

// 编译期确认真实的 principal.Verifier 满足接口。
var _ Verifier = (*principal.Verifier)(nil)

// fakeVerifier 只认 "good" 和 "good-2"。
type fakeVerifier struct{}

func (fakeVerifier) Verify(_ context.Context, raw string) (*identityv1.Principal, error) {
	if strings.HasPrefix(raw, "good") {
		return &identityv1.Principal{Sub: "alice", Tenant: "credit"}, nil
	}
	return nil, errors.New("签名不对：" + raw)
}

// echoRetrieval 把看到的调用者写进 index_version，便于断言。
type echoRetrieval struct {
	retrievalv1.UnimplementedRetrievalServiceServer
	downstream retrievalv1.RetrievalServiceClient
}

func (s echoRetrieval) Search(ctx context.Context, req *retrievalv1.SearchRequest) (*retrievalv1.SearchResponse, error) {
	if s.downstream != nil {
		// 中间一跳：把 Principal 原样转给下游，并把剩余时间缩短到 1 秒以内
		ctx, cancel := context.WithTimeout(ForwardPrincipal(ctx), time.Second)
		defer cancel()
		return s.downstream.Search(ctx, req)
	}
	p := PrincipalFrom(ctx)
	dl, _ := ctx.Deadline()
	return &retrievalv1.SearchResponse{IndexVersion: p.GetSub() + "@" + p.GetTenant(), PolicyVersion: time.Until(dl).Round(time.Second).String()}, nil
}

func serve(t *testing.T, srv retrievalv1.RetrievalServiceServer, logs *bytes.Buffer) retrievalv1.RetrievalServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer(grpc.UnaryInterceptor(UnaryServer(fakeVerifier{}, logging.New(logs, "test", slog.LevelDebug))))
	retrievalv1.RegisterRetrievalServiceServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return retrievalv1.NewRetrievalServiceClient(conn)
}

func call(c retrievalv1.RetrievalServiceClient, token string, timeout time.Duration) (*retrievalv1.SearchResponse, error) {
	ctx := context.Background()
	if token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return c.Search(ctx, &retrievalv1.SearchRequest{})
}

func TestUnaryServer(t *testing.T) {
	var logs bytes.Buffer
	c := serve(t, echoRetrieval{}, &logs)

	if _, err := call(c, "good", 0); status.Code(err) != codes.InvalidArgument {
		t.Errorf("没有 deadline 应返回 INVALID_ARGUMENT，得到 %v", err)
	}
	if _, err := call(c, "", 5*time.Second); status.Code(err) != codes.Unauthenticated {
		t.Errorf("没有 Principal 应返回 UNAUTHENTICATED，得到 %v", err)
	}
	_, err := call(c, "forged-token-xyz", 5*time.Second)
	if status.Code(err) != codes.Unauthenticated || strings.Contains(err.Error(), "签名") {
		t.Errorf("无效 Principal 应返回不带细节的 UNAUTHENTICATED，得到 %v", err)
	}
	if !strings.Contains(logs.String(), "Principal 校验失败") {
		t.Errorf("失败原因应写日志: %s", logs.String())
	}
	resp, err := call(c, "good", 5*time.Second)
	if err != nil || resp.GetIndexVersion() != "alice@credit" {
		t.Fatalf("有效请求应拿到调用者身份，得到 %v %v", resp, err)
	}
}

func TestForwardPrincipalAndDeadlineOnlyShrinks(t *testing.T) {
	var logs bytes.Buffer
	leaf := serve(t, echoRetrieval{}, &logs)
	mid := serve(t, echoRetrieval{downstream: leaf}, &logs)

	resp, err := call(mid, "good-2", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetIndexVersion() != "alice@credit" {
		t.Errorf("下游应拿到同一个 Principal，得到 %q", resp.GetIndexVersion())
	}
	if resp.GetPolicyVersion() != "1s" {
		t.Errorf("中间一跳设的 1 秒应传到下游，得到剩余 %s", resp.GetPolicyVersion())
	}

	// 上游只给 300 ms 时，中间一跳的 1 秒不能把它放长
	resp, err = call(mid, "good-2", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPolicyVersion() != "0s" {
		t.Errorf("deadline 只能缩短，下游剩余应不足 1 秒，得到 %s", resp.GetPolicyVersion())
	}
}
