package grpcmw

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
)

func TestUnaryServerForOnlyChecksListedServices(t *testing.T) {
	icpt := UnaryServerFor([]string{"bambuflow.admin.v1.SourceAdminService"}, fakeVerifier{}, logging.New(&bytes.Buffer{}, "t", slog.LevelInfo))
	ok := func(ctx context.Context, _ any) (any, error) { return PrincipalFrom(ctx).GetSub(), nil }
	ctx, cancel := context.WithTimeout(context.Background(), 1e9)
	defer cancel()
	// 不在名单里的服务：不要求 Principal
	if out, err := icpt(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/bambuflow.config.v1.ConfigService/GetSnapshot"}, ok); err != nil || out != "" {
		t.Fatalf("应原样放行: %v %v", out, err)
	}
	// 名单里的服务：没有 Principal 就拒绝，前缀相同的其他服务名不算
	if _, err := icpt(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/bambuflow.admin.v1.SourceAdminService/RegisterSource"}, ok); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("应要求 Principal，得到 %v", err)
	}
	withP := metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer good", TraceIDKey, "t-1"))
	out, err := icpt(withP, nil, &grpc.UnaryServerInfo{FullMethod: "/bambuflow.admin.v1.SourceAdminService/RegisterSource"}, ok)
	if err != nil || out != "alice" || TraceID(withP) != "t-1" {
		t.Fatalf("应通过: %v %v", out, err)
	}
}

func TestReasonRoundTrip(t *testing.T) {
	err := Reason(codes.PermissionDenied, "self_approval_forbidden", "不能审批自己发起的操作")
	if status.Code(err) != codes.PermissionDenied || ReasonOf(err) != "self_approval_forbidden" {
		t.Fatalf("错误码没有带上: %v", err)
	}
	if ReasonOf(status.Error(codes.Internal, "x")) != "" {
		t.Fatal("没有业务错误码时应为空")
	}
	if !HasRole(&identityv1.Principal{Roles: []string{"auditor"}}, "infosec", "auditor") || HasRole(&identityv1.Principal{}, "auditor") {
		t.Fatal("HasRole 判断不对")
	}
}
