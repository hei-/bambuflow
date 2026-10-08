package writer

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
)

var quiet = logging.New(&bytes.Buffer{}, "t", slog.LevelInfo)

type staticVerifier struct{ p *identityv1.Principal }

func (v staticVerifier) Verify(context.Context, string) (*identityv1.Principal, error) {
	return v.p, nil
}

func as(t *testing.T, p *identityv1.Principal) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer x"))
	var out context.Context
	if _, err := grpcmw.UnaryServer(staticVerifier{p}, quiet)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/x/y"},
		func(ctx context.Context, _ any) (any, error) { out = ctx; return nil, nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

// 角色与参数在读库之前判定：不是平台运维、参数不合法的请求不会碰到数据库（Pool 为 nil 也不出错）。
func TestVersionAdminRejectsBeforeTouchingStore(t *testing.T) {
	a := &VersionAdmin{Log: quiet, Now: time.Now}
	auditor := as(t, &identityv1.Principal{Sub: "audra", Roles: []string{"auditor"}})
	if _, err := a.ListIndexVersions(auditor, &adminv1.ListIndexVersionsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能看索引版本: %v", err)
	}
	if _, err := a.ActivateIndexVersion(auditor, &adminv1.ActivateIndexVersionRequest{Version: "ix-1", Reason: "x"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能切换: %v", err)
	}
	if _, err := a.ApproveOperation(auditor, &adminv1.ApproveOperationRequest{OperationId: "op_1"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能复核: %v", err)
	}
	ops := as(t, &identityv1.Principal{Sub: "olga", Roles: []string{RoleOperator}})
	for _, r := range []*adminv1.ActivateIndexVersionRequest{{Version: "ix-1"}, {Reason: "回滚"}, {Version: "ix-1", Reason: "  "}} {
		if _, err := a.ActivateIndexVersion(ops, r); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%v 应为参数错误: %v", r, err)
		}
	}
	for _, r := range []*adminv1.ListIndexVersionsRequest{{PageSize: 201}, {PageToken: "%%"}, {PageToken: "eDox"}} {
		if _, err := a.ListIndexVersions(ops, r); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%v 应为参数错误: %v", r, err)
		}
	}
}

func TestPageToken(t *testing.T) {
	n, off, err := page(0, pageToken(100))
	if err != nil || n != 50 || off != 100 {
		t.Fatalf("分页参数: %d %d %v", n, off, err)
	}
}

func TestOperationProto(t *testing.T) {
	now := time.Date(2026, 10, 7, 1, 0, 0, 0, time.UTC)
	o := operation{ID: "op_1", Action: actionActivate, Target: "ix-1", Live: "ix-2", RequestedBy: "olga", RequestedAt: now,
		Status: "executed", ApprovedBy: "oscar", ApprovedAt: &now, ExpiresAt: now.Add(operationTTL)}
	p := o.proto()
	if p.GetStatus() != adminv1.OperationStatus_OPERATION_STATUS_EXECUTED || p.GetApprovedAt() == nil ||
		!p.GetExpiresAt().AsTime().Equal(now.Add(24*time.Hour)) || p.GetLiveVersion() != "ix-2" {
		t.Fatalf("转换不对: %v", p)
	}
	o.Status, o.ApprovedBy, o.ApprovedAt = "pending", "", nil
	if p := o.proto(); p.GetApprovedAt() != nil || p.GetStatus() != adminv1.OperationStatus_OPERATION_STATUS_PENDING {
		t.Fatalf("待复核的操作没有复核人: %v", p)
	}
}
