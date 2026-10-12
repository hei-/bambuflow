package writer

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
)

// 角色与参数在读库之前判定（Pool 为 nil 也不出错）。
func TestRebuildAdminRejectsBeforeTouchingStore(t *testing.T) {
	a := &RebuildAdmin{Log: quiet, Now: time.Now}
	auditor := as(t, &identityv1.Principal{Sub: "audra", Roles: []string{"auditor"}})
	if _, err := a.StartRebuild(auditor, &adminv1.StartRebuildRequest{Reason: "x"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能申请: %v", err)
	}
	if _, err := a.ApproveRebuild(auditor, &adminv1.ApproveRebuildRequest{OperationId: "op_1"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能复核: %v", err)
	}
	if _, err := a.GetRebuild(auditor, &adminv1.GetRebuildRequest{RebuildId: "rb_1"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能看: %v", err)
	}
	ops := as(t, &identityv1.Principal{Sub: "olga", Roles: []string{RoleOperator}})
	long := string(make([]byte, 129))
	for name, r := range map[string]*adminv1.StartRebuildRequest{
		"缺原因":   {EmbeddingModel: "m2"},
		"模型名太长": {EmbeddingModel: long, Reason: "x"},
	} {
		if _, err := a.StartRebuild(ops, r); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s 应为参数错误: %v", name, err)
		}
	}
	if _, err := a.CancelRebuild(ops, &adminv1.CancelRebuildRequest{RebuildId: "rb_1"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("取消要写原因: %v", err)
	}
	if _, err := a.GetRebuild(ops, &adminv1.GetRebuildRequest{RebuildId: "x"}); status.Code(err) != codes.NotFound {
		t.Fatalf("ID 格式不对按不存在: %v", err)
	}
}

func TestRebuildProto(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	op := rebuildOp{ID: "op_1", Params: rebuildParams{Model: "m2", Chunker: "c1"}, Live: "ix-1", Status: "executed", RebuildID: "rb_1",
		RequestedAt: now, ExpiresAt: now.Add(operationTTL), ApprovedAt: &now, ApprovedBy: "oscar"}
	if p := op.proto(); p.GetEmbeddingModel() != "m2" || p.GetRebuildId() != "rb_1" || p.GetApprovedAt() == nil ||
		p.GetStatus() != adminv1.OperationStatus_OPERATION_STATUS_EXECUTED {
		t.Fatalf("%v", p)
	}
	b := rebuild{ID: "rb_1", Version: "ix-2", Status: "running", Written: 3, Total: 9, StartedAt: now}
	if p := b.proto(); p.GetWrittenDocs() != 3 || p.GetTotalDocs() != 9 || p.GetFinishedAt() != nil {
		t.Fatalf("%v", p)
	}
}
