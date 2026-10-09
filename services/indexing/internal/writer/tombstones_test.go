package writer

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
)

// 角色与参数在读库之前判定（Pool 为 nil 也不出错）。
func TestTombstoneAdminRejectsBeforeTouchingStore(t *testing.T) {
	a := &TombstoneAdmin{Log: quiet, Now: time.Now}
	auditor := as(t, &identityv1.Principal{Sub: "audra", Roles: []string{"auditor"}})
	if _, err := a.RevokeTombstones(auditor, &adminv1.RevokeTombstonesRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能撤销: %v", err)
	}
	if _, err := a.ApproveRevocation(auditor, &adminv1.ApproveRevocationRequest{OperationId: "op_1"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能复核: %v", err)
	}
	ops := as(t, &identityv1.Principal{Sub: "olga", Roles: []string{RoleOperator}})
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	ok := func() *adminv1.RevokeTombstonesRequest {
		return &adminv1.RevokeTombstonesRequest{SourceId: "src_wiki", TombstonedSince: timestamppb.New(day),
			TombstonedUntil: timestamppb.New(day.Add(24 * time.Hour)), Reason: "清单故障误删"}
	}
	many := make([]string, 1001)
	for i := range many {
		many[i] = "d"
	}
	bad := map[string]func(*adminv1.RevokeTombstonesRequest){
		"缺数据源":    func(r *adminv1.RevokeTombstonesRequest) { r.SourceId = "" },
		"数据源没有前缀": func(r *adminv1.RevokeTombstonesRequest) { r.SourceId = "wiki" },
		"缺原因":     func(r *adminv1.RevokeTombstonesRequest) { r.Reason = " " },
		"缺时间段":    func(r *adminv1.RevokeTombstonesRequest) { r.TombstonedUntil = nil },
		"时间段颠倒": func(r *adminv1.RevokeTombstonesRequest) {
			r.TombstonedSince, r.TombstonedUntil = r.TombstonedUntil, r.TombstonedSince
		},
		"跨度超过31天": func(r *adminv1.RevokeTombstonesRequest) {
			r.TombstonedUntil = timestamppb.New(day.Add(32 * 24 * time.Hour))
		},
		"doc_ids 太多": func(r *adminv1.RevokeTombstonesRequest) { r.DocIds = many },
		"doc_id 为空":  func(r *adminv1.RevokeTombstonesRequest) { r.DocIds = []string{""} },
	}
	for name, mut := range bad {
		r := ok()
		mut(r)
		if _, err := a.RevokeTombstones(ops, r); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s 应为参数错误: %v", name, err)
		}
	}
	p, reason, err := validateRevoke(ok())
	if err != nil || reason != "清单故障误删" || p.DocIDs == nil || !p.Until.Equal(day.Add(24*time.Hour)) {
		t.Fatalf("合法申请: %+v %q %v", p, reason, err)
	}
}

func TestRevocationProto(t *testing.T) {
	now := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	rv := revocation{ID: "op_1", Source: "src_wiki", Params: revokeParams{Since: now.Add(-time.Hour), Until: now, DocIDs: []string{"a"}},
		Reason: "误删", Matched: 3, Revoked: 2, RequestedBy: "olga", RequestedAt: now, Status: "executed", ApprovedBy: "oscar",
		ApprovedAt: &now, ExpiresAt: now.Add(operationTTL), RefetchEventID: "evt_1"}
	p := rv.proto()
	if p.GetStatus() != adminv1.OperationStatus_OPERATION_STATUS_EXECUTED || p.GetRevokedDocs() != 2 || p.GetMatchedDocs() != 3 ||
		p.GetApprovedAt() == nil || p.GetRefetchEventId() != "evt_1" || p.GetDocIds()[0] != "a" {
		t.Fatalf("%v", p)
	}
	if id := newEventID(); len(id) != len("evt_")+24 || id[:4] != "evt_" {
		t.Fatalf("event_id %q", id)
	}
}
