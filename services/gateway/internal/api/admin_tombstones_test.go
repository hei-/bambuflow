package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

type fakeTombstones struct {
	adminv1.TombstoneAdminServiceClient
	revoke  *adminv1.RevokeTombstonesRequest
	approve string
}

func (f *fakeTombstones) RevokeTombstones(_ context.Context, in *adminv1.RevokeTombstonesRequest, _ ...grpc.CallOption) (*adminv1.RevokeTombstonesResponse, error) {
	f.revoke = in
	return &adminv1.RevokeTombstonesResponse{Created: true, Revocation: &adminv1.TombstoneRevocation{OperationId: "op_ab", SourceId: in.GetSourceId(),
		TombstonedSince: in.GetTombstonedSince(), TombstonedUntil: in.GetTombstonedUntil(), MatchedDocs: 12, RequestedBy: "olga",
		RequestedAt: timestamppb.Now(), Status: adminv1.OperationStatus_OPERATION_STATUS_PENDING, ExpiresAt: timestamppb.Now()}}, nil
}

func (f *fakeTombstones) ApproveRevocation(_ context.Context, in *adminv1.ApproveRevocationRequest, _ ...grpc.CallOption) (*adminv1.ApproveRevocationResponse, error) {
	f.approve = in.GetOperationId()
	return &adminv1.ApproveRevocationResponse{Revocation: &adminv1.TombstoneRevocation{OperationId: in.GetOperationId(),
		Status: adminv1.OperationStatus_OPERATION_STATUS_EXECUTED, RevokedDocs: 11, RefetchEventId: "evt_1"}}, nil
}

func TestTombstoneRevocationRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeTombstones{}
	e.server.Tombstones = f
	olga, oscar := e.authz.token(t, "olga", "console"), e.authz.token(t, "oscar", "console")
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }

	body := `{"source_id":"src_wiki","tombstoned_since":"2026-10-01T00:00:00Z","tombstoned_until":"2026-10-02T00:00:00Z","reason":"清单故障误删"}`
	r := e.request(t, http.MethodPost, "/v1/admin/tombstone-revocations", olga, body, key("a"))
	if r.status != 202 || r.body["status"] != "pending" || r.body["matched_docs"] != float64(12) ||
		f.revoke.GetTombstonedUntil().AsTime().Day() != 2 || f.revoke.GetReason() != "清单故障误删" {
		t.Fatalf("申请: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/tombstone-revocations", olga,
		`{"source_id":"src_wiki","tombstoned_since":"昨天","tombstoned_until":"2026-10-02T00:00:00Z","reason":"x"}`, key("b")); r.status != 400 {
		t.Fatalf("时间格式不对应 400: %d", r.status)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/tombstone-revocations/operations/op_ab/approve", oscar, "", key("c"))
	if r.status != 200 || r.body["status"] != "executed" || r.body["refetch_event_id"] != "evt_1" || f.approve != "op_ab" {
		t.Fatalf("复核: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/tombstone-revocations/operations/bad/approve", oscar, "", key("d")); r.status != 404 {
		t.Fatalf("操作 ID 格式不对应 404: %d", r.status)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/tombstone-revocations", e.authz.token(t, "dana", "console"), body, key("e")); r.status != 403 {
		t.Fatalf("数据属主不是平台运维，应 403: %d", r.status)
	}
}
