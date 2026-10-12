package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

type fakeConnectors struct {
	adminv1.ConnectorAdminServiceClient
	pause   *adminv1.PauseSourceRequest
	confirm *adminv1.ConfirmDeletionRequest
	syncNow *adminv1.SyncNowRequest
	syncErr error
}

func (f *fakeConnectors) SyncNow(_ context.Context, in *adminv1.SyncNowRequest, _ ...grpc.CallOption) (*adminv1.SyncNowResponse, error) {
	f.syncNow = in
	if f.syncErr != nil {
		return nil, f.syncErr
	}
	return &adminv1.SyncNowResponse{Job: "connectors-sync-wiki-manual-abc", RequestedAt: timestamppb.Now()}, nil
}

func (f *fakeConnectors) ListPendingDeletions(_ context.Context, in *adminv1.ListPendingDeletionsRequest, _ ...grpc.CallOption) (*adminv1.ListPendingDeletionsResponse, error) {
	return &adminv1.ListPendingDeletionsResponse{Holds: []*adminv1.DeletionHold{{HoldId: "hold_ab", SourceId: in.GetSourceId(),
		DocIds: []string{"d1"}, Status: "pending", DetectedAt: timestamppb.Now(), UpdatedAt: timestamppb.Now()}}}, nil
}

func (f *fakeConnectors) ConfirmDeletion(_ context.Context, in *adminv1.ConfirmDeletionRequest, _ ...grpc.CallOption) (*adminv1.ConfirmDeletionResponse, error) {
	f.confirm = in
	return &adminv1.ConfirmDeletionResponse{Hold: &adminv1.DeletionHold{HoldId: in.GetHoldId(), Status: "awaiting_second", ConfirmedBy: "dana"}}, nil
}

func (f *fakeConnectors) GetSyncStatus(_ context.Context, in *adminv1.GetSyncStatusRequest, _ ...grpc.CallOption) (*adminv1.GetSyncStatusResponse, error) {
	return &adminv1.GetSyncStatusResponse{SourceId: in.GetSourceId(), FreshnessSeconds: 90,
		LastRun: &adminv1.SyncRun{Command: "sync-wiki", Result: "succeeded", Upserts: 2, StartedAt: timestamppb.Now(), FinishedAt: timestamppb.Now()},
		Pause:   &adminv1.Pause{PausedBy: "olga", Reason: "维护", PausedAt: timestamppb.Now()},
		LastReconcile: &adminv1.SyncRun{Command: "reconcile-wiki", Result: "succeeded", StartedAt: timestamppb.Now(), FinishedAt: timestamppb.Now(),
			Reconciliation: &adminv1.Reconciliation{Missing: 3, Indexed: 40}}}, nil
}

func (f *fakeConnectors) PauseSource(_ context.Context, in *adminv1.PauseSourceRequest, _ ...grpc.CallOption) (*adminv1.PauseSourceResponse, error) {
	f.pause = in
	return &adminv1.PauseSourceResponse{Changed: true, Pause: &adminv1.Pause{PausedBy: "olga", Reason: in.GetReason(), PausedAt: timestamppb.Now()}}, nil
}

func (f *fakeConnectors) ResumeSource(context.Context, *adminv1.ResumeSourceRequest, ...grpc.CallOption) (*adminv1.ResumeSourceResponse, error) {
	return &adminv1.ResumeSourceResponse{Changed: true}, nil
}

func TestConnectorAdminRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeConnectors{}
	e.server.Connectors = f
	olga, dana := e.authz.token(t, "olga", "console"), e.authz.token(t, "dana", "console")
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }

	r := e.request(t, http.MethodGet, "/v1/admin/sources/src_wiki/sync-status", dana, "", nil)
	if r.status != 200 || r.body["freshness_seconds"] != float64(90) || r.body["last_run"].(map[string]any)["upserts"] != float64(2) ||
		r.body["pause"].(map[string]any)["reason"] != "维护" ||
		r.body["last_reconcile"].(map[string]any)["reconciliation"].(map[string]any)["missing"] != float64(3) {
		t.Fatalf("数据属主看同步状态: %d %s", r.status, r.raw)
	}
	if _, ok := r.body["last_run"].(map[string]any)["reconciliation"]; ok {
		t.Fatalf("不是对账的同步不带对账结果: %s", r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/sources/src_wiki/sync", dana, `{"reason":"x"}`, key("s1")); r.status != 403 {
		t.Fatalf("数据属主不能立即同步: %d", r.status)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/sources/src_wiki/sync", olga, `{"reason":"源端刚修复"}`, key("s2"))
	if r.status != 200 || r.body["job"] != "connectors-sync-wiki-manual-abc" || f.syncNow.GetReason() != "源端刚修复" {
		t.Fatalf("立即同步: %d %s", r.status, r.raw)
	}
	f.syncErr = grpcmw.Reason(codes.FailedPrecondition, "scheduler_unavailable", "没有接入调度")
	if r := e.request(t, http.MethodPost, "/v1/admin/sources/src_wiki/sync", olga, `{"reason":"x"}`, key("s3")); r.status != 422 ||
		r.body["type"] != "scheduler_unavailable" {
		t.Fatalf("没有接入调度: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/sources/wiki/sync-status", olga, "", nil); r.status != 404 {
		t.Fatalf("数据源 ID 格式不对应 404: %d", r.status)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/sources/src_wiki/pause", dana, `{"reason":"x"}`, key("a")); r.status != 403 {
		t.Fatalf("数据属主不能暂停: %d", r.status)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/sources/src_wiki/pause", olga, `{"reason":"源系统维护"}`, key("b"))
	if r.status != 200 || r.body["changed"] != true || f.pause.GetReason() != "源系统维护" || f.pause.GetSourceId() != "src_wiki" {
		t.Fatalf("暂停: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/sources/src_wiki/resume", olga, "", key("c")); r.status != 200 || r.body["changed"] != true {
		t.Fatalf("恢复: %d %s", r.status, r.raw)
	}

	r = e.request(t, http.MethodGet, "/v1/admin/sources/src_wiki/pending-deletions", olga, "", nil)
	if r.status != 200 || len(r.body["holds"].([]any)) != 1 {
		t.Fatalf("待删批次: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/sources/src_wiki/pending-deletions/hold_ab/confirm", olga, `{"reason":"x"}`, key("d")); r.status != 403 {
		t.Fatalf("平台运维不能确认: %d", r.status)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/sources/src_wiki/pending-deletions/hold_ab/confirm", dana, `{"reason":"源端下线"}`, key("e"))
	if r.status != 200 || r.body["status"] != "awaiting_second" || f.confirm.GetHoldId() != "hold_ab" || f.confirm.GetReason() != "源端下线" {
		t.Fatalf("确认: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/sources/src_wiki/pending-deletions/x/confirm", dana, `{"reason":"x"}`, key("f")); r.status != 404 {
		t.Fatalf("批次 ID 格式不对应 404: %d", r.status)
	}
}
