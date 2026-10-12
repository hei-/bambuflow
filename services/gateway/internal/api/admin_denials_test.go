package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// fakeDenials 是 governance 的紧急拒绝接口：记下收到的请求，同一对象第二次登记返回已有的规则。
type fakeDenials struct {
	adminv1.EmergencyDenialAdminServiceClient
	created *adminv1.CreateEmergencyDenialRequest
	auth    string
	seen    map[string]bool
}

func (f *fakeDenials) CreateEmergencyDenial(ctx context.Context, in *adminv1.CreateEmergencyDenialRequest, _ ...grpc.CallOption) (*adminv1.CreateEmergencyDenialResponse, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	f.created, f.auth = in, md.Get("authorization")[0]
	key := in.GetScope().String() + in.GetTarget()
	created := !f.seen[key]
	f.seen[key] = true
	return &adminv1.CreateEmergencyDenialResponse{Created: created, Denial: &adminv1.EmergencyDenial{
		DenialId: "den_1", Scope: in.GetScope(), Target: in.GetTarget(), Reason: in.GetReason(), CreatedBy: "ivan",
		CreatedAt: timestamppb.New(time.Date(2026, 10, 7, 1, 2, 3, 0, time.UTC)), Status: adminv1.DenialStatus_DENIAL_STATUS_PENDING,
	}}, nil
}

func (f *fakeDenials) ListEmergencyDenials(_ context.Context, in *adminv1.ListEmergencyDenialsRequest, _ ...grpc.CallOption) (*adminv1.ListEmergencyDenialsResponse, error) {
	return &adminv1.ListEmergencyDenialsResponse{NextPageToken: "next-" + in.GetPageToken(), Denials: []*adminv1.EmergencyDenial{{
		DenialId: "den_1", Scope: adminv1.DenialScope_DENIAL_SCOPE_SOURCE, Target: "src_x", Status: adminv1.DenialStatus_DENIAL_STATUS_ACTIVE,
		CreatedAt: timestamppb.New(time.Date(2026, 10, 7, 1, 2, 3, 0, time.UTC)), AppliedAt: timestamppb.New(time.Date(2026, 10, 7, 1, 2, 4, 0, time.UTC)),
	}}}, nil
}

func TestEmergencyDenialRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeDenials{seen: map[string]bool{}}
	e.server.Denials = f
	ivan := e.authz.token(t, "ivan", "console")
	body := `{"scope":"document","target":"doc-1","reason":"工单 SEC-1"}`
	r := e.request(t, http.MethodPost, "/v1/admin/emergency-denials", ivan, body, map[string]string{"Idempotency-Key": "a"})
	if r.status != 201 || r.body["scope"] != "document" || r.body["status"] != "pending" || r.body["created_at"] != "2026-10-07T01:02:03Z" ||
		f.created.GetScope() != adminv1.DenialScope_DENIAL_SCOPE_DOCUMENT || f.auth == "" {
		t.Fatalf("登记: %d %s", r.status, r.raw)
	}
	if _, ok := r.body["applied_at"]; ok {
		t.Fatal("未生效的规则不应有 applied_at")
	}
	// 同一对象再登记（换一个幂等键）：返回已有的，200
	if r := e.request(t, http.MethodPost, "/v1/admin/emergency-denials", ivan, body, map[string]string{"Idempotency-Key": "b"}); r.status != 200 {
		t.Fatalf("已有规则应返回 200: %d", r.status)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/emergency-denials", ivan, `{"scope":"everything","target":"x","reason":"y"}`,
		map[string]string{"Idempotency-Key": "c"}); r.status != 400 {
		t.Fatalf("未知范围应 400: %d", r.status)
	}
	// 平台运维没有这个权限；没有删除接口
	if r := e.request(t, http.MethodPost, "/v1/admin/emergency-denials", e.authz.token(t, "olga", "console"), body,
		map[string]string{"Idempotency-Key": "d"}); r.status != 403 {
		t.Fatalf("平台运维应 403: %d", r.status)
	}
	if r := e.request(t, http.MethodDelete, "/v1/admin/emergency-denials/den_1", ivan, "", nil); r.status != 404 {
		t.Fatalf("不提供删除: %d", r.status)
	}
	l := e.request(t, http.MethodGet, "/v1/admin/emergency-denials?page_token=p1&page_size=10", e.authz.token(t, "dana", "console"), "", nil)
	ds, _ := l.body["denials"].([]any)
	if l.status != 200 || len(ds) != 1 || l.body["next_page_token"] != "next-p1" || ds[0].(map[string]any)["applied_at"] != "2026-10-07T01:02:04Z" {
		t.Fatalf("列表: %d %s", l.status, l.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/emergency-denials?page_size=0", ivan, "", nil); r.status != 400 {
		t.Fatalf("page_size 越界应 400: %d", r.status)
	}
}
