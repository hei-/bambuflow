package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
)

type fakeConfigs struct {
	adminv1.ConfigAdminServiceClient
	chg *adminv1.CreateChangeRequestRequest
	rel *adminv1.CreateReleaseRequest
}

func (f *fakeConfigs) ListTenants(context.Context, *adminv1.ListTenantsRequest, ...grpc.CallOption) (*adminv1.ListTenantsResponse, error) {
	return &adminv1.ListTenantsResponse{Tenants: []*adminv1.TenantSummary{{Tenant: "credit", Rollout: "general",
		Active: &adminv1.ConfigVersion{VersionId: "cfv_1", Number: 3, Active: true}}}}, nil
}

func (f *fakeConfigs) CreateChangeRequest(_ context.Context, in *adminv1.CreateChangeRequestRequest, _ ...grpc.CallOption) (*adminv1.CreateChangeRequestResponse, error) {
	f.chg = in
	return &adminv1.CreateChangeRequestResponse{ChangeRequest: &adminv1.ChangeRequest{ChgId: "chg_000000000000000000000001", Scope: in.GetScope(),
		Status: "open", Version: &adminv1.ConfigVersion{VersionId: "cfv_2"}}}, nil
}

func (f *fakeConfigs) ApproveChangeRequest(context.Context, *adminv1.ApproveChangeRequestRequest, ...grpc.CallOption) (*adminv1.ApproveChangeRequestResponse, error) {
	return nil, grpcmw.Reason(codes.FailedPrecondition, "stale_base_version", "生效版本已经变了")
}

func (f *fakeConfigs) CreateRelease(_ context.Context, in *adminv1.CreateReleaseRequest, _ ...grpc.CallOption) (*adminv1.CreateReleaseResponse, error) {
	f.rel = in
	return &adminv1.CreateReleaseResponse{Release: &adminv1.Release{ReleaseId: "rel_000000000000000000000001", Status: "pending_approval"}}, nil
}

func (f *fakeConfigs) RollbackRelease(_ context.Context, in *adminv1.RollbackReleaseRequest, _ ...grpc.CallOption) (*adminv1.RollbackReleaseResponse, error) {
	return &adminv1.RollbackReleaseResponse{Release: &adminv1.Release{ReleaseId: "rel_000000000000000000000002", Source: "rollback",
		SourceRef: in.GetReleaseId(), Status: "active"}}, nil
}

func TestConfigRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeConfigs{}
	e.server.Configs = f
	paul := e.authz.token(t, "paul", "console")
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }

	r := e.request(t, http.MethodGet, "/v1/admin/tenants", paul, "", nil)
	if r.status != 200 || r.body["tenants"].([]any)[0].(map[string]any)["rollout"] != "general" {
		t.Fatalf("租户列表: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/tenants", paul, `{"tenant":"newbiz","content":"tenant: newbiz\nrollout: pilot\n","reason":"新业务线"}`, key("a"))
	if r.status != 201 || f.chg.GetScope() != "newbiz" || f.chg.GetBaseVersionId() != "" || f.chg.GetObject() != "tenant" {
		t.Fatalf("新建租户: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/change-requests", paul,
		`{"object":"tenant","scope":"credit","base_version_id":"cfv_1","content":"x","reason":"改配额","gate_id":""}`, key("b"))
	if r.status != 201 || f.chg.GetBaseVersionId() != "cfv_1" {
		t.Fatalf("变更单: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/change-requests/chg_000000000000000000000001/approve", paul, "", key("c"))
	if r.status != 409 || r.body["type"] != "stale_base_version" {
		t.Fatalf("过期的变更单: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/releases", paul,
		`{"object":"tenant","scope":"credit","content":"x","source_ref":"git:3f2a9c1","reason":"合并后发布"}`, key("d"))
	if r.status != 202 || f.rel.GetSourceRef() != "git:3f2a9c1" {
		t.Fatalf("发布: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/releases/rel_000000000000000000000001/rollback", paul, `{"reason":"配错了"}`, key("e"))
	if r.status != 200 || r.body["source"] != "rollback" {
		t.Fatalf("回退: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/tenants", e.authz.token(t, "olga", "console"), "", nil); r.status != 403 {
		t.Fatalf("平台运维不能看: %d", r.status)
	}
}
