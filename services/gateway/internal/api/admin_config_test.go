package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

type fakeConfigs struct {
	adminv1.ConfigAdminServiceClient
	chg *adminv1.CreateChangeRequestRequest
	rel *adminv1.CreateReleaseRequest
	// operator 是最近一次 ListTenants 收到的管理 Principal 的声明
	operator map[string]any
}

func (f *fakeConfigs) ListTenants(ctx context.Context, _ *adminv1.ListTenantsRequest, _ ...grpc.CallOption) (*adminv1.ListTenantsResponse, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	parts := strings.Split(strings.TrimPrefix(md.Get("authorization")[0], "Bearer "), ".")
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	f.operator = map[string]any{}
	_ = json.Unmarshal(raw, &f.operator)
	return &adminv1.ListTenantsResponse{Tenants: []*adminv1.TenantSummary{{Tenant: "credit", Rollout: "general",
		Active: &adminv1.ConfigVersion{VersionId: "cfv_1", Number: 3, Active: true}}}}, nil
}

func (f *fakeConfigs) ListChangeRequests(_ context.Context, in *adminv1.ListChangeRequestsRequest, _ ...grpc.CallOption) (*adminv1.ListChangeRequestsResponse, error) {
	if in.GetStatus() == "bogus" {
		return nil, status.Error(codes.InvalidArgument, "status 不合法")
	}
	return &adminv1.ListChangeRequestsResponse{ChangeRequests: []*adminv1.ChangeRequest{{ChgId: "chg_000000000000000000000001",
		Status: in.GetStatus(), Version: &adminv1.ConfigVersion{VersionId: "cfv_2"}}}}, nil
}

func (f *fakeConfigs) ListReleases(_ context.Context, in *adminv1.ListReleasesRequest, _ ...grpc.CallOption) (*adminv1.ListReleasesResponse, error) {
	return &adminv1.ListReleasesResponse{Releases: []*adminv1.Release{{ReleaseId: "rel_000000000000000000000001", Status: in.GetStatus()}}}, nil
}

func (f *fakeConfigs) CreateChangeRequest(_ context.Context, in *adminv1.CreateChangeRequestRequest, _ ...grpc.CallOption) (*adminv1.CreateChangeRequestResponse, error) {
	f.chg = in
	c := &adminv1.ChangeRequest{ChgId: "chg_000000000000000000000001", Scope: in.GetScope(), Status: "open",
		Version: &adminv1.ConfigVersion{VersionId: "cfv_2"}}
	if in.GetObject() == "pipeline" {
		c.Status, c.MergeRequestUrl = "merge_request", "https://git.corp/config/-/merge_requests/7"
	}
	return &adminv1.CreateChangeRequestResponse{ChangeRequest: c}, nil
}

func (f *fakeConfigs) ListConfigVersions(_ context.Context, in *adminv1.ListConfigVersionsRequest, _ ...grpc.CallOption) (*adminv1.ListConfigVersionsResponse, error) {
	return &adminv1.ListConfigVersionsResponse{Versions: []*adminv1.ConfigVersion{{VersionId: "cfv_9", Object: in.GetObject(),
		Scope: in.GetScope(), Content: "deny_sources := set()", Active: true, Widened: []string{"租户 ops、密级 1：新增可检索数据源 mail"}}}}, nil
}

func (f *fakeConfigs) PromoteRelease(_ context.Context, in *adminv1.PromoteReleaseRequest, _ ...grpc.CallOption) (*adminv1.PromoteReleaseResponse, error) {
	return &adminv1.PromoteReleaseResponse{Release: &adminv1.Release{ReleaseId: in.GetReleaseId(), Status: "active"}}, nil
}

func (f *fakeConfigs) AbortRollout(_ context.Context, in *adminv1.AbortRolloutRequest, _ ...grpc.CallOption) (*adminv1.AbortRolloutResponse, error) {
	if in.GetReason() == "" {
		return nil, status.Error(codes.InvalidArgument, "reason 必填")
	}
	return &adminv1.AbortRolloutResponse{Release: &adminv1.Release{ReleaseId: in.GetReleaseId(), Status: "rolled_back",
		Rollout: &adminv1.Rollout{Tenant: "ops", Percent: 100}}}, nil
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
	r = e.request(t, http.MethodGet, "/v1/admin/change-requests?status=open", paul, "", nil)
	if r.status != 200 || r.body["change_requests"].([]any)[0].(map[string]any)["status"] != "open" {
		t.Fatalf("变更单列表: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/change-requests?status=bogus", paul, "", nil); r.status != 400 {
		t.Fatalf("不合法的状态: %d", r.status)
	}
	r = e.request(t, http.MethodGet, "/v1/admin/releases?status=pending_approval", paul, "", nil)
	if r.status != 200 || r.body["releases"].([]any)[0].(map[string]any)["status"] != "pending_approval" {
		t.Fatalf("发布列表: %d %s", r.status, r.raw)
	}
	// 租户负责人：管理 Principal 带 tenant_owner:<租户>、不带 tenant；只读，不能提交变更
	owen := e.authz.token(t, "owen", "console")
	if r := e.request(t, http.MethodGet, "/v1/admin/tenants", owen, "", nil); r.status != 200 ||
		fmt.Sprint(f.operator["roles"]) != "[tenant_owner:credit]" || f.operator["tenant"] != nil {
		t.Fatalf("租户负责人: %d %v", r.status, f.operator)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/change-requests", owen, `{"object":"tenant"}`, key("o")); r.status != 403 {
		t.Fatalf("租户负责人不能提交变更单: %d", r.status)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/tenants", e.authz.token(t, "olga", "console"), "", nil); r.status != 403 {
		t.Fatalf("平台运维不能看: %d", r.status)
	}
}

func TestGitObjectsGoThroughMergeRequests(t *testing.T) {
	e := newEnv(t)
	e.server.Configs = &fakeConfigs{}
	paul := e.authz.token(t, "paul", "console")
	r := e.request(t, http.MethodGet, "/v1/admin/config-versions?object=policy&scope=tenant:ops", paul, "", nil)
	vs, _ := r.body["versions"].([]any)
	if r.status != 200 || len(vs) != 1 || vs[0].(map[string]any)["widened"] == nil {
		t.Fatalf("按对象查版本: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/config-versions?object=secrets&scope=x", paul, "", nil); r.status != 400 {
		t.Fatalf("未知对象应为 400: %d", r.status)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/change-requests", paul,
		`{"object":"pipeline","scope":"ops","base_version_id":"","content":"answer: {}","reason":"调阈值","gate_id":""}`,
		map[string]string{"Idempotency-Key": "mr-1"})
	if r.status != 201 || r.body["status"] != "merge_request" || r.body["merge_request_url"] != "https://git.corp/config/-/merge_requests/7" {
		t.Fatalf("合并请求: %d %s", r.status, r.raw)
	}
}

func TestRolloutRoutes(t *testing.T) {
	e := newEnv(t)
	e.server.Configs = &fakeConfigs{}
	paul := e.authz.token(t, "paul", "console")
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }
	r := e.request(t, http.MethodPost, "/v1/admin/releases/rel_000000000000000000000001/promote", paul, "", key("p"))
	if r.status != 200 || r.body["status"] != "active" {
		t.Fatalf("全量: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/releases/rel_000000000000000000000001/abort", paul, `{"reason":"拒答率升高"}`, key("a"))
	if ro, _ := r.body["rollout"].(map[string]any); r.status != 200 || r.body["status"] != "rolled_back" || ro["tenant"] != "ops" {
		t.Fatalf("终止灰度: %d %s", r.status, r.raw)
	}
}
