package api

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
)

type fakeSources struct {
	adminv1.SourceAdminServiceClient
	registered *adminv1.RegisterSourceRequest
	deleted    *adminv1.DeleteGrantRequest
}

func (f *fakeSources) RegisterSource(_ context.Context, in *adminv1.RegisterSourceRequest, _ ...grpc.CallOption) (*adminv1.RegisterSourceResponse, error) {
	f.registered = in
	return &adminv1.RegisterSourceResponse{Source: &adminv1.Source{SourceId: "src_1", Type: in.GetType(), OwnerDept: in.GetOwnerDept(),
		DefaultClassification: in.GetDefaultClassification(), Status: adminv1.SourceStatus_SOURCE_STATUS_PENDING, CreatedBy: "paul", CreatedAt: t0}}, nil
}

func (f *fakeSources) ApproveSource(_ context.Context, in *adminv1.ApproveSourceRequest, _ ...grpc.CallOption) (*adminv1.ApproveSourceResponse, error) {
	return nil, grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "x")
}

func (f *fakeSources) RevokeSource(_ context.Context, in *adminv1.RevokeSourceRequest, _ ...grpc.CallOption) (*adminv1.RevokeSourceResponse, error) {
	return &adminv1.RevokeSourceResponse{DenialId: "den_9", Source: &adminv1.Source{SourceId: in.GetSourceId(),
		Status: adminv1.SourceStatus_SOURCE_STATUS_REVOKED, RevokedBy: "dana", RevokedAt: t0, CreatedAt: t0}}, nil
}

func (f *fakeSources) CreateGrant(_ context.Context, in *adminv1.CreateGrantRequest, _ ...grpc.CallOption) (*adminv1.CreateGrantResponse, error) {
	return &adminv1.CreateGrantResponse{Created: true, Grant: &adminv1.Grant{SourceId: in.GetSourceId(), Tenant: in.GetTenant(), GrantedBy: "dana", GrantedAt: t0}}, nil
}

func (f *fakeSources) DeleteGrant(_ context.Context, in *adminv1.DeleteGrantRequest, _ ...grpc.CallOption) (*adminv1.DeleteGrantResponse, error) {
	f.deleted = in
	return &adminv1.DeleteGrantResponse{}, nil
}

func (f *fakeSources) ListGrants(_ context.Context, in *adminv1.ListGrantsRequest, _ ...grpc.CallOption) (*adminv1.ListGrantsResponse, error) {
	return &adminv1.ListGrantsResponse{}, nil
}

func TestSourceRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeSources{}
	e.server.Sources = f
	paul, dana := e.authz.token(t, "paul", "console"), e.authz.token(t, "dana", "console")
	n := 0
	key := func() map[string]string { n++; return map[string]string{"Idempotency-Key": fmt.Sprintf("k-%d", n)} }

	r := e.request(t, http.MethodPost, "/v1/admin/sources", paul, `{"type":"wiki","owner_dept":"信贷部","default_classification":"internal"}`, key())
	if r.status != 201 || r.body["status"] != "pending" || r.body["type"] != "wiki" || r.body["default_classification"] != "internal" ||
		f.registered.GetType() != commonv1.SourceType_SOURCE_TYPE_WIKI {
		t.Fatalf("注册: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/sources", paul, `{"type":"ftp","owner_dept":"x","default_classification":"internal"}`, key()); r.status != 400 {
		t.Fatalf("未知类型应 400: %d", r.status)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/sources", dana, `{"type":"wiki","owner_dept":"x","default_classification":"internal"}`, key()); r.status != 403 {
		t.Fatalf("数据属主不能注册: %d", r.status)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/sources/src_1/approve", dana, `{"basis":"x"}`, key()); r.status != 403 || r.body["type"] != "self_approval_forbidden" {
		t.Fatalf("自批: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/sources/src_1/approve", paul, `{"basis":"x"}`, key()); r.status != 403 {
		t.Fatalf("平台管理员不能审批: %d", r.status)
	}
	rv := e.request(t, http.MethodPost, "/v1/admin/sources/src_1/revoke", dana, `{"reason":"下线"}`, key())
	if rv.status != 200 || rv.body["denial_id"] != "den_9" || rv.body["status"] != "revoked" {
		t.Fatalf("撤销: %d %s", rv.status, rv.raw)
	}
	g := e.request(t, http.MethodPost, "/v1/admin/sources/src_1/grants", dana, `{"tenant":"credit"}`, key())
	if g.status != 201 || g.body["tenant"] != "credit" {
		t.Fatalf("授权: %d %s", g.status, g.raw)
	}
	d := e.request(t, http.MethodDelete, "/v1/admin/sources/src_1/grants?tenant=credit", dana, "", key())
	if d.status != 200 || f.deleted.GetTenant() != "credit" {
		t.Fatalf("取消授权: %d %s", d.status, d.raw)
	}
	if r := e.request(t, http.MethodDelete, "/v1/admin/sources/src_1/grants", dana, "", key()); r.status != 400 {
		t.Fatalf("取消授权缺租户应 400: %d", r.status)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/sources/bad-id/grants", dana, "", nil); r.status != 404 {
		t.Fatalf("非法的数据源 ID 应 404: %d", r.status)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/sources", dana, "", nil); r.status != 404 {
		t.Fatalf("数据源列表没有登记，应 404: %d", r.status)
	}
}
