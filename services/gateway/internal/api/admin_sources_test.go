package api

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

type fakeSources struct {
	adminv1.SourceAdminServiceClient
	registered *adminv1.RegisterSourceRequest
	deleted    *adminv1.DeleteGrantRequest
	approved   *adminv1.ApproveSourceRequest
	listed     *adminv1.ListSourcesRequest
	applied    *adminv1.ApplyGrantRequest
}

func (f *fakeSources) ListSources(_ context.Context, in *adminv1.ListSourcesRequest, _ ...grpc.CallOption) (*adminv1.ListSourcesResponse, error) {
	f.listed = in
	return &adminv1.ListSourcesResponse{Sources: []*adminv1.Source{{SourceId: "src_1", Status: adminv1.SourceStatus_SOURCE_STATUS_ACTIVE,
		DefaultClassification: commonv1.Classification_CLASSIFICATION_RESTRICTED, CreatedAt: t0, ApprovedBy: "dana", ApprovedAt: t0,
		SecurityApprovedBy: "ivan", SecurityApprovedAt: t0, SecurityApprovalBasis: "SEC-3"}}}, nil
}

func (f *fakeSources) RegisterSource(_ context.Context, in *adminv1.RegisterSourceRequest, _ ...grpc.CallOption) (*adminv1.RegisterSourceResponse, error) {
	f.registered = in
	return &adminv1.RegisterSourceResponse{Source: &adminv1.Source{SourceId: "src_1", Type: in.GetType(), OwnerDept: in.GetOwnerDept(),
		DefaultClassification: in.GetDefaultClassification(), Status: adminv1.SourceStatus_SOURCE_STATUS_PENDING, CreatedBy: "paul", CreatedAt: t0}}, nil
}

func (f *fakeSources) ApproveSource(_ context.Context, in *adminv1.ApproveSourceRequest, _ ...grpc.CallOption) (*adminv1.ApproveSourceResponse, error) {
	f.approved = in
	if in.GetCapacity() == adminv1.ApprovalCapacity_APPROVAL_CAPACITY_INFOSEC {
		return &adminv1.ApproveSourceResponse{Source: &adminv1.Source{SourceId: in.GetSourceId(), CreatedAt: t0,
			Status: adminv1.SourceStatus_SOURCE_STATUS_ACTIVE, SecurityApprovedBy: "ivan"}}, nil
	}
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

func (f *fakeSources) ApplyGrant(_ context.Context, in *adminv1.ApplyGrantRequest, _ ...grpc.CallOption) (*adminv1.ApplyGrantResponse, error) {
	f.applied = in
	return &adminv1.ApplyGrantResponse{Application: &adminv1.GrantApplication{ApplicationId: "gra_000000000000000000000001", SourceId: in.GetSourceId(),
		Tenant: in.GetTenant(), Reason: in.GetReason(), Status: adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_PENDING, AppliedBy: "owen", AppliedAt: t0}}, nil
}

func (f *fakeSources) ApproveGrantApplication(_ context.Context, in *adminv1.ApproveGrantApplicationRequest, _ ...grpc.CallOption) (*adminv1.ApproveGrantApplicationResponse, error) {
	return &adminv1.ApproveGrantApplicationResponse{
		Application: &adminv1.GrantApplication{ApplicationId: in.GetApplicationId(), SourceId: "src_1", Tenant: "credit",
			Status: adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_APPROVED, AppliedBy: "owen", AppliedAt: t0, DecidedBy: "dana",
			DecidedAt: t0, DecisionNote: in.GetBasis()},
		Grant: &adminv1.Grant{SourceId: "src_1", Tenant: "credit", GrantedBy: "dana", GrantedAt: t0, ApplicationId: in.GetApplicationId()},
	}, nil
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
	l := e.request(t, http.MethodGet, "/v1/admin/sources?status=active", dana, "", nil)
	src := l.body["sources"].([]any)[0].(map[string]any)
	if l.status != 200 || f.listed.GetStatus() != adminv1.SourceStatus_SOURCE_STATUS_ACTIVE || src["security_approved_by"] != "ivan" ||
		src["default_classification"] != "restricted" {
		t.Fatalf("列表: %d %s", l.status, l.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/sources?status=gone", dana, "", nil); r.status != 400 {
		t.Fatalf("未知状态应 400: %d", r.status)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/sources", e.authz.token(t, "audra", "console"), "", nil); r.status != 403 {
		t.Fatalf("审计岗不能看: %d", r.status)
	}
	// 信息安全审批受限级数据源
	ivan := e.authz.token(t, "ivan", "console")
	sec := e.request(t, http.MethodPost, "/v1/admin/sources/src_1/approve", ivan, `{"basis":"SEC-3","capacity":"infosec"}`, key())
	if sec.status != 200 || f.approved.GetCapacity() != adminv1.ApprovalCapacity_APPROVAL_CAPACITY_INFOSEC || sec.body["security_approved_by"] != "ivan" {
		t.Fatalf("信息安全审批: %d %s", sec.status, sec.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/sources/src_1/approve", ivan, `{"basis":"x","capacity":"ceo"}`, key()); r.status != 400 {
		t.Fatalf("未知身份应 400: %d", r.status)
	}
}

func TestGrantApplicationRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeSources{}
	e.server.Sources = f
	owen, dana := e.authz.token(t, "owen", "console"), e.authz.token(t, "dana", "console")
	body := `{"source_id":"src_1","tenant":"credit","reason":"信贷问答需要制度库"}`
	a := e.request(t, http.MethodPost, "/v1/admin/grant-applications", owen, body, map[string]string{"Idempotency-Key": "k-1"})
	if a.status != 201 || a.body["status"] != "pending" || f.applied.GetTenant() != "credit" {
		t.Fatalf("租户负责人申请: %d %s", a.status, a.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/grant-applications", dana, body, map[string]string{"Idempotency-Key": "k-2"}); r.status != 403 {
		t.Fatalf("数据属主不发起申请: %d", r.status)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/grant-applications/gra_000000000000000000000001/approve", owen, `{"basis":"x"}`,
		map[string]string{"Idempotency-Key": "k-3"}); r.status != 403 {
		t.Fatalf("租户负责人不能审批: %d", r.status)
	}
	ok := e.request(t, http.MethodPost, "/v1/admin/grant-applications/gra_000000000000000000000001/approve", dana, `{"basis":"工单 GRANT-3"}`,
		map[string]string{"Idempotency-Key": "k-4"})
	if ok.status != 200 || ok.body["status"] != "approved" || ok.body["grant"].(map[string]any)["application_id"] != "gra_000000000000000000000001" {
		t.Fatalf("属主审批: %d %s", ok.status, ok.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/grant-applications/bad/approve", dana, `{"basis":"x"}`,
		map[string]string{"Idempotency-Key": "k-5"}); r.status != 404 {
		t.Fatalf("非法的申请 ID 应 404: %d", r.status)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/sources?status=active", owen, "", nil); r.status != 200 {
		t.Fatalf("租户负责人列数据源: %d", r.status)
	}
}
