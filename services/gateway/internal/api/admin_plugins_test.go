package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

type fakePlugins struct {
	adminv1.PluginAdminServiceClient
	reg    *adminv1.RegisterPluginRequest
	review *adminv1.ReviewPluginRequest
}

func (f *fakePlugins) ListPlugins(context.Context, *adminv1.ListPluginsRequest, ...grpc.CallOption) (*adminv1.ListPluginsResponse, error) {
	return &adminv1.ListPluginsResponse{Plugins: []*adminv1.Plugin{{PluginId: "plg_000000000000000000000001", Kind: "parser",
		Evaluations: []*adminv1.PluginEvaluation{{Tenant: "credit", Passed: true, EvaluatedAt: timestamppb.Now()}}, UsableTenants: []string{"credit"}}}}, nil
}

func (f *fakePlugins) RegisterPlugin(_ context.Context, in *adminv1.RegisterPluginRequest, _ ...grpc.CallOption) (*adminv1.RegisterPluginResponse, error) {
	f.reg = in
	return &adminv1.RegisterPluginResponse{Plugin: &adminv1.Plugin{PluginId: "plg_000000000000000000000002", Kind: in.GetKind(),
		SecurityReview: "pending", RegisteredAt: timestamppb.Now()}}, nil
}

func (f *fakePlugins) ReviewPlugin(_ context.Context, in *adminv1.ReviewPluginRequest, _ ...grpc.CallOption) (*adminv1.ReviewPluginResponse, error) {
	f.review = in
	return &adminv1.ReviewPluginResponse{Plugin: &adminv1.Plugin{PluginId: in.GetPluginId(), SecurityReview: "approved"}}, nil
}

func TestPluginRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakePlugins{}
	e.server.Plugins = f
	paul, ivan := e.authz.token(t, "paul", "console"), e.authz.token(t, "ivan", "console")
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }

	r := e.request(t, http.MethodGet, "/v1/admin/plugins", ivan, "", nil)
	if r.status != 200 || len(r.body["plugins"].([]any)) != 1 {
		t.Fatalf("列表: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/plugins", paul,
		`{"kind":"tool","name":"ledger-query","version":"1.2.0","description":"查询台账","tool_scopes":["read:ledger"]}`, key("a"))
	if r.status != 201 || r.body["security_review"] != "pending" || f.reg.GetToolScopes()[0] != "read:ledger" {
		t.Fatalf("登记: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/plugins", ivan, `{"kind":"parser"}`, key("b")); r.status != 403 {
		t.Fatalf("信息安全不能登记: %d", r.status)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/plugins/plg_000000000000000000000002/security-review", ivan,
		`{"approved":true,"note":"只读"}`, key("c"))
	if r.status != 200 || r.body["security_review"] != "approved" || !f.review.GetApproved() {
		t.Fatalf("评审: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/plugins/plg_000000000000000000000002/security-review", paul,
		`{"approved":true,"note":"x"}`, key("d")); r.status != 403 {
		t.Fatalf("平台管理员不能评审: %d", r.status)
	}
}
