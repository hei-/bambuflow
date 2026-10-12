package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

type fakeGates struct {
	adminv1.EvalGateAdminServiceClient
	list *adminv1.ListGatesRequest
}

var sampleGate = &adminv1.Gate{GateId: "gate_000000000000000000000001", Trigger: "ci", Tenants: []string{"credit"}, Conclusion: "failed",
	Metrics: []*adminv1.GateMetric{{Name: "groundedness", Value: proto.Float64(0.91)}, {Name: "latency_p95_ms"}},
	Config:  []*adminv1.GateConfigVersion{{Name: "index_version", Values: []string{"ix-1"}}}, CreatedAt: timestamppb.Now()}

func (f *fakeGates) ListGates(_ context.Context, in *adminv1.ListGatesRequest, _ ...grpc.CallOption) (*adminv1.ListGatesResponse, error) {
	f.list = in
	return &adminv1.ListGatesResponse{Gates: []*adminv1.Gate{sampleGate}}, nil
}

func (f *fakeGates) GetGate(context.Context, *adminv1.GetGateRequest, ...grpc.CallOption) (*adminv1.GetGateResponse, error) {
	return &adminv1.GetGateResponse{Gate: sampleGate, ReportMarkdown: "# 报告"}, nil
}

func TestEvalGateRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeGates{}
	e.server.Gates = f
	olga := e.authz.token(t, "olga", "console")

	r := e.request(t, http.MethodGet, "/v1/admin/eval-gates?tenant=credit&trigger=ci", olga, "", nil)
	if r.status != 200 || f.list.GetTenant() != "credit" || f.list.GetTrigger() != "ci" {
		t.Fatalf("列表: %d %s", r.status, r.raw)
	}
	g := r.body["gates"].([]any)[0].(map[string]any)
	m := g["metrics"].(map[string]any)
	if m["groundedness"] != 0.91 || m["latency_p95_ms"] != nil || g["report_markdown"] != nil {
		t.Fatalf("门禁: %v", g)
	}
	r = e.request(t, http.MethodGet, "/v1/admin/eval-gates/gate_000000000000000000000001", olga, "", nil)
	if r.status != 200 || r.body["report_markdown"] != "# 报告" {
		t.Fatalf("报告: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/eval-gates", e.authz.token(t, "audra", "console"), "", nil); r.status != 403 {
		t.Fatalf("审计岗不能看: %d", r.status)
	}
}
