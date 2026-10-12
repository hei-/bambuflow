package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

type fakeTrends struct {
	adminv1.QualityTrendServiceClient
	got *adminv1.GetQualityTrendsRequest
}

func (f *fakeTrends) GetQualityTrends(_ context.Context, in *adminv1.GetQualityTrendsRequest, _ ...grpc.CallOption) (*adminv1.GetQualityTrendsResponse, error) {
	f.got = in
	return &adminv1.GetQualityTrendsResponse{Tenant: in.GetTenant(), Groundedness_7D: 0.88, Groundedness_30D: 0.95, Declining: true,
		Days: []*adminv1.DailyQuality{{Day: "2026-10-06", Sampled: 200, Sentences: 640, Groundedness: 0.88, CitationPrecision: 0.9, JudgeModel: "nli-large"}}}, nil
}

func TestQualityTrendsRoute(t *testing.T) {
	e := newEnv(t)
	f := &fakeTrends{}
	e.server.Trends = f
	paul := e.authz.token(t, "paul", "console")
	r := e.request(t, http.MethodGet, "/v1/admin/quality-trends?tenant=credit&days=14", paul, "", nil)
	ds, _ := r.body["days"].([]any)
	if r.status != 200 || r.body["declining"] != true || len(ds) != 1 || f.got.GetDays() != 14 || f.got.GetTenant() != "credit" {
		t.Fatalf("趋势: %d %s", r.status, r.raw)
	}
	for _, q := range []string{"tenant=Credit", "tenant=credit&days=0", "tenant=credit&days=91", ""} {
		if r := e.request(t, http.MethodGet, "/v1/admin/quality-trends?"+q, paul, "", nil); r.status != 400 {
			t.Fatalf("%q 应 400: %d", q, r.status)
		}
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/quality-trends?tenant=credit", e.authz.token(t, "olga", "console"), "", nil); r.status != 403 {
		t.Fatalf("平台运维应 403: %d", r.status)
	}
}
