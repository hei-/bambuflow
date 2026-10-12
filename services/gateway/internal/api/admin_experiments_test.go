package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

const testExpID = "exp_0123456789abcdef01234567"

// fakeExperiments 是 governance 的实验接口：记下收到的请求。
type fakeExperiments struct {
	adminv1.ExperimentAdminServiceClient
	created *adminv1.CreateExperimentRequest
	ramped  *adminv1.RampExperimentRequest
	stopped *adminv1.StopExperimentRequest
}

func experimentFor(tenant string, traffic int32) *adminv1.Experiment {
	return &adminv1.Experiment{ExpId: testExpID, Tenant: tenant, Hypothesis: "h",
		PrimaryMetric: adminv1.PrimaryMetric_PRIMARY_METRIC_USEFUL_RATE, BaselineRate: 0.6, MinDetectableEffect: 0.05,
		SampleSizePerArm: 1471, TrafficPercent: traffic, Overrides: &adminv1.ExperimentOverrides{SupportThreshold: proto.Float64(0.4)},
		Status: adminv1.ExperimentStatus_EXPERIMENT_STATUS_RUNNING, CreatedBy: "paul",
		CreatedAt: timestamppb.New(time.Date(2026, 10, 7, 1, 2, 3, 0, time.UTC))}
}

func (f *fakeExperiments) CreateExperiment(_ context.Context, in *adminv1.CreateExperimentRequest, _ ...grpc.CallOption) (*adminv1.CreateExperimentResponse, error) {
	f.created = in
	return &adminv1.CreateExperimentResponse{Experiment: experimentFor(in.GetTenant(), 5)}, nil
}

func (f *fakeExperiments) RampExperiment(_ context.Context, in *adminv1.RampExperimentRequest, _ ...grpc.CallOption) (*adminv1.RampExperimentResponse, error) {
	f.ramped = in
	if in.GetTrafficPercent() <= 5 {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "只能向上调")
	}
	return &adminv1.RampExperimentResponse{Experiment: experimentFor("credit", in.GetTrafficPercent())}, nil
}

func (f *fakeExperiments) StopExperiment(_ context.Context, in *adminv1.StopExperimentRequest, _ ...grpc.CallOption) (*adminv1.StopExperimentResponse, error) {
	f.stopped = in
	e := experimentFor("credit", 5)
	e.Status, e.StoppedBy, e.StopReason = adminv1.ExperimentStatus_EXPERIMENT_STATUS_STOPPED, "paul", in.GetReason()
	e.StoppedAt = timestamppb.New(time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC))
	return &adminv1.StopExperimentResponse{Experiment: e}, nil
}

func (f *fakeExperiments) GetExperiment(_ context.Context, in *adminv1.GetExperimentRequest, _ ...grpc.CallOption) (*adminv1.GetExperimentResponse, error) {
	return &adminv1.GetExperimentResponse{Experiment: experimentFor("credit", 20)}, nil
}

func (f *fakeExperiments) ListExperiments(_ context.Context, in *adminv1.ListExperimentsRequest, _ ...grpc.CallOption) (*adminv1.ListExperimentsResponse, error) {
	return &adminv1.ListExperimentsResponse{Experiments: []*adminv1.Experiment{experimentFor("credit", 5)}}, nil
}

func TestExperimentRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeExperiments{}
	e.server.Experiments = f
	paul := e.authz.token(t, "paul", "console")
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }

	body := `{"tenant":"credit","hypothesis":"h","primary_metric":"useful_rate","baseline_rate":0.6,"min_detectable_effect":0.05,
		"overrides":{"support_threshold":0.4,"config_versions":["cfv_000000000000000000000001"]}}`
	r := e.request(t, http.MethodPost, "/v1/admin/experiments", paul, body, key("a"))
	if r.status != 201 || r.body["exp_id"] != testExpID || r.body["status"] != "running" || r.body["primary_metric"] != "useful_rate" ||
		r.body["traffic_percent"] != float64(5) || f.created.GetOverrides().GetSupportThreshold() != 0.4 || f.created.GetOverrides().MinCoverage != nil ||
		len(f.created.GetOverrides().GetConfigVersions()) != 1 {
		t.Fatalf("立项: %d %s", r.status, r.raw)
	}
	if _, ok := r.body["stopped_at"]; ok {
		t.Fatal("运行中的实验不应有 stopped_at")
	}
	for i, bad := range []string{
		`{"tenant":"credit","hypothesis":"h","primary_metric":"click_rate","baseline_rate":0.6,"min_detectable_effect":0.05,"overrides":{"min_coverage":0.9}}`,
		`{"tenant":"credit","hypothesis":"h","primary_metric":"useful_rate","baseline_rate":0.6,"min_detectable_effect":0.05}`,
	} {
		if r := e.request(t, http.MethodPost, "/v1/admin/experiments", paul, bad, key("bad"+string(rune('0'+i)))); r.status != 400 {
			t.Fatalf("第 %d 个非法请求应 400: %d", i, r.status)
		}
	}
	// 只有平台管理员
	if r := e.request(t, http.MethodPost, "/v1/admin/experiments", e.authz.token(t, "olga", "console"), body, key("b")); r.status != 403 {
		t.Fatalf("平台运维应 403: %d", r.status)
	}

	path := "/v1/admin/experiments/" + testExpID
	if r := e.request(t, http.MethodPost, path+"/ramp", paul, `{"traffic_percent":20}`, key("c")); r.status != 200 ||
		r.body["traffic_percent"] != float64(20) || f.ramped.GetExpId() != testExpID {
		t.Fatalf("推进: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, path+"/ramp", paul, `{"traffic_percent":5}`, key("d")); r.status != 409 {
		t.Fatalf("往回调应 409: %d", r.status)
	}
	r = e.request(t, http.MethodPost, path+"/stop", paul, `{"reason":"护栏触发"}`, key("e"))
	if r.status != 200 || r.body["status"] != "stopped" || r.body["stop_reason"] != "护栏触发" || r.body["stopped_at"] != "2026-10-08T00:00:00Z" {
		t.Fatalf("停止: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, path, paul, "", nil); r.status != 200 || r.body["traffic_percent"] != float64(20) {
		t.Fatalf("查看: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/experiments/exp_bad", paul, "", nil); r.status != 404 {
		t.Fatalf("格式不对的 ID 应 404: %d", r.status)
	}
	l := e.request(t, http.MethodGet, "/v1/admin/experiments", paul, "", nil)
	if xs, _ := l.body["experiments"].([]any); l.status != 200 || len(xs) != 1 {
		t.Fatalf("列表: %d %s", l.status, l.raw)
	}
	// 没有删除接口
	if r := e.request(t, http.MethodDelete, path, paul, "", nil); r.status != 404 {
		t.Fatalf("不提供删除: %d", r.status)
	}
}

func TestWhyCarriesExperimentArm(t *testing.T) {
	b, _ := json.Marshal(whyOf(&answerv1.Why{ExperimentId: testExpID, ExperimentArm: answerv1.ExperimentArm_EXPERIMENT_ARM_EXCLUDED}))
	if s := string(b); !strings.Contains(s, `"experiment_id":"`+testExpID+`"`) || !strings.Contains(s, `"experiment_arm":"excluded"`) {
		t.Fatalf("why 缺少实验分组: %s", s)
	}
	// 租户没有运行中的实验：两个字段都不输出
	if b, _ := json.Marshal(whyOf(&answerv1.Why{})); strings.Contains(string(b), "experiment") {
		t.Fatalf("没有实验时不应输出: %s", b)
	}
}

type fakeAnalysis struct {
	adminv1.ExperimentAnalysisServiceClient
	reached bool
}

func (f *fakeAnalysis) GetExperimentAnalysis(_ context.Context, in *adminv1.GetExperimentAnalysisRequest, _ ...grpc.CallOption) (*adminv1.GetExperimentAnalysisResponse, error) {
	a := &adminv1.ExperimentAnalysis{Experiment: experimentFor("credit", 20), Progress: 0.4,
		Control:    &adminv1.ArmSummary{Arm: "control", Users: 600, Answers: 1500, MetricUsers: 590, MetricMean: 0.61, P95LatencyMs: 3200},
		Treatment:  &adminv1.ArmSummary{Arm: "treatment", Users: 150, Answers: 380, MetricUsers: 148, MetricMean: 0.66, P95LatencyMs: 3300},
		Guardrails: []*adminv1.GuardrailCheck{{Name: "p95_latency_ms", Control: 3200, Treatment: 3300}},
		DataSince:  timestamppb.New(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)),
		DataUntil:  timestamppb.New(time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC))}
	if f.reached {
		a.Progress, a.Conclusion = 1, &adminv1.Conclusion{Delta: 0.05, CiLow: 0.01, CiHigh: 0.09, PValue: 0.01, Significant: true, Improved: true}
	}
	return &adminv1.GetExperimentAnalysisResponse{Analysis: a}, nil
}

func TestExperimentAnalysisRoute(t *testing.T) {
	e := newEnv(t)
	f := &fakeAnalysis{}
	e.server.Analysis = f
	paul := e.authz.token(t, "paul", "console")
	path := "/v1/admin/experiments/" + testExpID + "/analysis"
	r := e.request(t, http.MethodGet, path, paul, "", nil)
	if r.status != 200 || r.body["progress"] != 0.4 || r.body["data_until"] != "2026-10-07T00:00:00Z" {
		t.Fatalf("分析: %d %s", r.status, r.raw)
	}
	if _, ok := r.body["conclusion"]; ok {
		t.Fatal("达到样本量前不应返回结论")
	}
	f.reached = true
	r = e.request(t, http.MethodGet, path, paul, "", nil)
	if c, _ := r.body["conclusion"].(map[string]any); r.status != 200 || c["significant"] != true || c["p_value"] != 0.01 {
		t.Fatalf("结论: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, path, e.authz.token(t, "audra", "console"), "", nil); r.status != 403 {
		t.Fatalf("审计岗应 403: %d", r.status)
	}
}
