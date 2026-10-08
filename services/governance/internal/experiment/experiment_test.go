package experiment

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
)

var quiet = logging.New(&bytes.Buffer{}, "t", slog.LevelInfo)

type staticVerifier struct{ p *identityv1.Principal }

func (v staticVerifier) Verify(context.Context, string) (*identityv1.Principal, error) {
	return v.p, nil
}

func as(t *testing.T, sub string, roles ...string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer x"))
	var out context.Context
	_, err := grpcmw.UnaryServer(staticVerifier{&identityv1.Principal{Sub: sub, Roles: roles}}, quiet)(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/x/y"}, func(ctx context.Context, _ any) (any, error) { out = ctx; return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	return out
}

type memStore struct {
	mu   sync.Mutex
	list []Experiment
}

func (m *memStore) Create(_ context.Context, e Experiment, beforeCommit func() error) (Experiment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, x := range m.list {
		if x.Tenant == e.Tenant && x.Status == "running" {
			return Experiment{}, ErrRunning
		}
	}
	if err := beforeCommit(); err != nil {
		return Experiment{}, err
	}
	e.Status, e.CreatedAt = "running", time.Unix(int64(len(m.list)), 0)
	m.list = append(m.list, e)
	return e, nil
}

func (m *memStore) Get(_ context.Context, id string) (Experiment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, x := range m.list {
		if x.ID == id {
			return x, nil
		}
	}
	return Experiment{}, ErrNotFound
}

func (m *memStore) List(_ context.Context, offset, limit int) ([]Experiment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Experiment
	for i := len(m.list) - 1; i >= 0; i-- {
		out = append(out, m.list[i])
	}
	if offset >= len(out) {
		return nil, nil
	}
	return out[offset:min(len(out), offset+limit)], nil
}

func (m *memStore) Running(context.Context) ([]Experiment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Experiment
	for _, x := range m.list {
		if x.Status == "running" {
			out = append(out, x)
		}
	}
	return out, nil
}

func (m *memStore) Update(_ context.Context, id string, change func(e *Experiment) error) (Experiment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.list {
		if m.list[i].ID == id {
			e := m.list[i]
			if err := change(&e); err != nil {
				return m.list[i], err
			}
			m.list[i] = e
			return e, nil
		}
	}
	return Experiment{}, ErrNotFound
}

type memAudit struct {
	actions []string
	err     error
}

func (a *memAudit) Write(_ context.Context, action, _ string, _ any) error {
	if a.err != nil {
		return a.err
	}
	a.actions = append(a.actions, action)
	return nil
}

func newServer() (*Server, *memStore, *memAudit) {
	st, au := &memStore{}, &memAudit{}
	return &Server{Store: st, Audit: au, Log: quiet, Now: func() time.Time { return time.Unix(100, 0) }}, st, au
}

func create(tenant string) *adminv1.CreateExperimentRequest {
	return &adminv1.CreateExperimentRequest{Tenant: tenant, Hypothesis: "门槛调低后有用反馈率上升",
		PrimaryMetric: adminv1.PrimaryMetric_PRIMARY_METRIC_USEFUL_RATE, BaselineRate: 0.6, MinDetectableEffect: 0.05,
		Overrides: &adminv1.ExperimentOverrides{SupportThreshold: proto.Float64(0.4)}}
}

func TestSampleSizeMatchesTwoProportionFormula(t *testing.T) {
	// 0.60 → 0.65、α = 0.05、检验力 0.8 时每组约 1470 人（常用样本量表的值）
	if n := SampleSize(0.6, 0.05); n < 1460 || n > 1480 {
		t.Fatalf("得到 %d", n)
	}
	// 基线加差值超过 1 时向下比较
	if n := SampleSize(0.97, 0.05); n <= 0 {
		t.Fatalf("得到 %d", n)
	}
}

func TestCreateValidatesAndRequiresPlatformAdmin(t *testing.T) {
	s, _, au := newServer()
	if _, err := s.CreateExperiment(as(t, "olga", "platform_operator"), create("credit")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("非平台管理员应被拒绝：%v", err)
	}
	bad := []func(r *adminv1.CreateExperimentRequest){
		func(r *adminv1.CreateExperimentRequest) { r.Tenant = "Credit" },
		func(r *adminv1.CreateExperimentRequest) { r.Hypothesis = " " },
		func(r *adminv1.CreateExperimentRequest) {
			r.PrimaryMetric = adminv1.PrimaryMetric_PRIMARY_METRIC_UNSPECIFIED
		},
		func(r *adminv1.CreateExperimentRequest) { r.BaselineRate = 1 },
		func(r *adminv1.CreateExperimentRequest) { r.MinDetectableEffect = 0 },
		func(r *adminv1.CreateExperimentRequest) { r.Overrides = &adminv1.ExperimentOverrides{} },
		func(r *adminv1.CreateExperimentRequest) { r.Overrides.SupportThreshold = proto.Float64(0.95) },
		func(r *adminv1.CreateExperimentRequest) { r.Overrides.MinCoverage = proto.Float64(0.2) },
	}
	for i, f := range bad {
		r := create("credit")
		f(r)
		if _, err := s.CreateExperiment(as(t, "paul", RolePlatformAdmin), r); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("第 %d 个非法请求得到 %v", i, err)
		}
	}
	resp, err := s.CreateExperiment(as(t, "paul", RolePlatformAdmin), create("credit"))
	if err != nil {
		t.Fatal(err)
	}
	e := resp.GetExperiment()
	if e.GetTrafficPercent() != 5 || e.GetStatus() != adminv1.ExperimentStatus_EXPERIMENT_STATUS_RUNNING ||
		e.GetSampleSizePerArm() != SampleSize(0.6, 0.05) || e.GetCreatedBy() != "paul" || len(au.actions) != 1 {
		t.Fatalf("得到 %v，审计 %v", e, au.actions)
	}
	// 同一租户同时只能有一个运行中的实验
	_, err = s.CreateExperiment(as(t, "paul", RolePlatformAdmin), create("credit"))
	if status.Code(err) != codes.FailedPrecondition || grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("得到 %v", err)
	}
}

func TestCreateIsFailClosedOnAudit(t *testing.T) {
	s, st, au := newServer()
	au.err = errors.New("桶不可用")
	if _, err := s.CreateExperiment(as(t, "paul", RolePlatformAdmin), create("credit")); status.Code(err) != codes.Unavailable {
		t.Fatalf("得到 %v", err)
	}
	if len(st.list) != 0 {
		t.Fatal("审计写不进去时不应登记实验")
	}
}

func TestRampOnlyUpwardAndStopIsIdempotent(t *testing.T) {
	s, _, au := newServer()
	ctx := as(t, "paul", RolePlatformAdmin)
	resp, err := s.CreateExperiment(ctx, create("credit"))
	if err != nil {
		t.Fatal(err)
	}
	e := resp.GetExperiment()
	_, before, _ := s.Snapshot(ctx)
	if _, err := s.RampExperiment(ctx, &adminv1.RampExperimentRequest{ExpId: e.GetExpId(), TrafficPercent: 30}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("30%% 不是灰度台阶：%v", err)
	}
	r, err := s.RampExperiment(ctx, &adminv1.RampExperimentRequest{ExpId: e.GetExpId(), TrafficPercent: 50})
	if err != nil || r.GetExperiment().GetTrafficPercent() != 50 {
		t.Fatalf("得到 %v %v", r, err)
	}
	if _, err := s.RampExperiment(ctx, &adminv1.RampExperimentRequest{ExpId: e.GetExpId(), TrafficPercent: 20}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("不能往回调：%v", err)
	}
	running, after, _ := s.Snapshot(ctx)
	if before == after || len(running) != 1 || running[0].GetTrafficPercent() != 50 || running[0].GetSupportThreshold() != 0.4 {
		t.Fatalf("快照 %v，摘要 %s → %s", running, before, after)
	}

	stopped, err := s.StopExperiment(ctx, &adminv1.StopExperimentRequest{ExpId: e.GetExpId(), Reason: "护栏：误拒率上升"})
	if err != nil || stopped.GetExperiment().GetStatus() != adminv1.ExperimentStatus_EXPERIMENT_STATUS_STOPPED || stopped.GetExperiment().GetStoppedBy() != "paul" {
		t.Fatalf("得到 %v %v", stopped, err)
	}
	again, err := s.StopExperiment(ctx, &adminv1.StopExperimentRequest{ExpId: e.GetExpId(), Reason: "再停一次"})
	if err != nil || again.GetExperiment().GetStopReason() != "护栏：误拒率上升" {
		t.Fatalf("已停止的再停应原样返回：%v %v", again, err)
	}
	if running, _, _ := s.Snapshot(ctx); len(running) != 0 {
		t.Fatal("停止后不应再下发")
	}
	if got := au.actions; len(got) != 3 || got[0] != "experiment.create" || got[1] != "experiment.ramp" || got[2] != "experiment.stop" {
		t.Fatalf("审计 %v", got)
	}
	// 停止后同一租户可以开新实验
	if _, err := s.CreateExperiment(ctx, create("credit")); err != nil {
		t.Fatal(err)
	}
}

func TestGetAndListPages(t *testing.T) {
	s, _, _ := newServer()
	ctx := as(t, "paul", RolePlatformAdmin)
	for _, tenant := range []string{"credit", "risk", "ops"} {
		if _, err := s.CreateExperiment(ctx, create(tenant)); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.ListExperiments(ctx, &adminv1.ListExperimentsRequest{PageSize: 2})
	if err != nil || len(first.GetExperiments()) != 2 || first.GetNextPageToken() == "" || first.GetExperiments()[0].GetTenant() != "ops" {
		t.Fatalf("得到 %v %v", first, err)
	}
	rest, err := s.ListExperiments(ctx, &adminv1.ListExperimentsRequest{PageSize: 2, PageToken: first.GetNextPageToken()})
	if err != nil || len(rest.GetExperiments()) != 1 || rest.GetNextPageToken() != "" {
		t.Fatalf("得到 %v %v", rest, err)
	}
	got, err := s.GetExperiment(ctx, &adminv1.GetExperimentRequest{ExpId: rest.GetExperiments()[0].GetExpId()})
	if err != nil || got.GetExperiment().GetTenant() != "credit" {
		t.Fatalf("得到 %v %v", got, err)
	}
	if _, err := s.GetExperiment(ctx, &adminv1.GetExperimentRequest{ExpId: "exp_000000000000000000000000"}); status.Code(err) != codes.NotFound {
		t.Fatalf("得到 %v", err)
	}
}

func TestControlServiceForEvaluation(t *testing.T) {
	s, _, au := newServer()
	resp, err := s.CreateExperiment(as(t, "paul", RolePlatformAdmin), create("credit"))
	if err != nil {
		t.Fatal(err)
	}
	id := resp.GetExperiment().GetExpId()
	c := &ControlServer{Experiments: s}
	// 服务间接口：不带 Principal
	ctx := context.Background()
	running, err := c.ListRunningExperiments(ctx, &configv1.ListRunningExperimentsRequest{})
	if err != nil || len(running.GetExperiments()) != 1 || running.GetExperiments()[0].GetSampleSizePerArm() == 0 {
		t.Fatalf("得到 %v %v", running, err)
	}
	if got, err := c.GetExperiment(ctx, &configv1.GetExperimentRequest{ExpId: id}); err != nil || got.GetExperiment().GetTenant() != "credit" {
		t.Fatalf("得到 %v %v", got, err)
	}
	if _, err := c.StopForGuardrail(ctx, &configv1.StopForGuardrailRequest{ExpId: id}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("原因必填: %v", err)
	}
	stopped, err := c.StopForGuardrail(ctx, &configv1.StopForGuardrailRequest{ExpId: id, Reason: "拒答率 0.12 > 0.05 + 0.02"})
	if err != nil || stopped.GetExperiment().GetStoppedBy() != GuardrailActor ||
		stopped.GetExperiment().GetStopReason() != "护栏触发：拒答率 0.12 > 0.05 + 0.02" {
		t.Fatalf("得到 %v %v", stopped, err)
	}
	if running, _ := c.ListRunningExperiments(ctx, &configv1.ListRunningExperimentsRequest{}); len(running.GetExperiments()) != 0 {
		t.Fatal("停止后不应再列出")
	}
	if au.actions[len(au.actions)-1] != "experiment.stop" {
		t.Fatalf("审计 %v", au.actions)
	}
}
