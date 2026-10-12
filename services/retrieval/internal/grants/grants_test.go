package grants

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/hei-/bambuflow/libs/platform-go/logging"
	"github.com/hei-/bambuflow/libs/platform-go/policy"
)

type fakeConfig struct {
	Client
	resp *configv1.GetSnapshotResponse
	err  error
	asks []string
	acks []*configv1.AckRequest
	// GetCandidate 按版本返回的候选
	candidates    map[string]*configv1.Canary
	candidateAsks int
}

func (f *fakeConfig) GetCandidate(_ context.Context, in *configv1.GetCandidateRequest, _ ...grpc.CallOption) (*configv1.GetCandidateResponse, error) {
	f.candidateAsks++
	c, ok := f.candidates[in.GetVersionId()]
	if !ok || in.GetService() != "retrieval" {
		return nil, status.Error(codes.NotFound, "不存在")
	}
	return &configv1.GetCandidateResponse{Snapshot: &configv1.GetSnapshotResponse{SnapshotVersion: "candidate:" + c.GetVersionId(),
		Canaries: []*configv1.Canary{c}}}, nil
}

func (f *fakeConfig) Ack(_ context.Context, in *configv1.AckRequest, _ ...grpc.CallOption) (*configv1.AckResponse, error) {
	f.acks = append(f.acks, in)
	return &configv1.AckResponse{}, nil
}

// opened 当作验签通过；带 SigningKeyId "bad" 的当作验签失败
func opened(_ context.Context, r *configv1.GetSnapshotResponse) (*configv1.GetSnapshotResponse, error) {
	if r.GetSigningKeyId() == "bad" {
		return nil, errors.New("签名无效")
	}
	return r, nil
}

func (f *fakeConfig) GetSnapshot(_ context.Context, in *configv1.GetSnapshotRequest, _ ...grpc.CallOption) (*configv1.GetSnapshotResponse, error) {
	f.asks = append(f.asks, in.GetService())
	return f.resp, f.err
}

func TestSourcesFailClosed(t *testing.T) {
	now := time.Date(2026, 10, 7, 9, 0, 0, 0, time.UTC)
	f := &fakeConfig{resp: &configv1.GetSnapshotResponse{SnapshotVersion: "grants:1", SourceGrants: []*configv1.SourceGrant{
		{Tenant: "credit", SourceId: "src_a"}, {Tenant: "credit", SourceId: "src_b"}, {Tenant: "risk", SourceId: "src_a"},
	}}}
	w := &Watcher{Client: f, Open: opened, Instance: "r-1", Every: time.Second, MaxAge: 5 * time.Minute, Log: logging.New(&bytes.Buffer{}, "t", slog.LevelInfo),
		Now: func() time.Time { return now }}
	if _, _, err := w.Sources("credit"); !errors.Is(err, ErrUnavailable) || w.Ready() {
		t.Fatal("还没拿到快照时应拒绝")
	}
	if err := w.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, v, err := w.Sources("credit")
	if err != nil || v != "grants:1" || !slices.Equal(got, []string{"src_a", "src_b"}) || f.asks[0] != "retrieval" {
		t.Fatalf("授权: %v %s %v", got, v, err)
	}
	if got, _, _ := w.Sources("hr"); len(got) != 0 {
		t.Fatalf("没有授权的租户应为空: %v", got)
	}
	// 取消授权：下一次刷新后立即生效
	f.resp = &configv1.GetSnapshotResponse{SnapshotVersion: "grants:2", SourceGrants: []*configv1.SourceGrant{{Tenant: "credit", SourceId: "src_a"}}}
	if err := w.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := w.Sources("credit"); !slices.Equal(got, []string{"src_a"}) {
		t.Fatalf("取消授权应生效: %v", got)
	}
	// 刷新失败：期限内继续用上一份，超过期限拒绝
	f.err = errors.New("governance 不可用")
	if w.Refresh(context.Background()) == nil {
		t.Fatal("应返回错误")
	}
	now = now.Add(4 * time.Minute)
	if _, _, err := w.Sources("credit"); err != nil {
		t.Fatalf("期限内应继续可用: %v", err)
	}
	now = now.Add(2 * time.Minute)
	if _, _, err := w.Sources("credit"); !errors.Is(err, ErrUnavailable) || w.Ready() {
		t.Fatal("超过期限应拒绝")
	}
}

func TestUnsignedSnapshotIsNotLoadedAndAcked(t *testing.T) {
	now := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)
	f := &fakeConfig{resp: &configv1.GetSnapshotResponse{SnapshotVersion: "grants:1",
		SourceGrants: []*configv1.SourceGrant{{Tenant: "credit", SourceId: "src_a"}}}}
	w := &Watcher{Client: f, Open: opened, Instance: "r-1", Every: time.Second, MaxAge: 5 * time.Minute,
		Log: logging.New(&bytes.Buffer{}, "t", slog.LevelInfo), Now: func() time.Time { return now }}
	if err := w.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.acks) != 1 || !f.acks[0].GetLoaded() || f.acks[0].GetSnapshotVersion() != "grants:1" || f.acks[0].GetInstanceId() != "r-1" {
		t.Fatalf("加载成功应上报: %v", f.acks)
	}
	// 验签不过：保留旧授权，上报失败原因
	f.resp = &configv1.GetSnapshotResponse{SnapshotVersion: "grants:2", SigningKeyId: "bad"}
	if w.Refresh(context.Background()) == nil {
		t.Fatal("验签不过应返回错误")
	}
	if got, v, _ := w.Sources("credit"); v != "grants:1" || len(got) != 1 {
		t.Fatalf("应保留旧授权: %v %s", got, v)
	}
	if last := f.acks[len(f.acks)-1]; last.GetLoaded() || last.GetReason() != "signature_invalid" {
		t.Fatalf("应上报验签失败: %v", last)
	}
}

type memCache struct {
	saved     *configv1.GetSnapshotResponse
	confirmed time.Time
}

func (m *memCache) Save(s *configv1.GetSnapshotResponse) error {
	m.saved = s
	return nil
}

func (m *memCache) Load() (*configv1.GetSnapshotResponse, time.Time, error) {
	if m.saved == nil {
		return nil, time.Time{}, errors.New("没有本地快照")
	}
	return m.saved, m.confirmed, nil
}

func TestRestoreCountsMaxAgeFromLastConfirmation(t *testing.T) {
	now := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	cache := &memCache{}
	log := logging.New(&bytes.Buffer{}, "t", slog.LevelInfo)
	first := &Watcher{Client: &fakeConfig{resp: &configv1.GetSnapshotResponse{SnapshotVersion: "grants:1",
		SourceGrants: []*configv1.SourceGrant{{Tenant: "credit", SourceId: "src_a"}}}}, Open: opened, Cache: cache,
		MaxAge: 5 * time.Minute, Log: log, Now: func() time.Time { return now }}
	if err := first.Refresh(context.Background()); err != nil || cache.saved == nil {
		t.Fatalf("加载成功后应写盘: %v", err)
	}
	cache.confirmed = now.Add(-2 * time.Minute)
	restarted := &Watcher{Client: &fakeConfig{err: errors.New("governance 不可用")}, Open: opened, Cache: cache,
		MaxAge: 5 * time.Minute, Log: log, Now: func() time.Time { return now }}
	if err := restarted.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, v, err := restarted.Sources("credit"); err != nil || v != "grants:1" || !slices.Equal(got, []string{"src_a"}) {
		t.Fatalf("应使用盘上的授权: %v %s %v", got, v, err)
	}
	// 盘上的一份已超过最长使用期限：fail-closed
	cache.confirmed = now.Add(-6 * time.Minute)
	if err := restarted.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := restarted.Sources("credit"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("过期的本地授权应拒绝: %v", err)
	}
}

func bundle(t *testing.T, tenantBody string) *configv1.PolicyBundle {
	t.Helper()
	src, err := os.ReadFile("../../../../config/policies/global.rego")
	if err != nil {
		t.Fatal(err)
	}
	b, err := policy.Build([]policy.Layer{{Scope: policy.ScopeGlobal, Body: string(src)}, {Scope: "tenant:credit", Body: tenantBody}})
	if err != nil {
		t.Fatal(err)
	}
	out := &configv1.PolicyBundle{Version: b.Version}
	for _, m := range b.Modules {
		out.Modules = append(out.Modules, &configv1.PolicyModule{Path: m.Path, Source: m.Source})
	}
	return out
}

func TestPolicyFromSnapshotKeepsCurrentWhenNewOneIsInvalid(t *testing.T) {
	ctx := context.Background()
	grants := []*configv1.SourceGrant{{Tenant: "credit", SourceId: "src_a"}, {Tenant: "credit", SourceId: "src_b"}}
	f := &fakeConfig{resp: &configv1.GetSnapshotResponse{SnapshotVersion: "v1", SourceGrants: grants}}
	w := &Watcher{Client: f, Open: opened, Instance: "r-1", Every: time.Second, MaxAge: time.Hour,
		Log: logging.New(&bytes.Buffer{}, "t", slog.LevelInfo), Now: time.Now}
	alice := policy.Input{Sub: "alice", Tenant: "credit", Dept: "信贷部", Clearance: 2}
	// 快照里没有策略包（没有生效的全局基线）：拒绝全部检索，readiness 为 false
	if err := w.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Decide(ctx, alice, []string{"src_a"}); !errors.Is(err, ErrNoPolicy) || w.Ready() {
		t.Fatalf("没有策略包应拒绝: %v", err)
	}
	f.resp = &configv1.GetSnapshotResponse{SnapshotVersion: "v2", SourceGrants: grants, Policy: bundle(t, "deny_sources := {\"src_b\"}\n")}
	if err := w.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	d, err := w.Decide(ctx, alice, []string{"src_a", "src_b"})
	if err != nil || !slices.Equal(d.Filter.Sources, []string{"src_a"}) || !w.Ready() || w.Version() != f.resp.GetPolicy().GetVersion() {
		t.Fatalf("%+v %v", d, err)
	}
	good := w.Version()
	// 新包编译不过：整份快照都不换（授权也保持 v2），上报 policy_invalid
	f.resp = &configv1.GetSnapshotResponse{SnapshotVersion: "v3", SourceGrants: grants[:1], Policy: bundle(t, "deny_sources := {\n")}
	if err := w.Refresh(ctx); err == nil {
		t.Fatal("应返回错误")
	}
	if w.Version() != good {
		t.Fatal("应继续用当前的策略包")
	}
	if got, v, _ := w.Sources("credit"); v != "v2" || len(got) != 2 {
		t.Fatalf("授权也不应换成 v3: %v %s", got, v)
	}
	last := f.acks[len(f.acks)-1]
	if last.GetSnapshotVersion() != "v3" || last.GetLoaded() || last.GetReason() != "policy_invalid" {
		t.Fatalf("%v", last)
	}
}

func TestInCanaryMatchesOrchestrator(t *testing.T) {
	// 与 orchestrator plans.in_canary 的测试用同一组数：两边对同一用户必须得出同一个结论
	cases := []struct {
		release, user string
		bucket        int32
	}{{"rel_a", "alice", 47}, {"rel_a", "bob", 28}, {"rel_b", "alice", 75}}
	for _, c := range cases {
		if InCanary(c.release, c.user, c.bucket) || !InCanary(c.release, c.user, c.bucket+1) {
			t.Errorf("%s/%s 的桶号应为 %d", c.release, c.user, c.bucket)
		}
	}
}

func TestCanaryPolicyAndPlanByUser(t *testing.T) {
	ctx := context.Background()
	grants := []*configv1.SourceGrant{{Tenant: "credit", SourceId: "src_a"}, {Tenant: "credit", SourceId: "src_b"}}
	cand := bundle(t, "deny_sources := {\"src_b\"}\n")
	f := &fakeConfig{resp: &configv1.GetSnapshotResponse{SnapshotVersion: "v1", SourceGrants: grants, Policy: bundle(t, "deny_sources := set()\n"),
		Pipelines: []*configv1.PipelineConfig{{Tenant: "credit", VersionId: "cfv_active"}},
		Canaries: []*configv1.Canary{
			{Object: "policy", Scope: "tenant:credit", ReleaseId: "rel_a", Tenant: "credit", Percent: 40, Candidate: &configv1.Canary_Policy{Policy: cand}},
			{Object: "pipeline", Scope: "credit", ReleaseId: "rel_a", Tenant: "credit", Percent: 40,
				Candidate: &configv1.Canary_Pipeline{Pipeline: &configv1.PipelineConfig{Tenant: "credit", VersionId: "cfv_canary"}}},
		},
		Experiments: []*configv1.RunningExperiment{{ExpId: "exp_1", Tenant: "credit",
			Pipeline: &configv1.PipelineConfig{Tenant: "credit", VersionId: "cfv_variant"}}}}}
	w := &Watcher{Client: f, Open: opened, Instance: "r-1", Every: time.Second, MaxAge: time.Hour,
		Log: logging.New(&bytes.Buffer{}, "t", slog.LevelInfo), Now: time.Now}
	if err := w.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	// rel_a 下 bob 的桶号 28 < 40：用候选；alice 47：用当前的
	bob := policy.Input{Sub: "bob", Tenant: "credit", Dept: "信贷部", Clearance: 2}
	alice := policy.Input{Sub: "alice", Tenant: "credit", Dept: "信贷部", Clearance: 2}
	db, _ := w.Decide(ctx, bob, []string{"src_a", "src_b"})
	da, _ := w.Decide(ctx, alice, []string{"src_a", "src_b"})
	if db.Version != cand.GetVersion() || len(db.Filter.Sources) != 1 || da.Version == cand.GetVersion() || len(da.Filter.Sources) != 2 {
		t.Fatalf("bob %+v alice %+v", db, da)
	}
	if w.Plan(ctx, "credit", "bob", "").GetVersionId() != "cfv_canary" || w.Plan(ctx, "credit", "alice", "").GetVersionId() != "cfv_active" {
		t.Fatal("pipeline 按同一规则分流")
	}
	// A/B 实验组由 orchestrator 指定变体，优先于灰度；不认识的版本与别的租户按原来的规则
	if w.Plan(ctx, "credit", "bob", "cfv_variant").GetVersionId() != "cfv_variant" ||
		w.Plan(ctx, "credit", "alice", "cfv_gone").GetVersionId() != "cfv_active" || w.Plan(ctx, "risk", "bob", "cfv_variant") != nil {
		t.Fatal("实验组的检索计划")
	}
}

func TestEvalCandidateOverridesForTheRequest(t *testing.T) {
	ctx := context.Background()
	grants := []*configv1.SourceGrant{{Tenant: "credit", SourceId: "src_a"}, {Tenant: "credit", SourceId: "src_b"}}
	cand := bundle(t, "deny_sources := {\"src_b\"}\n")
	const pv, bv = "cfv_000000000000000000000001", "cfv_000000000000000000000002"
	f := &fakeConfig{resp: &configv1.GetSnapshotResponse{SnapshotVersion: "v1", SourceGrants: grants, Policy: bundle(t, "deny_sources := set()\n"),
		Pipelines: []*configv1.PipelineConfig{{Tenant: "credit", VersionId: "cfv_active"}}},
		candidates: map[string]*configv1.Canary{
			pv: {Object: "pipeline", Scope: "credit", VersionId: pv, Tenant: "credit", Percent: 100,
				Candidate: &configv1.Canary_Pipeline{Pipeline: &configv1.PipelineConfig{Tenant: "credit", VersionId: pv}}},
			bv: {Object: "policy", Scope: "tenant:credit", VersionId: bv, Percent: 100, Candidate: &configv1.Canary_Policy{Policy: cand}},
		}}
	now := time.Now()
	w := &Watcher{Client: f, Open: opened, Instance: "r-1", Every: time.Second, MaxAge: time.Hour,
		Log: logging.New(&bytes.Buffer{}, "t", slog.LevelInfo), Now: func() time.Time { return now }}
	if err := w.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	pc, err := w.WithCandidate(ctx, pv)
	if err != nil {
		t.Fatal(err)
	}
	// pipeline 候选只对它的租户
	if w.Plan(pc, "credit", "alice", "").GetVersionId() != pv || w.Plan(pc, "risk", "alice", "") != nil || w.Plan(ctx, "credit", "alice", "").GetVersionId() != "cfv_active" {
		t.Fatal("pipeline 候选")
	}
	bc, err := w.WithCandidate(ctx, bv)
	if err != nil {
		t.Fatal(err)
	}
	alice := policy.Input{Sub: "alice", Tenant: "credit", Dept: "信贷部", Clearance: 2}
	d, _ := w.Decide(bc, alice, []string{"src_a", "src_b"})
	if d.Version != cand.GetVersion() || len(d.Filter.Sources) != 1 {
		t.Fatalf("%+v", d)
	}
	// 缓存：再取同一版本不再问 governance，过了有效期才重取
	if _, err := w.WithCandidate(ctx, pv); err != nil || f.candidateAsks != 2 {
		t.Fatalf("%d %v", f.candidateAsks, err)
	}
	now = now.Add(candidateTTL + time.Second)
	if _, err := w.WithCandidate(ctx, pv); err != nil || f.candidateAsks != 3 {
		t.Fatalf("%d %v", f.candidateAsks, err)
	}
	// 取不到、不合法的版本：拒绝，不拿生效的配置冒充候选
	if _, err := w.WithCandidate(ctx, "cfv_000000000000000000000009"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("%v", err)
	}
	if _, err := w.WithCandidate(ctx, "rel_x"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("%v", err)
	}
}
