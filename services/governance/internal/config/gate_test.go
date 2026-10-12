package config

import (
	"context"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	evalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/evaluation/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

func TestReleasesPassTheirOwnGateBeforeApproval(t *testing.T) {
	s, _, audit := newServer()
	ctx := context.Background()
	if _, err := s.Bootstrap(ctx, map[string][]byte{"ops": []byte(tenantYAML("ops", "pilot"))}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	s.Now = func() time.Time { return now }
	evals := s.Evals.(*fakeEvals)
	ci, pete := as(t, "ci-bot"), as(t, "pete")
	release := func(object, scope, content string) *adminv1.Release {
		t.Helper()
		// 请求里附的门禁不用：门禁要评的是这个候选
		r, err := s.CreateRelease(ci, &adminv1.CreateReleaseRequest{Object: object, Scope: scope, Content: content,
			SourceRef: "git:3f2a9c1", Reason: "x", GateId: passedGate})
		if err != nil || r.GetRelease().GetStatus() != "gating" || r.GetRelease().GetGateId() != "" {
			t.Fatalf("%v %v", r, err)
		}
		return r.GetRelease()
	}
	statusOf := func(id string) *adminv1.Release {
		got, err := s.GetRelease(pete, &adminv1.GetReleaseRequest{ReleaseId: id})
		if err != nil {
			t.Fatal(err)
		}
		return got.GetRelease()
	}
	pl := release(ObjectPipeline, "ops", pipelineYAMLOK)
	rt := release(ObjectRouting, ScopeGlobal, routingYAMLOK)
	// 门禁中不能审批
	if _, r := code(func() error {
		_, err := s.ApproveRelease(pete, &adminv1.ApproveReleaseRequest{ReleaseId: pl.GetReleaseId()})
		return err
	}()); r != "conflict" {
		t.Fatalf("%s", r)
	}
	// evaluation 不可用：停在门禁中，下一轮再发起，不跳过
	evals.runErr = status.Error(codes.Unavailable, "down")
	if n, err := s.AdvanceGates(ctx); err != nil || n != 0 || statusOf(pl.GetReleaseId()).GetStatus() != "gating" {
		t.Fatalf("%d %v", n, err)
	}
	evals.runErr = nil
	evals.verdicts = map[string]evalv1.Verdict{rt.GetVersion().GetVersionId(): evalv1.Verdict_VERDICT_FAILED}
	if _, err := s.AdvanceGates(ctx); err != nil {
		t.Fatal(err)
	}
	// pipeline 只评它的租户，routing 评全部租户；幂等键是发布 ID，候选是这个版本
	if len(evals.runs) != 2 {
		t.Fatalf("%v", evals.runs)
	}
	for _, run := range evals.runs {
		want := map[string][]string{pl.GetReleaseId(): {"ops"}, rt.GetReleaseId(): nil}[run.GetIdempotencyKey()]
		if run.GetTrigger() != "config_release" || !slices.Equal(run.GetTenants(), want) || run.GetRuleset() != "" ||
			!slices.Contains([]string{pl.GetVersion().GetVersionId(), rt.GetVersion().GetVersionId()}, run.GetCandidate().GetConfigVersion()) {
			t.Fatalf("%v", run)
		}
	}
	if g := statusOf(pl.GetReleaseId()); g.GetStatus() != "gating" || g.GetGateId() == "" {
		t.Fatalf("门禁已发起、还没有结论: %v", g)
	}
	if n, err := s.AdvanceGates(ctx); err != nil || n != 2 {
		t.Fatalf("%d %v", n, err)
	}
	if statusOf(pl.GetReleaseId()).GetStatus() != "pending_approval" || statusOf(rt.GetReleaseId()).GetStatus() != "gate_failed" {
		t.Fatal("通过的待审批，不通过的 gate_failed")
	}
	if _, err := s.ApproveRelease(pete, &adminv1.ApproveReleaseRequest{ReleaseId: pl.GetReleaseId()}); err != nil {
		t.Fatal(err)
	}
	if _, r := code(func() error {
		_, err := s.ApproveRelease(pete, &adminv1.ApproveReleaseRequest{ReleaseId: rt.GetReleaseId()})
		return err
	}()); r != "conflict" {
		t.Fatalf("门禁不通过的不能审批: %s", r)
	}
	for _, a := range []string{"config.release_gate_started", "config.release_gate_passed", "config.release_gate_failed"} {
		if !slices.Contains(audit.actions, a) {
			t.Fatalf("审计缺 %s: %v", a, audit.actions)
		}
	}
	// 一直没有结论：超时按不通过
	pr := release(ObjectPrompt, "answer", promptOK)
	evals.verdicts[pr.GetVersion().GetVersionId()] = evalv1.Verdict_VERDICT_RUNNING
	if _, err := s.AdvanceGates(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(GateTimeout - time.Minute)
	if _, err := s.AdvanceGates(ctx); err != nil || statusOf(pr.GetReleaseId()).GetStatus() != "gating" {
		t.Fatal("还没到超时")
	}
	now = now.Add(2 * time.Minute)
	if _, err := s.AdvanceGates(ctx); err != nil || statusOf(pr.GetReleaseId()).GetStatus() != "gate_failed" {
		t.Fatal("超时应按不通过")
	}
	// 门禁发不起来（评测集里没有这个租户的题）：按不通过
	evals.runErr = status.Error(codes.FailedPrecondition, "评测集里没有这些租户的题")
	pl2 := release(ObjectPipeline, "ops", "answer:\n  min_coverage: 0.9\n")
	if n, _ := s.AdvanceGates(ctx); n != 1 || statusOf(pl2.GetReleaseId()).GetStatus() != "gate_failed" {
		t.Fatal("门禁无法发起应按不通过")
	}
}

func TestCandidateServesTheVersionToEachService(t *testing.T) {
	s, ctx := seededPolicy(t)
	ci := as(t, "ci-bot")
	version := func(object, scope, content string) string {
		t.Helper()
		r, err := s.CreateRelease(ci, &adminv1.CreateReleaseRequest{Object: object, Scope: scope, Content: content,
			SourceRef: "git:3f2a9c1", Reason: "x"})
		if err != nil {
			t.Fatal(err)
		}
		return r.GetRelease().GetVersion().GetVersionId()
	}
	pl := version(ObjectPipeline, "credit", pipelineYAMLOK)
	po := version(ObjectPolicy, "tenant:ops", "deny_sources := {\"mail\", \"wiki\"}\ntest_two if count(deny_sources) == 2\n")
	c, err := s.Candidate(ctx, "retrieval", pl)
	if err != nil || c.GetPipeline().GetVersionId() != pl || c.GetTenant() != "credit" || c.GetPercent() != 100 || c.GetReleaseId() != "" {
		t.Fatalf("pipeline 只对它的租户: %v %v", c, err)
	}
	// 策略包的候选是替换这一层后合成的整个包，在 retrieval；orchestrator 用不到它，只拿到不带内容的一项
	if c, err := s.Candidate(ctx, "retrieval", po); err != nil || c.GetPolicy() == nil || c.GetTenant() != "" {
		t.Fatalf("%v %v", c, err)
	}
	if c, err := s.Candidate(ctx, "orchestrator", po); err != nil || c.GetCandidate() != nil || c.GetObject() != ObjectPolicy {
		t.Fatalf("%v %v", c, err)
	}
	if _, err := s.Candidate(ctx, "gateway", pl); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("%v", err)
	}
	if _, err := s.Candidate(ctx, "orchestrator", "cfv_000000000000000000000000"); status.Code(err) != codes.NotFound {
		t.Fatalf("%v", err)
	}
}

func TestExperimentVariantsMustHavePassedTheirGate(t *testing.T) {
	s, _, _ := newServer()
	ctx := context.Background()
	if _, err := s.Bootstrap(ctx, map[string][]byte{"ops": []byte(tenantYAML("ops", "pilot"))}); err != nil {
		t.Fatal(err)
	}
	evals := s.Evals.(*fakeEvals)
	ci := as(t, "ci-bot")
	release := func(object, scope, content string) string {
		t.Helper()
		r, err := s.CreateRelease(ci, &adminv1.CreateReleaseRequest{Object: object, Scope: scope, Content: content,
			SourceRef: "git:3f2a9c1", Reason: "x"})
		if err != nil {
			t.Fatal(err)
		}
		return r.GetRelease().GetVersion().GetVersionId()
	}
	pl := release(ObjectPipeline, "ops", pipelineYAMLOK)
	rt := release(ObjectRouting, ScopeGlobal, "generation_model: qwen2.5-72b-instruct\n")
	bad := release(ObjectPrompt, "answer", promptOK)
	evals.verdicts = map[string]evalv1.Verdict{bad: evalv1.Verdict_VERDICT_FAILED}
	// 门禁还没结论：不能做实验
	if _, err := s.Variants(ctx, "ops", []string{pl}, true); grpcmw.ReasonOf(err) != "gate_failed" {
		t.Fatalf("门禁中的版本不能做实验：%v", err)
	}
	passGates(t, s)
	got, err := s.Variants(ctx, "ops", []string{pl, rt}, true)
	if err != nil || len(got) != 2 || got[0].GetPipeline().GetAnswer().GetAgentMaxRounds() != 2 ||
		got[1].GetRouting().GetGenerationModel() != "qwen2.5-72b-instruct" {
		t.Fatalf("门禁通过、待审批的版本可以做实验：%v %v", got, err)
	}
	for name, c := range map[string]struct {
		tenant string
		ids    []string
		reason string
	}{
		"门禁没通过":          {"ops", []string{bad}, "gate_failed"},
		"别的租户的 pipeline": {"risk", []string{pl}, ""},
		"同类对象两个":         {"ops", []string{rt, rt}, ""},
	} {
		_, err := s.Variants(ctx, c.tenant, c.ids, true)
		if c.reason != "" && grpcmw.ReasonOf(err) != c.reason || c.reason == "" && status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s：%v", name, err)
		}
	}
	// 下发时不核对：门禁没过的也能取到内容（立项时已拦下）
	if _, err := s.Variants(ctx, "ops", []string{bad}, false); err != nil {
		t.Fatal(err)
	}
}
