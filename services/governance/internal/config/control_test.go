package config

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

func TestObservationPromotesOrAbortsRollouts(t *testing.T) {
	s, ctx := seededPolicy(t)
	c := &RolloutControl{Configs: s}
	ci, pete := as(t, "ci-bot"), as(t, "pete")
	release := func(object, scope, content string, approvers ...string) string {
		t.Helper()
		r, err := s.CreateRelease(ci, &adminv1.CreateReleaseRequest{Object: object, Scope: scope, Content: content,
			SourceRef: "git:3f2a9c1", Reason: "x"})
		if err != nil {
			t.Fatal(err)
		}
		passGates(t, s)
		for _, who := range approvers {
			if _, err := s.ApproveRelease(as(t, who), &adminv1.ApproveReleaseRequest{ReleaseId: r.GetRelease().GetReleaseId()}); err != nil {
				t.Fatal(err)
			}
		}
		return r.GetRelease().GetReleaseId()
	}
	pl := release(ObjectPipeline, "credit", pipelineYAMLOK, "pete")
	pr := release(ObjectPrompt, "answer", promptOK, "pete")
	po := release(ObjectPolicy, "tenant:ops", "deny_sources := {\"mail\", \"wiki\"}\ntest_two if count(deny_sources) == 2\n", "pete", "ivy")

	list, err := c.ListRollouts(ctx, &configv1.ListRolloutsRequest{})
	if err != nil || len(list.GetRollouts()) != 3 {
		t.Fatalf("%v %v", list, err)
	}
	by := map[string]*configv1.ActiveRollout{}
	for _, r := range list.GetRollouts() {
		by[r.GetReleaseId()] = r
	}
	if r := by[pl]; r.GetTenant() != "credit" || r.GetPercent() != 10 || r.GetStartedAt() == nil {
		t.Fatalf("%v", r)
	}
	if r := by[pr]; r.GetTenant() != "ops" || r.GetPercent() != 100 {
		t.Fatalf("%v", r)
	}
	// orchestrator 的快照带 policy 灰度但不带候选包：只用来记提问人所在的灰度
	cs, _, err := s.Canaries(ctx, "orchestrator")
	if err != nil || len(cs) != 3 {
		t.Fatalf("%v %v", cs, err)
	}
	for _, c := range cs {
		if (c.GetObject() == ObjectPolicy) != (c.GetCandidate() == nil) {
			t.Fatalf("%v", c)
		}
	}
	rc, _, _ := s.Canaries(ctx, "retrieval")
	var candidate string
	for _, c := range rc {
		if c.GetObject() == ObjectPolicy {
			candidate = c.GetPolicy().GetVersion()
		}
	}
	report := func(id string, v configv1.Verdict, summary string) error {
		_, err := c.ReportObservation(ctx, &configv1.ReportObservationRequest{ReleaseId: id, VersionId: by[id].GetVersionId(),
			Verdict: v, Summary: summary})
		return err
	}
	// 继续观察：结果记在灰度上，console 能看到
	if err := report(pl, configv1.Verdict_VERDICT_CONTINUE, "候选 120 次、对照 1080 次"); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetRelease(pete, &adminv1.GetReleaseRequest{ReleaseId: pl})
	if ro := got.GetRelease().GetRollout(); ro.GetObservation() != "候选 120 次、对照 1080 次" || ro.GetObservedAt() == nil {
		t.Fatalf("%v", ro)
	}
	// 版本对不上（观察的不是这次灰度）不执行
	if _, err := c.ReportObservation(ctx, &configv1.ReportObservationRequest{ReleaseId: pl, VersionId: by[pr].GetVersionId(),
		Verdict: configv1.Verdict_VERDICT_PROMOTE, Summary: "x"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("%v", err)
	}
	if err := report(pl, configv1.Verdict_VERDICT_UNSPECIFIED, "x"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("%v", err)
	}
	// 观察期满没有超阈值：全量
	if err := report(pl, configv1.Verdict_VERDICT_PROMOTE, "覆盖率 0.91/0.90"); err != nil {
		t.Fatal(err)
	}
	if p, _, _ := s.Pipelines(ctx); len(p) != 1 {
		t.Fatal("自动全量后应生效")
	}
	// 超阈值：终止
	if err := report(pr, configv1.Verdict_VERDICT_ABORT, "拒答率 0.12，对照 0.03"); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetRelease(pete, &adminv1.GetReleaseRequest{ReleaseId: pr})
	if got.GetRelease().GetStatus() != "rolled_back" || got.GetRelease().GetRolledBackBy() != ObserverActor {
		t.Fatalf("%v", got)
	}
	if err := report(pr, configv1.Verdict_VERDICT_CONTINUE, "x"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("已终止的灰度不再记观察: %v", err)
	}
	// 策略包全量后生效的就是灰度时下发的候选包
	if err := report(po, configv1.Verdict_VERDICT_PROMOTE, "越权拦截 0/0"); err != nil {
		t.Fatal(err)
	}
	if b, _ := s.PolicyBundle(ctx); candidate == "" || b.GetVersion() != candidate {
		t.Fatalf("%s %s", b.GetVersion(), candidate)
	}
}
