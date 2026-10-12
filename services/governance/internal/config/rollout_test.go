package config

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

func TestRolloutRangesPromoteAbortAndRollbackPreemption(t *testing.T) {
	s, _, _ := newServer()
	ctx := context.Background()
	if _, err := s.Bootstrap(ctx, map[string][]byte{"ops": []byte(tenantYAML("ops", "pilot")),
		"credit": []byte(tenantYAML("credit", "general"))}); err != nil {
		t.Fatal(err)
	}
	ci, pete := as(t, "ci-bot"), as(t, "pete")
	release := func(object, scope, content, canary string) string {
		t.Helper()
		r, err := s.CreateRelease(ci, &adminv1.CreateReleaseRequest{Object: object, Scope: scope, Content: content,
			SourceRef: "git:3f2a9c1", Reason: "x", CanaryTenant: canary})
		if err != nil {
			t.Fatal(err)
		}
		passGates(t, s)
		a, err := s.ApproveRelease(pete, &adminv1.ApproveReleaseRequest{ReleaseId: r.GetRelease().GetReleaseId()})
		if err != nil {
			t.Fatal(err)
		}
		return a.GetRelease().GetReleaseId()
	}
	// 租户对象：该租户 10% 的用户；全局对象：第一个试点阶段的租户（ops）的全部用户
	pl := release(ObjectPipeline, "credit", pipelineYAMLOK, "")
	rt := release(ObjectRouting, ScopeGlobal, routingYAMLOK, "")
	got, _ := s.GetRelease(pete, &adminv1.GetReleaseRequest{ReleaseId: pl})
	if r := got.GetRelease().GetRollout(); got.GetRelease().GetStatus() != "rolling_out" || r.GetTenant() != "credit" || r.GetPercent() != 10 {
		t.Fatalf("%v", got)
	}
	got, _ = s.GetRelease(pete, &adminv1.GetReleaseRequest{ReleaseId: rt})
	if r := got.GetRelease().GetRollout(); r.GetTenant() != "ops" || r.GetPercent() != 100 {
		t.Fatalf("%v", got)
	}
	// 灰度中不切生效指针；快照里有候选版本
	if p, _, _ := s.Pipelines(ctx); len(p) != 0 {
		t.Fatal("灰度中不应全量生效")
	}
	cs, digest, err := s.Canaries(ctx, "orchestrator")
	if err != nil || len(cs) != 2 || digest == "" || cs[0].GetPipeline().GetTenant() != "credit" || cs[1].GetRouting() == nil {
		t.Fatalf("%v %v", cs, err)
	}
	if rc, _, _ := s.Canaries(ctx, "retrieval"); len(rc) != 1 || rc[0].GetObject() != ObjectPipeline {
		t.Fatalf("retrieval 只拿 pipeline 与 policy: %v", rc)
	}
	// 同一对象同时只有一个灰度
	r2, _ := s.CreateRelease(ci, &adminv1.CreateReleaseRequest{Object: ObjectPipeline, Scope: "credit", Content: "answer:\n  min_coverage: 0.9\n",
		SourceRef: "git:3f2a9c1", Reason: "y"})
	passGates(t, s)
	if _, r := code(func() error {
		_, err := s.ApproveRelease(pete, &adminv1.ApproveReleaseRequest{ReleaseId: r2.GetRelease().GetReleaseId()})
		return err
	}()); r != "conflict" {
		t.Fatalf("%s", r)
	}
	// 全量：切生效指针
	if _, err := s.PromoteRelease(pete, &adminv1.PromoteReleaseRequest{ReleaseId: pl}); err != nil {
		t.Fatal(err)
	}
	if p, _, _ := s.Pipelines(ctx); len(p) != 1 {
		t.Fatal("全量后生效")
	}
	// 终止：候选版本不再生效
	ab, err := s.AbortRollout(pete, &adminv1.AbortRolloutRequest{ReleaseId: rt, Reason: "拒答率升高"})
	if err != nil || ab.GetRelease().GetStatus() != "rolled_back" {
		t.Fatalf("%v %v", ab, err)
	}
	if r, _, _ := s.Routing(ctx); r != nil {
		t.Fatal("终止灰度后不应生效")
	}
	if _, err := s.PromoteRelease(pete, &adminv1.PromoteReleaseRequest{ReleaseId: rt}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("已终止的不能全量: %v", err)
	}
	// 回退插队：回退生效版本时终止同一对象正在进行的灰度
	canary := release(ObjectPipeline, "credit", "answer:\n  min_coverage: 0.95\n", "")
	if _, err := s.RollbackRelease(pete, &adminv1.RollbackReleaseRequest{ReleaseId: pl, Reason: "阈值不对"}); err == nil {
		t.Fatal("第一个版本没有可回退的版本")
	}
	got, _ = s.GetRelease(pete, &adminv1.GetReleaseRequest{ReleaseId: canary})
	if got.GetRelease().GetStatus() != "rolling_out" {
		t.Fatalf("回退失败时灰度不受影响: %v", got)
	}
	if _, err := s.PromoteRelease(pete, &adminv1.PromoteReleaseRequest{ReleaseId: canary}); err != nil {
		t.Fatal(err)
	}
	next := release(ObjectPipeline, "credit", "answer:\n  min_coverage: 0.9\n", "")
	if _, err := s.RollbackRelease(pete, &adminv1.RollbackReleaseRequest{ReleaseId: canary, Reason: "覆盖率门槛太高"}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetRelease(pete, &adminv1.GetReleaseRequest{ReleaseId: next})
	if got.GetRelease().GetStatus() != "rolled_back" {
		t.Fatalf("回退应终止正在进行的灰度: %v", got)
	}
	if cs, _, _ := s.Canaries(ctx, "orchestrator"); len(cs) != 0 {
		t.Fatalf("%v", cs)
	}
	// 指定的试点租户要有租户配置
	bad, _ := s.CreateRelease(ci, &adminv1.CreateReleaseRequest{Object: ObjectPrompt, Scope: "answer", Content: promptOK,
		SourceRef: "git:3f2a9c1", Reason: "z", CanaryTenant: "nobody"})
	passGates(t, s)
	if _, err := s.ApproveRelease(pete, &adminv1.ApproveReleaseRequest{ReleaseId: bad.GetRelease().GetReleaseId()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("%v", err)
	}
}
