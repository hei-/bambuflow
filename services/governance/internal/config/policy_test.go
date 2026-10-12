package config

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/hei-/bambuflow/libs/platform-go/policy"
)

func seededPolicy(t *testing.T) (*Server, context.Context) {
	t.Helper()
	s, _, _ := newServer()
	ctx := context.Background()
	if _, err := s.Bootstrap(ctx, map[string][]byte{"ops": []byte(tenantYAML("ops", "pilot")), "credit": []byte(tenantYAML("credit", "general"))}); err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile("../../../../config/policies/global.rego")
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.BootstrapPolicy(ctx, []policy.Layer{
		{Scope: policy.ScopeGlobal, Body: string(src)},
		{Scope: "tenant:ops", Body: "deny_sources := {\"mail\"}\ntest_mail if \"mail\" in deny_sources\n"},
	})
	if err != nil || n != 2 {
		t.Fatalf("%d %v", n, err)
	}
	return s, ctx
}

func TestPolicyBootstrapAndBundle(t *testing.T) {
	s, ctx := seededPolicy(t)
	b, err := s.PolicyBundle(ctx)
	if err != nil || !strings.HasPrefix(b.GetVersion(), "sha256:") || len(b.GetModules()) != 2 {
		t.Fatalf("%v %v", b, err)
	}
	// 再次启动：已有的层不动
	if n, err := s.BootstrapPolicy(ctx, []policy.Layer{{Scope: "tenant:ops", Body: "deny_sources := set()\ntest_x if true\n"}}); err != nil || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
	// 租户层要求租户配置已生效；不合法的层整批不导入
	if _, err := s.BootstrapPolicy(ctx, []policy.Layer{{Scope: "tenant:nobody", Body: "test_x if true\n"}}); err == nil {
		t.Fatal("没有租户配置的租户层应拒绝")
	}
	// 没有全局基线时不下发策略包：retrieval 拒绝全部检索
	empty, _, _ := newServer()
	if b, err := empty.PolicyBundle(ctx); err != nil || b != nil {
		t.Fatalf("%v %v", b, err)
	}
}

func TestPolicyReleaseShowsWideningAndNeedsGate(t *testing.T) {
	s, _ := seededPolicy(t)
	ci, pete := as(t, "ci-bot"), as(t, "pete")
	release := func(scope, body string) (*adminv1.CreateReleaseResponse, error) {
		return s.CreateRelease(ci, &adminv1.CreateReleaseRequest{Object: ObjectPolicy, Scope: scope, Content: body,
			SourceRef: "git:3f2a9c1", Reason: "放开邮件"})
	}
	looser := "deny_sources := set()\ntest_none if count(deny_sources) == 0\n"
	for name, body := range map[string]string{
		"测试失败":  "deny_sources := set()\ntest_wrong if count(deny_sources) == 1\n",
		"没有测试":  "deny_sources := set()\n",
		"自带包声明": "package bambuflow.authz\ndecision := {\"allow\": true}\ntest_x if true\n",
		"语法错误":  "deny_sources := {\n",
	} {
		if _, err := release("tenant:ops", body); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s 应拒绝: %v", name, err)
		}
	}
	if _, err := release("tenant:nobody", looser); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("没有租户配置: %v", err)
	}
	// 全局基线写错（密级上限写死成受限级）：它自己的测试先拦住；测试也一并改掉时核心约束拦住
	src, _ := os.ReadFile("../../../../config/policies/global.rego")
	bad := strings.Replace(string(src), `"max_classification": input.principal.clearance`, `"max_classification": 4`, 1)
	if _, err := release(policy.ScopeGlobal, bad); status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "策略测试失败") {
		t.Fatalf("测试: %v", err)
	}
	bad = strings.Replace(bad, "d.filter.max_classification == 2", "d.filter.max_classification == 4", 1)
	if _, err := release(policy.ScopeGlobal, bad); status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "核心约束") {
		t.Fatalf("核心约束: %v", err)
	}
	rel, err := release("tenant:ops", looser)
	if err != nil || rel.GetRelease().GetStatus() != "gating" {
		t.Fatalf("策略层先过门禁: %v %v", rel, err)
	}
	// 门禁只跑这个租户的权限题
	if runs := s.Evals.(*fakeEvals).runs; len(runs) != 0 {
		t.Fatalf("门禁在后台发起: %v", runs)
	}
	passGates(t, s)
	if run := s.Evals.(*fakeEvals).runs[0]; run.GetRuleset() != "permission" || !slices.Equal(run.GetTenants(), []string{"ops"}) ||
		run.GetCandidate().GetConfigVersion() != rel.GetRelease().GetVersion().GetVersionId() {
		t.Fatalf("%v", run)
	}
	widened := strings.Join(rel.GetRelease().GetVersion().GetWidened(), "\n")
	if !strings.Contains(widened, "租户 ops") || strings.Contains(widened, "租户 credit") {
		t.Fatalf("差异分析应列出 ops 多看到的: %s", widened)
	}
	before, _ := s.PolicyBundle(context.Background())
	id := rel.GetRelease().GetReleaseId()
	approve := func(who string) (*adminv1.ApproveReleaseResponse, error) {
		return s.ApproveRelease(as(t, who), &adminv1.ApproveReleaseRequest{ReleaseId: id})
	}
	// 策略层要两位审批人：第一位审批后还不生效
	first, err := approve("pete")
	if err != nil || first.GetRelease().GetStatus() != "pending_second_approval" {
		t.Fatalf("%v %v", first, err)
	}
	if b, _ := s.PolicyBundle(context.Background()); b.GetVersion() != before.GetVersion() {
		t.Fatal("一位审批后不应生效")
	}
	if _, r := code(func() error { _, err := approve("pete"); return err }()); r != "self_approval_forbidden" {
		t.Fatalf("同一人不能审批两次: %s", r)
	}
	// 两位都是平台管理员不够：至少一位是信息安全或数据属主
	if _, r := code(func() error { _, err := approve("paul"); return err }()); r != "security_approval_required" {
		t.Fatalf("%s", r)
	}
	done, err := approve("ivy")
	if err != nil || done.GetRelease().GetStatus() != "rolling_out" || done.GetRelease().GetSecondApprovedBy() != "ivy" {
		t.Fatalf("%v %v", done, err)
	}
	if _, err := s.PromoteRelease(as(t, "pete"), &adminv1.PromoteReleaseRequest{ReleaseId: id}); err != nil {
		t.Fatal(err)
	}
	after, _ := s.PolicyBundle(context.Background())
	if after.GetVersion() == before.GetVersion() {
		t.Fatal("生效后策略包版本应变化")
	}
	_ = pete
	// 只收紧的改动没有变大的主体
	tighter, err := release("tenant:ops", "deny_sources := {\"mail\", \"wiki\"}\ntest_two if count(deny_sources) == 2\n")
	if err != nil || len(tighter.GetRelease().GetVersion().GetWidened()) != 0 {
		t.Fatalf("%v %v", tighter, err)
	}
}

func TestStaleApprovalsExpireAfterSevenDays(t *testing.T) {
	s, ctx := seededPolicy(t)
	now := time.Now()
	s.Now = func() time.Time { return now }
	rel, err := s.CreateRelease(as(t, "ci-bot"), &adminv1.CreateReleaseRequest{Object: ObjectPolicy, Scope: "tenant:ops",
		Content: "deny_sources := set()\ntest_x if true\n", SourceRef: "git:3f2a9c1", Reason: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.ExpireStale(ctx); err != nil || n != 0 {
		t.Fatalf("还没到期: %d %v", n, err)
	}
	now = now.Add(ApprovalTimeout + time.Minute)
	if n, err := s.ExpireStale(ctx); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	got, _ := s.GetRelease(as(t, "pete"), &adminv1.GetReleaseRequest{ReleaseId: rel.GetRelease().GetReleaseId()})
	if got.GetRelease().GetStatus() != "expired" {
		t.Fatalf("%v", got)
	}
	if _, r := code(func() error {
		_, err := s.ApproveRelease(as(t, "ivy"), &adminv1.ApproveReleaseRequest{ReleaseId: rel.GetRelease().GetReleaseId()})
		return err
	}()); r != "conflict" {
		t.Fatalf("过期的发布不能再审批: %s", r)
	}
	// 信息安全只能审批策略层
	pl, err := s.CreateRelease(as(t, "ci-bot"), &adminv1.CreateReleaseRequest{Object: ObjectPipeline, Scope: "ops",
		Content: pipelineYAMLOK, SourceRef: "git:3f2a9c1", Reason: "x"})
	if err != nil {
		t.Fatal(err)
	}
	passGates(t, s)
	if _, err := s.ApproveRelease(as(t, "ivy"), &adminv1.ApproveReleaseRequest{ReleaseId: pl.GetRelease().GetReleaseId()}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("%v", err)
	}
}
