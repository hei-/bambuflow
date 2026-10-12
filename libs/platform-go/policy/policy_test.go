package policy

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
)

// 用仓库里的正式全局基线（config/policies/global.rego），测的就是线上会加载的那份规则。
func global(t *testing.T) Layer {
	t.Helper()
	src, err := os.ReadFile("../../../config/policies/global.rego")
	if err != nil {
		t.Fatal(err)
	}
	return Layer{Scope: ScopeGlobal, Body: string(src)}
}

const tested = "\ntest_layer if true\n"

func engine(t *testing.T, layers ...Layer) *Engine {
	t.Helper()
	b, err := Build(append([]Layer{global(t)}, layers...))
	if err != nil {
		t.Fatal(err)
	}
	e, err := Compile(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

var standard = []Layer{
	{Scope: "tenant:credit", Body: "pii_viewer_groups := {\"kyc-ops\"}\n" + tested},
	{Scope: "tenant:ops", Body: "deny_sources := {\"mail\"}\n" + tested},
	{Scope: "source:east", Body: "deny contains \"region\" if not input.principal.region in {\"华东\"}\n" + tested},
}

func alice() Input {
	return Input{Sub: "alice", Tenant: "credit", Dept: "信贷部", Clearance: 2, Region: "华北", Groups: []string{"credit-l2"}}
}

func TestDecideComposesLayers(t *testing.T) {
	e := engine(t, standard...)
	ctx := context.Background()
	d, err := e.Decide(ctx, alice(), []string{"wiki", "east"})
	if err != nil || !d.Allow {
		t.Fatalf("%v %v", d, err)
	}
	// 数据源层的地域限定排除 east；PII 默认掩码
	if !slices.Equal(d.Filter.Sources, []string{"wiki"}) || d.Filter.MaxClassification != 2 || d.Obligations.Redaction != "mask" {
		t.Fatalf("%+v", d)
	}
	in := alice()
	in.Region, in.Groups = "华东", []string{"kyc-ops"}
	if d, _ := e.Decide(ctx, in, []string{"wiki", "east"}); len(d.Filter.Sources) != 2 || d.Obligations.Redaction != "none" {
		t.Fatalf("华东、可看 PII 的群组: %+v", d)
	}
	ops := alice()
	ops.Tenant = "ops"
	if d, _ := e.Decide(ctx, ops, []string{"wiki", "mail"}); !slices.Equal(d.Filter.Sources, []string{"wiki"}) {
		t.Fatalf("租户层禁止的数据源: %+v", d)
	}
	other := alice()
	other.Tenant = "risk"
	if d, _ := e.Decide(ctx, other, []string{"wiki"}); d.Allow || d.Reason != "tenant_not_configured" {
		t.Fatalf("没有租户层: %+v", d)
	}
}

func TestBuildRejectsMalformedBundles(t *testing.T) {
	g := global(t)
	cases := map[string][]Layer{
		"缺全局基线":  {{Scope: "tenant:credit", Body: tested}},
		"作用域不合法": {g, {Scope: "tenant:Credit", Body: tested}},
		"重复":     {g, {Scope: "tenant:credit", Body: tested}, {Scope: "tenant:credit", Body: tested}},
		"自带包声明":  {g, {Scope: "tenant:credit", Body: "package bambuflow.authz\ndecision := {\"allow\": true}\n"}},
	}
	for name, ls := range cases {
		if _, err := Build(ls); err == nil {
			t.Errorf("%s 应拒绝", name)
		}
	}
	b, _ := Build([]Layer{g, {Scope: "tenant:credit", Body: "decision := {\n"}})
	if _, err := Compile(context.Background(), b); err == nil {
		t.Fatal("语法错误应编译失败")
	}
}

func TestVersionTracksContentNotOrder(t *testing.T) {
	g := global(t)
	a, _ := Build([]Layer{standard[0], g, standard[1]})
	b, _ := Build([]Layer{g, standard[1], standard[0]})
	c, _ := Build([]Layer{g, standard[0]})
	if a.Version != b.Version || a.Version == c.Version || !strings.HasPrefix(a.Version, "sha256:") {
		t.Fatalf("%s %s %s", a.Version, b.Version, c.Version)
	}
	if a.Modules[0].Path != "global.rego" || !strings.HasPrefix(a.Modules[1].Source, "package bambuflow.tenants[\"credit\"]") {
		t.Fatalf("%v", a.Modules)
	}
}

func TestRunsLayerTests(t *testing.T) {
	ctx := context.Background()
	b, _ := Build(append([]Layer{global(t)}, standard...))
	if err := Test(ctx, b); err != nil {
		t.Fatal(err)
	}
	b, _ = Build([]Layer{global(t), {Scope: "tenant:credit", Body: "deny_sources := set()\n"}})
	if err := Test(ctx, b); err == nil || !strings.Contains(err.Error(), "tenant/credit") {
		t.Fatalf("没有测试的层应拒绝: %v", err)
	}
	b, _ = Build([]Layer{global(t), {Scope: "tenant:credit", Body: "deny_sources := set()\ntest_wrong if count(deny_sources) == 1\n"}})
	if err := Test(ctx, b); err == nil {
		t.Fatal("测试失败应拒绝")
	}
}

func TestCoreConstraints(t *testing.T) {
	ctx := context.Background()
	granted := []string{"wiki", "east"}
	if err := CheckCore(ctx, engine(t, standard...), []string{"credit", "ops"}, granted); err != nil {
		t.Fatal(err)
	}
	// 全局基线写错：放行时把密级上限写死成受限级
	g := global(t)
	g.Body = strings.Replace(g.Body, `"max_classification": input.principal.clearance`, `"max_classification": 4`, 1)
	b, _ := Build([]Layer{g, standard[0]})
	bad, err := Compile(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckCore(ctx, bad, []string{"credit"}, granted); err == nil || !strings.Contains(err.Error(), "密级上限") {
		t.Fatalf("应拦住: %v", err)
	}
}

func TestWidenedListsWhoSeesMore(t *testing.T) {
	ctx := context.Background()
	old := engine(t, standard...)
	looser := slices.Clone(standard)
	looser[1] = Layer{Scope: "tenant:ops", Body: "deny_sources := set()\npii_viewer_groups := {\"ops-kyc\"}\n" + tested}
	got, err := Widened(ctx, old, engine(t, looser...), []string{"credit", "ops"}, []string{"wiki", "mail"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(got, "\n")
	if !strings.Contains(joined, "租户 ops、密级 1：新增可检索数据源 mail") || !strings.Contains(joined, "群组 ops-kyc：") ||
		!strings.Contains(joined, "PII 由掩码变为原文") || strings.Contains(joined, "租户 credit") {
		t.Fatalf("%s", joined)
	}
	// 只收紧时没有变大的主体
	if got, _ := Widened(ctx, old, old, []string{"credit", "ops"}, []string{"wiki"}); len(got) != 0 {
		t.Fatalf("%v", got)
	}
}

func TestLoadLayersFromSeedDirs(t *testing.T) {
	layers, err := LoadLayers("../../../config/policies", "../../../tools/test-upstream/policies")
	if err != nil {
		t.Fatal(err)
	}
	var scopes []string
	for _, l := range layers {
		scopes = append(scopes, l.Scope)
	}
	slices.Sort(scopes)
	if !slices.Equal(scopes, []string{"global", "tenant:bench", "tenant:credit", "tenant:ops", "tenant:risk"}) {
		t.Fatalf("%v", scopes)
	}
	// 开发与测试用的整套策略包也要过测试与核心约束
	b, err := Build(layers)
	if err != nil {
		t.Fatal(err)
	}
	if err := Test(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	e, _ := Compile(context.Background(), b)
	if err := CheckCore(context.Background(), e, []string{"bench", "credit", "ops", "risk"}, []string{"src_evalcredit", "wiki"}); err != nil {
		t.Fatal(err)
	}
}
