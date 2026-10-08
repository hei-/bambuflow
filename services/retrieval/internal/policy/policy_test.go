package policy

import (
	"context"
	"os"
	"slices"
	"testing"
	"testing/fstest"
)

// 用仓库里的正式策略（config/policy）和测试数据，测的就是线上会加载的那份规则。
func bundle(t *testing.T, data string) fstest.MapFS {
	t.Helper()
	src, err := os.ReadFile("../../../../config/policy/authz.rego")
	if err != nil {
		t.Fatal(err)
	}
	fsys := fstest.MapFS{"authz.rego": {Data: src}}
	if data != "" {
		fsys["data.json"] = &fstest.MapFile{Data: []byte(data)}
	}
	return fsys
}

const tenants = `{"tenants": {
	"credit": {},
	"ops": {"denied_sources": ["mail"]}
}}`

// 租户授权由 governance 下发，不在策略数据里
var granted = []string{"wiki", "mail"}

func engine(t *testing.T) *Engine {
	t.Helper()
	e, err := Load(context.Background(), bundle(t, tenants))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestDecide(t *testing.T) {
	e := engine(t)
	alice := Input{Sub: "alice", Tenant: "credit", Dept: "信贷部", Clearance: 2, Groups: []string{"credit-l2"}}
	cases := []struct {
		name   string
		in     func(Input) Input
		allow  bool
		reason string
	}{
		{"完整身份", func(i Input) Input { return i }, true, "allowed"},
		{"缺部门", func(i Input) Input { i.Dept = ""; return i }, false, "principal_incomplete"},
		{"缺密级", func(i Input) Input { i.Clearance = 0; return i }, false, "principal_incomplete"},
		{"密级越界", func(i Input) Input { i.Clearance = 5; return i }, false, "principal_incomplete"},
		{"缺用户", func(i Input) Input { i.Sub = ""; return i }, false, "principal_incomplete"},
		{"未配置的租户", func(i Input) Input { i.Tenant = "hr"; return i }, false, "tenant_not_configured"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := e.Decide(context.Background(), c.in(alice), granted)
			if err != nil {
				t.Fatal(err)
			}
			if d.Allow != c.allow || d.Reason != c.reason {
				t.Fatalf("得到 allow=%v reason=%q", d.Allow, d.Reason)
			}
			if !d.Allow && (len(d.Filter.Sources) > 0 || len(d.Filter.ACLTokens) > 0) {
				t.Fatalf("拒绝时不应带过滤条件: %+v", d.Filter)
			}
		})
	}
}

func TestFilter(t *testing.T) {
	e := engine(t)
	d, err := e.Decide(context.Background(), Input{Sub: "alice", Tenant: "credit", Dept: "信贷部", Clearance: 2, Groups: []string{"credit-l2", "all-staff"}}, granted)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"user:alice", "group:credit-l2", "group:all-staff", "dept:信贷部"}
	if !slices.Equal(d.Filter.ACLTokens, want) || d.Filter.MaxClassification != 2 || !slices.Equal(d.Filter.Sources, []string{"wiki", "mail"}) {
		t.Fatalf("过滤条件不符: %+v", d.Filter)
	}

	// 禁止的数据源要从授权数据源里减掉
	d, err = e.Decide(context.Background(), Input{Sub: "eve", Tenant: "ops", Dept: "运营部", Clearance: 1}, granted)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(d.Filter.Sources, []string{"wiki"}) || !slices.Equal(d.Filter.ACLTokens, []string{"user:eve", "dept:运营部"}) {
		t.Fatalf("过滤条件不符: %+v", d.Filter)
	}
}

// 没有授权的租户：策略允许但没有可检索的数据源；授权之外的数据源不会因为策略数据而出现（fail-closed）
func TestOnlyGrantedSources(t *testing.T) {
	e := engine(t)
	alice := Input{Sub: "alice", Tenant: "credit", Dept: "信贷部", Clearance: 2}
	d, err := e.Decide(context.Background(), alice, nil)
	if err != nil || !d.Allow || len(d.Filter.Sources) != 0 {
		t.Fatalf("没有授权时不应有数据源: %+v %v", d, err)
	}
	if d, _ = e.Decide(context.Background(), alice, []string{"wiki"}); !slices.Equal(d.Filter.Sources, []string{"wiki"}) {
		t.Fatalf("只能是授权的数据源: %v", d.Filter.Sources)
	}
}

func TestLoadRejectsIncompleteBundle(t *testing.T) {
	ctx := context.Background()
	if _, err := Load(ctx, bundle(t, "")); err == nil {
		t.Fatal("缺 data.json 应当加载失败")
	}
	if _, err := Load(ctx, fstest.MapFS{"data.json": {Data: []byte(tenants)}}); err == nil {
		t.Fatal("没有 .rego 文件应当加载失败")
	}
	if _, err := Load(ctx, fstest.MapFS{"authz.rego": {Data: []byte("package bambuflow.authz\nimport rego.v1\ndecision := {")}, "data.json": {Data: []byte(tenants)}}); err == nil {
		t.Fatal("语法错误的策略应当加载失败")
	}
}

func TestVersionTracksContent(t *testing.T) {
	ctx := context.Background()
	a, err := Load(ctx, bundle(t, tenants))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Load(ctx, bundle(t, `{"tenants": {}}`))
	if err != nil {
		t.Fatal(err)
	}
	if a.Version() == b.Version() || a.Version() != engine(t).Version() {
		t.Fatalf("版本号应随内容变化、内容相同则相同: %s %s", a.Version(), b.Version())
	}
}

func TestRegionAndRedactionObligations(t *testing.T) {
	data := `{"tenants": {"credit": {"pii_viewer_groups": ["kyc-ops"]}},
		"source_regions": {"east": ["华东"]}}`
	e, err := Load(context.Background(), bundle(t, data))
	if err != nil {
		t.Fatal(err)
	}
	in := Input{Sub: "alice", Tenant: "credit", Dept: "信贷部", Clearance: 2, Region: "华东", Groups: []string{"credit-l2"}}
	east := []string{"wiki", "east"}
	d, err := e.Decide(context.Background(), in, east)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(d.Filter.Sources, []string{"wiki", "east"}) || d.Obligations.Redaction != "mask" {
		t.Fatalf("华东用户: %+v", d)
	}
	in.Region = "华北"
	if d, _ = e.Decide(context.Background(), in, east); !slices.Equal(d.Filter.Sources, []string{"wiki"}) {
		t.Fatalf("限定地域的数据源应对其他地域排除: %v", d.Filter.Sources)
	}
	in.Groups = append(in.Groups, "kyc-ops")
	if d, _ = e.Decide(context.Background(), in, east); d.Obligations.Redaction != "none" {
		t.Fatalf("可看 PII 的群组应看原文: %+v", d.Obligations)
	}
}
