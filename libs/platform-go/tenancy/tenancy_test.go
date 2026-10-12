package tenancy

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTenants(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name, "tenant.yaml"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestLoadRejectsBadConfig(t *testing.T) {
	cases := map[string]struct {
		files    map[string]string
		platform []string
		want     string
	}{
		"同一客户端绑定两个租户": {map[string]string{
			"a": "tenant: a\nrollout: general\nbound_clients: [app]\n",
			"b": "tenant: b\nrollout: general\nbound_clients: [app]\n",
		}, nil, "只能绑定一个租户"},
		"平台级客户端又被绑定": {map[string]string{
			"a": "tenant: a\nrollout: general\nbound_clients: [console]\n",
		}, []string{"console"}, "只能绑定一个租户"},
		"rollout 取值不对":   {map[string]string{"a": "tenant: a\nrollout: beta\n"}, nil, "rollout"},
		"tenant 与目录名不一致": {map[string]string{"a": "tenant: b\nrollout: general\n"}, nil, "应与租户名一致"},
		"agent 同时是业务系统客户端": {map[string]string{
			"a": "tenant: a\nrollout: general\nbound_clients: [app]\n",
			"b": "tenant: b\nrollout: general\nmcp_agents: [{client_id: app, purpose: p, owner: o}]\n",
		}, nil, "只能登记为一个租户的 agent"},
		"agent 是平台级客户端": {map[string]string{
			"a": "tenant: a\nrollout: general\nmcp_agents: [{client_id: console, purpose: p, owner: o}]\n",
		}, []string{"console"}, "只能登记为一个租户的 agent"},
		"agent 登记两次": {map[string]string{
			"a": "tenant: a\nrollout: general\nmcp_agents: [{client_id: bot, purpose: p, owner: o}]\n",
			"b": "tenant: b\nrollout: general\nmcp_agents: [{client_id: bot, purpose: p, owner: o}]\n",
		}, nil, "只能登记为一个租户的 agent"},
		"agent 登记缺负责人": {map[string]string{
			"a": "tenant: a\nrollout: general\nmcp_agents: [{client_id: bot, purpose: p}]\n",
		}, nil, "owner"},
		"未知的 MCP 工具": {map[string]string{"a": "tenant: a\nrollout: general\nmcp_tools: [sql_query]\n"}, nil, "未知的 MCP 工具"},
		"配额为负":       {map[string]string{"a": "tenant: a\nrollout: general\nquota: {daily_tokens: -1}\n"}, nil, "quota"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeTenants(t, c.files), c.platform)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("应报错（含 %q），得到 %v", c.want, err)
			}
		})
	}
}

func TestQuotaDefaults(t *testing.T) {
	r, err := Load(writeTenants(t, map[string]string{
		"a": "tenant: a\nrollout: general\nmember_groups: [g]\nquota: {max_concurrency: 5}\n",
		"b": "tenant: b\nrollout: general\nmember_groups: [h]\n",
	}), []string{"console"})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := r.Resolve("console", "a", []string{"g"})
	if got := a.Limits(); got != (Quota{MaxConcurrency: 5, DailyTokens: DefaultDailyTokens}) {
		t.Errorf("只配置了并发上限时，配额应取默认值，得到 %+v", got)
	}
	b, _ := r.Resolve("console", "b", []string{"h"})
	if got := b.Limits(); got != (Quota{MaxConcurrency: DefaultMaxConcurrency, DailyTokens: DefaultDailyTokens}) {
		t.Errorf("未配置时取默认值，得到 %+v", got)
	}
}

func TestMemberOfIsSorted(t *testing.T) {
	r, err := Load(writeTenants(t, map[string]string{
		"risk":   "tenant: risk\nrollout: general\nmember_groups: [all]\n",
		"credit": "tenant: credit\nrollout: general\nmember_groups: [all]\n",
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.MemberOf([]string{"all"}); strings.Join(got, ",") != "credit,risk" {
		t.Errorf("MemberOf = %v", got)
	}
}

func TestOwnerOfNeedNotBeMember(t *testing.T) {
	r, err := Load(writeTenants(t, map[string]string{
		"risk":   "tenant: risk\nrollout: general\nmember_groups: [all]\nowner_groups: [risk-owners, biz-owners]\n",
		"credit": "tenant: credit\nrollout: general\nmember_groups: [all]\nowner_groups: [biz-owners]\n",
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.OwnerOf([]string{"biz-owners"}); strings.Join(got, ",") != "credit,risk" {
		t.Errorf("OwnerOf = %v", got)
	}
	if got := r.OwnerOf([]string{"all"}); len(got) != 0 {
		t.Errorf("成员不是负责人: %v", got)
	}
}

func TestResolveAgent(t *testing.T) {
	r, err := Load(writeTenants(t, map[string]string{
		"credit": "tenant: credit\nrollout: general\nmember_groups: [credit-l2]\nbound_clients: [biz]\n" +
			"mcp_agents: [{client_id: bot, purpose: 问答助手, owner: 信贷部, rps: 5}]\nmcp_tools: [search_documents]\n",
		"risk": "tenant: risk\nrollout: general\nmember_groups: [risk-dept]\n",
	}), []string{"console"})
	if err != nil {
		t.Fatal(err)
	}
	if tn, err := r.ResolveAgent("bot", "", []string{"credit-l2"}); err != nil || tn.Name != "credit" {
		t.Fatalf("登记过的 agent 绑定租户: %v %v", tn, err)
	}
	for name, c := range map[string][3]string{
		"未登记的客户端":   {"unknown", "", "credit-l2"},
		"平台级客户端":    {"console", "", "credit-l2"},
		"业务系统客户端":   {"biz", "", "credit-l2"},
		"请求头指定别的租户": {"bot", "risk", "credit-l2"},
		"用户不是租户成员":  {"bot", "", "risk-dept"},
	} {
		if _, err := r.ResolveAgent(c[0], c[1], []string{c[2]}); !errors.Is(err, ErrForbidden) {
			t.Errorf("%s 应拒绝，得到 %v", name, err)
		}
	}
	// agent 只能用 MCP：普通接口不认它
	if _, err := r.Resolve("bot", "", []string{"credit-l2"}); !errors.Is(err, ErrForbidden) {
		t.Errorf("agent 不能调用普通接口: %v", err)
	}
	if a, ok := r.Agent("bot"); !ok || a.RPS != 5 || a.Owner != "信贷部" {
		t.Errorf("登记信息: %+v %v", a, ok)
	}
	credit, risk := r.tenants["credit"], r.tenants["risk"]
	if !credit.MCPToolEnabled("search_documents") || credit.MCPToolEnabled("ask") {
		t.Error("配置了 mcp_tools 时只开放所列工具")
	}
	if !risk.MCPToolEnabled("ask") || risk.MCPToolEnabled("sql_query") {
		t.Error("没有配置 mcp_tools 时开放全部只读工具，且不开放其他工具")
	}
}

func TestParseAndNewSplitPerTenantAndCrossTenantChecks(t *testing.T) {
	a, err := Parse("a", []byte("tenant: a\nrollout: pilot\nbound_clients: [app]\n"))
	if err != nil || a.Rollout != "pilot" {
		t.Fatalf("%+v %v", a, err)
	}
	if _, err := Parse("Bad", []byte("tenant: Bad\nrollout: general\n")); err == nil {
		t.Fatal("租户名只能是小写字母、数字与连字符")
	}
	if _, err := Parse("a", []byte("tenant: a\nrollout: general\nmcp_tools: [delete_all]\n")); err == nil {
		t.Fatal("未知的 MCP 工具应拒绝")
	}
	b, _ := Parse("b", []byte("tenant: b\nrollout: general\nbound_clients: [app]\n"))
	if _, err := New([]Tenant{a, b}, nil); err == nil || !strings.Contains(err.Error(), "只能绑定一个租户") {
		t.Fatalf("同一客户端绑定两个租户: %v", err)
	}
	if _, err := New([]Tenant{a, a}, nil); err == nil {
		t.Fatal("同一租户出现两次应拒绝")
	}
	plain, _ := Parse("a", []byte("tenant: a\nrollout: pilot\n"))
	r, err := New([]Tenant{b, plain}, nil)
	if err != nil || len(r.Tenants()) != 2 || r.Tenants()[0].Name != "a" {
		t.Fatalf("%v %v", r, err)
	}
}
