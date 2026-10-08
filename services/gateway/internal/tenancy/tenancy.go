// Package tenancy 确定请求所属的租户并检查成员关系（2026-10-04 定：客户端绑定 + 请求头选择）。
//
// 业务系统的客户端登记时绑定一个租户，请求自动属于该租户；console 等平台级客户端用请求头
// Bambuflow-Tenant 在用户所属的租户中选一个，用户只属于一个租户时可以省略。任何情况下用户都必须是
// 该租户的成员（属于租户配置中的某个成员群组），否则拒绝。
//
// 经 MCP 接入的外部 agent 也在租户配置里登记（mcp_agents，gateway 详设 7.2）：登记即绑定该租户，
// 只能调用 MCP 端点；未登记的 client_id 即使拿到了用户令牌也拒绝。
package tenancy

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"go.yaml.in/yaml/v3"
)

// Header 是平台级客户端选择租户的请求头。
const Header = "Bambuflow-Tenant"

var (
	ErrForbidden      = errors.New("forbidden")
	ErrTenantRequired = errors.New("tenant_required")
)

// Tenant 是 config/tenants/<tenant>/tenant.yaml 中与 gateway 有关的部分。
type Tenant struct {
	Name         string   `yaml:"tenant"`
	Rollout      string   `yaml:"rollout"` // pilot | general（evaluation 详设 3.1）
	MemberGroups []string `yaml:"member_groups"`
	PilotGroups  []string `yaml:"pilot_groups"`
	BoundClients []string `yaml:"bound_clients"`
	// MCPAgents 是登记过的外部 agent；MCPTools 是对该租户开放的 MCP 工具，不配置时开放全部只读工具
	MCPAgents []Agent  `yaml:"mcp_agents"`
	MCPTools  []string `yaml:"mcp_tools"`
	// AuditorGroups 是该租户的审计岗：可以回放本租户任何人的回答（gateway 详设 4.1）。
	// 审计岗怎么识别设计没有定，先按群组配置；不配置时没有审计岗
	AuditorGroups []string `yaml:"auditor_groups"`
	// Quota 是租户这一层的限制（gateway 详设 5.1）；未配置的项取保守默认值，见 Limits
	Quota Quota `yaml:"quota"`
}

// Quota 是租户配置中的限流与配额（技术设计文档 TenantPipeline.quota）。
type Quota struct {
	// MaxConcurrency 是该租户同时在处理的在线请求数上限（批量不计入）
	MaxConcurrency int `yaml:"max_concurrency"`
	// DailyTokens 是该租户每个自然日可用的 LLM token 数；用尽后当日只提供仅检索
	DailyTokens int64 `yaml:"daily_tokens"`
}

// 租户没有配置时的默认值，宁可偏紧：平台峰值约 50 QPS × 4 s ≈ 200 路并发（技术设计文档的容量估算），单个租户默认最多占 20 路；
// 每日 200 万 token 约合 200 多次单跳问答，够试点使用，超出后由平台按申请调高。
const (
	DefaultMaxConcurrency = 20
	DefaultDailyTokens    = 2_000_000
)

// Limits 返回生效的租户限制：未配置的项取默认值。
func (t *Tenant) Limits() Quota {
	q := t.Quota
	if q.MaxConcurrency == 0 {
		q.MaxConcurrency = DefaultMaxConcurrency
	}
	if q.DailyTokens == 0 {
		q.DailyTokens = DefaultDailyTokens
	}
	return q
}

// Agent 是一个登记过的外部 agent（gateway 详设 7.2）：登记时声明用途、负责人与限流额度。
type Agent struct {
	ClientID string `yaml:"client_id"`
	Purpose  string `yaml:"purpose"`
	Owner    string `yaml:"owner"`
	// RPS 是该 agent 的调用方应用限流（每秒请求数）；为 0 时用 gateway 的默认值（gateway 详设 5.1）
	RPS int `yaml:"rps"`
}

// MCPTools 是 MCP 端点提供的全部只读工具（gateway 详设 7.1），租户配置只能从中选择。
var MCPTools = []string{"ask", "search_documents", "list_documents", "count_documents", "get_document_excerpt"}

type Registry struct {
	tenants         map[string]*Tenant
	boundTo         map[string]string // 客户端 → 绑定的租户
	platformClients map[string]bool
	agents          map[string]registeredAgent // client_id → 登记
}

type registeredAgent struct {
	tenant string
	Agent
}

// Load 读取 dir 下每个子目录的 tenant.yaml。
func Load(dir string, platformClients []string) (*Registry, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*", "tenant.yaml"))
	if err != nil {
		return nil, err
	}
	r := &Registry{tenants: map[string]*Tenant{}, boundTo: map[string]string{}, platformClients: map[string]bool{}, agents: map[string]registeredAgent{}}
	for _, c := range platformClients {
		r.platformClients[c] = true
	}
	for _, p := range paths {
		b, err := os.ReadFile(filepath.Clean(p))
		if err != nil {
			return nil, err
		}
		var t Tenant
		if err := yaml.Unmarshal(b, &t); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if t.Name == "" || filepath.Base(filepath.Dir(p)) != t.Name {
			return nil, fmt.Errorf("%s: tenant 字段应与目录名一致", p)
		}
		if t.Rollout != "pilot" && t.Rollout != "general" {
			return nil, fmt.Errorf("%s: rollout 只能是 pilot 或 general", p)
		}
		if t.Quota.MaxConcurrency < 0 || t.Quota.DailyTokens < 0 {
			return nil, fmt.Errorf("%s: quota 的取值不能为负", p)
		}
		for _, c := range t.BoundClients {
			if prev, dup := r.boundTo[c]; dup || r.platformClients[c] {
				return nil, fmt.Errorf("客户端 %s 只能绑定一个租户或作为平台级客户端（已绑定 %s）", c, prev)
			}
			r.boundTo[c] = t.Name
		}
		r.tenants[t.Name] = &t
	}
	// 业务系统客户端全部读完后再检查 agent：一个 client_id 只能有一种身份
	for _, t := range r.tenants {
		if err := r.addAgents(t); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *Registry) addAgents(t *Tenant) error {
	for _, name := range t.MCPTools {
		if !slices.Contains(MCPTools, name) {
			return fmt.Errorf("租户 %s: 未知的 MCP 工具 %q", t.Name, name)
		}
	}
	for _, a := range t.MCPAgents {
		switch prev, dup := r.agents[a.ClientID]; {
		case a.ClientID == "" || a.Purpose == "" || a.Owner == "" || a.RPS < 0:
			return fmt.Errorf("租户 %s: agent 登记须有 client_id、purpose、owner，rps 不能为负", t.Name)
		case dup, r.boundTo[a.ClientID] != "", r.platformClients[a.ClientID]:
			return fmt.Errorf("客户端 %s 只能登记为一个租户的 agent，且不能同时是业务系统或平台级客户端（已登记于 %s）", a.ClientID, prev.tenant)
		}
		r.agents[a.ClientID] = registeredAgent{tenant: t.Name, Agent: a}
	}
	return nil
}

// Agent 返回登记过的 agent；没有登记时 ok 为 false。
// PlatformClient 判断客户端是否为平台级客户端（如 console）。管理接口只接受平台级客户端：
// 业务系统与外部 agent 即使代表的用户有平台角色，也不能经它们调用管理接口。
func (r *Registry) PlatformClient(clientID string) bool { return r.platformClients[clientID] }

func (r *Registry) Agent(clientID string) (Agent, bool) {
	a, ok := r.agents[clientID]
	return a.Agent, ok
}

// ResolveAgent 确定 MCP 请求的租户：agent 登记即绑定租户，用法与业务系统客户端相同。
// 不是登记过的 agent（包括 console 与业务系统客户端）一律拒绝。
func (r *Registry) ResolveAgent(clientID, header string, groups []string) (*Tenant, error) {
	a, ok := r.agents[clientID]
	switch {
	case !ok:
		return nil, fmt.Errorf("%w: 不是登记过的 agent", ErrForbidden)
	case header != "" && header != a.tenant:
		return nil, fmt.Errorf("%w: agent 登记的租户与请求头不一致", ErrForbidden)
	}
	t := r.tenants[a.tenant]
	if !intersects(t.MemberGroups, groups) {
		return nil, fmt.Errorf("%w: 不是该租户的成员", ErrForbidden)
	}
	return t, nil
}

// MCPToolEnabled：租户没有配置 mcp_tools 时开放全部只读工具。
func (t *Tenant) MCPToolEnabled(name string) bool {
	if !slices.Contains(MCPTools, name) {
		return false
	}
	return t.MCPTools == nil || slices.Contains(t.MCPTools, name)
}

// MemberOf 返回用户所属的租户（按名字排序）。
func (r *Registry) MemberOf(groups []string) []string {
	var out []string
	for name, t := range r.tenants {
		if intersects(t.MemberGroups, groups) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Resolve 确定请求的租户。header 为请求头 Bambuflow-Tenant 的值（可能为空）。
func (r *Registry) Resolve(clientID, header string, groups []string) (*Tenant, error) {
	var name string
	switch bound, ok := r.boundTo[clientID]; {
	case ok:
		if header != "" && header != bound {
			return nil, fmt.Errorf("%w: 客户端绑定的租户与请求头不一致", ErrForbidden)
		}
		name = bound
	case r.platformClients[clientID]:
		name = header
		if name == "" {
			mine := r.MemberOf(groups)
			if len(mine) != 1 {
				return nil, ErrTenantRequired
			}
			name = mine[0]
		}
	default:
		return nil, fmt.Errorf("%w: 客户端未登记", ErrForbidden)
	}
	t, ok := r.tenants[name]
	if !ok || !intersects(t.MemberGroups, groups) {
		// 不存在的租户与非成员的租户同样处理，不让调用方据此探测租户名
		return nil, fmt.Errorf("%w: 不是该租户的成员", ErrForbidden)
	}
	return t, nil
}

// Auditor：用户属于该租户的审计岗群组。
func (t *Tenant) Auditor(groups []string) bool {
	return intersects(t.AuditorGroups, groups)
}

// AnswersAllowed：租户处于试点阶段时，问答只对试点群组开放（evaluation 详设 3.1）。
func (t *Tenant) AnswersAllowed(groups []string) bool {
	return t.Rollout == "general" || intersects(t.PilotGroups, groups)
}

func intersects(a, b []string) bool {
	for _, x := range a {
		if slices.Contains(b, x) {
			return true
		}
	}
	return false
}
