package policy

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/open-policy-agent/opa/v1/ast"
	"github.com/open-policy-agent/opa/v1/rego"
	"github.com/open-policy-agent/opa/v1/storage/inmem"
	"github.com/open-policy-agent/opa/v1/tester"
)

// Test 运行策略包里的单元测试（test_ 开头的规则，与规则写在同一层里），governance 发布前调用（详设 5.2 第 2 步）。
// 每一层都至少要有一个测试，任何一个测试失败或出错都返回错误。
func Test(ctx context.Context, b Bundle) error {
	modules := map[string]*ast.Module{}
	for _, m := range b.Modules {
		parsed, err := ast.ParseModule(m.Path, m.Source)
		if err != nil {
			return fmt.Errorf("%s: %w", m.Path, err)
		}
		modules[m.Path] = parsed
	}
	store := inmem.New()
	txn, err := store.NewTransaction(ctx)
	if err != nil {
		return err
	}
	defer store.Abort(ctx, txn)
	ch, err := tester.NewRunner().SetStore(store).SetModules(modules).RunTests(ctx, txn)
	if err != nil {
		return fmt.Errorf("运行策略测试: %w", err)
	}
	tested := map[string]int{}
	var failed []string
	for r := range ch {
		if r.Location != nil {
			tested[r.Location.File]++
		}
		if !r.Pass() {
			failed = append(failed, r.Package+"."+r.Name)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("策略测试失败: %s", strings.Join(failed, "、"))
	}
	for _, m := range b.Modules {
		if tested[m.Path] == 0 {
			return fmt.Errorf("策略层 %s 没有测试（test_ 开头的规则）", strings.TrimSuffix(m.Path, ".rego"))
		}
	}
	return nil
}

// principals 是一组代表性 Principal：每个租户（另加一个没有配置的租户）× 密级 1–4 × 是否在某个可看 PII 的群组里。
// 部门与地域取占位值：按部门、地域的收紧写在数据源层里，代表性主体覆盖不到，靠各层自己的测试。
func principals(tenants, groups []string) []Input {
	var out []Input
	for _, t := range append(slices.Clone(tenants), "unconfigured-probe") {
		for c := int32(1); c <= 4; c++ {
			out = append(out, Input{Sub: "probe-" + t, Tenant: t, Dept: "代表部门", Clearance: c})
			for _, g := range groups {
				out = append(out, Input{Sub: "probe-" + t, Tenant: t, Dept: "代表部门", Clearance: c, Groups: []string{g}})
			}
		}
	}
	return out
}

// viewerGroups 是各租户层给出的可看 PII 原文的群组（差异分析要覆盖它们，才看得出义务的放宽）。
func (e *Engine) viewerGroups(ctx context.Context) ([]string, error) {
	return e.tenantStrings(ctx, "pii_viewer_groups")
}

// tenantStrings 是各租户层某一项（字符串集合）的并集。
func (e *Engine) tenantStrings(ctx context.Context, rule string) ([]string, error) {
	rs, err := rego.New(append(moduleOpts(e.modules),
		rego.Query(fmt.Sprintf(`gs := {g | some t; some g in object.get(data.bambuflow.tenants[t], %q, set())}`, rule)))...).Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取租户层的 %s: %w", rule, err)
	}
	var out []string
	for _, r := range rs {
		vs, _ := r.Bindings["gs"].([]any)
		for _, v := range vs {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

// CheckCore 检查发布前必须成立的核心约束（详设 5.2 第 3 步），granted 是一组代表性的已授权数据源：
//   - 缺少属性（没有部门）的主体被拒绝，而不是看到全部；
//   - 没有租户层的租户默认拒绝（跨租户默认拒绝）；
//   - 放行时密级上限不超过主体的密级（受限级只对有受限级密级的主体放行），数据源不超出授权范围，
//     ACL token 只含主体自己的用户、群组与部门。
//
// 评估出错也算不通过：线上评估出错的结果是拒绝，发布前就要暴露出来。
func CheckCore(ctx context.Context, e *Engine, tenants, granted []string) error {
	groups, err := e.viewerGroups(ctx)
	if err != nil {
		return err
	}
	for _, t := range tenants {
		d, err := e.Decide(ctx, Input{Sub: "probe", Tenant: t, Clearance: 2}, granted)
		if err != nil {
			return err
		}
		if d.Allow {
			return fmt.Errorf("核心约束：租户 %s 缺少部门属性的主体被放行", t)
		}
	}
	for _, in := range principals(tenants, groups) {
		d, err := e.Decide(ctx, in, granted)
		if err != nil {
			return fmt.Errorf("核心约束：评估 %s 出错: %w", describe(in), err)
		}
		if !d.Allow {
			continue
		}
		if !slices.Contains(tenants, in.Tenant) {
			return fmt.Errorf("核心约束：没有租户策略的租户 %s 被放行", in.Tenant)
		}
		f := d.Filter
		if f.MaxClassification > in.Clearance {
			return fmt.Errorf("核心约束：%s 的密级上限 %d 超过本人密级", describe(in), f.MaxClassification)
		}
		for _, s := range f.Sources {
			if !slices.Contains(granted, s) {
				return fmt.Errorf("核心约束：%s 可检索未授权的数据源 %s", describe(in), s)
			}
		}
		own := []string{"user:" + in.Sub, "dept:" + in.Dept}
		for _, g := range in.Groups {
			own = append(own, "group:"+g)
		}
		for _, tok := range f.ACLTokens {
			if !slices.Contains(own, tok) {
				return fmt.Errorf("核心约束：%s 的 ACL token 含有不属于本人的 %s", describe(in), tok)
			}
		}
	}
	return nil
}

// Widened 列出新策略包下可见范围变大的代表性主体（详设 5.2 第 4 步）：从拒绝变为放行、多了可检索的数据源、
// 密级上限提高、PII 由掩码变为原文。审批人看到的是“谁多看到了什么”，而不只是代码 diff。old 为 nil（首个版本）时
// 所有放行都算变大。
//
// 代表性的已授权数据源在 granted 之外，还并入新旧两份包里租户层点名禁止的数据源：解除禁止才看得出来。
func Widened(ctx context.Context, old, next *Engine, tenants, granted []string) ([]string, error) {
	var groups []string
	granted = slices.Clone(granted)
	for _, e := range []*Engine{old, next} {
		if e == nil {
			continue
		}
		g, err := e.viewerGroups(ctx)
		if err != nil {
			return nil, err
		}
		d, err := e.tenantStrings(ctx, "deny_sources")
		if err != nil {
			return nil, err
		}
		groups, granted = append(groups, g...), append(granted, d...)
	}
	groups = slices.Compact(slices.Sorted(slices.Values(groups)))
	granted = slices.Compact(slices.Sorted(slices.Values(granted)))
	var out []string
	for _, in := range principals(tenants, groups) {
		n, err := next.Decide(ctx, in, granted)
		if err != nil {
			return nil, err
		}
		if !n.Allow {
			continue
		}
		var o Decision
		if old != nil {
			if o, err = old.Decide(ctx, in, granted); err != nil {
				return nil, err
			}
		}
		var more []string
		switch {
		case !o.Allow:
			more = append(more, "从拒绝变为放行")
		default:
			for _, s := range n.Filter.Sources {
				if !slices.Contains(o.Filter.Sources, s) {
					more = append(more, "新增可检索数据源 "+s)
				}
			}
			if n.Filter.MaxClassification > o.Filter.MaxClassification {
				more = append(more, fmt.Sprintf("密级上限 %d → %d", o.Filter.MaxClassification, n.Filter.MaxClassification))
			}
			if o.Obligations.Redaction != "none" && n.Obligations.Redaction == "none" {
				more = append(more, "PII 由掩码变为原文")
			}
		}
		if len(more) > 0 {
			out = append(out, describe(in)+"："+strings.Join(more, "；"))
		}
	}
	return out, nil
}

func describe(in Input) string {
	s := fmt.Sprintf("租户 %s、密级 %d", in.Tenant, in.Clearance)
	if len(in.Groups) > 0 {
		s += "、群组 " + strings.Join(in.Groups, ",")
	}
	return s
}
