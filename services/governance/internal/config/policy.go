package config

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/hei-/bambuflow/libs/platform-go/policy"
)

// ObjectPolicy 是检索权限策略包的一层（governance 详设第 5 节），scope 为 global、source:<数据源> 或 tenant:<租户>，
// 内容是该层的 rego 规则（不含 package，libs/platform-go/policy）。生效的各层合成一个策略包，随 retrieval 的快照签名下发。
const ObjectPolicy = "policy"

// probeSource 是代表性的已授权数据源之一：没有数据源层的数据源都按它对待（详设 5.2 核心约束与差异分析）
const probeSource = "probe-source"

func activeLayers(ctx context.Context, tx Tx) (map[string]string, error) {
	active, err := tx.Active(ctx, ObjectPolicy)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, v := range active {
		out[v.Scope] = v.Content
	}
	return out, nil
}

func build(layers map[string]string) (policy.Bundle, error) {
	var ls []policy.Layer
	for scope, body := range layers {
		ls = append(ls, policy.Layer{Scope: scope, Body: body})
	}
	return policy.Build(ls)
}

// probes 是核心约束与差异分析用的租户（全部生效的租户配置）与代表性的已授权数据源（各数据源层的数据源加一个
// 没有数据源层的）。
func probes(ctx context.Context, tx Tx, layers map[string]string) ([]string, []string, error) {
	active, err := tx.Active(ctx, ObjectTenant)
	if err != nil {
		return nil, nil, err
	}
	var tenants []string
	for _, v := range active {
		tenants = append(tenants, v.Scope)
	}
	granted := []string{probeSource}
	for scope := range layers {
		if s, ok := strings.CutPrefix(scope, "source:"); ok {
			granted = append(granted, s)
		}
	}
	slices.Sort(granted)
	return tenants, granted, nil
}

// checkPolicy 校验一个策略层的新内容（详设 5.2）：与其他生效的层合成后能编译、全部测试通过、核心约束成立；
// 返回差异分析（与当前生效的策略包相比，可见范围变大的代表性主体）。权限评测集零越权由评测门禁保证。
func checkPolicy(ctx context.Context, tx Tx, scope, content string) ([]string, error) {
	invalid := func(err error) error { return status.Error(codes.InvalidArgument, err.Error()) }
	if !policy.ValidScope(scope) {
		return nil, invalid(fmt.Errorf("policy 的 scope %q 不合法（global、source:<数据源>、tenant:<租户>）", scope))
	}
	layers, err := activeLayers(ctx, tx)
	if err != nil {
		return nil, err
	}
	tenants, granted, err := probes(ctx, tx, layers)
	if err != nil {
		return nil, err
	}
	if t, ok := strings.CutPrefix(scope, "tenant:"); ok && !slices.Contains(tenants, t) {
		return nil, invalid(fmt.Errorf("租户 %s 没有生效的租户配置", t))
	}
	var old *policy.Engine
	if _, ok := layers[policy.ScopeGlobal]; ok {
		b, err := build(layers)
		if err != nil {
			return nil, fmt.Errorf("当前生效的策略包: %w", err)
		}
		if old, err = policy.Compile(ctx, b); err != nil {
			return nil, fmt.Errorf("当前生效的策略包: %w", err)
		}
	}
	layers[scope] = content
	if s, ok := strings.CutPrefix(scope, "source:"); ok && !slices.Contains(granted, s) {
		granted = append(granted, s)
	}
	next, err := validated(ctx, layers, tenants, granted)
	if err != nil {
		return nil, invalid(err)
	}
	widened, err := policy.Widened(ctx, old, next, tenants, granted)
	if err != nil {
		return nil, invalid(err)
	}
	return widened, nil
}

func validated(ctx context.Context, layers map[string]string, tenants, granted []string) (*policy.Engine, error) {
	b, err := build(layers)
	if err != nil {
		return nil, err
	}
	e, err := policy.Compile(ctx, b)
	if err != nil {
		return nil, err
	}
	if err := policy.Test(ctx, b); err != nil {
		return nil, err
	}
	if err := policy.CheckCore(ctx, e, tenants, granted); err != nil {
		return nil, err
	}
	return e, nil
}

// BootstrapPolicy 为还没有生效版本的策略层导入部署目录里的初始内容（来源记 bootstrap），已有的层不动；
// 与生效的层合成后整体校验（编译、测试、核心约束），任何一层不合法就都不导入。要在租户配置导入之后调用：
// 租户层要求租户配置已生效。
func (s *Server) BootstrapPolicy(ctx context.Context, seed []policy.Layer) (int, error) {
	n := 0
	err := s.Store.InTx(ctx, func(tx Tx) error {
		layers, err := activeLayers(ctx, tx)
		if err != nil {
			return err
		}
		var add []policy.Layer
		for _, l := range seed {
			if _, ok := layers[l.Scope]; !ok {
				add = append(add, l)
				layers[l.Scope] = l.Body
			}
		}
		if len(add) == 0 {
			return nil
		}
		tenants, granted, err := probes(ctx, tx, layers)
		if err != nil {
			return err
		}
		for _, l := range add {
			if t, ok := strings.CutPrefix(l.Scope, "tenant:"); ok && !slices.Contains(tenants, t) {
				return fmt.Errorf("策略层 %s：租户没有生效的租户配置", l.Scope)
			}
		}
		if _, err := validated(ctx, layers, tenants, granted); err != nil {
			return err
		}
		slices.SortFunc(add, func(a, b policy.Layer) int { return strings.Compare(a.Scope, b.Scope) })
		now := s.Now()
		for _, l := range add {
			if _, err := tx.LockPointer(ctx, ObjectPolicy, l.Scope); err != nil {
				return err
			}
			v, err := newVersion(ctx, tx, ObjectPolicy, l.Scope, l.Body, "bootstrap", "bootstrap:"+l.Scope, now, nil)
			if err != nil {
				return err
			}
			rel := Release{ID: newID("rel_"), Object: ObjectPolicy, Scope: l.Scope, VersionID: v.ID, Source: "bootstrap",
				SourceRef: v.SourceRef, Status: "active", Reason: "部署目录里的初始策略", RequestedBy: "bootstrap", RequestedAt: now}
			if err := tx.SetPointer(ctx, ObjectPolicy, l.Scope, v.ID); err != nil {
				return err
			}
			if err := tx.InsertRelease(ctx, rel); err != nil {
				return err
			}
			if err := s.Audit.Write(ctx, "config.bootstrap", rel.ID, map[string]any{"object": ObjectPolicy, "scope": l.Scope,
				"version_id": v.ID, "content_hash": v.ContentHash}); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

// PolicyBundle 是 retrieval 快照里合成好的策略包；没有生效的全局基线时为 nil（retrieval 因此拒绝全部检索）。
func (s *Server) PolicyBundle(ctx context.Context) (*configv1.PolicyBundle, error) {
	var out *configv1.PolicyBundle
	err := s.Store.InTx(ctx, func(tx Tx) error {
		layers, err := activeLayers(ctx, tx)
		if err != nil || layers[policy.ScopeGlobal] == "" {
			return err
		}
		b, err := build(layers)
		if err != nil {
			return fmt.Errorf("合成生效的策略包: %w", err)
		}
		out = &configv1.PolicyBundle{Version: b.Version}
		for _, m := range b.Modules {
			out.Modules = append(out.Modules, &configv1.PolicyModule{Path: m.Path, Source: m.Source})
		}
		return nil
	})
	return out, err
}
