// Package policy 是检索权限策略包（governance 详设第 5 节、retrieval 详设 5.2）：内嵌 OPA，按 Principal 给出
// 允许 / 拒绝、过滤条件与义务。governance 用它在发布前合成、编译、测试与分析策略包，retrieval 用它评估。
//
// 策略包分三层（详设 5.1）：全局基线（global，信息安全）、数据源策略（source:<id>，属主）、租户策略（tenant:<租户>）。
// 每层的内容只是规则本身，package 由这里按作用域生成，一层不能把规则写进别的层的命名空间：
//   - global → package bambuflow.authz：合成规则与平台级约束，给出 decision；
//   - source:<id> → package bambuflow.sources["<id>"]：deny（这个主体不能检索该数据源的原因）；
//   - tenant:<t> → package bambuflow.tenants["<t>"]：deny（整个请求拒绝的原因）、deny_sources（租户不检索的数据源）、
//     pii_viewer_groups（能看 PII 原文的群组）。
//
// 合成规则只从下层读这几项拒绝类结果，下层因此只能在上层允许的范围内收紧。内核在下发前还会断言必含条件
// （services/retrieval/internal/search/filter.go），策略写错也不会放大权限。
package policy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/open-policy-agent/opa/v1/rego"
)

const query = "data.bambuflow.authz.decision"

// ScopeGlobal 是全局基线层的作用域。
const ScopeGlobal = "global"

var (
	scopePattern   = regexp.MustCompile(`^(global|source:[a-z0-9_-]{1,64}|tenant:[a-z0-9-]{1,64})$`)
	packagePattern = regexp.MustCompile(`(?m)^\s*package\s`)
)

// Layer 是策略包的一层：作用域与规则（不含 package 声明）。
type Layer struct {
	Scope string
	Body  string
}

// Module 是一层生成的 rego 模块。
type Module struct {
	Path   string
	Source string
}

// Bundle 是合成好的策略包；Version 是全部模块的摘要，写进检索响应的 policy_version、回答的 why 与审计。
type Bundle struct {
	Version string
	Modules []Module
}

// ValidScope 判断作用域格式。
func ValidScope(scope string) bool { return scopePattern.MatchString(scope) }

func packageOf(scope string) string {
	kind, name, _ := strings.Cut(scope, ":")
	switch kind {
	case "source":
		return fmt.Sprintf("bambuflow.sources[%q]", name)
	case "tenant":
		return fmt.Sprintf("bambuflow.tenants[%q]", name)
	}
	return "bambuflow.authz"
}

// Build 把各层合成一个策略包：必须恰好有一个全局基线层，作用域不重复，规则里不能自带 package 声明。
// 模块按作用域排序（全局基线在前），版本号与输入顺序无关。
func Build(layers []Layer) (Bundle, error) {
	seen := map[string]bool{}
	sorted := slices.Clone(layers)
	slices.SortFunc(sorted, func(a, b Layer) int {
		if (a.Scope == ScopeGlobal) != (b.Scope == ScopeGlobal) {
			if a.Scope == ScopeGlobal {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Scope, b.Scope)
	})
	h := sha256.New()
	var out Bundle
	for _, l := range sorted {
		switch {
		case !ValidScope(l.Scope):
			return Bundle{}, fmt.Errorf("策略层的作用域 %q 不合法（global、source:<数据源>、tenant:<租户>）", l.Scope)
		case seen[l.Scope]:
			return Bundle{}, fmt.Errorf("策略层 %s 重复", l.Scope)
		case packagePattern.MatchString(l.Body):
			return Bundle{}, fmt.Errorf("策略层 %s: 不能自带 package 声明，命名空间按作用域生成", l.Scope)
		}
		seen[l.Scope] = true
		m := Module{
			Path:   strings.ReplaceAll(l.Scope, ":", "/") + ".rego",
			Source: "package " + packageOf(l.Scope) + "\n\nimport rego.v1\n\n" + l.Body,
		}
		_, _ = fmt.Fprintf(h, "%s\x00%d\x00", m.Path, len(m.Source))
		_, _ = h.Write([]byte(m.Source))
		out.Modules = append(out.Modules, m)
	}
	if !seen[ScopeGlobal] {
		return Bundle{}, errors.New("策略包缺少全局基线层（global）")
	}
	out.Version = "sha256:" + hex.EncodeToString(h.Sum(nil))[:16]
	return out, nil
}

// Input 是策略的输入。Groups 是已展开的完整群组列表（Principal 只带引用时由调用方先查平台身份库）。
type Input struct {
	Sub       string   `json:"sub"`
	Tenant    string   `json:"tenant"`
	Dept      string   `json:"dept"`
	Clearance int32    `json:"clearance"`
	Region    string   `json:"region"`
	Groups    []string `json:"groups"`
}

// Filter 是策略产出的过滤条件，还要经内核断言必含条件后才能使用。
type Filter struct {
	Sources           []string `json:"sources"`
	MaxClassification int32    `json:"max_classification"`
	ACLTokens         []string `json:"acl_tokens"`
}

// Obligations 是策略的义务（技术设计文档 8.6）：Redaction 为 mask（PII 掩码）或 none。
type Obligations struct {
	Redaction string `json:"redaction"`
}

// Decision 是一次策略评估的结果。Reason 是原因码，写日志和审计，不返回给调用方。
type Decision struct {
	Allow       bool        `json:"allow"`
	Reason      string      `json:"reason"`
	Filter      Filter      `json:"filter"`
	Obligations Obligations `json:"obligations"`
	// Version 是给出这个决定的策略包版本（灰度时可能是候选包），由 Decide 填写
	Version string `json:"-"`
}

// Engine 持有一份已编译的策略包，可并发使用。
type Engine struct {
	modules  []Module
	prepared rego.PreparedEvalQuery
	version  string
}

// Compile 编译策略包；有语法或类型错误时返回错误（调用方不加载这份包）。
func Compile(ctx context.Context, b Bundle) (*Engine, error) {
	if len(b.Modules) == 0 || b.Version == "" {
		return nil, errors.New("策略包缺失")
	}
	prepared, err := rego.New(append(moduleOpts(b.Modules), rego.Query(query))...).PrepareForEval(ctx)
	if err != nil {
		return nil, fmt.Errorf("编译策略: %w", err)
	}
	return &Engine{modules: b.Modules, prepared: prepared, version: b.Version}, nil
}

func moduleOpts(ms []Module) []func(*rego.Rego) {
	var opts []func(*rego.Rego)
	for _, m := range ms {
		opts = append(opts, rego.Module(m.Path, m.Source))
	}
	return opts
}

// Version 返回策略包版本。
func (e *Engine) Version() string { return e.version }

// Decide 评估策略。granted 是租户被授权的数据源（governance 下发）；策略只会在它的范围内再收紧。
// 策略没有给出结果、或结果格式不符时返回错误，调用方按 fail-closed 处理。
func (e *Engine) Decide(ctx context.Context, in Input, granted []string) (Decision, error) {
	if in.Groups == nil {
		in.Groups = []string{}
	}
	if granted == nil {
		granted = []string{}
	}
	rs, err := e.prepared.Eval(ctx, rego.EvalInput(map[string]any{"principal": in, "granted_sources": granted}))
	if err != nil {
		return Decision{}, fmt.Errorf("评估策略: %w", err)
	}
	if len(rs) != 1 || len(rs[0].Expressions) != 1 {
		return Decision{}, errors.New("策略没有给出决定")
	}
	raw, err := json.Marshal(rs[0].Expressions[0].Value)
	if err != nil {
		return Decision{}, err
	}
	var d Decision
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return Decision{}, fmt.Errorf("策略结果格式不符: %w", err)
	}
	d.Version = e.version
	return d, nil
}

// LoadLayers 读部署目录里的策略层（初始导入用）：global.rego、source.<数据源>.rego、tenant.<租户>.rego，
// 其他文件忽略。多个目录合在一起，同一作用域出现两次报错。
func LoadLayers(dirs ...string) ([]Layer, error) {
	var out []Layer
	seen := map[string]string{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("读取策略目录 %s: %w", dir, err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".rego") {
				continue
			}
			scope := strings.TrimSuffix(name, ".rego")
			if kind, rest, ok := strings.Cut(scope, "."); ok {
				scope = kind + ":" + rest
			}
			if !ValidScope(scope) {
				return nil, fmt.Errorf("策略文件 %s/%s：文件名应为 global.rego、source.<数据源>.rego 或 tenant.<租户>.rego", dir, name)
			}
			if prev, dup := seen[scope]; dup {
				return nil, fmt.Errorf("策略层 %s 在 %s 与 %s 里重复", scope, prev, dir)
			}
			seen[scope] = dir
			b, err := os.ReadFile(filepath.Clean(filepath.Join(dir, name)))
			if err != nil {
				return nil, err
			}
			out = append(out, Layer{Scope: scope, Body: string(b)})
		}
	}
	return out, nil
}
