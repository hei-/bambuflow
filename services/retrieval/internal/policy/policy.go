// Package policy 内嵌 OPA，按 Principal 给出检索的允许 / 拒绝和过滤条件（retrieval 详设 5.2）。
//
// P0 从目录加载策略包（*.rego + data.json），governance 分发策略包后改为监听快照（详设 10.2）。
// 只有 retrieval 用到它；governance 开始校验策略包时再移到 libs/policy（AGENTS.md 第 3 节）。
package policy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"

	"github.com/open-policy-agent/opa/v1/rego"
	"github.com/open-policy-agent/opa/v1/storage/inmem"
)

const query = "data.bambuflow.authz.decision"

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
}

// Engine 持有一份已编译的策略包，可并发使用。
type Engine struct {
	prepared rego.PreparedEvalQuery
	version  string
}

// Load 从 fsys 的根目录加载策略包：至少一个 .rego 文件和一个 data.json，缺任何一个都视为策略包缺失。
// 版本号取全部文件内容的摘要，写入响应的 policy_version 与审计。
func Load(ctx context.Context, fsys fs.FS) (*Engine, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("读取策略包目录: %w", err)
	}
	var opts []func(*rego.Rego)
	var data map[string]any
	h := sha256.New()
	modules := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || (path.Ext(name) != ".rego" && name != "data.json") {
			continue
		}
		b, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, err
		}
		// 文件名和内容都计入摘要，ReadDir 已按文件名排序，版本号与目录遍历顺序无关
		_, _ = fmt.Fprintf(h, "%s\x00%d\x00", name, len(b))
		_, _ = h.Write(b)
		if name == "data.json" {
			dec := json.NewDecoder(bytes.NewReader(b))
			dec.UseNumber()
			if err := dec.Decode(&data); err != nil {
				return nil, fmt.Errorf("data.json: %w", err)
			}
			continue
		}
		opts = append(opts, rego.Module(name, string(b)))
		modules++
	}
	if modules == 0 || data == nil {
		return nil, errors.New("策略包缺失：需要至少一个 .rego 文件和 data.json")
	}
	opts = append(opts, rego.Query(query), rego.Store(inmem.NewFromObject(data)))
	prepared, err := rego.New(opts...).PrepareForEval(ctx)
	if err != nil {
		return nil, fmt.Errorf("编译策略: %w", err)
	}
	return &Engine{prepared: prepared, version: "sha256:" + hex.EncodeToString(h.Sum(nil))[:16]}, nil
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
	return d, nil
}
