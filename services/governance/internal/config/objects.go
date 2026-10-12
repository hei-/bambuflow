package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

// 影响回答的配置对象（governance 详设 3.1）。与租户配置走同一套版本、变更单与发布；
// 发布前在这里解析并校验，下发的是解析后的结构（configv1.PipelineConfig、RoutingConfig），使用方不再解析 YAML。
const (
	// ObjectPipeline 是租户的检索计划与回答阈值（pipeline.yaml），scope 为租户名；下发给 retrieval 与 orchestrator
	ObjectPipeline = "pipeline"
	// ObjectRouting 是全局的模型路由与阈值（回答生成的模型与置信度权重），scope 为 global；下发给 orchestrator
	ObjectRouting = "routing"
	ScopeGlobal   = "global"
	// ObjectPrompt 是回答模板（技术设计文档 7.7 的五个区），scope 为 answer（单跳）或 agent-answer（多跳）；下发给 orchestrator
	ObjectPrompt = "prompt"
)

// promptZones 是各回答模板用户消息里的区，按顺序（与 orchestrator prompt.ZONES 一致）
var promptZones = map[string][]string{
	"answer":       {"summary", "evidence", "question"},
	"agent-answer": {"summary", "evidence", "subquestions", "unresolved", "question"},
}

var (
	placeholderRe = regexp.MustCompile(`\{\{([a-z_]+)\}\}`)
	// 租户规则里不能出现提示词各区的标记，免得冒充证据区、问题或摘要
	zoneMarkerRe = regexp.MustCompile(`(?i)</?(evidence|question|summary|subquestions|unresolved)\b|\{\{`)
)

const maxTenantRules = 2000

// objects 是可以发布的对象；needsGate 为 true 的对象每次发布都先过评测门禁（详设 4.2“影响回答的对象”，gate.go 发起）。
var objects = map[string]struct{ needsGate bool }{
	ObjectTenant:   {false},
	ObjectPipeline: {true},
	ObjectRouting:  {true},
	// 策略包的任何改动都要过权限评测集（详设 4.2，gate.go 用 permission 规则集）
	ObjectPolicy: {true},
	ObjectPrompt: {true},
}

var (
	routes           = []string{"keyword", "dense", "graph", "identifier"}
	weightsVersionRe = regexp.MustCompile(`^confidence-[0-9a-z@.-]{1,64}$`)
	// 模型名：OpenAI 兼容网关的 model 字段（如 qwen2.5-72b-instruct、org/model:tag）
	modelNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@-]{0,127}$`)
)

// agent 预算的平台上限（orchestrator 详设第 6 节：最多 3 轮、每轮最多 4 次工具调用），pipeline 只能调小
const (
	maxAgentRounds       = 3
	maxAgentSubQuestions = 4
)

// decodeStrict 解析 YAML，不认识的字段报错：拼错的键不能被静默忽略成默认值。
func decodeStrict(content string, out any) error {
	d := yaml.NewDecoder(bytes.NewReader([]byte(content)))
	d.KnownFields(true)
	if err := d.Decode(out); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

type pipelineYAML struct {
	Retrieval struct {
		DisabledRoutes []string           `yaml:"disabled_routes"`
		RouteWeights   map[string]float64 `yaml:"route_weights"`
		Rerank         *bool              `yaml:"rerank"`
	} `yaml:"retrieval"`
	Answer struct {
		SupportThreshold   *float64 `yaml:"support_threshold"`
		MinCoverage        *float64 `yaml:"min_coverage"`
		SingleHopLLMRerank bool     `yaml:"single_hop_llm_rerank"`
		// 租户书面同意放宽 SLO 的记录（如审批单号）：开启单跳 LLM 重排的前提（技术设计文档 6.7）
		RelaxedSLOAgreement string `yaml:"relaxed_slo_agreement"`
		// 租户规则（提示词第 2 区）：业务口径、术语、禁止话题、回答风格
		TenantRules string `yaml:"tenant_rules"`
	} `yaml:"answer"`
	Agent struct {
		MaxRounds       *int32 `yaml:"max_rounds"`
		MaxSubQuestions *int32 `yaml:"max_sub_questions"`
	} `yaml:"agent"`
}

// ParsePipeline 解析并校验一个租户的 pipeline.yaml。
func ParsePipeline(tenant, content string) (*configv1.PipelineConfig, error) {
	var y pipelineYAML
	if err := decodeStrict(content, &y); err != nil {
		return nil, fmt.Errorf("pipeline.yaml: %w", err)
	}
	r := y.Retrieval
	for _, d := range r.DisabledRoutes {
		if !slices.Contains(routes, d) {
			return nil, fmt.Errorf("disabled_routes: 未知的召回路 %q（可选 %s）", d, strings.Join(routes, "、"))
		}
	}
	if len(r.DisabledRoutes) >= len(routes) {
		return nil, errors.New("disabled_routes: 不能关闭全部召回路")
	}
	for name, w := range r.RouteWeights {
		if !slices.Contains(routes, name) {
			return nil, fmt.Errorf("route_weights: 未知的召回路 %q", name)
		}
		if math.IsNaN(w) || w < 0.1 || w > 5 {
			return nil, fmt.Errorf("route_weights.%s: 须在 0.1–5 之间", name)
		}
	}
	a := y.Answer
	if t := a.SupportThreshold; t != nil && (math.IsNaN(*t) || *t < 0.3 || *t > 0.9) {
		return nil, errors.New("support_threshold: 须在 0.3–0.9 之间")
	}
	if c := a.MinCoverage; c != nil && (math.IsNaN(*c) || *c < 0.3 || *c > 1) {
		return nil, errors.New("min_coverage: 须在 0.3–1 之间")
	}
	rules := strings.TrimSpace(a.TenantRules)
	if utf8.RuneCountInString(rules) > maxTenantRules {
		return nil, fmt.Errorf("tenant_rules: 不超过 %d 字", maxTenantRules)
	}
	if zoneMarkerRe.MatchString(rules) {
		return nil, errors.New("tenant_rules: 不能含提示词各区的标记（<evidence>、<question> 等）或 {{ 占位")
	}
	if a.SingleHopLLMRerank && strings.TrimSpace(a.RelaxedSLOAgreement) == "" {
		return nil, errors.New("single_hop_llm_rerank: 只对书面同意放宽 SLO 的租户开放，须填写 relaxed_slo_agreement")
	}
	ag := y.Agent
	if n := ag.MaxRounds; n != nil && (*n < 1 || *n > maxAgentRounds) {
		return nil, fmt.Errorf("agent.max_rounds: 须在 1–%d 之间", maxAgentRounds)
	}
	if n := ag.MaxSubQuestions; n != nil && (*n < 1 || *n > maxAgentSubQuestions) {
		return nil, fmt.Errorf("agent.max_sub_questions: 须在 1–%d 之间", maxAgentSubQuestions)
	}
	return &configv1.PipelineConfig{
		Tenant: tenant,
		Retrieval: &configv1.RetrievalPlan{
			DisabledRoutes: slices.Sorted(slices.Values(r.DisabledRoutes)), RouteWeights: r.RouteWeights, Rerank: r.Rerank,
		},
		Answer: &configv1.AnswerPlan{
			SupportThreshold: a.SupportThreshold, MinCoverage: a.MinCoverage, SingleHopLlmRerank: a.SingleHopLLMRerank,
			TenantRules: rules, AgentMaxRounds: ag.MaxRounds, AgentMaxSubQuestions: ag.MaxSubQuestions,
		},
	}, nil
}

type routingYAML struct {
	// 回答生成用的模型，须是部署里允许的（orchestrator 的 BF_LLM_ALT_MODELS）；不写用默认模型
	GenerationModel string `yaml:"generation_model"`
	Confidence      *struct {
		Version   string   `yaml:"version"`
		Coverage  *float64 `yaml:"coverage"`
		Support   *float64 `yaml:"support"`
		Rerank    *float64 `yaml:"rerank"`
		Diversity *float64 `yaml:"diversity"`
		Conflict  *float64 `yaml:"conflict"`
		Bias      *float64 `yaml:"bias"`
	} `yaml:"confidence"`
}

// ParseRouting 解析并校验全局的 routing.yaml。置信度权重整组给出（evaluation 校准报告里的一行），不能只改其中几项：
// 部分替换会让权重组合不再是校准过的那一组。
func ParseRouting(content string) (*configv1.RoutingConfig, error) {
	var y routingYAML
	if err := decodeStrict(content, &y); err != nil {
		return nil, fmt.Errorf("routing.yaml: %w", err)
	}
	out := &configv1.RoutingConfig{}
	if m := y.GenerationModel; m != "" {
		if !modelNameRe.MatchString(m) {
			return nil, errors.New("generation_model: 模型名不合法")
		}
		// governance 不知道各部署允许哪些模型：不在允许范围内的由 orchestrator 拒绝调用，评测门禁因此不会通过
		out.GenerationModel = m
	}
	if c := y.Confidence; c != nil {
		if !weightsVersionRe.MatchString(c.Version) {
			return nil, errors.New("confidence.version: 须是校准报告给出的权重版本（confidence- 开头）")
		}
		vals := []*float64{c.Coverage, c.Support, c.Rerank, c.Diversity, c.Conflict, c.Bias}
		for _, v := range vals {
			if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) || math.Abs(*v) > 20 {
				return nil, errors.New("confidence: coverage、support、rerank、diversity、conflict、bias 须全部给出，绝对值不超过 20")
			}
		}
		if *c.Coverage <= 0 || *c.Support <= 0 || *c.Conflict < 0 {
			// 覆盖率、支持度越高越可信，冲突只能降低置信度：方向反了说明拟合或抄写出错
			return nil, errors.New("confidence: coverage、support 须为正，conflict 不能为负")
		}
		out.Confidence = &configv1.ConfidenceWeights{Version: c.Version, Coverage: *c.Coverage, Support: *c.Support,
			Rerank: *c.Rerank, Diversity: *c.Diversity, Conflict: *c.Conflict, Bias: *c.Bias}
	}
	return out, nil
}

type promptYAML struct {
	System string `yaml:"system"`
	User   string `yaml:"user"`
}

// ParsePrompt 解析回答模板并做五分区检查（governance 详设第 2 节“提示词五分区占位”）：系统消息里只有 {{tenant_rules}}，
// 用户消息里按顺序各有一次该模板的区；不可信的区（证据、问题）只能在用户消息里，系统消息里没有任何外部输入。
func ParsePrompt(scope, content string) (*configv1.PromptTemplate, error) {
	zones, ok := promptZones[scope]
	if !ok {
		return nil, fmt.Errorf("prompt 的 scope 只能是 answer 或 agent-answer，得到 %q", scope)
	}
	var y promptYAML
	if err := decodeStrict(content, &y); err != nil {
		return nil, fmt.Errorf("prompt: %w", err)
	}
	names := func(text string) []string {
		var out []string
		for _, m := range placeholderRe.FindAllStringSubmatch(text, -1) {
			out = append(out, m[1])
		}
		return out
	}
	if strings.TrimSpace(y.System) == "" || strings.TrimSpace(y.User) == "" {
		return nil, errors.New("prompt: system 与 user 都要有内容")
	}
	if got := names(y.System); !slices.Equal(got, []string{"tenant_rules"}) {
		return nil, fmt.Errorf("prompt: system 里只能有一处 {{tenant_rules}}，得到 %v", got)
	}
	if got := names(y.User); !slices.Equal(got, zones) {
		return nil, fmt.Errorf("prompt: user 里要按顺序各有一处 %v，得到 %v", zones, got)
	}
	return &configv1.PromptTemplate{Scope: scope, System: y.System, User: y.User}, nil
}

// Prompts 是 orchestrator 快照里经发布的回答模板与它们的摘要（并入 snapshot_version）。
func (s *Server) Prompts(ctx context.Context) ([]*configv1.PromptTemplate, string, error) {
	var out []*configv1.PromptTemplate
	err := s.Store.InTx(ctx, func(tx Tx) error {
		active, err := tx.Active(ctx, ObjectPrompt)
		if err != nil {
			return err
		}
		for _, v := range active {
			p, err := ParsePrompt(v.Scope, v.Content)
			if err != nil {
				return fmt.Errorf("生效版本 %s 解析失败: %w", v.ID, err)
			}
			p.VersionId = v.ID
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	ids := make([]string, len(out))
	for i, p := range out {
		ids[i] = p.GetVersionId()
	}
	return out, hash(strings.Join(ids, ","))[:16], nil
}

// checkObject 校验 pipeline 与 routing：内容本身合法、作用域合法（pipeline 的租户要有生效的租户配置）。
// 不合法时返回 InvalidArgument；读库出错原样返回。
func checkObject(ctx context.Context, tx Tx, object, scope, content string) error {
	invalid := func(err error) error { return status.Error(codes.InvalidArgument, err.Error()) }
	switch object {
	case ObjectPipeline:
		tenants, err := tx.Active(ctx, ObjectTenant)
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(tenants, func(v Version) bool { return v.Scope == scope }) {
			return invalid(fmt.Errorf("租户 %s 没有生效的租户配置", scope))
		}
		if _, err := ParsePipeline(scope, content); err != nil {
			return invalid(err)
		}
		return nil
	case ObjectRouting:
		if scope != ScopeGlobal {
			return invalid(errors.New("routing 的 scope 只能是 global"))
		}
		if _, err := ParseRouting(content); err != nil {
			return invalid(err)
		}
		return nil
	case ObjectPrompt:
		if _, err := ParsePrompt(scope, content); err != nil {
			return invalid(err)
		}
		return nil
	}
	return invalid(fmt.Errorf("未知的配置对象 %q", object))
}

// Pipelines 是 retrieval 与 orchestrator 快照里各租户生效的 pipeline 与它们的摘要（并入 snapshot_version）。
func (s *Server) Pipelines(ctx context.Context) ([]*configv1.PipelineConfig, string, error) {
	var out []*configv1.PipelineConfig
	err := s.Store.InTx(ctx, func(tx Tx) error {
		active, err := tx.Active(ctx, ObjectPipeline)
		if err != nil {
			return err
		}
		for _, v := range active {
			p, err := ParsePipeline(v.Scope, v.Content)
			if err != nil {
				return fmt.Errorf("生效版本 %s 解析失败: %w", v.ID, err)
			}
			p.VersionId = v.ID
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	slices.SortFunc(out, func(a, b *configv1.PipelineConfig) int { return strings.Compare(a.GetTenant(), b.GetTenant()) })
	ids := make([]string, len(out))
	for i, p := range out {
		ids[i] = p.GetVersionId()
	}
	return out, hash(strings.Join(ids, ","))[:16], nil
}

// Routing 是 orchestrator 快照里全局生效的 routing；没有生效版本时为 nil，摘要为空串。
func (s *Server) Routing(ctx context.Context) (*configv1.RoutingConfig, string, error) {
	var out *configv1.RoutingConfig
	err := s.Store.InTx(ctx, func(tx Tx) error {
		active, err := tx.Active(ctx, ObjectRouting)
		if err != nil {
			return err
		}
		for _, v := range active {
			if v.Scope != ScopeGlobal {
				continue
			}
			r, err := ParseRouting(v.Content)
			if err != nil {
				return fmt.Errorf("生效版本 %s 解析失败: %w", v.ID, err)
			}
			r.VersionId = v.ID
			out = r
		}
		return nil
	})
	if err != nil || out == nil {
		return nil, "", err
	}
	return out, out.GetVersionId(), nil
}
