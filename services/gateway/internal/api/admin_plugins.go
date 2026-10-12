package api

import (
	"context"
	"net/http"
	"regexp"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 插件登记（governance 详设 3.1、7.2）：GET/POST /v1/admin/plugins，POST .../{plugin_id}/security-review（Tool 插件）。

var pluginIDPattern = regexp.MustCompile(`^plg_[a-f0-9]{24}$`)

type pluginEvaluationJSON struct {
	Tenant      string `json:"tenant"`
	Passed      bool   `json:"passed"`
	ReportRef   string `json:"report_ref"`
	EvaluatedAt string `json:"evaluated_at"`
}

type pluginJSON struct {
	PluginID       string                 `json:"plugin_id"`
	Kind           string                 `json:"kind"`
	Name           string                 `json:"name"`
	Version        string                 `json:"version"`
	Host           string                 `json:"host"`
	Description    string                 `json:"description"`
	ToolScopes     []string               `json:"tool_scopes"`
	RegisteredBy   string                 `json:"registered_by"`
	RegisteredAt   string                 `json:"registered_at"`
	Evaluations    []pluginEvaluationJSON `json:"evaluations"`
	SecurityReview string                 `json:"security_review"`
	ReviewedBy     string                 `json:"reviewed_by,omitempty"`
	ReviewedAt     string                 `json:"reviewed_at,omitempty"`
	ReviewNote     string                 `json:"review_note,omitempty"`
	UsableTenants  []string               `json:"usable_tenants"`
}

func pluginOf(p *adminv1.Plugin) pluginJSON {
	out := pluginJSON{PluginID: p.GetPluginId(), Kind: p.GetKind(), Name: p.GetName(), Version: p.GetVersion(), Host: p.GetHost(),
		Description: p.GetDescription(), ToolScopes: nonNil(p.GetToolScopes()), RegisteredBy: p.GetRegisteredBy(),
		RegisteredAt: rfc3339(p.GetRegisteredAt()), Evaluations: []pluginEvaluationJSON{}, SecurityReview: p.GetSecurityReview(),
		ReviewedBy: p.GetReviewedBy(), ReviewedAt: rfc3339(p.GetReviewedAt()), ReviewNote: p.GetReviewNote(),
		UsableTenants: nonNil(p.GetUsableTenants())}
	for _, e := range p.GetEvaluations() {
		out.Evaluations = append(out.Evaluations, pluginEvaluationJSON{Tenant: e.GetTenant(), Passed: e.GetPassed(),
			ReportRef: e.GetReportRef(), EvaluatedAt: rfc3339(e.GetEvaluatedAt())})
	}
	return out
}

type pluginListJSON struct {
	Plugins       []pluginJSON `json:"plugins"`
	NextPageToken string       `json:"next_page_token,omitempty"`
}

func (s *Server) listPlugins(ctx context.Context, in *adminInput) (adminOutput, error) {
	size, err := pageSize(in)
	if err != nil {
		return adminOutput{}, err
	}
	r, err := s.Plugins.ListPlugins(ctx, &adminv1.ListPluginsRequest{PageSize: size, PageToken: in.query.Get("page_token")})
	if err != nil {
		return adminOutput{}, err
	}
	out := pluginListJSON{Plugins: []pluginJSON{}, NextPageToken: r.GetNextPageToken()}
	for _, p := range r.GetPlugins() {
		out.Plugins = append(out.Plugins, pluginOf(p))
	}
	return adminOutput{body: out}, nil
}

type registerPluginJSON struct {
	Kind        string   `json:"kind"`
	Name        string   `json:"name"`
	Version     string   `json:"version"`
	Description string   `json:"description"`
	ToolScopes  []string `json:"tool_scopes"`
}

func (s *Server) registerPlugin(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req registerPluginJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Plugins.RegisterPlugin(ctx, &adminv1.RegisterPluginRequest{Kind: req.Kind, Name: req.Name, Version: req.Version,
		Description: req.Description, ToolScopes: req.ToolScopes})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{status: http.StatusCreated, body: pluginOf(r.GetPlugin())}, nil
}

type reviewPluginJSON struct {
	Approved bool   `json:"approved"`
	Note     string `json:"note"`
}

func (s *Server) reviewPlugin(ctx context.Context, in *adminInput) (adminOutput, error) {
	id := in.params["plugin_id"]
	if !pluginIDPattern.MatchString(id) {
		return adminOutput{}, problemNotFound
	}
	var req reviewPluginJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Plugins.ReviewPlugin(ctx, &adminv1.ReviewPluginRequest{PluginId: id, Approved: req.Approved, Note: req.Note})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: pluginOf(r.GetPlugin())}, nil
}
