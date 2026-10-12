// Package plugin 是插件登记（governance 详设 3.1、7.2，技术设计文档 12.4）：平台管理员登记插件版本，
// CI 写入各租户评测集上的评估结果，信息安全评审 Tool 插件。插件在某租户评估通过（Tool 还要评审通过）后，
// 该租户的 pipeline.yaml 才能引用它。
package plugin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/services/governance/internal/paging"
)

const (
	RolePlatformAdmin = "platform_admin"
	RoleInfoSec       = "infosec"
)

var (
	ErrNotFound  = errors.New("插件不存在")
	ErrDuplicate = errors.New("同名同版本已登记")

	// 7 个扩展点（技术设计文档 12.1）与宿主模块（插件放在 plugins/<宿主>/）
	hosts = map[string]string{
		"connector": "connectors", "parser": "processing", "chunker": "processing", "retriever": "retrieval",
		"reranker": "retrieval", "model": "orchestrator", "tool": "orchestrator",
	}
	namePattern    = regexp.MustCompile(`^[a-z][a-z0-9-]{1,63}$`)
	versionPattern = regexp.MustCompile(`^[A-Za-z0-9._+-]{1,64}$`)
	idPattern      = regexp.MustCompile(`^plg_[0-9a-f]{24}$`)
	tenantPattern  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	// Tool 的读写范围：内核只放行只读调用（技术设计文档 12.4），所以只接受 read: 开头的声明
	scopePattern = regexp.MustCompile(`^read:[a-z0-9_.:-]{1,64}$`)
)

type Evaluation struct {
	Tenant, ReportRef string
	Passed            bool
	EvaluatedAt       time.Time
}

type Plugin struct {
	ID, Kind, Name, Version, Host, Description, RegisteredBy string
	ToolScopes                                               []string
	RegisteredAt                                             time.Time
	SecurityReview, ReviewedBy, ReviewNote                   string
	ReviewedAt                                               *time.Time
	Evaluations                                              []Evaluation
}

// Store 是插件登记的读写（生产为 PG）。beforeCommit 写变更审计，与改动同一事务。
type Store interface {
	Create(ctx context.Context, p Plugin, beforeCommit func() error) (Plugin, error)
	Get(ctx context.Context, id string) (Plugin, error)
	List(ctx context.Context, offset, limit int) ([]Plugin, error)
	Update(ctx context.Context, id string, change func(p *Plugin) error) (Plugin, error)
	RecordEvaluation(ctx context.Context, id string, e Evaluation, beforeCommit func() error) (Plugin, error)
}

type Auditor interface {
	Write(ctx context.Context, action, objectID string, detail any) error
}

// Server 实现 PluginAdminService 与 PluginEvaluationService。
type Server struct {
	adminv1.UnimplementedPluginAdminServiceServer
	adminv1.UnimplementedPluginEvaluationServiceServer
	Store Store
	Audit Auditor
	Log   *slog.Logger
	Now   func() time.Time
}

func text(s string, max int) (string, bool) {
	s = strings.TrimSpace(s)
	n := utf8.RuneCountInString(s)
	return s, n > 0 && n <= max
}

func (s *Server) storeErr(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, "插件不存在")
	case errors.Is(err, ErrDuplicate):
		return grpcmw.Reason(codes.FailedPrecondition, "conflict", ErrDuplicate.Error())
	}
	var st interface{ GRPCStatus() *status.Status }
	if errors.As(err, &st) {
		return err
	}
	s.Log.ErrorContext(ctx, "读写插件登记失败", slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}

func (s *Server) ListPlugins(ctx context.Context, r *adminv1.ListPluginsRequest) (*adminv1.ListPluginsResponse, error) {
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), RolePlatformAdmin, RoleInfoSec) {
		return nil, status.Error(codes.PermissionDenied, "需要平台管理员或信息安全")
	}
	size, offset, err := paging.Parse(r.GetPageSize(), r.GetPageToken())
	if err != nil {
		return nil, err
	}
	list, err := s.Store.List(ctx, offset, size+1)
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	out := &adminv1.ListPluginsResponse{}
	if len(list) > size {
		list, out.NextPageToken = list[:size], paging.Token(offset+size)
	}
	for _, p := range list {
		out.Plugins = append(out.Plugins, p.proto())
	}
	return out, nil
}

func (s *Server) RegisterPlugin(ctx context.Context, r *adminv1.RegisterPluginRequest) (*adminv1.RegisterPluginResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RolePlatformAdmin) {
		return nil, status.Error(codes.PermissionDenied, "需要平台管理员")
	}
	host, ok := hosts[r.GetKind()]
	desc, descOK := text(r.GetDescription(), 500)
	if !ok || !namePattern.MatchString(r.GetName()) || !versionPattern.MatchString(r.GetVersion()) || !descOK {
		return nil, status.Error(codes.InvalidArgument, "kind、name、version 或 description 不合法")
	}
	scopes := slices.Clone(r.GetToolScopes())
	slices.Sort(scopes)
	scopes = slices.Compact(scopes)
	review := "not_required"
	if r.GetKind() == "tool" {
		review = "pending"
		if len(scopes) == 0 || len(scopes) > 20 {
			return nil, status.Error(codes.InvalidArgument, "Tool 插件必须声明 1–20 个读写范围")
		}
		for _, sc := range scopes {
			if !scopePattern.MatchString(sc) {
				return nil, status.Error(codes.InvalidArgument, "Tool 插件只能声明只读范围（read: 开头）："+sc)
			}
		}
	} else if len(scopes) > 0 {
		return nil, status.Error(codes.InvalidArgument, "只有 Tool 插件声明读写范围")
	}
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	pl := Plugin{ID: "plg_" + hex.EncodeToString(b), Kind: r.GetKind(), Name: r.GetName(), Version: r.GetVersion(), Host: host,
		Description: desc, ToolScopes: scopes, RegisteredBy: p.GetSub(), RegisteredAt: s.Now(), SecurityReview: review}
	pl, err := s.Store.Create(ctx, pl, func() error {
		return s.Audit.Write(ctx, "plugin.register", pl.ID, map[string]any{"kind": pl.Kind, "name": pl.Name, "version": pl.Version,
			"tool_scopes": pl.ToolScopes})
	})
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	s.Log.InfoContext(ctx, "插件已登记", slog.String("plugin_id", pl.ID), slog.String("name", pl.Name), slog.String("version", pl.Version))
	return &adminv1.RegisterPluginResponse{Plugin: pl.proto()}, nil
}

func (s *Server) ReviewPlugin(ctx context.Context, r *adminv1.ReviewPluginRequest) (*adminv1.ReviewPluginResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RoleInfoSec) {
		return nil, status.Error(codes.PermissionDenied, "需要信息安全")
	}
	note, ok := text(r.GetNote(), 500)
	if !idPattern.MatchString(r.GetPluginId()) || !ok {
		return nil, status.Error(codes.InvalidArgument, "plugin_id 或 note 不合法")
	}
	pl, err := s.Store.Update(ctx, r.GetPluginId(), func(pl *Plugin) error {
		switch {
		case pl.SecurityReview == "not_required":
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "只有 Tool 插件需要安全评审")
		case pl.SecurityReview != "pending":
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "已经评审过")
		case pl.RegisteredBy == p.GetSub():
			return grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "不能评审自己登记的插件")
		}
		now := s.Now()
		pl.SecurityReview, pl.ReviewedBy, pl.ReviewedAt, pl.ReviewNote = "rejected", p.GetSub(), &now, note
		if r.GetApproved() {
			pl.SecurityReview = "approved"
		}
		return s.Audit.Write(ctx, "plugin.review", pl.ID, map[string]any{"result": pl.SecurityReview, "note": note})
	})
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	return &adminv1.ReviewPluginResponse{Plugin: pl.proto()}, nil
}

// RecordEvaluation 由 CI 写入评估结果（服务间接口，没有 Principal；调用方由服务网格限定为 CI 的服务身份）。
func (s *Server) RecordEvaluation(ctx context.Context, r *adminv1.RecordEvaluationRequest) (*adminv1.RecordEvaluationResponse, error) {
	ref, ok := text(r.GetReportRef(), 500)
	if !idPattern.MatchString(r.GetPluginId()) || !tenantPattern.MatchString(r.GetTenant()) || !ok {
		return nil, status.Error(codes.InvalidArgument, "plugin_id、tenant 或 report_ref 不合法")
	}
	e := Evaluation{Tenant: r.GetTenant(), Passed: r.GetPassed(), ReportRef: ref, EvaluatedAt: s.Now()}
	pl, err := s.Store.RecordEvaluation(ctx, r.GetPluginId(), e, func() error {
		return s.Audit.Write(ctx, "plugin.evaluation", r.GetPluginId(), map[string]any{"tenant": e.Tenant, "passed": e.Passed,
			"report_ref": e.ReportRef})
	})
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	return &adminv1.RecordEvaluationResponse{Plugin: pl.proto()}, nil
}

// UsableTenants 是可以引用该插件的租户：评估通过，且 Tool 插件的安全评审已通过。
func (p Plugin) UsableTenants() []string {
	if p.SecurityReview != "not_required" && p.SecurityReview != "approved" {
		return nil
	}
	var out []string
	for _, e := range p.Evaluations {
		if e.Passed {
			out = append(out, e.Tenant)
		}
	}
	slices.Sort(out)
	return out
}

func (p Plugin) proto() *adminv1.Plugin {
	out := &adminv1.Plugin{PluginId: p.ID, Kind: p.Kind, Name: p.Name, Version: p.Version, Host: p.Host, Description: p.Description,
		ToolScopes: p.ToolScopes, RegisteredBy: p.RegisteredBy, RegisteredAt: timestamppb.New(p.RegisteredAt),
		SecurityReview: p.SecurityReview, ReviewedBy: p.ReviewedBy, ReviewNote: p.ReviewNote, UsableTenants: p.UsableTenants()}
	if p.ReviewedAt != nil {
		out.ReviewedAt = timestamppb.New(*p.ReviewedAt)
	}
	for _, e := range p.Evaluations {
		out.Evaluations = append(out.Evaluations, &adminv1.PluginEvaluation{Tenant: e.Tenant, Passed: e.Passed, ReportRef: e.ReportRef,
			EvaluatedAt: timestamppb.New(e.EvaluatedAt)})
	}
	return out
}
