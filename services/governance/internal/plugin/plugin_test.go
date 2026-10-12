package plugin

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

type memStore struct{ ps []Plugin }

func (m *memStore) find(id string) (int, error) {
	for i, p := range m.ps {
		if p.ID == id {
			return i, nil
		}
	}
	return 0, ErrNotFound
}

func (m *memStore) Create(_ context.Context, p Plugin, beforeCommit func() error) (Plugin, error) {
	for _, x := range m.ps {
		if x.Name == p.Name && x.Version == p.Version {
			return Plugin{}, ErrDuplicate
		}
	}
	if err := beforeCommit(); err != nil {
		return Plugin{}, err
	}
	m.ps = append(m.ps, p)
	return p, nil
}

func (m *memStore) Get(_ context.Context, id string) (Plugin, error) {
	i, err := m.find(id)
	if err != nil {
		return Plugin{}, err
	}
	return m.ps[i], nil
}

func (m *memStore) List(_ context.Context, offset, limit int) ([]Plugin, error) {
	return m.ps[min(offset, len(m.ps)):min(offset+limit, len(m.ps))], nil
}

func (m *memStore) Update(_ context.Context, id string, change func(p *Plugin) error) (Plugin, error) {
	i, err := m.find(id)
	if err != nil {
		return Plugin{}, err
	}
	p := m.ps[i]
	if err := change(&p); err != nil {
		return Plugin{}, err
	}
	m.ps[i] = p
	return p, nil
}

func (m *memStore) RecordEvaluation(_ context.Context, id string, e Evaluation, beforeCommit func() error) (Plugin, error) {
	i, err := m.find(id)
	if err != nil {
		return Plugin{}, err
	}
	if err := beforeCommit(); err != nil {
		return Plugin{}, err
	}
	m.ps[i].Evaluations = append(slices.DeleteFunc(m.ps[i].Evaluations, func(x Evaluation) bool { return x.Tenant == e.Tenant }), e)
	return m.ps[i], nil
}

type memAudit struct{ actions []string }

func (a *memAudit) Write(_ context.Context, action, _ string, _ any) error {
	a.actions = append(a.actions, action)
	return nil
}

type staticVerifier struct{ p *identityv1.Principal }

func (v staticVerifier) Verify(context.Context, string) (*identityv1.Principal, error) {
	return v.p, nil
}

func as(t *testing.T, sub string, roles ...string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer x"))
	var out context.Context
	if _, err := grpcmw.UnaryServer(staticVerifier{&identityv1.Principal{Sub: sub, Roles: roles}}, slog.New(slog.NewTextHandler(io.Discard, nil)))(
		ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/x/y"}, func(ctx context.Context, _ any) (any, error) { out = ctx; return nil, nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPluginLifecycle(t *testing.T) {
	audit := &memAudit{}
	s := &Server{Store: &memStore{}, Audit: audit, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: time.Now}
	paul, ivan, ivan2 := as(t, "paul", RolePlatformAdmin), as(t, "ivan", RoleInfoSec), as(t, "paul", RoleInfoSec)

	bad := map[string]*adminv1.RegisterPluginRequest{
		"未知扩展点":       {Kind: "widget", Name: "x-1", Version: "1.0", Description: "x"},
		"名字不合法":       {Kind: "parser", Name: "X", Version: "1.0", Description: "x"},
		"缺说明":         {Kind: "parser", Name: "ocr-paddle", Version: "1.0"},
		"Tool 不声明范围":  {Kind: "tool", Name: "ledger", Version: "1.0", Description: "x"},
		"Tool 写范围":    {Kind: "tool", Name: "ledger", Version: "1.0", Description: "x", ToolScopes: []string{"write:ledger"}},
		"非 Tool 声明范围": {Kind: "parser", Name: "x-1", Version: "1.0", Description: "x", ToolScopes: []string{"read:a"}},
	}
	for name, r := range bad {
		if _, err := s.RegisterPlugin(paul, r); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s 应为参数错误: %v", name, err)
		}
	}
	if _, err := s.RegisterPlugin(ivan, &adminv1.RegisterPluginRequest{Kind: "parser", Name: "x-1", Version: "1", Description: "x"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("信息安全不能登记: %v", err)
	}
	tool, err := s.RegisterPlugin(paul, &adminv1.RegisterPluginRequest{Kind: "tool", Name: "ledger-query", Version: "1.2.0",
		Description: "查询台账", ToolScopes: []string{"read:ledger", "read:ledger"}})
	if err != nil || tool.GetPlugin().GetHost() != "orchestrator" || tool.GetPlugin().GetSecurityReview() != "pending" ||
		len(tool.GetPlugin().GetToolScopes()) != 1 {
		t.Fatalf("登记 Tool: %v %v", tool, err)
	}
	id := tool.GetPlugin().GetPluginId()
	if _, err := s.RegisterPlugin(paul, &adminv1.RegisterPluginRequest{Kind: "tool", Name: "ledger-query", Version: "1.2.0",
		Description: "重复", ToolScopes: []string{"read:ledger"}}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("同名同版本重复登记: %v", err)
	}

	// 评估通过但安全评审没过：不能被引用
	ev, err := s.RecordEvaluation(context.Background(), &adminv1.RecordEvaluationRequest{PluginId: id, Tenant: "credit", Passed: true, ReportRef: "eval-run-1"})
	if err != nil || len(ev.GetPlugin().GetEvaluations()) != 1 || len(ev.GetPlugin().GetUsableTenants()) != 0 {
		t.Fatalf("评估: %v %v", ev, err)
	}
	if _, err := s.ReviewPlugin(ivan2, &adminv1.ReviewPluginRequest{PluginId: id, Approved: true, Note: "自己评自己"}); grpcmw.ReasonOf(err) != "self_approval_forbidden" {
		t.Fatalf("登记人不能评审: %v", err)
	}
	rv, err := s.ReviewPlugin(ivan, &adminv1.ReviewPluginRequest{PluginId: id, Approved: true, Note: "只读，范围合理"})
	if err != nil || rv.GetPlugin().GetSecurityReview() != "approved" || !slices.Equal(rv.GetPlugin().GetUsableTenants(), []string{"credit"}) {
		t.Fatalf("评审: %v %v", rv, err)
	}
	if _, err := s.ReviewPlugin(ivan, &adminv1.ReviewPluginRequest{PluginId: id, Approved: false, Note: "再评"}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("不能重复评审: %v", err)
	}
	parser, _ := s.RegisterPlugin(paul, &adminv1.RegisterPluginRequest{Kind: "parser", Name: "ocr-paddle", Version: "2", Description: "OCR"})
	if _, err := s.ReviewPlugin(ivan, &adminv1.ReviewPluginRequest{PluginId: parser.GetPlugin().GetPluginId(), Approved: true, Note: "x"}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("非 Tool 不需要评审: %v", err)
	}
	list, err := s.ListPlugins(ivan, &adminv1.ListPluginsRequest{PageSize: 1})
	if err != nil || len(list.GetPlugins()) != 1 || list.GetNextPageToken() == "" {
		t.Fatalf("列表: %v %v", list, err)
	}
	want := []string{"plugin.register", "plugin.evaluation", "plugin.review", "plugin.register"}
	if !slices.Equal(audit.actions, want) {
		t.Fatalf("审计: %v", audit.actions)
	}
}
