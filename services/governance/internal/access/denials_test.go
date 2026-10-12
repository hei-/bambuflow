package access

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/logging"
)

var quiet = logging.New(&bytes.Buffer{}, "t", slog.LevelInfo)

type staticVerifier struct{ p *identityv1.Principal }

func (v staticVerifier) Verify(context.Context, string) (*identityv1.Principal, error) {
	return v.p, nil
}

// as 返回带管理 Principal 与入口 trace_id 的上下文（经真实的拦截器放进去）。
func as(t *testing.T, p *identityv1.Principal) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer x", grpcmw.TraceIDKey, "trace-1"))
	var out context.Context
	_, err := grpcmw.UnaryServer(staticVerifier{p}, quiet)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/x/y"},
		func(ctx context.Context, _ any) (any, error) { out = ctx; return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	return out
}

type memStore struct {
	mu      sync.Mutex
	denials []Denial
	sources map[string]Source
	err     error
}

func (m *memStore) CreateDenial(_ context.Context, d Denial, beforeCommit func() error) (Denial, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, x := range m.denials {
		if x.Scope == d.Scope && x.Target == d.Target {
			return Denial{}, ErrExists
		}
	}
	if err := beforeCommit(); err != nil {
		return Denial{}, err
	}
	d.CreatedAt = time.Now()
	m.denials = append(m.denials, d)
	return d, nil
}

func (m *memStore) DenialByTarget(_ context.Context, scope, target string) (Denial, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, x := range m.denials {
		if x.Scope == scope && x.Target == target {
			return x, nil
		}
	}
	return Denial{}, ErrNotFound
}

func (m *memStore) ListDenials(_ context.Context, offset, limit int) ([]Denial, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.denials[min(offset, len(m.denials)):min(offset+limit, len(m.denials))], m.err
}

func (m *memStore) PendingDenials(context.Context) ([]Denial, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Denial
	for _, d := range m.denials {
		if d.AppliedAt == nil {
			out = append(out, d)
		}
	}
	return out, nil
}

func (m *memStore) MarkApplied(_ context.Context, id string) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for i := range m.denials {
		if m.denials[i].ID == id {
			m.denials[i].AppliedAt = &now
		}
	}
	return now, nil
}

func (m *memStore) UnpublishedDenials(context.Context) ([]Denial, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Denial
	for _, d := range m.denials {
		if d.PublishedAt == nil {
			out = append(out, d)
		}
	}
	return out, nil
}

func (m *memStore) MarkPublished(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for i := range m.denials {
		if m.denials[i].ID == id {
			m.denials[i].PublishedAt = &now
		}
	}
	return nil
}

type fakeBackup struct {
	mu        sync.Mutex
	published []Denial
	err       error
}

func (f *fakeBackup) Publish(_ context.Context, d Denial) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, d)
	return nil
}

func (f *fakeBackup) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

func (m *memStore) Source(_ context.Context, id string) (Source, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sources[id]
	if !ok {
		return Source{}, ErrNotFound
	}
	return s, nil
}

type memAudit struct {
	mu      sync.Mutex
	actions []string
	traces  []string
	fail    bool
}

func (a *memAudit) Write(ctx context.Context, action, objectID string, _ any) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail {
		return errors.New("审计桶不可用")
	}
	a.actions = append(a.actions, action+":"+objectID)
	a.traces = append(a.traces, grpcmw.TraceID(ctx))
	return nil
}

type fakeIndex struct {
	mu   sync.Mutex
	reqs []*adminv1.AddDenyRulesRequest
	err  error
}

func (f *fakeIndex) AddDenyRules(_ context.Context, in *adminv1.AddDenyRulesRequest, _ ...grpc.CallOption) (*adminv1.AddDenyRulesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.reqs = append(f.reqs, in)
	return &adminv1.AddDenyRulesResponse{Added: 1}, nil
}

func (f *fakeIndex) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

var (
	infosec    = &identityv1.Principal{Sub: "ivan", Dept: "信息安全部", Roles: []string{RoleInfoSec}}
	creditOwn  = &identityv1.Principal{Sub: "dana", Dept: "信贷部", Roles: []string{RoleDataOwner}}
	platformOp = &identityv1.Principal{Sub: "olga", Dept: "平台部", Roles: []string{"platform_operator"}}
)

func newDenialServer() (*DenialServer, *memStore, *memAudit, *fakeIndex) {
	st := &memStore{sources: map[string]Source{
		"src_credit": {ID: "src_credit", OwnerDept: "信贷部", Status: "active"},
		"src_risk":   {ID: "src_risk", OwnerDept: "风险管理部", Status: "active"},
	}}
	a, idx := &memAudit{}, &fakeIndex{}
	return &DenialServer{Store: st, Audit: a, Index: idx, Backup: &fakeBackup{}, Log: quiet}, st, a, idx
}

func req(scope adminv1.DenialScope, target string) *adminv1.CreateEmergencyDenialRequest {
	return &adminv1.CreateEmergencyDenialRequest{Scope: scope, Target: target, Reason: "工单 SEC-1"}
}

func TestCreateDenialScopesAndRoles(t *testing.T) {
	src, doc, ten := adminv1.DenialScope_DENIAL_SCOPE_SOURCE, adminv1.DenialScope_DENIAL_SCOPE_DOCUMENT, adminv1.DenialScope_DENIAL_SCOPE_TENANT
	cases := []struct {
		name   string
		p      *identityv1.Principal
		req    *adminv1.CreateEmergencyDenialRequest
		code   codes.Code
		reason string
	}{
		{"信息安全按文档", infosec, req(doc, "doc-1"), codes.OK, ""},
		{"信息安全按租户", infosec, req(ten, "credit"), codes.OK, ""},
		{"信息安全按未登记的数据源", infosec, req(src, "srcunknown"), codes.InvalidArgument, ""},
		{"属主按本部门数据源", creditOwn, req(src, "src_credit"), codes.OK, ""},
		{"属主按别的部门数据源", creditOwn, req(src, "src_risk"), codes.PermissionDenied, "out_of_scope"},
		{"属主按不存在的数据源", creditOwn, req(src, "src_nope"), codes.PermissionDenied, "out_of_scope"},
		{"属主按文档", creditOwn, req(doc, "doc-1"), codes.PermissionDenied, "out_of_scope"},
		{"平台运维", platformOp, req(doc, "doc-1"), codes.PermissionDenied, ""},
		{"缺原因", infosec, &adminv1.CreateEmergencyDenialRequest{Scope: doc, Target: "doc-2"}, codes.InvalidArgument, ""},
		{"未指定范围", infosec, &adminv1.CreateEmergencyDenialRequest{Target: "doc-2", Reason: "x"}, codes.InvalidArgument, ""},
		{"租户名不合法", infosec, req(ten, "Credit!"), codes.InvalidArgument, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _, a, idx := newDenialServer()
			resp, err := s.CreateEmergencyDenial(as(t, c.p), c.req)
			if status.Code(err) != c.code || grpcmw.ReasonOf(err) != c.reason {
				t.Fatalf("得到 %v（%s）", err, grpcmw.ReasonOf(err))
			}
			if c.code != codes.OK {
				if len(a.actions) != 0 || idx.count() != 0 {
					t.Fatal("被拒绝的请求不应登记或下发")
				}
				return
			}
			d := resp.GetDenial()
			if !resp.GetCreated() || d.GetStatus() != adminv1.DenialStatus_DENIAL_STATUS_ACTIVE || d.GetAppliedAt() == nil ||
				d.GetCreatedBy() != c.p.GetSub() || len(d.GetDenialId()) != len("den_")+24 {
				t.Fatalf("规则不对: %v", resp)
			}
			if len(a.actions) != 1 || a.traces[0] != "trace-1" || idx.count() != 1 {
				t.Fatalf("应写变更审计并下发: %v %d", a.actions, idx.count())
			}
			r := idx.reqs[0]
			if got := len(r.GetDocIds()) + len(r.GetSourceIds()) + len(r.GetTenants()); got != 1 {
				t.Fatalf("下发的规则数不对: %v", r)
			}
		})
	}
}

func TestDenialIsAddOnceAndFailClosedOnAudit(t *testing.T) {
	s, st, a, idx := newDenialServer()
	ctx := as(t, infosec)
	first, err := s.CreateEmergencyDenial(ctx, req(adminv1.DenialScope_DENIAL_SCOPE_DOCUMENT, "doc-1"))
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.CreateEmergencyDenial(ctx, req(adminv1.DenialScope_DENIAL_SCOPE_DOCUMENT, "doc-1"))
	if err != nil || again.GetCreated() || again.GetDenial().GetDenialId() != first.GetDenial().GetDenialId() || len(st.denials) != 1 {
		t.Fatalf("同一对象只登记一次: %v %v", again, err)
	}
	if idx.count() != 1 {
		t.Fatalf("已生效的规则不应重复下发: %d", idx.count())
	}
	a.fail = true
	if _, err := s.CreateEmergencyDenial(ctx, req(adminv1.DenialScope_DENIAL_SCOPE_DOCUMENT, "doc-2")); status.Code(err) != codes.Unavailable || len(st.denials) != 1 {
		t.Fatalf("变更审计写不进去时不能登记: %v", err)
	}
}

// 索引库不可用：规则登记为 pending 并返回（调用方看到的状态即告警），后台重试成功后变为 active。
func TestPendingDenialIsRetried(t *testing.T) {
	s, st, _, idx := newDenialServer()
	idx.err = status.Error(codes.Unavailable, "indexing 不可用")
	resp, err := s.CreateEmergencyDenial(as(t, infosec), req(adminv1.DenialScope_DENIAL_SCOPE_SOURCE, "src_credit"))
	if err != nil || resp.GetDenial().GetStatus() != adminv1.DenialStatus_DENIAL_STATUS_PENDING || resp.GetDenial().GetAppliedAt() != nil {
		t.Fatalf("应返回 pending: %v %v", resp, err)
	}
	idx.mu.Lock()
	idx.err = nil
	idx.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Reconcile(ctx, 10*time.Millisecond) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		pending, _ := st.PendingDenials(ctx)
		if len(pending) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("重试后应生效")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if idx.reqs[0].GetSourceIds()[0] != "src_credit" {
		t.Fatalf("下发内容不对: %v", idx.reqs[0])
	}
}

func TestListDenialsPages(t *testing.T) {
	s, _, _, _ := newDenialServer()
	ctx := as(t, infosec)
	for _, d := range []string{"a", "b", "c"} {
		if _, err := s.CreateEmergencyDenial(ctx, req(adminv1.DenialScope_DENIAL_SCOPE_DOCUMENT, d)); err != nil {
			t.Fatal(err)
		}
	}
	p1, err := s.ListEmergencyDenials(ctx, &adminv1.ListEmergencyDenialsRequest{PageSize: 2})
	if err != nil || len(p1.GetDenials()) != 2 || p1.GetNextPageToken() == "" {
		t.Fatalf("第一页: %v %v", p1, err)
	}
	p2, err := s.ListEmergencyDenials(ctx, &adminv1.ListEmergencyDenialsRequest{PageSize: 2, PageToken: p1.GetNextPageToken()})
	if err != nil || len(p2.GetDenials()) != 1 || p2.GetNextPageToken() != "" {
		t.Fatalf("第二页: %v %v", p2, err)
	}
	for _, bad := range []*adminv1.ListEmergencyDenialsRequest{{PageSize: 201}, {PageToken: "%%"}, {PageToken: "eDox"}} {
		if _, err := s.ListEmergencyDenials(ctx, bad); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%v 应为参数错误: %v", bad, err)
		}
	}
	if _, err := s.ListEmergencyDenials(as(t, platformOp), &adminv1.ListEmergencyDenialsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("平台运维不能查看: %v", err)
	}
}

// 备用通道与拒绝名单互不依赖：indexing 不可用时规则照样发到事件流；事件流不可用时 Reconcile 重发。
func TestBackupChannelIsIndependentAndRetried(t *testing.T) {
	s, st, _, idx := newDenialServer()
	backup := s.Backup.(*fakeBackup)
	idx.err = status.Error(codes.Unavailable, "indexing 不可用")
	if _, err := s.CreateEmergencyDenial(as(t, infosec), req(adminv1.DenialScope_DENIAL_SCOPE_TENANT, "credit")); err != nil {
		t.Fatal(err)
	}
	if backup.count() != 1 || backup.published[0].Target != "credit" || backup.published[0].CreatedBy != "ivan" {
		t.Fatalf("indexing 不可用时也应发到备用通道: %+v", backup.published)
	}

	backup.mu.Lock()
	backup.err = errors.New("Redis 不可用")
	backup.mu.Unlock()
	if _, err := s.CreateEmergencyDenial(as(t, infosec), req(adminv1.DenialScope_DENIAL_SCOPE_SOURCE, "src_risk")); err != nil {
		t.Fatal(err)
	}
	if un, _ := st.UnpublishedDenials(context.Background()); len(un) != 1 {
		t.Fatalf("没发出去的应记为未发布: %v", un)
	}
	backup.mu.Lock()
	backup.err = nil
	backup.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Reconcile(ctx, 10*time.Millisecond) }()
	deadline := time.Now().Add(2 * time.Second)
	for backup.count() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("恢复后应重发到备用通道")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}
