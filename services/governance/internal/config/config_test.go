package config

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	evalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/evaluation/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

// memStore 是 Store 的内存实现：InTx 失败时整体回滚（拷贝一份状态再改）。
type memStore struct{ st memState }

type memState struct {
	versions map[string]Version
	pointers map[string]string
	releases map[string]Release
	changes  map[string]Change
	rollouts map[string]Rollout
}

func (s memState) clone() memState {
	c := memState{versions: map[string]Version{}, pointers: map[string]string{}, releases: map[string]Release{}, changes: map[string]Change{},
		rollouts: map[string]Rollout{}}
	for k, v := range s.rollouts {
		c.rollouts[k] = v
	}
	for k, v := range s.versions {
		c.versions[k] = v
	}
	for k, v := range s.pointers {
		c.pointers[k] = v
	}
	for k, v := range s.releases {
		c.releases[k] = v
	}
	for k, v := range s.changes {
		c.changes[k] = v
	}
	return c
}

func newMem() *memStore { return &memStore{st: memState{}.clone()} }

func (m *memStore) InTx(_ context.Context, fn func(Tx) error) error {
	tx := &memTx{m.st.clone()}
	if err := fn(tx); err != nil {
		return err
	}
	m.st = tx.memState
	return nil
}

type memTx struct{ memState }

func key(object, scope string) string { return object + "/" + scope }

func (t *memTx) Active(_ context.Context, object string) ([]Version, error) {
	var out []Version
	for k, id := range t.pointers {
		if strings.HasPrefix(k, object+"/") {
			out = append(out, t.versions[id])
		}
	}
	slices.SortFunc(out, func(a, b Version) int { return strings.Compare(a.Scope, b.Scope) })
	return out, nil
}

func (t *memTx) LockRollout(_ context.Context, object, scope string) (Rollout, bool, error) {
	r, ok := t.rollouts[key(object, scope)]
	return r, ok, nil
}

func (t *memTx) InsertRollout(_ context.Context, r Rollout) error {
	if _, ok := t.rollouts[key(r.Object, r.Scope)]; ok {
		return ErrBusy
	}
	t.rollouts[key(r.Object, r.Scope)] = r
	return nil
}

func (t *memTx) DeleteRollout(_ context.Context, object, scope string) error {
	delete(t.rollouts, key(object, scope))
	return nil
}

func (t *memTx) ObserveRollout(_ context.Context, object, scope, observation string, at time.Time) error {
	if r, ok := t.rollouts[key(object, scope)]; ok {
		r.Observation, r.ObservedAt = observation, &at
		t.rollouts[key(object, scope)] = r
	}
	return nil
}

func (t *memTx) Rollouts(context.Context) ([]Rollout, error) {
	var out []Rollout
	for _, r := range t.rollouts {
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b Rollout) int { return strings.Compare(a.Object+a.Scope, b.Object+b.Scope) })
	return out, nil
}

func (t *memTx) ExpireBefore(_ context.Context, before time.Time) ([]string, []string, error) {
	var rels, chgs []string
	for id, r := range t.releases {
		if slices.Contains([]string{"gating", "pending_approval", "pending_second_approval"}, r.Status) && r.RequestedAt.Before(before) {
			r.Status = "expired"
			t.releases[id] = r
			rels = append(rels, id)
		}
	}
	for id, c := range t.changes {
		if c.Status == "open" && c.CreatedAt.Before(before) {
			c.Status = "expired"
			t.changes[id] = c
			chgs = append(chgs, id)
		}
	}
	return rels, chgs, nil
}

func (t *memTx) LockPointer(_ context.Context, object, scope string) (string, error) {
	return t.pointers[key(object, scope)], nil
}

func (t *memTx) SetPointer(_ context.Context, object, scope, id string) error {
	t.pointers[key(object, scope)] = id
	return nil
}

func (t *memTx) NextNumber(_ context.Context, object, scope string) (int32, error) {
	var n int32
	for _, v := range t.versions {
		if v.Object == object && v.Scope == scope && v.Number > n {
			n = v.Number
		}
	}
	return n + 1, nil
}

func (t *memTx) InsertVersion(_ context.Context, v Version) error { t.versions[v.ID] = v; return nil }

func (t *memTx) Version(_ context.Context, id string) (Version, error) {
	v, ok := t.versions[id]
	if !ok {
		return Version{}, ErrNotFound
	}
	return v, nil
}

func (t *memTx) Versions(_ context.Context, object, scope string) ([]Version, error) {
	var out []Version
	for _, v := range t.versions {
		if v.Object == object && v.Scope == scope {
			out = append(out, v)
		}
	}
	slices.SortFunc(out, func(a, b Version) int { return int(b.Number - a.Number) })
	return out, nil
}

func (t *memTx) InsertRelease(_ context.Context, r Release) error { t.releases[r.ID] = r; return nil }

func (t *memTx) LockRelease(_ context.Context, id string) (Release, error) {
	r, ok := t.releases[id]
	if !ok {
		return Release{}, ErrNotFound
	}
	return r, nil
}

func (t *memTx) UpdateRelease(_ context.Context, r Release) error { t.releases[r.ID] = r; return nil }

func (t *memTx) SupersedeActive(_ context.Context, object, scope string) error {
	for id, r := range t.releases {
		if r.Object == object && r.Scope == scope && r.Status == "active" {
			r.Status = "superseded"
			t.releases[id] = r
		}
	}
	return nil
}

func (t *memTx) InsertChange(_ context.Context, c Change) error {
	for _, x := range t.changes {
		if x.Object == c.Object && x.Scope == c.Scope && x.Status == "open" {
			return ErrBusy
		}
	}
	t.changes[c.ID] = c
	return nil
}

func (t *memTx) LockChange(_ context.Context, id string) (Change, error) {
	c, ok := t.changes[id]
	if !ok {
		return Change{}, ErrNotFound
	}
	return c, nil
}

func (t *memTx) UpdateChange(_ context.Context, c Change) error { t.changes[c.ID] = c; return nil }

func (t *memTx) Changes(_ context.Context, status string, limit int) ([]Change, error) {
	var out []Change
	for _, c := range t.changes {
		if status == "" || c.Status == status {
			out = append(out, c)
		}
	}
	slices.SortFunc(out, func(a, b Change) int { return b.CreatedAt.Compare(a.CreatedAt) })
	return out[:min(len(out), limit)], nil
}

func (t *memTx) Releases(_ context.Context, status string, limit int) ([]Release, error) {
	var out []Release
	for _, r := range t.releases {
		if status == "" || r.Status == status {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, func(a, b Release) int { return b.RequestedAt.Compare(a.RequestedAt) })
	return out[:min(len(out), limit)], nil
}

func (t *memTx) ReleasesOf(_ context.Context, versionID string) ([]Release, error) {
	var out []Release
	for _, r := range t.releases {
		if r.VersionID == versionID {
			out = append(out, r)
		}
	}
	return out, nil
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

func as(t *testing.T, sub string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer x"))
	var out context.Context
	p := &identityv1.Principal{Sub: sub, Roles: []string{RolePlatformAdmin}}
	switch sub {
	case "audra":
		p.Roles = []string{"auditor"}
	case "ivy": // 信息安全
		p.Roles = []string{RoleInfoSec}
	case "tina", "tom": // 租户负责人：tina 负责 ops，tom 负责 credit
		p.Roles = map[string][]string{"tina": {"tenant_owner:ops"}, "tom": {"tenant_owner:credit"}}[sub]
	}
	if _, err := grpcmw.UnaryServer(staticVerifier{p}, slog.New(slog.NewTextHandler(io.Discard, nil)))(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/x/y"}, func(ctx context.Context, _ any) (any, error) { out = ctx; return nil, nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

const passedGate = "gate_000000000000000000000001"

// fakeEvals 是 evaluation 的 EvalService：门禁发起后查一次就得出 verdicts 里按候选版本给的结论（默认通过）。
type fakeEvals struct {
	runs     []*evalv1.RunGateRequest
	verdicts map[string]evalv1.Verdict
	// runErr 不为空时发起门禁失败（evaluation 不可用等）
	runErr error
}

func (f *fakeEvals) RunGate(_ context.Context, in *evalv1.RunGateRequest, _ ...grpc.CallOption) (*evalv1.RunGateResponse, error) {
	if f.runErr != nil {
		return nil, f.runErr
	}
	f.runs = append(f.runs, in)
	return &evalv1.RunGateResponse{GateId: fmt.Sprintf("gate_%024d", len(f.runs))}, nil
}

func (f *fakeEvals) GetGate(_ context.Context, in *evalv1.GetGateRequest, _ ...grpc.CallOption) (*evalv1.GetGateResponse, error) {
	var n int
	if _, err := fmt.Sscanf(in.GetGateId(), "gate_%d", &n); err != nil || n < 1 || n > len(f.runs) {
		return nil, status.Error(codes.NotFound, "门禁不存在")
	}
	v, ok := f.verdicts[f.runs[n-1].GetCandidate().GetConfigVersion()]
	if !ok {
		v = evalv1.Verdict_VERDICT_PASSED
	}
	return &evalv1.GetGateResponse{GateId: in.GetGateId(), Verdict: v, Violations: map[bool][]string{true: {"引用覆盖率下降 3 个点"}}[v == evalv1.Verdict_VERDICT_FAILED]}, nil
}

func newServer() (*Server, *memStore, *memAudit) {
	store, audit := newMem(), &memAudit{}
	return &Server{Store: store, Audit: audit, PlatformClients: []string{"console"}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now: time.Now, Evals: &fakeEvals{}, Gate: func(_ context.Context, gateID, tenant, stage string) error {
			// 切全量时要求门禁里这个租户的评测集已到 general 阶段
			if gateID == passedGate && tenant == "credit" && stage == "general" {
				return nil
			}
			return fmt.Errorf("%w：%s", ErrGateFailed, gateID)
		}}, store, audit
}

// passGates 让门禁中的发布走完门禁（发起一轮、查结论一轮）。
func passGates(t *testing.T, s *Server) {
	t.Helper()
	for range 2 {
		if _, err := s.AdvanceGates(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func tenantYAML(name, rollout string, extra ...string) string {
	return "tenant: " + name + "\nrollout: " + rollout + "\nmember_groups: [" + name + "-g]\n" + strings.Join(extra, "")
}

func code(err error) (codes.Code, string) { return status.Code(err), grpcmw.ReasonOf(err) }

func TestBootstrapImportsOnlyMissingTenants(t *testing.T) {
	s, _, _ := newServer()
	ctx := context.Background()
	n, err := s.Bootstrap(ctx, map[string][]byte{"credit": []byte(tenantYAML("credit", "general")), "ops": []byte(tenantYAML("ops", "pilot"))})
	if err != nil || n != 2 {
		t.Fatalf("%d %v", n, err)
	}
	// 再次启动：已有版本的租户不动，新的才导入
	n, err = s.Bootstrap(ctx, map[string][]byte{"credit": []byte(tenantYAML("credit", "pilot")), "risk": []byte(tenantYAML("risk", "pilot"))})
	if err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	tenants, digest, err := s.Snapshot(ctx)
	if err != nil || len(tenants) != 3 || tenants[0].GetTenant() != "credit" || !strings.Contains(tenants[0].GetContent(), "general") || digest == "" {
		t.Fatalf("%v %s %v", tenants, digest, err)
	}
	// 一份不合法，整批都不导入
	if _, err := s.Bootstrap(ctx, map[string][]byte{"x": []byte("tenant: y\nrollout: general\n")}); err == nil {
		t.Fatal("不合法的配置应拒绝")
	}
}

func TestChangeRequestNeedsAnotherAdminAndCurrentBase(t *testing.T) {
	s, _, audit := newServer()
	ctx := context.Background()
	if _, err := s.Bootstrap(ctx, map[string][]byte{"ops": []byte(tenantYAML("ops", "pilot"))}); err != nil {
		t.Fatal(err)
	}
	paul, pete := as(t, "paul"), as(t, "pete")
	vs, err := s.ListConfigVersions(paul, &adminv1.ListConfigVersionsRequest{Object: ObjectTenant, Scope: "ops"})
	if err != nil || len(vs.GetVersions()) != 1 || !vs.GetVersions()[0].GetActive() {
		t.Fatalf("%v %v", vs, err)
	}
	base := vs.GetVersions()[0].GetVersionId()
	req := &adminv1.CreateChangeRequestRequest{Object: ObjectTenant, Scope: "ops", BaseVersionId: base,
		Content: tenantYAML("ops", "pilot", "pilot_groups: [ops-pilot]\n"), Reason: "加试点群组"}

	if _, err := s.CreateChangeRequest(as(t, "audra"), req); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能发起: %v", err)
	}
	bad := proto.CloneOf(req)
	bad.Content = "tenant: ops\nrollout: beta\n"
	if c, _ := code(func() error { _, err := s.CreateChangeRequest(paul, bad); return err }()); c != codes.InvalidArgument {
		t.Fatalf("不合法的内容应拒绝: %v", c)
	}
	stale := proto.CloneOf(req)
	stale.BaseVersionId = ""
	if _, r := code(func() error { _, err := s.CreateChangeRequest(paul, stale); return err }()); r != "stale_base_version" {
		t.Fatalf("base 不是生效版本: %s", r)
	}
	chg, err := s.CreateChangeRequest(paul, req)
	if err != nil || chg.GetChangeRequest().GetStatus() != "open" || chg.GetChangeRequest().GetVersion().GetNumber() != 2 {
		t.Fatalf("%v %v", chg, err)
	}
	id := chg.GetChangeRequest().GetChgId()
	open, err := s.ListChangeRequests(pete, &adminv1.ListChangeRequestsRequest{Status: "open"})
	if err != nil || len(open.GetChangeRequests()) != 1 || open.GetChangeRequests()[0].GetChgId() != id ||
		open.GetChangeRequests()[0].GetVersion().GetContent() == "" {
		t.Fatalf("待审批列表: %v %v", open, err)
	}
	if _, err := s.ListChangeRequests(pete, &adminv1.ListChangeRequestsRequest{Status: "done"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("不合法的状态: %v", err)
	}
	if _, err := s.ListChangeRequests(as(t, "audra"), &adminv1.ListChangeRequestsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能列: %v", err)
	}
	if _, r := code(func() error { _, err := s.CreateChangeRequest(pete, req); return err }()); r != "conflict" {
		t.Fatalf("同一对象只能有一个待审批的变更单: %s", r)
	}
	if _, r := code(func() error {
		_, err := s.ApproveChangeRequest(paul, &adminv1.ApproveChangeRequestRequest{ChgId: id})
		return err
	}()); r != "self_approval_forbidden" {
		t.Fatalf("作者不能审批: %s", r)
	}
	ok, err := s.ApproveChangeRequest(pete, &adminv1.ApproveChangeRequestRequest{ChgId: id})
	if err != nil || ok.GetChangeRequest().GetStatus() != "approved" || ok.GetChangeRequest().GetReleaseId() == "" {
		t.Fatalf("%v %v", ok, err)
	}
	tenants, _, _ := s.Snapshot(ctx)
	if !strings.Contains(tenants[0].GetContent(), "ops-pilot") {
		t.Fatalf("审批后立即生效: %v", tenants)
	}

	// 两个变更单基于同一个 base：先批的生效，后批的作废
	v2 := ok.GetChangeRequest().GetVersion().GetVersionId()
	a, err := s.CreateChangeRequest(paul, &adminv1.CreateChangeRequestRequest{Object: ObjectTenant, Scope: "ops", BaseVersionId: v2,
		Content: tenantYAML("ops", "pilot", "quota: {max_concurrency: 30}\n"), Reason: "调并发"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RejectChangeRequest(paul, &adminv1.RejectChangeRequestRequest{ChgId: a.GetChangeRequest().GetChgId(), Reason: "撤回"}); err != nil {
		t.Fatal(err)
	}
	b, _ := s.CreateChangeRequest(paul, &adminv1.CreateChangeRequestRequest{Object: ObjectTenant, Scope: "ops", BaseVersionId: v2,
		Content: tenantYAML("ops", "pilot", "quota: {max_concurrency: 40}\n"), Reason: "调并发"})
	if _, err := s.ApproveChangeRequest(pete, &adminv1.ApproveChangeRequestRequest{ChgId: b.GetChangeRequest().GetChgId()}); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(audit.actions, "config.change_request_rejected") || !slices.Contains(audit.actions, "config.release") {
		t.Fatalf("审计: %v", audit.actions)
	}
}

func TestWideningToGeneralNeedsPassedGate(t *testing.T) {
	s, _, _ := newServer()
	ctx := context.Background()
	if _, err := s.Bootstrap(ctx, map[string][]byte{"credit": []byte(tenantYAML("credit", "pilot"))}); err != nil {
		t.Fatal(err)
	}
	paul := as(t, "paul")
	vs, _ := s.ListConfigVersions(paul, &adminv1.ListConfigVersionsRequest{Object: ObjectTenant, Scope: "credit"})
	req := &adminv1.CreateChangeRequestRequest{Object: ObjectTenant, Scope: "credit", BaseVersionId: vs.GetVersions()[0].GetVersionId(),
		Content: tenantYAML("credit", "general"), Reason: "转全量"}
	if _, r := code(func() error { _, err := s.CreateChangeRequest(paul, req); return err }()); r != "gate_failed" {
		t.Fatalf("没附门禁: %s", r)
	}
	req.GateId = "gate_000000000000000000000002"
	if _, r := code(func() error { _, err := s.CreateChangeRequest(paul, req); return err }()); r != "gate_failed" {
		t.Fatalf("门禁没通过: %s", r)
	}
	req.GateId = passedGate
	if _, err := s.CreateChangeRequest(paul, req); err != nil {
		t.Fatalf("门禁通过: %v", err)
	}
	// 新租户直接全量同样要门禁
	if _, r := code(func() error {
		_, err := s.CreateChangeRequest(paul, &adminv1.CreateChangeRequestRequest{Object: ObjectTenant, Scope: "newbiz",
			Content: tenantYAML("newbiz", "general"), Reason: "新业务线"})
		return err
	}()); r != "gate_failed" {
		t.Fatalf("新租户直接全量: %s", r)
	}
}

func TestCrossTenantConflictsAreRejected(t *testing.T) {
	s, _, _ := newServer()
	ctx := context.Background()
	if _, err := s.Bootstrap(ctx, map[string][]byte{"credit": []byte(tenantYAML("credit", "pilot", "bound_clients: [biz-credit]\n"))}); err != nil {
		t.Fatal(err)
	}
	_, err := s.CreateChangeRequest(as(t, "paul"), &adminv1.CreateChangeRequestRequest{Object: ObjectTenant, Scope: "ops",
		Content: tenantYAML("ops", "pilot", "bound_clients: [biz-credit]\n"), Reason: "新租户"})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "只能绑定一个租户") {
		t.Fatalf("同一客户端绑定两个租户: %v", err)
	}
}

func TestCIReleaseApproveAndRollback(t *testing.T) {
	s, _, _ := newServer()
	ctx := context.Background()
	if _, err := s.Bootstrap(ctx, map[string][]byte{"ops": []byte(tenantYAML("ops", "pilot"))}); err != nil {
		t.Fatal(err)
	}
	ci, pete := as(t, "ci-bot"), as(t, "pete")
	if _, err := s.CreateRelease(ci, &adminv1.CreateReleaseRequest{Object: ObjectTenant, Scope: "ops", Content: tenantYAML("ops", "pilot"),
		SourceRef: "main", Reason: "x"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("source_ref 须为 git:<提交>: %v", err)
	}
	rel, err := s.CreateRelease(ci, &adminv1.CreateReleaseRequest{Object: ObjectTenant, Scope: "ops",
		Content: tenantYAML("ops", "pilot", "auditor_groups: [ops-audit]\n"), SourceRef: "git:3f2a9c1", Reason: "加审计岗"})
	if err != nil || rel.GetRelease().GetStatus() != "pending_approval" {
		t.Fatalf("%v %v", rel, err)
	}
	id := rel.GetRelease().GetReleaseId()
	pending, err := s.ListReleases(pete, &adminv1.ListReleasesRequest{Status: "pending_approval"})
	if err != nil || len(pending.GetReleases()) != 1 || pending.GetReleases()[0].GetReleaseId() != id {
		t.Fatalf("待审批的发布: %v %v", pending, err)
	}
	if _, r := code(func() error {
		_, err := s.ApproveRelease(ci, &adminv1.ApproveReleaseRequest{ReleaseId: id})
		return err
	}()); r != "self_approval_forbidden" {
		t.Fatalf("发起人不能审批: %s", r)
	}
	done, err := s.ApproveRelease(pete, &adminv1.ApproveReleaseRequest{ReleaseId: id})
	if err != nil || done.GetRelease().GetStatus() != "active" || done.GetRelease().GetFromVersionId() == "" {
		t.Fatalf("%v %v", done, err)
	}
	tenants, _, _ := s.Snapshot(ctx)
	if !strings.Contains(tenants[0].GetContent(), "ops-audit") {
		t.Fatal("审批后生效")
	}
	back, err := s.RollbackRelease(pete, &adminv1.RollbackReleaseRequest{ReleaseId: id, Reason: "审计岗配错了"})
	if err != nil || back.GetRelease().GetSource() != "rollback" || back.GetRelease().GetVersion().GetVersionId() != done.GetRelease().GetFromVersionId() {
		t.Fatalf("%v %v", back, err)
	}
	tenants, _, _ = s.Snapshot(ctx)
	if strings.Contains(tenants[0].GetContent(), "ops-audit") {
		t.Fatal("回退后应切回原版本")
	}
	got, _ := s.GetRelease(pete, &adminv1.GetReleaseRequest{ReleaseId: id})
	if got.GetRelease().GetStatus() != "rolled_back" || got.GetRelease().GetRolledBackBy() != "pete" {
		t.Fatalf("%v", got)
	}
	// 回退过的不能再回退；初始导入的没有可回退的版本
	if _, r := code(func() error {
		_, err := s.RollbackRelease(pete, &adminv1.RollbackReleaseRequest{ReleaseId: id, Reason: "再退"})
		return err
	}()); r != "conflict" {
		t.Fatalf("%s", r)
	}
	// 租户负责人只看自己负责的租户
	owner := as(t, "tina")
	if l, err := s.ListTenants(owner, &adminv1.ListTenantsRequest{}); err != nil || len(l.GetTenants()) != 1 {
		t.Fatalf("负责人看本租户: %v %v", l, err)
	}
	if _, err := s.ListConfigVersions(owner, &adminv1.ListConfigVersionsRequest{Object: ObjectTenant, Scope: "ops"}); err != nil {
		t.Fatal(err)
	}
	other := as(t, "tom")
	if l, _ := s.ListTenants(other, &adminv1.ListTenantsRequest{}); len(l.GetTenants()) != 0 {
		t.Fatalf("别的租户的负责人看不到: %v", l)
	}
	if _, err := s.ListConfigVersions(other, &adminv1.ListConfigVersionsRequest{Object: ObjectTenant, Scope: "ops"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("别的租户的版本: %v", err)
	}
	if _, err := s.CreateChangeRequest(owner, &adminv1.CreateChangeRequestRequest{Object: ObjectTenant, Scope: "ops"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("负责人不能提交变更: %v", err)
	}
	list, _ := s.ListTenants(pete, &adminv1.ListTenantsRequest{})
	if len(list.GetTenants()) != 1 || list.GetTenants()[0].GetRollout() != "pilot" || list.GetTenants()[0].GetActive().GetNumber() != 1 {
		t.Fatalf("%v", list)
	}
	all, _ := s.ListReleases(pete, &adminv1.ListReleasesRequest{})
	if len(all.GetReleases()) != 3 {
		t.Fatalf("初始导入、CI 发布、回退共 3 个发布: %v", all)
	}
	if _, err := s.GetRelease(pete, &adminv1.GetReleaseRequest{ReleaseId: "rel_000000000000000000000000"}); status.Code(err) != codes.NotFound {
		t.Fatalf("不存在的发布: %v", err)
	}
}
