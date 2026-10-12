package access

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

// memSources 在 memStore（紧急拒绝）之外加上数据源与授权。
type memSources struct {
	*memStore
	mu     sync.Mutex
	grants map[string]Grant
	apps   map[string]Application
}

func (m *memSources) RegisterSource(_ context.Context, src Source, beforeCommit func() error) (Source, error) {
	if err := beforeCommit(); err != nil {
		return Source{}, err
	}
	m.memStore.mu.Lock()
	defer m.memStore.mu.Unlock()
	src.CreatedAt = time.Now()
	m.sources[src.ID] = src
	return src, nil
}

func (m *memSources) UpdateSource(_ context.Context, id string, change func(tx pgx.Tx, src *Source) error) (Source, error) {
	m.memStore.mu.Lock()
	src, ok := m.sources[id]
	m.memStore.mu.Unlock()
	if !ok {
		return Source{}, ErrNotFound
	}
	if err := change(nil, &src); err != nil {
		return Source{}, err
	}
	m.memStore.mu.Lock()
	defer m.memStore.mu.Unlock()
	m.sources[id] = src
	return src, nil
}

func (m *memSources) Sources(_ context.Context, dept, st string) ([]Source, error) {
	m.memStore.mu.Lock()
	defer m.memStore.mu.Unlock()
	var out []Source
	for _, x := range m.sources {
		if (dept == "" || x.OwnerDept == dept) && (st == "" || x.Status == st) {
			out = append(out, x)
		}
	}
	slices.SortFunc(out, func(a, b Source) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func (m *memSources) Grants(_ context.Context, sourceID string) ([]Grant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Grant
	for _, g := range m.grants {
		if g.SourceID == sourceID {
			out = append(out, g)
		}
	}
	return out, nil
}

func (m *memSources) CreateGrant(_ context.Context, g Grant, beforeCommit func() error) (Grant, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.grants[g.SourceID+"/"+g.Tenant]; ok {
		return old, false, nil
	}
	if err := beforeCommit(); err != nil {
		return Grant{}, false, err
	}
	g.GrantedAt = time.Now()
	m.grants[g.SourceID+"/"+g.Tenant] = g
	return g, true, nil
}

func (m *memSources) DeleteGrant(_ context.Context, sourceID, tenant string, beforeCommit func() error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.grants[sourceID+"/"+tenant]; !ok {
		return ErrNotFound
	}
	if err := beforeCommit(); err != nil {
		return err
	}
	delete(m.grants, sourceID+"/"+tenant)
	return nil
}

func (m *memSources) ActiveGrants(context.Context) ([]Grant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Grant
	for _, g := range m.grants {
		if m.sources[g.SourceID].Status == "active" {
			out = append(out, g)
		}
	}
	return out, nil
}

type events struct{ ids []string }

func (e *events) write(_ context.Context, _ pgx.Tx, id string, _ []byte) error {
	e.ids = append(e.ids, id)
	return nil
}

var (
	paul     = &identityv1.Principal{Sub: "paul", Dept: "平台部", Roles: []string{RolePlatformAdmin}}
	riskOwn  = &identityv1.Principal{Sub: "rick", Dept: "风险管理部", Roles: []string{RoleDataOwner}}
	selfOwns = &identityv1.Principal{Sub: "dana", Dept: "信贷部", Roles: []string{RoleDataOwner, RolePlatformAdmin}}
)

func newSourceServer() (*SourceServer, *memSources, *memAudit, *fakeIndex, *events) {
	ds, st, a, idx := newDenialServer()
	ms := &memSources{memStore: st, grants: map[string]Grant{}, apps: map[string]Application{}}
	ds.Store = ms
	ev := &events{}
	return &SourceServer{Store: ms, Audit: a, Denials: ds, WriteEvent: ev.write, Log: quiet, Now: time.Now}, ms, a, idx, ev
}

func register(t *testing.T, s *SourceServer, p *identityv1.Principal) *adminv1.Source {
	t.Helper()
	r, err := s.RegisterSource(as(t, p), &adminv1.RegisterSourceRequest{Type: commonv1.SourceType_SOURCE_TYPE_WIKI, OwnerDept: "信贷部",
		DefaultClassification: commonv1.Classification_CLASSIFICATION_INTERNAL, Description: "信贷制度库"})
	if err != nil {
		t.Fatal(err)
	}
	return r.GetSource()
}

func TestSourceLifecycle(t *testing.T) {
	s, ms, a, idx, ev := newSourceServer()
	src := register(t, s, paul)
	if !strings.HasPrefix(src.GetSourceId(), "src_") || len(src.GetSourceId()) != 20 || src.GetStatus() != adminv1.SourceStatus_SOURCE_STATUS_PENDING ||
		src.GetCreatedBy() != "paul" {
		t.Fatalf("注册: %v", src)
	}
	id := src.GetSourceId()
	// 待审批的数据源不能授权
	if _, err := s.CreateGrant(as(t, creditOwn), &adminv1.CreateGrantRequest{SourceId: id, Tenant: "credit"}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("待审批不能授权: %v", err)
	}
	// 别的部门的属主看不到它（404），缺依据 400
	if _, err := s.ApproveSource(as(t, riskOwn), &adminv1.ApproveSourceRequest{SourceId: id, Basis: "x"}); status.Code(err) != codes.NotFound {
		t.Fatalf("别的部门: %v", err)
	}
	if _, err := s.ApproveSource(as(t, creditOwn), &adminv1.ApproveSourceRequest{SourceId: id}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("缺依据: %v", err)
	}
	ok, err := s.ApproveSource(as(t, creditOwn), &adminv1.ApproveSourceRequest{SourceId: id, Basis: "工单 DATA-7"})
	if err != nil || ok.GetSource().GetStatus() != adminv1.SourceStatus_SOURCE_STATUS_ACTIVE || ok.GetSource().GetApprovedBy() != "dana" ||
		ok.GetSource().GetApprovalBasis() != "工单 DATA-7" || ok.GetSource().GetApprovedAt() == nil {
		t.Fatalf("审批: %v %v", ok, err)
	}
	if _, err := s.ApproveSource(as(t, creditOwn), &adminv1.ApproveSourceRequest{SourceId: id, Basis: "再批"}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("重复审批: %v", err)
	}

	g, err := s.CreateGrant(as(t, creditOwn), &adminv1.CreateGrantRequest{SourceId: id, Tenant: "credit"})
	if err != nil || !g.GetCreated() || g.GetGrant().GetGrantedBy() != "dana" {
		t.Fatalf("授权: %v %v", g, err)
	}
	if again, _ := s.CreateGrant(as(t, creditOwn), &adminv1.CreateGrantRequest{SourceId: id, Tenant: "credit"}); again.GetCreated() {
		t.Fatal("重复授权应返回已有的")
	}
	snap, _ := s.Snapshot(context.Background())
	if len(snap.GetSourceGrants()) != 1 || snap.GetSourceGrants()[0].GetTenant() != "credit" || !strings.HasPrefix(snap.GetSnapshotVersion(), "grants:") {
		t.Fatalf("快照: %v", snap)
	}
	if _, err := s.DeleteGrant(as(t, creditOwn), &adminv1.DeleteGrantRequest{SourceId: id, Tenant: "credit"}); err != nil {
		t.Fatal(err)
	}
	if snap2, _ := s.Snapshot(context.Background()); len(snap2.GetSourceGrants()) != 0 || snap2.GetSnapshotVersion() == snap.GetSnapshotVersion() {
		t.Fatalf("取消授权后快照应变化: %v", snap2)
	}
	if _, err := s.DeleteGrant(as(t, creditOwn), &adminv1.DeleteGrantRequest{SourceId: id, Tenant: "credit"}); status.Code(err) != codes.NotFound {
		t.Fatalf("取消不存在的授权: %v", err)
	}
	if _, err := s.CreateGrant(as(t, creditOwn), &adminv1.CreateGrantRequest{SourceId: id, Tenant: "risk"}); err != nil {
		t.Fatal(err)
	}

	// 撤销：先写按数据源的紧急拒绝，再标为撤销、发事件；撤销后不再下发授权
	rv, err := s.RevokeSource(as(t, creditOwn), &adminv1.RevokeSourceRequest{SourceId: id, Reason: "源系统下线"})
	if err != nil || rv.GetSource().GetStatus() != adminv1.SourceStatus_SOURCE_STATUS_REVOKED || !strings.HasPrefix(rv.GetDenialId(), "den_") {
		t.Fatalf("撤销: %v %v", rv, err)
	}
	if idx.count() != 1 || idx.reqs[0].GetSourceIds()[0] != id || len(ev.ids) != 1 {
		t.Fatalf("撤销应写拒绝名单并发事件: %v %v", idx.reqs, ev.ids)
	}
	if snap3, _ := s.Snapshot(context.Background()); len(snap3.GetSourceGrants()) != 0 {
		t.Fatalf("撤销的数据源不应再下发授权: %v", snap3)
	}
	if _, err := s.RevokeSource(as(t, creditOwn), &adminv1.RevokeSourceRequest{SourceId: id, Reason: "再撤"}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("重复撤销: %v", err)
	}
	if ms.sources[id].RevokedBy != "dana" {
		t.Fatalf("撤销记录: %+v", ms.sources[id])
	}
	want := []string{"source.register", "source.approve", "source_grant.create", "source_grant.delete", "source_grant.create",
		"emergency_denial.create", "source.revoke"}
	var got []string
	for _, x := range a.actions {
		got = append(got, strings.Split(x, ":")[0])
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("变更审计: %v", got)
	}
}

func TestSourceRolesAndSelfApproval(t *testing.T) {
	s, _, _, _, _ := newSourceServer()
	if _, err := s.RegisterSource(as(t, creditOwn), &adminv1.RegisterSourceRequest{Type: commonv1.SourceType_SOURCE_TYPE_WIKI, OwnerDept: "信贷部",
		DefaultClassification: commonv1.Classification_CLASSIFICATION_INTERNAL}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("只有平台管理员能注册: %v", err)
	}
	for name, r := range map[string]*adminv1.RegisterSourceRequest{
		"缺密级": {Type: commonv1.SourceType_SOURCE_TYPE_WIKI, OwnerDept: "信贷部"},
		"缺类型": {OwnerDept: "信贷部", DefaultClassification: commonv1.Classification_CLASSIFICATION_INTERNAL},
		"缺部门": {Type: commonv1.SourceType_SOURCE_TYPE_WIKI, DefaultClassification: commonv1.Classification_CLASSIFICATION_INTERNAL},
	} {
		if _, err := s.RegisterSource(as(t, paul), r); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}
	// 注册人同时是该部门的数据属主：不能自批
	src := register(t, s, selfOwns)
	_, err := s.ApproveSource(as(t, selfOwns), &adminv1.ApproveSourceRequest{SourceId: src.GetSourceId(), Basis: "x"})
	if grpcmw.ReasonOf(err) != "self_approval_forbidden" {
		t.Fatalf("注册人不能自批: %v", err)
	}
	// 平台管理员不能审批、撤销、授权
	if _, err := s.ApproveSource(as(t, paul), &adminv1.ApproveSourceRequest{SourceId: src.GetSourceId(), Basis: "x"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("平台管理员不能审批: %v", err)
	}
	if _, err := s.RevokeSource(as(t, riskOwn), &adminv1.RevokeSourceRequest{SourceId: src.GetSourceId(), Reason: "x"}); status.Code(err) != codes.NotFound {
		t.Fatalf("别的部门不能撤销: %v", err)
	}
}

// ownerSO 同时是信贷部的数据属主和信息安全
var ownerSO = &identityv1.Principal{Sub: "sol", Dept: "信贷部", Roles: []string{RoleDataOwner, RoleInfoSec}}

func TestRestrictedSourceNeedsOwnerAndInfoSec(t *testing.T) {
	s, _, a, _, _ := newSourceServer()
	r, err := s.RegisterSource(as(t, paul), &adminv1.RegisterSourceRequest{Type: commonv1.SourceType_SOURCE_TYPE_DB, OwnerDept: "信贷部",
		DefaultClassification: commonv1.Classification_CLASSIFICATION_RESTRICTED, Description: "客户征信库"})
	if err != nil {
		t.Fatal(err)
	}
	id := r.GetSource().GetSourceId()
	asSec := adminv1.ApprovalCapacity_APPROVAL_CAPACITY_INFOSEC
	// 属主不能以信息安全的身份审批
	if _, err := s.ApproveSource(as(t, creditOwn), &adminv1.ApproveSourceRequest{SourceId: id, Basis: "x", Capacity: asSec}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("需要信息安全: %v", err)
	}
	// 属主先批：还待信息安全审批
	ok, err := s.ApproveSource(as(t, creditOwn), &adminv1.ApproveSourceRequest{SourceId: id, Basis: "工单 DATA-9"})
	if err != nil || ok.GetSource().GetStatus() != adminv1.SourceStatus_SOURCE_STATUS_PENDING || ok.GetSource().GetApprovedBy() != "dana" {
		t.Fatalf("属主审批后仍待审批: %v %v", ok, err)
	}
	if _, err := s.CreateGrant(as(t, creditOwn), &adminv1.CreateGrantRequest{SourceId: id, Tenant: "credit"}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("信息安全批之前不能授权: %v", err)
	}
	// 属主不能再批一次；同时有两个角色的人不能把两项都批了
	if _, err := s.ApproveSource(as(t, creditOwn), &adminv1.ApproveSourceRequest{SourceId: id, Basis: "x"}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("重复审批: %v", err)
	}
	r2, _ := s.RegisterSource(as(t, paul), &adminv1.RegisterSourceRequest{Type: commonv1.SourceType_SOURCE_TYPE_DB, OwnerDept: "信贷部",
		DefaultClassification: commonv1.Classification_CLASSIFICATION_RESTRICTED})
	if _, err := s.ApproveSource(as(t, ownerSO), &adminv1.ApproveSourceRequest{SourceId: r2.GetSource().GetSourceId(), Basis: "x"}); err != nil {
		t.Fatal(err)
	}
	_, err = s.ApproveSource(as(t, ownerSO), &adminv1.ApproveSourceRequest{SourceId: r2.GetSource().GetSourceId(), Basis: "x", Capacity: asSec})
	if grpcmw.ReasonOf(err) != "self_approval_forbidden" {
		t.Fatalf("两项审批不能是同一人: %v", err)
	}
	// 信息安全审批任何部门的受限级数据源，批完生效
	done, err := s.ApproveSource(as(t, infosec), &adminv1.ApproveSourceRequest{SourceId: id, Basis: "安全评审 SEC-3", Capacity: asSec})
	if err != nil || done.GetSource().GetStatus() != adminv1.SourceStatus_SOURCE_STATUS_ACTIVE || done.GetSource().GetSecurityApprovedBy() != "ivan" ||
		done.GetSource().GetSecurityApprovalBasis() != "安全评审 SEC-3" || done.GetSource().GetSecurityApprovedAt() == nil {
		t.Fatalf("信息安全审批后生效: %v %v", done, err)
	}
	if !slices.Contains(a.actions, "source.security_approve:"+id) {
		t.Fatalf("变更审计: %v", a.actions)
	}
	// 非受限级不需要信息安全审批；不存在的数据源 404
	plain := register(t, s, paul)
	if _, err := s.ApproveSource(as(t, infosec), &adminv1.ApproveSourceRequest{SourceId: plain.GetSourceId(), Basis: "x", Capacity: asSec}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("非受限级: %v", err)
	}
	if _, err := s.ApproveSource(as(t, infosec), &adminv1.ApproveSourceRequest{SourceId: "src_nope", Basis: "x", Capacity: asSec}); status.Code(err) != codes.NotFound {
		t.Fatalf("不存在: %v", err)
	}
}

func TestListSourcesScope(t *testing.T) {
	s, ms, _, _, _ := newSourceServer()
	register(t, s, paul)
	ms.sources["src_riskdb"] = Source{ID: "src_riskdb", OwnerDept: "风险管理部", Status: "active", Type: "db"}
	for _, p := range []*identityv1.Principal{paul, infosec} {
		all, err := s.ListSources(as(t, p), &adminv1.ListSourcesRequest{})
		if err != nil || len(all.GetSources()) != 4 {
			t.Fatalf("%s 看全部: %v %v", p.GetSub(), all, err)
		}
	}
	mine, err := s.ListSources(as(t, riskOwn), &adminv1.ListSourcesRequest{})
	if err != nil || len(mine.GetSources()) != 2 || mine.GetSources()[1].GetSourceId() != "src_riskdb" {
		t.Fatalf("属主只看本部门: %v %v", mine, err)
	}
	pending, _ := s.ListSources(as(t, paul), &adminv1.ListSourcesRequest{Status: adminv1.SourceStatus_SOURCE_STATUS_PENDING})
	if len(pending.GetSources()) != 1 {
		t.Fatalf("按状态: %v", pending)
	}
	audra := &identityv1.Principal{Sub: "audra", Roles: []string{RoleAuditor}}
	if _, err := s.ListSources(as(t, audra), &adminv1.ListSourcesRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗: %v", err)
	}
}

func TestPushClientIsApprovedWithSource(t *testing.T) {
	s, _, _, _, _ := newSourceServer()
	if _, err := s.RegisterSource(as(t, paul), &adminv1.RegisterSourceRequest{Type: commonv1.SourceType_SOURCE_TYPE_FILE, OwnerDept: "信贷部",
		DefaultClassification: commonv1.Classification_CLASSIFICATION_INTERNAL, PushClient: "CRM Sync"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("推送客户端格式不对: %v", err)
	}
	r, err := s.RegisterSource(as(t, paul), &adminv1.RegisterSourceRequest{Type: commonv1.SourceType_SOURCE_TYPE_FILE, OwnerDept: "信贷部",
		DefaultClassification: commonv1.Classification_CLASSIFICATION_CONFIDENTIAL, PushClient: "crm-sync"})
	if err != nil || r.GetSource().GetPushClient() != "crm-sync" {
		t.Fatalf("%v %v", r, err)
	}
	if r.GetSource().GetPermissionModel() != adminv1.PermissionModel_PERMISSION_MODEL_DOCUMENT {
		t.Fatalf("未声明按文档级: %v", r.GetSource())
	}
	id := r.GetSource().GetSourceId()
	// 审批之前不下发：推送客户端随数据源一起审批
	push, digest, err := s.PushSources(context.Background())
	if err != nil || len(push) != 0 {
		t.Fatalf("待审批的不下发: %v %v", push, err)
	}
	if _, err := s.ApproveSource(as(t, creditOwn), &adminv1.ApproveSourceRequest{SourceId: id, Basis: "工单 DATA-9"}); err != nil {
		t.Fatal(err)
	}
	push, digest2, _ := s.PushSources(context.Background())
	if len(push) != 1 || push[0].GetSourceId() != id || push[0].GetPushClient() != "crm-sync" ||
		push[0].GetDefaultClassification() != commonv1.Classification_CLASSIFICATION_CONFIDENTIAL || digest2 == digest {
		t.Fatalf("审批后下发: %v %s", push, digest2)
	}
	if _, err := s.RevokeSource(as(t, creditOwn), &adminv1.RevokeSourceRequest{SourceId: id, Reason: "停止推送"}); err != nil {
		t.Fatal(err)
	}
	if push, _, _ := s.PushSources(context.Background()); len(push) != 0 {
		t.Fatalf("撤销后不再接受推送: %v", push)
	}
}

func TestPartitionPermissionModelIsRecorded(t *testing.T) {
	s, _, _, _, _ := newSourceServer()
	r, err := s.RegisterSource(as(t, paul), &adminv1.RegisterSourceRequest{Type: commonv1.SourceType_SOURCE_TYPE_WIKI, OwnerDept: "信贷部",
		DefaultClassification: commonv1.Classification_CLASSIFICATION_INTERNAL, PermissionModel: adminv1.PermissionModel_PERMISSION_MODEL_PARTITION})
	if err != nil || r.GetSource().GetPermissionModel() != adminv1.PermissionModel_PERMISSION_MODEL_PARTITION {
		t.Fatalf("%v %v", r, err)
	}
	if _, err := s.RegisterSource(as(t, paul), &adminv1.RegisterSourceRequest{Type: commonv1.SourceType_SOURCE_TYPE_WIKI, OwnerDept: "信贷部",
		DefaultClassification: commonv1.Classification_CLASSIFICATION_INTERNAL, PermissionModel: 9}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("未知的权限模型: %v", err)
	}
}
