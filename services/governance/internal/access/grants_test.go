package access

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

func (m *memSources) ApplyGrant(_ context.Context, a Application, staleBefore time.Time, beforeCommit func() error) (Application, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, x := range m.apps {
		if x.SourceID == a.SourceID && x.Tenant == a.Tenant && x.Status == "pending" {
			if !x.AppliedAt.Before(staleBefore) {
				return Application{}, ErrExists
			}
			x.Status = "expired"
			m.apps[id] = x
		}
	}
	if err := beforeCommit(); err != nil {
		return Application{}, err
	}
	a.AppliedAt = time.Now()
	m.apps[a.ID] = a
	return a, nil
}

func (m *memSources) Applications(_ context.Context, tenants []string, dept string, limit int) ([]Application, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Application
	for _, a := range m.apps {
		if slices.Contains(tenants, a.Tenant) || (dept != "" && m.sources[a.SourceID].OwnerDept == dept) {
			out = append(out, a)
		}
	}
	slices.SortFunc(out, func(a, b Application) int { return b.AppliedAt.Compare(a.AppliedAt) })
	return out[:min(len(out), limit)], nil
}

func (m *memSources) DecideApplication(_ context.Context, id string, change func(a *Application, src Source) (*Grant, error)) (Application, Grant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.apps[id]
	if !ok {
		return Application{}, Grant{}, ErrNotFound
	}
	g, err := change(&a, m.sources[a.SourceID])
	if err != nil {
		return Application{}, Grant{}, err
	}
	var out Grant
	if g != nil {
		out = *g
		if old, ok := m.grants[g.SourceID+"/"+g.Tenant]; ok {
			out = old
		} else {
			out.GrantedAt = time.Now()
			m.grants[g.SourceID+"/"+g.Tenant] = out
		}
	}
	m.apps[id] = a
	return a, out, nil
}

var (
	// 信贷租户的负责人
	olive = &identityv1.Principal{Sub: "olive", Roles: []string{"tenant_owner:credit"}}
	// 信贷部的另一位属主，同时负责信贷租户
	ownerAndTenant = &identityv1.Principal{Sub: "otto", Dept: "信贷部", Roles: []string{RoleDataOwner, "tenant_owner:credit"}}
)

func activeSource(t *testing.T, s *SourceServer) string {
	t.Helper()
	id := register(t, s, paul).GetSourceId()
	if _, err := s.ApproveSource(as(t, creditOwn), &adminv1.ApproveSourceRequest{SourceId: id, Basis: "工单 DATA-7"}); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestGrantApplicationLifecycle(t *testing.T) {
	s, ms, a, _, _ := newSourceServer()
	id := activeSource(t, s)
	apply := func(p *identityv1.Principal, tenant string) (*adminv1.ApplyGrantResponse, error) {
		return s.ApplyGrant(as(t, p), &adminv1.ApplyGrantRequest{SourceId: id, Tenant: tenant, Reason: "信贷问答需要制度库"})
	}
	if _, err := apply(olive, "risk"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("只能为自己负责的租户申请: %v", err)
	}
	if _, err := s.ApplyGrant(as(t, olive), &adminv1.ApplyGrantRequest{SourceId: id, Tenant: "credit"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("缺理由: %v", err)
	}
	r, err := apply(olive, "credit")
	if err != nil || !strings.HasPrefix(r.GetApplication().GetApplicationId(), "gra_") ||
		r.GetApplication().GetStatus() != adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_PENDING {
		t.Fatalf("申请: %v %v", r, err)
	}
	app := r.GetApplication().GetApplicationId()
	if _, err := apply(olive, "credit"); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("已有待审批的申请: %v", err)
	}
	// 租户负责人能看到已生效的数据源与自己的申请；别的部门的属主看不到这条申请
	ms.sources["src_pending"] = Source{ID: "src_pending", OwnerDept: "信贷部", Status: "pending"}
	l, err := s.ListSources(as(t, olive), &adminv1.ListSourcesRequest{})
	if err != nil || !slices.ContainsFunc(l.GetSources(), func(x *adminv1.Source) bool { return x.GetSourceId() == id }) ||
		slices.ContainsFunc(l.GetSources(), func(x *adminv1.Source) bool { return x.GetStatus() != adminv1.SourceStatus_SOURCE_STATUS_ACTIVE }) {
		t.Fatalf("租户负责人只列已生效的数据源: %v %v", l, err)
	}
	if l, _ := s.ListGrantApplications(as(t, olive), &adminv1.ListGrantApplicationsRequest{}); len(l.GetApplications()) != 1 {
		t.Fatalf("申请人看自己的申请: %v", l)
	}
	if l, _ := s.ListGrantApplications(as(t, riskOwn), &adminv1.ListGrantApplicationsRequest{}); len(l.GetApplications()) != 0 {
		t.Fatalf("别的部门的属主不该看到: %v", l)
	}
	if _, err := s.ApproveGrantApplication(as(t, riskOwn), &adminv1.ApproveGrantApplicationRequest{ApplicationId: app, Basis: "x"}); status.Code(err) != codes.NotFound {
		t.Fatalf("别的部门的属主审批: %v", err)
	}
	if _, err := s.ApproveGrantApplication(as(t, olive), &adminv1.ApproveGrantApplicationRequest{ApplicationId: app, Basis: "x"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("租户负责人不能审批: %v", err)
	}
	ok, err := s.ApproveGrantApplication(as(t, creditOwn), &adminv1.ApproveGrantApplicationRequest{ApplicationId: app, Basis: "工单 GRANT-3"})
	if err != nil || ok.GetApplication().GetStatus() != adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_APPROVED ||
		ok.GetApplication().GetDecidedBy() != "dana" || ok.GetApplication().GetDecisionNote() != "工单 GRANT-3" ||
		ok.GetGrant().GetTenant() != "credit" || ok.GetGrant().GetApplicationId() != app {
		t.Fatalf("审批: %v %v", ok, err)
	}
	if snap, _ := s.Snapshot(context.Background()); len(snap.GetSourceGrants()) != 1 {
		t.Fatalf("审批后应下发授权: %v", snap)
	}
	if _, err := s.ApproveGrantApplication(as(t, creditOwn), &adminv1.ApproveGrantApplicationRequest{ApplicationId: app, Basis: "再批"}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("重复审批: %v", err)
	}
	if _, err := apply(olive, "credit"); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("已授权的不能再申请: %v", err)
	}
	if ms.apps[app].Status != "approved" {
		t.Fatalf("申请状态: %+v", ms.apps[app])
	}
	var got []string
	for _, x := range a.actions {
		got = append(got, strings.Split(x, ":")[0])
	}
	if !slices.Contains(got, "source_grant.apply") || !slices.Contains(got, "source_grant.application_approved") {
		t.Fatalf("变更审计: %v", got)
	}
}

func TestGrantApplicationSelfApprovalRejectAndExpiry(t *testing.T) {
	s, ms, _, _, _ := newSourceServer()
	id := activeSource(t, s)
	r, err := s.ApplyGrant(as(t, ownerAndTenant), &adminv1.ApplyGrantRequest{SourceId: id, Tenant: "credit", Reason: "自用"})
	if err != nil {
		t.Fatal(err)
	}
	app := r.GetApplication().GetApplicationId()
	// 既是属主又是租户负责人：自己的申请要另一位属主批
	if _, err := s.ApproveGrantApplication(as(t, ownerAndTenant), &adminv1.ApproveGrantApplicationRequest{ApplicationId: app, Basis: "x"}); grpcmw.ReasonOf(err) != "self_approval_forbidden" {
		t.Fatalf("自批: %v", err)
	}
	// 申请人撤回
	w, err := s.RejectGrantApplication(as(t, ownerAndTenant), &adminv1.RejectGrantApplicationRequest{ApplicationId: app, Reason: "不需要了"})
	if err != nil || w.GetApplication().GetStatus() != adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_WITHDRAWN {
		t.Fatalf("撤回: %v %v", w, err)
	}

	// 属主驳回；无关的人按不存在处理
	r, _ = s.ApplyGrant(as(t, olive), &adminv1.ApplyGrantRequest{SourceId: id, Tenant: "credit", Reason: "再申请"})
	app = r.GetApplication().GetApplicationId()
	if _, err := s.RejectGrantApplication(as(t, riskOwn), &adminv1.RejectGrantApplicationRequest{ApplicationId: app, Reason: "x"}); status.Code(err) != codes.NotFound {
		t.Fatalf("别的部门驳回: %v", err)
	}
	rj, err := s.RejectGrantApplication(as(t, creditOwn), &adminv1.RejectGrantApplicationRequest{ApplicationId: app, Reason: "用途不清楚"})
	if err != nil || rj.GetApplication().GetStatus() != adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_REJECTED {
		t.Fatalf("驳回: %v %v", rj, err)
	}

	// 7 天未审批：读出为过期，不能审批，可以重新申请
	r, _ = s.ApplyGrant(as(t, olive), &adminv1.ApplyGrantRequest{SourceId: id, Tenant: "credit", Reason: "第三次"})
	app = r.GetApplication().GetApplicationId()
	old := ms.apps[app]
	old.AppliedAt = time.Now().Add(-8 * 24 * time.Hour)
	ms.apps[app] = old
	l, _ := s.ListGrantApplications(as(t, olive), &adminv1.ListGrantApplicationsRequest{Status: adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_EXPIRED})
	if len(l.GetApplications()) != 1 || l.GetApplications()[0].GetApplicationId() != app {
		t.Fatalf("过期的申请: %v", l)
	}
	if _, err := s.ApproveGrantApplication(as(t, creditOwn), &adminv1.ApproveGrantApplicationRequest{ApplicationId: app, Basis: "x"}); grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("过期的不能审批: %v", err)
	}
	if _, err := s.ApplyGrant(as(t, olive), &adminv1.ApplyGrantRequest{SourceId: id, Tenant: "credit", Reason: "第四次"}); err != nil {
		t.Fatalf("过期后可以重新申请: %v", err)
	}
	if ms.apps[app].Status != "expired" {
		t.Fatalf("重新申请时旧的记为过期: %+v", ms.apps[app])
	}
}
