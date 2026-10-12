package access

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

// 授权申请（governance 详设 6.2、9.1）：租户负责人为自己负责的租户申请数据源，数据源所属部门的数据属主审批，
// 审批通过的同一事务里写入授权。申请人不能审批自己的申请（同一人既是租户负责人又是该部门属主时，要另一位属主批）。
// 数据属主仍可以直接授权（CreateGrant）：审批人本来就是属主，申请只是让租户一方有正式的发起入口与记录。

// ownerRolePrefix：租户负责人的角色是 tenant_owner:<租户>（gateway 按租户配置的 owner_groups 给出）
const ownerRolePrefix = "tenant_owner:"

// applicationTimeout：待审批超过它的申请按过期处理（与配置变更单相同，详设 4.1）
const applicationTimeout = 7 * 24 * time.Hour

const maxApplications = 200

var applicationPattern = regexp.MustCompile(`^gra_[0-9a-f]{24}$`)

// Application 是一条授权申请。Status 为 pending、approved、rejected、withdrawn、expired。
type Application struct {
	ID, SourceID, Tenant, Reason, Status, AppliedBy string
	AppliedAt                                       time.Time
	DecidedBy, Note                                 string
	DecidedAt                                       *time.Time
}

// effective 是读出时的状态：待审批超过 applicationTimeout 的算过期。
func (a Application) effective(now time.Time) string {
	if a.Status == "pending" && now.Sub(a.AppliedAt) > applicationTimeout {
		return "expired"
	}
	return a.Status
}

var applicationStatus = map[string]adminv1.GrantApplicationStatus{
	"pending": adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_PENDING, "approved": adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_APPROVED,
	"rejected": adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_REJECTED, "withdrawn": adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_WITHDRAWN,
	"expired": adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_EXPIRED,
}

func (a Application) proto(now time.Time) *adminv1.GrantApplication {
	return &adminv1.GrantApplication{ApplicationId: a.ID, SourceId: a.SourceID, Tenant: a.Tenant, Reason: a.Reason,
		Status: applicationStatus[a.effective(now)], AppliedBy: a.AppliedBy, AppliedAt: timestamppb.New(a.AppliedAt),
		DecidedBy: a.DecidedBy, DecidedAt: ts(a.DecidedAt), DecisionNote: a.Note}
}

// ownedTenants 是调用者负责的租户。
func ownedTenants(ctx context.Context) []string {
	var out []string
	for _, r := range grpcmw.PrincipalFrom(ctx).GetRoles() {
		if t, ok := strings.CutPrefix(r, ownerRolePrefix); ok {
			out = append(out, t)
		}
	}
	return out
}

func (s *SourceServer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// ApplyGrant 租户负责人提交申请。数据源须已生效、该租户还没有授权；同一数据源与租户已有待审批的申请时报冲突。
func (s *SourceServer) ApplyGrant(ctx context.Context, r *adminv1.ApplyGrantRequest) (*adminv1.ApplyGrantResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !slices.Contains(ownedTenants(ctx), r.GetTenant()) {
		return nil, status.Error(codes.PermissionDenied, "只能为自己负责的租户申请授权")
	}
	if !short(r.GetReason(), 500) {
		return nil, status.Error(codes.InvalidArgument, "reason 必填，最多 500 字")
	}
	if !sourceIDPattern.MatchString(r.GetSourceId()) {
		return nil, status.Error(codes.NotFound, "数据源不存在")
	}
	src, err := s.Store.Source(ctx, r.GetSourceId())
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	if src.Status != "active" {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "只能申请已审批、未撤销的数据源")
	}
	gs, err := s.Store.Grants(ctx, src.ID)
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	if slices.ContainsFunc(gs, func(g Grant) bool { return g.Tenant == r.GetTenant() }) {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "该租户已经有这个数据源的授权")
	}
	a := Application{ID: newID("gra_"), SourceID: src.ID, Tenant: r.GetTenant(), Reason: strings.TrimSpace(r.GetReason()),
		Status: "pending", AppliedBy: p.GetSub()}
	a, err = s.Store.ApplyGrant(ctx, a, s.now().Add(-applicationTimeout), func() error {
		return s.Audit.Write(ctx, "source_grant.apply", src.ID, map[string]string{"application_id": a.ID, "tenant": a.Tenant, "reason": a.Reason})
	})
	if errors.Is(err, ErrExists) {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "这个数据源与租户已有待审批的申请")
	}
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	return &adminv1.ApplyGrantResponse{Application: a.proto(s.now())}, nil
}

// ListGrantApplications 租户负责人看自己负责的租户的申请，数据属主看本部门数据源收到的申请（两种身份都有时合在一起）。
func (s *SourceServer) ListGrantApplications(ctx context.Context, r *adminv1.ListGrantApplicationsRequest) (*adminv1.ListGrantApplicationsResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	tenants, dept := ownedTenants(ctx), ""
	if grpcmw.HasRole(p, RoleDataOwner) {
		dept = p.GetDept()
	}
	if len(tenants) == 0 && !grpcmw.HasRole(p, RoleDataOwner) {
		return nil, status.Error(codes.PermissionDenied, "需要租户负责人或数据属主")
	}
	want := ""
	if r.GetStatus() != adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_UNSPECIFIED {
		for k, v := range applicationStatus {
			if v == r.GetStatus() {
				want = k
			}
		}
		if want == "" {
			return nil, status.Error(codes.InvalidArgument, "status 不合法")
		}
	}
	out := &adminv1.ListGrantApplicationsResponse{}
	if len(tenants) == 0 && dept == "" {
		return out, nil
	}
	as, err := s.Store.Applications(ctx, tenants, dept, maxApplications)
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	now := s.now()
	for _, a := range as {
		if want == "" || a.effective(now) == want {
			out.Applications = append(out.Applications, a.proto(now))
		}
	}
	return out, nil
}

// ApproveGrantApplication 本部门的数据属主审批，记下依据，同一事务里写入授权与变更审计。
func (s *SourceServer) ApproveGrantApplication(ctx context.Context, r *adminv1.ApproveGrantApplicationRequest) (*adminv1.ApproveGrantApplicationResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RoleDataOwner) {
		return nil, status.Error(codes.PermissionDenied, "需要数据属主")
	}
	if !short(r.GetBasis(), 500) {
		return nil, status.Error(codes.InvalidArgument, "basis 必填，最多 500 字")
	}
	now := s.now()
	a, g, err := s.decide(ctx, r.GetApplicationId(), true, func(a *Application, src Source) (*Grant, error) {
		switch {
		case a.AppliedBy == p.GetSub():
			return nil, grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "申请人不能审批自己的申请")
		case src.Status != "active":
			return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "数据源已不是生效状态")
		}
		a.Status, a.DecidedBy, a.DecidedAt, a.Note = "approved", p.GetSub(), &now, strings.TrimSpace(r.GetBasis())
		if err := s.Audit.Write(ctx, "source_grant.application_approved", a.SourceID, map[string]string{"application_id": a.ID,
			"tenant": a.Tenant, "applied_by": a.AppliedBy, "basis": a.Note}); err != nil {
			return nil, err
		}
		return &Grant{SourceID: a.SourceID, Tenant: a.Tenant, GrantedBy: p.GetSub(), ApplicationID: a.ID}, nil
	})
	if err != nil {
		return nil, err
	}
	return &adminv1.ApproveGrantApplicationResponse{Application: a.proto(now), Grant: g.proto()}, nil
}

// RejectGrantApplication 本部门的数据属主驳回，或申请人撤回。都是收紧，一人即可。
func (s *SourceServer) RejectGrantApplication(ctx context.Context, r *adminv1.RejectGrantApplicationRequest) (*adminv1.RejectGrantApplicationResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !short(r.GetReason(), 500) {
		return nil, status.Error(codes.InvalidArgument, "reason 必填，最多 500 字")
	}
	now := s.now()
	a, _, err := s.decide(ctx, r.GetApplicationId(), false, func(a *Application, src Source) (*Grant, error) {
		switch {
		case a.AppliedBy == p.GetSub():
			a.Status = "withdrawn"
		case grpcmw.HasRole(p, RoleDataOwner) && src.OwnerDept == p.GetDept():
			a.Status = "rejected"
		default:
			return nil, status.Error(codes.NotFound, "申请不存在")
		}
		a.DecidedBy, a.DecidedAt, a.Note = p.GetSub(), &now, strings.TrimSpace(r.GetReason())
		return nil, s.Audit.Write(ctx, "source_grant.application_"+a.Status, a.SourceID, map[string]string{"application_id": a.ID,
			"tenant": a.Tenant, "reason": a.Note})
	})
	if err != nil {
		return nil, err
	}
	return &adminv1.RejectGrantApplicationResponse{Application: a.proto(now)}, nil
}

// decide 锁住待审批的申请与它的数据源交给 change。ownerOnly 时调用者须是该数据源所属部门的属主，
// 否则按不存在处理（不让调用方探测别的部门收到了哪些申请）；不是 ownerOnly 时属主或申请人本人都可以。
func (s *SourceServer) decide(ctx context.Context, id string, ownerOnly bool,
	change func(a *Application, src Source) (*Grant, error)) (Application, Grant, error) {
	if !applicationPattern.MatchString(id) {
		return Application{}, Grant{}, status.Error(codes.NotFound, "申请不存在")
	}
	p := grpcmw.PrincipalFrom(ctx)
	now := s.now()
	a, g, err := s.Store.DecideApplication(ctx, id, func(a *Application, src Source) (*Grant, error) {
		mine := a.AppliedBy == p.GetSub()
		owner := grpcmw.HasRole(p, RoleDataOwner) && src.OwnerDept == p.GetDept()
		switch {
		case !owner && (ownerOnly || !mine):
			return nil, status.Error(codes.NotFound, "申请不存在")
		case a.effective(now) != "pending":
			return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "申请不是待审批状态（可能已过期）")
		}
		return change(a, src)
	})
	if err != nil {
		return Application{}, Grant{}, s.storeErr(ctx, err)
	}
	return a, g, nil
}
