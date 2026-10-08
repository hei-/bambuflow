package access

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/outbox"
)

// SourceStore 是数据源与授权的读写（生产为 PG）。
type SourceStore interface {
	Source(ctx context.Context, id string) (Source, error)
	RegisterSource(ctx context.Context, src Source, beforeCommit func() error) (Source, error)
	UpdateSource(ctx context.Context, id string, change func(tx pgx.Tx, src *Source) error) (Source, error)
	Grants(ctx context.Context, sourceID string) ([]Grant, error)
	CreateGrant(ctx context.Context, g Grant, beforeCommit func() error) (Grant, bool, error)
	DeleteGrant(ctx context.Context, sourceID, tenant string, beforeCommit func() error) error
	ActiveGrants(ctx context.Context) ([]Grant, error)
}

// SourceServer 实现 SourceAdminService（governance 详设 6.2、7.2）。
type SourceServer struct {
	adminv1.UnimplementedSourceAdminServiceServer
	Store   SourceStore
	Audit   Auditor
	Denials *DenialServer
	// WriteEvent 在撤销的同一事务里写 source.revoked 事件（生产为 access.WriteEvent）
	WriteEvent func(ctx context.Context, tx pgx.Tx, id string, payload []byte) error
	Log        *slog.Logger
	Now        func() time.Time
}

var sourceTypes = map[commonv1.SourceType]string{
	commonv1.SourceType_SOURCE_TYPE_WIKI: "wiki", commonv1.SourceType_SOURCE_TYPE_MAIL: "mail",
	commonv1.SourceType_SOURCE_TYPE_FILE: "file", commonv1.SourceType_SOURCE_TYPE_IMAGE: "image",
	commonv1.SourceType_SOURCE_TYPE_TABLE: "table", commonv1.SourceType_SOURCE_TYPE_LOG: "log",
	commonv1.SourceType_SOURCE_TYPE_DB: "db",
}

var sourceStatus = map[string]adminv1.SourceStatus{
	"pending": adminv1.SourceStatus_SOURCE_STATUS_PENDING, "active": adminv1.SourceStatus_SOURCE_STATUS_ACTIVE,
	"revoked": adminv1.SourceStatus_SOURCE_STATUS_REVOKED,
}

func short(v string, n int) bool {
	v = strings.TrimSpace(v)
	return v != "" && utf8.RuneCountInString(v) <= n
}

func (s *SourceServer) RegisterSource(ctx context.Context, r *adminv1.RegisterSourceRequest) (*adminv1.RegisterSourceResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RolePlatformAdmin) {
		return nil, status.Error(codes.PermissionDenied, "需要平台管理员")
	}
	typ, ok := sourceTypes[r.GetType()]
	c := r.GetDefaultClassification()
	if !ok || !short(r.GetOwnerDept(), 64) || utf8.RuneCountInString(r.GetDescription()) > 500 ||
		c < commonv1.Classification_CLASSIFICATION_PUBLIC || c > commonv1.Classification_CLASSIFICATION_CONFIDENTIAL {
		// 受限级数据源还要信息安全审批（governance 详设 6.2），这一步尚未实现，先不接受注册
		return nil, status.Error(codes.InvalidArgument, "type、owner_dept、default_classification（公开到机密）或 description 不合法")
	}
	src := Source{ID: newID("src_")[:4+16], Type: typ, OwnerDept: strings.TrimSpace(r.GetOwnerDept()), Status: "pending",
		Description: strings.TrimSpace(r.GetDescription()), CreatedBy: p.GetSub(), DefaultClassification: int16(c)}
	src, err := s.Store.RegisterSource(ctx, src, func() error {
		return s.Audit.Write(ctx, "source.register", src.ID, map[string]string{"type": typ, "owner_dept": src.OwnerDept})
	})
	if err != nil {
		s.Log.ErrorContext(ctx, "注册数据源失败", slog.Any("err", err))
		return nil, errInternal
	}
	return &adminv1.RegisterSourceResponse{Source: src.proto()}, nil
}

// owner 校验调用者是数据源所属部门的数据属主（governance 详设 9.1：不能影响其他部门的数据源）。
// 不存在与不在范围内都按 404 处理，不让调用方据此探测别的部门有哪些数据源。
func (s *SourceServer) owner(ctx context.Context, id string) (Source, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RoleDataOwner) {
		return Source{}, status.Error(codes.PermissionDenied, "需要数据属主")
	}
	if !sourceIDPattern.MatchString(id) {
		return Source{}, status.Error(codes.NotFound, "数据源不存在")
	}
	src, err := s.Store.Source(ctx, id)
	if errors.Is(err, ErrNotFound) || (err == nil && src.OwnerDept != p.GetDept()) {
		return Source{}, status.Error(codes.NotFound, "数据源不存在")
	}
	if err != nil {
		s.Log.ErrorContext(ctx, "读取数据源失败", slog.Any("err", err))
		return Source{}, errInternal
	}
	return src, nil
}

func (s *SourceServer) storeErr(ctx context.Context, err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, ErrNotFound) {
		return status.Error(codes.NotFound, "不存在")
	}
	s.Log.ErrorContext(ctx, "读写数据源失败", slog.Any("err", err))
	return errInternal
}

func (s *SourceServer) ApproveSource(ctx context.Context, r *adminv1.ApproveSourceRequest) (*adminv1.ApproveSourceResponse, error) {
	if _, err := s.owner(ctx, r.GetSourceId()); err != nil {
		return nil, err
	}
	if !short(r.GetBasis(), 500) {
		return nil, status.Error(codes.InvalidArgument, "basis（审批依据）必填，不超过 500 字")
	}
	sub := grpcmw.PrincipalFrom(ctx).GetSub()
	src, err := s.Store.UpdateSource(ctx, r.GetSourceId(), func(_ pgx.Tx, src *Source) error {
		switch {
		case src.CreatedBy == sub:
			return grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "注册人不能审批自己注册的数据源")
		case src.Status != "pending":
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "数据源不是待审批状态")
		}
		now := s.Now()
		src.Status, src.ApprovedBy, src.ApprovalBasis, src.ApprovedAt = "active", sub, strings.TrimSpace(r.GetBasis()), &now
		return s.Audit.Write(ctx, "source.approve", src.ID, map[string]string{"basis": src.ApprovalBasis})
	})
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	return &adminv1.ApproveSourceResponse{Source: src.proto()}, nil
}

// RevokeSource 先写按数据源的紧急拒绝规则（在线立即不可见），再把数据源标为撤销、写 source.revoked 事件
// （connectors 据此对该源全部文档写删除标记，governance 详设 6.2）。撤销后授权不再下发给 retrieval。
func (s *SourceServer) RevokeSource(ctx context.Context, r *adminv1.RevokeSourceRequest) (*adminv1.RevokeSourceResponse, error) {
	src, err := s.owner(ctx, r.GetSourceId())
	if err != nil {
		return nil, err
	}
	if !short(r.GetReason(), 500) {
		return nil, status.Error(codes.InvalidArgument, "reason 必填，不超过 500 字")
	}
	if src.Status == "revoked" {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "数据源已撤销")
	}
	d, _, err := s.Denials.create(ctx, "source", src.ID, "撤销数据源："+r.GetReason())
	if err != nil {
		return nil, err
	}
	sub := grpcmw.PrincipalFrom(ctx).GetSub()
	src, err = s.Store.UpdateSource(ctx, src.ID, func(tx pgx.Tx, src *Source) error {
		if src.Status == "revoked" {
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "数据源已撤销")
		}
		now := s.Now()
		src.Status, src.RevokedBy, src.RevokeReason, src.RevokedAt = "revoked", sub, strings.TrimSpace(r.GetReason()), &now
		evt, pb, err := outbox.New(outbox.SourceRevoked, "", src.ID, now)
		if err != nil {
			return err
		}
		if err := s.WriteEvent(ctx, tx, evt.GetEventId(), pb); err != nil {
			return err
		}
		return s.Audit.Write(ctx, "source.revoke", src.ID, map[string]string{"reason": src.RevokeReason, "denial_id": d.ID})
	})
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	s.Log.WarnContext(ctx, "数据源已撤销", slog.String("source_id", src.ID), slog.String("denial_id", d.ID))
	return &adminv1.RevokeSourceResponse{Source: src.proto(), DenialId: d.ID}, nil
}

func (s *SourceServer) ListGrants(ctx context.Context, r *adminv1.ListGrantsRequest) (*adminv1.ListGrantsResponse, error) {
	if _, err := s.owner(ctx, r.GetSourceId()); err != nil {
		return nil, err
	}
	gs, err := s.Store.Grants(ctx, r.GetSourceId())
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	out := &adminv1.ListGrantsResponse{}
	for _, g := range gs {
		out.Grants = append(out.Grants, g.proto())
	}
	return out, nil
}

func (s *SourceServer) CreateGrant(ctx context.Context, r *adminv1.CreateGrantRequest) (*adminv1.CreateGrantResponse, error) {
	src, err := s.owner(ctx, r.GetSourceId())
	if err != nil {
		return nil, err
	}
	if !tenantPattern.MatchString(r.GetTenant()) {
		return nil, status.Error(codes.InvalidArgument, "tenant 不合法")
	}
	if src.Status != "active" {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "只能授权已审批、未撤销的数据源")
	}
	g, created, err := s.Store.CreateGrant(ctx, Grant{SourceID: src.ID, Tenant: r.GetTenant(), GrantedBy: grpcmw.PrincipalFrom(ctx).GetSub()},
		func() error {
			return s.Audit.Write(ctx, "source_grant.create", src.ID, map[string]string{"tenant": r.GetTenant()})
		})
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	return &adminv1.CreateGrantResponse{Grant: g.proto(), Created: created}, nil
}

func (s *SourceServer) DeleteGrant(ctx context.Context, r *adminv1.DeleteGrantRequest) (*adminv1.DeleteGrantResponse, error) {
	if _, err := s.owner(ctx, r.GetSourceId()); err != nil {
		return nil, err
	}
	if !tenantPattern.MatchString(r.GetTenant()) {
		return nil, status.Error(codes.NotFound, "授权不存在")
	}
	err := s.Store.DeleteGrant(ctx, r.GetSourceId(), r.GetTenant(),
		func() error {
			return s.Audit.Write(ctx, "source_grant.delete", r.GetSourceId(), map[string]string{"tenant": r.GetTenant()})
		})
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	return &adminv1.DeleteGrantResponse{}, nil
}

// Snapshot 是 retrieval 的配置快照：租户可访问的数据源。版本号取内容摘要，内容不变版本号就不变。
func (s *SourceServer) Snapshot(ctx context.Context) (*configv1.GetSnapshotResponse, error) {
	gs, err := s.Store.ActiveGrants(ctx)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	out := &configv1.GetSnapshotResponse{}
	for _, g := range gs {
		_, _ = h.Write([]byte(g.Tenant + "\x00" + g.SourceID + "\n"))
		out.SourceGrants = append(out.SourceGrants, &configv1.SourceGrant{Tenant: g.Tenant, SourceId: g.SourceID})
	}
	out.SnapshotVersion = "grants:" + hex.EncodeToString(h.Sum(nil))[:16]
	return out, nil
}

func (x Source) proto() *adminv1.Source {
	out := &adminv1.Source{
		SourceId: x.ID, OwnerDept: x.OwnerDept, DefaultClassification: commonv1.Classification(x.DefaultClassification),
		Description: x.Description, Status: sourceStatus[x.Status], CreatedBy: x.CreatedBy, CreatedAt: timestamppb.New(x.CreatedAt),
		ApprovedBy: x.ApprovedBy, ApprovalBasis: x.ApprovalBasis, RevokedBy: x.RevokedBy, RevokeReason: x.RevokeReason,
	}
	for k, v := range sourceTypes {
		if v == x.Type {
			out.Type = k
		}
	}
	if x.ApprovedAt != nil {
		out.ApprovedAt = timestamppb.New(*x.ApprovedAt)
	}
	if x.RevokedAt != nil {
		out.RevokedAt = timestamppb.New(*x.RevokedAt)
	}
	return out
}

func (g Grant) proto() *adminv1.Grant {
	return &adminv1.Grant{SourceId: g.SourceID, Tenant: g.Tenant, GrantedBy: g.GrantedBy, GrantedAt: timestamppb.New(g.GrantedAt)}
}
