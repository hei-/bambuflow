package access

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/services/governance/internal/paging"
)

// 平台角色（gateway 的 BF_ADMIN_ROLES_FILE 映射，governance 详设 9.1）
const (
	RoleInfoSec       = "infosec"
	RoleDataOwner     = "data_owner"
	RolePlatformAdmin = "platform_admin"
	RoleAuditor       = "auditor"
	RoleOperator      = "platform_operator"
)

// Auditor 写变更审计（internal/changeaudit）。
type Auditor interface {
	Write(ctx context.Context, action, objectID string, detail any) error
}

// Denier 把拒绝规则写进索引库的拒绝名单：只有 indexing 写索引库，经 IndexAdminService.AddDenyRules。
type Denier interface {
	AddDenyRules(ctx context.Context, in *adminv1.AddDenyRulesRequest, opts ...grpc.CallOption) (*adminv1.AddDenyRulesResponse, error)
}

// DenialStore 是紧急拒绝规则与数据源的读写（生产为 PG）。
type DenialStore interface {
	CreateDenial(ctx context.Context, d Denial, beforeCommit func() error) (Denial, error)
	DenialByTarget(ctx context.Context, scope, target string) (Denial, error)
	ListDenials(ctx context.Context, offset, limit int) ([]Denial, error)
	PendingDenials(ctx context.Context) ([]Denial, error)
	MarkApplied(ctx context.Context, id string) (time.Time, error)
	UnpublishedDenials(ctx context.Context) ([]Denial, error)
	MarkPublished(ctx context.Context, id string) error
	Source(ctx context.Context, id string) (Source, error)
}

// BackupChannel 把规则签名后发到事件流的独立主题（governance 详设 8.2 的备用通道）：索引库的拒绝名单写不进去时，
// retrieval 仍能从这里拿到规则。
type BackupChannel interface {
	Publish(ctx context.Context, d Denial) error
}

// applyTimeout：写拒绝名单的单次超时。超时的规则保持 pending，由 Reconcile 重试。
const applyTimeout = 3 * time.Second

var (
	scopes = map[adminv1.DenialScope]string{
		adminv1.DenialScope_DENIAL_SCOPE_SOURCE: "source", adminv1.DenialScope_DENIAL_SCOPE_DOCUMENT: "document",
		adminv1.DenialScope_DENIAL_SCOPE_TENANT: "tenant",
	}
	sourceIDPattern = regexp.MustCompile(`^src_[A-Za-z0-9]{1,64}$`)
	tenantPattern   = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	errInternal     = status.Error(codes.Unavailable, "暂时不可用")
)

// DenialServer 实现 EmergencyDenialAdminService（governance 详设 4.2、8.2）。
type DenialServer struct {
	adminv1.UnimplementedEmergencyDenialAdminServiceServer
	Store DenialStore
	Audit Auditor
	Index Denier
	// Backup 是备用通道
	Backup BackupChannel
	Log    *slog.Logger
}

func newID(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}

// validTarget 按范围校验对象标识。文档 ID 由数据源决定，只限制长度与不含控制字符。
func validTarget(scope, target string) bool {
	switch scope {
	case "source":
		return sourceIDPattern.MatchString(target)
	case "tenant":
		return tenantPattern.MatchString(target)
	default:
		return target != "" && len(target) <= 512 && utf8.ValidString(target) &&
			!strings.ContainsFunc(target, unicode.IsControl)
	}
}

// validReason：原因必填，最多 500 字。
func validReason(r string) bool {
	n := utf8.RuneCountInString(strings.TrimSpace(r))
	return n > 0 && n <= 500
}

func (s *DenialServer) CreateEmergencyDenial(ctx context.Context, r *adminv1.CreateEmergencyDenialRequest) (*adminv1.CreateEmergencyDenialResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RoleInfoSec, RoleDataOwner) {
		return nil, status.Error(codes.PermissionDenied, "需要信息安全或数据属主")
	}
	scope, ok := scopes[r.GetScope()]
	if !ok || !validTarget(scope, r.GetTarget()) || !validReason(r.GetReason()) {
		return nil, status.Error(codes.InvalidArgument, "scope、target 或 reason 不合法")
	}
	// 数据属主只能拒绝本部门的数据源（governance 详设 9.1）；按文档、按租户拒绝只有信息安全能做：
	// governance 不掌握文档属于哪个数据源，无法核对文档是否在属主的范围内
	if !grpcmw.HasRole(p, RoleInfoSec) {
		if scope != "source" {
			return nil, grpcmw.Reason(codes.PermissionDenied, "out_of_scope", "数据属主只能按数据源拒绝")
		}
		src, err := s.Store.Source(ctx, r.GetTarget())
		if errors.Is(err, ErrNotFound) || (err == nil && src.OwnerDept != p.GetDept()) {
			return nil, grpcmw.Reason(codes.PermissionDenied, "out_of_scope", "不是本部门的数据源")
		}
		if err != nil {
			s.Log.ErrorContext(ctx, "读取数据源失败", slog.Any("err", err))
			return nil, errInternal
		}
	}

	d, created, err := s.create(ctx, scope, r.GetTarget(), r.GetReason())
	if err != nil {
		return nil, err
	}
	return &adminv1.CreateEmergencyDenialResponse{Denial: d.proto(), Created: created}, nil
}

// create 登记一条规则（调用方已校验角色与参数）并尝试写进拒绝名单；同一对象已有规则时返回已有的、created 为 false。
// 撤销数据源也经它先写按数据源的规则（governance 详设 6.2）。
func (s *DenialServer) create(ctx context.Context, scope, target, reason string) (Denial, bool, error) {
	d := Denial{ID: newID("den_"), Scope: scope, Target: target, Reason: strings.TrimSpace(reason),
		CreatedBy: grpcmw.PrincipalFrom(ctx).GetSub(), TraceID: grpcmw.TraceID(ctx)}
	d, err := s.Store.CreateDenial(ctx, d, func() error {
		return s.Audit.Write(ctx, "emergency_denial.create", d.ID, map[string]string{"scope": scope, "target": d.Target, "reason": d.Reason})
	})
	created := true
	if errors.Is(err, ErrExists) {
		// 同一对象已有规则：返回已有的，不重复登记（规则只增不减，重复无意义）
		created = false
		d, err = s.Store.DenialByTarget(ctx, scope, target)
	}
	if err != nil {
		s.Log.ErrorContext(ctx, "登记紧急拒绝规则失败", slog.Any("err", err))
		return Denial{}, false, errInternal
	}
	if d.PublishedAt == nil {
		s.publish(ctx, &d)
	}
	if d.AppliedAt == nil {
		s.apply(ctx, &d)
	}
	return d, created, nil
}

// apply 把规则写进索引库的拒绝名单。失败时规则保持 pending：返回给调用方的状态就是告警，
// 同时记错误日志，Reconcile 每 10 秒重试（governance 详设 8.2）。
func (s *DenialServer) apply(ctx context.Context, d *Denial) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), applyTimeout)
	defer cancel()
	req := &adminv1.AddDenyRulesRequest{Reason: d.ID + " " + d.Reason}
	switch d.Scope {
	case "source":
		req.SourceIds = []string{d.Target}
	case "document":
		req.DocIds = []string{d.Target}
	case "tenant":
		req.Tenants = []string{d.Target}
	}
	if _, err := s.Index.AddDenyRules(ctx, req); err != nil {
		s.Log.ErrorContext(ctx, "紧急拒绝规则未能写进拒绝名单，稍后重试", slog.String("denial_id", d.ID),
			slog.Duration("pending_for", time.Since(d.CreatedAt)), slog.Any("err", err))
		return
	}
	at, err := s.Store.MarkApplied(ctx, d.ID)
	if err != nil {
		// 拒绝名单已经写入，检索已生效；只是状态没记下，下一轮重试时会再写一次（幂等）再记
		s.Log.ErrorContext(ctx, "记录紧急拒绝规则生效时间失败", slog.String("denial_id", d.ID), slog.Any("err", err))
		return
	}
	d.AppliedAt = &at
	s.Log.WarnContext(ctx, "紧急拒绝规则已生效", slog.String("denial_id", d.ID), slog.String("scope", d.Scope))
}

// publish 把规则发到备用通道；失败只记错误日志，Reconcile 重试。
func (s *DenialServer) publish(ctx context.Context, d *Denial) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), applyTimeout)
	defer cancel()
	if err := s.Backup.Publish(ctx, *d); err != nil {
		s.Log.ErrorContext(ctx, "紧急拒绝规则未能发到备用通道，稍后重试", slog.String("denial_id", d.ID), slog.Any("err", err))
		return
	}
	if err := s.Store.MarkPublished(ctx, d.ID); err != nil {
		// 已经发出去了，下一轮会再发一次：retrieval 按规则 ID 去重
		s.Log.ErrorContext(ctx, "记录紧急拒绝规则发布时间失败", slog.String("denial_id", d.ID), slog.Any("err", err))
		return
	}
	now := time.Now()
	d.PublishedAt = &now
}

// Reconcile 每隔 every 把还没生效的规则重新写一遍拒绝名单、还没发到备用通道的重新发一遍，直到 ctx 结束。
func (s *DenialServer) Reconcile(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		pending, err := s.Store.PendingDenials(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取未生效的紧急拒绝规则失败", slog.Any("err", err))
			continue
		}
		for i := range pending {
			s.apply(ctx, &pending[i])
		}
		unpublished, err := s.Store.UnpublishedDenials(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取未发到备用通道的紧急拒绝规则失败", slog.Any("err", err))
			continue
		}
		for i := range unpublished {
			s.publish(ctx, &unpublished[i])
		}
	}
}

func (s *DenialServer) ListEmergencyDenials(ctx context.Context, r *adminv1.ListEmergencyDenialsRequest) (*adminv1.ListEmergencyDenialsResponse, error) {
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), RoleInfoSec, RoleDataOwner) {
		return nil, status.Error(codes.PermissionDenied, "需要信息安全或数据属主")
	}
	size, offset, err := paging.Parse(r.GetPageSize(), r.GetPageToken())
	if err != nil {
		return nil, err
	}
	ds, err := s.Store.ListDenials(ctx, offset, size+1)
	if err != nil {
		s.Log.ErrorContext(ctx, "读取紧急拒绝规则失败", slog.Any("err", err))
		return nil, errInternal
	}
	out := &adminv1.ListEmergencyDenialsResponse{}
	if len(ds) > size {
		ds, out.NextPageToken = ds[:size], paging.Token(offset+size)
	}
	for _, d := range ds {
		out.Denials = append(out.Denials, d.proto())
	}
	return out, nil
}

func (d Denial) proto() *adminv1.EmergencyDenial {
	out := &adminv1.EmergencyDenial{
		DenialId: d.ID, Target: d.Target, Reason: d.Reason, CreatedBy: d.CreatedBy, CreatedAt: timestamppb.New(d.CreatedAt),
		Status: adminv1.DenialStatus_DENIAL_STATUS_PENDING,
	}
	for k, v := range scopes {
		if v == d.Scope {
			out.Scope = k
		}
	}
	if d.AppliedAt != nil {
		out.Status, out.AppliedAt = adminv1.DenialStatus_DENIAL_STATUS_ACTIVE, timestamppb.New(*d.AppliedAt)
	}
	return out
}
