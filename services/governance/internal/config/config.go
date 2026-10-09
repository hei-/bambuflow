// Package config 是配置对象的版本、变更单与发布（governance 详设 3、4、7.2、8.3）。
//
// 现在的配置对象只有租户配置（object = tenant，scope = 租户名，内容是 tenant.yaml）。每次修改产生一个不可变版本，
// 每个对象只有一个生效指针，只有发布能改它；生效后经 ConfigService 推给 gateway（vocab.Server 的 gateway 快照）。
//
//   - 变更单（console 发起，带 base_version）：作者以外的平台管理员审批后生成发布并立即生效；审批时生效版本已不是
//     base 则变更单作废为 stale，要求基于最新版本重新提交，不自动合并（governance 详设 8.3）。
//   - 发布（CI 发起，带 source_ref）：作者以外的平台管理员审批后生效。
//   - 回退：一人执行，切回该发布之前的生效版本（governance 详设 4.2：回退不再过门禁）。
//   - 租户从试点切到全量（或新建即为全量）要附一个已通过、覆盖该租户的评测门禁（evaluation 详设 3.1）。
//
// 没有做的：灰度（试点租户、10% 实例）与观察期、Git 合并请求的自动生成（变更单内容存在这里，需要人工同步回仓库）。
package config

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/tenancy"
)

// RolePlatformAdmin 发起与审批配置变更（governance 详设 9.1；设计里 pipeline 由租户负责人审批，平台角色里还没有）。
const RolePlatformAdmin = "platform_admin"

// ObjectTenant 是租户配置。
const ObjectTenant = "tenant"

const maxContent = 64 << 10

var (
	ErrNotFound = errors.New("不存在")
	// ErrBusy：同一对象已有待审批的变更单
	ErrBusy = errors.New("该对象已有待审批的变更单")

	chgPattern  = regexp.MustCompile(`^chg_[0-9a-f]{24}$`)
	relPattern  = regexp.MustCompile(`^rel_[0-9a-f]{24}$`)
	gatePattern = regexp.MustCompile(`^gate_[0-9a-f]{24}$`)
	refPattern  = regexp.MustCompile(`^git:[0-9a-f]{7,64}$`)
)

type Version struct {
	ID, Object, Scope, Content, ContentHash, CreatedBy, SourceRef string
	Number                                                        int32
	CreatedAt                                                     time.Time
}

type Release struct {
	ID, Object, Scope, VersionID, FromVersionID, Source, SourceRef, GateID, Status, Reason string
	RequestedBy, ApprovedBy, RolledBackBy                                                  string
	RequestedAt                                                                            time.Time
	ApprovedAt, RolledBackAt                                                               *time.Time
}

type Change struct {
	ID, Object, Scope, BaseVersionID, VersionID, Reason, GateID, Author, Status string
	DecidedBy, RejectReason, ReleaseID                                          string
	CreatedAt                                                                   time.Time
	DecidedAt                                                                   *time.Time
}

// Tx 是一次事务里的读写（生产为 PG 事务）。Lock 开头的方法锁住读到的行直到事务结束。
type Tx interface {
	// Active 返回该对象全部作用域的生效版本（校验跨租户约束用）
	Active(ctx context.Context, object string) ([]Version, error)
	// LockPointer 锁住并返回作用域的生效版本 ID；没有时为空
	LockPointer(ctx context.Context, object, scope string) (string, error)
	SetPointer(ctx context.Context, object, scope, versionID string) error
	NextNumber(ctx context.Context, object, scope string) (int32, error)
	InsertVersion(ctx context.Context, v Version) error
	Version(ctx context.Context, id string) (Version, error)
	Versions(ctx context.Context, object, scope string) ([]Version, error)
	InsertRelease(ctx context.Context, r Release) error
	LockRelease(ctx context.Context, id string) (Release, error)
	UpdateRelease(ctx context.Context, r Release) error
	// SupersedeActive 把该作用域状态为 active 的发布改为 superseded（新发布生效前调用）
	SupersedeActive(ctx context.Context, object, scope string) error
	InsertChange(ctx context.Context, c Change) error
	LockChange(ctx context.Context, id string) (Change, error)
	UpdateChange(ctx context.Context, c Change) error
}

type Store interface {
	InTx(ctx context.Context, fn func(Tx) error) error
}

type Auditor interface {
	Write(ctx context.Context, action, objectID string, detail any) error
}

// GateChecker 核对评测门禁已通过且覆盖该租户（evaluation 的 EvalGateAdminService）。
type GateChecker func(ctx context.Context, gateID, tenant string) error

// ErrGateFailed：门禁不存在、没通过或不覆盖该租户。
var ErrGateFailed = errors.New("评测门禁不满足")

type Server struct {
	adminv1.UnimplementedConfigAdminServiceServer
	Store Store
	Audit Auditor
	Gate  GateChecker
	// PlatformClients 是平台级客户端（与 gateway 的 BF_PLATFORM_CLIENTS 一致），校验客户端身份不重复时用
	PlatformClients []string
	Log             *slog.Logger
	Now             func() time.Time
}

func newID(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}

func hash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func text(s string, max int) (string, bool) {
	s = strings.TrimSpace(s)
	n := utf8.RuneCountInString(s)
	return s, n > 0 && n <= max
}

func (s *Server) admin(ctx context.Context) (string, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RolePlatformAdmin) {
		return "", status.Error(codes.PermissionDenied, "需要平台管理员")
	}
	return p.GetSub(), nil
}

func (s *Server) fail(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, "不存在")
	case errors.Is(err, ErrBusy):
		return grpcmw.Reason(codes.FailedPrecondition, "conflict", ErrBusy.Error())
	case errors.Is(err, ErrGateFailed):
		return grpcmw.Reason(codes.FailedPrecondition, "gate_failed", err.Error())
	}
	var st interface{ GRPCStatus() *status.Status }
	if errors.As(err, &st) {
		return err
	}
	s.Log.ErrorContext(ctx, "配置变更失败", slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}

// validate 校验新内容：本身合法，且与其他租户的生效配置放在一起也合法（同一份校验代码 gateway 加载时也用）。
// 返回旧的生效配置（新建时为 nil）与新配置。
func (s *Server) validate(ctx context.Context, tx Tx, object, scope, content string) (*tenancy.Tenant, tenancy.Tenant, error) {
	if object != ObjectTenant {
		return nil, tenancy.Tenant{}, status.Error(codes.InvalidArgument, "object 只能是 tenant")
	}
	if content == "" || len(content) > maxContent {
		return nil, tenancy.Tenant{}, status.Error(codes.InvalidArgument, "content 为空或超过 64 KB")
	}
	next, err := tenancy.Parse(scope, []byte(content))
	if err != nil {
		return nil, tenancy.Tenant{}, status.Error(codes.InvalidArgument, err.Error())
	}
	active, err := tx.Active(ctx, object)
	if err != nil {
		return nil, tenancy.Tenant{}, err
	}
	var old *tenancy.Tenant
	all := []tenancy.Tenant{next}
	for _, v := range active {
		t, err := tenancy.Parse(v.Scope, []byte(v.Content))
		if err != nil {
			return nil, tenancy.Tenant{}, fmt.Errorf("生效版本 %s 解析失败: %w", v.ID, err)
		}
		if v.Scope == scope {
			old = &t
			continue
		}
		all = append(all, t)
	}
	if _, err := tenancy.New(all, s.PlatformClients); err != nil {
		return nil, tenancy.Tenant{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return old, next, nil
}

// checkGate：租户切到全量（或新建即为全量）时，必须附已通过、覆盖该租户的门禁（evaluation 详设 3.1）。
func (s *Server) checkGate(ctx context.Context, old *tenancy.Tenant, next tenancy.Tenant, gateID string) error {
	widening := next.Rollout == "general" && (old == nil || old.Rollout != "general")
	switch {
	case gateID != "" && !gatePattern.MatchString(gateID):
		return status.Error(codes.InvalidArgument, "gate_id 不合法")
	case !widening && gateID == "":
		return nil
	case widening && gateID == "":
		return grpcmw.Reason(codes.FailedPrecondition, "gate_failed", "租户切到全量须附已通过、覆盖该租户的评测门禁")
	}
	return s.Gate(ctx, gateID, next.Name)
}

// newVersion 在事务里写一个新版本。
func newVersion(ctx context.Context, tx Tx, object, scope, content, by, ref string, now time.Time) (Version, error) {
	n, err := tx.NextNumber(ctx, object, scope)
	if err != nil {
		return Version{}, err
	}
	v := Version{ID: newID("cfv_"), Object: object, Scope: scope, Number: n, Content: content, ContentHash: hash(content),
		CreatedBy: by, CreatedAt: now, SourceRef: ref}
	return v, tx.InsertVersion(ctx, v)
}

// activate 让发布生效：核对生效指针仍是发布时的版本（否则 stale），切指针，原来生效的发布标为 superseded。
func activate(ctx context.Context, tx Tx, r *Release) (bool, error) {
	cur, err := tx.LockPointer(ctx, r.Object, r.Scope)
	if err != nil {
		return false, err
	}
	if cur != r.FromVersionID {
		return false, nil
	}
	if err := tx.SupersedeActive(ctx, r.Object, r.Scope); err != nil {
		return false, err
	}
	if err := tx.SetPointer(ctx, r.Object, r.Scope, r.VersionID); err != nil {
		return false, err
	}
	r.Status = "active"
	return true, nil
}

func (s *Server) CreateChangeRequest(ctx context.Context, r *adminv1.CreateChangeRequestRequest) (*adminv1.CreateChangeRequestResponse, error) {
	sub, err := s.admin(ctx)
	if err != nil {
		return nil, err
	}
	reason, ok := text(r.GetReason(), 500)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "reason 必填，不超过 500 字")
	}
	var c Change
	var v Version
	err = s.Store.InTx(ctx, func(tx Tx) error {
		cur, err := tx.LockPointer(ctx, r.GetObject(), r.GetScope())
		if err != nil {
			return err
		}
		if cur != r.GetBaseVersionId() {
			return grpcmw.Reason(codes.FailedPrecondition, "stale_base_version", "base_version_id 不是当前生效版本，请基于最新版本修改")
		}
		old, next, err := s.validate(ctx, tx, r.GetObject(), r.GetScope(), r.GetContent())
		if err != nil {
			return err
		}
		if err := s.checkGate(ctx, old, next, r.GetGateId()); err != nil {
			return err
		}
		now := s.Now()
		c = Change{ID: newID("chg_"), Object: r.GetObject(), Scope: r.GetScope(), BaseVersionID: cur, Reason: reason,
			GateID: r.GetGateId(), Author: sub, CreatedAt: now, Status: "open"}
		if v, err = newVersion(ctx, tx, c.Object, c.Scope, r.GetContent(), sub, c.ID, now); err != nil {
			return err
		}
		c.VersionID = v.ID
		if err := tx.InsertChange(ctx, c); err != nil {
			return err
		}
		return s.Audit.Write(ctx, "config.change_request", c.ID, map[string]any{"object": c.Object, "scope": c.Scope,
			"base_version_id": c.BaseVersionID, "version_id": v.ID, "content_hash": v.ContentHash, "gate_id": c.GateID})
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &adminv1.CreateChangeRequestResponse{ChangeRequest: c.proto(&v)}, nil
}

func (s *Server) ApproveChangeRequest(ctx context.Context, r *adminv1.ApproveChangeRequestRequest) (*adminv1.ApproveChangeRequestResponse, error) {
	sub, err := s.admin(ctx)
	if err != nil {
		return nil, err
	}
	if !chgPattern.MatchString(r.GetChgId()) {
		return nil, status.Error(codes.NotFound, "变更单不存在")
	}
	var c Change
	var v Version
	err = s.Store.InTx(ctx, func(tx Tx) error {
		if c, err = tx.LockChange(ctx, r.GetChgId()); err != nil {
			return err
		}
		switch {
		case c.Status != "open":
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "变更单已经处理过")
		case c.Author == sub:
			return grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "作者不能审批自己的变更单")
		}
		if v, err = tx.Version(ctx, c.VersionID); err != nil {
			return err
		}
		now := s.Now()
		c.DecidedBy, c.DecidedAt = sub, &now
		rel := Release{ID: newID("rel_"), Object: c.Object, Scope: c.Scope, VersionID: c.VersionID, FromVersionID: c.BaseVersionID,
			Source: "change_request", SourceRef: c.ID, GateID: c.GateID, Reason: c.Reason, RequestedBy: c.Author, RequestedAt: c.CreatedAt,
			ApprovedBy: sub, ApprovedAt: &now}
		ok, err := activate(ctx, tx, &rel)
		if err != nil {
			return err
		}
		if !ok {
			// 生效版本已经变了：不自动合并，作者基于最新版本重新提交（governance 详设 8.3）
			c.Status = "stale"
			return tx.UpdateChange(ctx, c)
		}
		if err := tx.InsertRelease(ctx, rel); err != nil {
			return err
		}
		c.Status, c.ReleaseID = "approved", rel.ID
		if err := tx.UpdateChange(ctx, c); err != nil {
			return err
		}
		return s.Audit.Write(ctx, "config.release", rel.ID, map[string]any{"object": rel.Object, "scope": rel.Scope,
			"from_version_id": rel.FromVersionID, "version_id": rel.VersionID, "change_request": c.ID, "approved_by": sub})
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	if c.Status == "stale" {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "stale_base_version", "生效版本已经变了，变更单作废，请基于最新版本重新提交")
	}
	s.Log.InfoContext(ctx, "配置变更已生效", slog.String("chg_id", c.ID), slog.String("scope", c.Scope), slog.String("release_id", c.ReleaseID))
	return &adminv1.ApproveChangeRequestResponse{ChangeRequest: c.proto(&v)}, nil
}

func (s *Server) RejectChangeRequest(ctx context.Context, r *adminv1.RejectChangeRequestRequest) (*adminv1.RejectChangeRequestResponse, error) {
	sub, err := s.admin(ctx)
	if err != nil {
		return nil, err
	}
	reason, ok := text(r.GetReason(), 500)
	if !chgPattern.MatchString(r.GetChgId()) || !ok {
		return nil, status.Error(codes.InvalidArgument, "chg_id 或 reason 不合法")
	}
	var c Change
	var v Version
	err = s.Store.InTx(ctx, func(tx Tx) error {
		if c, err = tx.LockChange(ctx, r.GetChgId()); err != nil {
			return err
		}
		if c.Status != "open" {
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "变更单已经处理过")
		}
		if v, err = tx.Version(ctx, c.VersionID); err != nil {
			return err
		}
		now := s.Now()
		// 作者撤回自己的变更单也走这里（驳回不会放宽任何东西，不需要另一个人）
		c.Status, c.DecidedBy, c.DecidedAt, c.RejectReason = "rejected", sub, &now, reason
		if err := tx.UpdateChange(ctx, c); err != nil {
			return err
		}
		return s.Audit.Write(ctx, "config.change_request_rejected", c.ID, map[string]any{"reason": reason})
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &adminv1.RejectChangeRequestResponse{ChangeRequest: c.proto(&v)}, nil
}

func (s *Server) CreateRelease(ctx context.Context, r *adminv1.CreateReleaseRequest) (*adminv1.CreateReleaseResponse, error) {
	sub, err := s.admin(ctx)
	if err != nil {
		return nil, err
	}
	reason, ok := text(r.GetReason(), 500)
	if !ok || !refPattern.MatchString(r.GetSourceRef()) {
		return nil, status.Error(codes.InvalidArgument, "reason 必填，source_ref 须为 git:<提交>")
	}
	var rel Release
	var v Version
	err = s.Store.InTx(ctx, func(tx Tx) error {
		cur, err := tx.LockPointer(ctx, r.GetObject(), r.GetScope())
		if err != nil {
			return err
		}
		old, next, err := s.validate(ctx, tx, r.GetObject(), r.GetScope(), r.GetContent())
		if err != nil {
			return err
		}
		if err := s.checkGate(ctx, old, next, r.GetGateId()); err != nil {
			return err
		}
		now := s.Now()
		if v, err = newVersion(ctx, tx, r.GetObject(), r.GetScope(), r.GetContent(), sub, r.GetSourceRef(), now); err != nil {
			return err
		}
		rel = Release{ID: newID("rel_"), Object: v.Object, Scope: v.Scope, VersionID: v.ID, FromVersionID: cur, Source: "ci",
			SourceRef: r.GetSourceRef(), GateID: r.GetGateId(), Status: "pending_approval", Reason: reason, RequestedBy: sub, RequestedAt: now}
		if err := tx.InsertRelease(ctx, rel); err != nil {
			return err
		}
		return s.Audit.Write(ctx, "config.release_requested", rel.ID, map[string]any{"object": rel.Object, "scope": rel.Scope,
			"version_id": v.ID, "content_hash": v.ContentHash, "source_ref": rel.SourceRef, "gate_id": rel.GateID})
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &adminv1.CreateReleaseResponse{Release: rel.proto(&v)}, nil
}

func (s *Server) ApproveRelease(ctx context.Context, r *adminv1.ApproveReleaseRequest) (*adminv1.ApproveReleaseResponse, error) {
	sub, err := s.admin(ctx)
	if err != nil {
		return nil, err
	}
	if !relPattern.MatchString(r.GetReleaseId()) {
		return nil, status.Error(codes.NotFound, "发布不存在")
	}
	var rel Release
	var v Version
	err = s.Store.InTx(ctx, func(tx Tx) error {
		if rel, err = tx.LockRelease(ctx, r.GetReleaseId()); err != nil {
			return err
		}
		switch {
		case rel.Status != "pending_approval":
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "发布不是待审批状态")
		case rel.RequestedBy == sub:
			return grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "作者不能审批自己的发布")
		}
		if v, err = tx.Version(ctx, rel.VersionID); err != nil {
			return err
		}
		now := s.Now()
		rel.ApprovedBy, rel.ApprovedAt = sub, &now
		ok, err := activate(ctx, tx, &rel)
		if err != nil {
			return err
		}
		if !ok {
			rel.Status = "stale"
		}
		if err := tx.UpdateRelease(ctx, rel); err != nil {
			return err
		}
		return s.Audit.Write(ctx, "config.release", rel.ID, map[string]any{"object": rel.Object, "scope": rel.Scope,
			"from_version_id": rel.FromVersionID, "version_id": rel.VersionID, "status": rel.Status, "approved_by": sub})
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	if rel.Status == "stale" {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "stale_base_version", "生效版本已经变了，发布作废，请重新发起")
	}
	return &adminv1.ApproveReleaseResponse{Release: rel.proto(&v)}, nil
}

func (s *Server) GetRelease(ctx context.Context, r *adminv1.GetReleaseRequest) (*adminv1.GetReleaseResponse, error) {
	if _, err := s.admin(ctx); err != nil {
		return nil, err
	}
	if !relPattern.MatchString(r.GetReleaseId()) {
		return nil, status.Error(codes.NotFound, "发布不存在")
	}
	var rel Release
	var v Version
	err := s.Store.InTx(ctx, func(tx Tx) error {
		var err error
		if rel, err = tx.LockRelease(ctx, r.GetReleaseId()); err != nil {
			return err
		}
		v, err = tx.Version(ctx, rel.VersionID)
		return err
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &adminv1.GetReleaseResponse{Release: rel.proto(&v)}, nil
}

// RollbackRelease 切回该发布之前的生效版本。只能回退当前生效的那次发布；新建对象的第一次发布没有可回退的版本。
func (s *Server) RollbackRelease(ctx context.Context, r *adminv1.RollbackReleaseRequest) (*adminv1.RollbackReleaseResponse, error) {
	sub, err := s.admin(ctx)
	if err != nil {
		return nil, err
	}
	reason, ok := text(r.GetReason(), 500)
	if !relPattern.MatchString(r.GetReleaseId()) || !ok {
		return nil, status.Error(codes.InvalidArgument, "release_id 或 reason 不合法")
	}
	var back Release
	var v Version
	err = s.Store.InTx(ctx, func(tx Tx) error {
		orig, err := tx.LockRelease(ctx, r.GetReleaseId())
		if err != nil {
			return err
		}
		switch {
		case orig.Status != "active":
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "只能回退当前生效的发布")
		case orig.FromVersionID == "":
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "这是该对象的第一个版本，没有可回退的版本")
		}
		if v, err = tx.Version(ctx, orig.FromVersionID); err != nil {
			return err
		}
		now := s.Now()
		back = Release{ID: newID("rel_"), Object: orig.Object, Scope: orig.Scope, VersionID: orig.FromVersionID,
			FromVersionID: orig.VersionID, Source: "rollback", SourceRef: orig.ID, Reason: reason, RequestedBy: sub, RequestedAt: now,
			ApprovedBy: sub, ApprovedAt: &now}
		ok, err := activate(ctx, tx, &back)
		if err != nil {
			return err
		}
		if !ok {
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "生效版本已经变了")
		}
		// activate 把 orig 标成了 superseded，回退的要标为 rolled_back
		orig.Status, orig.RolledBackBy, orig.RolledBackAt = "rolled_back", sub, &now
		if err := tx.UpdateRelease(ctx, orig); err != nil {
			return err
		}
		if err := tx.InsertRelease(ctx, back); err != nil {
			return err
		}
		return s.Audit.Write(ctx, "config.rollback", back.ID, map[string]any{"object": back.Object, "scope": back.Scope,
			"rolled_back": orig.ID, "to_version_id": back.VersionID, "reason": reason})
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	s.Log.WarnContext(ctx, "配置已回退", slog.String("release_id", back.ID), slog.String("scope", back.Scope), slog.String("by", sub))
	return &adminv1.RollbackReleaseResponse{Release: back.proto(&v)}, nil
}

func (s *Server) ListTenants(ctx context.Context, _ *adminv1.ListTenantsRequest) (*adminv1.ListTenantsResponse, error) {
	if _, err := s.admin(ctx); err != nil {
		return nil, err
	}
	out := &adminv1.ListTenantsResponse{}
	err := s.Store.InTx(ctx, func(tx Tx) error {
		active, err := tx.Active(ctx, ObjectTenant)
		if err != nil {
			return err
		}
		for _, v := range active {
			sum := &adminv1.TenantSummary{Tenant: v.Scope, Active: v.proto(true)}
			if t, err := tenancy.Parse(v.Scope, []byte(v.Content)); err == nil {
				sum.Rollout = t.Rollout
			}
			out.Tenants = append(out.Tenants, sum)
		}
		return nil
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return out, nil
}

func (s *Server) ListConfigVersions(ctx context.Context, r *adminv1.ListConfigVersionsRequest) (*adminv1.ListConfigVersionsResponse, error) {
	if _, err := s.admin(ctx); err != nil {
		return nil, err
	}
	if r.GetObject() != ObjectTenant {
		return nil, status.Error(codes.InvalidArgument, "object 只能是 tenant")
	}
	out := &adminv1.ListConfigVersionsResponse{}
	err := s.Store.InTx(ctx, func(tx Tx) error {
		cur, err := tx.LockPointer(ctx, r.GetObject(), r.GetScope())
		if err != nil {
			return err
		}
		vs, err := tx.Versions(ctx, r.GetObject(), r.GetScope())
		if err != nil {
			return err
		}
		if len(vs) == 0 {
			return ErrNotFound
		}
		for _, v := range vs {
			out.Versions = append(out.Versions, v.proto(v.ID == cur))
		}
		return nil
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return out, nil
}

// Bootstrap 为还没有任何生效版本的租户导入部署目录里的 tenant.yaml 作为初始版本（来源记 bootstrap），
// 已有版本的租户不动：之后的修改只走变更单与发布。全部内容放在一起先校验，任何一份不合法就都不导入。
func (s *Server) Bootstrap(ctx context.Context, files map[string][]byte) (int, error) {
	n := 0
	err := s.Store.InTx(ctx, func(tx Tx) error {
		active, err := tx.Active(ctx, ObjectTenant)
		if err != nil {
			return err
		}
		have := map[string]bool{}
		var all []tenancy.Tenant
		for _, v := range active {
			have[v.Scope] = true
			t, err := tenancy.Parse(v.Scope, []byte(v.Content))
			if err != nil {
				return err
			}
			all = append(all, t)
		}
		var names []string
		for name, b := range files {
			if have[name] {
				continue
			}
			t, err := tenancy.Parse(name, b)
			if err != nil {
				return err
			}
			all, names = append(all, t), append(names, name)
		}
		if _, err := tenancy.New(all, s.PlatformClients); err != nil {
			return err
		}
		slices.Sort(names)
		now := s.Now()
		for _, name := range names {
			if _, err := tx.LockPointer(ctx, ObjectTenant, name); err != nil {
				return err
			}
			v, err := newVersion(ctx, tx, ObjectTenant, name, string(files[name]), "bootstrap", "bootstrap:"+name+"/tenant.yaml", now)
			if err != nil {
				return err
			}
			rel := Release{ID: newID("rel_"), Object: ObjectTenant, Scope: name, VersionID: v.ID, Source: "bootstrap",
				SourceRef: v.SourceRef, Status: "active", Reason: "部署目录里的初始配置", RequestedBy: "bootstrap", RequestedAt: now}
			if err := tx.SetPointer(ctx, ObjectTenant, name, v.ID); err != nil {
				return err
			}
			if err := tx.InsertRelease(ctx, rel); err != nil {
				return err
			}
			if err := s.Audit.Write(ctx, "config.bootstrap", rel.ID, map[string]any{"scope": name, "version_id": v.ID,
				"content_hash": v.ContentHash}); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

// Snapshot 是 gateway 快照里的租户配置与它们的摘要（并入 snapshot_version）。
func (s *Server) Snapshot(ctx context.Context) ([]*configv1.TenantConfig, string, error) {
	var out []*configv1.TenantConfig
	err := s.Store.InTx(ctx, func(tx Tx) error {
		active, err := tx.Active(ctx, ObjectTenant)
		if err != nil {
			return err
		}
		for _, v := range active {
			out = append(out, &configv1.TenantConfig{Tenant: v.Scope, VersionId: v.ID, Content: v.Content})
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	slices.SortFunc(out, func(a, b *configv1.TenantConfig) int { return strings.Compare(a.GetTenant(), b.GetTenant()) })
	ids := make([]string, len(out))
	for i, t := range out {
		ids[i] = t.GetVersionId()
	}
	return out, hash(strings.Join(ids, ","))[:16], nil
}

func ts(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func (v Version) proto(active bool) *adminv1.ConfigVersion {
	return &adminv1.ConfigVersion{VersionId: v.ID, Object: v.Object, Scope: v.Scope, Number: v.Number, Content: v.Content,
		ContentHash: v.ContentHash, CreatedBy: v.CreatedBy, CreatedAt: timestamppb.New(v.CreatedAt), SourceRef: v.SourceRef, Active: active}
}

func (c Change) proto(v *Version) *adminv1.ChangeRequest {
	return &adminv1.ChangeRequest{ChgId: c.ID, Object: c.Object, Scope: c.Scope, BaseVersionId: c.BaseVersionID,
		Version: v.proto(c.Status == "approved"), Reason: c.Reason, GateId: c.GateID, Author: c.Author, CreatedAt: timestamppb.New(c.CreatedAt),
		Status: c.Status, DecidedBy: c.DecidedBy, DecidedAt: ts(c.DecidedAt), RejectReason: c.RejectReason, ReleaseId: c.ReleaseID}
}

func (r Release) proto(v *Version) *adminv1.Release {
	return &adminv1.Release{ReleaseId: r.ID, Object: r.Object, Scope: r.Scope, Version: v.proto(r.Status == "active"),
		FromVersionId: r.FromVersionID, Source: r.Source, SourceRef: r.SourceRef, GateId: r.GateID, Status: r.Status,
		RequestedBy: r.RequestedBy, RequestedAt: timestamppb.New(r.RequestedAt), ApprovedBy: r.ApprovedBy, ApprovedAt: ts(r.ApprovedAt),
		RolledBackBy: r.RolledBackBy, RolledBackAt: ts(r.RolledBackAt), Reason: r.Reason}
}
