// Package config 是配置对象的版本、变更单与发布（governance 详设 3、4、7.2、8.3）。
//
// 配置对象有租户配置（object = tenant，scope = 租户名，内容是 tenant.yaml，下发给 gateway）、pipeline（scope = 租户名，
// 下发给 retrieval 与 orchestrator）与 routing（scope = global，下发给 orchestrator），见 objects.go。每次修改产生一个
// 不可变版本，每个对象在每个作用域只有一个生效指针，只有发布能改它；生效后经 ConfigService 推给使用方（vocab.Server）。
// 影响回答的对象（pipeline、routing、prompt）与策略包的发布先过评测门禁：governance 用候选版本发起（gate.go），
// 通过后才进入审批（governance 详设 4.1 第 ③④ 步、4.2）。
//
//   - 变更单（console 发起，带 base_version）：作者以外的平台管理员审批后生成发布并立即生效；审批时生效版本已不是
//     base 则变更单作废为 stale，要求基于最新版本重新提交，不自动合并（governance 详设 8.3）。
//   - 发布（CI 发起，带 source_ref）：作者以外的平台管理员审批后生效。
//   - 回退：一人执行，切回该发布之前的生效版本（governance 详设 4.2：回退不再过门禁）。
//   - 租户从试点切到全量（或新建即为全量）要附一个已通过、覆盖该租户的评测门禁（evaluation 详设 3.1）。
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

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/tenancy"
	"github.com/hei-/bambuflow/services/governance/internal/forge"
)

// RolePlatformAdmin 发起与审批配置变更（governance 详设 9.1；设计里 pipeline 由租户负责人审批，平台角色里还没有）。
const RolePlatformAdmin = "platform_admin"

// 信息安全与数据属主：策略层的发布要两位审批人，至少一位是其中之一（详设 4.2、9.1）
const (
	RoleInfoSec   = "infosec"
	RoleDataOwner = "data_owner"
)

// ApprovalTimeout：待审批的发布与变更单超过它自动关闭（详设 4.1）
const ApprovalTimeout = 7 * 24 * time.Hour

// ObjectTenant 是租户配置。
const ObjectTenant = "tenant"

const maxContent = 64 << 10

var (
	ErrNotFound = errors.New("不存在")
	// ErrBusy：同一对象已有待审批的变更单
	ErrBusy = errors.New("该对象已有待审批的变更单")

	chgPattern     = regexp.MustCompile(`^chg_[0-9a-f]{24}$`)
	relPattern     = regexp.MustCompile(`^rel_[0-9a-f]{24}$`)
	versionPattern = regexp.MustCompile(`^cfv_[0-9a-f]{24}$`)
	gatePattern    = regexp.MustCompile(`^gate_[0-9a-f]{24}$`)
	refPattern     = regexp.MustCompile(`^git:[0-9a-f]{7,64}$`)
)

type Version struct {
	ID, Object, Scope, Content, ContentHash, CreatedBy, SourceRef string
	Number                                                        int32
	CreatedAt                                                     time.Time
	// 策略层的差异分析：生效后可见范围变大的代表性主体（objects.go checkPolicy）
	Analysis []string
}

type Release struct {
	ID, Object, Scope, VersionID, FromVersionID, Source, SourceRef, GateID, Status, Reason string
	RequestedBy, ApprovedBy, RolledBackBy                                                  string
	RequestedAt                                                                            time.Time
	ApprovedAt, RolledBackAt                                                               *time.Time
	// 策略层要两位审批人（详设 4.2）：第二位与时间；SecurityApproved 记两位里是否已有信息安全或数据属主
	SecondApprovedBy string
	SecondApprovedAt *time.Time
	SecurityApproved bool
	// 全局对象灰度的试点租户（CI 发布时指定）；Rollout 是灰度中的范围，只在读出时填
	CanaryTenant string
	Rollout      *Rollout
	// 门禁发起的时间（gate.go）；门禁按它判断超时
	GateStartedAt *time.Time
}

type Change struct {
	ID, Object, Scope, BaseVersionID, VersionID, Reason, GateID, Author, Status string
	DecidedBy, RejectReason, ReleaseID, MergeRequestURL                         string
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
	// Changes、Releases 按状态列出（status 为空不限），新的在前，最多 limit 条
	Changes(ctx context.Context, status string, limit int) ([]Change, error)
	Releases(ctx context.Context, status string, limit int) ([]Release, error)
	// ReleasesOf 是某个版本的全部发布（实验变体核对门禁用，variant.go）
	ReleasesOf(ctx context.Context, versionID string) ([]Release, error)
	// ExpireBefore 把 before 之前发起、仍待审批的发布与变更单记为 expired，返回它们的 ID
	ExpireBefore(ctx context.Context, before time.Time) (releases, changes []string, err error)
	// 灰度（rollout.go）：LockRollout 锁住并返回作用域的灰度，没有时 ok 为 false；InsertRollout 遇到同一作用域已有灰度时返回 ErrBusy
	LockRollout(ctx context.Context, object, scope string) (r Rollout, ok bool, err error)
	InsertRollout(ctx context.Context, r Rollout) error
	DeleteRollout(ctx context.Context, object, scope string) error
	// ObserveRollout 记下 evaluation 最近一次观察的结果
	ObserveRollout(ctx context.Context, object, scope, observation string, at time.Time) error
	Rollouts(ctx context.Context) ([]Rollout, error)
}

type Store interface {
	InTx(ctx context.Context, fn func(Tx) error) error
}

type Auditor interface {
	Write(ctx context.Context, action, objectID string, detail any) error
}

// GateChecker 核对评测门禁已通过且覆盖该租户（evaluation 的 EvalGateAdminService）；stage 不为空时还要求门禁里
// 这个租户的评测集已满足该阶段（evaluation 详设 3.1 分阶段接入）。
type GateChecker func(ctx context.Context, gateID, tenant, stage string) error

// ErrGateFailed：门禁不存在、没通过或不覆盖该租户。
var ErrGateFailed = errors.New("评测门禁不满足")

type Server struct {
	adminv1.UnimplementedConfigAdminServiceServer
	Store Store
	Audit Auditor
	Gate  GateChecker
	// Evals 是 evaluation 的 EvalService：配置发布的门禁由这里发起（gate.go）
	Evals EvalGates
	// Forge 把 console 对 Git 类对象的修改写成合并请求（internal/forge）；nil 时 console 不能修改 Git 类对象
	Forge forge.Forge
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

// approver 是能审批与查看发布的人：平台管理员、信息安全、数据属主（后两者只能审批策略层，见 ApproveRelease），
// 以及租户负责人（只限自己负责的租户的 pipeline，详设 3.1、9.1）。
type approver struct {
	sub string
	// 调用者是信息安全或数据属主
	security bool
	// 只是租户负责人时为负责的租户，否则为 nil（不限）
	owned []string
}

// may 判断调用者能否看到、审批这个发布。
func (a approver) may(rel Release) bool {
	return a.owned == nil || (rel.Object == ObjectPipeline && slices.Contains(a.owned, rel.Scope))
}

func (s *Server) approver(ctx context.Context) (approver, error) {
	p := grpcmw.PrincipalFrom(ctx)
	a := approver{sub: p.GetSub(), security: grpcmw.HasRole(p, RoleInfoSec, RoleDataOwner)}
	if a.security || grpcmw.HasRole(p, RolePlatformAdmin) {
		return a, nil
	}
	for _, r := range p.GetRoles() {
		if t, ok := strings.CutPrefix(r, ownerRolePrefix); ok {
			a.owned = append(a.owned, t)
		}
	}
	if len(a.owned) == 0 {
		return approver{}, status.Error(codes.PermissionDenied, "需要平台管理员、信息安全、数据属主或租户负责人")
	}
	return a, nil
}

// scoped 读出发布并核对调用者能否操作它；不能时按不存在处理（不让租户负责人探测别的租户的发布）。
func (s *Server) scoped(ctx context.Context, a approver, id string) error {
	if !relPattern.MatchString(id) {
		return status.Error(codes.NotFound, "发布不存在")
	}
	return s.Store.InTx(ctx, func(tx Tx) error {
		rel, err := tx.LockRelease(ctx, id)
		if err != nil {
			return err
		}
		if !a.may(rel) {
			return ErrNotFound
		}
		return nil
	})
}

func (s *Server) admin(ctx context.Context) (string, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RolePlatformAdmin) {
		return "", status.Error(codes.PermissionDenied, "需要平台管理员")
	}
	return p.GetSub(), nil
}

// ownerRolePrefix：租户负责人在管理 Principal 里的角色是 tenant_owner:<租户>（gateway 按租户配置的 owner_groups 给出）
const ownerRolePrefix = "tenant_owner:"

// reader 是只读接口的权限：平台管理员看全部（all），租户负责人只看自己负责的租户（owned）。
func (s *Server) reader(ctx context.Context) (all bool, owned []string, err error) {
	p := grpcmw.PrincipalFrom(ctx)
	if grpcmw.HasRole(p, RolePlatformAdmin) {
		return true, nil, nil
	}
	for _, r := range p.GetRoles() {
		if t, ok := strings.CutPrefix(r, ownerRolePrefix); ok {
			owned = append(owned, t)
		}
	}
	if len(owned) == 0 {
		return false, nil, status.Error(codes.PermissionDenied, "需要平台管理员或租户负责人")
	}
	return false, owned, nil
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

// check 是变更单与 CI 发布共用的校验：对象内容与作用域合法；租户配置切到全量时核对附带的评测门禁。
// 其他对象的门禁在发布之后由 governance 用候选版本发起（gate.go），这里不核对。
// 返回策略层的差异分析（其他对象为空），存进新版本供审批时查看。
func (s *Server) check(ctx context.Context, tx Tx, object, scope, content, gateID string) ([]string, error) {
	if _, ok := objects[object]; !ok {
		return nil, status.Error(codes.InvalidArgument, "object 只能是 tenant、pipeline、routing、policy 或 prompt")
	}
	if content == "" || len(content) > maxContent {
		return nil, status.Error(codes.InvalidArgument, "content 为空或超过 64 KB")
	}
	if object == ObjectTenant {
		old, next, err := s.validate(ctx, tx, object, scope, content)
		if err != nil {
			return nil, err
		}
		return nil, s.checkGate(ctx, old, next, gateID)
	}
	var analysis []string
	var err error
	if object == ObjectPolicy {
		analysis, err = checkPolicy(ctx, tx, scope, content)
	} else {
		err = checkObject(ctx, tx, object, scope, content)
	}
	if err != nil {
		return nil, err
	}
	return analysis, nil
}

// checkGate：租户切到全量（或新建即为全量）时，必须附已通过、覆盖该租户的门禁，且门禁里这个租户的评测集
// 已满足全量的要求（≥ 300 题、双人标注 ≥ 150 等，evaluation 详设 3.1）。不是切全量但附了门禁时只核对通过与覆盖。
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
	stage := ""
	if widening {
		stage = "general"
	}
	return s.Gate(ctx, gateID, next.Name, stage)
}

// newVersion 在事务里写一个新版本。
func newVersion(ctx context.Context, tx Tx, object, scope, content, by, ref string, now time.Time, analysis []string) (Version, error) {
	n, err := tx.NextNumber(ctx, object, scope)
	if err != nil {
		return Version{}, err
	}
	v := Version{ID: newID("cfv_"), Object: object, Scope: scope, Number: n, Content: content, ContentHash: hash(content),
		CreatedBy: by, CreatedAt: now, SourceRef: ref, Analysis: analysis}
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
		// Git 类对象写成合并请求，合并后由 CI 发布，发布后再过门禁；这里只校验内容
		git := gitObject(r.GetObject())
		analysis, err := s.check(ctx, tx, r.GetObject(), r.GetScope(), r.GetContent(), r.GetGateId())
		if err != nil {
			return err
		}
		now := s.Now()
		c = Change{ID: newID("chg_"), Object: r.GetObject(), Scope: r.GetScope(), BaseVersionID: cur, Reason: reason,
			GateID: r.GetGateId(), Author: sub, CreatedAt: now, Status: "open"}
		if v, err = newVersion(ctx, tx, c.Object, c.Scope, r.GetContent(), sub, c.ID, now, analysis); err != nil {
			return err
		}
		c.VersionID = v.ID
		if git {
			if c.MergeRequestURL, err = s.openMergeRequest(ctx, c, v); err != nil {
				return err
			}
			c.Status = "merge_request"
		}
		if err := tx.InsertChange(ctx, c); err != nil {
			return err
		}
		return s.Audit.Write(ctx, "config.change_request", c.ID, map[string]any{"object": c.Object, "scope": c.Scope,
			"base_version_id": c.BaseVersionID, "version_id": v.ID, "content_hash": v.ContentHash, "gate_id": c.GateID,
			"merge_request_url": c.MergeRequestURL})
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
		case c.Status == "merge_request":
			return grpcmw.Reason(codes.FailedPrecondition, "merge_request", "这个变更在合并请求里评审："+c.MergeRequestURL+"；合并后由 CI 发布")
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
		analysis, err := s.check(ctx, tx, r.GetObject(), r.GetScope(), r.GetContent(), r.GetGateId())
		if err != nil {
			return err
		}
		now := s.Now()
		if v, err = newVersion(ctx, tx, r.GetObject(), r.GetScope(), r.GetContent(), sub, r.GetSourceRef(), now, analysis); err != nil {
			return err
		}
		rel = Release{ID: newID("rel_"), Object: v.Object, Scope: v.Scope, VersionID: v.ID, FromVersionID: cur, Source: "ci",
			SourceRef: r.GetSourceRef(), GateID: r.GetGateId(), Status: "pending_approval", Reason: reason, RequestedBy: sub, RequestedAt: now,
			CanaryTenant: r.GetCanaryTenant()}
		// 影响回答的对象与策略包先过门禁（gate.go 发起），请求里附的门禁评的不是这个候选，不用
		if objects[rel.Object].needsGate {
			rel.Status, rel.GateID = "gating", ""
		}
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

// ApproveRelease 审批 CI 发起的发布。策略层要两位不同的人（作者不算），至少一位是信息安全或数据属主：第一位审批后
// 状态为 pending_second_approval，第二位审批后生效；其他对象由作者以外的一位平台管理员审批，
// pipeline 也可以由该租户的负责人审批（详设 3.1、4.2、9.1）。
func (s *Server) ApproveRelease(ctx context.Context, r *adminv1.ApproveReleaseRequest) (*adminv1.ApproveReleaseResponse, error) {
	a, err := s.approver(ctx)
	if err != nil {
		return nil, err
	}
	sub, security := a.sub, a.security
	if !relPattern.MatchString(r.GetReleaseId()) {
		return nil, status.Error(codes.NotFound, "发布不存在")
	}
	var rel Release
	var v Version
	err = s.Store.InTx(ctx, func(tx Tx) error {
		if rel, err = tx.LockRelease(ctx, r.GetReleaseId()); err != nil {
			return err
		}
		dual := rel.Object == ObjectPolicy
		switch {
		case !a.may(rel):
			return ErrNotFound
		case rel.Status != "pending_approval" && rel.Status != "pending_second_approval":
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "发布不是待审批状态")
		case rel.RequestedBy == sub:
			return grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "作者不能审批自己的发布")
		case !dual && a.owned == nil && !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), RolePlatformAdmin):
			return status.Error(codes.PermissionDenied, "只有平台管理员（pipeline 还有该租户的负责人）能审批这个对象的发布")
		case rel.Status == "pending_second_approval" && rel.ApprovedBy == sub:
			return grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "第二位审批人须是另一个人")
		case dual && rel.Status == "pending_second_approval" && !rel.SecurityApproved && !security:
			return grpcmw.Reason(codes.PermissionDenied, "security_approval_required",
				"策略包的两位审批人里至少要有一位信息安全或数据属主")
		}
		if v, err = tx.Version(ctx, rel.VersionID); err != nil {
			return err
		}
		now := s.Now()
		if dual && rel.Status == "pending_approval" {
			rel.ApprovedBy, rel.ApprovedAt, rel.SecurityApproved, rel.Status = sub, &now, security, "pending_second_approval"
			if err := tx.UpdateRelease(ctx, rel); err != nil {
				return err
			}
			return s.Audit.Write(ctx, "config.release_first_approval", rel.ID, map[string]any{"object": rel.Object,
				"scope": rel.Scope, "version_id": rel.VersionID, "approved_by": sub, "security": security})
		}
		if rel.Status == "pending_second_approval" {
			rel.SecondApprovedBy, rel.SecondApprovedAt, rel.SecurityApproved = sub, &now, rel.SecurityApproved || security
		} else {
			rel.ApprovedBy, rel.ApprovedAt = sub, &now
		}
		// Git 类对象先进灰度（rollout.go），全量后才切生效指针
		var ok bool
		if grayed(rel) {
			ok, err = startRollout(ctx, tx, &rel, now)
		} else {
			ok, err = activate(ctx, tx, &rel)
		}
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
			"from_version_id": rel.FromVersionID, "version_id": rel.VersionID, "status": rel.Status, "approved_by": rel.ApprovedBy,
			"second_approved_by": rel.SecondApprovedBy})
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
	a, err := s.approver(ctx)
	if err != nil {
		return nil, err
	}
	if !relPattern.MatchString(r.GetReleaseId()) {
		return nil, status.Error(codes.NotFound, "发布不存在")
	}
	var rel Release
	var v Version
	err = s.Store.InTx(ctx, func(tx Tx) error {
		var err error
		if rel, err = tx.LockRelease(ctx, r.GetReleaseId()); err != nil {
			return err
		}
		if !a.may(rel) {
			return ErrNotFound
		}
		if v, err = tx.Version(ctx, rel.VersionID); err != nil {
			return err
		}
		return withRollout(ctx, tx, &rel)
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &adminv1.GetReleaseResponse{Release: rel.proto(&v)}, nil
}

const listLimit = 100

var (
	changeStatuses  = []string{"open", "approved", "rejected", "stale", "merge_request", "expired"}
	releaseStatuses = []string{"gating", "gate_failed", "pending_approval", "pending_second_approval", "active", "superseded", "rolled_back", "rejected",
		"stale", "expired"}
)

// ListChangeRequests 按状态列出变更单，审批人据此找待审批的（不必由发起人转交 ID）。
func (s *Server) ListChangeRequests(ctx context.Context, r *adminv1.ListChangeRequestsRequest) (*adminv1.ListChangeRequestsResponse, error) {
	if _, err := s.admin(ctx); err != nil {
		return nil, err
	}
	if r.GetStatus() != "" && !slices.Contains(changeStatuses, r.GetStatus()) {
		return nil, status.Error(codes.InvalidArgument, "status 不合法")
	}
	out := &adminv1.ListChangeRequestsResponse{}
	err := s.Store.InTx(ctx, func(tx Tx) error {
		cs, err := tx.Changes(ctx, r.GetStatus(), listLimit)
		if err != nil {
			return err
		}
		for _, c := range cs {
			v, err := tx.Version(ctx, c.VersionID)
			if err != nil {
				return err
			}
			out.ChangeRequests = append(out.ChangeRequests, c.proto(&v))
		}
		return nil
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return out, nil
}

// ListReleases 按状态列出发布（如待审批的 CI 发布）。
func (s *Server) ListReleases(ctx context.Context, r *adminv1.ListReleasesRequest) (*adminv1.ListReleasesResponse, error) {
	a, err := s.approver(ctx)
	if err != nil {
		return nil, err
	}
	if r.GetStatus() != "" && !slices.Contains(releaseStatuses, r.GetStatus()) {
		return nil, status.Error(codes.InvalidArgument, "status 不合法")
	}
	out := &adminv1.ListReleasesResponse{}
	err = s.Store.InTx(ctx, func(tx Tx) error {
		rs, err := tx.Releases(ctx, r.GetStatus(), listLimit)
		if err != nil {
			return err
		}
		for _, rel := range rs {
			if !a.may(rel) {
				continue
			}
			v, err := tx.Version(ctx, rel.VersionID)
			if err != nil {
				return err
			}
			if err := withRollout(ctx, tx, &rel); err != nil {
				return err
			}
			out.Releases = append(out.Releases, rel.proto(&v))
		}
		return nil
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return out, nil
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
		// 回退可以插队：同一对象正在进行的灰度一并终止（详设 8.3）
		if ro, rolling, err := tx.LockRollout(ctx, orig.Object, orig.Scope); err != nil {
			return err
		} else if rolling {
			canary, err := tx.LockRelease(ctx, ro.ReleaseID)
			if err != nil {
				return err
			}
			if err := s.endRollout(ctx, tx, &canary, sub, "回退生效版本，终止灰度："+reason); err != nil {
				return err
			}
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
	all, owned, err := s.reader(ctx)
	if err != nil {
		return nil, err
	}
	out := &adminv1.ListTenantsResponse{}
	err = s.Store.InTx(ctx, func(tx Tx) error {
		active, err := tx.Active(ctx, ObjectTenant)
		if err != nil {
			return err
		}
		for _, v := range active {
			if !all && !slices.Contains(owned, v.Scope) {
				continue
			}
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
	all, owned, err := s.reader(ctx)
	if err != nil {
		return nil, err
	}
	if !all && !slices.Contains(owned, r.GetScope()) {
		return nil, status.Error(codes.PermissionDenied, "只能查看自己负责的租户")
	}
	if _, ok := objects[r.GetObject()]; !ok {
		return nil, status.Error(codes.InvalidArgument, "object 只能是 tenant、pipeline、routing、policy 或 prompt")
	}
	out := &adminv1.ListConfigVersionsResponse{}
	err = s.Store.InTx(ctx, func(tx Tx) error {
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

// ExpireStale 关闭超过 ApprovalTimeout 仍未审批的发布与变更单（详设 4.1），每个写一条审计；返回关闭的个数。
// 由 governance 定时调用。
func (s *Server) ExpireStale(ctx context.Context) (int, error) {
	n := 0
	err := s.Store.InTx(ctx, func(tx Tx) error {
		rels, chgs, err := tx.ExpireBefore(ctx, s.Now().Add(-ApprovalTimeout))
		if err != nil {
			return err
		}
		for _, id := range rels {
			if err := s.Audit.Write(ctx, "config.release_expired", id, map[string]any{"after": ApprovalTimeout.String()}); err != nil {
				return err
			}
		}
		for _, id := range chgs {
			if err := s.Audit.Write(ctx, "config.change_request_expired", id, map[string]any{"after": ApprovalTimeout.String()}); err != nil {
				return err
			}
		}
		n = len(rels) + len(chgs)
		return nil
	})
	return n, err
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
			v, err := newVersion(ctx, tx, ObjectTenant, name, string(files[name]), "bootstrap", "bootstrap:"+name+"/tenant.yaml", now, nil)
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
		ContentHash: v.ContentHash, CreatedBy: v.CreatedBy, CreatedAt: timestamppb.New(v.CreatedAt), SourceRef: v.SourceRef, Active: active,
		Widened: v.Analysis}
}

func (c Change) proto(v *Version) *adminv1.ChangeRequest {
	return &adminv1.ChangeRequest{ChgId: c.ID, Object: c.Object, Scope: c.Scope, BaseVersionId: c.BaseVersionID,
		Version: v.proto(c.Status == "approved"), Reason: c.Reason, GateId: c.GateID, Author: c.Author, CreatedAt: timestamppb.New(c.CreatedAt),
		Status: c.Status, DecidedBy: c.DecidedBy, DecidedAt: ts(c.DecidedAt), RejectReason: c.RejectReason, ReleaseId: c.ReleaseID,
		MergeRequestUrl: c.MergeRequestURL}
}

func (r Release) rolloutProto() *adminv1.Rollout {
	if r.Rollout == nil {
		return nil
	}
	return r.Rollout.proto()
}

func (r Release) proto(v *Version) *adminv1.Release {
	return &adminv1.Release{ReleaseId: r.ID, Object: r.Object, Scope: r.Scope, Version: v.proto(r.Status == "active"),
		FromVersionId: r.FromVersionID, Source: r.Source, SourceRef: r.SourceRef, GateId: r.GateID, Status: r.Status,
		RequestedBy: r.RequestedBy, RequestedAt: timestamppb.New(r.RequestedAt), ApprovedBy: r.ApprovedBy, ApprovedAt: ts(r.ApprovedAt),
		RolledBackBy: r.RolledBackBy, RolledBackAt: ts(r.RolledBackAt), Reason: r.Reason,
		SecondApprovedBy: r.SecondApprovedBy, SecondApprovedAt: ts(r.SecondApprovedAt), CanaryTenant: r.CanaryTenant,
		Rollout: r.rolloutProto()}
}
