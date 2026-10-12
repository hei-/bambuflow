package config

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/tenancy"
)

// 灰度（governance 详设 3.2、4.1、8.3）：Git 类对象的 CI 发布审批后先进灰度，候选版本只对一部分用户生效，
// 全量后才切生效指针。租户对象（pipeline、租户策略层）对该租户 TenantCanaryPercent 的用户；全局对象（routing、回答模板、
// 全局与数据源策略层）对一个试点租户的全部用户。分流按 sha256(release_id + 用户)，同一用户在一次灰度里始终用同一个版本。
// 回退可以插队：回退生效版本时终止同一对象正在进行的灰度。初始导入与回退不走灰度。

// TenantCanaryPercent 是租户对象灰度的用户比例（详设 4.1“租户对象先对该租户 10% 的实例生效”，这里按用户分流）
const TenantCanaryPercent = 10

// Rollout 是一个灰度中的发布。
type Rollout struct {
	Object, Scope, ReleaseID, VersionID, Tenant string
	Percent                                     int32
	StartedAt                                   time.Time
	// evaluation 最近一次观察的结果（RolloutControl）
	Observation string
	ObservedAt  *time.Time
}

func grayed(r Release) bool {
	return gitObject(r.Object) && r.Source == "ci"
}

// canaryRange 定灰度范围：租户对象是该租户的一部分用户；全局对象是试点租户（发起时指定的，否则第一个试点阶段的租户，
// 都没有时第一个租户）的全部用户。
func canaryRange(ctx context.Context, tx Tx, r Release) (string, int32, error) {
	if r.Object == ObjectPipeline {
		return r.Scope, TenantCanaryPercent, nil
	}
	if t, ok := strings.CutPrefix(r.Scope, "tenant:"); ok && r.Object == ObjectPolicy {
		return t, TenantCanaryPercent, nil
	}
	active, err := tx.Active(ctx, ObjectTenant)
	if err != nil {
		return "", 0, err
	}
	var names, pilots []string
	for _, v := range active {
		names = append(names, v.Scope)
		if t, err := tenancy.Parse(v.Scope, []byte(v.Content)); err == nil && t.Rollout == "pilot" {
			pilots = append(pilots, v.Scope)
		}
	}
	switch {
	case r.CanaryTenant != "":
		if !slices.Contains(names, r.CanaryTenant) {
			return "", 0, status.Errorf(codes.FailedPrecondition, "试点租户 %s 没有生效的租户配置", r.CanaryTenant)
		}
		return r.CanaryTenant, 100, nil
	case len(pilots) > 0:
		return pilots[0], 100, nil
	case len(names) > 0:
		return names[0], 100, nil
	}
	return "", 0, status.Error(codes.FailedPrecondition, "没有可以灰度的租户")
}

// startRollout 让审批通过的发布进灰度：核对生效指针仍是发布时的版本（否则 stale），同一对象同一作用域没有别的灰度。
func startRollout(ctx context.Context, tx Tx, r *Release, now time.Time) (bool, error) {
	cur, err := tx.LockPointer(ctx, r.Object, r.Scope)
	if err != nil {
		return false, err
	}
	if cur != r.FromVersionID {
		return false, nil
	}
	if _, busy, err := tx.LockRollout(ctx, r.Object, r.Scope); err != nil || busy {
		if err == nil {
			err = grpcmw.Reason(codes.FailedPrecondition, "conflict", "这个对象已有发布在灰度中，先全量或终止它")
		}
		return false, err
	}
	tenant, percent, err := canaryRange(ctx, tx, *r)
	if err != nil {
		return false, err
	}
	ro := Rollout{Object: r.Object, Scope: r.Scope, ReleaseID: r.ID, VersionID: r.VersionID, Tenant: tenant, Percent: percent, StartedAt: now}
	if err := tx.InsertRollout(ctx, ro); err != nil {
		return false, err
	}
	r.Status, r.Rollout = "rolling_out", &ro
	return true, nil
}

// lockRolling 锁住灰度中的发布与它的灰度记录。version 非空时还要求候选版本是它（evaluation 按它观察的版本下结论）。
func lockRolling(ctx context.Context, tx Tx, id, version string) (Release, Rollout, error) {
	rel, err := tx.LockRelease(ctx, id)
	if err != nil {
		return Release{}, Rollout{}, err
	}
	if rel.Status != "rolling_out" || (version != "" && rel.VersionID != version) {
		return Release{}, Rollout{}, grpcmw.Reason(codes.FailedPrecondition, "conflict", "发布不在灰度中")
	}
	ro, ok, err := tx.LockRollout(ctx, rel.Object, rel.Scope)
	if err != nil {
		return Release{}, Rollout{}, err
	}
	if !ok || ro.ReleaseID != rel.ID {
		return Release{}, Rollout{}, fmt.Errorf("发布 %s 记为灰度中，但没有对应的灰度记录", rel.ID)
	}
	return rel, ro, nil
}

// PromoteRelease 让灰度中的发布全量生效：切生效指针，删掉灰度记录。生效版本在灰度期间被别的发布改过时作废（stale）。
func (s *Server) PromoteRelease(ctx context.Context, r *adminv1.PromoteReleaseRequest) (*adminv1.PromoteReleaseResponse, error) {
	a, err := s.approver(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.scoped(ctx, a, r.GetReleaseId()); err != nil {
		return nil, s.fail(ctx, err)
	}
	rel, v, err := s.promote(ctx, r.GetReleaseId(), "", a.sub, "")
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &adminv1.PromoteReleaseResponse{Release: rel.proto(&v)}, nil
}

// promote 全量；observation 是自动全量时 evaluation 的观察结果，写进审计。
func (s *Server) promote(ctx context.Context, id, version, by, observation string) (Release, Version, error) {
	var rel Release
	var v Version
	err := s.Store.InTx(ctx, func(tx Tx) error {
		var err error
		if rel, _, err = lockRolling(ctx, tx, id, version); err != nil {
			return err
		}
		if v, err = tx.Version(ctx, rel.VersionID); err != nil {
			return err
		}
		ok, err := activate(ctx, tx, &rel)
		if err != nil {
			return err
		}
		if !ok {
			rel.Status = "stale"
		}
		if err := tx.DeleteRollout(ctx, rel.Object, rel.Scope); err != nil {
			return err
		}
		if err := tx.UpdateRelease(ctx, rel); err != nil {
			return err
		}
		return s.Audit.Write(ctx, "config.rollout_promoted", rel.ID, map[string]any{"object": rel.Object, "scope": rel.Scope,
			"version_id": rel.VersionID, "status": rel.Status, "by": by, "observation": observation})
	})
	if err == nil && rel.Status == "stale" {
		err = grpcmw.Reason(codes.FailedPrecondition, "stale_base_version", "灰度期间生效版本已经变了，发布作废，请重新发起")
	}
	return rel, v, err
}

// AbortRollout 终止灰度：候选版本不再生效，发布记为 rolled_back。一人执行：收紧不需要第二个人。
func (s *Server) AbortRollout(ctx context.Context, r *adminv1.AbortRolloutRequest) (*adminv1.AbortRolloutResponse, error) {
	a, err := s.approver(ctx)
	if err != nil {
		return nil, err
	}
	reason, ok := text(r.GetReason(), 500)
	if !relPattern.MatchString(r.GetReleaseId()) || !ok {
		return nil, status.Error(codes.InvalidArgument, "release_id 或 reason 不合法")
	}
	if err := s.scoped(ctx, a, r.GetReleaseId()); err != nil {
		return nil, s.fail(ctx, err)
	}
	sub := a.sub
	rel, v, err := s.abort(ctx, r.GetReleaseId(), "", sub, reason)
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	s.Log.WarnContext(ctx, "灰度已终止", "release_id", rel.ID, "scope", rel.Scope, "by", sub)
	return &adminv1.AbortRolloutResponse{Release: rel.proto(&v)}, nil
}

func (s *Server) abort(ctx context.Context, id, version, by, reason string) (Release, Version, error) {
	var rel Release
	var v Version
	err := s.Store.InTx(ctx, func(tx Tx) error {
		var err error
		if rel, _, err = lockRolling(ctx, tx, id, version); err != nil {
			return err
		}
		if v, err = tx.Version(ctx, rel.VersionID); err != nil {
			return err
		}
		return s.endRollout(ctx, tx, &rel, by, reason)
	})
	return rel, v, err
}

// endRollout 删掉灰度记录，发布记为 rolled_back 并写审计。
func (s *Server) endRollout(ctx context.Context, tx Tx, rel *Release, by, reason string) error {
	if err := tx.DeleteRollout(ctx, rel.Object, rel.Scope); err != nil {
		return err
	}
	now := s.Now()
	rel.Status, rel.RolledBackBy, rel.RolledBackAt = "rolled_back", by, &now
	if err := tx.UpdateRelease(ctx, *rel); err != nil {
		return err
	}
	return s.Audit.Write(ctx, "config.rollout_aborted", rel.ID, map[string]any{"object": rel.Object, "scope": rel.Scope,
		"version_id": rel.VersionID, "by": by, "reason": reason})
}

// withRollout 给灰度中的发布填上灰度范围（读出时用）。
func withRollout(ctx context.Context, tx Tx, rel *Release) error {
	if rel.Status != "rolling_out" {
		return nil
	}
	ro, ok, err := tx.LockRollout(ctx, rel.Object, rel.Scope)
	if err == nil && ok {
		rel.Rollout = &ro
	}
	return err
}

func (r Rollout) proto() *adminv1.Rollout {
	out := &adminv1.Rollout{Tenant: r.Tenant, Percent: r.Percent, StartedAt: timestamppb.New(r.StartedAt), Observation: r.Observation}
	if r.ObservedAt != nil {
		out.ObservedAt = timestamppb.New(*r.ObservedAt)
	}
	return out
}

// Canaries 是快照里的候选版本：retrieval 拿 pipeline 与 policy，orchestrator 拿 pipeline、routing 与 prompt，
// 另拿不带候选包的 policy 灰度（只用来把提问人所在的灰度写进 why，灰度观察据此分组）。
// 返回内容的摘要（并入 snapshot_version）。
func (s *Server) Canaries(ctx context.Context, service string) ([]*configv1.Canary, string, error) {
	want := served[service]
	if service == "orchestrator" {
		want = append(slices.Clone(want), ObjectPolicy)
	}
	var out []*configv1.Canary
	err := s.Store.InTx(ctx, func(tx Tx) error {
		ros, err := tx.Rollouts(ctx)
		if err != nil {
			return err
		}
		for _, ro := range ros {
			if !slices.Contains(want, ro.Object) {
				continue
			}
			v, err := tx.Version(ctx, ro.VersionID)
			if err != nil {
				return err
			}
			c := &configv1.Canary{Object: ro.Object, Scope: ro.Scope, ReleaseId: ro.ReleaseID, VersionId: ro.VersionID,
				Tenant: ro.Tenant, Percent: ro.Percent}
			if service == "orchestrator" && ro.Object == ObjectPolicy {
				out = append(out, c)
				continue
			}
			if err := fillCandidate(ctx, tx, c, v); err != nil {
				return fmt.Errorf("灰度 %s 的候选版本 %s: %w", ro.ReleaseID, v.ID, err)
			}
			out = append(out, c)
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	ids := make([]string, len(out))
	for i, c := range out {
		ids[i] = c.GetReleaseId() + ":" + c.GetVersionId()
	}
	return out, hash(strings.Join(ids, ","))[:16], nil
}

// served 是各服务用到候选内容的对象：pipeline 两边都用，routing 与回答模板在 orchestrator，策略包在 retrieval。
var served = map[string][]string{"retrieval": {ObjectPipeline, ObjectPolicy}, "orchestrator": {ObjectPipeline, ObjectRouting, ObjectPrompt}}

// Candidate 是 ConfigService.GetCandidate 给评测副本的候选（governance 详设 4.1 第 ③④ 步）：按版本组装，内容与灰度里的候选相同。
// 这个服务用不到的对象（如 orchestrator 取策略包的版本）返回不带候选内容的一项，评测副本照常用生效的配置。
// pipeline 只对它的租户生效，其他对象不限租户。调用方身份（评测身份）由 ConfigService 校验。
func (s *Server) Candidate(ctx context.Context, service, versionID string) (*configv1.Canary, error) {
	want, ok := served[service]
	if !ok || !versionPattern.MatchString(versionID) {
		return nil, status.Error(codes.InvalidArgument, "service 只能是 retrieval 或 orchestrator，version_id 须为 cfv_ 版本")
	}
	var c *configv1.Canary
	err := s.Store.InTx(ctx, func(tx Tx) error {
		v, err := tx.Version(ctx, versionID)
		if err != nil {
			return err
		}
		if !gitObject(v.Object) {
			return status.Error(codes.InvalidArgument, "只有 pipeline、routing、prompt 与 policy 有候选")
		}
		c = &configv1.Canary{Object: v.Object, Scope: v.Scope, VersionId: v.ID, Percent: 100}
		if v.Object == ObjectPipeline {
			c.Tenant = v.Scope
		}
		if !slices.Contains(want, v.Object) {
			return nil
		}
		return fillCandidate(ctx, tx, c, v)
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return c, nil
}

func fillCandidate(ctx context.Context, tx Tx, c *configv1.Canary, v Version) error {
	switch v.Object {
	case ObjectPipeline:
		p, err := ParsePipeline(v.Scope, v.Content)
		if err != nil {
			return err
		}
		p.VersionId = v.ID
		c.Candidate = &configv1.Canary_Pipeline{Pipeline: p}
	case ObjectRouting:
		r, err := ParseRouting(v.Content)
		if err != nil {
			return err
		}
		r.VersionId = v.ID
		c.Candidate = &configv1.Canary_Routing{Routing: r}
	case ObjectPrompt:
		p, err := ParsePrompt(v.Scope, v.Content)
		if err != nil {
			return err
		}
		p.VersionId = v.ID
		c.Candidate = &configv1.Canary_Prompt{Prompt: p}
	case ObjectPolicy:
		layers, err := activeLayers(ctx, tx)
		if err != nil {
			return err
		}
		layers[v.Scope] = v.Content
		b, err := build(layers)
		if err != nil {
			return err
		}
		pb := &configv1.PolicyBundle{Version: b.Version}
		for _, m := range b.Modules {
			pb.Modules = append(pb.Modules, &configv1.PolicyModule{Path: m.Path, Source: m.Source})
		}
		c.Candidate = &configv1.Canary_Policy{Policy: pb}
	}
	return nil
}
