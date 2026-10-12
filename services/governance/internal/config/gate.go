package config

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	evalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/evaluation/v1"
)

// 配置发布的评测门禁（governance 详设 4.1 第 ③④ 步、evaluation 详设 5.1）：影响回答的对象与策略包发布后状态为 gating，
// AdvanceGates 定期推进——还没发起的用候选版本发起门禁（幂等键是发布 ID，重复发起拿到同一个门禁），发起过的查结论。
// 通过转为 pending_approval；不通过、无法判定或超时为 gate_failed。evaluation 不可用时停在 gating
// （不允许跳过门禁，详设第 11 节），7 天后随待审批的一起关闭。

// GateTimeout：门禁发起后这么久还没有结论按不通过。evaluation 自己按 30 分钟预算结束门禁，这里多留排队与重启的余量
// （evaluation 详设 5：调用方自己决定等多久，超时未得到结论一律按不通过处理）。
const GateTimeout = time.Hour

// EvalGates 是 evaluation 的 EvalService（生成的客户端）。
type EvalGates interface {
	RunGate(ctx context.Context, in *evalv1.RunGateRequest, opts ...grpc.CallOption) (*evalv1.RunGateResponse, error)
	GetGate(ctx context.Context, in *evalv1.GetGateRequest, opts ...grpc.CallOption) (*evalv1.GetGateResponse, error)
}

// gateScope 是门禁评测的租户（空 = 评测集里的全部租户）与规则集（空 = 按触发类型的默认，即全部题）：
// pipeline 与租户策略层只影响它的租户；routing、回答模板、全局与数据源策略层按引用关系算是全部租户（evaluation 详设 5.1）。
// 策略包只跑权限题（详设 4.2）。
func gateScope(rel Release) (tenants []string, ruleset string) {
	if rel.Object == ObjectPolicy {
		ruleset = "permission"
	}
	switch {
	case rel.Object == ObjectPipeline:
		return []string{rel.Scope}, ruleset
	case rel.Object == ObjectPolicy && strings.HasPrefix(rel.Scope, "tenant:"):
		return []string{strings.TrimPrefix(rel.Scope, "tenant:")}, ruleset
	}
	return nil, ruleset
}

// AdvanceGates 推进所有门禁中的发布，返回本次得出结论的个数。单个发布出错只记日志、下一轮再试。
func (s *Server) AdvanceGates(ctx context.Context) (int, error) {
	var gating []Release
	err := s.Store.InTx(ctx, func(tx Tx) error {
		var err error
		gating, err = tx.Releases(ctx, "gating", listLimit)
		return err
	})
	if err != nil {
		return 0, err
	}
	n := 0
	for _, rel := range gating {
		done, err := s.advance(ctx, rel)
		if err != nil {
			s.Log.WarnContext(ctx, "推进发布的评测门禁失败，稍后重试", slog.String("release_id", rel.ID), slog.Any("err", err))
			continue
		}
		if done {
			n++
		}
	}
	return n, nil
}

func (s *Server) advance(ctx context.Context, rel Release) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if rel.GateID == "" {
		tenants, ruleset := gateScope(rel)
		resp, err := s.Evals.RunGate(ctx, &evalv1.RunGateRequest{Trigger: "config_release",
			Candidate: &evalv1.EvalConfig{ConfigVersion: rel.VersionID}, Tenants: tenants, Ruleset: ruleset, IdempotencyKey: rel.ID})
		switch status.Code(err) {
		case codes.OK:
			return false, s.gateStep(ctx, rel.ID, "", func(r *Release) (string, map[string]any) {
				now := s.Now()
				r.GateID, r.GateStartedAt = resp.GetGateId(), &now
				return "config.release_gate_started", map[string]any{"gate_id": r.GateID, "version_id": r.VersionID,
					"tenants": tenants, "ruleset": ruleset}
			})
		case codes.InvalidArgument, codes.FailedPrecondition:
			// 门禁发不起来（如评测集里没有这个租户的题）：这个候选没法评，按不通过
			return true, s.failGate(ctx, rel, "", []string{"门禁无法发起：" + status.Convert(err).Message()})
		default:
			return false, err
		}
	}
	resp, err := s.Evals.GetGate(ctx, &evalv1.GetGateRequest{GateId: rel.GateID})
	switch {
	case status.Code(err) == codes.NotFound:
		return true, s.failGate(ctx, rel, rel.GateID, []string{"门禁记录不存在"})
	case err != nil:
		return false, err
	}
	switch resp.GetVerdict() {
	case evalv1.Verdict_VERDICT_PASSED:
		err := s.gateStep(ctx, rel.ID, rel.GateID, func(r *Release) (string, map[string]any) {
			r.Status = "pending_approval"
			return "config.release_gate_passed", map[string]any{"gate_id": r.GateID, "version_id": r.VersionID}
		})
		if err == nil {
			s.Log.InfoContext(ctx, "发布已通过评测门禁，等待审批", slog.String("release_id", rel.ID), slog.String("gate_id", rel.GateID))
		}
		return true, err
	case evalv1.Verdict_VERDICT_FAILED, evalv1.Verdict_VERDICT_INDETERMINATE:
		reasons := resp.GetViolations()
		if resp.GetVerdict() == evalv1.Verdict_VERDICT_INDETERMINATE {
			reasons = append([]string{"无法判定"}, reasons...)
		}
		return true, s.failGate(ctx, rel, rel.GateID, reasons)
	}
	if rel.GateStartedAt != nil && s.Now().Sub(*rel.GateStartedAt) > GateTimeout {
		return true, s.failGate(ctx, rel, rel.GateID, []string{fmt.Sprintf("超过 %s 仍没有结论", GateTimeout)})
	}
	return false, nil
}

func (s *Server) failGate(ctx context.Context, rel Release, gateID string, reasons []string) error {
	err := s.gateStep(ctx, rel.ID, gateID, func(r *Release) (string, map[string]any) {
		r.Status = "gate_failed"
		return "config.release_gate_failed", map[string]any{"gate_id": r.GateID, "version_id": r.VersionID, "reasons": reasons}
	})
	if err == nil {
		s.Log.WarnContext(ctx, "发布未通过评测门禁", slog.String("release_id", rel.ID), slog.String("gate_id", gateID),
			slog.Any("reasons", reasons))
	}
	return err
}

// gateStep 在事务里改一个门禁中的发布并写审计；发布已不在门禁中、或门禁已不是读到时的那个（另一个副本先推进了）时什么也不做。
func (s *Server) gateStep(ctx context.Context, id, gateID string, apply func(*Release) (string, map[string]any)) error {
	return s.Store.InTx(ctx, func(tx Tx) error {
		r, err := tx.LockRelease(ctx, id)
		if err != nil {
			return err
		}
		if r.Status != "gating" || r.GateID != gateID {
			return nil
		}
		action, detail := apply(&r)
		if err := tx.UpdateRelease(ctx, r); err != nil {
			return err
		}
		return s.Audit.Write(ctx, action, r.ID, detail)
	})
}
