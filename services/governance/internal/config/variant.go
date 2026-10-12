package config

import (
	"context"
	"fmt"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

// A/B 实验的变体（orchestrator 详设 12.3，governance 详设 3.1“变体都已单独过门禁”）：实验组替换的 pipeline、routing、
// 回答模板引用配置版本，内容随实验下发（internal/experiment）。

// gatePassed 是评测门禁已通过、之后也没有被否决的发布状态。驳回（审批人否决）与回退（上线后被人或灰度观察撤下）
// 的版本不能再做实验；门禁中、门禁未通过的还没有结论或没过。
var gatePassed = []string{"pending_approval", "pending_second_approval", "active", "superseded", "stale", "expired"}

// Variants 取实验组替换的配置版本的内容，每个版本一项（Canary 只用来装内容：object、scope、version_id 与 candidate）。
// check 为 true 时（立项）逐项核对：是 pipeline、routing 或 prompt，pipeline 属于该租户，每类对象（prompt 按 scope）
// 至多一个，且有一次门禁已通过、未被否决的发布（gate_id 不为空：引导导入与回退的发布没有过门禁）。
// 下发快照时 check 为 false：立项时已核对，之后发布被回退也不自动停止实验，由运营看护栏指标决定。
func (s *Server) Variants(ctx context.Context, tenant string, versionIDs []string, check bool) ([]*configv1.Canary, error) {
	var out []*configv1.Canary
	err := s.Store.InTx(ctx, func(tx Tx) error {
		seen := map[string]bool{}
		for _, id := range versionIDs {
			if !versionPattern.MatchString(id) {
				return status.Error(codes.InvalidArgument, "config_versions 须为 cfv_ 版本")
			}
			v, err := tx.Version(ctx, id)
			if err != nil {
				return err
			}
			c := &configv1.Canary{Object: v.Object, Scope: v.Scope, VersionId: v.ID}
			if check {
				if err := checkVariant(ctx, tx, tenant, v, seen); err != nil {
					return err
				}
			}
			if err := fillCandidate(ctx, tx, c, v); err != nil {
				return err
			}
			out = append(out, c)
		}
		return nil
	})
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return out, nil
}

func checkVariant(ctx context.Context, tx Tx, tenant string, v Version, seen map[string]bool) error {
	switch {
	case v.Object != ObjectPipeline && v.Object != ObjectRouting && v.Object != ObjectPrompt:
		// 策略包决定谁能看到什么，不能拿一部分用户做实验
		return status.Errorf(codes.InvalidArgument, "%s 只能是 pipeline、routing 或回答模板的版本", v.ID)
	case v.Object == ObjectPipeline && v.Scope != tenant:
		return status.Errorf(codes.InvalidArgument, "%s 是租户 %s 的 pipeline，不是实验所在的租户", v.ID, v.Scope)
	}
	key := v.Object
	if v.Object == ObjectPrompt {
		key += ":" + v.Scope
	}
	if seen[key] {
		return status.Errorf(codes.InvalidArgument, "每类对象至多替换一个版本（%s 重复）", key)
	}
	seen[key] = true
	rels, err := tx.ReleasesOf(ctx, v.ID)
	if err != nil {
		return err
	}
	for _, r := range rels {
		if r.GateID != "" && slices.Contains(gatePassed, r.Status) {
			return nil
		}
	}
	return fmt.Errorf("%w：%s 没有门禁已通过、未被驳回或回退的发布", ErrGateFailed, v.ID)
}
