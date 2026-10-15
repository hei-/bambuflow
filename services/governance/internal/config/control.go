package config

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/hei-/bambuflow/libs/platform-go/changeaudit"
	"github.com/hei-/bambuflow/libs/platform-go/telemetry"
)

// ObserverActor 是灰度观察自动全量或终止时记下的操作人。
const ObserverActor = "evaluation"

// RolloutControl 实现 RolloutControlService：给 evaluation 的服务间接口（governance 详设 4.1 第 ⑨ 步），
// 不要求 Principal，调用方由服务网格与网络策略限定。比较与阈值在 evaluation（它能读回答记录），这里只执行结论。
type RolloutControl struct {
	configv1.UnimplementedRolloutControlServiceServer
	Configs *Server
}

var rolloutAborts, _ = telemetry.Meter("bambuflow/governance").Int64Counter("governance.rollout.auto_aborts",
	metric.WithDescription("灰度观察超阈值自动终止的次数"))

func (c *RolloutControl) ListRollouts(ctx context.Context, _ *configv1.ListRolloutsRequest) (*configv1.ListRolloutsResponse, error) {
	out := &configv1.ListRolloutsResponse{}
	err := c.Configs.Store.InTx(ctx, func(tx Tx) error {
		ros, err := tx.Rollouts(ctx)
		if err != nil {
			return err
		}
		for _, ro := range ros {
			out.Rollouts = append(out.Rollouts, &configv1.ActiveRollout{ReleaseId: ro.ReleaseID, Object: ro.Object, Scope: ro.Scope,
				VersionId: ro.VersionID, Tenant: ro.Tenant, Percent: ro.Percent, StartedAt: timestamppb.New(ro.StartedAt)})
		}
		return nil
	})
	if err != nil {
		return nil, c.Configs.fail(ctx, err)
	}
	return out, nil
}

func (c *RolloutControl) ReportObservation(ctx context.Context, r *configv1.ReportObservationRequest) (*configv1.ReportObservationResponse, error) {
	summary, ok := text(r.GetSummary(), 500)
	if !relPattern.MatchString(r.GetReleaseId()) || !versionPattern.MatchString(r.GetVersionId()) || !ok {
		return nil, status.Error(codes.InvalidArgument, "release_id、version_id 或 summary 不合法")
	}
	s := c.Configs
	ctx = changeaudit.AsService(ctx, ObserverActor)
	var err error
	switch r.GetVerdict() {
	case configv1.Verdict_VERDICT_CONTINUE:
		err = s.Store.InTx(ctx, func(tx Tx) error {
			rel, _, err := lockRolling(ctx, tx, r.GetReleaseId(), r.GetVersionId())
			if err != nil {
				return err
			}
			return tx.ObserveRollout(ctx, rel.Object, rel.Scope, summary, s.Now())
		})
	case configv1.Verdict_VERDICT_PROMOTE:
		var rel Release
		if rel, _, err = s.promote(ctx, r.GetReleaseId(), r.GetVersionId(), ObserverActor, summary); err == nil {
			s.Log.InfoContext(ctx, "灰度观察期满、没有超阈值，已全量", "release_id", rel.ID, "scope", rel.Scope, "observation", summary)
		}
	case configv1.Verdict_VERDICT_ABORT:
		var rel Release
		if rel, _, err = s.abort(ctx, r.GetReleaseId(), r.GetVersionId(), ObserverActor, "灰度观察超阈值："+summary); err == nil {
			// 每次自动终止都告警（governance 详设 10.1“灰度自动回退次数”）
			rolloutAborts.Add(ctx, 1, metric.WithAttributes(attribute.String("object", rel.Object)))
			s.Log.WarnContext(ctx, "灰度观察超阈值，已自动终止", "release_id", rel.ID, "object", rel.Object, "scope", rel.Scope,
				"observation", summary)
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "verdict 不合法")
	}
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &configv1.ReportObservationResponse{}, nil
}
