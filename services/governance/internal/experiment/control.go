package experiment

import (
	"context"
	"log/slog"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/hei-/bambuflow/libs/platform-go/changeaudit"
)

// GuardrailActor 是护栏触发停止实验时记下的停止人。
const GuardrailActor = "evaluation"

// ControlServer 实现 ExperimentControlService：给 evaluation 的服务间接口（evaluation 详设 7.2），
// 不要求 Principal，调用方由服务网格与网络策略限定。
type ControlServer struct {
	configv1.UnimplementedExperimentControlServiceServer
	Experiments *Server
}

func (c *ControlServer) GetExperiment(ctx context.Context, r *configv1.GetExperimentRequest) (*configv1.GetExperimentResponse, error) {
	if !idPattern.MatchString(r.GetExpId()) {
		return nil, status.Error(codes.NotFound, "实验不存在")
	}
	e, err := c.Experiments.Store.Get(ctx, r.GetExpId())
	if err != nil {
		return nil, c.Experiments.storeErr(ctx, err)
	}
	return &configv1.GetExperimentResponse{Experiment: e.proto()}, nil
}

func (c *ControlServer) ListRunningExperiments(ctx context.Context, _ *configv1.ListRunningExperimentsRequest) (*configv1.ListRunningExperimentsResponse, error) {
	list, err := c.Experiments.Store.Running(ctx)
	if err != nil {
		return nil, c.Experiments.storeErr(ctx, err)
	}
	out := &configv1.ListRunningExperimentsResponse{}
	for _, e := range list {
		out.Experiments = append(out.Experiments, e.proto())
	}
	return out, nil
}

func (c *ControlServer) StopForGuardrail(ctx context.Context, r *configv1.StopForGuardrailRequest) (*configv1.StopForGuardrailResponse, error) {
	if !idPattern.MatchString(r.GetExpId()) || !validText(r.GetReason(), 500) {
		return nil, status.Error(codes.InvalidArgument, "exp_id 或 reason 不合法")
	}
	e, err := c.Experiments.Stop(changeaudit.AsService(ctx, GuardrailActor), r.GetExpId(), GuardrailActor,
		"护栏触发："+strings.TrimSpace(r.GetReason()))
	if err != nil {
		return nil, err
	}
	c.Experiments.Log.WarnContext(ctx, "护栏触发，实验已停止", slog.String("exp_id", e.ID), slog.String("reason", r.GetReason()))
	return &configv1.StopForGuardrailResponse{Experiment: e.proto()}, nil
}
