// Package experiment 是 A/B 实验的登记与流量分配（governance 详设 3.1、7.2，技术设计文档 10.5）。
// 实验以 governance 的数据库为唯一事实来源；运行中的实验经配置快照下发给 orchestrator，由它分组（orchestrator 详设 12.3），
// 分析在 evaluation。实验流量属运营数据，按 governance 详设 4.2 一人操作、1 分钟内生效，不走门禁与灰度发布流程。
package experiment

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/services/governance/internal/paging"
)

// RolePlatformAdmin 管理实验（governance 详设 9.1）。
const RolePlatformAdmin = "platform_admin"

var (
	ErrNotFound  = errors.New("不存在")
	ErrRunning   = errors.New("该租户已有运行中的实验")
	errUnchanged = errors.New("无变化")
	errInternal  = status.Error(codes.Unavailable, "暂时不可用")

	tenantPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	idPattern     = regexp.MustCompile(`^exp_[0-9a-f]{24}$`)

	metrics = map[adminv1.PrimaryMetric]string{
		adminv1.PrimaryMetric_PRIMARY_METRIC_USEFUL_RATE:                "useful_rate",
		adminv1.PrimaryMetric_PRIMARY_METRIC_INSUFFICIENT_EVIDENCE_RATE: "insufficient_evidence_rate",
	}
	// 灰度按 5% → 20% → 50% 推进（技术设计文档 10.5）
	trafficSteps = []int32{5, 20, 50}
)

// Experiment 是一条实验记录。PrimaryMetric 为 useful_rate 或 insufficient_evidence_rate，Status 为 running 或 stopped。
type Experiment struct {
	ID, Tenant, Hypothesis, PrimaryMetric, Status, CreatedBy string
	BaselineRate, MinDetectableEffect                        float64
	SampleSizePerArm                                         int64
	TrafficPercent                                           int32
	SupportThreshold, MinCoverage                            *float64
	CreatedAt                                                time.Time
	StoppedBy, StopReason                                    string
	StoppedAt                                                *time.Time
}

// Store 是实验的读写（生产为 PG）。
type Store interface {
	Create(ctx context.Context, e Experiment, beforeCommit func() error) (Experiment, error)
	Get(ctx context.Context, id string) (Experiment, error)
	List(ctx context.Context, offset, limit int) ([]Experiment, error)
	Running(ctx context.Context) ([]Experiment, error)
	Update(ctx context.Context, id string, change func(e *Experiment) error) (Experiment, error)
}

// Auditor 写变更审计（internal/changeaudit）。
type Auditor interface {
	Write(ctx context.Context, action, objectID string, detail any) error
}

// Server 实现 ExperimentAdminService。
type Server struct {
	adminv1.UnimplementedExperimentAdminServiceServer
	Store Store
	Audit Auditor
	Log   *slog.Logger
	Now   func() time.Time
}

// SampleSize 是双样本比例检验每组需要的用户数：双侧 α = 0.05、检验力 0.8，检测从 baseline 变化 mde（绝对值）。
// A/B 按用户聚合（evaluation 详设 7.2），所以这里的单位是用户而不是请求。
func SampleSize(baseline, mde float64) int64 {
	const zAlpha, zBeta = 1.959964, 0.841621
	p2 := baseline + mde
	if p2 >= 1 {
		p2 = baseline - mde
	}
	mean := (baseline + p2) / 2
	a := zAlpha*math.Sqrt(2*mean*(1-mean)) + zBeta*math.Sqrt(baseline*(1-baseline)+p2*(1-p2))
	return int64(math.Ceil(a * a / (mde * mde)))
}

func validText(s string, max int) bool {
	n := utf8.RuneCountInString(strings.TrimSpace(s))
	return n > 0 && n <= max
}

func validOverrides(o *adminv1.ExperimentOverrides) bool {
	if o == nil || (o.SupportThreshold == nil && o.MinCoverage == nil) {
		// 没有替换任何对象的实验两组完全相同，没有意义
		return false
	}
	if o.SupportThreshold != nil && (o.GetSupportThreshold() < 0.3 || o.GetSupportThreshold() > 0.9) {
		return false
	}
	return o.MinCoverage == nil || (o.GetMinCoverage() >= 0.5 && o.GetMinCoverage() <= 1)
}

func (s *Server) admin(ctx context.Context) error {
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), RolePlatformAdmin) {
		return status.Error(codes.PermissionDenied, "需要平台管理员")
	}
	return nil
}

func (s *Server) storeErr(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, "实验不存在")
	case errors.Is(err, ErrRunning):
		return grpcmw.Reason(codes.FailedPrecondition, "conflict", ErrRunning.Error())
	}
	var st interface{ GRPCStatus() *status.Status }
	if errors.As(err, &st) {
		return err
	}
	s.Log.ErrorContext(ctx, "读写实验失败", slog.Any("err", err))
	return errInternal
}

func (s *Server) CreateExperiment(ctx context.Context, r *adminv1.CreateExperimentRequest) (*adminv1.CreateExperimentResponse, error) {
	if err := s.admin(ctx); err != nil {
		return nil, err
	}
	metric, ok := metrics[r.GetPrimaryMetric()]
	base, mde := r.GetBaselineRate(), r.GetMinDetectableEffect()
	if !ok || !tenantPattern.MatchString(r.GetTenant()) || !validText(r.GetHypothesis(), 1000) ||
		base <= 0 || base >= 1 || mde <= 0 || (base+mde >= 1 && base-mde <= 0) || !validOverrides(r.GetOverrides()) {
		return nil, status.Error(codes.InvalidArgument, "tenant、hypothesis、primary_metric、baseline_rate、min_detectable_effect 或 overrides 不合法")
	}
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	e := Experiment{ID: "exp_" + hex.EncodeToString(b), Tenant: r.GetTenant(), Hypothesis: strings.TrimSpace(r.GetHypothesis()),
		PrimaryMetric: metric, BaselineRate: base, MinDetectableEffect: mde, SampleSizePerArm: SampleSize(base, mde),
		TrafficPercent: trafficSteps[0], SupportThreshold: r.GetOverrides().SupportThreshold, MinCoverage: r.GetOverrides().MinCoverage,
		CreatedBy: grpcmw.PrincipalFrom(ctx).GetSub()}
	e, err := s.Store.Create(ctx, e, func() error {
		return s.Audit.Write(ctx, "experiment.create", e.ID, map[string]any{"tenant": e.Tenant, "primary_metric": metric,
			"baseline_rate": base, "min_detectable_effect": mde, "sample_size_per_arm": e.SampleSizePerArm,
			"traffic_percent": e.TrafficPercent, "support_threshold": e.SupportThreshold, "min_coverage": e.MinCoverage})
	})
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	s.Log.InfoContext(ctx, "实验已开始", slog.String("exp_id", e.ID), slog.String("tenant", e.Tenant))
	return &adminv1.CreateExperimentResponse{Experiment: e.proto()}, nil
}

func (s *Server) RampExperiment(ctx context.Context, r *adminv1.RampExperimentRequest) (*adminv1.RampExperimentResponse, error) {
	if err := s.admin(ctx); err != nil {
		return nil, err
	}
	if !idPattern.MatchString(r.GetExpId()) || (r.GetTrafficPercent() != 20 && r.GetTrafficPercent() != 50) {
		return nil, status.Error(codes.InvalidArgument, "exp_id 或 traffic_percent 不合法（只能调到 20 或 50）")
	}
	e, err := s.Store.Update(ctx, r.GetExpId(), func(e *Experiment) error {
		if e.Status != "running" || r.GetTrafficPercent() <= e.TrafficPercent {
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "只能对运行中的实验向上调整流量")
		}
		from := e.TrafficPercent
		e.TrafficPercent = r.GetTrafficPercent()
		return s.Audit.Write(ctx, "experiment.ramp", e.ID, map[string]any{"from": from, "to": e.TrafficPercent})
	})
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	return &adminv1.RampExperimentResponse{Experiment: e.proto()}, nil
}

func (s *Server) StopExperiment(ctx context.Context, r *adminv1.StopExperimentRequest) (*adminv1.StopExperimentResponse, error) {
	if err := s.admin(ctx); err != nil {
		return nil, err
	}
	if !idPattern.MatchString(r.GetExpId()) || !validText(r.GetReason(), 500) {
		return nil, status.Error(codes.InvalidArgument, "exp_id 或 reason 不合法")
	}
	e, err := s.Stop(ctx, r.GetExpId(), grpcmw.PrincipalFrom(ctx).GetSub(), r.GetReason())
	if err != nil {
		return nil, err
	}
	return &adminv1.StopExperimentResponse{Experiment: e.proto()}, nil
}

// Stop 停止实验（调用方已校验身份）；已停止的原样返回、不再审计。
func (s *Server) Stop(ctx context.Context, id, by, reason string) (Experiment, error) {
	e, err := s.Store.Update(ctx, id, func(e *Experiment) error {
		if e.Status == "stopped" {
			return errUnchanged
		}
		now := s.Now()
		e.Status, e.StoppedBy, e.StoppedAt, e.StopReason = "stopped", by, &now, strings.TrimSpace(reason)
		return s.Audit.Write(ctx, "experiment.stop", e.ID, map[string]string{"reason": e.StopReason})
	})
	if errors.Is(err, errUnchanged) {
		return e, nil
	}
	if err != nil {
		return Experiment{}, s.storeErr(ctx, err)
	}
	s.Log.InfoContext(ctx, "实验已停止", slog.String("exp_id", e.ID), slog.String("by", by))
	return e, nil
}

func (s *Server) GetExperiment(ctx context.Context, r *adminv1.GetExperimentRequest) (*adminv1.GetExperimentResponse, error) {
	if err := s.admin(ctx); err != nil {
		return nil, err
	}
	if !idPattern.MatchString(r.GetExpId()) {
		return nil, status.Error(codes.NotFound, "实验不存在")
	}
	e, err := s.Store.Get(ctx, r.GetExpId())
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	return &adminv1.GetExperimentResponse{Experiment: e.proto()}, nil
}

func (s *Server) ListExperiments(ctx context.Context, r *adminv1.ListExperimentsRequest) (*adminv1.ListExperimentsResponse, error) {
	if err := s.admin(ctx); err != nil {
		return nil, err
	}
	size, offset, err := paging.Parse(r.GetPageSize(), r.GetPageToken())
	if err != nil {
		return nil, err
	}
	list, err := s.Store.List(ctx, offset, size+1)
	if err != nil {
		return nil, s.storeErr(ctx, err)
	}
	out := &adminv1.ListExperimentsResponse{}
	if len(list) > size {
		list, out.NextPageToken = list[:size], paging.Token(offset+size)
	}
	for _, e := range list {
		out.Experiments = append(out.Experiments, e.proto())
	}
	return out, nil
}

// Snapshot 是 orchestrator 快照里的运行中实验与它们的摘要（并入 snapshot_version，回放时据此还原分组配置）。
func (s *Server) Snapshot(ctx context.Context) ([]*configv1.RunningExperiment, string, error) {
	list, err := s.Store.Running(ctx)
	if err != nil {
		return nil, "", err
	}
	h := sha256.New()
	out := make([]*configv1.RunningExperiment, 0, len(list))
	for _, e := range list {
		out = append(out, &configv1.RunningExperiment{ExpId: e.ID, Tenant: e.Tenant, TrafficPercent: e.TrafficPercent,
			SupportThreshold: e.SupportThreshold, MinCoverage: e.MinCoverage})
		_, _ = fmt.Fprintf(h, "%s|%s|%d|%v|%v\n", e.ID, e.Tenant, e.TrafficPercent, deref(e.SupportThreshold), deref(e.MinCoverage))
	}
	return out, hex.EncodeToString(h.Sum(nil))[:16], nil
}

func deref(v *float64) any {
	if v == nil {
		return "-"
	}
	return *v
}

func (e Experiment) proto() *adminv1.Experiment {
	out := &adminv1.Experiment{ExpId: e.ID, Tenant: e.Tenant, Hypothesis: e.Hypothesis, BaselineRate: e.BaselineRate,
		MinDetectableEffect: e.MinDetectableEffect, SampleSizePerArm: e.SampleSizePerArm, TrafficPercent: e.TrafficPercent,
		Overrides:  &adminv1.ExperimentOverrides{SupportThreshold: e.SupportThreshold, MinCoverage: e.MinCoverage},
		Status:     adminv1.ExperimentStatus_EXPERIMENT_STATUS_RUNNING,
		CreatedBy:  e.CreatedBy,
		CreatedAt:  timestamppb.New(e.CreatedAt),
		StoppedBy:  e.StoppedBy,
		StopReason: e.StopReason,
	}
	for k, v := range metrics {
		if v == e.PrimaryMetric {
			out.PrimaryMetric = k
		}
	}
	if e.Status == "stopped" {
		out.Status = adminv1.ExperimentStatus_EXPERIMENT_STATUS_STOPPED
	}
	if e.StoppedAt != nil {
		out.StoppedAt = timestamppb.New(*e.StoppedAt)
	}
	return out
}
