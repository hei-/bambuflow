package api

import (
	"context"
	"net/http"
	"regexp"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// A/B 实验（governance 详设 7.2，技术设计文档 10.5）：/v1/admin/experiments，由 governance 实现，平台管理员操作。
// 分析结果 GET /v1/admin/experiments/{exp_id}/analysis 由 evaluation 实现（evaluation 详设 7.2、8.2）。

var (
	primaryMetrics = map[string]adminv1.PrimaryMetric{
		"useful_rate":                adminv1.PrimaryMetric_PRIMARY_METRIC_USEFUL_RATE,
		"insufficient_evidence_rate": adminv1.PrimaryMetric_PRIMARY_METRIC_INSUFFICIENT_EVIDENCE_RATE,
	}
	expIDPattern = regexp.MustCompile(`^exp_[0-9a-f]{24}$`)
)

type overridesJSON struct {
	SupportThreshold *float64 `json:"support_threshold,omitempty"`
	MinCoverage      *float64 `json:"min_coverage,omitempty"`
	ConfigVersions   []string `json:"config_versions,omitempty"`
}

type experimentJSON struct {
	ExpID               string        `json:"exp_id"`
	Tenant              string        `json:"tenant"`
	Hypothesis          string        `json:"hypothesis"`
	PrimaryMetric       string        `json:"primary_metric"`
	BaselineRate        float64       `json:"baseline_rate"`
	MinDetectableEffect float64       `json:"min_detectable_effect"`
	SampleSizePerArm    int64         `json:"sample_size_per_arm"`
	TrafficPercent      int32         `json:"traffic_percent"`
	Overrides           overridesJSON `json:"overrides"`
	Status              string        `json:"status"`
	CreatedBy           string        `json:"created_by"`
	CreatedAt           string        `json:"created_at"`
	StoppedBy           string        `json:"stopped_by,omitempty"`
	StoppedAt           string        `json:"stopped_at,omitempty"`
	StopReason          string        `json:"stop_reason,omitempty"`
}

func experimentOf(e *adminv1.Experiment) experimentJSON {
	return experimentJSON{ExpID: e.GetExpId(), Tenant: e.GetTenant(), Hypothesis: e.GetHypothesis(),
		PrimaryMetric: enumName(e.GetPrimaryMetric()), BaselineRate: e.GetBaselineRate(), MinDetectableEffect: e.GetMinDetectableEffect(),
		SampleSizePerArm: e.GetSampleSizePerArm(), TrafficPercent: e.GetTrafficPercent(),
		Overrides: overridesJSON{SupportThreshold: e.GetOverrides().SupportThreshold, MinCoverage: e.GetOverrides().MinCoverage,
			ConfigVersions: e.GetOverrides().GetConfigVersions()},
		Status: enumName(e.GetStatus()), CreatedBy: e.GetCreatedBy(), CreatedAt: rfc3339(e.GetCreatedAt()),
		StoppedBy: e.GetStoppedBy(), StoppedAt: rfc3339(e.GetStoppedAt()), StopReason: e.GetStopReason()}
}

type experimentListJSON struct {
	Experiments   []experimentJSON `json:"experiments"`
	NextPageToken string           `json:"next_page_token,omitempty"`
}

func (s *Server) listExperiments(ctx context.Context, in *adminInput) (adminOutput, error) {
	size, err := pageSize(in)
	if err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Experiments.ListExperiments(ctx, &adminv1.ListExperimentsRequest{PageSize: size, PageToken: in.query.Get("page_token")})
	if err != nil {
		return adminOutput{}, err
	}
	out := experimentListJSON{Experiments: []experimentJSON{}, NextPageToken: resp.GetNextPageToken()}
	for _, e := range resp.GetExperiments() {
		out.Experiments = append(out.Experiments, experimentOf(e))
	}
	return adminOutput{body: out}, nil
}

type createExperimentJSON struct {
	Tenant              string         `json:"tenant"`
	Hypothesis          string         `json:"hypothesis"`
	PrimaryMetric       string         `json:"primary_metric"`
	BaselineRate        float64        `json:"baseline_rate"`
	MinDetectableEffect float64        `json:"min_detectable_effect"`
	Overrides           *overridesJSON `json:"overrides"`
}

func (s *Server) createExperiment(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req createExperimentJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	metric, ok := primaryMetrics[req.PrimaryMetric]
	if !ok || req.Overrides == nil {
		return adminOutput{}, problemInvalid
	}
	e, err := s.Experiments.CreateExperiment(ctx, &adminv1.CreateExperimentRequest{Tenant: req.Tenant, Hypothesis: req.Hypothesis,
		PrimaryMetric: metric, BaselineRate: req.BaselineRate, MinDetectableEffect: req.MinDetectableEffect,
		Overrides: &adminv1.ExperimentOverrides{SupportThreshold: req.Overrides.SupportThreshold, MinCoverage: req.Overrides.MinCoverage,
			ConfigVersions: req.Overrides.ConfigVersions}})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{status: http.StatusCreated, body: experimentOf(e.GetExperiment())}, nil
}

// expID 取路径里的实验 ID；格式不对按不存在处理。
func expID(in *adminInput) (string, error) {
	id := in.params["exp_id"]
	if !expIDPattern.MatchString(id) {
		return "", problemNotFound
	}
	return id, nil
}

func (s *Server) getExperiment(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := expID(in)
	if err != nil {
		return adminOutput{}, err
	}
	e, err := s.Experiments.GetExperiment(ctx, &adminv1.GetExperimentRequest{ExpId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: experimentOf(e.GetExperiment())}, nil
}

type rampExperimentJSON struct {
	TrafficPercent int32 `json:"traffic_percent"`
}

func (s *Server) rampExperiment(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := expID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req rampExperimentJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	e, err := s.Experiments.RampExperiment(ctx, &adminv1.RampExperimentRequest{ExpId: id, TrafficPercent: req.TrafficPercent})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: experimentOf(e.GetExperiment())}, nil
}

type stopExperimentJSON struct {
	Reason string `json:"reason"`
}

func (s *Server) stopExperiment(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := expID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req stopExperimentJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	e, err := s.Experiments.StopExperiment(ctx, &adminv1.StopExperimentRequest{ExpId: id, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: experimentOf(e.GetExperiment())}, nil
}

type armJSON struct {
	Arm          string  `json:"arm"`
	Users        int64   `json:"users"`
	Answers      int64   `json:"answers"`
	MetricUsers  int64   `json:"metric_users"`
	MetricMean   float64 `json:"metric_mean"`
	RefusedRate  float64 `json:"refused_rate"`
	P95LatencyMS int64   `json:"p95_latency_ms"`
	MeanTokens   float64 `json:"mean_tokens"`
}

func armOf(a *adminv1.ArmSummary) armJSON {
	return armJSON{Arm: a.GetArm(), Users: a.GetUsers(), Answers: a.GetAnswers(), MetricUsers: a.GetMetricUsers(),
		MetricMean: a.GetMetricMean(), RefusedRate: a.GetRefusedRate(), P95LatencyMS: a.GetP95LatencyMs(), MeanTokens: a.GetMeanTokens()}
}

type guardrailJSON struct {
	Name      string  `json:"name"`
	Control   float64 `json:"control"`
	Treatment float64 `json:"treatment"`
	Breached  bool    `json:"breached"`
}

type conclusionJSON struct {
	Delta       float64 `json:"delta"`
	CILow       float64 `json:"ci_low"`
	CIHigh      float64 `json:"ci_high"`
	PValue      float64 `json:"p_value"`
	Significant bool    `json:"significant"`
	Improved    bool    `json:"improved"`
}

type analysisJSON struct {
	Experiment experimentJSON  `json:"experiment"`
	Control    armJSON         `json:"control"`
	Treatment  armJSON         `json:"treatment"`
	Progress   float64         `json:"progress"`
	Guardrails []guardrailJSON `json:"guardrails"`
	// 达到样本量前不返回（evaluation 详设 7.2：只显示进度，避免提前偏看）
	Conclusion *conclusionJSON `json:"conclusion,omitempty"`
	DataSince  string          `json:"data_since"`
	DataUntil  string          `json:"data_until"`
}

func (s *Server) experimentAnalysis(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := expID(in)
	if err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Analysis.GetExperimentAnalysis(ctx, &adminv1.GetExperimentAnalysisRequest{ExpId: id})
	if err != nil {
		return adminOutput{}, err
	}
	a := resp.GetAnalysis()
	out := analysisJSON{Experiment: experimentOf(a.GetExperiment()), Control: armOf(a.GetControl()), Treatment: armOf(a.GetTreatment()),
		Progress: a.GetProgress(), Guardrails: []guardrailJSON{}, DataSince: rfc3339(a.GetDataSince()), DataUntil: rfc3339(a.GetDataUntil())}
	for _, g := range a.GetGuardrails() {
		out.Guardrails = append(out.Guardrails, guardrailJSON{Name: g.GetName(), Control: g.GetControl(), Treatment: g.GetTreatment(), Breached: g.GetBreached()})
	}
	if c := a.GetConclusion(); c != nil {
		out.Conclusion = &conclusionJSON{Delta: c.GetDelta(), CILow: c.GetCiLow(), CIHigh: c.GetCiHigh(), PValue: c.GetPValue(),
			Significant: c.GetSignificant(), Improved: c.GetImproved()}
	}
	return adminOutput{body: out}, nil
}
