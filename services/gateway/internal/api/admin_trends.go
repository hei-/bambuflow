package api

import (
	"context"
	"regexp"
	"strconv"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 在线抽样评审的趋势（evaluation 详设 7.1、8.2）：GET /v1/admin/quality-trends?tenant=&days=，由 evaluation 实现。

// 租户名的写法与 governance 一致；格式不对直接 400，不转给实现方
var tenantName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

type dailyQualityJSON struct {
	Day               string  `json:"day"`
	Sampled           int32   `json:"sampled"`
	Sentences         int32   `json:"sentences"`
	Groundedness      float64 `json:"groundedness"`
	CitationPrecision float64 `json:"citation_precision"`
	JudgeModel        string  `json:"judge_model"`
	ComputedAt        string  `json:"computed_at,omitempty"`
}

type qualityTrendsJSON struct {
	Tenant          string             `json:"tenant"`
	Days            []dailyQualityJSON `json:"days"`
	Groundedness7d  float64            `json:"groundedness_7d"`
	Groundedness30d float64            `json:"groundedness_30d"`
	Declining       bool               `json:"declining"`
}

func (s *Server) qualityTrends(ctx context.Context, in *adminInput) (adminOutput, error) {
	tenant := in.query.Get("tenant")
	if !tenantName.MatchString(tenant) {
		return adminOutput{}, problemInvalid
	}
	var days int32
	if v := in.query.Get("days"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n < 1 || n > 90 {
			return adminOutput{}, problemInvalid
		}
		days = int32(n)
	}
	resp, err := s.Trends.GetQualityTrends(ctx, &adminv1.GetQualityTrendsRequest{Tenant: tenant, Days: days})
	if err != nil {
		return adminOutput{}, err
	}
	out := qualityTrendsJSON{Tenant: resp.GetTenant(), Days: []dailyQualityJSON{}, Groundedness7d: resp.GetGroundedness_7D(),
		Groundedness30d: resp.GetGroundedness_30D(), Declining: resp.GetDeclining()}
	for _, d := range resp.GetDays() {
		out.Days = append(out.Days, dailyQualityJSON{Day: d.GetDay(), Sampled: d.GetSampled(), Sentences: d.GetSentences(),
			Groundedness: d.GetGroundedness(), CitationPrecision: d.GetCitationPrecision(), JudgeModel: d.GetJudgeModel(),
			ComputedAt: rfc3339(d.GetComputedAt())})
	}
	return adminOutput{body: out}, nil
}
