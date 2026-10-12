package api

import (
	"context"
	"regexp"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 评测门禁（evaluation 详设 5.2、8.2）：GET /v1/admin/eval-gates、GET .../{gate_id}，由 evaluation 实现。

var gateIDPattern = regexp.MustCompile(`^gate_[a-f0-9]{24}$`)

type gateJSON struct {
	GateID        string              `json:"gate_id"`
	Trigger       string              `json:"trigger"`
	GoldenSet     string              `json:"golden_set"`
	GoldenVersion string              `json:"golden_version"`
	Tenants       []string            `json:"tenants"`
	Conclusion    string              `json:"conclusion"`
	Reasons       []string            `json:"reasons"`
	Unstable      []string            `json:"unstable"`
	Metrics       map[string]*float64 `json:"metrics"`
	Config        map[string][]string `json:"config"`
	Repeats       int32               `json:"repeats"`
	CreatedAt     string              `json:"created_at"`
	Readiness     []readinessJSON     `json:"readiness"`
	Report        string              `json:"report_markdown,omitempty"`
}

type readinessJSON struct {
	Tenant         string   `json:"tenant"`
	Stage          string   `json:"stage"`
	Counted        int32    `json:"counted"`
	Unreviewed     int32    `json:"unreviewed"`
	RefuseShare    float64  `json:"refuse_share"`
	SyntheticShare float64  `json:"synthetic_share"`
	DoubleReviewed int32    `json:"double_reviewed"`
	Gaps           []string `json:"gaps"`
}

func gateOf(g *adminv1.Gate) gateJSON {
	out := gateJSON{GateID: g.GetGateId(), Trigger: g.GetTrigger(), GoldenSet: g.GetGoldenSet(), GoldenVersion: g.GetGoldenVersion(),
		Tenants: nonNil(g.GetTenants()), Conclusion: g.GetConclusion(), Reasons: nonNil(g.GetReasons()), Unstable: nonNil(g.GetUnstable()),
		Metrics: map[string]*float64{}, Config: map[string][]string{}, Repeats: g.GetRepeats(), CreatedAt: rfc3339(g.GetCreatedAt())}
	for _, m := range g.GetMetrics() {
		// 没有样本的指标输出 null
		out.Metrics[m.GetName()] = m.Value
	}
	for _, c := range g.GetConfig() {
		out.Config[c.GetName()] = nonNil(c.GetValues())
	}
	out.Readiness = []readinessJSON{}
	for _, r := range g.GetReadiness() {
		out.Readiness = append(out.Readiness, readinessJSON{Tenant: r.GetTenant(), Stage: r.GetStage(), Counted: r.GetCounted(),
			Unreviewed: r.GetUnreviewed(), RefuseShare: r.GetRefuseShare(), SyntheticShare: r.GetSyntheticShare(),
			DoubleReviewed: r.GetDoubleReviewed(), Gaps: nonNil(r.GetGaps())})
	}
	return out
}

type gateListJSON struct {
	Gates         []gateJSON `json:"gates"`
	NextPageToken string     `json:"next_page_token,omitempty"`
}

func (s *Server) listGates(ctx context.Context, in *adminInput) (adminOutput, error) {
	size, err := pageSize(in)
	if err != nil {
		return adminOutput{}, err
	}
	q := in.query
	r, err := s.Gates.ListGates(ctx, &adminv1.ListGatesRequest{Tenant: q.Get("tenant"), Trigger: q.Get("trigger"), PageSize: size,
		PageToken: q.Get("page_token")})
	if err != nil {
		return adminOutput{}, err
	}
	out := gateListJSON{Gates: []gateJSON{}, NextPageToken: r.GetNextPageToken()}
	for _, g := range r.GetGates() {
		out.Gates = append(out.Gates, gateOf(g))
	}
	return adminOutput{body: out}, nil
}

func (s *Server) getGate(ctx context.Context, in *adminInput) (adminOutput, error) {
	id := in.params["gate_id"]
	if !gateIDPattern.MatchString(id) {
		return adminOutput{}, problemNotFound
	}
	r, err := s.Gates.GetGate(ctx, &adminv1.GetGateRequest{GateId: id})
	if err != nil {
		return adminOutput{}, err
	}
	out := gateOf(r.GetGate())
	out.Report = r.GetReportMarkdown()
	return adminOutput{body: out}, nil
}
