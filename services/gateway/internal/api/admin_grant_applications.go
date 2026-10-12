package api

import (
	"context"
	"net/http"
	"regexp"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 授权申请（governance 详设 6.2、9.1）：租户负责人申请、数据源所属部门的数据属主审批，由 governance 实现；
// 谁能看到哪些申请、能否审批由 governance 按角色与部门判断。

var (
	applicationIDPattern  = regexp.MustCompile(`^gra_[0-9a-f]{24}$`)
	applicationStatusName = map[string]adminv1.GrantApplicationStatus{
		"pending": adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_PENDING, "approved": adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_APPROVED,
		"rejected": adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_REJECTED, "withdrawn": adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_WITHDRAWN,
		"expired": adminv1.GrantApplicationStatus_GRANT_APPLICATION_STATUS_EXPIRED,
	}
)

type grantApplicationJSON struct {
	ApplicationID string `json:"application_id"`
	SourceID      string `json:"source_id"`
	Tenant        string `json:"tenant"`
	Reason        string `json:"reason"`
	Status        string `json:"status"`
	AppliedBy     string `json:"applied_by"`
	AppliedAt     string `json:"applied_at"`
	DecidedBy     string `json:"decided_by,omitempty"`
	DecidedAt     string `json:"decided_at,omitempty"`
	DecisionNote  string `json:"decision_note,omitempty"`
}

func grantApplicationOf(a *adminv1.GrantApplication) grantApplicationJSON {
	return grantApplicationJSON{ApplicationID: a.GetApplicationId(), SourceID: a.GetSourceId(), Tenant: a.GetTenant(), Reason: a.GetReason(),
		Status: enumName(a.GetStatus()), AppliedBy: a.GetAppliedBy(), AppliedAt: rfc3339(a.GetAppliedAt()), DecidedBy: a.GetDecidedBy(),
		DecidedAt: rfc3339(a.GetDecidedAt()), DecisionNote: a.GetDecisionNote()}
}

func (s *Server) listGrantApplications(ctx context.Context, in *adminInput) (adminOutput, error) {
	req := &adminv1.ListGrantApplicationsRequest{}
	if v := in.query.Get("status"); v != "" {
		st, ok := applicationStatusName[v]
		if !ok {
			return adminOutput{}, problemInvalid
		}
		req.Status = st
	}
	resp, err := s.Sources.ListGrantApplications(ctx, req)
	if err != nil {
		return adminOutput{}, err
	}
	out := struct {
		Applications []grantApplicationJSON `json:"applications"`
	}{Applications: []grantApplicationJSON{}}
	for _, a := range resp.GetApplications() {
		out.Applications = append(out.Applications, grantApplicationOf(a))
	}
	return adminOutput{body: out}, nil
}

func (s *Server) applyGrant(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req struct {
		SourceID string `json:"source_id"`
		Tenant   string `json:"tenant"`
		Reason   string `json:"reason"`
	}
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Sources.ApplyGrant(ctx, &adminv1.ApplyGrantRequest{SourceId: req.SourceID, Tenant: req.Tenant, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{status: http.StatusCreated, body: grantApplicationOf(resp.GetApplication())}, nil
}

func applicationID(in *adminInput) (string, error) {
	id := in.params["application_id"]
	if !applicationIDPattern.MatchString(id) {
		return "", problemNotFound
	}
	return id, nil
}

func (s *Server) approveGrantApplication(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := applicationID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req struct {
		Basis string `json:"basis"`
	}
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Sources.ApproveGrantApplication(ctx, &adminv1.ApproveGrantApplicationRequest{ApplicationId: id, Basis: req.Basis})
	if err != nil {
		return adminOutput{}, err
	}
	out := struct {
		grantApplicationJSON
		Grant grantJSON `json:"grant"`
	}{grantApplicationOf(resp.GetApplication()), grantOf(resp.GetGrant())}
	return adminOutput{body: out}, nil
}

func (s *Server) rejectGrantApplication(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := applicationID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req struct {
		Reason string `json:"reason"`
	}
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Sources.RejectGrantApplication(ctx, &adminv1.RejectGrantApplicationRequest{ApplicationId: id, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: grantApplicationOf(resp.GetApplication())}, nil
}
