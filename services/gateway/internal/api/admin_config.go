package api

import (
	"context"
	"net/http"
	"regexp"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 配置对象的版本、变更单与发布（governance 详设 3、4、7.2）：/v1/admin/tenants、/v1/admin/change-requests、
// /v1/admin/releases，由 governance 实现。现在的配置对象只有租户配置。

var (
	chgIDPattern = regexp.MustCompile(`^chg_[a-f0-9]{24}$`)
	relIDPattern = regexp.MustCompile(`^rel_[a-f0-9]{24}$`)
)

type configVersionJSON struct {
	VersionID   string `json:"version_id"`
	Object      string `json:"object"`
	Scope       string `json:"scope"`
	Number      int32  `json:"number"`
	Content     string `json:"content"`
	ContentHash string `json:"content_hash"`
	CreatedBy   string `json:"created_by"`
	CreatedAt   string `json:"created_at"`
	SourceRef   string `json:"source_ref"`
	Active      bool   `json:"active"`
}

func configVersionOf(v *adminv1.ConfigVersion) *configVersionJSON {
	if v == nil {
		return nil
	}
	return &configVersionJSON{VersionID: v.GetVersionId(), Object: v.GetObject(), Scope: v.GetScope(), Number: v.GetNumber(),
		Content: v.GetContent(), ContentHash: v.GetContentHash(), CreatedBy: v.GetCreatedBy(), CreatedAt: rfc3339(v.GetCreatedAt()),
		SourceRef: v.GetSourceRef(), Active: v.GetActive()}
}

type changeRequestJSON struct {
	ChgID         string             `json:"chg_id"`
	Object        string             `json:"object"`
	Scope         string             `json:"scope"`
	BaseVersionID string             `json:"base_version_id,omitempty"`
	Version       *configVersionJSON `json:"version"`
	Reason        string             `json:"reason"`
	GateID        string             `json:"gate_id,omitempty"`
	Author        string             `json:"author"`
	CreatedAt     string             `json:"created_at"`
	Status        string             `json:"status"`
	DecidedBy     string             `json:"decided_by,omitempty"`
	DecidedAt     string             `json:"decided_at,omitempty"`
	RejectReason  string             `json:"reject_reason,omitempty"`
	ReleaseID     string             `json:"release_id,omitempty"`
}

func changeRequestOf(c *adminv1.ChangeRequest) changeRequestJSON {
	return changeRequestJSON{ChgID: c.GetChgId(), Object: c.GetObject(), Scope: c.GetScope(), BaseVersionID: c.GetBaseVersionId(),
		Version: configVersionOf(c.GetVersion()), Reason: c.GetReason(), GateID: c.GetGateId(), Author: c.GetAuthor(),
		CreatedAt: rfc3339(c.GetCreatedAt()), Status: c.GetStatus(), DecidedBy: c.GetDecidedBy(), DecidedAt: rfc3339(c.GetDecidedAt()),
		RejectReason: c.GetRejectReason(), ReleaseID: c.GetReleaseId()}
}

type releaseJSON struct {
	ReleaseID     string             `json:"release_id"`
	Object        string             `json:"object"`
	Scope         string             `json:"scope"`
	Version       *configVersionJSON `json:"version"`
	FromVersionID string             `json:"from_version_id,omitempty"`
	Source        string             `json:"source"`
	SourceRef     string             `json:"source_ref"`
	GateID        string             `json:"gate_id,omitempty"`
	Status        string             `json:"status"`
	Reason        string             `json:"reason"`
	RequestedBy   string             `json:"requested_by"`
	RequestedAt   string             `json:"requested_at"`
	ApprovedBy    string             `json:"approved_by,omitempty"`
	ApprovedAt    string             `json:"approved_at,omitempty"`
	RolledBackBy  string             `json:"rolled_back_by,omitempty"`
	RolledBackAt  string             `json:"rolled_back_at,omitempty"`
}

func releaseOf(r *adminv1.Release) releaseJSON {
	return releaseJSON{ReleaseID: r.GetReleaseId(), Object: r.GetObject(), Scope: r.GetScope(), Version: configVersionOf(r.GetVersion()),
		FromVersionID: r.GetFromVersionId(), Source: r.GetSource(), SourceRef: r.GetSourceRef(), GateID: r.GetGateId(),
		Status: r.GetStatus(), Reason: r.GetReason(), RequestedBy: r.GetRequestedBy(), RequestedAt: rfc3339(r.GetRequestedAt()),
		ApprovedBy: r.GetApprovedBy(), ApprovedAt: rfc3339(r.GetApprovedAt()), RolledBackBy: r.GetRolledBackBy(),
		RolledBackAt: rfc3339(r.GetRolledBackAt())}
}

type tenantSummaryJSON struct {
	Tenant  string             `json:"tenant"`
	Rollout string             `json:"rollout"`
	Active  *configVersionJSON `json:"active"`
}

type tenantListJSON struct {
	Tenants []tenantSummaryJSON `json:"tenants"`
}

func (s *Server) listTenants(ctx context.Context, _ *adminInput) (adminOutput, error) {
	r, err := s.Configs.ListTenants(ctx, &adminv1.ListTenantsRequest{})
	if err != nil {
		return adminOutput{}, err
	}
	out := tenantListJSON{Tenants: []tenantSummaryJSON{}}
	for _, t := range r.GetTenants() {
		out.Tenants = append(out.Tenants, tenantSummaryJSON{Tenant: t.GetTenant(), Rollout: t.GetRollout(), Active: configVersionOf(t.GetActive())})
	}
	return adminOutput{body: out}, nil
}

type createTenantJSON struct {
	Tenant  string `json:"tenant"`
	Content string `json:"content"`
	Reason  string `json:"reason"`
	GateID  string `json:"gate_id"`
}

// createTenant 新建租户：生成一张没有 base 的变更单（201），审批后才生效。
func (s *Server) createTenant(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req createTenantJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	if !goldenTenantPattern.MatchString(req.Tenant) {
		return adminOutput{}, problemInvalid
	}
	r, err := s.Configs.CreateChangeRequest(ctx, &adminv1.CreateChangeRequestRequest{Object: "tenant", Scope: req.Tenant,
		Content: req.Content, Reason: req.Reason, GateId: req.GateID})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{status: http.StatusCreated, body: changeRequestOf(r.GetChangeRequest())}, nil
}

type configVersionListJSON struct {
	Versions []*configVersionJSON `json:"versions"`
}

func (s *Server) listConfigVersions(ctx context.Context, in *adminInput) (adminOutput, error) {
	tenant := in.params["tenant"]
	if !goldenTenantPattern.MatchString(tenant) {
		return adminOutput{}, problemNotFound
	}
	r, err := s.Configs.ListConfigVersions(ctx, &adminv1.ListConfigVersionsRequest{Object: "tenant", Scope: tenant})
	if err != nil {
		return adminOutput{}, err
	}
	out := configVersionListJSON{Versions: []*configVersionJSON{}}
	for _, v := range r.GetVersions() {
		out.Versions = append(out.Versions, configVersionOf(v))
	}
	return adminOutput{body: out}, nil
}

type createChangeRequestJSON struct {
	Object        string `json:"object"`
	Scope         string `json:"scope"`
	BaseVersionID string `json:"base_version_id"`
	Content       string `json:"content"`
	Reason        string `json:"reason"`
	GateID        string `json:"gate_id"`
}

func (s *Server) createChangeRequest(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req createChangeRequestJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Configs.CreateChangeRequest(ctx, &adminv1.CreateChangeRequestRequest{Object: req.Object, Scope: req.Scope,
		BaseVersionId: req.BaseVersionID, Content: req.Content, Reason: req.Reason, GateId: req.GateID})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{status: http.StatusCreated, body: changeRequestOf(r.GetChangeRequest())}, nil
}

func chgID(in *adminInput) (string, error) {
	id := in.params["chg_id"]
	if !chgIDPattern.MatchString(id) {
		return "", problemNotFound
	}
	return id, nil
}

func (s *Server) approveChangeRequest(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := chgID(in)
	if err != nil {
		return adminOutput{}, err
	}
	if err := decodeBody(in.body, &struct{}{}); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Configs.ApproveChangeRequest(ctx, &adminv1.ApproveChangeRequestRequest{ChgId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: changeRequestOf(r.GetChangeRequest())}, nil
}

func (s *Server) rejectChangeRequest(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := chgID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req activateJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Configs.RejectChangeRequest(ctx, &adminv1.RejectChangeRequestRequest{ChgId: id, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: changeRequestOf(r.GetChangeRequest())}, nil
}

type createReleaseJSON struct {
	Object    string `json:"object"`
	Scope     string `json:"scope"`
	Content   string `json:"content"`
	SourceRef string `json:"source_ref"`
	GateID    string `json:"gate_id"`
	Reason    string `json:"reason"`
}

// createRelease 由 CI 在 Git 合并后发起（202，待审批）。
func (s *Server) createRelease(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req createReleaseJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Configs.CreateRelease(ctx, &adminv1.CreateReleaseRequest{Object: req.Object, Scope: req.Scope, Content: req.Content,
		SourceRef: req.SourceRef, GateId: req.GateID, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{status: http.StatusAccepted, body: releaseOf(r.GetRelease())}, nil
}

func relID(in *adminInput) (string, error) {
	id := in.params["rel_id"]
	if !relIDPattern.MatchString(id) {
		return "", problemNotFound
	}
	return id, nil
}

func (s *Server) approveRelease(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := relID(in)
	if err != nil {
		return adminOutput{}, err
	}
	if err := decodeBody(in.body, &struct{}{}); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Configs.ApproveRelease(ctx, &adminv1.ApproveReleaseRequest{ReleaseId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: releaseOf(r.GetRelease())}, nil
}

func (s *Server) getRelease(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := relID(in)
	if err != nil {
		return adminOutput{}, err
	}
	r, err := s.Configs.GetRelease(ctx, &adminv1.GetReleaseRequest{ReleaseId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: releaseOf(r.GetRelease())}, nil
}

func (s *Server) rollbackRelease(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := relID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req activateJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Configs.RollbackRelease(ctx, &adminv1.RollbackReleaseRequest{ReleaseId: id, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: releaseOf(r.GetRelease())}, nil
}
