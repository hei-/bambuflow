package api

import (
	"context"
	"net/http"
	"regexp"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 配置对象的版本、变更单与发布（governance 详设 3、4、7.2）：/v1/admin/tenants、/v1/admin/change-requests、
// /v1/admin/releases、/v1/admin/config-versions，由 governance 实现。配置对象有租户配置（变更单审批后生效）与
// pipeline、routing、policy、prompt（Git 类：console 的修改写成合并请求，合并后由 CI 发布）。

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
	// 策略层的差异分析：生效后可见范围变大的代表性主体
	Widened []string `json:"widened,omitempty"`
}

func configVersionOf(v *adminv1.ConfigVersion) *configVersionJSON {
	if v == nil {
		return nil
	}
	return &configVersionJSON{VersionID: v.GetVersionId(), Object: v.GetObject(), Scope: v.GetScope(), Number: v.GetNumber(),
		Content: v.GetContent(), ContentHash: v.GetContentHash(), CreatedBy: v.GetCreatedBy(), CreatedAt: rfc3339(v.GetCreatedAt()),
		SourceRef: v.GetSourceRef(), Active: v.GetActive(), Widened: v.GetWidened()}
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
	// Git 类对象写成的合并请求
	MergeRequestURL string `json:"merge_request_url,omitempty"`
}

func changeRequestOf(c *adminv1.ChangeRequest) changeRequestJSON {
	return changeRequestJSON{ChgID: c.GetChgId(), Object: c.GetObject(), Scope: c.GetScope(), BaseVersionID: c.GetBaseVersionId(),
		Version: configVersionOf(c.GetVersion()), Reason: c.GetReason(), GateID: c.GetGateId(), Author: c.GetAuthor(),
		CreatedAt: rfc3339(c.GetCreatedAt()), Status: c.GetStatus(), DecidedBy: c.GetDecidedBy(), DecidedAt: rfc3339(c.GetDecidedAt()),
		RejectReason: c.GetRejectReason(), ReleaseID: c.GetReleaseId(), MergeRequestURL: c.GetMergeRequestUrl()}
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
	// 策略层的第二位审批人
	SecondApprovedBy string `json:"second_approved_by,omitempty"`
	SecondApprovedAt string `json:"second_approved_at,omitempty"`
	CanaryTenant     string `json:"canary_tenant,omitempty"`
	// status 为 rolling_out 时的灰度范围
	Rollout *rolloutJSON `json:"rollout,omitempty"`
}

type rolloutJSON struct {
	Tenant      string `json:"tenant"`
	Percent     int32  `json:"percent"`
	StartedAt   string `json:"started_at"`
	Observation string `json:"observation,omitempty"`
	ObservedAt  string `json:"observed_at,omitempty"`
}

func releaseOf(r *adminv1.Release) releaseJSON {
	var ro *rolloutJSON
	if x := r.GetRollout(); x != nil {
		ro = &rolloutJSON{Tenant: x.GetTenant(), Percent: x.GetPercent(), StartedAt: rfc3339(x.GetStartedAt()),
			Observation: x.GetObservation(), ObservedAt: rfc3339(x.GetObservedAt())}
	}
	return releaseJSON{CanaryTenant: r.GetCanaryTenant(), Rollout: ro, ReleaseID: r.GetReleaseId(), Object: r.GetObject(), Scope: r.GetScope(), Version: configVersionOf(r.GetVersion()),
		FromVersionID: r.GetFromVersionId(), Source: r.GetSource(), SourceRef: r.GetSourceRef(), GateID: r.GetGateId(),
		Status: r.GetStatus(), Reason: r.GetReason(), RequestedBy: r.GetRequestedBy(), RequestedAt: rfc3339(r.GetRequestedAt()),
		ApprovedBy: r.GetApprovedBy(), ApprovedAt: rfc3339(r.GetApprovedAt()), RolledBackBy: r.GetRolledBackBy(),
		RolledBackAt: rfc3339(r.GetRolledBackAt()), SecondApprovedBy: r.GetSecondApprovedBy(),
		SecondApprovedAt: rfc3339(r.GetSecondApprovedAt())}
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

// configObjects 是 /v1/admin/config-versions 可查的对象；租户配置另有 /v1/admin/tenants/{tenant}/config-versions
var configObjects = map[string]bool{"tenant": true, "pipeline": true, "routing": true, "policy": true, "prompt": true}

// listObjectVersions 按对象与作用域列出版本（console 编辑 Git 类对象时取当前生效内容）。作用域格式由 governance 校验。
func (s *Server) listObjectVersions(ctx context.Context, in *adminInput) (adminOutput, error) {
	object, scope := in.query.Get("object"), in.query.Get("scope")
	if !configObjects[object] || scope == "" || len(scope) > 128 {
		return adminOutput{}, problemInvalid
	}
	r, err := s.Configs.ListConfigVersions(ctx, &adminv1.ListConfigVersionsRequest{Object: object, Scope: scope})
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

// listChangeRequests、listReleases 按状态列出（新的在前，最多 100 条）；状态是否合法由 governance 判断。
func (s *Server) listChangeRequests(ctx context.Context, in *adminInput) (adminOutput, error) {
	r, err := s.Configs.ListChangeRequests(ctx, &adminv1.ListChangeRequestsRequest{Status: in.query.Get("status")})
	if err != nil {
		return adminOutput{}, err
	}
	out := struct {
		ChangeRequests []changeRequestJSON `json:"change_requests"`
	}{ChangeRequests: []changeRequestJSON{}}
	for _, c := range r.GetChangeRequests() {
		out.ChangeRequests = append(out.ChangeRequests, changeRequestOf(c))
	}
	return adminOutput{body: out}, nil
}

func (s *Server) listReleases(ctx context.Context, in *adminInput) (adminOutput, error) {
	r, err := s.Configs.ListReleases(ctx, &adminv1.ListReleasesRequest{Status: in.query.Get("status")})
	if err != nil {
		return adminOutput{}, err
	}
	out := struct {
		Releases []releaseJSON `json:"releases"`
	}{Releases: []releaseJSON{}}
	for _, x := range r.GetReleases() {
		out.Releases = append(out.Releases, releaseOf(x))
	}
	return adminOutput{body: out}, nil
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
	// 全局对象灰度的试点租户；不填则审批时选第一个试点阶段的租户
	CanaryTenant string `json:"canary_tenant"`
}

// createRelease 由 CI 在 Git 合并后发起（202，待审批）。
func (s *Server) createRelease(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req createReleaseJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Configs.CreateRelease(ctx, &adminv1.CreateReleaseRequest{Object: req.Object, Scope: req.Scope, Content: req.Content,
		SourceRef: req.SourceRef, GateId: req.GateID, Reason: req.Reason, CanaryTenant: req.CanaryTenant})
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

// promoteRelease 让灰度中的发布全量生效。
func (s *Server) promoteRelease(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := relID(in)
	if err != nil {
		return adminOutput{}, err
	}
	if err := decodeBody(in.body, &struct{}{}); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Configs.PromoteRelease(ctx, &adminv1.PromoteReleaseRequest{ReleaseId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: releaseOf(r.GetRelease())}, nil
}

// abortRollout 终止灰度。
func (s *Server) abortRollout(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := relID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req activateJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Configs.AbortRollout(ctx, &adminv1.AbortRolloutRequest{ReleaseId: id, Reason: req.Reason})
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
