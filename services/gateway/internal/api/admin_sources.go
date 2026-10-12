package api

import (
	"context"
	"net/http"
	"regexp"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
)

// 数据源注册、审批、撤销与租户授权（技术设计文档 13.2，governance 详设 6.2、7.2），由 governance 实现：
// GET/POST /v1/admin/sources、POST .../{src_id}/approve、POST .../{src_id}/revoke、GET/POST/DELETE .../{src_id}/grants。

var (
	sourceIDPattern = regexp.MustCompile(`^src_[A-Za-z0-9]{1,64}$`)
	sourceTypeNames = map[string]commonv1.SourceType{
		"wiki": commonv1.SourceType_SOURCE_TYPE_WIKI, "mail": commonv1.SourceType_SOURCE_TYPE_MAIL,
		"file": commonv1.SourceType_SOURCE_TYPE_FILE, "image": commonv1.SourceType_SOURCE_TYPE_IMAGE,
		"table": commonv1.SourceType_SOURCE_TYPE_TABLE, "log": commonv1.SourceType_SOURCE_TYPE_LOG,
		"db": commonv1.SourceType_SOURCE_TYPE_DB,
	}
	classificationNames = map[string]commonv1.Classification{
		"public": commonv1.Classification_CLASSIFICATION_PUBLIC, "internal": commonv1.Classification_CLASSIFICATION_INTERNAL,
		"confidential": commonv1.Classification_CLASSIFICATION_CONFIDENTIAL, "restricted": commonv1.Classification_CLASSIFICATION_RESTRICTED,
	}
	sourceStatusNames = map[string]adminv1.SourceStatus{
		"pending": adminv1.SourceStatus_SOURCE_STATUS_PENDING, "active": adminv1.SourceStatus_SOURCE_STATUS_ACTIVE,
		"revoked": adminv1.SourceStatus_SOURCE_STATUS_REVOKED,
	}
	permissionModelNames = map[string]adminv1.PermissionModel{
		"": adminv1.PermissionModel_PERMISSION_MODEL_DOCUMENT, "document": adminv1.PermissionModel_PERMISSION_MODEL_DOCUMENT,
		"partition": adminv1.PermissionModel_PERMISSION_MODEL_PARTITION,
	}
	approvalCapacities = map[string]adminv1.ApprovalCapacity{
		"": adminv1.ApprovalCapacity_APPROVAL_CAPACITY_DATA_OWNER, "data_owner": adminv1.ApprovalCapacity_APPROVAL_CAPACITY_DATA_OWNER,
		"infosec": adminv1.ApprovalCapacity_APPROVAL_CAPACITY_INFOSEC,
	}
)

type sourceJSON struct {
	SourceID              string `json:"source_id"`
	Type                  string `json:"type"`
	OwnerDept             string `json:"owner_dept"`
	DefaultClassification string `json:"default_classification"`
	Description           string `json:"description,omitempty"`
	Status                string `json:"status"`
	CreatedBy             string `json:"created_by"`
	CreatedAt             string `json:"created_at"`
	ApprovedBy            string `json:"approved_by,omitempty"`
	ApprovedAt            string `json:"approved_at,omitempty"`
	ApprovalBasis         string `json:"approval_basis,omitempty"`
	RevokedBy             string `json:"revoked_by,omitempty"`
	RevokedAt             string `json:"revoked_at,omitempty"`
	RevokeReason          string `json:"revoke_reason,omitempty"`
	// 信息安全的审批，只有受限级数据源有
	SecurityApprovedBy    string `json:"security_approved_by,omitempty"`
	SecurityApprovedAt    string `json:"security_approved_at,omitempty"`
	SecurityApprovalBasis string `json:"security_approval_basis,omitempty"`
	PushClient            string `json:"push_client,omitempty"`
	PermissionModel       string `json:"permission_model"`
	// 只有撤销的响应带：撤销时写入的按数据源的紧急拒绝规则
	DenialID string `json:"denial_id,omitempty"`
}

func sourceOf(x *adminv1.Source) sourceJSON {
	return sourceJSON{
		SourceID: x.GetSourceId(), Type: enumName(x.GetType()), OwnerDept: x.GetOwnerDept(),
		DefaultClassification: enumName(x.GetDefaultClassification()), Description: x.GetDescription(), Status: enumName(x.GetStatus()),
		CreatedBy: x.GetCreatedBy(), CreatedAt: rfc3339(x.GetCreatedAt()), ApprovedBy: x.GetApprovedBy(), ApprovedAt: rfc3339(x.GetApprovedAt()),
		ApprovalBasis: x.GetApprovalBasis(), RevokedBy: x.GetRevokedBy(), RevokedAt: rfc3339(x.GetRevokedAt()), RevokeReason: x.GetRevokeReason(),
		SecurityApprovedBy: x.GetSecurityApprovedBy(), SecurityApprovedAt: rfc3339(x.GetSecurityApprovedAt()),
		SecurityApprovalBasis: x.GetSecurityApprovalBasis(), PushClient: x.GetPushClient(), PermissionModel: enumName(x.GetPermissionModel()),
	}
}

// listSources 列出数据源，可按状态筛选；能看到哪些由 governance 按角色决定（数据属主只看本部门）。
func (s *Server) listSources(ctx context.Context, in *adminInput) (adminOutput, error) {
	req := &adminv1.ListSourcesRequest{}
	if v := in.query.Get("status"); v != "" {
		st, ok := sourceStatusNames[v]
		if !ok {
			return adminOutput{}, problemInvalid
		}
		req.Status = st
	}
	resp, err := s.Sources.ListSources(ctx, req)
	if err != nil {
		return adminOutput{}, err
	}
	out := struct {
		Sources []sourceJSON `json:"sources"`
	}{Sources: []sourceJSON{}}
	for _, x := range resp.GetSources() {
		out.Sources = append(out.Sources, sourceOf(x))
	}
	return adminOutput{body: out}, nil
}

type registerSourceJSON struct {
	Type                  string `json:"type"`
	OwnerDept             string `json:"owner_dept"`
	DefaultClassification string `json:"default_classification"`
	Description           string `json:"description"`
	PushClient            string `json:"push_client"`
	// document（默认）或 partition
	PermissionModel string `json:"permission_model"`
}

func (s *Server) registerSource(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req registerSourceJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	typ, ok1 := sourceTypeNames[req.Type]
	cls, ok2 := classificationNames[req.DefaultClassification]
	model, ok3 := permissionModelNames[req.PermissionModel]
	if !ok1 || !ok2 || !ok3 {
		return adminOutput{}, problemInvalid
	}
	resp, err := s.Sources.RegisterSource(ctx, &adminv1.RegisterSourceRequest{
		Type: typ, OwnerDept: req.OwnerDept, DefaultClassification: cls, Description: req.Description, PushClient: req.PushClient,
		PermissionModel: model,
	})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{status: http.StatusCreated, body: sourceOf(resp.GetSource())}, nil
}

// sourceID 取路径里的数据源 ID；格式不对按不存在处理。
func sourceID(in *adminInput) (string, error) {
	id := in.params["src_id"]
	if !sourceIDPattern.MatchString(id) {
		return "", problemNotFound
	}
	return id, nil
}

func (s *Server) approveSource(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := sourceID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req struct {
		Basis string `json:"basis"`
		// data_owner（默认）或 infosec（受限级数据源的信息安全审批）
		Capacity string `json:"capacity"`
	}
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	capacity, ok := approvalCapacities[req.Capacity]
	if !ok {
		return adminOutput{}, problemInvalid
	}
	resp, err := s.Sources.ApproveSource(ctx, &adminv1.ApproveSourceRequest{SourceId: id, Basis: req.Basis, Capacity: capacity})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: sourceOf(resp.GetSource())}, nil
}

func (s *Server) revokeSource(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := sourceID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req struct {
		Reason string `json:"reason"`
	}
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Sources.RevokeSource(ctx, &adminv1.RevokeSourceRequest{SourceId: id, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	out := sourceOf(resp.GetSource())
	out.DenialID = resp.GetDenialId()
	return adminOutput{body: out}, nil
}

type grantJSON struct {
	SourceID  string `json:"source_id"`
	Tenant    string `json:"tenant"`
	GrantedBy string `json:"granted_by,omitempty"`
	GrantedAt string `json:"granted_at,omitempty"`
	// 经授权申请审批的授权才有
	ApplicationID string `json:"application_id,omitempty"`
}

func grantOf(g *adminv1.Grant) grantJSON {
	return grantJSON{SourceID: g.GetSourceId(), Tenant: g.GetTenant(), GrantedBy: g.GetGrantedBy(), GrantedAt: rfc3339(g.GetGrantedAt()),
		ApplicationID: g.GetApplicationId()}
}

func (s *Server) listGrants(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := sourceID(in)
	if err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Sources.ListGrants(ctx, &adminv1.ListGrantsRequest{SourceId: id})
	if err != nil {
		return adminOutput{}, err
	}
	out := struct {
		Grants []grantJSON `json:"grants"`
	}{Grants: []grantJSON{}}
	for _, g := range resp.GetGrants() {
		out.Grants = append(out.Grants, grantOf(g))
	}
	return adminOutput{body: out}, nil
}

func (s *Server) createGrant(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := sourceID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req struct {
		Tenant string `json:"tenant"`
	}
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Sources.CreateGrant(ctx, &adminv1.CreateGrantRequest{SourceId: id, Tenant: req.Tenant})
	if err != nil {
		return adminOutput{}, err
	}
	code := http.StatusOK
	if resp.GetCreated() {
		code = http.StatusCreated
	}
	return adminOutput{status: code, body: grantOf(resp.GetGrant())}, nil
}

// deleteGrant 取消授权：租户放在查询参数 tenant 里（DELETE 不带请求体）。
func (s *Server) deleteGrant(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := sourceID(in)
	if err != nil {
		return adminOutput{}, err
	}
	if len(in.body) > 0 {
		return adminOutput{}, problemInvalid
	}
	tenant := in.query.Get("tenant")
	if tenant == "" {
		return adminOutput{}, problemInvalid
	}
	if _, err := s.Sources.DeleteGrant(ctx, &adminv1.DeleteGrantRequest{SourceId: id, Tenant: tenant}); err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: grantJSON{SourceID: id, Tenant: tenant}}, nil
}
