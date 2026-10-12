package api

import (
	"context"
	"net/http"
	"regexp"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 索引版本（入库管道详设 14.4）：GET /v1/admin/index-versions、POST .../{version}/activate、
// POST .../operations/{operation_id}/approve，由 indexing 实现；切换或回滚需双人复核。

var (
	indexVersionPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	operationIDPattern  = regexp.MustCompile(`^op_[a-f0-9]{1,64}$`)
)

type indexVersionJSON struct {
	Version         string `json:"version"`
	Status          string `json:"status"`
	EmbeddingModel  string `json:"embedding_model"`
	ChunkerVersion  string `json:"chunker_version"`
	DocCount        int64  `json:"doc_count"`
	MissingDocCount int64  `json:"missing_doc_count"`
	CreatedAt       string `json:"created_at"`
	PublishedAt     string `json:"published_at,omitempty"`
}

type indexVersionListJSON struct {
	Versions      []indexVersionJSON `json:"versions"`
	NextPageToken string             `json:"next_page_token,omitempty"`
}

func (s *Server) listIndexVersions(ctx context.Context, in *adminInput) (adminOutput, error) {
	size, err := pageSize(in)
	if err != nil {
		return adminOutput{}, err
	}
	resp, err := s.IndexVersions.ListIndexVersions(ctx, &adminv1.ListIndexVersionsRequest{PageSize: size, PageToken: in.query.Get("page_token")})
	if err != nil {
		return adminOutput{}, err
	}
	out := indexVersionListJSON{Versions: []indexVersionJSON{}, NextPageToken: resp.GetNextPageToken()}
	for _, v := range resp.GetVersions() {
		out.Versions = append(out.Versions, indexVersionJSON{
			Version: v.GetVersion(), Status: enumName(v.GetStatus()), EmbeddingModel: v.GetEmbeddingModel(),
			ChunkerVersion: v.GetChunkerVersion(), DocCount: v.GetDocCount(), MissingDocCount: v.GetMissingDocCount(),
			CreatedAt: rfc3339(v.GetCreatedAt()), PublishedAt: rfc3339(v.GetPublishedAt()),
		})
	}
	return adminOutput{body: out}, nil
}

type operationJSON struct {
	OperationID   string `json:"operation_id"`
	Action        string `json:"action"`
	TargetVersion string `json:"target_version"`
	LiveVersion   string `json:"live_version"`
	Reason        string `json:"reason"`
	RequestedBy   string `json:"requested_by"`
	RequestedAt   string `json:"requested_at"`
	Status        string `json:"status"`
	ApprovedBy    string `json:"approved_by,omitempty"`
	ApprovedAt    string `json:"approved_at,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
	ExpiresAt     string `json:"expires_at"`
}

func operationOf(o *adminv1.Operation) operationJSON {
	return operationJSON{
		OperationID: o.GetOperationId(), Action: o.GetAction(), TargetVersion: o.GetTargetVersion(), LiveVersion: o.GetLiveVersion(),
		Reason: o.GetReason(), RequestedBy: o.GetRequestedBy(), RequestedAt: rfc3339(o.GetRequestedAt()), Status: enumName(o.GetStatus()),
		ApprovedBy: o.GetApprovedBy(), ApprovedAt: rfc3339(o.GetApprovedAt()), FailureReason: o.GetFailureReason(),
		ExpiresAt: rfc3339(o.GetExpiresAt()),
	}
}

type activateJSON struct {
	Reason string `json:"reason"`
}

// activateIndexVersion 只创建待复核的操作（202）；同一版本已有待复核的操作时返回它（200）。
func (s *Server) activateIndexVersion(ctx context.Context, in *adminInput) (adminOutput, error) {
	version := in.params["version"]
	if !indexVersionPattern.MatchString(version) {
		return adminOutput{}, problemNotFound
	}
	var req activateJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	resp, err := s.IndexVersions.ActivateIndexVersion(ctx, &adminv1.ActivateIndexVersionRequest{Version: version, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	code := http.StatusOK
	if resp.GetCreated() {
		code = http.StatusAccepted
	}
	return adminOutput{status: code, body: operationOf(resp.GetOperation())}, nil
}

func (s *Server) approveOperation(ctx context.Context, in *adminInput) (adminOutput, error) {
	id := in.params["operation_id"]
	if !operationIDPattern.MatchString(id) {
		return adminOutput{}, problemNotFound
	}
	if err := decodeBody(in.body, &struct{}{}); err != nil {
		return adminOutput{}, err
	}
	resp, err := s.IndexVersions.ApproveOperation(ctx, &adminv1.ApproveOperationRequest{OperationId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: operationOf(resp.GetOperation())}, nil
}
