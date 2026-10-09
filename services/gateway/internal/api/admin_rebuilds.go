package api

import (
	"context"
	"net/http"
	"regexp"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 全量重建（入库管道详设第 10 节、14.4）：POST /v1/admin/index-rebuilds 只创建待复核的操作，
// 另一位平台运维 POST .../operations/{operation_id}/approve 后开始；GET .../{rebuild_id} 看进度，POST .../cancel 取消。

var rebuildIDPattern = regexp.MustCompile(`^rb_[a-f0-9]{1,64}$`)

type rebuildOperationJSON struct {
	OperationID    string `json:"operation_id"`
	EmbeddingModel string `json:"embedding_model"`
	ChunkerVersion string `json:"chunker_version"`
	LiveVersion    string `json:"live_version"`
	Reason         string `json:"reason"`
	RequestedBy    string `json:"requested_by"`
	RequestedAt    string `json:"requested_at"`
	Status         string `json:"status"`
	ApprovedBy     string `json:"approved_by,omitempty"`
	ApprovedAt     string `json:"approved_at,omitempty"`
	FailureReason  string `json:"failure_reason,omitempty"`
	ExpiresAt      string `json:"expires_at"`
	RebuildID      string `json:"rebuild_id,omitempty"`
}

func rebuildOperationOf(o *adminv1.RebuildOperation) rebuildOperationJSON {
	return rebuildOperationJSON{OperationID: o.GetOperationId(), EmbeddingModel: o.GetEmbeddingModel(), ChunkerVersion: o.GetChunkerVersion(),
		LiveVersion: o.GetLiveVersion(), Reason: o.GetReason(), RequestedBy: o.GetRequestedBy(), RequestedAt: rfc3339(o.GetRequestedAt()),
		Status: enumName(o.GetStatus()), ApprovedBy: o.GetApprovedBy(), ApprovedAt: rfc3339(o.GetApprovedAt()),
		FailureReason: o.GetFailureReason(), ExpiresAt: rfc3339(o.GetExpiresAt()), RebuildID: o.GetRebuildId()}
}

type rebuildJSON struct {
	RebuildID      string `json:"rebuild_id"`
	Version        string `json:"version"`
	EmbeddingModel string `json:"embedding_model"`
	ChunkerVersion string `json:"chunker_version"`
	Status         string `json:"status"`
	WrittenDocs    int64  `json:"written_docs"`
	TotalDocs      int64  `json:"total_docs"`
	RequestedBy    string `json:"requested_by"`
	ApprovedBy     string `json:"approved_by"`
	StartedAt      string `json:"started_at"`
	FinishedAt     string `json:"finished_at,omitempty"`
	CancelledBy    string `json:"cancelled_by,omitempty"`
	Reason         string `json:"reason"`
}

func rebuildOf(b *adminv1.Rebuild) rebuildJSON {
	return rebuildJSON{RebuildID: b.GetRebuildId(), Version: b.GetVersion(), EmbeddingModel: b.GetEmbeddingModel(),
		ChunkerVersion: b.GetChunkerVersion(), Status: b.GetStatus(), WrittenDocs: b.GetWrittenDocs(), TotalDocs: b.GetTotalDocs(),
		RequestedBy: b.GetRequestedBy(), ApprovedBy: b.GetApprovedBy(), StartedAt: rfc3339(b.GetStartedAt()),
		FinishedAt: rfc3339(b.GetFinishedAt()), CancelledBy: b.GetCancelledBy(), Reason: b.GetReason()}
}

type startRebuildJSON struct {
	EmbeddingModel string `json:"embedding_model"`
	ChunkerVersion string `json:"chunker_version"`
	Reason         string `json:"reason"`
}

// startRebuild 只创建待复核的操作（202）；已有待复核的申请时返回它（200）。
func (s *Server) startRebuild(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req startRebuildJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Rebuilds.StartRebuild(ctx, &adminv1.StartRebuildRequest{EmbeddingModel: req.EmbeddingModel,
		ChunkerVersion: req.ChunkerVersion, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	code := http.StatusOK
	if r.GetCreated() {
		code = http.StatusAccepted
	}
	return adminOutput{status: code, body: rebuildOperationOf(r.GetOperation())}, nil
}

func (s *Server) approveRebuild(ctx context.Context, in *adminInput) (adminOutput, error) {
	id := in.params["operation_id"]
	if !operationIDPattern.MatchString(id) {
		return adminOutput{}, problemNotFound
	}
	if err := decodeBody(in.body, &struct{}{}); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Rebuilds.ApproveRebuild(ctx, &adminv1.ApproveRebuildRequest{OperationId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: rebuildOperationOf(r.GetOperation())}, nil
}

func (s *Server) getRebuild(ctx context.Context, in *adminInput) (adminOutput, error) {
	id := in.params["rebuild_id"]
	if !rebuildIDPattern.MatchString(id) {
		return adminOutput{}, problemNotFound
	}
	r, err := s.Rebuilds.GetRebuild(ctx, &adminv1.GetRebuildRequest{RebuildId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: rebuildOf(r.GetRebuild())}, nil
}

func (s *Server) cancelRebuild(ctx context.Context, in *adminInput) (adminOutput, error) {
	id := in.params["rebuild_id"]
	if !rebuildIDPattern.MatchString(id) {
		return adminOutput{}, problemNotFound
	}
	var req activateJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Rebuilds.CancelRebuild(ctx, &adminv1.CancelRebuildRequest{RebuildId: id, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: rebuildOf(r.GetRebuild())}, nil
}
