package api

import (
	"context"
	"net/http"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 撤销删除标记（入库管道详设第 5 节、14.4）：POST /v1/admin/tombstone-revocations 只创建待复核的操作，
// 另一位平台运维 POST .../operations/{operation_id}/approve 后由 indexing 执行。

type revocationJSON struct {
	OperationID     string   `json:"operation_id"`
	SourceID        string   `json:"source_id"`
	TombstonedSince string   `json:"tombstoned_since"`
	TombstonedUntil string   `json:"tombstoned_until"`
	DocIDs          []string `json:"doc_ids"`
	Reason          string   `json:"reason"`
	MatchedDocs     int32    `json:"matched_docs"`
	RevokedDocs     int32    `json:"revoked_docs"`
	RequestedBy     string   `json:"requested_by"`
	RequestedAt     string   `json:"requested_at"`
	Status          string   `json:"status"`
	ApprovedBy      string   `json:"approved_by,omitempty"`
	ApprovedAt      string   `json:"approved_at,omitempty"`
	FailureReason   string   `json:"failure_reason,omitempty"`
	ExpiresAt       string   `json:"expires_at"`
	RefetchEventID  string   `json:"refetch_event_id,omitempty"`
}

func revocationOf(r *adminv1.TombstoneRevocation) revocationJSON {
	return revocationJSON{
		OperationID: r.GetOperationId(), SourceID: r.GetSourceId(), TombstonedSince: rfc3339(r.GetTombstonedSince()),
		TombstonedUntil: rfc3339(r.GetTombstonedUntil()), DocIDs: nonNil(r.GetDocIds()), Reason: r.GetReason(),
		MatchedDocs: r.GetMatchedDocs(), RevokedDocs: r.GetRevokedDocs(), RequestedBy: r.GetRequestedBy(),
		RequestedAt: rfc3339(r.GetRequestedAt()), Status: enumName(r.GetStatus()), ApprovedBy: r.GetApprovedBy(),
		ApprovedAt: rfc3339(r.GetApprovedAt()), FailureReason: r.GetFailureReason(), ExpiresAt: rfc3339(r.GetExpiresAt()),
		RefetchEventID: r.GetRefetchEventId(),
	}
}

type revokeJSON struct {
	SourceID        string   `json:"source_id"`
	TombstonedSince string   `json:"tombstoned_since"`
	TombstonedUntil string   `json:"tombstoned_until"`
	DocIDs          []string `json:"doc_ids"`
	Reason          string   `json:"reason"`
}

// revokeTombstones 只创建待复核的操作（202）；该数据源已有待复核的撤销时返回它（200）。
func (s *Server) revokeTombstones(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req revokeJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	since, err1 := time.Parse(time.RFC3339, req.TombstonedSince)
	until, err2 := time.Parse(time.RFC3339, req.TombstonedUntil)
	if err1 != nil || err2 != nil {
		return adminOutput{}, problemInvalid
	}
	resp, err := s.Tombstones.RevokeTombstones(ctx, &adminv1.RevokeTombstonesRequest{SourceId: req.SourceID,
		TombstonedSince: timestamppb.New(since), TombstonedUntil: timestamppb.New(until), DocIds: req.DocIDs, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	code := http.StatusOK
	if resp.GetCreated() {
		code = http.StatusAccepted
	}
	return adminOutput{status: code, body: revocationOf(resp.GetRevocation())}, nil
}

func (s *Server) approveRevocation(ctx context.Context, in *adminInput) (adminOutput, error) {
	id := in.params["operation_id"]
	if !operationIDPattern.MatchString(id) {
		return adminOutput{}, problemNotFound
	}
	if err := decodeBody(in.body, &struct{}{}); err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Tombstones.ApproveRevocation(ctx, &adminv1.ApproveRevocationRequest{OperationId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: revocationOf(resp.GetRevocation())}, nil
}
