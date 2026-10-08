package api

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 审计查询（技术设计文档 13.2，governance 详设 7.2）：GET /v1/admin/audit?sub=&doc_id=&from=&to=，由 governance 实现。

type auditRecordJSON struct {
	Time              string   `json:"time"`
	TraceID           string   `json:"trace_id"`
	AnswerID          string   `json:"answer_id,omitempty"`
	BatchID           string   `json:"batch_id,omitempty"`
	Sub               string   `json:"sub,omitempty"`
	Tenant            string   `json:"tenant,omitempty"`
	Actor             string   `json:"actor,omitempty"`
	Endpoint          string   `json:"endpoint"`
	Status            int32    `json:"status"`
	Reason            string   `json:"reason,omitempty"`
	LatencyMS         int64    `json:"latency_ms"`
	CandidateChunkIDs []string `json:"candidate_chunk_ids"`
	ContextChunkIDs   []string `json:"context_chunk_ids"`
	ShownChunkIDs     []string `json:"shown_chunk_ids"`
	ListedDocIDs      []string `json:"listed_doc_ids"`
	// 按文档查询时，文档出现在哪几类里
	MatchedIn   []string `json:"matched_in,omitempty"`
	AdminMethod string   `json:"admin_method,omitempty"`
	AdminPath   string   `json:"admin_path,omitempty"`
}

type auditListJSON struct {
	Records       []auditRecordJSON `json:"records"`
	NextPageToken string            `json:"next_page_token,omitempty"`
}

func (s *Server) queryAudit(ctx context.Context, in *adminInput) (adminOutput, error) {
	size, err := pageSize(in)
	if err != nil {
		return adminOutput{}, err
	}
	from, err1 := time.Parse(time.RFC3339, in.query.Get("from"))
	to, err2 := time.Parse(time.RFC3339, in.query.Get("to"))
	if err1 != nil || err2 != nil {
		return adminOutput{}, problemInvalid
	}
	resp, err := s.AuditQuery.QueryAudit(ctx, &adminv1.QueryAuditRequest{
		Sub: in.query.Get("sub"), DocId: in.query.Get("doc_id"), From: timestamppb.New(from), To: timestamppb.New(to),
		PageSize: size, PageToken: in.query.Get("page_token"),
	})
	if err != nil {
		return adminOutput{}, err
	}
	out := auditListJSON{Records: []auditRecordJSON{}, NextPageToken: resp.GetNextPageToken()}
	for _, r := range resp.GetRecords() {
		out.Records = append(out.Records, auditRecordJSON{
			Time: r.GetTime().AsTime().UTC().Format(time.RFC3339Nano), TraceID: r.GetTraceId(), AnswerID: r.GetAnswerId(),
			BatchID: r.GetBatchId(), Sub: r.GetSub(), Tenant: r.GetTenant(), Actor: r.GetActor(), Endpoint: r.GetEndpoint(),
			Status: r.GetStatus(), Reason: r.GetReason(), LatencyMS: r.GetLatencyMs(),
			CandidateChunkIDs: nonNil(r.GetCandidateChunkIds()), ContextChunkIDs: nonNil(r.GetContextChunkIds()),
			ShownChunkIDs: nonNil(r.GetShownChunkIds()), ListedDocIDs: nonNil(r.GetListedDocIds()), MatchedIn: r.GetMatchedIn(),
			AdminMethod: r.GetAdminMethod(), AdminPath: r.GetAdminPath(),
		})
	}
	return adminOutput{body: out}, nil
}
