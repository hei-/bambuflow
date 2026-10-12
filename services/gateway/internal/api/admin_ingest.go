package api

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 入库运维（入库管道详设 11、14.4）：入库状态、死信与隔离文档、按条件重放，由 processing 的管理服务实现，平台运维调用。

type ingestErrorJSON struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func ingestErrorOf(e *adminv1.IngestError) *ingestErrorJSON {
	if e == nil {
		return nil
	}
	return &ingestErrorJSON{Code: e.GetCode(), Message: e.GetMessage()}
}

type ingestStatusJSON struct {
	DocID     string           `json:"doc_id"`
	SourceID  string           `json:"source_id"`
	EventID   string           `json:"event_id"`
	Stage     string           `json:"stage"`
	State     string           `json:"state"`
	LastError *ingestErrorJSON `json:"last_error,omitempty"`
	Attempts  int32            `json:"attempts"`
	BatchID   string           `json:"ingest_batch_id,omitempty"`
	UpdatedAt string           `json:"updated_at"`
}

func (s *Server) ingestStatus(ctx context.Context, in *adminInput) (adminOutput, error) {
	r, err := s.Ingest.GetIngestStatus(ctx, &adminv1.GetIngestStatusRequest{DocId: in.params["doc_id"]})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: ingestStatusJSON{DocID: r.GetDocId(), SourceID: r.GetSourceId(), EventID: r.GetEventId(),
		Stage: r.GetStage(), State: r.GetState(), LastError: ingestErrorOf(r.GetLastError()), Attempts: r.GetAttempts(),
		BatchID: r.GetIngestBatchId(), UpdatedAt: rfc3339(r.GetUpdatedAt())}}, nil
}

type deadLetterJSON struct {
	DocID           string           `json:"doc_id"`
	SourceID        string           `json:"source_id"`
	EventID         string           `json:"event_id"`
	Stage           string           `json:"stage"`
	Error           *ingestErrorJSON `json:"error"`
	PipelineVersion string           `json:"pipeline_version"`
	Attempts        int32            `json:"attempts"`
	Replays         int32            `json:"replays"`
	Quarantined     bool             `json:"quarantined"`
	FirstFailedAt   string           `json:"first_failed_at"`
	LastFailedAt    string           `json:"last_failed_at"`
}

type deadLetterListJSON struct {
	DeadLetters   []deadLetterJSON `json:"dead_letters"`
	NextPageToken string           `json:"next_page_token,omitempty"`
}

func (s *Server) listDeadLetters(ctx context.Context, in *adminInput) (adminOutput, error) {
	size, err := pageSize(in)
	if err != nil {
		return adminOutput{}, err
	}
	q := in.query
	quarantined := false
	switch q.Get("quarantined") {
	case "", "false":
	case "true":
		quarantined = true
	default:
		return adminOutput{}, problemInvalid
	}
	r, err := s.Ingest.ListDeadLetters(ctx, &adminv1.ListDeadLettersRequest{SourceId: q.Get("source_id"), Stage: q.Get("stage"),
		ErrorCode: q.Get("error_code"), QuarantinedOnly: quarantined, PageSize: size, PageToken: q.Get("page_token")})
	if err != nil {
		return adminOutput{}, err
	}
	out := deadLetterListJSON{DeadLetters: []deadLetterJSON{}, NextPageToken: r.GetNextPageToken()}
	for _, d := range r.GetDeadLetters() {
		out.DeadLetters = append(out.DeadLetters, deadLetterJSON{DocID: d.GetDocId(), SourceID: d.GetSourceId(),
			EventID: d.GetEventId(), Stage: d.GetStage(), Error: ingestErrorOf(d.GetError()), PipelineVersion: d.GetPipelineVersion(),
			Attempts: d.GetAttempts(), Replays: d.GetReplays(), Quarantined: d.GetQuarantined(),
			FirstFailedAt: rfc3339(d.GetFirstFailedAt()), LastFailedAt: rfc3339(d.GetLastFailedAt())})
	}
	return adminOutput{body: out}, nil
}

type replayJSON struct {
	SourceID  string   `json:"source_id"`
	Stage     string   `json:"stage"`
	ErrorCode string   `json:"error_code"`
	DocIDs    []string `json:"doc_ids"`
	Reason    string   `json:"reason"`
	// 时间段、组件版本，以及是否也重新处理已成功入库的文档（入库管道详设第 11 节）
	From             string `json:"from"`
	To               string `json:"to"`
	PipelineVersion  string `json:"pipeline_version"`
	IncludeSucceeded bool   `json:"include_succeeded"`
}

type replayResultJSON struct {
	BatchID            string `json:"ingest_batch_id,omitempty"`
	Replayed           int32  `json:"replayed"`
	Reprocessed        int32  `json:"reprocessed"`
	SkippedQuarantined int32  `json:"skipped_quarantined"`
}

func (s *Server) replay(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req replayJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	pr := &adminv1.ReplayRequest{SourceId: req.SourceID, Stage: req.Stage, ErrorCode: req.ErrorCode,
		DocIds: req.DocIDs, Reason: req.Reason, PipelineVersion: req.PipelineVersion, IncludeSucceeded: req.IncludeSucceeded}
	for v, dst := range map[string]**timestamppb.Timestamp{req.From: &pr.From, req.To: &pr.To} {
		if v == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return adminOutput{}, problemInvalid
		}
		*dst = timestamppb.New(t)
	}
	r, err := s.Ingest.Replay(ctx, pr)
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: replayResultJSON{BatchID: r.GetIngestBatchId(), Replayed: r.GetReplayed(),
		SkippedQuarantined: r.GetSkippedQuarantined(), Reprocessed: r.GetReprocessed()}}, nil
}
