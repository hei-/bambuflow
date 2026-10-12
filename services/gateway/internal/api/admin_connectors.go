package api

import (
	"context"
	"regexp"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 数据源同步的运维接口（入库管道详设 14.4）：GET /v1/admin/sources/{src_id}/sync-status、POST .../pause、.../resume、.../sync，
// 由 connectors 的管理服务实现。

type syncRunJSON struct {
	Command             string `json:"command"`
	StartedAt           string `json:"started_at"`
	FinishedAt          string `json:"finished_at"`
	Result              string `json:"result"`
	Error               string `json:"error,omitempty"`
	Documents           int32  `json:"documents"`
	Upserts             int32  `json:"upserts"`
	ACLOnly             int32  `json:"acl_only"`
	Deletes             int32  `json:"deletes"`
	DeletesPaused       bool   `json:"deletes_paused"`
	DeletesPausedReason string `json:"deletes_paused_reason,omitempty"`
	Refetched           int32  `json:"refetched"`
	// 对账才有
	Reconciliation *reconciliationJSON `json:"reconciliation,omitempty"`
	// 清单里没能读出、本轮跳过的文档
	Skipped      int32    `json:"skipped,omitempty"`
	SkippedItems []string `json:"skipped_items,omitempty"`
}

type reconciliationJSON struct {
	Missing            int32 `json:"missing"`
	ContentMismatch    int32 `json:"content_mismatch"`
	ACLMismatch        int32 `json:"acl_mismatch"`
	Orphaned           int32 `json:"orphaned"`
	TombstonedInSource int32 `json:"tombstoned_in_source"`
	Indexed            int32 `json:"indexed"`
}

func syncRunOf(r *adminv1.SyncRun) *syncRunJSON {
	if r == nil {
		return nil
	}
	return &syncRunJSON{Command: r.GetCommand(), StartedAt: rfc3339(r.GetStartedAt()), FinishedAt: rfc3339(r.GetFinishedAt()),
		Result: r.GetResult(), Error: r.GetError(), Documents: r.GetDocuments(), Upserts: r.GetUpserts(), ACLOnly: r.GetAclOnly(),
		Deletes: r.GetDeletes(), DeletesPaused: r.GetDeletesPaused(), DeletesPausedReason: r.GetDeletesPausedReason(),
		Refetched: r.GetRefetched(), Reconciliation: reconciliationOf(r.GetReconciliation()), Skipped: r.GetSkipped(),
		SkippedItems: r.GetSkippedItems()}
}

func reconciliationOf(c *adminv1.Reconciliation) *reconciliationJSON {
	if c == nil {
		return nil
	}
	return &reconciliationJSON{Missing: c.GetMissing(), ContentMismatch: c.GetContentMismatch(), ACLMismatch: c.GetAclMismatch(),
		Orphaned: c.GetOrphaned(), TombstonedInSource: c.GetTombstonedInSource(), Indexed: c.GetIndexed()}
}

type pauseJSON struct {
	PausedBy string `json:"paused_by"`
	PausedAt string `json:"paused_at"`
	Reason   string `json:"reason"`
}

func pauseOf(p *adminv1.Pause) *pauseJSON {
	if p == nil {
		return nil
	}
	return &pauseJSON{PausedBy: p.GetPausedBy(), PausedAt: rfc3339(p.GetPausedAt()), Reason: p.GetReason()}
}

type syncStatusJSON struct {
	SourceID         string       `json:"source_id"`
	LastRun          *syncRunJSON `json:"last_run,omitempty"`
	LastSuccess      *syncRunJSON `json:"last_success,omitempty"`
	FreshnessSeconds int64        `json:"freshness_seconds"`
	Pause            *pauseJSON   `json:"pause,omitempty"`
	LastReconcile    *syncRunJSON `json:"last_reconcile,omitempty"`
	RevokedAt        string       `json:"revoked_at,omitempty"`
	// 常驻清单轮询的当前间隔与下一轮时间；按定时任务同步的源没有
	ListingIntervalSeconds int64  `json:"listing_interval_seconds,omitempty"`
	NextRoundAt            string `json:"next_round_at,omitempty"`
}

func (s *Server) syncStatus(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := sourceID(in)
	if err != nil {
		return adminOutput{}, err
	}
	r, err := s.Connectors.GetSyncStatus(ctx, &adminv1.GetSyncStatusRequest{SourceId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: syncStatusJSON{SourceID: r.GetSourceId(), LastRun: syncRunOf(r.GetLastRun()),
		LastSuccess: syncRunOf(r.GetLastSuccess()), FreshnessSeconds: r.GetFreshnessSeconds(), Pause: pauseOf(r.GetPause()),
		LastReconcile: syncRunOf(r.GetLastReconcile()), RevokedAt: rfc3339(r.GetRevokedAt()),
		ListingIntervalSeconds: r.GetListingIntervalSeconds(), NextRoundAt: rfc3339(r.GetNextRoundAt())}}, nil
}

type syncNowResultJSON struct {
	Job         string `json:"job"`
	RequestedAt string `json:"requested_at"`
}

func (s *Server) syncSourceNow(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := sourceID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req pauseRequestJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Connectors.SyncNow(ctx, &adminv1.SyncNowRequest{SourceId: id, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: syncNowResultJSON{Job: r.GetJob(), RequestedAt: rfc3339(r.GetRequestedAt())}}, nil
}

type pauseRequestJSON struct {
	Reason string `json:"reason"`
}

type pauseResultJSON struct {
	Pause   *pauseJSON `json:"pause"`
	Changed bool       `json:"changed"`
}

func (s *Server) pauseSource(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := sourceID(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req pauseRequestJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Connectors.PauseSource(ctx, &adminv1.PauseSourceRequest{SourceId: id, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: pauseResultJSON{Pause: pauseOf(r.GetPause()), Changed: r.GetChanged()}}, nil
}

type resumeResultJSON struct {
	Changed bool `json:"changed"`
}

func (s *Server) resumeSource(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := sourceID(in)
	if err != nil {
		return adminOutput{}, err
	}
	if err := decodeBody(in.body, &struct{}{}); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Connectors.ResumeSource(ctx, &adminv1.ResumeSourceRequest{SourceId: id})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: resumeResultJSON{Changed: r.GetChanged()}}, nil
}

// 删除保护拦下的待删批次（入库管道详设第 5 节）：GET .../pending-deletions、POST .../{hold_id}/confirm、.../discard。

var holdIDPattern = regexp.MustCompile(`^hold_[a-f0-9]{1,64}$`)

type deletionHoldJSON struct {
	HoldID      string   `json:"hold_id"`
	SourceID    string   `json:"source_id"`
	DocIDs      []string `json:"doc_ids"`
	SyncedDocs  int32    `json:"synced_docs"`
	DetectedAt  string   `json:"detected_at"`
	Status      string   `json:"status"`
	ConfirmedBy string   `json:"confirmed_by,omitempty"`
	ApprovedBy  string   `json:"approved_by,omitempty"`
	DiscardedBy string   `json:"discarded_by,omitempty"`
	Reason      string   `json:"reason,omitempty"`
	AppliedDocs int32    `json:"applied_docs"`
	UpdatedAt   string   `json:"updated_at"`
}

func deletionHoldOf(h *adminv1.DeletionHold) deletionHoldJSON {
	return deletionHoldJSON{HoldID: h.GetHoldId(), SourceID: h.GetSourceId(), DocIDs: nonNil(h.GetDocIds()), SyncedDocs: h.GetSyncedDocs(),
		DetectedAt: rfc3339(h.GetDetectedAt()), Status: h.GetStatus(), ConfirmedBy: h.GetConfirmedBy(), ApprovedBy: h.GetApprovedBy(),
		DiscardedBy: h.GetDiscardedBy(), Reason: h.GetReason(), AppliedDocs: h.GetAppliedDocs(), UpdatedAt: rfc3339(h.GetUpdatedAt())}
}

type deletionHoldListJSON struct {
	Holds []deletionHoldJSON `json:"holds"`
}

func (s *Server) listPendingDeletions(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, err := sourceID(in)
	if err != nil {
		return adminOutput{}, err
	}
	r, err := s.Connectors.ListPendingDeletions(ctx, &adminv1.ListPendingDeletionsRequest{SourceId: id})
	if err != nil {
		return adminOutput{}, err
	}
	out := deletionHoldListJSON{Holds: []deletionHoldJSON{}}
	for _, h := range r.GetHolds() {
		out.Holds = append(out.Holds, deletionHoldOf(h))
	}
	return adminOutput{body: out}, nil
}

type holdActionJSON struct {
	Reason string `json:"reason"`
}

func holdTarget(in *adminInput) (string, string, holdActionJSON, error) {
	var req holdActionJSON
	id, err := sourceID(in)
	if err != nil {
		return "", "", req, err
	}
	hold := in.params["hold_id"]
	if !holdIDPattern.MatchString(hold) {
		return "", "", req, problemNotFound
	}
	err = decodeBody(in.body, &req)
	return id, hold, req, err
}

func (s *Server) confirmDeletion(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, hold, req, err := holdTarget(in)
	if err != nil {
		return adminOutput{}, err
	}
	r, err := s.Connectors.ConfirmDeletion(ctx, &adminv1.ConfirmDeletionRequest{SourceId: id, HoldId: hold, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: deletionHoldOf(r.GetHold())}, nil
}

func (s *Server) discardDeletion(ctx context.Context, in *adminInput) (adminOutput, error) {
	id, hold, req, err := holdTarget(in)
	if err != nil {
		return adminOutput{}, err
	}
	r, err := s.Connectors.DiscardDeletion(ctx, &adminv1.DiscardDeletionRequest{SourceId: id, HoldId: hold, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: deletionHoldOf(r.GetHold())}, nil
}
