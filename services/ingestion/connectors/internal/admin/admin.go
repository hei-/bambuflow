// Package admin 实现 ConnectorAdminService（入库管道详设 14.4）：数据源的同步状态、暂停与恢复。
// 只经 gateway 的管理接口调用，只接受管理 Principal。
package admin

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/holds"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/syncstatus"
)

const (
	roleOperator  = "platform_operator"
	roleDataOwner = "data_owner"
)

type Server struct {
	adminv1.UnimplementedConnectorAdminServiceServer
	Store syncstatus.Store
	Holds holds.Store
	Log   *slog.Logger
	Now   func() time.Time
}

func sourceID(id string) error {
	if !strings.HasPrefix(id, "src_") || len(id) > 128 {
		return status.Error(codes.InvalidArgument, "source_id 不合法")
	}
	return nil
}

func (s *Server) unavailable(ctx context.Context, err error) error {
	s.Log.ErrorContext(ctx, "读写同步状态失败", slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}

func (s *Server) GetSyncStatus(ctx context.Context, r *adminv1.GetSyncStatusRequest) (*adminv1.GetSyncStatusResponse, error) {
	// 只读、不含文档内容；数据属主的部门范围由 governance 管，这里不重复判断
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), roleOperator, roleDataOwner) {
		return nil, status.Error(codes.PermissionDenied, "需要平台运维或数据属主")
	}
	if err := sourceID(r.GetSourceId()); err != nil {
		return nil, err
	}
	st, err := s.Store.Get(ctx, r.GetSourceId())
	if err != nil {
		return nil, s.unavailable(ctx, err)
	}
	if !st.Found {
		return nil, status.Error(codes.NotFound, "这个数据源还没有同步记录")
	}
	out := &adminv1.GetSyncStatusResponse{SourceId: r.GetSourceId(), LastRun: runOf(st.Last), LastSuccess: runOf(st.LastSuccess),
		FreshnessSeconds: -1, Pause: pauseOf(st.Pause)}
	if st.LastSuccess != nil {
		out.FreshnessSeconds = int64(s.Now().Sub(st.LastSuccess.FinishedAt).Seconds())
	}
	return out, nil
}

func (s *Server) PauseSource(ctx context.Context, r *adminv1.PauseSourceRequest) (*adminv1.PauseSourceResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, roleOperator) {
		return nil, status.Error(codes.PermissionDenied, "需要平台运维")
	}
	if err := sourceID(r.GetSourceId()); err != nil {
		return nil, err
	}
	reason := strings.TrimSpace(r.GetReason())
	if reason == "" || utf8.RuneCountInString(reason) > 500 {
		return nil, status.Error(codes.InvalidArgument, "reason 必填，不超过 500 字")
	}
	got, changed, err := s.Store.SetPause(ctx, r.GetSourceId(), syncstatus.Pause{By: p.GetSub(), At: s.Now(), Reason: reason})
	if err != nil {
		return nil, s.unavailable(ctx, err)
	}
	if changed {
		s.Log.WarnContext(ctx, "数据源已暂停同步", slog.String("source_id", r.GetSourceId()), slog.String("by", p.GetSub()))
	}
	return &adminv1.PauseSourceResponse{Pause: pauseOf(&got), Changed: changed}, nil
}

func (s *Server) ResumeSource(ctx context.Context, r *adminv1.ResumeSourceRequest) (*adminv1.ResumeSourceResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, roleOperator) {
		return nil, status.Error(codes.PermissionDenied, "需要平台运维")
	}
	if err := sourceID(r.GetSourceId()); err != nil {
		return nil, err
	}
	changed, err := s.Store.Resume(ctx, r.GetSourceId())
	if err != nil {
		return nil, s.unavailable(ctx, err)
	}
	if changed {
		s.Log.InfoContext(ctx, "数据源已恢复同步", slog.String("source_id", r.GetSourceId()), slog.String("by", p.GetSub()))
	}
	return &adminv1.ResumeSourceResponse{Changed: changed}, nil
}

func runOf(r *syncstatus.Run) *adminv1.SyncRun {
	if r == nil {
		return nil
	}
	return &adminv1.SyncRun{Command: r.Command, StartedAt: timestamppb.New(r.StartedAt), FinishedAt: timestamppb.New(r.FinishedAt),
		Result: r.Result, Error: r.Error, Documents: count(r.Documents), Upserts: count(r.Upserts), AclOnly: count(r.ACLOnly),
		Deletes: count(r.Deletes), DeletesPaused: r.DeletesPaused, DeletesPausedReason: r.DeletesPausedReason,
		Refetched: count(r.Refetched)}
}

func pauseOf(p *syncstatus.Pause) *adminv1.Pause {
	if p == nil {
		return nil
	}
	return &adminv1.Pause{PausedBy: p.By, PausedAt: timestamppb.New(p.At), Reason: p.Reason}
}

func count(n int) int32 {
	if n < 0 || n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}

// 待删批次（入库管道详设第 5 节）：数据属主确认或丢弃，确认需两位不同的数据属主；平台运维可以看。
// 数据属主的部门范围由 governance 管，这里不重复判断。

func (s *Server) ListPendingDeletions(ctx context.Context, r *adminv1.ListPendingDeletionsRequest) (*adminv1.ListPendingDeletionsResponse, error) {
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), roleOperator, roleDataOwner) {
		return nil, status.Error(codes.PermissionDenied, "需要平台运维或数据属主")
	}
	if err := sourceID(r.GetSourceId()); err != nil {
		return nil, err
	}
	hs, err := s.Holds.List(ctx, r.GetSourceId())
	if err != nil {
		return nil, s.unavailable(ctx, err)
	}
	out := &adminv1.ListPendingDeletionsResponse{}
	for _, h := range hs {
		out.Holds = append(out.Holds, holdOf(r.GetSourceId(), h))
	}
	return out, nil
}

func (s *Server) ConfirmDeletion(ctx context.Context, r *adminv1.ConfirmDeletionRequest) (*adminv1.ConfirmDeletionResponse, error) {
	who, reason, err := s.ownerAction(ctx, r.GetSourceId(), r.GetHoldId(), r.GetReason())
	if err != nil {
		return nil, err
	}
	h, err := s.Holds.Confirm(ctx, r.GetSourceId(), r.GetHoldId(), who, reason, s.Now())
	if err != nil {
		return nil, s.holdError(ctx, err)
	}
	s.Log.WarnContext(ctx, "待删批次已确认", slog.String("source_id", r.GetSourceId()), slog.String("hold_id", h.ID),
		slog.String("status", h.Status), slog.String("by", who))
	return &adminv1.ConfirmDeletionResponse{Hold: holdOf(r.GetSourceId(), h)}, nil
}

func (s *Server) DiscardDeletion(ctx context.Context, r *adminv1.DiscardDeletionRequest) (*adminv1.DiscardDeletionResponse, error) {
	who, reason, err := s.ownerAction(ctx, r.GetSourceId(), r.GetHoldId(), r.GetReason())
	if err != nil {
		return nil, err
	}
	h, err := s.Holds.Discard(ctx, r.GetSourceId(), r.GetHoldId(), who, reason, s.Now())
	if err != nil {
		return nil, s.holdError(ctx, err)
	}
	s.Log.InfoContext(ctx, "待删批次已丢弃", slog.String("source_id", r.GetSourceId()), slog.String("hold_id", h.ID), slog.String("by", who))
	return &adminv1.DiscardDeletionResponse{Hold: holdOf(r.GetSourceId(), h)}, nil
}

func (s *Server) ownerAction(ctx context.Context, src, holdID, reason string) (string, string, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, roleDataOwner) {
		return "", "", status.Error(codes.PermissionDenied, "需要数据属主")
	}
	if err := sourceID(src); err != nil {
		return "", "", err
	}
	reason = strings.TrimSpace(reason)
	if !strings.HasPrefix(holdID, "hold_") || len(holdID) > 64 || reason == "" || utf8.RuneCountInString(reason) > 500 {
		return "", "", status.Error(codes.InvalidArgument, "hold_id 不合法，或 reason 为空、超过 500 字")
	}
	return p.GetSub(), reason, nil
}

func (s *Server) holdError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, holds.ErrNotFound):
		return status.Error(codes.NotFound, "批次不存在")
	case errors.Is(err, holds.ErrSelfApproval):
		return grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "不能复核自己确认的批次")
	case errors.Is(err, holds.ErrState):
		return grpcmw.Reason(codes.FailedPrecondition, "conflict", "批次的当前状态不允许这个操作")
	}
	return s.unavailable(ctx, err)
}

func holdOf(src string, h holds.Hold) *adminv1.DeletionHold {
	return &adminv1.DeletionHold{HoldId: h.ID, SourceId: src, DocIds: h.DocIDs, SyncedDocs: count(h.SyncedDocs),
		DetectedAt: timestamppb.New(h.DetectedAt), Status: h.Status, ConfirmedBy: h.ConfirmedBy, ApprovedBy: h.ApprovedBy,
		DiscardedBy: h.DiscardedBy, Reason: h.Reason, AppliedDocs: count(h.AppliedDocs), UpdatedAt: timestamppb.New(h.UpdatedAt)}
}
