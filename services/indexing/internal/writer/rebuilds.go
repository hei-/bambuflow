package writer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	ingestionv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
)

const (
	actionRebuild = "start_rebuild"
	// 一条 RefetchRequested 最多带的文档数：全量重建按数据源发，大的数据源分成多条
	refetchChunk = 5000
)

// ErrRebuildIncomplete：重建的待发布版本还没写完全部在用文档，不能发布（Publish 用）。
var ErrRebuildIncomplete = errors.New("全量重建还没写完")

// RebuildAdmin 实现 RebuildAdminService（入库管道详设第 10 节、14.4）。
//
// 现在只有一个待发布版本，所以重建期间旧模型的增量写入会得到 MODEL_MISMATCH、留在 processing 里重试，
// 等重建发布后再写（权限变更与删除在文档级，不受影响）；设计里“同时写入线上版本和新版本”要等多个待发布版本。
type RebuildAdmin struct {
	adminv1.UnimplementedRebuildAdminServiceServer
	Pool *pgxpool.Pool
	Log  *slog.Logger
	Now  func() time.Time
}

type rebuildParams struct {
	Model   string `json:"embedding_model"`
	Chunker string `json:"chunker_version"`
}

func (a *RebuildAdmin) operator(ctx context.Context) (string, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RoleOperator) {
		return "", status.Error(codes.PermissionDenied, "需要平台运维")
	}
	return p.GetSub(), nil
}

func reasonOf(r string) (string, error) {
	r = strings.TrimSpace(r)
	if r == "" || utf8.RuneCountInString(r) > 500 {
		return "", status.Error(codes.InvalidArgument, "reason 必填，不超过 500 字")
	}
	return r, nil
}

func (a *RebuildAdmin) StartRebuild(ctx context.Context, r *adminv1.StartRebuildRequest) (*adminv1.StartRebuildResponse, error) {
	sub, err := a.operator(ctx)
	if err != nil {
		return nil, err
	}
	reason, err := reasonOf(r.GetReason())
	if err != nil {
		return nil, err
	}
	if len(r.GetEmbeddingModel()) > 128 || len(r.GetChunkerVersion()) > 128 {
		return nil, status.Error(codes.InvalidArgument, "embedding_model、chunker_version 不超过 128 个字符")
	}
	var op rebuildOp
	created := true
	err = pgx.BeginFunc(ctx, a.Pool, func(tx pgx.Tx) error {
		live, model, chunker, err := liveVersion(ctx, tx)
		if err != nil {
			return err
		}
		p := rebuildParams{Model: r.GetEmbeddingModel(), Chunker: r.GetChunkerVersion()}
		if p.Model == "" {
			p.Model = model
		}
		if p.Chunker == "" {
			p.Chunker = chunker
		}
		if p.Model == model && p.Chunker == chunker {
			return status.Error(codes.InvalidArgument, "嵌入模型与分块器都与线上版本相同，不需要重建")
		}
		if err := refuseIfBusy(ctx, tx); err != nil {
			return err
		}
		params, err := json.Marshal(p)
		if err != nil {
			return err
		}
		now := a.Now()
		op = rebuildOp{ID: newOperationID(), Params: p, Live: live, Reason: reason, RequestedBy: sub, RequestedAt: now,
			Status: "pending", ExpiresAt: now.Add(operationTTL)}
		_, err = tx.Exec(ctx, `
			INSERT INTO idx.operations (operation_id, action, live_version, params, reason, requested_by, requested_at, status, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8)`,
			op.ID, actionRebuild, op.Live, params, op.Reason, op.RequestedBy, op.RequestedAt, op.ExpiresAt)
		return err
	})
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		created = false
		op, err = a.pendingOp(ctx)
	}
	if err != nil {
		return nil, a.fail(ctx, err)
	}
	a.Log.InfoContext(ctx, "申请全量重建，等待复核", slog.String("operation_id", op.ID), slog.String("model", op.Params.Model),
		slog.String("chunker", op.Params.Chunker))
	return &adminv1.StartRebuildResponse{Operation: op.proto(), Created: created}, nil
}

// refuseIfBusy：已有进行中的重建，或已有待发布版本（增量写入还没发布）时不能开始，先发布或等它结束。
func refuseIfBusy(ctx context.Context, tx pgx.Tx) error {
	var running, pending bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM idx.rebuilds WHERE status = 'running'),
		EXISTS (SELECT 1 FROM idx.index_versions WHERE status = 'pending')`).Scan(&running, &pending); err != nil {
		return err
	}
	switch {
	case running:
		return grpcmw.Reason(codes.FailedPrecondition, "conflict", "已有进行中的全量重建")
	case pending:
		return grpcmw.Reason(codes.FailedPrecondition, "conflict", "有待发布版本，先发布再重建")
	}
	return nil
}

func liveVersion(ctx context.Context, tx pgx.Tx) (string, string, string, error) {
	var live, model, chunker string
	err := tx.QueryRow(ctx, `SELECT a.version, v.embedding_model, v.chunker_version FROM idx.aliases a
		JOIN idx.index_versions v USING (version) WHERE a.alias = 'prod'`).Scan(&live, &model, &chunker)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", "", grpcmw.Reason(codes.FailedPrecondition, "conflict", "还没有线上版本")
	}
	return live, model, chunker, err
}

func (a *RebuildAdmin) ApproveRebuild(ctx context.Context, r *adminv1.ApproveRebuildRequest) (*adminv1.ApproveRebuildResponse, error) {
	sub, err := a.operator(ctx)
	if err != nil {
		return nil, err
	}
	var op rebuildOp
	err = pgx.BeginFunc(ctx, a.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+rebuildOpCols+` FROM idx.operations WHERE operation_id = $1 AND action = $2 FOR UPDATE`,
			r.GetOperationId(), actionRebuild)
		if err != nil {
			return err
		}
		if op, err = pgx.CollectExactlyOneRow(rows, scanRebuildOp); err != nil {
			return err
		}
		switch {
		case op.RequestedBy == sub:
			return grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "不能复核自己发起的操作")
		case op.Status != "pending":
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "操作已经不是待复核状态")
		}
		now := a.Now()
		if !now.Before(op.ExpiresAt) {
			op.Status, op.FailureReason = "expired", "24 小时内没有复核"
		} else {
			op.ApprovedBy, op.ApprovedAt = sub, &now
			if err := a.begin(ctx, tx, &op, now); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `UPDATE idx.operations SET status = $2, approved_by = $3, approved_at = $4, failure_reason = $5,
			rebuild_id = $6 WHERE operation_id = $1`, op.ID, op.Status, op.ApprovedBy, op.ApprovedAt, op.FailureReason, op.RebuildID)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "操作不存在")
	}
	if err != nil {
		return nil, a.fail(ctx, err)
	}
	if op.Status == "expired" {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "操作已过期")
	}
	a.Log.InfoContext(ctx, "全量重建复核", slog.String("operation_id", op.ID), slog.String("status", op.Status),
		slog.String("rebuild_id", op.RebuildID), slog.String("requested_by", op.RequestedBy), slog.String("approved_by", sub))
	return &adminv1.ApproveRebuildResponse{Operation: op.proto()}, nil
}

// begin 在复核的事务里开始重建：再核对一次能否开始，新建待发布版本与重建记录，并按数据源发出 RefetchRequested。
func (a *RebuildAdmin) begin(ctx context.Context, tx pgx.Tx, op *rebuildOp, now time.Time) error {
	if err := refuseIfBusy(ctx, tx); err != nil {
		var st interface{ GRPCStatus() *status.Status }
		if errors.As(err, &st) {
			op.Status, op.FailureReason = "failed", st.GRPCStatus().Message()
			return nil
		}
		return err
	}
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	version := "ix-" + now.UTC().Format("20060102-150405") + "-" + hex.EncodeToString(b)
	if _, err := tx.Exec(ctx, `INSERT INTO idx.index_versions (version, status, embedding_model, chunker_version) VALUES ($1, 'pending', $2, $3)`,
		version, op.Params.Model, op.Params.Chunker); err != nil {
		return err
	}
	op.RebuildID = "rb_" + strings.TrimPrefix(newOperationID(), "op_")
	if _, err := tx.Exec(ctx, `
		INSERT INTO idx.rebuilds (rebuild_id, version, embedding_model, chunker_version, status, reason, requested_by, approved_by, started_at)
		VALUES ($1, $2, $3, $4, 'running', $5, $6, $7, $8)`,
		op.RebuildID, version, op.Params.Model, op.Params.Chunker, op.Reason, op.RequestedBy, op.ApprovedBy, now); err != nil {
		return err
	}
	if err := refetchAll(ctx, tx, now); err != nil {
		return err
	}
	op.Status = "executed"
	return nil
}

// refetchAll 对每个数据源的全部在用文档发出 RefetchRequested（分批），connectors 下一轮同步全部重新发出。
func refetchAll(ctx context.Context, tx pgx.Tx, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT source_id, array_agg(doc_id ORDER BY doc_id) FROM idx.doc_acl
		WHERE state = 'active' GROUP BY source_id ORDER BY source_id`)
	if err != nil {
		return err
	}
	type group struct {
		source string
		docs   []string
	}
	groups, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (group, error) {
		var g group
		err := r.Scan(&g.source, &g.docs)
		return g, err
	})
	if err != nil {
		return err
	}
	for _, g := range groups {
		for i := 0; i < len(g.docs); i += refetchChunk {
			evt := &ingestionv1.RefetchRequested{EventId: newEventID(), SourceId: g.source, DocIds: g.docs[i:min(i+refetchChunk, len(g.docs))],
				Reason: ingestionv1.RefetchReason_REFETCH_REASON_FULL_REBUILD, OccurredAt: timestamppb.New(now)}
			pb, err := proto.Marshal(evt)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO idx.refetch_outbox (event_id, payload) VALUES ($1, $2)`, evt.GetEventId(), pb); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *RebuildAdmin) GetRebuild(ctx context.Context, r *adminv1.GetRebuildRequest) (*adminv1.GetRebuildResponse, error) {
	if _, err := a.operator(ctx); err != nil {
		return nil, err
	}
	rb, err := a.load(ctx, a.Pool, r.GetRebuildId(), false)
	if err != nil {
		return nil, a.fail(ctx, err)
	}
	return &adminv1.GetRebuildResponse{Rebuild: rb.proto()}, nil
}

func (a *RebuildAdmin) CancelRebuild(ctx context.Context, r *adminv1.CancelRebuildRequest) (*adminv1.CancelRebuildResponse, error) {
	sub, err := a.operator(ctx)
	if err != nil {
		return nil, err
	}
	reason, err := reasonOf(r.GetReason())
	if err != nil {
		return nil, err
	}
	var rb rebuild
	err = pgx.BeginFunc(ctx, a.Pool, func(tx pgx.Tx) error {
		if rb, err = a.load(ctx, tx, r.GetRebuildId(), true); err != nil {
			return err
		}
		if rb.Status != "running" {
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "重建已经结束")
		}
		now := a.Now()
		rb.Status, rb.FinishedAt, rb.CancelledBy = "cancelled", &now, sub
		// 丢弃新版本即可：它的块按 doc_version 存放，线上版本引用的旧块不受影响；之后的增量写入发现块的模型不同会自动重写
		if _, err := tx.Exec(ctx, `UPDATE idx.index_versions SET status = 'discarded' WHERE version = $1 AND status = 'pending'`,
			rb.Version); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE idx.rebuilds SET status = 'cancelled', finished_at = $2, cancelled_by = $3 WHERE rebuild_id = $1`,
			rb.ID, now, sub)
		return err
	})
	if err != nil {
		return nil, a.fail(ctx, err)
	}
	a.Log.WarnContext(ctx, "全量重建已取消", slog.String("rebuild_id", rb.ID), slog.String("by", sub), slog.String("reason", reason))
	return &adminv1.CancelRebuildResponse{Rebuild: rb.proto()}, nil
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// load 读重建记录与进度：进度是新版本已写入的在用文档数 / 在用文档总数。
func (a *RebuildAdmin) load(ctx context.Context, q querier, id string, lock bool) (rebuild, error) {
	if !strings.HasPrefix(id, "rb_") || len(id) > 64 {
		return rebuild{}, status.Error(codes.NotFound, "重建不存在")
	}
	sql := `SELECT rebuild_id, version, embedding_model, chunker_version, status, reason, requested_by, approved_by, started_at,
		finished_at, cancelled_by FROM idx.rebuilds WHERE rebuild_id = $1`
	if lock {
		sql += ` FOR UPDATE`
	}
	rows, err := q.Query(ctx, sql, id)
	if err != nil {
		return rebuild{}, err
	}
	rb, err := pgx.CollectExactlyOneRow(rows, func(r pgx.CollectableRow) (rebuild, error) {
		var b rebuild
		err := r.Scan(&b.ID, &b.Version, &b.Model, &b.Chunker, &b.Status, &b.Reason, &b.RequestedBy, &b.ApprovedBy, &b.StartedAt,
			&b.FinishedAt, &b.CancelledBy)
		return b, err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return rebuild{}, status.Error(codes.NotFound, "重建不存在")
	}
	if err != nil {
		return rebuild{}, err
	}
	err = q.QueryRow(ctx, rebuildProgressSQL, rb.Version).Scan(&rb.Written, &rb.Total)
	return rb, err
}

const rebuildProgressSQL = `
	SELECT (SELECT count(*) FROM idx.version_docs vd JOIN idx.doc_acl a USING (doc_id) WHERE vd.version = $1 AND a.state = 'active'),
		(SELECT count(*) FROM idx.doc_acl WHERE state = 'active')`

// checkRebuildComplete 在 Publish 的事务里调用：待发布版本是进行中的重建时，必须写完全部在用文档才能发布，
// 发布后把重建标为 completed。不是重建的版本不受影响。
func checkRebuildComplete(ctx context.Context, tx pgx.Tx, version string) error {
	var id string
	err := tx.QueryRow(ctx, `SELECT rebuild_id FROM idx.rebuilds WHERE version = $1 AND status = 'running' FOR UPDATE`, version).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var written, total int64
	if err := tx.QueryRow(ctx, rebuildProgressSQL, version).Scan(&written, &total); err != nil {
		return err
	}
	if written < total {
		return fmt.Errorf("%w：%d 份在用文档中已写入 %d 份", ErrRebuildIncomplete, total, written)
	}
	_, err = tx.Exec(ctx, `UPDATE idx.rebuilds SET status = 'completed', finished_at = now() WHERE rebuild_id = $1`, id)
	return err
}

func (a *RebuildAdmin) fail(ctx context.Context, err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	a.Log.ErrorContext(ctx, "全量重建操作失败", slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}

func (a *RebuildAdmin) pendingOp(ctx context.Context) (rebuildOp, error) {
	rows, err := a.Pool.Query(ctx, `SELECT `+rebuildOpCols+` FROM idx.operations WHERE action = $1 AND status = 'pending'`, actionRebuild)
	if err != nil {
		return rebuildOp{}, err
	}
	return pgx.CollectExactlyOneRow(rows, scanRebuildOp)
}

type rebuildOp struct {
	ID, Live, Reason, RequestedBy, Status, ApprovedBy, FailureReason, RebuildID string
	Params                                                                      rebuildParams
	RequestedAt, ExpiresAt                                                      time.Time
	ApprovedAt                                                                  *time.Time
}

const rebuildOpCols = `operation_id, live_version, params, reason, requested_by, requested_at, status, approved_by, approved_at,
	failure_reason, expires_at, rebuild_id`

func scanRebuildOp(r pgx.CollectableRow) (rebuildOp, error) {
	var o rebuildOp
	var params []byte
	err := r.Scan(&o.ID, &o.Live, &params, &o.Reason, &o.RequestedBy, &o.RequestedAt, &o.Status, &o.ApprovedBy, &o.ApprovedAt,
		&o.FailureReason, &o.ExpiresAt, &o.RebuildID)
	if err == nil {
		err = json.Unmarshal(params, &o.Params)
	}
	return o, err
}

func (o rebuildOp) proto() *adminv1.RebuildOperation {
	out := &adminv1.RebuildOperation{OperationId: o.ID, EmbeddingModel: o.Params.Model, ChunkerVersion: o.Params.Chunker,
		LiveVersion: o.Live, Reason: o.Reason, RequestedBy: o.RequestedBy, RequestedAt: timestamppb.New(o.RequestedAt),
		Status: operationStatus[o.Status], ApprovedBy: o.ApprovedBy, FailureReason: o.FailureReason,
		ExpiresAt: timestamppb.New(o.ExpiresAt), RebuildId: o.RebuildID}
	if o.ApprovedAt != nil {
		out.ApprovedAt = timestamppb.New(*o.ApprovedAt)
	}
	return out
}

type rebuild struct {
	ID, Version, Model, Chunker, Status, Reason, RequestedBy, ApprovedBy, CancelledBy string
	StartedAt                                                                         time.Time
	FinishedAt                                                                        *time.Time
	Written, Total                                                                    int64
}

func (b rebuild) proto() *adminv1.Rebuild {
	out := &adminv1.Rebuild{RebuildId: b.ID, Version: b.Version, EmbeddingModel: b.Model, ChunkerVersion: b.Chunker, Status: b.Status,
		WrittenDocs: b.Written, TotalDocs: b.Total, RequestedBy: b.RequestedBy, ApprovedBy: b.ApprovedBy,
		StartedAt: timestamppb.New(b.StartedAt), CancelledBy: b.CancelledBy, Reason: b.Reason}
	if b.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(*b.FinishedAt)
	}
	return out
}
