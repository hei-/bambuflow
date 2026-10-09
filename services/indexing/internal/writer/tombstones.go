package writer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
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
	actionRevoke = "revoke_tombstones"
	// 一次最多撤销的删除标记数：再多说明条件太宽，应缩小时间段分批做，避免一次把大批文档重新拉取压垮源系统
	maxRevoke = 10000
	// 时间段最长 31 天，doc_ids 最多 1000 个
	maxRevokeSpan   = 31 * 24 * time.Hour
	maxRevokeDocIDs = 1000
	// RefetchStream 是 RefetchRequested 的事件流，connectors 消费
	RefetchStream = "ingest:refetch-requested"
)

// RefetchOutboxSelectSQL、RefetchOutboxMarkSQL 供 outbox.Relay 转发 idx.refetch_outbox。
const (
	RefetchOutboxSelectSQL = `SELECT event_id, payload FROM idx.refetch_outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`
	RefetchOutboxMarkSQL   = `UPDATE idx.refetch_outbox SET sent_at = now() WHERE event_id = ANY($1)`
)

// TombstoneAdmin 实现 TombstoneAdminService（入库管道详设第 5 节、14.4）：撤销误写的删除标记，平台运维双人复核。
type TombstoneAdmin struct {
	adminv1.UnimplementedTombstoneAdminServiceServer
	Pool *pgxpool.Pool
	Log  *slog.Logger
	Now  func() time.Time
}

// revokeParams 是撤销的条件，存在 idx.operations.params。
type revokeParams struct {
	Since  time.Time `json:"since"`
	Until  time.Time `json:"until"`
	DocIDs []string  `json:"doc_ids"`
}

// validateRevoke 校验申请；在碰数据库之前完成。
func validateRevoke(r *adminv1.RevokeTombstonesRequest) (revokeParams, string, error) {
	reason := strings.TrimSpace(r.GetReason())
	src := r.GetSourceId()
	if !strings.HasPrefix(src, "src_") || len(src) > 128 {
		return revokeParams{}, "", status.Error(codes.InvalidArgument, "source_id 必填，前缀 src_")
	}
	if reason == "" || utf8.RuneCountInString(reason) > 500 {
		return revokeParams{}, "", status.Error(codes.InvalidArgument, "reason 必填，不超过 500 字")
	}
	if r.GetTombstonedSince() == nil || r.GetTombstonedUntil() == nil {
		return revokeParams{}, "", status.Error(codes.InvalidArgument, "tombstoned_since 与 tombstoned_until 必填")
	}
	p := revokeParams{Since: r.GetTombstonedSince().AsTime(), Until: r.GetTombstonedUntil().AsTime(), DocIDs: r.GetDocIds()}
	if !p.Since.Before(p.Until) || p.Until.Sub(p.Since) > maxRevokeSpan {
		return revokeParams{}, "", status.Error(codes.InvalidArgument, "时间段须 since < until，跨度不超过 31 天")
	}
	if len(p.DocIDs) > maxRevokeDocIDs {
		return revokeParams{}, "", status.Error(codes.InvalidArgument, "doc_ids 最多 1000 个")
	}
	for _, id := range p.DocIDs {
		if id == "" || len(id) > 512 {
			return revokeParams{}, "", status.Error(codes.InvalidArgument, "doc_ids 里有不合法的 ID")
		}
	}
	if p.DocIDs == nil {
		p.DocIDs = []string{}
	}
	return p, reason, nil
}

// tombstonedDocs 按条件选删除标记，最多取 maxRevoke+1 条（用来判断是否超限）。lock 为 true 时锁住这些行。
func tombstonedDocs(ctx context.Context, q pgx.Tx, src string, p revokeParams, lock bool) ([]string, error) {
	sql := `SELECT doc_id FROM idx.doc_acl
		WHERE source_id = $1 AND state = 'tombstoned' AND tombstoned_at >= $2 AND tombstoned_at < $3
			AND (cardinality($4::text[]) = 0 OR doc_id = ANY($4))
		ORDER BY doc_id LIMIT $5`
	if lock {
		sql += ` FOR UPDATE`
	}
	rows, err := q.Query(ctx, sql, src, p.Since, p.Until, p.DocIDs, maxRevoke+1)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (a *TombstoneAdmin) operator(ctx context.Context) (string, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RoleOperator) {
		return "", status.Error(codes.PermissionDenied, "需要平台运维")
	}
	return p.GetSub(), nil
}

func (a *TombstoneAdmin) RevokeTombstones(ctx context.Context, r *adminv1.RevokeTombstonesRequest) (*adminv1.RevokeTombstonesResponse, error) {
	sub, err := a.operator(ctx)
	if err != nil {
		return nil, err
	}
	p, reason, err := validateRevoke(r)
	if err != nil {
		return nil, err
	}
	var rv revocation
	created := true
	err = pgx.BeginFunc(ctx, a.Pool, func(tx pgx.Tx) error {
		docs, err := tombstonedDocs(ctx, tx, r.GetSourceId(), p, false)
		if err != nil {
			return err
		}
		switch {
		case len(docs) == 0:
			return status.Error(codes.NotFound, "没有符合条件的删除标记")
		case len(docs) > maxRevoke:
			return status.Error(codes.InvalidArgument, "符合条件的删除标记超过 10000 份，请缩小时间段分批撤销")
		}
		params, err := json.Marshal(p)
		if err != nil {
			return err
		}
		now := a.Now()
		rv = revocation{ID: newOperationID(), Source: r.GetSourceId(), Params: p, Reason: reason, Matched: count(docs),
			RequestedBy: sub, RequestedAt: now, Status: "pending", ExpiresAt: now.Add(operationTTL)}
		_, err = tx.Exec(ctx, `
			INSERT INTO idx.operations (operation_id, action, source_id, params, matched_docs, reason, requested_by, requested_at, status, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9)`,
			rv.ID, actionRevoke, rv.Source, params, rv.Matched, rv.Reason, rv.RequestedBy, rv.RequestedAt, rv.ExpiresAt)
		return err
	})
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		// 该数据源已有待复核的撤销：返回已有的，不重复申请
		created = false
		rv, err = a.pending(ctx, r.GetSourceId())
	}
	if err != nil {
		return nil, a.fail(ctx, err)
	}
	a.Log.InfoContext(ctx, "申请撤销删除标记，等待复核", slog.String("operation_id", rv.ID), slog.String("source_id", rv.Source),
		slog.Int("matched", int(rv.Matched)))
	return &adminv1.RevokeTombstonesResponse{Revocation: rv.proto(), Created: created}, nil
}

func (a *TombstoneAdmin) ApproveRevocation(ctx context.Context, r *adminv1.ApproveRevocationRequest) (*adminv1.ApproveRevocationResponse, error) {
	sub, err := a.operator(ctx)
	if err != nil {
		return nil, err
	}
	var rv revocation
	err = pgx.BeginFunc(ctx, a.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+revocationCols+` FROM idx.operations WHERE operation_id = $1 AND action = $2 FOR UPDATE`,
			r.GetOperationId(), actionRevoke)
		if err != nil {
			return err
		}
		if rv, err = pgx.CollectExactlyOneRow(rows, scanRevocation); err != nil {
			return err
		}
		switch {
		case rv.RequestedBy == sub:
			return grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "不能复核自己发起的操作")
		case rv.Status != "pending":
			return grpcmw.Reason(codes.FailedPrecondition, "conflict", "操作已经不是待复核状态")
		}
		now := a.Now()
		if !now.Before(rv.ExpiresAt) {
			rv.Status, rv.FailureReason = "expired", "24 小时内没有复核"
		} else {
			rv.ApprovedBy, rv.ApprovedAt = sub, &now
			if err := a.execute(ctx, tx, &rv, now); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `UPDATE idx.operations SET status = $2, approved_by = $3, approved_at = $4, failure_reason = $5,
			revoked_docs = $6, refetch_event_id = $7 WHERE operation_id = $1`,
			rv.ID, rv.Status, rv.ApprovedBy, rv.ApprovedAt, rv.FailureReason, rv.Revoked, rv.RefetchEventID)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "操作不存在")
	}
	if err != nil {
		return nil, a.fail(ctx, err)
	}
	if rv.Status == "expired" {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "操作已过期")
	}
	a.Log.InfoContext(ctx, "删除标记已撤销", slog.String("operation_id", rv.ID), slog.String("source_id", rv.Source),
		slog.String("status", rv.Status), slog.Int("revoked", int(rv.Revoked)), slog.String("refetch_event_id", rv.RefetchEventID),
		slog.String("requested_by", rv.RequestedBy), slog.String("approved_by", sub))
	return &adminv1.ApproveRevocationResponse{Revocation: rv.proto()}, nil
}

// execute 在复核的事务里重新按条件选取并撤销：清掉删除时间（文档仍是 tombstoned，检索不到，直到重新入库时
// 写入路径把它改回 active），同一事务写 RefetchRequested。申请之后可能又有文档被删或已重新入库，所以不沿用申请时的结果。
func (a *TombstoneAdmin) execute(ctx context.Context, tx pgx.Tx, rv *revocation, now time.Time) error {
	docs, err := tombstonedDocs(ctx, tx, rv.Source, rv.Params, true)
	if err != nil {
		return err
	}
	switch {
	case len(docs) == 0:
		rv.Status, rv.FailureReason = "failed", "复核时已没有符合条件的删除标记"
		return nil
	case len(docs) > maxRevoke:
		rv.Status, rv.FailureReason = "failed", "复核时符合条件的删除标记超过 10000 份"
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE idx.doc_acl SET tombstoned_at = NULL, updated_at = now() WHERE doc_id = ANY($1)`, docs); err != nil {
		return err
	}
	evt := &ingestionv1.RefetchRequested{EventId: newEventID(), SourceId: rv.Source, DocIds: docs,
		Reason: ingestionv1.RefetchReason_REFETCH_REASON_TOMBSTONE_REVOKED, OccurredAt: timestamppb.New(now)}
	pb, err := proto.Marshal(evt)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO idx.refetch_outbox (event_id, payload) VALUES ($1, $2)`, evt.GetEventId(), pb); err != nil {
		return err
	}
	rv.Status, rv.Revoked, rv.RefetchEventID = "executed", count(docs), evt.GetEventId()
	return nil
}

func (a *TombstoneAdmin) fail(ctx context.Context, err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	a.Log.ErrorContext(ctx, "撤销删除标记失败", slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}

func (a *TombstoneAdmin) pending(ctx context.Context, source string) (revocation, error) {
	rows, err := a.Pool.Query(ctx, `SELECT `+revocationCols+` FROM idx.operations
		WHERE action = $1 AND source_id = $2 AND status = 'pending'`, actionRevoke, source)
	if err != nil {
		return revocation{}, err
	}
	return pgx.CollectExactlyOneRow(rows, scanRevocation)
}

type revocation struct {
	ID, Source, Reason, RequestedBy, Status, ApprovedBy, FailureReason, RefetchEventID string
	Params                                                                             revokeParams
	Matched, Revoked                                                                   int32
	RequestedAt, ExpiresAt                                                             time.Time
	ApprovedAt                                                                         *time.Time
}

const revocationCols = `operation_id, source_id, params, reason, matched_docs, revoked_docs, requested_by, requested_at, status,
	approved_by, approved_at, failure_reason, expires_at, refetch_event_id`

func scanRevocation(r pgx.CollectableRow) (revocation, error) {
	var o revocation
	var params []byte
	err := r.Scan(&o.ID, &o.Source, &params, &o.Reason, &o.Matched, &o.Revoked, &o.RequestedBy, &o.RequestedAt, &o.Status,
		&o.ApprovedBy, &o.ApprovedAt, &o.FailureReason, &o.ExpiresAt, &o.RefetchEventID)
	if err == nil {
		err = json.Unmarshal(params, &o.Params)
	}
	return o, err
}

func (o revocation) proto() *adminv1.TombstoneRevocation {
	out := &adminv1.TombstoneRevocation{
		OperationId: o.ID, SourceId: o.Source, TombstonedSince: timestamppb.New(o.Params.Since),
		TombstonedUntil: timestamppb.New(o.Params.Until), DocIds: o.Params.DocIDs, Reason: o.Reason,
		MatchedDocs: o.Matched, RevokedDocs: o.Revoked, RequestedBy: o.RequestedBy,
		RequestedAt: timestamppb.New(o.RequestedAt), Status: operationStatus[o.Status], ApprovedBy: o.ApprovedBy,
		FailureReason: o.FailureReason, ExpiresAt: timestamppb.New(o.ExpiresAt), RefetchEventId: o.RefetchEventID,
	}
	if o.ApprovedAt != nil {
		out.ApprovedAt = timestamppb.New(*o.ApprovedAt)
	}
	return out
}

// count 是条数；条件已把上限限制在 maxRevoke+1，这里的判断只为类型转换安全
func count(docs []string) int32 {
	n := len(docs)
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}

func newEventID() string {
	return "evt_" + strings.TrimPrefix(newOperationID(), "op_")
}
