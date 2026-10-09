package writer

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/outbox"
)

// RoleOperator 是平台运维（gateway 的 BF_ADMIN_ROLES_FILE 映射）：索引版本的切换与复核只给它。
const RoleOperator = "platform_operator"

// operationTTL：待复核的操作 24 小时内没人复核就作废，过期的申请不能被拿来执行。
const operationTTL = 24 * time.Hour

const actionActivate = "activate_index_version"

// ErrActivationRefused：目标版本不能上线（不是已下线的版本、清单不全或嵌入模型与当前不同）。
var ErrActivationRefused = errors.New("拒绝切换")

// VersionAdmin 实现 IndexVersionAdminService（入库管道详设 14.4）。复核状态存在 idx.operations；
// 谁申请、谁复核写在操作记录里，管理审计由 gateway 统一写。
type VersionAdmin struct {
	adminv1.UnimplementedIndexVersionAdminServiceServer
	Pool *pgxpool.Pool
	Log  *slog.Logger
	Now  func() time.Time
}

// checkActivatable 核对目标版本能否上线，返回当前线上版本。切换不重算向量，所以目标版本的嵌入模型必须与当前线上版本相同
// （“模型已下线”：检索只按线上版本的模型生成查询向量）；清单列出的每份文档的块记录都必须还在（清单不全不能上线）。
// 权限与删除标记在文档级、不分版本，切换后仍是最新值，不需要重放。
func checkActivatable(ctx context.Context, tx pgx.Tx, target string) (string, error) {
	var live, liveModel string
	if err := tx.QueryRow(ctx, `
		SELECT a.version, v.embedding_model FROM idx.aliases a JOIN idx.index_versions v USING (version)
		WHERE a.alias = 'prod' FOR UPDATE OF a`).Scan(&live, &liveModel); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("%w: 还没有线上版本", ErrActivationRefused)
		}
		return "", fmt.Errorf("读取线上版本: %w", err)
	}
	var state, model string
	if err := tx.QueryRow(ctx, `SELECT status, embedding_model FROM idx.index_versions WHERE version = $1`,
		target).Scan(&state, &model); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("%w: 版本 %s 不存在", ErrActivationRefused, target)
		}
		return "", err
	}
	if state != "retired" || target == live {
		return "", fmt.Errorf("%w: 版本 %s 的状态是 %s，只能切到已下线的版本", ErrActivationRefused, target, state)
	}
	if model != liveModel {
		return "", fmt.Errorf("%w: 版本 %s 的嵌入模型 %s 已下线（当前为 %s）", ErrActivationRefused, target, model, liveModel)
	}
	var missing int
	if err := tx.QueryRow(ctx, missingDocsSQL, target).Scan(&missing); err != nil {
		return "", err
	}
	if missing > 0 {
		return "", fmt.Errorf("%w: 版本 %s 的清单不全，%d 份文档的块记录已不存在", ErrActivationRefused, target, missing)
	}
	return live, nil
}

// 清单里块记录已不存在的文档数
const missingDocsSQL = `
	SELECT count(*) FROM idx.version_docs vd
	WHERE vd.version = $1 AND NOT EXISTS (
		SELECT 1 FROM idx.chunks c WHERE c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version)`

// activate 在调用方的事务里把 prod 别名切到 target，原线上版本标为 retired，并写 index.version.live 事件（与发布一致）。
func activate(ctx context.Context, tx pgx.Tx, target string) (string, error) {
	from, err := checkActivatable(ctx, tx, target)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE idx.index_versions SET status = 'retired' WHERE version = $1`, from); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE idx.index_versions SET status = 'live', published_at = now() WHERE version = $1`, target); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE idx.aliases SET version = $1, switched_at = now() WHERE alias = 'prod'`, target); err != nil {
		return "", err
	}
	evt, pb, err := outbox.New(outbox.IndexVersionLive, "", target, time.Now())
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO idx.outbox (event_id, payload) VALUES ($1, $2)`, evt.GetEventId(), pb)
	return from, err
}

func (a *VersionAdmin) operator(ctx context.Context) (string, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if !grpcmw.HasRole(p, RoleOperator) {
		return "", status.Error(codes.PermissionDenied, "需要平台运维")
	}
	return p.GetSub(), nil
}

func (a *VersionAdmin) ListIndexVersions(ctx context.Context, r *adminv1.ListIndexVersionsRequest) (*adminv1.ListIndexVersionsResponse, error) {
	if _, err := a.operator(ctx); err != nil {
		return nil, err
	}
	size, offset, err := page(r.GetPageSize(), r.GetPageToken())
	if err != nil {
		return nil, err
	}
	rows, err := a.Pool.Query(ctx, `
		SELECT v.version, v.status, v.embedding_model, v.chunker_version, v.created_at, v.published_at,
			(SELECT count(*) FROM idx.version_docs vd WHERE vd.version = v.version),
			(SELECT count(*) FROM idx.version_docs vd WHERE vd.version = v.version AND NOT EXISTS (
				SELECT 1 FROM idx.chunks c WHERE c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version))
		FROM idx.index_versions v ORDER BY v.created_at DESC, v.version DESC OFFSET $1 LIMIT $2`,
		offset, size+1)
	if err != nil {
		a.Log.ErrorContext(ctx, "读取索引版本失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	versions, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (*adminv1.IndexVersion, error) {
		v := &adminv1.IndexVersion{}
		var st string
		var created time.Time
		var published *time.Time
		err := r.Scan(&v.Version, &st, &v.EmbeddingModel, &v.ChunkerVersion, &created, &published, &v.DocCount, &v.MissingDocCount)
		v.Status, v.CreatedAt = versionStatus[st], timestamppb.New(created)
		if published != nil {
			v.PublishedAt = timestamppb.New(*published)
		}
		return v, err
	})
	if err != nil {
		a.Log.ErrorContext(ctx, "读取索引版本失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	out := &adminv1.ListIndexVersionsResponse{}
	if len(versions) > size {
		versions, out.NextPageToken = versions[:size], pageToken(offset+size)
	}
	out.Versions = versions
	return out, nil
}

var versionStatus = map[string]adminv1.IndexVersionStatus{
	"pending": adminv1.IndexVersionStatus_INDEX_VERSION_STATUS_PENDING,
	"live":    adminv1.IndexVersionStatus_INDEX_VERSION_STATUS_LIVE,
	"retired": adminv1.IndexVersionStatus_INDEX_VERSION_STATUS_RETIRED,
}

func (a *VersionAdmin) ActivateIndexVersion(ctx context.Context, r *adminv1.ActivateIndexVersionRequest) (*adminv1.ActivateIndexVersionResponse, error) {
	sub, err := a.operator(ctx)
	if err != nil {
		return nil, err
	}
	reason := strings.TrimSpace(r.GetReason())
	if r.GetVersion() == "" || len(r.GetVersion()) > 128 || reason == "" || utf8.RuneCountInString(reason) > 500 {
		return nil, status.Error(codes.InvalidArgument, "version 与 reason 必填，reason 不超过 500 字")
	}
	var op operation
	created := true
	err = pgx.BeginFunc(ctx, a.Pool, func(tx pgx.Tx) error {
		live, err := checkActivatable(ctx, tx, r.GetVersion())
		if err != nil {
			return err
		}
		now := a.Now()
		op = operation{ID: newOperationID(), Action: actionActivate, Target: r.GetVersion(), Live: live, Reason: reason,
			RequestedBy: sub, RequestedAt: now, Status: "pending", ExpiresAt: now.Add(operationTTL)}
		_, err = tx.Exec(ctx, `
			INSERT INTO idx.operations (operation_id, action, target_version, live_version, reason, requested_by, requested_at, status, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8)`,
			op.ID, op.Action, op.Target, op.Live, op.Reason, op.RequestedBy, op.RequestedAt, op.ExpiresAt)
		return err
	})
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		// 同一版本已有待复核的操作：返回已有的，不重复申请
		created = false
		op, err = a.pendingFor(ctx, r.GetVersion())
	}
	if err != nil {
		return nil, a.refusal(ctx, err)
	}
	a.Log.InfoContext(ctx, "申请切换索引版本，等待复核", slog.String("operation_id", op.ID), slog.String("target", op.Target))
	return &adminv1.ActivateIndexVersionResponse{Operation: op.proto(), Created: created}, nil
}

func (a *VersionAdmin) ApproveOperation(ctx context.Context, r *adminv1.ApproveOperationRequest) (*adminv1.ApproveOperationResponse, error) {
	sub, err := a.operator(ctx)
	if err != nil {
		return nil, err
	}
	var op operation
	var refused error
	err = pgx.BeginFunc(ctx, a.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+operationCols+` FROM idx.operations WHERE operation_id = $1 AND action = $2 FOR UPDATE`,
			r.GetOperationId(), actionActivate)
		if err != nil {
			return err
		}
		if op, err = pgx.CollectExactlyOneRow(rows, scanOperation); err != nil {
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
		} else if _, err := activate(ctx, tx, op.Target); errors.Is(err, ErrActivationRefused) {
			// 复核时再核对一次：申请之后清单可能变得不全，或线上版本已经换了模型
			op.Status, op.FailureReason, refused = "failed", err.Error(), err
		} else if err != nil {
			return err
		} else {
			op.Status = "executed"
		}
		op.ApprovedBy, op.ApprovedAt = sub, &now
		if op.Status == "expired" {
			op.ApprovedBy, op.ApprovedAt = "", nil
		}
		_, err = tx.Exec(ctx, `UPDATE idx.operations SET status = $2, approved_by = $3, approved_at = $4, failure_reason = $5
			WHERE operation_id = $1`, op.ID, op.Status, op.ApprovedBy, op.ApprovedAt, op.FailureReason)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "操作不存在")
	}
	if err != nil {
		return nil, a.refusal(ctx, err)
	}
	switch {
	case refused != nil:
		a.Log.WarnContext(ctx, "复核时目标版本不能上线，未执行", slog.String("operation_id", op.ID), slog.Any("err", refused))
		return nil, grpcmw.Reason(codes.FailedPrecondition, "activation_refused", refused.Error())
	case op.Status == "expired":
		return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", "操作已过期")
	}
	a.Log.InfoContext(ctx, "索引版本已切换", slog.String("operation_id", op.ID), slog.String("from", op.Live),
		slog.String("to", op.Target), slog.String("requested_by", op.RequestedBy), slog.String("approved_by", sub))
	return &adminv1.ApproveOperationResponse{Operation: op.proto()}, nil
}

// refusal 把业务拒绝转成带错误码的 gRPC 错误，其余按不可用处理（细节只记日志）。
func (a *VersionAdmin) refusal(ctx context.Context, err error) error {
	if errors.Is(err, ErrActivationRefused) {
		return grpcmw.Reason(codes.FailedPrecondition, "activation_refused", err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	a.Log.ErrorContext(ctx, "索引版本操作失败", slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}

func (a *VersionAdmin) pendingFor(ctx context.Context, target string) (operation, error) {
	rows, err := a.Pool.Query(ctx, `SELECT `+operationCols+` FROM idx.operations
		WHERE action = $1 AND target_version = $2 AND status = 'pending'`, actionActivate, target)
	if err != nil {
		return operation{}, err
	}
	return pgx.CollectExactlyOneRow(rows, scanOperation)
}

type operation struct {
	ID, Action, Target, Live, Reason, RequestedBy, Status, ApprovedBy, FailureReason string
	RequestedAt, ExpiresAt                                                           time.Time
	ApprovedAt                                                                       *time.Time
}

const operationCols = `operation_id, action, target_version, live_version, reason, requested_by, requested_at, status,
	approved_by, approved_at, failure_reason, expires_at`

func scanOperation(r pgx.CollectableRow) (operation, error) {
	var o operation
	err := r.Scan(&o.ID, &o.Action, &o.Target, &o.Live, &o.Reason, &o.RequestedBy, &o.RequestedAt, &o.Status,
		&o.ApprovedBy, &o.ApprovedAt, &o.FailureReason, &o.ExpiresAt)
	return o, err
}

var operationStatus = map[string]adminv1.OperationStatus{
	"pending":  adminv1.OperationStatus_OPERATION_STATUS_PENDING,
	"executed": adminv1.OperationStatus_OPERATION_STATUS_EXECUTED,
	"failed":   adminv1.OperationStatus_OPERATION_STATUS_FAILED,
	"expired":  adminv1.OperationStatus_OPERATION_STATUS_EXPIRED,
}

func (o operation) proto() *adminv1.Operation {
	out := &adminv1.Operation{
		OperationId: o.ID, Action: o.Action, TargetVersion: o.Target, LiveVersion: o.Live, Reason: o.Reason,
		RequestedBy: o.RequestedBy, RequestedAt: timestamppb.New(o.RequestedAt), Status: operationStatus[o.Status],
		ApprovedBy: o.ApprovedBy, FailureReason: o.FailureReason, ExpiresAt: timestamppb.New(o.ExpiresAt),
	}
	if o.ApprovedAt != nil {
		out.ApprovedAt = timestamppb.New(*o.ApprovedAt)
	}
	return out
}

func newOperationID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "op_" + hex.EncodeToString(b)
}

// page 解析分页参数：page_size 0 取 50、上限 200；page_token 是不透明的下一页起点。
func page(size int32, token string) (int, int, error) {
	n := int(size)
	if n == 0 {
		n = 50
	}
	if n < 1 || n > 200 {
		return 0, 0, status.Error(codes.InvalidArgument, "page_size 应为 1–200")
	}
	if token == "" {
		return n, 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	s, ok := strings.CutPrefix(string(raw), "o:")
	offset, aerr := strconv.Atoi(s)
	if err != nil || !ok || aerr != nil || offset < 0 {
		return 0, 0, status.Error(codes.InvalidArgument, "page_token 不合法")
	}
	return n, offset, nil
}

func pageToken(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte("o:" + strconv.Itoa(offset)))
}
