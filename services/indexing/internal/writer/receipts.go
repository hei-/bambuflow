package writer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

// RecordPurge 把对象存储完成删除的时间补记进删除凭证；已记过的保留最早的时间。
func (s *Server) RecordPurge(ctx context.Context, r *ingestionv1.RecordPurgeRequest) (*ingestionv1.RecordPurgeResponse, error) {
	if !strings.HasPrefix(r.GetReceiptId(), "del_") || r.GetStore() != storeObjectStore || r.GetCompletedAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "receipt_id、store 或 completed_at 不合法")
	}
	at, err := json.Marshal(r.GetCompletedAt().AsTime().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "completed_at 不合法")
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE idx.deletion_receipts
		SET stores = jsonb_build_object($2::text, $3::jsonb) || stores WHERE receipt_id = $1`,
		r.GetReceiptId(), r.GetStore(), string(at))
	if err != nil {
		s.Log.ErrorContext(ctx, "补记删除凭证失败", slog.String("receipt_id", r.GetReceiptId()), slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "写入失败")
	}
	if tag.RowsAffected() == 0 {
		return nil, status.Error(codes.NotFound, "删除凭证不存在")
	}
	return &ingestionv1.RecordPurgeResponse{}, nil
}

// ExpectedStores 是一份完整的删除凭证应有的存储：索引侧各副本加对象存储。
func ExpectedStores() []string {
	out := make([]string, 0, len(purgeSteps)+1)
	for _, st := range purgeSteps {
		out = append(out, st.store)
	}
	return append(out, storeObjectStore)
}

// pendingStores 是凭证里还没有完成时间的存储。
func pendingStores(done map[string]time.Time) []string {
	var out []string
	for _, st := range ExpectedStores() {
		if _, ok := done[st]; !ok {
			out = append(out, st)
		}
	}
	return out
}

const (
	RoleAuditor = "auditor"
	RoleInfosec = "infosec"
)

// DeletionReceiptAdmin 实现 DeletionReceiptAdminService：审计岗与信息安全查删除凭证。
type DeletionReceiptAdmin struct {
	adminv1.UnimplementedDeletionReceiptAdminServiceServer
	Pool *pgxpool.Pool
	Log  *slog.Logger
}

func (a *DeletionReceiptAdmin) ListDeletionReceipts(ctx context.Context, r *adminv1.ListDeletionReceiptsRequest) (*adminv1.ListDeletionReceiptsResponse, error) {
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), RoleAuditor, RoleInfosec) {
		return nil, status.Error(codes.PermissionDenied, "需要审计岗或信息安全")
	}
	size := int(r.GetPageSize())
	if size == 0 {
		size = 50
	}
	offset, ok := receiptOffset(r.GetPageToken())
	switch {
	case r.GetDocId() == "" && r.GetSourceId() == "":
		return nil, status.Error(codes.InvalidArgument, "doc_id 与 source_id 至少给一个")
	case size < 1 || size > 200 || !ok || len(r.GetDocId()) > 512 || len(r.GetSourceId()) > 128:
		return nil, status.Error(codes.InvalidArgument, "page_size、page_token 或查询条件不合法")
	}
	rows, err := a.Pool.Query(ctx, `SELECT receipt_id, doc_id, source_id, tombstoned_at, stores, created_at
		FROM idx.deletion_receipts
		WHERE ($1 = '' OR doc_id = $1) AND ($2 = '' OR source_id = $2)
		ORDER BY created_at DESC, receipt_id OFFSET $3 LIMIT $4`, r.GetDocId(), r.GetSourceId(), offset, size+1)
	if err != nil {
		return nil, a.unavailable(ctx, err)
	}
	receipts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*adminv1.DeletionReceipt, error) {
		var id, doc, source string
		var tombstoned *time.Time
		var raw []byte
		var created time.Time
		if err := row.Scan(&id, &doc, &source, &tombstoned, &raw, &created); err != nil {
			return nil, err
		}
		done := map[string]time.Time{}
		if err := json.Unmarshal(raw, &done); err != nil {
			return nil, err
		}
		return receiptProto(id, doc, source, tombstoned, done, created), nil
	})
	if err != nil {
		return nil, a.unavailable(ctx, err)
	}
	out := &adminv1.ListDeletionReceiptsResponse{}
	if len(receipts) > size {
		receipts = receipts[:size]
		out.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset + size)))
	}
	out.Receipts = receipts
	return out, nil
}

func receiptProto(id, doc, source string, tombstoned *time.Time, done map[string]time.Time, created time.Time) *adminv1.DeletionReceipt {
	out := &adminv1.DeletionReceipt{ReceiptId: id, DocId: doc, SourceId: source, CreatedAt: timestamppb.New(created),
		PendingStores: pendingStores(done)}
	if tombstoned != nil {
		out.TombstonedAt = timestamppb.New(*tombstoned)
	}
	names := make([]string, 0, len(done))
	for k := range done {
		names = append(names, k)
	}
	slices.Sort(names)
	for _, k := range names {
		out.Stores = append(out.Stores, &adminv1.StoreDeletion{Store: k, CompletedAt: timestamppb.New(done[k])})
	}
	return out
}

func receiptOffset(token string) (int, bool) {
	if token == "" {
		return 0, true
	}
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, false
	}
	n, err := strconv.Atoi(string(b))
	return n, err == nil && n >= 0
}

func (a *DeletionReceiptAdmin) unavailable(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "已取消")
	}
	a.Log.ErrorContext(ctx, "查询删除凭证失败", slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}
