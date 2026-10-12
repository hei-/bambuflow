package writer

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// Admin 实现 IndexAdminService。命令行的 publish 与它调用同一个函数；Rollback 已停用（改为 VersionAdmin 双人复核）。
type Admin struct {
	adminv1.UnimplementedIndexAdminServiceServer
	Pool  *pgxpool.Pool
	Audit Auditor
	Log   *slog.Logger
}

// Publish 发布增量的待发布版本；没有时发布全量重建的版本（仍要写完全部在用文档）。
func (a *Admin) Publish(ctx context.Context, _ *adminv1.PublishRequest) (*adminv1.PublishResponse, error) {
	incremental, rebuild, err := PendingVersions(ctx, a.Pool)
	var carried int64
	version := ""
	switch {
	case err != nil:
	case incremental != nil:
		version = incremental.Version
	case rebuild != nil:
		version = rebuild.Version
	default:
		err = ErrNoPending
	}
	if err == nil {
		carried, err = Publish(ctx, a.Pool, version, a.Audit)
	}
	if err != nil {
		a.Log.WarnContext(ctx, "发布失败", slog.Any("err", err))
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	a.Log.InfoContext(ctx, "已发布", slog.String("version", version), slog.Int64("carried_from_live", carried))
	return &adminv1.PublishResponse{Version: version, Carried: carried}, nil
}

// AddDenyRules 把文档、数据源或租户加入紧急拒绝名单，对线上立即生效（retrieval 在过滤与复核两处读取）。
// 三类在同一个事务里写入；已有的规则不动（重复调用幂等）。
func (a *Admin) AddDenyRules(ctx context.Context, r *adminv1.AddDenyRulesRequest) (*adminv1.AddDenyRulesResponse, error) {
	if len(r.GetDocIds())+len(r.GetSourceIds())+len(r.GetTenants()) == 0 || r.GetReason() == "" {
		return nil, status.Error(codes.InvalidArgument, "doc_ids、source_ids、tenants 至少一项，reason 必填")
	}
	var added int64
	err := pgx.BeginFunc(ctx, a.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO idx.deny_list (doc_id, reason) SELECT unnest($1::text[]), $2 ON CONFLICT (doc_id) DO NOTHING`,
			r.GetDocIds(), r.GetReason())
		if err != nil {
			return err
		}
		added += tag.RowsAffected()
		for kind, values := range map[string][]string{"source": r.GetSourceIds(), "tenant": r.GetTenants()} {
			tag, err := tx.Exec(ctx, `
				INSERT INTO idx.deny_scopes (kind, value, reason) SELECT $1, unnest($2::text[]), $3 ON CONFLICT DO NOTHING`,
				kind, values, r.GetReason())
			if err != nil {
				return err
			}
			added += tag.RowsAffected()
		}
		return nil
	})
	if err != nil {
		a.Log.ErrorContext(ctx, "写拒绝名单失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "写入失败")
	}
	a.Log.WarnContext(ctx, "加入拒绝名单", slog.Any("doc_ids", r.GetDocIds()), slog.Any("source_ids", r.GetSourceIds()),
		slog.Any("tenants", r.GetTenants()), slog.String("reason", r.GetReason()))
	return &adminv1.AddDenyRulesResponse{Added: added}, nil
}

func (a *Admin) RemoveDenyRules(ctx context.Context, r *adminv1.RemoveDenyRulesRequest) (*adminv1.RemoveDenyRulesResponse, error) {
	if len(r.GetDocIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "doc_ids 必填")
	}
	tag, err := a.Pool.Exec(ctx, `DELETE FROM idx.deny_list WHERE doc_id = ANY($1::text[])`, r.GetDocIds())
	if err != nil {
		a.Log.ErrorContext(ctx, "删除拒绝规则失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "写入失败")
	}
	a.Log.WarnContext(ctx, "移出拒绝名单", slog.Any("doc_ids", r.GetDocIds()))
	return &adminv1.RemoveDenyRulesResponse{Removed: tag.RowsAffected()}, nil
}
