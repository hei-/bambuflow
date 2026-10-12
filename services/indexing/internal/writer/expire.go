package writer

import (
	"context"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

// expireBatch 是每个事务处理的文档数：分批提交，锁住的行少，在线写入不受影响
const expireBatch = 500

var sourcePattern = regexp.MustCompile(`^src_[A-Za-z0-9_]{1,64}$`)

// RetentionFile 是按数据源的保留期（部署配置，技术设计文档 9.2）：
//
//	retention_days:
//	  src_mail: 2555
type RetentionFile struct {
	RetentionDays map[string]int32 `yaml:"retention_days"`
}

// LoadRetention 读部署配置的保留期文件（BF_RETENTION_FILE）。
func LoadRetention(path string) (map[string]int32, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	return ParseRetention(b)
}

// ParseRetention 解析并校验保留期配置：数据源 ID 合法、天数大于 0。
func ParseRetention(b []byte) (map[string]int32, error) {
	var f RetentionFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	if err := validRetention(f.RetentionDays); err != nil {
		return nil, err
	}
	return f.RetentionDays, nil
}

func validRetention(m map[string]int32) error {
	for src, days := range m {
		if !sourcePattern.MatchString(src) || days <= 0 {
			return status.Errorf(codes.InvalidArgument, "数据源 %q 的保留天数不合法：%d", src, days)
		}
	}
	return nil
}

// Expire 给每个数据源里源端时间早于“now - 保留天数”的在用文档写删除标记：与源端删除走同一个函数，
// 对线上立即生效；删除时间记为 now，之后比它早产生的内容写入都被挡住（删除优先）。
// 每批写一条变更审计（document.expire，列出写了删除标记的文档），与删除标记同一事务。
func Expire(ctx context.Context, pool *pgxpool.Pool, retention map[string]int32, now time.Time, audit Auditor) (map[string]int64, error) {
	out := map[string]int64{}
	for _, src := range slices.Sorted(maps.Keys(retention)) {
		before := now.Add(-time.Duration(retention[src]) * 24 * time.Hour)
		for done := false; !done; {
			var n int64
			err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				rows, err := tx.Query(ctx, `SELECT a.doc_id FROM idx.doc_acl a JOIN idx.documents d USING (doc_id)
					WHERE a.source_id = $1 AND a.state = 'active' AND d.occurred_at < $2 ORDER BY a.doc_id LIMIT $3`,
					src, before, expireBatch)
				if err != nil {
					return err
				}
				docs, err := pgx.CollectRows(rows, pgx.RowTo[string])
				if err != nil {
					return err
				}
				var expired []string
				for _, doc := range docs {
					r, err := tombstone(ctx, tx, &ingestionv1.TombstoneDocumentRequest{SourceId: src, DocId: doc, OccurredAt: timestamppb.New(now)})
					if err != nil {
						return err
					}
					if r == ingestionv1.TombstoneResult_TOMBSTONE_RESULT_TOMBSTONED {
						n++
						expired = append(expired, doc)
					}
				}
				if len(expired) > 0 {
					if err := audit.Write(ctx, "document.expire", src, map[string]any{"doc_ids": expired,
						"retention_days": retention[src], "occurred_before": before}); err != nil {
						return err
					}
				}
				// 取不满一批说明已经处理完；已写删除标记的不再被选中，下一批从剩下的取
				done = len(docs) < expireBatch
				return nil
			})
			if err != nil {
				return out, err
			}
			out[src] += n
		}
	}
	return out, nil
}

func (a *Admin) ExpireDocuments(ctx context.Context, r *adminv1.ExpireDocumentsRequest) (*adminv1.ExpireDocumentsResponse, error) {
	if err := validRetention(r.GetRetentionDays()); err != nil {
		return nil, err
	}
	got, err := Expire(ctx, a.Pool, r.GetRetentionDays(), time.Now(), a.Audit)
	if err != nil {
		a.Log.ErrorContext(ctx, "按保留期写删除标记失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	a.Log.InfoContext(ctx, "已按保留期写删除标记", slog.Any("tombstoned", got))
	return &adminv1.ExpireDocumentsResponse{Tombstoned: got}, nil
}
