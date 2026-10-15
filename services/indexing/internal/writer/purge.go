package writer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/services/indexing/internal/backup"
)

// 索引之外由别的组件删除、补记进删除凭证的存储（RecordPurge）
const (
	storeObjectStore = "object_store"
	storeLLMTraces   = "llm_traces"
)

// PurgeStream 是 DocumentPurged 的事件流，connectors 消费。
const PurgeStream = "ingest:document-purged"

// PurgeOutboxSelectSQL、PurgeOutboxMarkSQL 供 outbox.Relay 转发 idx.purge_outbox。
const (
	PurgeOutboxSelectSQL = `SELECT event_id, payload FROM idx.purge_outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`
	PurgeOutboxMarkSQL   = `UPDATE idx.purge_outbox SET sent_at = now() WHERE event_id = ANY($1)`
)

// DefaultPurgeGrace 是删除标记到物理删除之间的观察期：期间可以撤销误写的删除标记（Z6），也覆盖回滚的观察期。
const DefaultPurgeGrace = 168 * time.Hour

// purgeBatch 是每个事务处理的文档数
const purgeBatch = 200

// purgeSteps 是索引侧要删除的副本，按顺序执行（键写进删除凭证）。关键词与向量索引是块表上的列，随块一起删除。
// 文档行与权限行保留：删除优先要靠删除标记挡住更早产生的内容写入；文档行只清掉标题、地址等内容字段，
// content_hash 清空，源端以后重新创建同一文档时按新内容写入。
var purgeSteps = []struct{ store, sql string }{
	{"graph", `DELETE FROM idx.relations WHERE doc_id = $1`},
	{"sections", `DELETE FROM idx.sections WHERE doc_id = $1`},
	{"manifests", `DELETE FROM idx.version_docs WHERE doc_id = $1`},
	{"chunks", `DELETE FROM idx.chunks WHERE doc_id = $1`},
	{"metadata", `UPDATE idx.documents SET title = '', source_uri = '', owner = '', content_hash = '', keywords = '{}',
		summary_chunk_id = '', doc_type = '', dept = '', topic_codes = '{}', updated_at = now() WHERE doc_id = $1`},
}

// PurgeDeleted 物理删除删除标记已超过观察期的文档在索引侧的全部副本，每份文档写一份删除凭证。
// 删除时间被撤销（Z6，tombstoned_at 为空）的文档等着重新拉取，不删。返回删除的文档数。
// 每批写一条变更审计（document.purge，列出文档与删除凭证），与删除同一事务。
func PurgeDeleted(ctx context.Context, pool *pgxpool.Pool, grace time.Duration, now time.Time, audit Auditor) (int64, error) {
	var total int64
	for done := false; !done; {
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `SELECT doc_id, source_id, tombstoned_at FROM idx.doc_acl
				WHERE state = 'tombstoned' AND purged_at IS NULL AND tombstoned_at < $1
				ORDER BY tombstoned_at LIMIT $2 FOR UPDATE SKIP LOCKED`, now.Add(-grace), purgeBatch)
			if err != nil {
				return err
			}
			type due struct {
				doc, source string
				at          time.Time
			}
			docs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (due, error) {
				var d due
				err := r.Scan(&d.doc, &d.source, &d.at)
				return d, err
			})
			if err != nil {
				return err
			}
			var receipts []map[string]string
			for _, d := range docs {
				stores := map[string]time.Time{}
				for _, step := range purgeSteps {
					if _, err := tx.Exec(ctx, step.sql, d.doc); err != nil {
						return err
					}
					stores[step.store] = time.Now().UTC()
				}
				receipt := "del_" + strings.TrimPrefix(newOperationID(), "op_")
				// 灾备备份：销毁这份文档的备份数据密钥（加密删除）。备份桶里的密钥环分片重写后才补记 backups；
				// 从没备份过的文档没有密钥，备份里也没有它，直接记为完成
				var shard int16
				err := tx.QueryRow(ctx, `DELETE FROM idx.backup_keys WHERE doc_id = $1 RETURNING shard`, d.doc).Scan(&shard)
				switch {
				case errors.Is(err, pgx.ErrNoRows):
					stores[backup.StoreBackups] = time.Now().UTC()
				case err != nil:
					return err
				default:
					if _, err := tx.Exec(ctx, `INSERT INTO idx.backup_shreds (receipt_id, shard) VALUES ($1, $2)`, receipt, shard); err != nil {
						return err
					}
					if _, err := tx.Exec(ctx, `INSERT INTO idx.backup_keyring_dirty (shard) VALUES ($1)
						ON CONFLICT (shard) DO UPDATE SET marked_at = now()`, shard); err != nil {
						return err
					}
				}
				b, err := json.Marshal(stores)
				if err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO idx.deletion_receipts (receipt_id, doc_id, source_id, tombstoned_at, stores)
					VALUES ($1, $2, $3, $4, $5)`, receipt, d.doc, d.source, d.at, b); err != nil {
					return err
				}
				receipts = append(receipts, map[string]string{"doc_id": d.doc, "source_id": d.source, "receipt_id": receipt})
				// 对象存储里的原文与文档包在 connectors 那边：同一事务发出 DocumentPurged，删完后补记进这份凭证
				evt := &ingestionv1.DocumentPurged{EventId: newEventID(), SourceId: d.source, DocId: d.doc, ReceiptId: receipt,
					OccurredAt: timestamppb.New(now)}
				pb, err := proto.Marshal(evt)
				if err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO idx.purge_outbox (event_id, payload) VALUES ($1, $2)`, evt.GetEventId(), pb); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `UPDATE idx.doc_acl SET purged_at = now() WHERE doc_id = $1`, d.doc); err != nil {
					return err
				}
			}
			if len(docs) > 0 {
				if err := audit.Write(ctx, "document.purge", "", map[string]any{"documents": receipts, "grace_hours": grace.Hours()}); err != nil {
					return err
				}
			}
			// 实体名称来自正文：不再被任何关系引用的实体一并删除
			if len(docs) > 0 {
				if _, err := tx.Exec(ctx, `DELETE FROM idx.entities e WHERE NOT EXISTS (
					SELECT 1 FROM idx.relations r WHERE r.from_entity = e.entity_id OR r.to_entity = e.entity_id)`); err != nil {
					return err
				}
			}
			total += int64(len(docs))
			done = len(docs) < purgeBatch
			return nil
		})
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (a *Admin) PurgeDeleted(ctx context.Context, r *adminv1.PurgeDeletedRequest) (*adminv1.PurgeDeletedResponse, error) {
	grace := DefaultPurgeGrace
	if r.GetGraceHours() < 0 {
		return nil, status.Error(codes.InvalidArgument, "grace_hours 不能为负")
	}
	if r.GetGraceHours() > 0 {
		grace = time.Duration(r.GetGraceHours()) * time.Hour
	}
	n, err := PurgeDeleted(ctx, a.Pool, grace, time.Now(), a.Audit)
	if err != nil {
		a.Log.ErrorContext(ctx, "物理删除失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	a.Log.InfoContext(ctx, "已物理删除到期的删除标记文档", slog.Int64("docs", n))
	return &adminv1.PurgeDeletedResponse{Purged: n}, nil
}
