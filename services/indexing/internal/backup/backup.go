// Package backup 是灾备备份与恢复（总体设计 11.5，部署配置清单 P2）。
//
// 每小时（indexing backup，CronJob）把线上索引版本写进备份桶，桶跨地域复制到灾备站点；灾备站点用 indexing restore
// 把某个快照恢复进一个空库，再把别名与 DNS 切过去（RPO 1 小时、RTO 4 小时）。
//
// 桶里的布局：
//
//	docs/<sha256(doc_id)>/<doc_version>   一份文档一个内容版本的全部行（文档、块、关系、章节摘要、涉及的实体、替代声明），
//	                                     用这份文档的备份数据密钥加密；只在第一次或文档行变化后写
//	keyring/<分片>.json                   文档数据密钥（经密钥服务包裹），按 doc_id 的 SHA-256 首字节分 256 片
//	snapshots/<快照>/meta、part-NNNN      快照级数据（线上版本、拒绝名单、分区授权）与文档清单（含权限行），用快照密钥加密
//	snapshots/<快照>/manifest.json        最后写，写完这个快照才算完成；包裹后的快照密钥也在这里
//
// 删除：文档物理删除时销毁它的备份数据密钥（删 idx.backup_keys 的行），下一次备份重写那个密钥环分片并清掉旧版本，
// 之后所有快照里这份文档的内容都解不开（加密删除），这时补记进删除凭证（backups）。
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Retention 是快照的保留时长；最新的一个快照总是保留。
const Retention = 48 * time.Hour

const (
	// docBatch 是每个事务备份的文档数
	docBatch = 200
	// partSize 是文档清单每个分片的文档数
	partSize = 20000
)

// StoreBackups 是删除凭证里的这一项
const StoreBackups = "backups"

// Manifest 是一个快照的说明，明文。
type Manifest struct {
	Snapshot       string    `json:"snapshot"`
	CreatedAt      time.Time `json:"created_at"`
	LiveVersion    string    `json:"live_version"`
	EmbeddingModel string    `json:"embedding_model"`
	// Schema 是备份时 indexing 最新的迁移编号，恢复的目标库必须一致（行按列名整行写回）
	Schema     int64    `json:"schema"`
	Docs       int      `json:"docs"`
	Parts      []string `json:"parts"`
	WrappedKey string   `json:"wrapped_key"`
}

type Backup struct {
	Pool   *pgxpool.Pool
	Bucket Bucket
	Keys   Wrapper
	Log    *slog.Logger
	Now    func() time.Time
}

// Result 是一次备份的统计。
type Result struct {
	Snapshot      string
	Docs, Written int
	Shards        int
	Receipts      int
	Expired       int
}

// Run 做一次备份：写新增或变化的文档对象 → 写快照 → 重写有变化的密钥环分片并补记删除凭证 → 清理过期快照与对象。
func (b *Backup) Run(ctx context.Context) (Result, error) {
	now := b.Now().UTC()
	res := Result{Snapshot: now.Format("20060102T150405Z")}
	var m Manifest
	err := b.Pool.QueryRow(ctx, `SELECT a.version, v.embedding_model,
			(SELECT coalesce(max(version), 0) FROM public.schema_migrations WHERE service = 'indexing')
		FROM idx.aliases a JOIN idx.index_versions v USING (version) WHERE a.alias = 'prod'`).
		Scan(&m.LiveVersion, &m.EmbeddingModel, &m.Schema)
	if errors.Is(err, pgx.ErrNoRows) {
		b.Log.InfoContext(ctx, "还没有线上版本，不备份")
		return res, nil
	}
	if err != nil {
		return res, err
	}
	if res.Written, err = b.writeDocs(ctx, m.LiveVersion); err != nil {
		return res, fmt.Errorf("写文档对象: %w", err)
	}
	m.Snapshot, m.CreatedAt = res.Snapshot, now
	if err := b.writeSnapshot(ctx, &m); err != nil {
		return res, fmt.Errorf("写快照: %w", err)
	}
	res.Docs = m.Docs
	if res.Shards, res.Receipts, err = b.writeKeyring(ctx); err != nil {
		return res, fmt.Errorf("重写密钥环: %w", err)
	}
	if res.Expired, err = b.expire(ctx, now); err != nil {
		return res, fmt.Errorf("清理过期备份: %w", err)
	}
	return res, nil
}

// docKey 是文档对象在桶里的键；doc_id 取摘要，键里不出现文档 ID。
func docKey(docID string, version int64) string {
	h := sha256.Sum256([]byte(docID))
	return fmt.Sprintf("docs/%s/%d", hex.EncodeToString(h[:]), version)
}

func docAAD(docID string, version int64) []byte { return fmt.Appendf(nil, "%s#%d", docID, version) }

// shardOf 是 doc_id 所在的密钥环分片。
func shardOf(docID string) int16 {
	h := sha256.Sum256([]byte(docID))
	return int16(h[0])
}

func keyringKey(shard int16) string { return fmt.Sprintf("keyring/%02x.json", shard) }

// docPayloadSQL 取一份文档一个内容版本的全部行（整行 JSON，恢复时按同一张表的列写回）
const docPayloadSQL = `SELECT json_build_object(
	'documents', (SELECT row_to_json(d) FROM idx.documents d WHERE d.doc_id = $1),
	'chunks', (SELECT coalesce(json_agg(c), '[]') FROM idx.chunks c WHERE c.doc_id = $1 AND c.doc_version = $2),
	'relations', (SELECT coalesce(json_agg(r), '[]') FROM idx.relations r WHERE r.doc_id = $1 AND r.doc_version = $2),
	'sections', (SELECT coalesce(json_agg(s), '[]') FROM idx.sections s WHERE s.doc_id = $1 AND s.doc_version = $2),
	'entities', (SELECT coalesce(json_agg(e), '[]') FROM idx.entities e WHERE e.entity_id IN (
		SELECT from_entity FROM idx.relations WHERE doc_id = $1 AND doc_version = $2
		UNION SELECT to_entity FROM idx.relations WHERE doc_id = $1 AND doc_version = $2)),
	'supersessions', (SELECT coalesce(json_agg(p), '[]') FROM idx.supersessions p
		WHERE p.by_doc_id = $1 AND p.source_id = (SELECT source_id FROM idx.documents WHERE doc_id = $1))
)::text`

type pending struct {
	doc     string
	version int64
	payload []byte
}

// writeDocs 写线上版本里还没备份、或文档行在备份之后变过的文档对象。每批在一个事务里锁住文档的权限行（FOR SHARE），
// 跳过已物理删除的：与物理删除互斥，不会给刚销毁了密钥的文档再建一把密钥。
func (b *Backup) writeDocs(ctx context.Context, version string) (int, error) {
	written, after := 0, ""
	for {
		var batch []pending
		var keys map[string][]byte
		var readAt time.Time
		err := pgx.BeginFunc(ctx, b.Pool, func(tx pgx.Tx) error {
			if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&readAt); err != nil {
				return err
			}
			rows, err := tx.Query(ctx, `SELECT vd.doc_id, vd.doc_version FROM idx.version_docs vd
				JOIN idx.documents d ON d.doc_id = vd.doc_id
				JOIN idx.doc_acl a ON a.doc_id = vd.doc_id
				LEFT JOIN idx.backup_objects bo ON bo.doc_id = vd.doc_id AND bo.doc_version = vd.doc_version
				WHERE vd.version = $1 AND vd.doc_id > $2 AND a.purged_at IS NULL
					AND (bo.doc_id IS NULL OR d.updated_at > bo.written_at)
				ORDER BY vd.doc_id LIMIT $3
				FOR SHARE OF a`, version, after, docBatch)
			if err != nil {
				return err
			}
			batch, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (pending, error) {
				var p pending
				err := r.Scan(&p.doc, &p.version)
				return p, err
			})
			if err != nil {
				return err
			}
			for i := range batch {
				var s string
				if err := tx.QueryRow(ctx, docPayloadSQL, batch[i].doc, batch[i].version).Scan(&s); err != nil {
					return err
				}
				batch[i].payload = []byte(s)
			}
			keys, err = b.docKeys(ctx, tx, batch)
			return err
		})
		if err != nil {
			return written, err
		}
		if len(batch) == 0 {
			return written, nil
		}
		after = batch[len(batch)-1].doc
		for _, p := range batch {
			sealed, err := seal(keys[p.doc], p.payload, docAAD(p.doc, p.version))
			if err != nil {
				return written, err
			}
			if err := b.Bucket.Put(ctx, docKey(p.doc, p.version), sealed); err != nil {
				return written, err
			}
			if _, err := b.Pool.Exec(ctx, `INSERT INTO idx.backup_objects (doc_id, doc_version, written_at, last_ref)
				VALUES ($1, $2, $3, $3) ON CONFLICT (doc_id, doc_version) DO UPDATE SET written_at = EXCLUDED.written_at`,
				p.doc, p.version, readAt); err != nil {
				return written, err
			}
			written++
		}
		if len(batch) < docBatch {
			return written, nil
		}
	}
}

// docKeys 取这一批文档的数据密钥：已有的经密钥服务解开，没有的新建、包裹后写进 idx.backup_keys 并标记密钥环分片。
func (b *Backup) docKeys(ctx context.Context, tx pgx.Tx, batch []pending) (map[string][]byte, error) {
	docs := make([]string, len(batch))
	for i, p := range batch {
		docs[i] = p.doc
	}
	rows, err := tx.Query(ctx, `SELECT doc_id, wrapped FROM idx.backup_keys WHERE doc_id = ANY($1::text[])`, docs)
	if err != nil {
		return nil, err
	}
	type kw struct{ doc, wrapped string }
	have, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (kw, error) {
		var k kw
		err := r.Scan(&k.doc, &k.wrapped)
		return k, err
	})
	if err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	if len(have) > 0 {
		wrapped := make([]string, len(have))
		for i, k := range have {
			wrapped[i] = k.wrapped
		}
		plain, err := b.Keys.Unwrap(ctx, wrapped)
		if err != nil {
			return nil, err
		}
		for i, k := range have {
			out[k.doc] = plain[i]
		}
	}
	var missing []string
	var fresh [][]byte
	for _, d := range docs {
		if _, ok := out[d]; ok || slices.Contains(missing, d) {
			continue
		}
		k, err := newKey()
		if err != nil {
			return nil, err
		}
		missing, fresh = append(missing, d), append(fresh, k)
	}
	if len(missing) == 0 {
		return out, nil
	}
	wrapped, err := b.Keys.Wrap(ctx, fresh)
	if err != nil {
		return nil, err
	}
	for i, d := range missing {
		if _, err := tx.Exec(ctx, `INSERT INTO idx.backup_keys (doc_id, shard, wrapped) VALUES ($1, $2, $3)`,
			d, shardOf(d), wrapped[i]); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO idx.backup_keyring_dirty (shard) VALUES ($1)
			ON CONFLICT (shard) DO UPDATE SET marked_at = now()`, shardOf(d)); err != nil {
			return nil, err
		}
		out[d] = fresh[i]
	}
	return out, nil
}

// entry 是文档清单里的一项：文档、内容版本与权限行（权限不分版本，取备份时的最新值）。
type entry struct {
	Doc     string          `json:"doc_id"`
	Version int64           `json:"doc_version"`
	ACL     json.RawMessage `json:"acl"`
}

// meta 是快照级的行
type meta struct {
	IndexVersion        json.RawMessage `json:"index_version"`
	DenyList            json.RawMessage `json:"deny_list"`
	DenyScopes          json.RawMessage `json:"deny_scopes"`
	PartitionGrants     json.RawMessage `json:"partition_grants"`
	PartitionGrantState json.RawMessage `json:"partition_grant_state"`
}

const metaSQL = `SELECT json_build_object(
	'index_version', (SELECT row_to_json(v) FROM idx.index_versions v WHERE v.version = $1),
	'deny_list', (SELECT coalesce(json_agg(x), '[]') FROM idx.deny_list x),
	'deny_scopes', (SELECT coalesce(json_agg(x), '[]') FROM idx.deny_scopes x),
	'partition_grants', (SELECT coalesce(json_agg(x), '[]') FROM idx.partition_grants x),
	'partition_grant_state', (SELECT coalesce(json_agg(x), '[]') FROM idx.partition_grant_state x)
)::text`

func partAAD(snapshot, name string) []byte { return []byte(snapshot + "/" + name) }

// writeSnapshot 写快照：快照级的行、分片的文档清单（只列已有备份对象的文档），最后写 manifest。同时刷新这些对象的 last_ref。
func (b *Backup) writeSnapshot(ctx context.Context, m *Manifest) error {
	key, err := newKey()
	if err != nil {
		return err
	}
	wrapped, err := b.Keys.Wrap(ctx, [][]byte{key})
	if err != nil {
		return err
	}
	m.WrappedKey = wrapped[0]
	prefix := "snapshots/" + m.Snapshot + "/"
	var metaJSON string
	if err := b.Pool.QueryRow(ctx, metaSQL, m.LiveVersion).Scan(&metaJSON); err != nil {
		return err
	}
	sealed, err := seal(key, []byte(metaJSON), partAAD(m.Snapshot, "meta"))
	if err != nil {
		return err
	}
	if err := b.Bucket.Put(ctx, prefix+"meta", sealed); err != nil {
		return err
	}
	after := ""
	for {
		rows, err := b.Pool.Query(ctx, `SELECT vd.doc_id, vd.doc_version, row_to_json(a)::text FROM idx.version_docs vd
			JOIN idx.doc_acl a ON a.doc_id = vd.doc_id
			JOIN idx.backup_objects bo ON bo.doc_id = vd.doc_id AND bo.doc_version = vd.doc_version
			WHERE vd.version = $1 AND vd.doc_id > $2 AND a.purged_at IS NULL
			ORDER BY vd.doc_id LIMIT $3`, m.LiveVersion, after, partSize)
		if err != nil {
			return err
		}
		docs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (entry, error) {
			var e entry
			var acl string
			err := r.Scan(&e.Doc, &e.Version, &acl)
			e.ACL = json.RawMessage(acl)
			return e, err
		})
		if err != nil {
			return err
		}
		if len(docs) == 0 {
			break
		}
		name := fmt.Sprintf("part-%04d", len(m.Parts))
		body, err := json.Marshal(docs)
		if err != nil {
			return err
		}
		sealed, err := seal(key, body, partAAD(m.Snapshot, name))
		if err != nil {
			return err
		}
		if err := b.Bucket.Put(ctx, prefix+name, sealed); err != nil {
			return err
		}
		ids, versions := make([]string, len(docs)), make([]int64, len(docs))
		for i, e := range docs {
			ids[i], versions[i] = e.Doc, e.Version
		}
		if _, err := b.Pool.Exec(ctx, `UPDATE idx.backup_objects bo SET last_ref = $3
			FROM unnest($1::text[], $2::bigint[]) AS u(doc_id, doc_version)
			WHERE bo.doc_id = u.doc_id AND bo.doc_version = u.doc_version`, ids, versions, m.CreatedAt); err != nil {
			return err
		}
		m.Parts = append(m.Parts, name)
		m.Docs += len(docs)
		after = docs[len(docs)-1].Doc
		if len(docs) < partSize {
			break
		}
	}
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return b.Bucket.Put(ctx, prefix+"manifest.json", body)
}

// keyring 是一个密钥环分片：doc_id → 包裹后的数据密钥
type keyring struct {
	Keys map[string]string `json:"keys"`
}

// writeKeyring 重写有变化的密钥环分片，清掉旧版本（桶开了版本控制时覆盖写不会让旧内容消失），
// 然后把这些分片上销毁的密钥补记进删除凭证。返回重写的分片数与补记的凭证数。
func (b *Backup) writeKeyring(ctx context.Context) (int, int, error) {
	rows, err := b.Pool.Query(ctx, `SELECT shard FROM idx.backup_keyring_dirty ORDER BY shard`)
	if err != nil {
		return 0, 0, err
	}
	shards, err := pgx.CollectRows(rows, pgx.RowTo[int16])
	if err != nil {
		return 0, 0, err
	}
	receipts := 0
	for _, shard := range shards {
		var readAt time.Time
		kr := keyring{Keys: map[string]string{}}
		err := pgx.BeginFunc(ctx, b.Pool, func(tx pgx.Tx) error {
			if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&readAt); err != nil {
				return err
			}
			rows, err := tx.Query(ctx, `SELECT doc_id, wrapped FROM idx.backup_keys WHERE shard = $1`, shard)
			if err != nil {
				return err
			}
			for rows.Next() {
				var d, w string
				if err := rows.Scan(&d, &w); err != nil {
					rows.Close()
					return err
				}
				kr.Keys[d] = w
			}
			return rows.Err()
		})
		if err != nil {
			return 0, receipts, err
		}
		body, err := json.Marshal(kr)
		if err != nil {
			return 0, receipts, err
		}
		if err := b.Bucket.Put(ctx, keyringKey(shard), body); err != nil {
			return 0, receipts, err
		}
		if err := b.Bucket.Purge(ctx, keyringKey(shard)); err != nil {
			return 0, receipts, err
		}
		done, err := json.Marshal(b.Now().UTC())
		if err != nil {
			return 0, receipts, err
		}
		err = pgx.BeginFunc(ctx, b.Pool, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `DELETE FROM idx.backup_shreds WHERE shard = $1 AND destroyed_at <= $2 RETURNING receipt_id`,
				shard, readAt)
			if err != nil {
				return err
			}
			ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				return err
			}
			if len(ids) > 0 {
				if _, err := tx.Exec(ctx, `UPDATE idx.deletion_receipts SET stores = jsonb_build_object($2::text, $3::jsonb) || stores
					WHERE receipt_id = ANY($1::text[])`, ids, StoreBackups, string(done)); err != nil {
					return err
				}
			}
			receipts += len(ids)
			_, err = tx.Exec(ctx, `DELETE FROM idx.backup_keyring_dirty WHERE shard = $1 AND marked_at <= $2`, shard, readAt)
			return err
		})
		if err != nil {
			return 0, receipts, err
		}
	}
	return len(shards), receipts, nil
}

// snapshotsIn 从桶里的键列出已完成（有 manifest）的快照，按时间升序。
func snapshotsIn(keys []string) []string {
	var out []string
	for _, k := range keys {
		if id, ok := strings.CutSuffix(strings.TrimPrefix(k, "snapshots/"), "/manifest.json"); ok && !strings.Contains(id, "/") {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// expired 返回要删的快照（早于 now-Retention，最新的一个总是保留）与保留下来的最老快照的时间。
func expired(snapshots []string, now time.Time) ([]string, time.Time) {
	var drop []string
	oldest := now
	for i, id := range snapshots {
		at, err := time.Parse("20060102T150405Z", id)
		if err != nil {
			continue
		}
		if at.Before(now.Add(-Retention)) && i < len(snapshots)-1 {
			drop = append(drop, id)
			continue
		}
		if at.Before(oldest) {
			oldest = at
		}
	}
	return drop, oldest
}

// expire 删过期快照，再删保留的快照都不再引用的文档对象（last_ref 早于保留的最老快照）。返回删掉的快照数。
func (b *Backup) expire(ctx context.Context, now time.Time) (int, error) {
	keys, err := b.Bucket.List(ctx, "snapshots/")
	if err != nil {
		return 0, err
	}
	drop, oldest := expired(snapshotsIn(keys), now)
	for _, id := range drop {
		// manifest 先删：删到一半的快照不会被当成完整的拿去恢复
		if err := b.Bucket.Delete(ctx, "snapshots/"+id+"/manifest.json"); err != nil {
			return 0, err
		}
		for _, k := range keys {
			if strings.HasPrefix(k, "snapshots/"+id+"/") && !strings.HasSuffix(k, "/manifest.json") {
				if err := b.Bucket.Delete(ctx, k); err != nil {
					return 0, err
				}
			}
		}
	}
	for {
		rows, err := b.Pool.Query(ctx, `SELECT doc_id, doc_version FROM idx.backup_objects WHERE last_ref < $1 LIMIT 1000`, oldest)
		if err != nil {
			return len(drop), err
		}
		gone, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (pending, error) {
			var p pending
			err := r.Scan(&p.doc, &p.version)
			return p, err
		})
		if err != nil || len(gone) == 0 {
			return len(drop), err
		}
		for _, p := range gone {
			if err := b.Bucket.Delete(ctx, docKey(p.doc, p.version)); err != nil {
				return len(drop), err
			}
			if _, err := b.Pool.Exec(ctx, `DELETE FROM idx.backup_objects WHERE doc_id = $1 AND doc_version = $2`, p.doc, p.version); err != nil {
				return len(drop), err
			}
		}
	}
}
