package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Restore 把一个快照恢复进灾备站点的空库（已执行过同一版本的迁移），恢复完线上别名指向快照里的版本。
// 密钥已销毁（文档已物理删除）的文档跳过；文档对象缺失的计数并跳过，不中断恢复。
type Restore struct {
	Pool   *pgxpool.Pool
	Bucket Bucket
	Keys   Wrapper
	Log    *slog.Logger
}

// RestoreResult 是一次恢复的统计。
type RestoreResult struct {
	Snapshot string
	Version  string
	Restored int
	// Shredded 是备份数据密钥已销毁、按设计不再能恢复的文档
	Shredded int
	// Missing 是清单里有、桶里找不到对象的文档
	Missing int
}

// ErrNotEmpty 表示目标库里已经有索引数据：恢复只往空库里写，不覆盖。
var ErrNotEmpty = errors.New("目标库里已有索引数据，恢复只写入空库")

// Latest 返回桶里最新的已完成快照。
func Latest(ctx context.Context, bucket Bucket) (string, error) {
	keys, err := bucket.List(ctx, "snapshots/")
	if err != nil {
		return "", err
	}
	ids := snapshotsIn(keys)
	if len(ids) == 0 {
		return "", errors.New("备份桶里没有已完成的快照")
	}
	return ids[len(ids)-1], nil
}

func (r *Restore) Run(ctx context.Context, snapshot string) (RestoreResult, error) {
	res := RestoreResult{Snapshot: snapshot}
	body, err := r.Bucket.Get(ctx, "snapshots/"+snapshot+"/manifest.json")
	if err != nil {
		return res, fmt.Errorf("读快照 %s: %w", snapshot, err)
	}
	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return res, err
	}
	res.Version = m.LiveVersion
	var docs int
	var schema int64
	if err := r.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM idx.documents) + (SELECT count(*) FROM idx.index_versions),
		(SELECT coalesce(max(version), 0) FROM public.schema_migrations WHERE service = 'indexing')`).Scan(&docs, &schema); err != nil {
		return res, err
	}
	if docs > 0 {
		return res, ErrNotEmpty
	}
	if schema != m.Schema {
		return res, fmt.Errorf("目标库的迁移版本是 %d，快照是 %d：先用同一版本的 indexing 执行 migrate", schema, m.Schema)
	}
	keys, err := r.Keys.Unwrap(ctx, []string{m.WrappedKey})
	if err != nil {
		return res, fmt.Errorf("解开快照密钥: %w", err)
	}
	key := keys[0]
	if err := r.meta(ctx, key, &m); err != nil {
		return res, fmt.Errorf("恢复快照级数据: %w", err)
	}
	rings := map[int16]map[string]string{}
	for _, part := range m.Parts {
		if err := r.part(ctx, key, &m, part, rings, &res); err != nil {
			return res, fmt.Errorf("恢复 %s: %w", part, err)
		}
		r.Log.InfoContext(ctx, "已恢复一个文档清单分片", slog.String("part", part), slog.Int("restored", res.Restored))
	}
	// 权限变更序号接着备份里的最大值往上发
	if _, err := r.Pool.Exec(ctx, `SELECT setval('idx.acl_seq', greatest((SELECT max(acl_seq) FROM idx.doc_acl), 1))`); err != nil {
		return res, err
	}
	return res, nil
}

func (r *Restore) meta(ctx context.Context, key []byte, m *Manifest) error {
	sealed, err := r.Bucket.Get(ctx, "snapshots/"+m.Snapshot+"/meta")
	if err != nil {
		return err
	}
	plain, err := open(key, sealed, partAAD(m.Snapshot, "meta"))
	if err != nil {
		return err
	}
	var mt meta
	if err := json.Unmarshal(plain, &mt); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, r.Pool, func(tx pgx.Tx) error {
		// 快照里的版本恢复成线上版本；待发布、已退役的版本不在快照里
		if _, err := tx.Exec(ctx, `INSERT INTO idx.index_versions
			SELECT (json_populate_record(NULL::idx.index_versions, $1::json)).*`, string(mt.IndexVersion)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE idx.index_versions SET status = 'live' WHERE version = $1`, m.LiveVersion); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO idx.aliases (alias, version) VALUES ('prod', $1)`, m.LiveVersion); err != nil {
			return err
		}
		for table, rows := range map[string]json.RawMessage{"deny_list": mt.DenyList, "deny_scopes": mt.DenyScopes,
			"partition_grants": mt.PartitionGrants, "partition_grant_state": mt.PartitionGrantState} {
			if _, err := tx.Exec(ctx, `INSERT INTO idx.`+table+` SELECT * FROM json_populate_recordset(NULL::idx.`+table+`, $1::json)`,
				string(rows)); err != nil {
				return fmt.Errorf("%s: %w", table, err)
			}
		}
		return nil
	})
}

// docRows 是一个文档对象里的行
type docRows struct {
	Documents     json.RawMessage `json:"documents"`
	Chunks        json.RawMessage `json:"chunks"`
	Relations     json.RawMessage `json:"relations"`
	Sections      json.RawMessage `json:"sections"`
	Entities      json.RawMessage `json:"entities"`
	Supersessions json.RawMessage `json:"supersessions"`
}

func (r *Restore) part(ctx context.Context, key []byte, m *Manifest, part string, rings map[int16]map[string]string, res *RestoreResult) error {
	sealed, err := r.Bucket.Get(ctx, "snapshots/"+m.Snapshot+"/"+part)
	if err != nil {
		return err
	}
	plain, err := open(key, sealed, partAAD(m.Snapshot, part))
	if err != nil {
		return err
	}
	var entries []entry
	if err := json.Unmarshal(plain, &entries); err != nil {
		return err
	}
	// 先把这一分片涉及的数据密钥批量解开
	var wanted []entry
	var wrapped []string
	for _, e := range entries {
		ring, err := r.ring(ctx, shardOf(e.Doc), rings)
		if err != nil {
			return err
		}
		w, ok := ring[e.Doc]
		if !ok {
			res.Shredded++
			continue
		}
		wanted, wrapped = append(wanted, e), append(wrapped, w)
	}
	plainKeys, err := r.Keys.Unwrap(ctx, wrapped)
	if err != nil {
		return err
	}
	for i, e := range wanted {
		sealed, err := r.Bucket.Get(ctx, docKey(e.Doc, e.Version))
		if errors.Is(err, ErrNotFound) {
			r.Log.WarnContext(ctx, "备份桶里找不到文档对象", slog.String("doc_id", e.Doc), slog.Int64("doc_version", e.Version))
			res.Missing++
			continue
		}
		if err != nil {
			return err
		}
		body, err := open(plainKeys[i], sealed, docAAD(e.Doc, e.Version))
		if err != nil {
			return fmt.Errorf("%s: %w", e.Doc, err)
		}
		var rows docRows
		if err := json.Unmarshal(body, &rows); err != nil {
			return err
		}
		if err := pgx.BeginFunc(ctx, r.Pool, func(tx pgx.Tx) error { return insertDoc(ctx, tx, m.LiveVersion, e, rows) }); err != nil {
			return fmt.Errorf("%s: %w", e.Doc, err)
		}
		res.Restored++
	}
	return nil
}

// ring 读一个密钥环分片（同一次恢复里只读一次）；分片不存在说明它从来没有密钥
func (r *Restore) ring(ctx context.Context, shard int16, rings map[int16]map[string]string) (map[string]string, error) {
	if ring, ok := rings[shard]; ok {
		return ring, nil
	}
	body, err := r.Bucket.Get(ctx, keyringKey(shard))
	if errors.Is(err, ErrNotFound) {
		rings[shard] = map[string]string{}
		return rings[shard], nil
	}
	if err != nil {
		return nil, err
	}
	var kr keyring
	if err := json.Unmarshal(body, &kr); err != nil {
		return nil, err
	}
	rings[shard] = kr.Keys
	return kr.Keys, nil
}

func insertDoc(ctx context.Context, tx pgx.Tx, version string, e entry, rows docRows) error {
	// 文档行在先（其余各表引用它），实体在关系之前；实体与替代声明可能被别的文档带过，已有的不动
	steps := []struct{ sql, rows string }{
		{`INSERT INTO idx.documents SELECT (json_populate_record(NULL::idx.documents, $1::json)).*`, string(rows.Documents)},
		{`INSERT INTO idx.doc_acl SELECT (json_populate_record(NULL::idx.doc_acl, $1::json)).*`, string(e.ACL)},
		{`INSERT INTO idx.chunks SELECT * FROM json_populate_recordset(NULL::idx.chunks, $1::json)`, string(rows.Chunks)},
		{`INSERT INTO idx.entities SELECT * FROM json_populate_recordset(NULL::idx.entities, $1::json) ON CONFLICT DO NOTHING`, string(rows.Entities)},
		{`INSERT INTO idx.relations SELECT * FROM json_populate_recordset(NULL::idx.relations, $1::json)`, string(rows.Relations)},
		{`INSERT INTO idx.sections SELECT * FROM json_populate_recordset(NULL::idx.sections, $1::json)`, string(rows.Sections)},
		{`INSERT INTO idx.supersessions SELECT * FROM json_populate_recordset(NULL::idx.supersessions, $1::json) ON CONFLICT DO NOTHING`,
			string(rows.Supersessions)},
	}
	for _, st := range steps {
		if _, err := tx.Exec(ctx, st.sql, st.rows); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `INSERT INTO idx.version_docs (version, doc_id, doc_version) VALUES ($1, $2, $3)`, version, e.Doc, e.Version)
	return err
}
