package auditquery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PG 是审计索引表（迁移 0019）。
type PG struct {
	Pool *pgxpool.Pool
}

// indexLock 是索引任务的 advisory lock 键（多副本只让一个在跑）
const indexLock int64 = 0x6175646974

const rowCols = `ts, record_id, kind, source, object_key, line, trace_id, tenant, user_id, action, object_id,
	shown_doc_ids, context_doc_ids, listed_doc_ids`

func (s *PG) Indexed(ctx context.Context, keys []string) (map[string]bool, error) {
	rows, err := s.Pool.Query(ctx, `SELECT object_key FROM governance.audit_index_objects WHERE object_key = ANY($1)`, keys)
	if err != nil {
		return nil, err
	}
	have, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(have))
	for _, k := range have {
		out[k] = true
	}
	return out, nil
}

func (s *PG) Add(ctx context.Context, objectKey string, rows []Row) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		months := map[time.Time]bool{}
		for _, r := range rows {
			t := r.TS.UTC()
			months[time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)] = true
		}
		for m := range months {
			// 分区名与边界都由日期算出，不含外部输入
			if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS governance.audit_index_y%04dm%02d
				PARTITION OF governance.audit_index FOR VALUES FROM ('%s') TO ('%s')`,
				m.Year(), m.Month(), m.Format(time.RFC3339), m.AddDate(0, 1, 0).Format(time.RFC3339))); err != nil {
				return fmt.Errorf("建分区: %w", err)
			}
		}
		b := &pgx.Batch{}
		for _, r := range rows {
			b.Queue(`INSERT INTO governance.audit_index (`+rowCols+`) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
				ON CONFLICT DO NOTHING`, r.TS, r.RecordID, r.Kind, r.Source, r.ObjectKey, r.Line, r.TraceID, r.Tenant, r.UserID, r.Action,
				r.ObjectID, nonNil(r.ShownDocIDs), nonNil(r.ContextDocIDs), nonNil(r.ListedDocIDs))
		}
		b.Queue(`INSERT INTO governance.audit_index_objects (object_key, records) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			objectKey, len(rows))
		return tx.SendBatch(ctx, b).Close()
	})
}

func nonNil(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

func (s *PG) Find(ctx context.Context, f Filter) ([]Row, error) {
	// 条件按需拼接（只拼占位符，不拼值）：带 “$n = '' OR …” 的通用写法在缓存的通用计划里用不上 GIN 索引
	where := []string{"ts BETWEEN $1 AND $2", "kind = ANY($3)"}
	args := []any{f.From, f.To, f.Kinds}
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if f.Sub != "" {
		where = append(where, "user_id = "+arg(f.Sub))
	}
	if f.DocID != "" {
		d := arg([]string{f.DocID})
		where = append(where, fmt.Sprintf("(shown_doc_ids @> %s OR context_doc_ids @> %s OR listed_doc_ids @> %s OR object_id = %s)",
			d, d, d, arg(f.DocID)))
	}
	if f.After != nil {
		where = append(where, fmt.Sprintf("(ts, record_id) > (%s::timestamptz, %s::text)", arg(f.After.TS), arg(f.After.RecordID)))
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+rowCols+` FROM governance.audit_index WHERE `+strings.Join(where, " AND ")+
		` ORDER BY ts, record_id LIMIT `+arg(f.Limit), args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Row, error) {
		var x Row
		err := r.Scan(&x.TS, &x.RecordID, &x.Kind, &x.Source, &x.ObjectKey, &x.Line, &x.TraceID, &x.Tenant, &x.UserID, &x.Action,
			&x.ObjectID, &x.ShownDocIDs, &x.ContextDocIDs, &x.ListedDocIDs)
		return x, err
	})
}

func (s *PG) State(ctx context.Context, name string) (string, error) {
	var v string
	err := s.Pool.QueryRow(ctx, `SELECT value FROM governance.audit_index_state WHERE name = $1`, name).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *PG) SetState(ctx context.Context, name, value string) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO governance.audit_index_state (name, value) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET value = excluded.value`, name, value)
	return err
}

func (s *PG) Exclusive(ctx context.Context, fn func(ctx context.Context) error) (bool, error) {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Release()
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, indexLock).Scan(&ok); err != nil || !ok {
		return false, err
	}
	defer func() {
		// 释放失败时连接随后归还连接池，锁跟着会话：关掉这条连接，锁一并释放
		if _, err := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, indexLock); err != nil {
			_ = conn.Conn().Close(context.WithoutCancel(ctx))
		}
	}()
	return true, fn(ctx)
}
