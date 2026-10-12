package identitysync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (s *PGStore) GetResource(ctx context.Context, source, typ, id string) (*Resource, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+resourceCols+` FROM identity.scim_resources WHERE source = $1 AND type = $2 AND id = $3`,
		source, typ, id)
	if err != nil {
		return nil, err
	}
	r, err := pgx.CollectOneRow(rows, scanResource)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return r, err
}

func (s *PGStore) ListResources(ctx context.Context, source, typ string, f Filter, offset, limit int) ([]*Resource, int, error) {
	where, args := `source = $1 AND type = $2`, []any{source, typ}
	switch f.Attr {
	case "":
	case "externalId":
		where, args = where+` AND body->>'externalId' = $3`, append(args, f.Value)
	default:
		// id 即 userName / displayName，不区分大小写（RFC 7643 里两者 caseExact 为 false）
		where, args = where+` AND lower(id) = lower($3)`, append(args, f.Value)
	}
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM identity.scim_resources WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT %s FROM identity.scim_resources WHERE %s ORDER BY id OFFSET $%d LIMIT $%d`,
		resourceCols, where, len(args)+1, len(args)+2), append(args, offset, limit)...)
	if err != nil {
		return nil, 0, err
	}
	out, err := pgx.CollectRows(rows, scanResource)
	return out, total, err
}

func (s *PGStore) ModifyResource(ctx context.Context, source, typ, id string, fn ModifyFunc) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+resourceCols+` FROM identity.scim_resources
			WHERE source = $1 AND type = $2 AND id = $3 FOR UPDATE`, source, typ, id)
		if err != nil {
			return err
		}
		cur, err := pgx.CollectOneRow(rows, scanResource)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			cur = nil
		case err != nil:
			return err
		}
		next, b, err := fn(cur)
		if err != nil {
			return err
		}
		switch {
		case next == nil && cur != nil:
			if _, err := tx.Exec(ctx, `DELETE FROM identity.scim_resources WHERE source = $1 AND type = $2 AND id = $3`,
				source, typ, id); err != nil {
				return err
			}
		case next != nil:
			body, err := json.Marshal(next.Body)
			if err != nil {
				return err
			}
			if cur == nil {
				// 与别的请求同时新建同一个 id：主键冲突，返回 ErrExists，上游重试时会查到已有的资源
				_, err = tx.Exec(ctx, `INSERT INTO identity.scim_resources (source, type, id, body) VALUES ($1, $2, $3, $4)`,
					source, typ, id, body)
				if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "23505" {
					return ErrExists
				}
			} else {
				_, err = tx.Exec(ctx, `UPDATE identity.scim_resources SET body = $4, version = version + 1, updated_at = now()
					WHERE source = $1 AND type = $2 AND id = $3`, source, typ, id, body)
			}
			if err != nil {
				return err
			}
		}
		return apply(ctx, tx, source, b)
	})
}

// Touch 记一次同步成功（SCIM 端点在运行、能写库）：推送式的上游没有“拉取成功”可记。
func (s *PGStore) Touch(ctx context.Context, source string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO identity.sync_state (source, last_success_at, last_attempt_at) VALUES ($1, now(), now())
		ON CONFLICT (source) DO UPDATE SET last_success_at = now(), last_attempt_at = now(), last_error = ''`, source)
	return err
}

const resourceCols = `type, id, body, version, created_at, updated_at`

func scanResource(row pgx.CollectableRow) (*Resource, error) {
	var r Resource
	var body []byte
	var created, updated time.Time
	if err := row.Scan(&r.Type, &r.ID, &body, &r.Version, &created, &updated); err != nil {
		return nil, err
	}
	r.Created, r.Updated = created, updated
	return &r, json.Unmarshal(body, &r.Body)
}
