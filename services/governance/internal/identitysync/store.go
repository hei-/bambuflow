package identitysync

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore 把身份同步结果写入元数据库中的平台身份库（identity schema）。
type PGStore struct {
	pool *pgxpool.Pool
}

func NewPGStore(pool *pgxpool.Pool) *PGStore { return &PGStore{pool: pool} }

func (s *PGStore) State(ctx context.Context, source string) (State, error) {
	var st State
	var full *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT last_full_at, users_cursor, groups_cursor FROM identity.sync_state WHERE source = $1`, source).
		Scan(&full, &st.UsersCursor, &st.GroupsCursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return State{}, nil
	}
	if full != nil {
		st.LastFullAt = *full
	}
	return st, err
}

func (s *PGStore) Apply(ctx context.Context, source string, b Batch) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error { return apply(ctx, tx, source, b) })
}

// apply 在 tx 里写入 batch 并把同步源标为成功。
func apply(ctx context.Context, tx pgx.Tx, source string, b Batch) error {
	seen := make([]string, 0, len(b.Identities))
	for _, id := range b.Identities {
		status := "active"
		if !id.Active {
			status = "disabled"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO identity.identities (username, display_name, dept, title, clearance, region, status, source, source_dn, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
			ON CONFLICT (username) DO UPDATE SET display_name = EXCLUDED.display_name, dept = EXCLUDED.dept,
				title = EXCLUDED.title, clearance = EXCLUDED.clearance, region = EXCLUDED.region,
				status = EXCLUDED.status, source = EXCLUDED.source, source_dn = EXCLUDED.source_dn, updated_at = now()`,
			id.Username, id.DisplayName, id.Dept, id.Title, id.Clearance, id.Region, status, source, id.DN); err != nil {
			return err
		}
		seen = append(seen, id.Username)
	}
	seenGroups := make([]string, 0, len(b.Groups))
	for _, g := range b.Groups {
		// 群组按整体替换：成员被移出的变化体现在群组条目的修改时间上，增量拉到后整组重写
		if _, err := tx.Exec(ctx, `DELETE FROM identity.identity_groups WHERE source = $1 AND grp = $2`, source, g.Name); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO identity.identity_groups (grp, username, source)
			SELECT $1, u, $2 FROM unnest($3::text[]) AS u ON CONFLICT DO NOTHING`, g.Name, source, g.Members); err != nil {
			return err
		}
		seenGroups = append(seenGroups, g.Name)
	}
	if b.Full {
		// 全量对账：目录里已不存在的用户停用，已删除的群组清掉
		if _, err := tx.Exec(ctx, `
			UPDATE identity.identities SET status = 'disabled', updated_at = now()
			WHERE source = $1 AND status = 'active' AND NOT (username = ANY($2::text[]))`, source, seen); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM identity.identity_groups WHERE source = $1 AND NOT (grp = ANY($2::text[]))`, source, seenGroups); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO identity.sync_state (source, last_success_at, last_attempt_at, last_full_at, users_cursor, groups_cursor, last_error)
		VALUES ($1, now(), now(), CASE WHEN $2 THEN now() END, $3, $4, '')
		ON CONFLICT (source) DO UPDATE SET last_success_at = now(), last_attempt_at = now(),
			last_full_at = CASE WHEN $2 THEN now() ELSE identity.sync_state.last_full_at END,
			users_cursor = $3, groups_cursor = $4, last_error = ''`,
		source, b.Full, b.UsersCursor, b.GroupsCursor)
	return err
}

func (s *PGStore) RecordFailure(ctx context.Context, source string, cause error) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO identity.sync_state (source, last_attempt_at, last_error) VALUES ($1, now(), $2)
		ON CONFLICT (source) DO UPDATE SET last_attempt_at = now(), last_error = $2`, source, cause.Error())
	return err
}
