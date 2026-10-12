// Package identity 读取平台身份库（identities、identity_groups，由 governance 的身份同步写入）。
package identity

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// User 是平台身份库中的用户属性与群组。
type User struct {
	Username, Dept, Title, Region string
	Clearance                     int32
	Active                        bool
	Groups                        []string
}

type PGStore struct {
	pool *pgxpool.Pool
}

func NewPGStore(pool *pgxpool.Pool) *PGStore { return &PGStore{pool: pool} }

// Lookup 返回用户；不存在时 ok 为 false。
func (s *PGStore) Lookup(ctx context.Context, username string) (User, bool, error) {
	u := User{Username: username}
	var status string
	err := s.pool.QueryRow(ctx,
		`SELECT dept, title, region, clearance, status FROM identity.identities WHERE username = $1`, username).
		Scan(&u.Dept, &u.Title, &u.Region, &u.Clearance, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, false, nil
	}
	if err != nil {
		return User{}, false, err
	}
	u.Active = status == "active"
	rows, err := s.pool.Query(ctx, `SELECT grp FROM identity.identity_groups WHERE username = $1 ORDER BY grp`, username)
	if err != nil {
		return User{}, false, err
	}
	u.Groups, err = pgx.CollectRows(rows, pgx.RowTo[string])
	return u, true, err
}

// Unknown 返回平台身份库里不存在的用户与群组（推送入库校验 ACL 用，入库管道详设 14.5）。停用的用户仍算存在：
// 停用只影响他自己的访问，ACL 里留着不会放大权限。
func (s *PGStore) Unknown(ctx context.Context, users, groups []string) ([]string, []string, error) {
	rows, err := s.pool.Query(ctx, `SELECT u FROM unnest($1::text[]) u
		WHERE NOT EXISTS (SELECT 1 FROM identity.identities WHERE username = u) ORDER BY u`, users)
	if err != nil {
		return nil, nil, err
	}
	badUsers, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, nil, err
	}
	rows, err = s.pool.Query(ctx, `SELECT g FROM unnest($1::text[]) g
		WHERE NOT EXISTS (SELECT 1 FROM identity.identity_groups WHERE grp = g) ORDER BY g`, groups)
	if err != nil {
		return nil, nil, err
	}
	badGroups, err := pgx.CollectRows(rows, pgx.RowTo[string])
	return badUsers, badGroups, err
}

// LastSync 返回所有同步源中最早的一次成功时间；任一同步源从未成功过时返回零值（视为不新鲜）。
func (s *PGStore) LastSync(ctx context.Context) (time.Time, error) {
	var t *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT CASE WHEN bool_or(last_success_at IS NULL) THEN NULL ELSE min(last_success_at) END FROM identity.sync_state`).Scan(&t)
	if err != nil || t == nil {
		return time.Time{}, err
	}
	return *t, nil
}
