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
