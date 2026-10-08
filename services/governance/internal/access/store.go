// Package access 是 governance 里“谁能看到什么”的运营数据（governance 详设 3.1、6.2）：数据源登记与租户授权、
// 紧急拒绝规则。它们以 governance 的数据库为唯一事实来源；紧急拒绝规则下发到索引库的拒绝名单，授权经配置快照下发给 retrieval。
package access

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound：对象不存在。ErrExists：同一对象已存在（唯一约束）。
var (
	ErrNotFound = errors.New("不存在")
	ErrExists   = errors.New("已存在")
)

// Denial 是一条紧急拒绝规则。Scope 为 source、document、tenant。
type Denial struct {
	ID, Scope, Target, Reason, CreatedBy, TraceID string
	CreatedAt                                     time.Time
	// AppliedAt 为空表示还没写进索引库的拒绝名单
	AppliedAt *time.Time
}

// Source 是一个登记的数据源（总体设计 4.1 sources）。Type 为 wiki、mail 等；DefaultClassification 为契约 Classification 的编号。
type Source struct {
	ID, Type, OwnerDept, Status, Description, CreatedBy string
	DefaultClassification                               int16
	CreatedAt                                           time.Time
	ApprovedBy, ApprovalBasis                           string
	ApprovedAt                                          *time.Time
	RevokedBy, RevokeReason                             string
	RevokedAt                                           *time.Time
}

// Grant 是一条租户授权。
type Grant struct {
	SourceID, Tenant, GrantedBy string
	GrantedAt                   time.Time
}

type PG struct {
	Pool *pgxpool.Pool
}

// CreateDenial 在一个事务里插入规则并执行 beforeCommit（写变更审计）：审计写不进去就不登记规则。
func (s *PG) CreateDenial(ctx context.Context, d Denial, beforeCommit func() error) (Denial, error) {
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			INSERT INTO governance.emergency_denials (denial_id, scope, target, reason, created_by, trace_id)
			VALUES ($1, $2, $3, $4, $5, $6) RETURNING created_at`,
			d.ID, d.Scope, d.Target, d.Reason, d.CreatedBy, d.TraceID).Scan(&d.CreatedAt)
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" {
			return ErrExists
		}
		if err != nil {
			return err
		}
		return beforeCommit()
	})
	return d, err
}

const denialCols = `denial_id, scope, target, reason, created_by, trace_id, created_at, applied_at`

func scanDenial(r pgx.CollectableRow) (Denial, error) {
	var d Denial
	err := r.Scan(&d.ID, &d.Scope, &d.Target, &d.Reason, &d.CreatedBy, &d.TraceID, &d.CreatedAt, &d.AppliedAt)
	return d, err
}

// DenialByTarget 取同一对象上的规则。
func (s *PG) DenialByTarget(ctx context.Context, scope, target string) (Denial, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+denialCols+` FROM governance.emergency_denials WHERE scope = $1 AND target = $2`, scope, target)
	if err != nil {
		return Denial{}, err
	}
	d, err := pgx.CollectExactlyOneRow(rows, scanDenial)
	if errors.Is(err, pgx.ErrNoRows) {
		return Denial{}, ErrNotFound
	}
	return d, err
}

// ListDenials 按创建时间倒序分页。
func (s *PG) ListDenials(ctx context.Context, offset, limit int) ([]Denial, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+denialCols+` FROM governance.emergency_denials
		ORDER BY created_at DESC, denial_id OFFSET $1 LIMIT $2`, offset, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanDenial)
}

// PendingDenials 是还没写进拒绝名单的规则，按创建时间顺序。
func (s *PG) PendingDenials(ctx context.Context) ([]Denial, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+denialCols+` FROM governance.emergency_denials
		WHERE applied_at IS NULL ORDER BY created_at LIMIT 100`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanDenial)
}

// MarkApplied 记下写进拒绝名单的时间（只记第一次）。
func (s *PG) MarkApplied(ctx context.Context, id string) (time.Time, error) {
	var at time.Time
	err := s.Pool.QueryRow(ctx, `
		UPDATE governance.emergency_denials SET applied_at = COALESCE(applied_at, now()) WHERE denial_id = $1
		RETURNING applied_at`, id).Scan(&at)
	return at, err
}

const sourceCols = `source_id, type, owner_dept, status, description, created_by, default_classification, created_at,
	approved_by, approval_basis, approved_at, revoked_by, revoke_reason, revoked_at`

func scanSource(r pgx.CollectableRow) (Source, error) {
	var x Source
	err := r.Scan(&x.ID, &x.Type, &x.OwnerDept, &x.Status, &x.Description, &x.CreatedBy, &x.DefaultClassification, &x.CreatedAt,
		&x.ApprovedBy, &x.ApprovalBasis, &x.ApprovedAt, &x.RevokedBy, &x.RevokeReason, &x.RevokedAt)
	return x, err
}

// Source 取一个数据源。
func (s *PG) Source(ctx context.Context, id string) (Source, error) {
	return sourceIn(ctx, s.Pool, id, false)
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func sourceIn(ctx context.Context, q querier, id string, lock bool) (Source, error) {
	sql := `SELECT ` + sourceCols + ` FROM governance.sources WHERE source_id = $1`
	if lock {
		sql += ` FOR UPDATE`
	}
	rows, err := q.Query(ctx, sql, id)
	if err != nil {
		return Source{}, err
	}
	x, err := pgx.CollectExactlyOneRow(rows, scanSource)
	if errors.Is(err, pgx.ErrNoRows) {
		return Source{}, ErrNotFound
	}
	return x, err
}

// RegisterSource 登记待审批的数据源，与变更审计同一事务。
func (s *PG) RegisterSource(ctx context.Context, src Source, beforeCommit func() error) (Source, error) {
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			INSERT INTO governance.sources (source_id, type, owner_dept, default_classification, description, status, created_by)
			VALUES ($1, $2, $3, $4, $5, 'pending', $6) RETURNING created_at`,
			src.ID, src.Type, src.OwnerDept, src.DefaultClassification, src.Description, src.CreatedBy).Scan(&src.CreatedAt); err != nil {
			return err
		}
		return beforeCommit()
	})
	src.Status = "pending"
	return src, err
}

// UpdateSource 锁住数据源，交给 change 判断与修改，再写回审批、撤销字段；change 还可以在同一事务里写发件表与变更审计。
func (s *PG) UpdateSource(ctx context.Context, id string, change func(tx pgx.Tx, src *Source) error) (Source, error) {
	var src Source
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		if src, err = sourceIn(ctx, tx, id, true); err != nil {
			return err
		}
		if err := change(tx, &src); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE governance.sources SET status = $2, approved_by = $3, approval_basis = $4, approved_at = $5,
				revoked_by = $6, revoke_reason = $7, revoked_at = $8
			WHERE source_id = $1`,
			id, src.Status, src.ApprovedBy, src.ApprovalBasis, src.ApprovedAt, src.RevokedBy, src.RevokeReason, src.RevokedAt)
		return err
	})
	return src, err
}

// WriteEvent 在事务里写一条平台事件到发件表（governance serve 转发到事件流）。
func WriteEvent(ctx context.Context, tx pgx.Tx, id string, payload []byte) error {
	_, err := tx.Exec(ctx, `INSERT INTO governance.outbox (event_id, payload) VALUES ($1, $2)`, id, payload)
	return err
}

func (s *PG) Grants(ctx context.Context, sourceID string) ([]Grant, error) {
	rows, err := s.Pool.Query(ctx, `SELECT source_id, tenant, granted_by, granted_at FROM governance.source_grants
		WHERE source_id = $1 ORDER BY tenant`, sourceID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Grant, error) {
		var g Grant
		err := r.Scan(&g.SourceID, &g.Tenant, &g.GrantedBy, &g.GrantedAt)
		return g, err
	})
}

// CreateGrant 写一条授权；已有时返回已有的、created 为 false。
func (s *PG) CreateGrant(ctx context.Context, g Grant, beforeCommit func() error) (Grant, bool, error) {
	created := false
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO governance.source_grants (source_id, tenant, granted_by) VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`, g.SourceID, g.Tenant, g.GrantedBy)
		if err != nil {
			return err
		}
		if created = tag.RowsAffected() == 1; created {
			if err := beforeCommit(); err != nil {
				return err
			}
		}
		return tx.QueryRow(ctx, `SELECT granted_by, granted_at FROM governance.source_grants WHERE source_id = $1 AND tenant = $2`,
			g.SourceID, g.Tenant).Scan(&g.GrantedBy, &g.GrantedAt)
	})
	return g, created, err
}

// DeleteGrant 取消授权；没有这条授权时返回 ErrNotFound。
func (s *PG) DeleteGrant(ctx context.Context, sourceID, tenant string, beforeCommit func() error) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM governance.source_grants WHERE source_id = $1 AND tenant = $2`, sourceID, tenant)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return beforeCommit()
	})
}

// ActiveGrants 是检索要用的授权：只含已审批、未撤销的数据源，按租户、数据源排序（快照摘要据此稳定）。
func (s *PG) ActiveGrants(ctx context.Context) ([]Grant, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT g.source_id, g.tenant FROM governance.source_grants g JOIN governance.sources s USING (source_id)
		WHERE s.status = 'active' ORDER BY g.tenant, g.source_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Grant, error) {
		var g Grant
		err := r.Scan(&g.SourceID, &g.Tenant)
		return g, err
	})
}
