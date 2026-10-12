package config

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PG 是 Store 的生产实现（governance 自有 schema）。
type PG struct {
	Pool *pgxpool.Pool
}

func (s *PG) InTx(ctx context.Context, fn func(Tx) error) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error { return fn(pgTx{tx}) })
}

type pgTx struct{ tx pgx.Tx }

const versionCols = `version_id, object, scope, number, content, content_hash, created_by, created_at, source_ref, analysis`

func scanVersion(r pgx.CollectableRow) (Version, error) {
	var v Version
	err := r.Scan(&v.ID, &v.Object, &v.Scope, &v.Number, &v.Content, &v.ContentHash, &v.CreatedBy, &v.CreatedAt, &v.SourceRef, &v.Analysis)
	return v, err
}

func (t pgTx) Active(ctx context.Context, object string) ([]Version, error) {
	rows, err := t.tx.Query(ctx, `SELECT v.version_id, v.object, v.scope, v.number, v.content, v.content_hash, v.created_by,
		v.created_at, v.source_ref, v.analysis FROM governance.active_pointers p JOIN governance.config_versions v USING (version_id)
		WHERE p.object = $1 ORDER BY p.scope`, object)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanVersion)
}

func (t pgTx) LockPointer(ctx context.Context, object, scope string) (string, error) {
	var id string
	err := t.tx.QueryRow(ctx, `SELECT version_id FROM governance.active_pointers WHERE object = $1 AND scope = $2 FOR UPDATE`,
		object, scope).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return id, err
}

func (t pgTx) SetPointer(ctx context.Context, object, scope, versionID string) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO governance.active_pointers (object, scope, version_id) VALUES ($1, $2, $3)
		ON CONFLICT (object, scope) DO UPDATE SET version_id = EXCLUDED.version_id, updated_at = now()`, object, scope, versionID)
	return err
}

func (t pgTx) NextNumber(ctx context.Context, object, scope string) (int32, error) {
	var n int32
	err := t.tx.QueryRow(ctx, `SELECT coalesce(max(number), 0) + 1 FROM governance.config_versions WHERE object = $1 AND scope = $2`,
		object, scope).Scan(&n)
	return n, err
}

func (t pgTx) InsertVersion(ctx context.Context, v Version) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO governance.config_versions (`+versionCols+`) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		v.ID, v.Object, v.Scope, v.Number, v.Content, v.ContentHash, v.CreatedBy, v.CreatedAt, v.SourceRef, nonNil(v.Analysis))
	return err
}

func (t pgTx) Version(ctx context.Context, id string) (Version, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+versionCols+` FROM governance.config_versions WHERE version_id = $1`, id)
	if err != nil {
		return Version{}, err
	}
	v, err := pgx.CollectExactlyOneRow(rows, scanVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, ErrNotFound
	}
	return v, err
}

func (t pgTx) Versions(ctx context.Context, object, scope string) ([]Version, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+versionCols+` FROM governance.config_versions WHERE object = $1 AND scope = $2
		ORDER BY number DESC`, object, scope)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanVersion)
}

const releaseCols = `release_id, object, scope, version_id, coalesce(from_version_id, ''), source, source_ref, gate_id, status, reason,
	requested_by, requested_at, approved_by, approved_at, rolled_back_by, rolled_back_at, second_approved_by, second_approved_at,
	security_approved, canary_tenant, gate_started_at`

func (t pgTx) InsertRelease(ctx context.Context, r Release) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO governance.releases (release_id, object, scope, version_id, from_version_id, source,
		source_ref, gate_id, status, reason, requested_by, requested_at, approved_by, approved_at, canary_tenant)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		r.ID, r.Object, r.Scope, r.VersionID, r.FromVersionID, r.Source, r.SourceRef, r.GateID, r.Status, r.Reason, r.RequestedBy,
		r.RequestedAt, r.ApprovedBy, r.ApprovedAt, r.CanaryTenant)
	return err
}

func scanRelease(row pgx.CollectableRow) (Release, error) {
	var r Release
	err := row.Scan(&r.ID, &r.Object, &r.Scope, &r.VersionID, &r.FromVersionID, &r.Source, &r.SourceRef, &r.GateID, &r.Status,
		&r.Reason, &r.RequestedBy, &r.RequestedAt, &r.ApprovedBy, &r.ApprovedAt, &r.RolledBackBy, &r.RolledBackAt,
		&r.SecondApprovedBy, &r.SecondApprovedAt, &r.SecurityApproved, &r.CanaryTenant, &r.GateStartedAt)
	return r, err
}

func (t pgTx) Releases(ctx context.Context, status string, limit int) ([]Release, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+releaseCols+` FROM governance.releases WHERE $1 = '' OR status = $1
		ORDER BY requested_at DESC, release_id LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanRelease)
}

func (t pgTx) ReleasesOf(ctx context.Context, versionID string) ([]Release, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+releaseCols+` FROM governance.releases WHERE version_id = $1`, versionID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanRelease)
}

func (t pgTx) LockRelease(ctx context.Context, id string) (Release, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+releaseCols+` FROM governance.releases WHERE release_id = $1 FOR UPDATE`, id)
	if err != nil {
		return Release{}, err
	}
	r, err := pgx.CollectExactlyOneRow(rows, scanRelease)
	if errors.Is(err, pgx.ErrNoRows) {
		return Release{}, ErrNotFound
	}
	return r, err
}

func (t pgTx) UpdateRelease(ctx context.Context, r Release) error {
	_, err := t.tx.Exec(ctx, `UPDATE governance.releases SET status = $2, approved_by = $3, approved_at = $4, rolled_back_by = $5,
		rolled_back_at = $6, second_approved_by = $7, second_approved_at = $8, security_approved = $9, gate_id = $10,
		gate_started_at = $11 WHERE release_id = $1`,
		r.ID, r.Status, r.ApprovedBy, r.ApprovedAt, r.RolledBackBy, r.RolledBackAt, r.SecondApprovedBy, r.SecondApprovedAt,
		r.SecurityApproved, r.GateID, r.GateStartedAt)
	return err
}

func (t pgTx) ExpireBefore(ctx context.Context, before time.Time) ([]string, []string, error) {
	rows, err := t.tx.Query(ctx, `UPDATE governance.releases SET status = 'expired'
		WHERE status IN ('gating', 'pending_approval', 'pending_second_approval') AND requested_at < $1 RETURNING release_id`, before)
	if err != nil {
		return nil, nil, err
	}
	rels, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, nil, err
	}
	rows, err = t.tx.Query(ctx, `UPDATE governance.change_requests SET status = 'expired'
		WHERE status = 'open' AND created_at < $1 RETURNING chg_id`, before)
	if err != nil {
		return nil, nil, err
	}
	chgs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	return rels, chgs, err
}

func (t pgTx) SupersedeActive(ctx context.Context, object, scope string) error {
	_, err := t.tx.Exec(ctx, `UPDATE governance.releases SET status = 'superseded' WHERE object = $1 AND scope = $2 AND status = 'active'`,
		object, scope)
	return err
}

const changeCols = `chg_id, object, scope, coalesce(base_version_id, ''), version_id, reason, gate_id, author, created_at, status,
	decided_by, decided_at, reject_reason, release_id, merge_request_url`

func (t pgTx) InsertChange(ctx context.Context, c Change) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO governance.change_requests (chg_id, object, scope, base_version_id, version_id, reason,
		gate_id, author, created_at, status, merge_request_url) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, $7, $8, $9, $10, $11)`,
		c.ID, c.Object, c.Scope, c.BaseVersionID, c.VersionID, c.Reason, c.GateID, c.Author, c.CreatedAt, c.Status, c.MergeRequestURL)
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		return ErrBusy
	}
	return err
}

func scanChange(row pgx.CollectableRow) (Change, error) {
	var c Change
	err := row.Scan(&c.ID, &c.Object, &c.Scope, &c.BaseVersionID, &c.VersionID, &c.Reason, &c.GateID, &c.Author, &c.CreatedAt,
		&c.Status, &c.DecidedBy, &c.DecidedAt, &c.RejectReason, &c.ReleaseID, &c.MergeRequestURL)
	return c, err
}

func (t pgTx) Changes(ctx context.Context, status string, limit int) ([]Change, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+changeCols+` FROM governance.change_requests WHERE $1 = '' OR status = $1
		ORDER BY created_at DESC, chg_id LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanChange)
}

func (t pgTx) LockChange(ctx context.Context, id string) (Change, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+changeCols+` FROM governance.change_requests WHERE chg_id = $1 FOR UPDATE`, id)
	if err != nil {
		return Change{}, err
	}
	c, err := pgx.CollectExactlyOneRow(rows, scanChange)
	if errors.Is(err, pgx.ErrNoRows) {
		return Change{}, ErrNotFound
	}
	return c, err
}

func (t pgTx) UpdateChange(ctx context.Context, c Change) error {
	_, err := t.tx.Exec(ctx, `UPDATE governance.change_requests SET status = $2, decided_by = $3, decided_at = $4, reject_reason = $5,
		release_id = $6 WHERE chg_id = $1`, c.ID, c.Status, c.DecidedBy, c.DecidedAt, c.RejectReason, c.ReleaseID)
	return err
}

func nonNil(xs []string) []string {
	if xs == nil {
		return []string{}
	}
	return xs
}

const rolloutCols = `object, scope, release_id, version_id, tenant, percent, started_at, observation, observed_at`

func scanRollout(row pgx.CollectableRow) (Rollout, error) {
	var r Rollout
	err := row.Scan(&r.Object, &r.Scope, &r.ReleaseID, &r.VersionID, &r.Tenant, &r.Percent, &r.StartedAt, &r.Observation, &r.ObservedAt)
	return r, err
}

func (t pgTx) LockRollout(ctx context.Context, object, scope string) (Rollout, bool, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+rolloutCols+` FROM governance.rollouts WHERE object = $1 AND scope = $2 FOR UPDATE`,
		object, scope)
	if err != nil {
		return Rollout{}, false, err
	}
	r, err := pgx.CollectExactlyOneRow(rows, scanRollout)
	if errors.Is(err, pgx.ErrNoRows) {
		return Rollout{}, false, nil
	}
	return r, err == nil, err
}

func (t pgTx) InsertRollout(ctx context.Context, r Rollout) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO governance.rollouts (object, scope, release_id, version_id, tenant, percent, started_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		r.Object, r.Scope, r.ReleaseID, r.VersionID, r.Tenant, r.Percent, r.StartedAt)
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		return ErrBusy
	}
	return err
}

func (t pgTx) DeleteRollout(ctx context.Context, object, scope string) error {
	_, err := t.tx.Exec(ctx, `DELETE FROM governance.rollouts WHERE object = $1 AND scope = $2`, object, scope)
	return err
}

func (t pgTx) ObserveRollout(ctx context.Context, object, scope, observation string, at time.Time) error {
	_, err := t.tx.Exec(ctx, `UPDATE governance.rollouts SET observation = $3, observed_at = $4 WHERE object = $1 AND scope = $2`,
		object, scope, observation, at)
	return err
}

func (t pgTx) Rollouts(ctx context.Context) ([]Rollout, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+rolloutCols+` FROM governance.rollouts ORDER BY object, scope`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanRollout)
}
