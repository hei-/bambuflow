package plugin

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PG struct {
	Pool *pgxpool.Pool
}

const columns = `plugin_id, kind, name, version, host, description, tool_scopes, registered_by, registered_at, security_review,
	reviewed_by, reviewed_at, review_note`

func scan(r pgx.CollectableRow) (Plugin, error) {
	var p Plugin
	err := r.Scan(&p.ID, &p.Kind, &p.Name, &p.Version, &p.Host, &p.Description, &p.ToolScopes, &p.RegisteredBy, &p.RegisteredAt,
		&p.SecurityReview, &p.ReviewedBy, &p.ReviewedAt, &p.ReviewNote)
	return p, err
}

type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// withEvaluations 补上各插件的评估结果。
func withEvaluations(ctx context.Context, q querier, ps []Plugin) ([]Plugin, error) {
	if len(ps) == 0 {
		return ps, nil
	}
	ids := make([]string, len(ps))
	at := map[string]int{}
	for i, p := range ps {
		ids[i], at[p.ID] = p.ID, i
	}
	rows, err := q.Query(ctx, `SELECT plugin_id, tenant, passed, report_ref, evaluated_at FROM governance.plugin_evaluations
		WHERE plugin_id = ANY($1) ORDER BY plugin_id, tenant`, ids)
	if err != nil {
		return nil, err
	}
	type row struct {
		id string
		e  Evaluation
	}
	evals, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		err := r.Scan(&x.id, &x.e.Tenant, &x.e.Passed, &x.e.ReportRef, &x.e.EvaluatedAt)
		return x, err
	})
	if err != nil {
		return nil, err
	}
	for _, x := range evals {
		ps[at[x.id]].Evaluations = append(ps[at[x.id]].Evaluations, x.e)
	}
	return ps, nil
}

func get(ctx context.Context, q querier, id string, lock bool) (Plugin, error) {
	sql := `SELECT ` + columns + ` FROM governance.plugins WHERE plugin_id = $1`
	if lock {
		sql += ` FOR UPDATE`
	}
	rows, err := q.Query(ctx, sql, id)
	if err != nil {
		return Plugin{}, err
	}
	p, err := pgx.CollectExactlyOneRow(rows, scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return Plugin{}, ErrNotFound
	}
	if err != nil {
		return Plugin{}, err
	}
	ps, err := withEvaluations(ctx, q, []Plugin{p})
	if err != nil {
		return Plugin{}, err
	}
	return ps[0], nil
}

func (s *PG) Create(ctx context.Context, p Plugin, beforeCommit func() error) (Plugin, error) {
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO governance.plugins (plugin_id, kind, name, version, host, description, tool_scopes,
			registered_by, registered_at, security_review) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			p.ID, p.Kind, p.Name, p.Version, p.Host, p.Description, p.ToolScopes, p.RegisteredBy, p.RegisteredAt, p.SecurityReview)
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" {
			return ErrDuplicate
		}
		if err != nil {
			return err
		}
		return beforeCommit()
	})
	if err != nil {
		return Plugin{}, err
	}
	return p, nil
}

func (s *PG) Get(ctx context.Context, id string) (Plugin, error) {
	return get(ctx, s.Pool, id, false)
}

func (s *PG) List(ctx context.Context, offset, limit int) ([]Plugin, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+columns+` FROM governance.plugins ORDER BY registered_at DESC, plugin_id DESC
		OFFSET $1 LIMIT $2`, offset, limit)
	if err != nil {
		return nil, err
	}
	ps, err := pgx.CollectRows(rows, scan)
	if err != nil {
		return nil, err
	}
	return withEvaluations(ctx, s.Pool, ps)
}

// Update 锁住插件，交给 change 修改评审字段后写回（change 里写变更审计，同一事务）。
func (s *PG) Update(ctx context.Context, id string, change func(p *Plugin) error) (Plugin, error) {
	var p Plugin
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		if p, err = get(ctx, tx, id, true); err != nil {
			return err
		}
		if err := change(&p); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE governance.plugins SET security_review = $2, reviewed_by = $3, reviewed_at = $4, review_note = $5
			WHERE plugin_id = $1`, p.ID, p.SecurityReview, p.ReviewedBy, p.ReviewedAt, p.ReviewNote)
		return err
	})
	return p, err
}

// RecordEvaluation 写入（覆盖）该租户最近一次评估。
func (s *PG) RecordEvaluation(ctx context.Context, id string, e Evaluation, beforeCommit func() error) (Plugin, error) {
	var p Plugin
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := get(ctx, tx, id, true); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO governance.plugin_evaluations (plugin_id, tenant, passed, report_ref, evaluated_at)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (plugin_id, tenant) DO UPDATE SET passed = EXCLUDED.passed,
			report_ref = EXCLUDED.report_ref, evaluated_at = EXCLUDED.evaluated_at`,
			id, e.Tenant, e.Passed, e.ReportRef, e.EvaluatedAt); err != nil {
			return err
		}
		if err := beforeCommit(); err != nil {
			return err
		}
		var err error
		p, err = get(ctx, tx, id, false)
		return err
	})
	return p, err
}
