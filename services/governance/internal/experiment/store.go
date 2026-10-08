package experiment

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

const columns = `exp_id, tenant, hypothesis, primary_metric, baseline_rate, min_detectable_effect, sample_size_per_arm,
	traffic_percent, support_threshold, min_coverage, status, created_by, created_at, stopped_by, stopped_at, stop_reason`

func scan(r pgx.CollectableRow) (Experiment, error) {
	var e Experiment
	err := r.Scan(&e.ID, &e.Tenant, &e.Hypothesis, &e.PrimaryMetric, &e.BaselineRate, &e.MinDetectableEffect, &e.SampleSizePerArm,
		&e.TrafficPercent, &e.SupportThreshold, &e.MinCoverage, &e.Status, &e.CreatedBy, &e.CreatedAt, &e.StoppedBy, &e.StoppedAt,
		&e.StopReason)
	return e, err
}

func one(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, sql string, args ...any) (Experiment, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return Experiment{}, err
	}
	e, err := pgx.CollectExactlyOneRow(rows, scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return Experiment{}, ErrNotFound
	}
	return e, err
}

// Create 插入运行中的实验，与变更审计同一事务；该租户已有运行中的实验时返回 ErrRunning。
func (s *PG) Create(ctx context.Context, e Experiment, beforeCommit func() error) (Experiment, error) {
	var out Experiment
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		out, err = one(ctx, tx, `
			INSERT INTO governance.experiments (exp_id, tenant, hypothesis, primary_metric, baseline_rate, min_detectable_effect,
				sample_size_per_arm, traffic_percent, support_threshold, min_coverage, status, created_by)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'running', $11) RETURNING `+columns,
			e.ID, e.Tenant, e.Hypothesis, e.PrimaryMetric, e.BaselineRate, e.MinDetectableEffect, e.SampleSizePerArm,
			e.TrafficPercent, e.SupportThreshold, e.MinCoverage, e.CreatedBy)
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" {
			return ErrRunning
		}
		if err != nil {
			return err
		}
		return beforeCommit()
	})
	return out, err
}

func (s *PG) Get(ctx context.Context, id string) (Experiment, error) {
	return one(ctx, s.Pool, `SELECT `+columns+` FROM governance.experiments WHERE exp_id = $1`, id)
}

func (s *PG) List(ctx context.Context, offset, limit int) ([]Experiment, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+columns+` FROM governance.experiments ORDER BY created_at DESC, exp_id DESC
		OFFSET $1 LIMIT $2`, offset, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scan)
}

// Running 是快照要下发的实验，按租户排序（快照摘要据此稳定）。
func (s *PG) Running(ctx context.Context) ([]Experiment, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+columns+` FROM governance.experiments WHERE status = 'running' ORDER BY tenant`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scan)
}

// Update 锁住实验，交给 change 判断与修改，再写回流量与停止字段；change 返回 ErrUnchanged 时不写、不审计。
func (s *PG) Update(ctx context.Context, id string, change func(e *Experiment) error) (Experiment, error) {
	var e Experiment
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		if e, err = one(ctx, tx, `SELECT `+columns+` FROM governance.experiments WHERE exp_id = $1 FOR UPDATE`, id); err != nil {
			return err
		}
		if err := change(&e); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE governance.experiments SET traffic_percent = $2, status = $3, stopped_by = $4, stopped_at = $5,
			stop_reason = $6 WHERE exp_id = $1`, id, e.TrafficPercent, e.Status, e.StoppedBy, e.StoppedAt, e.StopReason)
		return err
	})
	return e, err
}
