// Package store 是 qa/batch 在元数据库里的批次与条目（batch schema）。
//
// 条目按“领取 → 执行 → 结束或稍后重试”流转：领取时加租约，实例中途退出的条目在租约过期后被别的实例重新领取；
// 结束时带上领取时的尝试次数，只有当前持有者的结果会被接受。批次在最后一个条目结束的同一事务里收尾，
// 并写入 batch.completed / batch.failed 事件（发件表，libs/platform-go/outbox）。
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	batchv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/batch/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/outbox"
)

// Retention 是批次结束后结果的保留时间（技术设计文档 13.5）。
const Retention = 7 * 24 * time.Hour

var (
	ErrNotFound = errors.New("批次不存在")
	// ErrConflict：同一提交人的同一幂等键对应了不同的请求内容。
	ErrConflict = errors.New("幂等键冲突")
)

// 发件表的转发语句（outbox.Relay）。
const (
	OutboxSelectSQL = `SELECT event_id, payload FROM batch.outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`
	OutboxMarkSQL   = `UPDATE batch.outbox SET sent_at = now() WHERE event_id = ANY($1)`
)

type NewBatch struct {
	ID, Tenant, Submitter, Actor, Delegation string
	IdempotencyKey, Digest                   string
	Mode                                     batchv1.BatchMode
}

// Record 是一个批次及其归属。
type Record struct {
	Batch             *batchv1.Batch
	Tenant, Submitter string
}

// ResultRow 是一个已结束的条目。
type ResultRow struct {
	Seq               int64
	CustomID, Status  string
	Result            []byte
	ErrType, ErrTitle string
}

// Task 是领取到的一个条目。
type Task struct {
	BatchID            string
	Seq                int64
	CustomID, Question string
	Filters            []byte
	// 含本次在内的尝试次数
	Attempts   int
	Delegation string
}

// Outcome 是一个条目的最终结果：Result 与 ErrType 二者有一。
type Outcome struct {
	BatchID           string
	Seq               int64
	Attempts          int
	AnswerID          string
	Result            []byte
	ErrType, ErrTitle string
}

type PG struct {
	Pool *pgxpool.Pool
}

// Create 写入批次与全部条目。带幂等键时，同一提交人已有该键的批次则返回它（内容不同返回 ErrConflict）。
func (s *PG) Create(ctx context.Context, b NewBatch, items []*batchv1.BatchItem) (*batchv1.Batch, error) {
	if b.IdempotencyKey != "" {
		existing, err := s.byIdempotencyKey(ctx, b)
		if err == nil || !errors.Is(err, ErrNotFound) {
			return existing, err
		}
	}
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var key *string
		if b.IdempotencyKey != "" {
			key = &b.IdempotencyKey
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO batch.batches (batch_id, tenant, submitter, actor, mode, delegation, status, idempotency_key, request_digest)
			VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7, $8)`,
			b.ID, b.Tenant, b.Submitter, b.Actor, int32(b.Mode), b.Delegation, key, b.Digest); err != nil {
			return err
		}
		rows := make([][]any, len(items))
		for i, it := range items {
			var filters []byte
			if it.GetFilters() != nil {
				f, err := proto.Marshal(it.GetFilters())
				if err != nil {
					return err
				}
				filters = f
			}
			rows[i] = []any{b.ID, int32(i), it.GetCustomId(), it.GetQuestion(), filters, "queued"}
		}
		_, err := tx.CopyFrom(ctx, pgx.Identifier{"batch", "items"},
			[]string{"batch_id", "seq", "custom_id", "question", "filters", "status"}, pgx.CopyFromRows(rows))
		return err
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "batches_idempotency" {
		// 同一个键的两次提交同时到达，另一次先写入了
		return s.byIdempotencyKey(ctx, b)
	}
	if err != nil {
		return nil, err
	}
	rec, err := s.Get(ctx, b.ID)
	return rec.Batch, err
}

func (s *PG) byIdempotencyKey(ctx context.Context, b NewBatch) (*batchv1.Batch, error) {
	var id, digest string
	err := s.Pool.QueryRow(ctx, `
		SELECT batch_id, request_digest FROM batch.batches WHERE tenant = $1 AND submitter = $2 AND idempotency_key = $3`,
		b.Tenant, b.Submitter, b.IdempotencyKey).Scan(&id, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if digest != b.Digest {
		return nil, ErrConflict
	}
	rec, err := s.Get(ctx, id)
	return rec.Batch, err
}

// Get 返回批次、进度与归属。
func (s *PG) Get(ctx context.Context, id string) (Record, error) {
	var (
		rec                              Record
		status                           string
		mode                             int32
		created                          time.Time
		started, completed, expires      *time.Time
		total, succeeded, failed, cancel int64
	)
	err := s.Pool.QueryRow(ctx, `
		SELECT b.tenant, b.submitter, b.status, b.mode, b.created_at, b.started_at, b.completed_at, b.expires_at,
		       count(i.seq), count(i.seq) FILTER (WHERE i.status = 'succeeded'),
		       count(i.seq) FILTER (WHERE i.status = 'failed'), count(i.seq) FILTER (WHERE i.status = 'cancelled')
		FROM batch.batches b LEFT JOIN batch.items i ON i.batch_id = b.batch_id
		WHERE b.batch_id = $1 AND (b.expires_at IS NULL OR b.expires_at > now())
		GROUP BY b.batch_id`, id).
		Scan(&rec.Tenant, &rec.Submitter, &status, &mode, &created, &started, &completed, &expires,
			&total, &succeeded, &failed, &cancel)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, err
	}
	rec.Batch = &batchv1.Batch{
		BatchId: id, Status: statusOf(status), Mode: batchv1.BatchMode(mode),
		Counts:    &batchv1.BatchCounts{Total: total, Succeeded: succeeded, Failed: failed, Cancelled: cancel},
		CreatedAt: timestamppb.New(created), StartedAt: ts(started), CompletedAt: ts(completed), ExpiresAt: ts(expires),
	}
	return rec, nil
}

func ts(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func statusOf(s string) batchv1.BatchStatus {
	switch s {
	case "queued":
		return batchv1.BatchStatus_BATCH_STATUS_QUEUED
	case "running":
		return batchv1.BatchStatus_BATCH_STATUS_RUNNING
	case "completed":
		return batchv1.BatchStatus_BATCH_STATUS_COMPLETED
	case "failed":
		return batchv1.BatchStatus_BATCH_STATUS_FAILED
	case "cancelled":
		return batchv1.BatchStatus_BATCH_STATUS_CANCELLED
	}
	return batchv1.BatchStatus_BATCH_STATUS_UNSPECIFIED
}

// Cancel 取消还没开始执行的条目；没有执行中的条目时批次随即结束。
func (s *PG) Cancel(ctx context.Context, id string) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE batch.items SET status = 'cancelled', finished_at = now()
			WHERE batch_id = $1 AND status = 'queued'`, id); err != nil {
			return err
		}
		return finalize(ctx, tx, id)
	})
}

// Results 返回 seq 大于 after 的已结束条目，按 seq 升序，最多 limit 条。
func (s *PG) Results(ctx context.Context, id string, after int64, limit int) ([]ResultRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT seq, custom_id, status, result, coalesce(error_type, ''), coalesce(error_title, '')
		FROM batch.items
		WHERE batch_id = $1 AND seq > $2 AND status IN ('succeeded', 'failed', 'cancelled')
		ORDER BY seq LIMIT $3`, id, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ResultRow, error) {
		var x ResultRow
		err := r.Scan(&x.Seq, &x.CustomID, &x.Status, &x.Result, &x.ErrType, &x.ErrTitle)
		return x, err
	})
}

// Claim 领取至多 n 个到期的条目（排队中且到了重试时间，或执行中但租约已过期），加租约 lease。
// 按到期时间、批次提交时间、提交顺序领取，先提交的批次先执行。
func (s *PG) Claim(ctx context.Context, n int, lease time.Duration) ([]Task, error) {
	var out []Task
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE batch.items i
			SET status = 'running', attempts = i.attempts + 1, lease_until = now() + make_interval(secs => $2)
			FROM batch.batches b
			WHERE b.batch_id = i.batch_id AND (i.batch_id, i.seq) IN (
				SELECT c.batch_id, c.seq FROM batch.items c JOIN batch.batches cb ON cb.batch_id = c.batch_id
				WHERE cb.status IN ('queued', 'running')
				  AND ((c.status = 'queued' AND c.next_attempt_at <= now()) OR (c.status = 'running' AND c.lease_until < now()))
				ORDER BY c.next_attempt_at, cb.created_at, c.seq
				LIMIT $1
				FOR UPDATE OF c SKIP LOCKED)
			RETURNING i.batch_id, i.seq, i.custom_id, i.question, i.filters, i.attempts, b.delegation`,
			n, lease.Seconds())
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Task, error) {
			var t Task
			err := r.Scan(&t.BatchID, &t.Seq, &t.CustomID, &t.Question, &t.Filters, &t.Attempts, &t.Delegation)
			return t, err
		})
		if err != nil || len(out) == 0 {
			return err
		}
		ids := make([]string, len(out))
		for i, t := range out {
			ids[i] = t.BatchID
		}
		_, err = tx.Exec(ctx, `
			UPDATE batch.batches SET status = 'running', started_at = coalesce(started_at, now())
			WHERE batch_id = ANY($1) AND status = 'queued'`, ids)
		return err
	})
	return out, err
}

// Finish 记下条目的最终结果。条目已被别的实例重新领取（尝试次数对不上）时不覆盖，返回 false。
func (s *PG) Finish(ctx context.Context, o Outcome) (bool, error) {
	status := "succeeded"
	var errType, errTitle *string
	if o.ErrType != "" {
		status, errType, errTitle = "failed", &o.ErrType, &o.ErrTitle
	}
	var answerID *string
	if o.AnswerID != "" {
		answerID = &o.AnswerID
	}
	owned := false
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE batch.items SET status = $4, answer_id = $5, result = $6, error_type = $7, error_title = $8,
			       finished_at = now(), lease_until = NULL
			WHERE batch_id = $1 AND seq = $2 AND attempts = $3 AND status = 'running'`,
			o.BatchID, o.Seq, o.Attempts, status, answerID, o.Result, errType, errTitle)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		owned = true
		return finalize(ctx, tx, o.BatchID)
	})
	return owned, err
}

// Retry 把条目放回队列，at 之后再领取。
func (s *PG) Retry(ctx context.Context, t Task, at time.Time) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE batch.items SET status = 'queued', next_attempt_at = $4, lease_until = NULL
		WHERE batch_id = $1 AND seq = $2 AND attempts = $3 AND status = 'running'`, t.BatchID, t.Seq, t.Attempts, at)
	return err
}

// FailRemaining 让批次里还在排队的条目全部失败（提交人已无权执行时，gateway 详设 3.3）。
func (s *PG) FailRemaining(ctx context.Context, id, errType, errTitle string) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE batch.items SET status = 'failed', error_type = $2, error_title = $3, finished_at = now()
			WHERE batch_id = $1 AND status = 'queued'`, id, errType, errTitle); err != nil {
			return err
		}
		return finalize(ctx, tx, id)
	})
}

// finalize 在没有排队或执行中的条目时结束批次：有被取消的条目为 cancelled，否则至少一条成功为 completed、
// 一条都没成功为 failed；同一事务写入结束事件（取消是调用方自己发起的，不发事件），清空委托。
func finalize(ctx context.Context, tx pgx.Tx, id string) error {
	var (
		tenant, status               string
		active, succeeded, cancelled int64
	)
	// 先锁批次行：同一批次的多个条目同时结束时串行收尾，只收尾一次
	if err := tx.QueryRow(ctx, `SELECT tenant, status FROM batch.batches WHERE batch_id = $1 FOR UPDATE`, id).
		Scan(&tenant, &status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if status != "queued" && status != "running" {
		return nil
	}
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE status IN ('queued', 'running')), count(*) FILTER (WHERE status = 'succeeded'),
		       count(*) FILTER (WHERE status = 'cancelled')
		FROM batch.items WHERE batch_id = $1`, id).Scan(&active, &succeeded, &cancelled); err != nil {
		return err
	}
	if active > 0 {
		return nil
	}
	final := FinalStatus(succeeded, cancelled)
	if _, err := tx.Exec(ctx, `
		UPDATE batch.batches SET status = $2, completed_at = now(), expires_at = now() + make_interval(secs => $3), delegation = ''
		WHERE batch_id = $1`, id, final, Retention.Seconds()); err != nil {
		return err
	}
	typ := EventType(final)
	if typ == "" {
		return nil
	}
	evt, pb, err := outbox.New(typ, tenant, id, time.Now())
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO batch.outbox (event_id, payload) VALUES ($1, $2)`, evt.GetEventId(), pb); err != nil {
		return fmt.Errorf("写发件表: %w", err)
	}
	return nil
}

// FinalStatus 是全部条目结束后批次的状态。
func FinalStatus(succeeded, cancelled int64) string {
	switch {
	case cancelled > 0:
		return "cancelled"
	case succeeded > 0:
		return "completed"
	default:
		return "failed"
	}
}

// EventType 是批次结束时发出的事件；被取消的批次不发。
func EventType(final string) string {
	switch final {
	case "completed":
		return outbox.BatchCompleted
	case "failed":
		return outbox.BatchFailed
	}
	return ""
}

// DeleteExpired 删除结果已过保留期的批次（条目随之删除），返回删除的批次数。
func (s *PG) DeleteExpired(ctx context.Context) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM batch.batches WHERE expires_at < now()`)
	return tag.RowsAffected(), err
}
