// Package outbox 是平台事件（contracts/bambuflow/events/v1）的发件表与转发（技术设计文档 13.7、ADR-12）。
//
// 产生方把事件和业务变更写在同一个事务里（自己 schema 下的 outbox 表），提交后由 Relay 转发到 Redis 事件流，
// 转发成功才标为已发送：业务变更提交了，事件就不会丢；转发中途失败会重发，消费方按 event_id 去重。
// qa/batch（batch.*）、indexing（index.version.*）与 governance（source.*）是产生方，governance 消费后投递 Webhook。
//
// 每个产生方的发件表结构相同：
//
//	event_id text PRIMARY KEY, payload bytea NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), sent_at timestamptz
package outbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/events/v1"
)

const (
	// Stream 是平台事件流的键。
	Stream = "platform:events"
	// streamMaxLen 是事件流保留的大致条数：消费方掉线太久时最旧的事件被裁掉，
	// 只影响 Webhook 通知（接收方仍可轮询状态接口），不影响业务数据。
	streamMaxLen = 100_000
	// batchSize 是每轮转发的最多条数。
	batchSize = 100
)

// 事件名（对象.动作，技术设计文档 13.11）。
const (
	BatchCompleted   = "batch.completed"
	BatchFailed      = "batch.failed"
	IndexVersionLive = "index.version.live"
	// SourceRevoked：数据源被撤销，connectors 据此对该源全部文档写删除标记（governance 详设 6.2）
	SourceRevoked = "source.revoked"
)

// New 生成一条事件（event_id 前缀 evt_）并序列化，payload 原样写进发件表。
func New(typ, tenant, objectID string, at time.Time) (*eventsv1.PlatformEvent, []byte, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return nil, nil, err
	}
	evt := &eventsv1.PlatformEvent{
		EventId: "evt_" + hex.EncodeToString(b), Type: typ, Tenant: tenant, ObjectId: objectID,
		OccurredAt: timestamppb.New(at),
	}
	pb, err := proto.Marshal(evt)
	return evt, pb, err
}

// Decode 解析事件流里的一条消息（Relay 写入的格式）。
func Decode(values map[string]any) (*eventsv1.PlatformEvent, error) {
	raw, ok := values["pb"].(string)
	if !ok {
		return nil, errors.New("事件缺少 pb 字段")
	}
	evt := &eventsv1.PlatformEvent{}
	if err := proto.Unmarshal([]byte(raw), evt); err != nil {
		return nil, fmt.Errorf("解析事件: %w", err)
	}
	if evt.GetEventId() == "" || evt.GetType() == "" {
		return nil, errors.New("事件缺少 event_id 或 type")
	}
	return evt, nil
}

// Relay 把发件表里未发送的事件转发到事件流。
type Relay struct {
	Pool  *pgxpool.Pool
	Redis redis.Cmdable
	// SelectSQL 锁住至多 $1 条未发送的事件，返回 event_id、payload，如
	// SELECT event_id, payload FROM idx.outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED
	SelectSQL string
	// MarkSQL 把 $1（event_id 数组）标为已发送，如 UPDATE idx.outbox SET sent_at = now() WHERE event_id = ANY($1)
	MarkSQL string
	Log     *slog.Logger
}

// Run 每隔 every 转发一轮，直到 ctx 结束。出错只记日志，下一轮重试（事件留在发件表里，不会丢）。
func (r *Relay) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		for {
			n, err := r.Once(ctx)
			if err != nil {
				if ctx.Err() == nil {
					r.Log.WarnContext(ctx, "转发平台事件失败，稍后重试", slog.Any("err", err))
				}
				break
			}
			// 一轮取满说明还有积压，接着转发
			if n < batchSize {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Once 转发一轮，返回转发的条数。
func (r *Relay) Once(ctx context.Context) (int, error) {
	n := 0
	err := pgx.BeginFunc(ctx, r.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, r.SelectSQL, batchSize)
		if err != nil {
			return err
		}
		type row struct {
			id string
			pb []byte
		}
		pending, err := pgx.CollectRows(rows, func(rw pgx.CollectableRow) (row, error) {
			var x row
			err := rw.Scan(&x.id, &x.pb)
			return x, err
		})
		if err != nil || len(pending) == 0 {
			return err
		}
		ids := make([]string, 0, len(pending))
		for _, p := range pending {
			err := r.Redis.XAdd(ctx, &redis.XAddArgs{
				Stream: Stream, MaxLen: streamMaxLen, Approx: true,
				Values: map[string]any{"event_id": p.id, "pb": p.pb},
			}).Err()
			if err != nil {
				return fmt.Errorf("写事件流: %w", err)
			}
			ids = append(ids, p.id)
		}
		if _, err := tx.Exec(ctx, r.MarkSQL, ids); err != nil {
			return err
		}
		n = len(ids)
		return nil
	})
	return n, err
}
