package webhook

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	eventsv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/events/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/outbox"
)

// Group 是 governance 在平台事件流上的消费组。
const Group = "governance-webhook"

// Enqueuer 为事件登记投递（PG.Enqueue）。
type Enqueuer interface {
	Enqueue(ctx context.Context, evt *eventsv1.PlatformEvent) (int64, error)
}

// Consumer 从平台事件流读取事件并登记投递。先处理本消费者名下未确认的消息（上次退出时没做完的），再读新消息。
type Consumer struct {
	Redis *redis.Client
	Store Enqueuer
	// 消费者名，取主机名：未确认的消息归属于它，重启后接着处理
	Name string
	Log  *slog.Logger
}

func (c *Consumer) Run(ctx context.Context) error {
	err := c.Redis.XGroupCreateMkStream(ctx, outbox.Stream, Group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	// "0" 读本消费者未确认的消息，读空后切到 ">" 读新消息；登记失败时切回 "0" 重读
	start := "0"
	for ctx.Err() == nil {
		res, err := c.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: Group, Consumer: c.Name, Streams: []string{outbox.Stream, start}, Count: 50, Block: 5 * time.Second,
		}).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) && ctx.Err() == nil {
				c.Log.WarnContext(ctx, "读取平台事件流失败", slog.Any("err", err))
				sleep(ctx, time.Second)
			}
			continue
		}
		msgs := res[0].Messages
		if start == "0" && len(msgs) == 0 {
			start = ">"
			continue
		}
		for _, m := range msgs {
			if !c.Process(ctx, m.Values) {
				start = "0"
				sleep(ctx, time.Second)
				break
			}
			if err := c.Redis.XAck(ctx, outbox.Stream, Group, m.ID).Err(); err != nil {
				c.Log.WarnContext(ctx, "确认事件失败，稍后重读", slog.Any("err", err))
			}
		}
	}
	return nil
}

// Process 处理一条消息，返回是否可以确认。格式不对的消息记日志后确认（重读也解析不了）；
// 登记投递失败的不确认，之后重读。
func (c *Consumer) Process(ctx context.Context, values map[string]any) bool {
	evt, err := outbox.Decode(values)
	if err != nil {
		c.Log.ErrorContext(ctx, "丢弃无法解析的平台事件", slog.Any("err", err))
		return true
	}
	n, err := c.Store.Enqueue(ctx, evt)
	if err != nil {
		c.Log.WarnContext(ctx, "登记 Webhook 投递失败", slog.String("event_id", evt.GetEventId()), slog.Any("err", err))
		return false
	}
	if n > 0 {
		c.Log.InfoContext(ctx, "已登记 Webhook 投递", slog.String("event_id", evt.GetEventId()),
			slog.String("type", evt.GetType()), slog.Int64("deliveries", n))
	}
	return true
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
