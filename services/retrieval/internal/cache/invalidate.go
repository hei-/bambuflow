package cache

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/events/v1"
)

// Stream 是 indexing 发 ChunkAclChanged 的 Redis 流（indexing 的 writer.ChunkAclStream）。
const Stream = "retrieval:chunk-acl"

// Invalidator 读 ChunkAclChanged，按文档清检索结果缓存（详设 3.5、8.2）。每个副本各自从启动时的最新处读，不用消费组：
// 缓存在 Redis 里是共享的，多个副本重复清同一批键没有害处；启动前错过的事件对应的缓存最多 10 分钟后过期。
type Invalidator struct {
	// Events 是事件流的 Redis，Results 是缓存的 Redis
	Events  *redis.Client
	Results *Results
	Log     *slog.Logger
}

func (x *Invalidator) Run(ctx context.Context) {
	last := "$"
	for ctx.Err() == nil {
		res, err := x.Events.XRead(ctx, &redis.XReadArgs{Streams: []string{Stream, last}, Count: 100, Block: 5 * time.Second}).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) && ctx.Err() == nil {
				x.Log.WarnContext(ctx, "读 ChunkAclChanged 失败，稍后重试", slog.Any("err", err))
				sleep(ctx, time.Second)
			}
			continue
		}
		for _, m := range res[0].Messages {
			last = m.ID
			raw, _ := m.Values["pb"].(string)
			var evt eventsv1.ChunkAclChanged
			if err := proto.Unmarshal([]byte(raw), &evt); err != nil {
				x.Log.ErrorContext(ctx, "丢弃无法解析的 ChunkAclChanged", slog.Any("err", err))
				continue
			}
			// 记成变化发生的时间（不晚于现在）：在它之后开始的请求才可能读到新权限，之前开始的不写缓存
			at := time.Now()
			if o := evt.GetOccurredAt(); o != nil && o.AsTime().Before(at) {
				at = o.AsTime()
			}
			if err := x.Results.Invalidate(ctx, evt.GetDocIds(), at); err != nil {
				// 清不掉的条目最多 10 分钟后过期，组装复核兜底，不重试阻塞后面的事件
				x.Log.WarnContext(ctx, "清检索结果缓存失败", slog.Any("err", err))
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
