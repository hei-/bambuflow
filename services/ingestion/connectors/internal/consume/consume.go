// Package consume 是 connectors 管理服务里常驻的事件消费循环（Redis Streams 消费组）：先处理本消费者名下未确认的消息
// （上次退出时没做完的），再读新消息；处理失败的消息不确认，过一会儿从头重读。同一消息在本进程里的重试有上限，
// 超过后停止重试并记错误日志，消息留在未确认列表里，服务重启时再做一轮。启动时与之后每分钟认领空闲超过 10 分钟的
// 挂起消息（别的实例或换了主机名的旧 Pod 没做完的，streams.ClaimIdle）。撤销数据源（internal/revoke）与
// 物理删除后清理对象存储（internal/rawpurge）共用。
package consume

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hei-/bambuflow/libs/platform-go/streams"
)

// claimIdle 大于本循环的重试间隔与单条处理时长：仍在重试的消息每次重读都会重置空闲时长，不会被抢走
const (
	claimIdle  = 10 * time.Minute
	claimEvery = time.Minute
)

// Loop 是一个消费组上的循环。
type Loop struct {
	Redis  *redis.Client
	Stream string
	Group  string
	// 消费者名，取主机名：未确认的消息归属于它，重启后接着处理
	Name string
	// MaxAttempts 是同一消息在本进程里的重试上限，Retry 是两次重试的间隔
	MaxAttempts int
	Retry       time.Duration
	// Handle 处理一条消息，返回是否可以确认
	Handle func(ctx context.Context, values map[string]any) bool
	// What 是日志里对这类处理的称呼，如“撤销数据源”
	What string
	Log  *slog.Logger

	attempts map[string]int
	given    map[string]bool
}

func (l *Loop) Run(ctx context.Context) error {
	err := l.Redis.XGroupCreateMkStream(ctx, l.Stream, l.Group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	l.attempts, l.given = map[string]int{}, map[string]bool{}
	start := "0"
	var nextClaim time.Time
	for ctx.Err() == nil {
		if now := time.Now(); now.After(nextClaim) {
			nextClaim = now.Add(claimEvery)
			if n, err := streams.ClaimIdle(ctx, l.Redis, l.Stream, l.Group, l.Name, claimIdle); err != nil {
				l.Log.WarnContext(ctx, "认领挂起消息失败", slog.String("stream", l.Stream), slog.Any("err", err))
			} else if n > 0 {
				l.Log.InfoContext(ctx, "认领了别的消费者没做完的消息", slog.String("stream", l.Stream), slog.Int("count", n))
				start = "0"
			}
		}
		res, err := l.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: l.Group, Consumer: l.Name, Streams: []string{l.Stream, start}, Count: 20, Block: 5 * time.Second,
		}).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) && ctx.Err() == nil {
				l.Log.WarnContext(ctx, "读取事件流失败", slog.String("stream", l.Stream), slog.Any("err", err))
				sleep(ctx, time.Second)
			}
			continue
		}
		msgs := res[0].Messages
		pending := 0
		for _, m := range msgs {
			if !l.given[m.ID] {
				pending++
			}
		}
		// 未确认的读完了（或剩下的都已放弃重试）：改读新消息
		if start == "0" && pending == 0 {
			start = ">"
			continue
		}
		for _, m := range msgs {
			if l.given[m.ID] {
				continue
			}
			if !l.Handle(ctx, m.Values) {
				l.attempts[m.ID]++
				if l.attempts[m.ID] >= l.MaxAttempts {
					l.given[m.ID] = true
					l.Log.ErrorContext(ctx, l.What+"多次失败，停止重试，需人工处理（重启 connectors 管理服务会再做一轮）",
						slog.String("message_id", m.ID), slog.Int("attempts", l.attempts[m.ID]))
					continue
				}
				start = "0"
				sleep(ctx, l.Retry)
				break
			}
			if err := l.Redis.XAck(ctx, l.Stream, l.Group, m.ID).Err(); err != nil {
				l.Log.WarnContext(ctx, "确认事件失败，稍后重读", slog.Any("err", err))
			}
		}
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
