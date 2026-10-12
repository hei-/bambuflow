// Package streams 是 Redis Streams 消费组的公共做法。
package streams

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// ClaimIdle 把消费组里空闲超过 minIdle 的挂起消息转到 consumer 名下（XAUTOCLAIM），返回转来的条数。
//
// 消费者名取主机名，而 Kubernetes 里 Pod 重建、CronJob 每次运行主机名都会变：旧名下读了但没确认的消息
// 没人再读，会一直挂着（入库管道详设第 11 节要求失败只重试、不丢）。消费者启动时与之后定期认领，接着处理。
// minIdle 要大于正常的处理时长与重试间隔，免得抢走别的实例仍在处理的消息；真抢到了也只是重复处理一次，
// 消费方都按事件幂等。
func ClaimIdle(ctx context.Context, rdb redis.Cmdable, stream, group, consumer string, minIdle time.Duration) (int, error) {
	n, start := 0, "0-0"
	for {
		ids, next, err := rdb.XAutoClaimJustID(ctx, &redis.XAutoClaimArgs{
			Stream: stream, Group: group, Consumer: consumer, MinIdle: minIdle, Start: start, Count: 100,
		}).Result()
		if err != nil {
			return n, err
		}
		n += len(ids)
		if next == "0-0" || next == "" {
			return n, nil
		}
		start = next
	}
}
