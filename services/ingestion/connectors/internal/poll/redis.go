package poll

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// WakeKey 是唤醒一个数据源的轮询用的 Redis 列表；Webhook 接收与管理服务的立即同步往里推，轮询进程阻塞读取。
func WakeKey(sourceID string) string { return "bf:connector:" + sourceID + ":wake" }

// Wake 唤醒数据源的轮询。列表只留一项：连续的唤醒合并成一轮。
func Wake(ctx context.Context, rdb redis.Cmdable, sourceID string) error {
	k := WakeKey(sourceID)
	pipe := rdb.TxPipeline()
	pipe.LPush(ctx, k, "1")
	pipe.LTrim(ctx, k, 0, 0)
	// 没有轮询进程消费时不留存：过期后不会在进程启动时触发一轮多余的同步
	pipe.Expire(ctx, k, time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

// RedisWaker 阻塞读取唤醒列表。
type RedisWaker struct {
	Client   *redis.Client
	SourceID string
}

// blockSlice：每次阻塞读的上限。go-redis 的阻塞读不随 ctx 取消返回，分段读才能及时停下
const blockSlice = 5 * time.Second

func (w RedisWaker) Wait(ctx context.Context, d time.Duration) (bool, error) {
	deadline := time.Now().Add(d)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return false, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		slice := min(left, blockSlice)
		_, err := w.Client.BLPop(ctx, slice, WakeKey(w.SourceID)).Result()
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, redis.Nil):
		case ctx.Err() != nil:
			return false, ctx.Err()
		default:
			// Redis 暂时不可用：按没被唤醒处理，退化为固定间隔，不让轮询停下
			t := time.NewTimer(slice)
			select {
			case <-ctx.Done():
				t.Stop()
				return false, ctx.Err()
			case <-t.C:
			}
		}
	}
}
