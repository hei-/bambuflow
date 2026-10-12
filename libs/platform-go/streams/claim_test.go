package streams

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestClaimIdleTakesOverDeadConsumer(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	if err := rdb.XGroupCreateMkStream(ctx, "s", "g", "0").Err(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s", Values: map[string]any{"k": "v"}})
	}
	// 旧 Pod 读了没确认就没了
	if err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "old-pod", Streams: []string{"s", ">"}, Count: 3}).Err(); err != nil {
		t.Fatal(err)
	}
	// 还没空闲够：不抢
	if n, err := ClaimIdle(ctx, rdb, "s", "g", "new-pod", time.Hour); err != nil || n != 0 {
		t.Fatalf("不应认领仍在处理的消息: %d %v", n, err)
	}
	mr.SetTime(time.Now().Add(2 * time.Hour))
	if n, err := ClaimIdle(ctx, rdb, "s", "g", "new-pod", time.Hour); err != nil || n != 3 {
		t.Fatalf("应认领 3 条: %d %v", n, err)
	}
	res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "new-pod", Streams: []string{"s", "0"}}).Result()
	if err != nil || len(res[0].Messages) != 3 {
		t.Fatalf("新消费者名下应有 3 条挂起消息: %v %v", res, err)
	}
}
