package ratelimit

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// 连不上的 Redis：验证退化为本地限流（有 Redis 的路径由 e2e 覆盖）
func TestFallsBackToLocalBucketSplitAcrossInstances(t *testing.T) {
	l := &Limiter{
		Redis:     redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 10 * time.Millisecond, MaxRetries: -1}),
		Instances: 2,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	rule := Rule{Name: "user", Rate: 0.001, Burst: 4}
	ctx := context.Background()
	// 两个实例均分：本实例只有 2 个令牌
	for i := range 2 {
		if !l.Allow(ctx, rule, "alice") {
			t.Fatalf("第 %d 次应放行", i+1)
		}
	}
	if l.Allow(ctx, rule, "alice") {
		t.Fatal("Redis 不可用时不能放开限流")
	}
	if !l.Allow(ctx, rule, "bob") {
		t.Fatal("各用户的桶互不影响")
	}
}
