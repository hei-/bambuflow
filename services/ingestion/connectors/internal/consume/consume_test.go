package consume

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// 旧 Pod 读了没确认就没了：换了主机名的新实例认领后接着处理并确认
func TestLoopTakesOverMessagesOfGoneConsumer(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.XGroupCreateMkStream(ctx, "s", "g", "0").Err(); err != nil {
		t.Fatal(err)
	}
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s", Values: map[string]any{"k": "v1"}})
	if err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "old-pod", Streams: []string{"s", ">"}}).Err(); err != nil {
		t.Fatal(err)
	}
	mr.SetTime(time.Now().Add(time.Hour))

	var mu sync.Mutex
	var got []string
	l := &Loop{Redis: rdb, Stream: "s", Group: "g", Name: "new-pod", MaxAttempts: 3, Retry: time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Handle: func(_ context.Context, v map[string]any) bool {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, v["k"].(string))
			return true
		}}
	done := make(chan struct{})
	go func() { _ = l.Run(ctx); close(done) }()
	for ctx.Err() == nil {
		if n, err := rdb.XPending(ctx, "s", "g").Result(); err == nil && n.Count == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "v1" {
		t.Fatalf("应处理旧消费者留下的消息并确认: %v", got)
	}
}
