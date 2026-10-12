package poll

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var quiet = slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

func TestPacer(t *testing.T) {
	p := DefaultPacer
	m := time.Minute
	cases := []struct {
		cur, took time.Duration
		err       error
		want      time.Duration
	}{
		{5 * m, 10 * time.Second, nil, 5 * m},
		{5 * m, 10 * time.Second, fmt.Errorf("拉清单: %w", ErrThrottled), 10 * m},
		{10 * m, 10 * time.Second, ErrThrottled, 15 * m},
		{15 * m, 10 * time.Second, ErrThrottled, 15 * m},
		// 耗时过长按限流处理
		{5 * m, 3 * m, nil, 10 * m},
		// 恢复后逐步缩回
		{15 * m, 10 * time.Second, nil, 14 * m},
		// 其他失败不改间隔
		{12 * m, 10 * time.Second, errors.New("连不上"), 12 * m},
	}
	for _, c := range cases {
		if got := p.Next(c.cur, c.took, c.err); got != c.want {
			t.Errorf("Next(%v, %v, %v) = %v，应为 %v", c.cur, c.took, c.err, got, c.want)
		}
	}
}

type recordStatus struct {
	mu        sync.Mutex
	intervals []time.Duration
}

func (r *recordStatus) SetPolling(_ context.Context, d time.Duration, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.intervals = append(r.intervals, d)
	return nil
}

func TestLoopWakesEarlyAndAdapts(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rounds := make(chan struct{}, 10)
	n := 0
	st := &recordStatus{}
	l := &Loop{
		// 间隔远长于测试时长：第二轮只能靠唤醒
		Pacer: Pacer{Min: time.Hour, Max: 3 * time.Hour, Slow: time.Hour, Step: time.Minute},
		Waker: RedisWaker{Client: rdb, SourceID: "src_wiki"}, Status: st, Log: quiet, Now: time.Now, Gap: time.Second,
		Round: func(context.Context) error {
			n++
			rounds <- struct{}{}
			if n == 1 {
				return ErrThrottled
			}
			return nil
		},
	}
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	<-rounds
	// 第一轮限流：间隔翻倍。唤醒在最短间隔之内到达，等它过后才开始下一轮
	if err := Wake(ctx, rdb, "src_wiki"); err != nil {
		t.Fatal(err)
	}
	if err := Wake(ctx, rdb, "src_wiki"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-rounds:
	case <-time.After(l.Gap + 5*time.Second):
		t.Fatal("唤醒后应开始下一轮")
	}
	// 两次唤醒合并成一轮
	select {
	case <-rounds:
		t.Fatal("连续的唤醒应合并")
	case <-time.After(l.Gap + 2*time.Second):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.intervals) < 2 || st.intervals[0] != 2*time.Hour || st.intervals[1] != 2*time.Hour-time.Minute {
		t.Fatalf("间隔: %v", st.intervals)
	}
}
