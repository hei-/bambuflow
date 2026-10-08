package idempotency

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestBeginFinishReplayConflict(t *testing.T) {
	mr := miniredis.RunT(t)
	s := &Store{Redis: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()
	if r, err := s.Begin(ctx, "alice", "k1", "d1"); r != nil || err != nil {
		t.Fatalf("第一次应执行: %v %v", r, err)
	}
	// 第一次还没结束时同键再来：冲突
	if _, err := s.Begin(ctx, "alice", "k1", "d1"); !errors.Is(err, ErrConflict) {
		t.Fatalf("处理中的键应冲突: %v", err)
	}
	if err := s.Finish(ctx, "alice", "k1", "d1", Result{Status: 201, Body: []byte(`{"id":"x"}`)}); err != nil {
		t.Fatal(err)
	}
	r, err := s.Begin(ctx, "alice", "k1", "d1")
	if err != nil || r == nil || r.Status != 201 || string(r.Body) != `{"id":"x"}` {
		t.Fatalf("应返回第一次的结果: %v %v", r, err)
	}
	if _, err := s.Begin(ctx, "alice", "k1", "d2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("同键不同内容应冲突: %v", err)
	}
	// 键按用户隔离
	if r, err := s.Begin(ctx, "bob", "k1", "d2"); r != nil || err != nil {
		t.Fatalf("别的用户的同名键互不影响: %v %v", r, err)
	}
	// 失败后删除登记，可以重试
	if err := s.Abort(ctx, "bob", "k1"); err != nil {
		t.Fatal(err)
	}
	if r, err := s.Begin(ctx, "bob", "k1", "d3"); r != nil || err != nil {
		t.Fatalf("失败后应能重试: %v %v", r, err)
	}
	mr.Close()
	if _, err := s.Begin(ctx, "carl", "k1", "d1"); err == nil {
		t.Fatal("Redis 不可用应返回错误")
	}
}
