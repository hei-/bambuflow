// Package idempotency 是管理写操作的幂等键（gateway 详设 4.3）：同一用户的同一个 Idempotency-Key 在 24 小时内
// 只执行一次，重复请求返回第一次的结果；同一个键对应不同请求体时返回冲突。存在 Redis 里，各 gateway 实例共用。
package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// TTL 是幂等键的保留期（gateway 详设 4.3）。
const TTL = 24 * time.Hour

// ErrConflict：同一个键对应了不同的请求体，或者第一次请求还在处理中。
var ErrConflict = errors.New("idempotency_conflict")

// Result 是第一次请求成功时的响应。
type Result struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

type entry struct {
	Digest string  `json:"digest"`
	Done   *Result `json:"done,omitempty"`
}

type Store struct {
	Redis *redis.Client
}

func key(scope, k string) string { return "admin-idem:" + scope + ":" + k }

// Begin 登记一次请求。返回 nil, nil 表示这是第一次，调用方执行后必须调用 Finish 或 Abort；
// 返回非 nil 的 Result 表示已经执行过，直接返回它。Redis 不可用时返回错误，调用方拒绝请求（fail-closed）。
func (s *Store) Begin(ctx context.Context, scope, k, digest string) (*Result, error) {
	b, err := json.Marshal(entry{Digest: digest})
	if err != nil {
		return nil, err
	}
	ok, err := s.Redis.SetNX(ctx, key(scope, k), b, TTL).Result()
	if err != nil {
		return nil, err
	}
	if ok {
		return nil, nil
	}
	raw, err := s.Redis.Get(ctx, key(scope, k)).Bytes()
	if errors.Is(err, redis.Nil) {
		// 第一次请求刚好失败、键已删除：按冲突处理，调用方稍后重试即可
		return nil, ErrConflict
	}
	if err != nil {
		return nil, err
	}
	var e entry
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}
	if e.Digest != digest || e.Done == nil {
		return nil, ErrConflict
	}
	return e.Done, nil
}

// Finish 记下成功的结果，之后同键同内容的请求直接返回它。
func (s *Store) Finish(ctx context.Context, scope, k, digest string, r Result) error {
	b, err := json.Marshal(entry{Digest: digest, Done: &r})
	if err != nil {
		return err
	}
	return s.Redis.Set(ctx, key(scope, k), b, TTL).Err()
}

// Abort 在执行失败时删除登记，调用方可以用同一个键重试。
func (s *Store) Abort(ctx context.Context, scope, k string) error {
	return s.Redis.Del(ctx, key(scope, k)).Err()
}
