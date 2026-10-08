// Package admission 是 gateway 令牌桶之外的三类准入限制（gateway 详设 5.1、5.2、第 6 节）：
//
//   - Slots：并发上限（租户并发、每用户并发 SSE 流），计数放 Redis，各实例共享；
//   - Quota：租户每日 token 配额，请求开始时按预估值预占，结束时按实际用量结算；
//   - Adaptive：全局自适应并发，以 orchestrator 的 p95 延迟为信号，按优先级从低往高拒绝。
//
// 与 internal/ratelimit 相同：Redis 不可用时退化为本实例的本地计数，额度按实例数均分，而不是放开限制；
// 超限立即拒绝，不排队（详设 5.2）。
package admission

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis 调用的超时：准入不能吃掉请求的时间预算，超时就按 Redis 不可用处理
const redisTimeout = 50 * time.Millisecond

// DefaultLease 是一个并发名额的租约：gateway 实例崩溃、来不及释放时，名额最迟在租约到期后自动收回。
// 在线请求都受 deadline 约束（问答最长 10 s，SSE 也在 deadline 到期时结束），租约取它的数倍，
// 正常请求不会在持有期间过期，因此不续约。
const DefaultLease = time.Minute

// 并发名额脚本：先清掉过期的租约，未满时登记一个新租约（成员为租约 ID，分值为到期时刻），成功返回 1。
// 用 Redis 服务器时间，各实例的时钟偏差不影响到期判断。
var acquire = redis.NewScript(`
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local limit, lease = tonumber(ARGV[1]), tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ms)
if redis.call('ZCARD', KEYS[1]) >= limit then return 0 end
redis.call('ZADD', KEYS[1], ms + lease, ARGV[3])
redis.call('PEXPIRE', KEYS[1], lease)
return 1
`)

// Slots 是按键计数的并发上限，例如每个租户的在线并发、每个用户的并发 SSE 流。
type Slots struct {
	Redis *redis.Client
	// Instances 是 gateway 实例数，只用于 Redis 不可用时均分上限
	Instances int
	Lease     time.Duration
	Log       *slog.Logger

	mu    sync.Mutex
	local map[string]int
}

// Acquire 在 name 这一层、key 对应的计数上占一个名额；已满返回 ok = false。
// 成功时必须调用 release 归还名额（可以重复调用，只生效一次）。
func (s *Slots) Acquire(ctx context.Context, name, key string, limit int) (release func(), ok bool) {
	k := "bf:slots:" + name + ":" + key
	id := leaseID()
	rctx, cancel := context.WithTimeout(ctx, redisTimeout)
	defer cancel()
	n, err := acquire.Run(rctx, s.Redis, []string{k}, limit, s.Lease.Milliseconds(), id).Int()
	if err == nil {
		if n != 1 {
			return nil, false
		}
		return once(func() {
			// 请求本身可能已被取消，归还名额不能跟着失败；失败时租约到期后自动收回
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), redisTimeout)
			defer cancel()
			if err := s.Redis.ZRem(rctx, k, id).Err(); err != nil {
				s.Log.WarnContext(ctx, "归还并发名额失败，等租约到期收回", slog.String("slots", name), slog.Any("err", err))
			}
		}), true
	}
	s.Log.WarnContext(ctx, "并发计数不可用，退化为本地计数", slog.String("slots", name), slog.Any("err", err))
	return s.acquireLocal(k, max(limit/max(s.Instances, 1), 1))
}

func (s *Slots) acquireLocal(key string, limit int) (func(), bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.local == nil {
		s.local = map[string]int{}
	}
	if s.local[key] >= limit {
		return nil, false
	}
	s.local[key]++
	return once(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.local[key]--; s.local[key] <= 0 {
			delete(s.local, key)
		}
	}), true
}

func once(f func()) func() {
	var o sync.Once
	return func() { o.Do(f) }
}

func leaseID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
