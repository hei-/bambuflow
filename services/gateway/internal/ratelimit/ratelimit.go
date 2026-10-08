// Package ratelimit 是 gateway 的令牌桶限流（gateway 详设 5.1）。
//
// 计数放在 Redis，各 gateway 实例共享同一个桶；Redis 不可用时退化为本实例的本地令牌桶，
// 速率按实例数均分，而不是放开限流。超限立即拒绝，不排队（详设 5.2）。
package ratelimit

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

// Rule 是一层限流：Rate 为每秒补充的令牌数，Burst 为桶容量。
type Rule struct {
	Name  string
	Rate  float64
	Burst int
}

// 令牌桶脚本：按 Redis 服务器时间补充令牌，取一个令牌成功返回 1。用服务器时间，各实例的时钟偏差不影响计数。
var bucket = redis.NewScript(`
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local rate, burst = tonumber(ARGV[1]), tonumber(ARGV[2])
local v = redis.call('HMGET', KEYS[1], 't', 'ts')
local t, ts = tonumber(v[1]) or burst, tonumber(v[2]) or ms
t = math.min(burst, t + (ms - ts) * rate / 1000)
local ok = 0
if t >= 1 then t = t - 1; ok = 1 end
redis.call('HSET', KEYS[1], 't', t, 'ts', ms)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return ok
`)

// Redis 调用的超时：限流不能吃掉请求的时间预算，超时就按 Redis 不可用处理
const redisTimeout = 50 * time.Millisecond

type Limiter struct {
	Redis *redis.Client
	// Instances 是 gateway 实例数，只用于 Redis 不可用时均分速率
	Instances int
	Log       *slog.Logger

	mu    sync.Mutex
	local map[string]*rate.Limiter
}

// Allow 从 rule 这一层、key 对应的桶里取一个令牌，取到返回 true。
func (l *Limiter) Allow(ctx context.Context, rule Rule, key string) bool {
	k := "bf:ratelimit:" + rule.Name + ":" + key
	rctx, cancel := context.WithTimeout(ctx, redisTimeout)
	defer cancel()
	ok, err := bucket.Run(rctx, l.Redis, []string{k}, rule.Rate, rule.Burst).Int()
	if err == nil {
		return ok == 1
	}
	l.Log.WarnContext(ctx, "限流计数不可用，退化为本地限流", slog.String("rule", rule.Name), slog.Any("err", err))
	return l.localLimiter(rule, k).Allow()
}

func (l *Limiter) localLimiter(rule Rule, key string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.local == nil {
		l.local = map[string]*rate.Limiter{}
	}
	lim, ok := l.local[key]
	if !ok {
		n := max(l.Instances, 1)
		lim = rate.NewLimiter(rate.Limit(rule.Rate/float64(n)), max(rule.Burst/n, 1))
		l.local[key] = lim
	}
	return lim
}
