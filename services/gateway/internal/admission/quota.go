package admission

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// EstimateTokens 是一次问答开始时预占的 token 数：单跳路径的上下文 8k 加最大输出 800（orchestrator 详设 7.1）。
// agent 路径会多出规划与多轮检索的调用，按实际用量结算时补足，预占只用来挡住已经用尽配额的租户。
const EstimateTokens = 8800

// 每日计数保留两天：结算可能跨过零点（计入预占那一天），之后不再需要
const dayTTL = 48 * time.Hour

// 预占脚本：已用量加预占量超过配额就拒绝（返回 0），否则记入预占量（返回 1）。
var reserve = redis.NewScript(`
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local limit, n = tonumber(ARGV[1]), tonumber(ARGV[2])
if used + n > limit then return 0 end
redis.call('INCRBY', KEYS[1], n)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
`)

// Quota 是租户每日 token 配额（gateway 详设 5.1）：按租户与自然日计数，时区由部署配置给出。
// 超配额的判断只在请求开始时做，不会在生成中途截断回答。
type Quota struct {
	Redis *redis.Client
	// Instances 是 gateway 实例数，只用于 Redis 不可用时均分配额
	Instances int
	// Location 决定“当日”从哪个时刻开始
	Location *time.Location
	Log      *slog.Logger
	Now      func() time.Time

	mu sync.Mutex
	// Redis 不可用时的本地计数：每个租户只留当天的
	local map[string]localDay
}

type localDay struct {
	day  string
	used int64
}

// Hold 是一次预占，结束时用 Settle 按实际用量结算。
type Hold struct {
	q                *Quota
	key, tenant, day string
	estimate         int64
	local            bool
	once             sync.Once
}

// Reserve 为 tenant 预占 estimate 个 token；当日剩余配额不足时返回 ok = false。
func (q *Quota) Reserve(ctx context.Context, tenant string, limit, estimate int64) (*Hold, bool) {
	day := q.Now().In(q.Location).Format(time.DateOnly)
	k := "bf:quota:tokens:" + tenant + ":" + day
	rctx, cancel := context.WithTimeout(ctx, redisTimeout)
	defer cancel()
	n, err := reserve.Run(rctx, q.Redis, []string{k}, limit, estimate, int64(dayTTL.Seconds())).Int()
	if err == nil {
		if n != 1 {
			return nil, false
		}
		return &Hold{q: q, key: k, tenant: tenant, day: day, estimate: estimate}, true
	}
	q.Log.WarnContext(ctx, "配额计数不可用，退化为本地计数", slog.String("tenant", tenant), slog.Any("err", err))
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.local == nil {
		q.local = map[string]localDay{}
	}
	cur := q.local[tenant]
	if cur.day != day {
		cur = localDay{day: day}
	}
	if cur.used+estimate > limit/int64(max(q.Instances, 1)) {
		return nil, false
	}
	cur.used += estimate
	q.local[tenant] = cur
	return &Hold{q: q, key: k, tenant: tenant, day: day, estimate: estimate, local: true}, true
}

// Settle 把预占量改成实际用量（多退少补），计入预占的那一天；重复调用只生效一次。
func (h *Hold) Settle(ctx context.Context, actual int64) {
	h.once.Do(func() {
		delta := max(actual, 0) - h.estimate
		if delta == 0 {
			return
		}
		if h.local {
			h.q.mu.Lock()
			defer h.q.mu.Unlock()
			// 结算前已经换了一天：当天的本地计数从零开始，不再补记前一天的
			if cur := h.q.local[h.tenant]; cur.day == h.day {
				cur.used += delta
				h.q.local[h.tenant] = cur
			}
			return
		}
		// 请求本身可能已被取消（客户端断开），已产生的用量照常计入（gateway 详设第 6 节）
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), redisTimeout)
		defer cancel()
		if err := h.q.Redis.IncrBy(rctx, h.key, delta).Err(); err != nil {
			h.q.Log.WarnContext(ctx, "配额结算失败，按预占量计", slog.Int64("delta", delta), slog.Any("err", err))
		}
	})
}
