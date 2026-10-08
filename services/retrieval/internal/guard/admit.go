package guard

import (
	"sync"

	"golang.org/x/time/rate"
)

// Reject 说明请求为什么被拒绝；调用方转成 RESOURCE_EXHAUSTED。
type Reject string

const (
	RateLimited Reject = "rate_limited" // 超出租户令牌桶
	Overloaded  Reject = "overloaded"   // 在途请求过多，按调用目的分级拒绝
)

// Admission 是入口准入（详设 9.2）：按租户令牌桶限流，并按调用目的分三档做过载保护。
// 过载以本副本的在途请求数为信号：batch 在在途数达到上限的 60% 时最先被拒，search、mcp 到 80%，
// answer、agent_round 到上限才拒，保证过载时先牺牲批量（qa/batch 会退避重试），再是仅检索，最后才是问答。
type Admission struct {
	// TenantRate、TenantBurst 是每个租户在本副本上的令牌桶参数（运行参数，来自部署配置）
	TenantRate  rate.Limit
	TenantBurst int
	// MaxInflight 是本副本的在途请求上限
	MaxInflight int

	mu       sync.Mutex
	tenants  map[string]*rate.Limiter
	inflight int
}

// 低两档请求可用的在途份额
const (
	midShare   = 0.8
	batchShare = 0.6
)

// Enter 判断是否放行；放行时返回的 done 必须在请求结束时调用一次。
func (a *Admission) Enter(tenant, purpose string) (done func(), reject Reject) {
	a.mu.Lock()
	defer a.mu.Unlock()
	limit := a.MaxInflight
	switch purpose {
	case "answer", "agent_round":
	case "batch":
		limit = int(float64(a.MaxInflight) * batchShare)
	default:
		limit = int(float64(a.MaxInflight) * midShare)
	}
	if a.inflight >= limit {
		return nil, Overloaded
	}
	if a.tenants == nil {
		a.tenants = map[string]*rate.Limiter{}
	}
	l, ok := a.tenants[tenant]
	if !ok {
		l = rate.NewLimiter(a.TenantRate, a.TenantBurst)
		a.tenants[tenant] = l
	}
	if !l.Allow() {
		return nil, RateLimited
	}
	a.inflight++
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			a.inflight--
			a.mu.Unlock()
		})
	}, ""
}
