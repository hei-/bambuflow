// Package guard 是 retrieval 的熔断、限流与过载保护（详设 9.2）。
package guard

import (
	"slices"
	"sync"
	"time"
)

// 熔断规则（详设 9.2）：10 秒窗口内至少 20 次请求，且错误率超过 50% 或 p95 超过预算 2 倍时打开；
// 30 秒后半开，放行 5 次探测，全部成功才关闭，任何一次失败重新打开。
const (
	window      = 10 * time.Second
	minRequests = 20
	maxErrRate  = 0.5
	openFor     = 30 * time.Second
	probes      = 5
	// 窗口内最多保留的样本数：QPS 很高时只看最近的请求，内存有上限
	maxSamples = 2000
)

type state int

const (
	closed state = iota
	open
	halfOpen
)

type sample struct {
	at      time.Time
	failed  bool
	latency time.Duration
}

type breaker struct {
	state    state
	samples  []sample
	openedAt time.Time
	issued   int // 半开状态已放行的探测数
	passed   int // 半开状态已成功的探测数
}

// Breakers 按召回路维护熔断器。现在每路只有一个后端（Postgres），所以按路名统计就是“召回路 × 后端”；
// 一路有多个后端时，键要带上后端名。
type Breakers struct {
	// Budget 是单路召回的计划预算（详设 9.1 的 150 ms）；p95 超过它的 2 倍算慢
	Budget time.Duration
	Now    func() time.Time

	mu sync.Mutex
	m  map[string]*breaker
}

func NewBreakers(budget time.Duration) *Breakers {
	return &Breakers{Budget: budget, Now: time.Now, m: map[string]*breaker{}}
}

func (bs *Breakers) get(key string) *breaker {
	b, ok := bs.m[key]
	if !ok {
		b = &breaker{}
		bs.m[key] = b
	}
	return b
}

// Allow 报告这一路现在能否调用；打开状态满 30 秒后转为半开，放行最多 5 次探测。
func (bs *Breakers) Allow(key string) bool {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	b := bs.get(key)
	now := bs.Now()
	if b.state == open && now.Sub(b.openedAt) >= openFor {
		b.state, b.issued, b.passed = halfOpen, 0, 0
	}
	switch b.state {
	case closed:
		return true
	case halfOpen:
		if b.issued < probes {
			b.issued++
			return true
		}
	}
	return false
}

// Record 记下一次调用的结果。超过预算 2 倍的调用在半开探测里也算失败。
func (bs *Breakers) Record(key string, failed bool, latency time.Duration) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	b := bs.get(key)
	now := bs.Now()
	switch b.state {
	case halfOpen:
		if failed || latency > 2*bs.Budget {
			b.trip(now)
			return
		}
		if b.passed++; b.passed >= probes {
			b.state, b.samples = closed, nil
		}
	case closed:
		b.samples = append(b.samples, sample{now, failed, latency})
		cut := 0
		for cut < len(b.samples) && (now.Sub(b.samples[cut].at) > window || len(b.samples)-cut > maxSamples) {
			cut++
		}
		b.samples = b.samples[cut:]
		if b.unhealthy(2 * bs.Budget) {
			b.trip(now)
		}
	}
}

// State 返回这一路的熔断状态（closed、open、half_open），用于指标与日志。
func (bs *Breakers) State(key string) string {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return [...]string{"closed", "open", "half_open"}[bs.get(key).state]
}

func (b *breaker) trip(now time.Time) {
	b.state, b.openedAt, b.samples = open, now, nil
}

func (b *breaker) unhealthy(slow time.Duration) bool {
	n := len(b.samples)
	if n < minRequests {
		return false
	}
	failed := 0
	lat := make([]time.Duration, n)
	for i, s := range b.samples {
		if s.failed {
			failed++
		}
		lat[i] = s.latency
	}
	slices.Sort(lat)
	p95 := lat[(n*95+99)/100-1]
	return float64(failed)/float64(n) > maxErrRate || p95 > slow
}
