package admission

import (
	"math"
	"slices"
	"sync"
	"time"
)

// Priority 是请求的优先级（gateway 详设 5.2）：过载时从低往高拒绝。
// 管理接口优先级最高，不受这层限制；它们接入 gateway 时直接跳过 Admit。
type Priority int

// 从 1 开始：零值不是任何优先级，漏填优先级的请求一律被拒绝，而不是被当成最高优先级放行
const (
	PriorityAnswer Priority = iota + 1 // 同步问答
	PrioritySearch                     // 仅检索
	PriorityCompat                     // 兼容端点与 MCP
	PriorityBatch                      // 批量提交与查询
)

// 每个优先级最多能用到全局上限的多少：在途请求越接近上限，越低的优先级越先被拒绝。
// 与 retrieval 的准入分档同一思路（batch 60%、search/mcp 80%、answer 100%）
var shares = [...]float64{PriorityAnswer: 1, PrioritySearch: 0.9, PriorityCompat: 0.8, PriorityBatch: 0.6}

const (
	// 下游 p95 超过预算的这个比例就下调上限（gateway 详设 5.2）
	target = 0.8
	// 调整间隔与观察窗口：每秒最多调整一次，p95 取最近 10 秒的样本
	adjustEvery = time.Second
	window      = 10 * time.Second
	// 样本太少时 p95 没有意义，只做恢复不做下调
	minSamples = 10
	maxSamples = 2048
	// 恢复时每次上调 5% 再加 1：从下限回到上限需要一两分钟，避免刚恢复就再次压垮下游
	recoverRatio = 1.05
	// 下游直接拒绝（RESOURCE_EXHAUSTED）时立即收紧到在途数的 90%
	exhaustedRatio = 0.9
)

type sample struct {
	at    time.Time
	ratio float64
}

// Adaptive 是全局自适应并发限制（gateway 详设 5.1、5.2）。每个 gateway 实例各自计数：
// 信号是本实例看到的下游延迟，不需要跨实例同步，也不依赖 Redis。
type Adaptive struct {
	max, min float64
	now      func() time.Time

	mu         sync.Mutex
	limit      float64
	inflight   int
	samples    []sample
	lastAdjust time.Time
}

// NewAdaptive 返回一个上限为 maxInflight 的限制，从上限开始，只在下游变慢时下调；下限为 10 与上限中的较小者。
func NewAdaptive(maxInflight int, now func() time.Time) *Adaptive {
	m := float64(max(maxInflight, 1))
	return &Adaptive{max: m, min: math.Min(10, m), limit: m, now: now, lastAdjust: now()}
}

// Admit 按优先级判断能否再放行一个请求；放行时必须调用 done（可以重复调用，只生效一次）。
func (a *Adaptive) Admit(p Priority) (done func(), ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.adjust()
	if float64(a.inflight+1) > math.Floor(a.limit*shares[p]) {
		return nil, false
	}
	a.inflight++
	return once(func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.inflight--
	}), true
}

// Observe 记录一次 orchestrator 调用的耗时与其延迟预算之比（1 表示刚好用完预算）。
func (a *Adaptive) Observe(ratio float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.samples = append(a.samples, sample{a.now(), ratio})
	if len(a.samples) > maxSamples {
		a.samples = a.samples[len(a.samples)-maxSamples:]
	}
	a.adjust()
}

// Exhausted：下游已经拒绝（RESOURCE_EXHAUSTED），不等 p95，立即收紧。
func (a *Adaptive) Exhausted() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.limit = math.Max(a.min, math.Min(a.limit, math.Max(float64(a.inflight), a.min))*exhaustedRatio)
	a.samples, a.lastAdjust = nil, a.now()
}

// Limit 返回当前的全局上限（取整）。
func (a *Adaptive) Limit() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return int(a.limit)
}

// adjust 每秒最多一次：p95 超过预算的 80% 时按比例下调（以在途数为基准，立即生效），否则缓慢上调。
// 调用方持有锁。
func (a *Adaptive) adjust() {
	now := a.now()
	if now.Sub(a.lastAdjust) < adjustEvery {
		return
	}
	a.lastAdjust = now
	i := 0
	for i < len(a.samples) && now.Sub(a.samples[i].at) > window {
		i++
	}
	a.samples = a.samples[i:]
	if len(a.samples) >= minSamples {
		ratios := make([]float64, len(a.samples))
		for j, s := range a.samples {
			ratios[j] = s.ratio
		}
		slices.Sort(ratios)
		p95 := ratios[int(math.Ceil(0.95*float64(len(ratios))))-1]
		if p95 > target {
			base := math.Min(a.limit, math.Max(float64(a.inflight), a.min))
			a.limit = math.Max(a.min, base*target/p95)
			// 下调后重新攒样本：旧样本反映的是下调前的负载，再用它会连续下调
			a.samples = nil
			return
		}
	}
	a.limit = math.Min(a.max, a.limit*recoverRatio+1)
}
