package guard

import (
	"testing"
	"time"

	"golang.org/x/time/rate"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newBreakers() (*Breakers, *clock) {
	c := &clock{time.Unix(0, 0)}
	b := NewBreakers(150 * time.Millisecond)
	b.Now = c.now
	return b, c
}

func TestBreakerOpensOnErrorsAndClosesAfterProbes(t *testing.T) {
	b, c := newBreakers()
	// 不足 20 次请求时，即使全部失败也不打开
	for range 19 {
		b.Record("dense", true, 10*time.Millisecond)
	}
	if !b.Allow("dense") {
		t.Fatal("请求数不足时不应打开")
	}
	b.Record("dense", true, 10*time.Millisecond)
	if b.Allow("dense") || b.State("dense") != "open" {
		t.Fatal("错误率超过 50% 应打开")
	}
	if !b.Allow("keyword") {
		t.Fatal("一路熔断不影响另一路")
	}
	c.t = c.t.Add(29 * time.Second)
	if b.Allow("dense") {
		t.Fatal("30 秒内保持打开")
	}
	c.t = c.t.Add(time.Second)
	for i := range probes {
		if !b.Allow("dense") {
			t.Fatalf("半开应放行第 %d 次探测", i+1)
		}
	}
	if b.Allow("dense") {
		t.Fatal("半开只放行 5 次探测")
	}
	for range probes {
		b.Record("dense", false, 10*time.Millisecond)
	}
	if b.State("dense") != "closed" || !b.Allow("dense") {
		t.Fatal("探测全部成功应关闭")
	}
}

func TestBreakerReopensOnFailedProbe(t *testing.T) {
	b, c := newBreakers()
	for range 20 {
		b.Record("dense", true, 0)
	}
	c.t = c.t.Add(openFor)
	b.Allow("dense")
	b.Record("dense", false, 0)
	b.Allow("dense")
	// 探测超过预算 2 倍也算失败
	b.Record("dense", false, 400*time.Millisecond)
	if b.State("dense") != "open" || b.Allow("dense") {
		t.Fatal("探测失败应重新打开")
	}
}

func TestBreakerOpensOnSlowP95AndForgetsOldSamples(t *testing.T) {
	b, c := newBreakers()
	for range 20 {
		b.Record("keyword", true, 0)
		c.t = c.t.Add(time.Second)
	}
	// 每秒一次失败：窗口里始终只有 10 个样本，不足 20 次，不打开
	if b.State("keyword") != "closed" {
		t.Fatal("窗口外的样本应被遗忘")
	}
	for range 20 {
		b.Record("keyword", false, 301*time.Millisecond)
	}
	if b.State("keyword") != "open" {
		t.Fatal("p95 超过预算 2 倍应打开")
	}
}

func TestAdmissionRateLimitsPerTenant(t *testing.T) {
	a := &Admission{TenantRate: rate.Limit(0.001), TenantBurst: 2, MaxInflight: 100}
	for range 2 {
		done, r := a.Enter("credit", "answer")
		if r != "" {
			t.Fatal("突发额度内应放行")
		}
		done()
	}
	if _, r := a.Enter("credit", "answer"); r != RateLimited {
		t.Fatalf("超出令牌桶应限流: %q", r)
	}
	if _, r := a.Enter("ops", "answer"); r != "" {
		t.Fatal("各租户的令牌桶互不影响")
	}
}

func TestAdmissionShedsLowPriorityFirst(t *testing.T) {
	a := &Admission{TenantRate: rate.Inf, MaxInflight: 10}
	var dones []func()
	for range 8 {
		done, r := a.Enter("credit", "search")
		if r != "" {
			t.Fatal("80% 以内应放行")
		}
		dones = append(dones, done)
	}
	if _, r := a.Enter("credit", "search"); r != Overloaded {
		t.Fatal("在途达到 80% 时应先拒绝仅检索")
	}
	for range 2 {
		done, r := a.Enter("credit", "answer")
		if r != "" {
			t.Fatal("问答在上限内应放行")
		}
		dones = append(dones, done)
	}
	if _, r := a.Enter("credit", "answer"); r != Overloaded {
		t.Fatal("到上限后问答也拒绝")
	}
	dones[0]()
	dones[0]() // 重复调用只释放一次
	if _, r := a.Enter("credit", "answer"); r != "" {
		t.Fatal("释放后应放行")
	}
	if _, r := a.Enter("credit", "answer"); r != Overloaded {
		t.Fatal("done 重复调用不应多释放")
	}
}

// 三档优先级：batch 在 60% 处最先被拒，此时仅检索与问答仍放行（详设 9.2）。
func TestAdmissionShedsBatchBeforeSearch(t *testing.T) {
	a := &Admission{TenantRate: rate.Inf, MaxInflight: 10}
	for range 6 {
		if _, r := a.Enter("credit", "batch"); r != "" {
			t.Fatal("60% 以内应放行批量")
		}
	}
	if _, r := a.Enter("credit", "batch"); r != Overloaded {
		t.Fatal("在途达到 60% 时应先拒绝批量")
	}
	if _, r := a.Enter("credit", "mcp"); r != "" {
		t.Fatal("批量被拒时 MCP 仍应放行")
	}
	if _, r := a.Enter("credit", "agent_round"); r != "" {
		t.Fatal("批量被拒时问答仍应放行")
	}
}
