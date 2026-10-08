package admission

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

// 连不上的 Redis：验证退化为本地计数
func downRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 10 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestSlotsSharedInRedisWithLease(t *testing.T) {
	mr, rdb := newRedis(t)
	mr.SetTime(time.Now())
	ctx := context.Background()
	// 两个实例共用同一个 Redis：上限按全局计
	a := &Slots{Redis: rdb, Instances: 2, Lease: DefaultLease, Log: quiet}
	b := &Slots{Redis: rdb, Instances: 2, Lease: DefaultLease, Log: quiet}
	r1, ok1 := a.Acquire(ctx, "tenant", "credit", 2)
	_, ok2 := b.Acquire(ctx, "tenant", "credit", 2)
	if !ok1 || !ok2 {
		t.Fatal("前两个名额应放行")
	}
	if _, ok := a.Acquire(ctx, "tenant", "credit", 2); ok {
		t.Fatal("超过上限应拒绝")
	}
	if _, ok := a.Acquire(ctx, "tenant", "risk", 2); !ok {
		t.Fatal("各租户互不影响")
	}
	r1()
	r1() // 重复归还只生效一次
	if _, ok := b.Acquire(ctx, "tenant", "credit", 2); !ok {
		t.Fatal("归还后应可再占")
	}
	if _, ok := b.Acquire(ctx, "tenant", "credit", 2); ok {
		t.Fatal("重复归还不能多放出名额")
	}
	// 持有名额的实例崩溃、没有归还：租约到期后名额自动收回
	mr.SetTime(time.Now().Add(DefaultLease + time.Second))
	if _, ok := a.Acquire(ctx, "tenant", "credit", 2); !ok {
		t.Fatal("租约到期后名额应收回")
	}
}

func TestSlotsFallBackToLocalSplitAcrossInstances(t *testing.T) {
	s := &Slots{Redis: downRedis(t), Instances: 2, Lease: DefaultLease, Log: quiet}
	ctx := context.Background()
	var held []func()
	for i := range 2 {
		r, ok := s.Acquire(ctx, "tenant", "credit", 4)
		if !ok {
			t.Fatalf("第 %d 个应放行", i+1)
		}
		held = append(held, r)
	}
	if _, ok := s.Acquire(ctx, "tenant", "credit", 4); ok {
		t.Fatal("Redis 不可用时按实例数均分，不能放开限制")
	}
	held[0]()
	held[0]()
	if _, ok := s.Acquire(ctx, "tenant", "credit", 4); !ok {
		t.Fatal("归还后应可再占")
	}
	if _, ok := s.Acquire(ctx, "tenant", "credit", 4); ok {
		t.Fatal("重复归还不能多放出名额")
	}
	// 上限小于实例数时每个实例至少留一个名额，不至于完全不可用
	if _, ok := s.Acquire(ctx, "user-streams", "alice", 1); !ok {
		t.Fatal("每个实例至少一个名额")
	}
}

func TestQuotaReserveSettleInRedis(t *testing.T) {
	mr, rdb := newRedis(t)
	shanghai, _ := time.LoadLocation("Asia/Shanghai")
	now := time.Date(2026, 10, 6, 17, 0, 0, 0, time.UTC) // 上海已是 10 月 7 日凌晨
	q := &Quota{Redis: rdb, Instances: 2, Location: shanghai, Log: quiet, Now: func() time.Time { return now }}
	ctx := context.Background()
	key := "bf:quota:tokens:credit:2026-10-07"

	h1, ok := q.Reserve(ctx, "credit", 20000, 8800)
	if !ok {
		t.Fatal("应能预占")
	}
	if _, ok := q.Reserve(ctx, "credit", 20000, 8800); !ok {
		t.Fatal("应能预占")
	}
	if _, ok := q.Reserve(ctx, "credit", 20000, 8800); ok {
		t.Fatal("剩余 2400 不够预占，应拒绝")
	}
	if got, _ := mr.Get(key); got != "17600" {
		t.Fatalf("按部署配置的时区取自然日，计数应为 17600，得到 %q", got)
	}
	if ttl := mr.TTL(key); ttl <= 24*time.Hour {
		t.Errorf("计数要保留到跨零点结算之后，TTL %v", ttl)
	}
	// 少用多退
	h1.Settle(ctx, 1000)
	h1.Settle(ctx, 0) // 重复结算只生效一次
	if got, _ := mr.Get(key); got != "9800" {
		t.Fatalf("结算后应为 9800，得到 %q", got)
	}
	h3, ok := q.Reserve(ctx, "credit", 20000, 8800)
	if !ok {
		t.Fatal("退回后应能预占")
	}
	// 多用少补：超出预占也照实计入
	h3.Settle(ctx, 12000)
	if got, _ := mr.Get(key); got != "21800" {
		t.Fatalf("应补记超出预占的部分，得到 %q", got)
	}
	// 第二天重新计数；前一天的预占在第二天结算，仍计入前一天
	h4, ok := q.Reserve(ctx, "risk", 20000, 8800)
	if !ok {
		t.Fatal("应能预占")
	}
	now = now.Add(24 * time.Hour)
	if _, ok := q.Reserve(ctx, "credit", 20000, 8800); !ok {
		t.Fatal("新的一天应重新计数")
	}
	h4.Settle(ctx, 100)
	if got, _ := mr.Get("bf:quota:tokens:risk:2026-10-07"); got != "100" {
		t.Fatalf("跨零点的结算计入预占那一天，得到 %q", got)
	}
}

func TestQuotaFallsBackToLocalSplitAcrossInstances(t *testing.T) {
	now := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	q := &Quota{Redis: downRedis(t), Instances: 2, Location: time.UTC, Log: quiet, Now: func() time.Time { return now }}
	ctx := context.Background()
	// 配额 20000、两个实例：本实例只有 10000
	h, ok := q.Reserve(ctx, "credit", 20000, 8800)
	if !ok {
		t.Fatal("应能预占")
	}
	if _, ok := q.Reserve(ctx, "credit", 20000, 8800); ok {
		t.Fatal("Redis 不可用时按实例数均分配额，不能放开")
	}
	h.Settle(ctx, 500)
	if _, ok := q.Reserve(ctx, "credit", 20000, 8800); !ok {
		t.Fatal("结算退回后应能预占")
	}
	if _, ok := q.Reserve(ctx, "credit", 20000, 8800); ok {
		t.Fatal("本地计数应累计")
	}
	now = now.Add(24 * time.Hour)
	if _, ok := q.Reserve(ctx, "credit", 20000, 8800); !ok {
		t.Fatal("新的一天本地计数从零开始")
	}
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func admitN(t *testing.T, a *Adaptive, p Priority, n int) []func() {
	t.Helper()
	var out []func()
	for i := range n {
		done, ok := a.Admit(p)
		if !ok {
			t.Fatalf("第 %d 个应放行", i+1)
		}
		out = append(out, done)
	}
	return out
}

func TestAdaptivePriorities(t *testing.T) {
	c := &clock{t: time.Now()}
	a := NewAdaptive(100, c.now)
	admitN(t, a, PriorityAnswer, 60)
	// 在途 60：批量（60%）先被拒，其余照常
	if _, ok := a.Admit(PriorityBatch); ok {
		t.Fatal("批量应最先被拒")
	}
	admitN(t, a, PriorityCompat, 20)
	if _, ok := a.Admit(PriorityCompat); ok {
		t.Fatal("在途 80 时兼容端点与 MCP 应被拒")
	}
	admitN(t, a, PrioritySearch, 10)
	if _, ok := a.Admit(PrioritySearch); ok {
		t.Fatal("在途 90 时仅检索应被拒")
	}
	done := admitN(t, a, PriorityAnswer, 10)
	if _, ok := a.Admit(PriorityAnswer); ok {
		t.Fatal("到达上限后同步问答也被拒")
	}
	done[0]()
	done[0]()
	if _, ok := a.Admit(PriorityAnswer); !ok {
		t.Fatal("结束一个后应放行")
	}
	if _, ok := a.Admit(PriorityAnswer); ok {
		t.Fatal("重复结束不能多放出名额")
	}
	if _, ok := a.Admit(Priority(0)); ok {
		t.Fatal("没有给出优先级的请求一律拒绝")
	}
}

func TestAdaptiveFollowsDownstreamP95(t *testing.T) {
	c := &clock{t: time.Now()}
	a := NewAdaptive(100, c.now)
	done := admitN(t, a, PriorityAnswer, 50)

	// p95 只有预算的 70%：不下调
	for range 20 {
		a.Observe(0.7)
	}
	c.advance(time.Second)
	a.Observe(0.7)
	if got := a.Limit(); got != 100 {
		t.Fatalf("p95 未超过预算的 80%% 不应下调，得到 %d", got)
	}

	// p95 达到预算的 160%：以在途数 50 为基准按 0.8/1.6 下调到 25，立即生效
	for range 20 {
		a.Observe(1.6)
	}
	c.advance(time.Second)
	a.Observe(1.6)
	if got := a.Limit(); got != 25 {
		t.Fatalf("应按比例下调到 25，得到 %d", got)
	}
	if _, ok := a.Admit(PriorityAnswer); ok {
		t.Fatal("下调后在途 50 已超过上限，新请求应被拒")
	}

	// 下游恢复：每秒上调 5% 再加 1，缓慢回到上限
	for _, d := range done {
		d()
	}
	c.advance(time.Second)
	if _, ok := a.Admit(PriorityAnswer); !ok {
		t.Fatal("恢复后应放行")
	}
	if got := a.Limit(); got != 27 {
		t.Fatalf("恢复时应缓慢上调（25 × 1.05 + 1），得到 %d", got)
	}
	for range 120 {
		c.advance(time.Second)
		a.Observe(0.1)
	}
	if got := a.Limit(); got != 100 {
		t.Fatalf("持续正常后应回到上限，得到 %d", got)
	}

	// 样本太少时不下调（先等观察窗口里的旧样本过期）
	c.advance(window + time.Second)
	a.Observe(5)
	c.advance(time.Second)
	a.Observe(5)
	if got := a.Limit(); got != 100 {
		t.Fatalf("样本不足时不应下调，得到 %d", got)
	}
}

func TestAdaptiveExhaustedShrinksImmediately(t *testing.T) {
	c := &clock{t: time.Now()}
	a := NewAdaptive(100, c.now)
	admitN(t, a, PriorityAnswer, 40)
	a.Exhausted()
	if got := a.Limit(); got != 36 {
		t.Fatalf("下游满载时应立即收紧到在途数的 90%%，得到 %d", got)
	}
	if _, ok := a.Admit(PriorityAnswer); ok {
		t.Fatal("收紧后应拒绝")
	}
	for range 100 {
		a.Exhausted()
	}
	if got := a.Limit(); got != 10 {
		t.Fatalf("不低于下限 10，得到 %d", got)
	}
}
