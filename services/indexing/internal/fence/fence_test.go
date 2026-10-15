package fence

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/hei-/bambuflow/libs/platform-go/revocation"
)

// db 是一个按调用返回位点的假数据库：主库给提交位点，副本每被查一次回放位点前进 step。
type db struct {
	mu    sync.Mutex
	lsn   revocation.LSN
	step  revocation.LSN
	calls int
	log   *[]string
	name  string
}

type row struct {
	v   string
	err error
}

func (r row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	switch d := dest[0].(type) {
	case **string:
		*d = &r.v
	case *float64:
		*d = 0
	}
	return nil
}

func (d *db) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.log != nil {
		*d.log = append(*d.log, d.name)
	}
	v := d.lsn
	d.lsn += d.step
	return row{v: v.String()}
}

func newFence(t *testing.T, primary revocation.LSN, replicas ...*db) (*Fence, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)
	f := &Fence{Primary: &db{lsn: primary}, Store: &revocation.Store{Redis: redis.NewClient(&redis.Options{Addr: mr.Addr()})},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Poll: time.Millisecond, Timeout: 50 * time.Millisecond,
		Settle: 30 * time.Millisecond, MaxBlock: 100 * time.Millisecond}
	for i, r := range replicas {
		f.Replicas = append(f.Replicas, Replica{Name: string(rune('a' + i)), DB: r})
	}
	return f, mr
}

func fence(t *testing.T, f *Fence) revocation.LSN {
	t.Helper()
	v, err := f.Store.Read(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return v.Fence
}

func TestCommitRaisesFenceThenWaitsForReplicas(t *testing.T) {
	slow := &db{lsn: 100, step: 100}
	f, _ := newFence(t, 1000, &db{lsn: 5000}, slow)
	f.Timeout = 10 * time.Second
	if err := f.Commit(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if fence(t, f) != 1000 {
		t.Fatalf("撤权位点应抬到提交位点: %v", fence(t, f))
	}
	// 慢副本被查到回放位点 ≥ 1000 才返回
	if slow.lsn < 1000 {
		t.Fatalf("没等慢副本追上就返回了: %v", slow.lsn)
	}
}

func TestCommitGivesUpOnStuckReplicaOnceFenceIsRaised(t *testing.T) {
	f, _ := newFence(t, 1000, &db{lsn: 10})
	start := time.Now()
	// 位点已抬上：落后的副本会被 retrieval 拒绝，超时后不再等，也不报错
	if err := f.Commit(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < f.Timeout {
		t.Fatal("应等到超时")
	}
}

func TestContentWriteOnlyRaisesFence(t *testing.T) {
	r := &db{lsn: 10}
	f, _ := newFence(t, 1000, r)
	if err := f.Commit(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if fence(t, f) != 1000 || r.calls != 0 {
		t.Fatalf("内容写入只抬位点、不等副本: %v %d", fence(t, f), r.calls)
	}
}

func TestFallbackWaitsForAllReplicasAndSettleTime(t *testing.T) {
	r := &db{lsn: 900, step: 50}
	f, mr := newFence(t, 1000, r)
	f.MaxBlock = 10 * time.Second
	mr.Close()
	start := time.Now()
	// 撤权位点写不进：内容写入也要等全部副本追上，并至少等够 Settle
	if err := f.Commit(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if r.lsn < 1000 || time.Since(start) < f.Settle {
		t.Fatalf("兜底等待不够: lsn=%v 用时 %v", r.lsn, time.Since(start))
	}
}

func TestFallbackFailsWhenAReplicaNeverCatchesUp(t *testing.T) {
	f, mr := newFence(t, 1000, &db{lsn: 5000}, &db{lsn: 10})
	mr.Close()
	if err := f.Commit(context.Background(), true); !errors.Is(err, ErrNotSettled) {
		t.Fatalf("撤权位点写不进又等不到全部副本时应报错（事件重做）: %v", err)
	}
}

// 快速通道的顺序（详设 7.3“等待不可绕过”）：登记名单 → 提交 → 抬位点 → 等副本 → 通知 → 移出名单。
func TestRevokeOrder(t *testing.T) {
	var log []string
	f, _ := newFence(t, 1000, &db{lsn: 990, step: 10, log: &log, name: "replica"})
	f.Timeout = 10 * time.Second
	ctx := context.Background()
	m := revocation.DocMember("d1")
	held := func() bool { v, _ := f.Store.Read(ctx, []string{m}); return v.Held[m] }
	err := f.Revoke(ctx, []string{m}, func() error {
		if !held() {
			t.Fatal("提交前应已登记名单")
		}
		log = append(log, "commit")
		return nil
	}, func() {
		if fence(t, f) != 1000 || !held() {
			t.Fatal("通知前撤权位点应已抬上、名单还在")
		}
		log = append(log, "notify")
	})
	if err != nil {
		t.Fatal(err)
	}
	if held() {
		t.Fatal("完成后应移出名单")
	}
	want := []string{"commit", "replica", "replica", "notify"}
	if len(log) != len(want) {
		t.Fatalf("顺序: %v", log)
	}
	for i := range want {
		if log[i] != want[i] {
			t.Fatalf("顺序: %v", log)
		}
	}
}

func TestRevokeKeepsHoldWhenCommitFails(t *testing.T) {
	f, _ := newFence(t, 1000)
	ctx := context.Background()
	m := revocation.DocMember("d1")
	err := f.Revoke(ctx, []string{m}, func() error { return errors.New("主库不可用") }, func() { t.Fatal("提交失败不通知") })
	if err == nil {
		t.Fatal("应返回错误")
	}
	if v, _ := f.Store.Read(ctx, []string{m}); !v.Held[m] {
		t.Fatal("主库写不进时撤权靠名单生效，条目要留着")
	}
}

func TestEnsureRebuildsLostFence(t *testing.T) {
	f, _ := newFence(t, 7000)
	f.ensure(context.Background())
	if fence(t, f) != 7000 {
		t.Fatalf("位点缺失时用主库当前位点重建: %v", fence(t, f))
	}
}
