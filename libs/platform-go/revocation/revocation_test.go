package revocation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestParseLSN(t *testing.T) {
	for in, want := range map[string]LSN{"0/0": 0, "16/B374D848": 0x16B374D848, "FFFFFFFF/FFFFFFFF": ^LSN(0)} {
		got, err := ParseLSN(in)
		if err != nil || got != want {
			t.Fatalf("%s → %d %v，期望 %d", in, got, err, want)
		}
		if got.String() != in {
			t.Fatalf("%s 往返后是 %s", in, got)
		}
	}
	for _, bad := range []string{"", "16", "x/1", "1/100000000"} {
		if _, err := ParseLSN(bad); err == nil {
			t.Fatalf("%q 应解析失败", bad)
		}
	}
}

func newStore(t *testing.T) (*Store, *miniredis.Miniredis, *time.Time) {
	mr := miniredis.RunT(t)
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	return &Store{Redis: redis.NewClient(&redis.Options{Addr: mr.Addr()}), Now: func() time.Time { return now }}, mr, &now
}

func TestFenceOnlyGoesUp(t *testing.T) {
	s, _, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Read(ctx, nil); !errors.Is(err, ErrNoFence) {
		t.Fatalf("还没有位点时应返回 ErrNoFence: %v", err)
	}
	// 跨过 32 位边界的大小比较也要对：按定长十六进制比较
	for _, p := range []LSN{0x1_0000_0000, 0xFFFF_FFFF, 0x2_0000_0010, 0x2_0000_000F} {
		if err := s.Raise(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	v, err := s.Read(ctx, nil)
	if err != nil || v.Fence != 0x2_0000_0010 {
		t.Fatalf("位点应为最大值: %v %v", v.Fence, err)
	}
	if ok, err := s.Init(ctx, 1); ok || err != nil {
		t.Fatalf("已有位点时不重建: %v %v", ok, err)
	}
}

func TestInitRebuildsLostFence(t *testing.T) {
	s, mr, _ := newStore(t)
	ctx := context.Background()
	_ = s.Raise(ctx, 100)
	mr.Del(FenceKey)
	if ok, err := s.Init(ctx, 500); !ok || err != nil {
		t.Fatalf("位点丢了应重建: %v %v", ok, err)
	}
	if v, _ := s.Read(ctx, nil); v.Fence != 500 {
		t.Fatalf("重建后的位点: %v", v.Fence)
	}
}

func TestPendingHoldReleaseAndExpiry(t *testing.T) {
	s, _, now := newStore(t)
	ctx := context.Background()
	a, b, p := DocMember("doc-a"), DocMember("doc-b"), PartitionMember("src_hr", "dept-7")
	if err := s.Hold(ctx, a, b, p); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, b); err != nil {
		t.Fatal(err)
	}
	// 分区不用点名，名单里的全部取回
	v, err := s.Read(ctx, []string{a, b, DocMember("doc-c")})
	if !errors.Is(err, ErrNoFence) {
		t.Fatalf("没有位点时名单照样能读: %v", err)
	}
	if !v.Held[a] || v.Held[b] || !v.Held[p] || len(v.Held) != 2 {
		t.Fatalf("名单: %v", v.Held)
	}
	// 到期的条目不再算在名单里；续期后重新生效
	*now = now.Add(PendingTTL + time.Second)
	if v, _ := s.Read(ctx, []string{a}); v.Held[a] || v.Held[p] {
		t.Fatal("到期的条目不应再算")
	}
	if err := s.Hold(ctx, a); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Read(ctx, []string{a}); !v.Held[a] {
		t.Fatal("续期后应在名单里")
	}
}

func TestStatsTracksFirstHoldNotRenewals(t *testing.T) {
	s, _, now := newStore(t)
	ctx := context.Background()
	a, b := DocMember("doc-a"), PartitionMember("src_hr", "x")
	_ = s.Hold(ctx, a)
	*now = now.Add(4 * time.Minute)
	// 续期不改登记时间：卡住的撤权年龄照样增长
	_ = s.Hold(ctx, a, b)
	*now = now.Add(2 * time.Minute)
	n, oldest, err := s.Stats(ctx)
	if err != nil || n != 2 || oldest != 6*time.Minute {
		t.Fatalf("统计: %d %v %v", n, oldest, err)
	}
	_ = s.Release(ctx, a)
	if n, oldest, _ := s.Stats(ctx); n != 1 || oldest != 2*time.Minute {
		t.Fatalf("移出后: %d %v", n, oldest)
	}
	// 到期没移出的条目不再算
	*now = now.Add(PendingTTL)
	if n, oldest, _ := s.Stats(ctx); n != 0 || oldest != 0 {
		t.Fatalf("到期后: %d %v", n, oldest)
	}
}
