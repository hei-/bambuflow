package holds

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func store(t *testing.T) Store {
	t.Helper()
	return Store{Client: redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})}
}

var t0 = time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)

func TestObserveKeepsOneOpenHoldPerSource(t *testing.T) {
	ctx, s := context.Background(), store(t)
	a, err := s.Observe(ctx, "src_wiki", []string{"d1", "d2"}, 40, t0)
	if err != nil || a.Status != Pending || a.ID[:5] != "hold_" {
		t.Fatalf("%+v %v", a, err)
	}
	// 同一清单：还是那一批
	same, _ := s.Observe(ctx, "src_wiki", []string{"d1", "d2"}, 40, t0.Add(time.Hour))
	if same.ID != a.ID {
		t.Fatalf("同一清单不应另起一批: %+v", same)
	}
	// 清单变了：旧的作废，另起一批
	b, _ := s.Observe(ctx, "src_wiki", []string{"d1", "d2", "d3"}, 40, t0.Add(2*time.Hour))
	hs, _ := s.List(ctx, "src_wiki")
	if b.ID == a.ID || len(hs) != 2 || hs[0].ID != b.ID || hs[1].Status != Superseded {
		t.Fatalf("%+v", hs)
	}
	// 丢弃后同一清单不再拦成新批次
	if _, err := s.Discard(ctx, "src_wiki", b.ID, "dana", "清单误报", t0.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	again, _ := s.Observe(ctx, "src_wiki", []string{"d1", "d2", "d3"}, 40, t0.Add(4*time.Hour))
	if again.ID != b.ID || again.Status != Discarded {
		t.Fatalf("丢弃过的同一清单: %+v", again)
	}
}

func TestConfirmNeedsTwoDifferentOwners(t *testing.T) {
	ctx, s := context.Background(), store(t)
	h, _ := s.Observe(ctx, "src_wiki", []string{"d1"}, 10, t0)
	if c, _ := s.Confirmed(ctx, "src_wiki"); c != nil {
		t.Fatal("待确认的批次不应执行")
	}
	first, err := s.Confirm(ctx, "src_wiki", h.ID, "dana", "源端确实下线", t0)
	if err != nil || first.Status != AwaitingSecond || first.ConfirmedBy != "dana" {
		t.Fatalf("%+v %v", first, err)
	}
	if _, err := s.Confirm(ctx, "src_wiki", h.ID, "dana", "", t0); !errors.Is(err, ErrSelfApproval) {
		t.Fatalf("同一人不能复核: %v", err)
	}
	second, err := s.Confirm(ctx, "src_wiki", h.ID, "rick", "", t0)
	if err != nil || second.Status != Confirmed || second.ApprovedBy != "rick" {
		t.Fatalf("%+v %v", second, err)
	}
	c, _ := s.Confirmed(ctx, "src_wiki")
	if c == nil || c.ID != h.ID {
		t.Fatalf("应有待执行的批次: %+v", c)
	}
	if err := s.MarkApplied(ctx, "src_wiki", h.ID, 1, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Confirm(ctx, "src_wiki", h.ID, "ivan", "", t0); !errors.Is(err, ErrState) {
		t.Fatalf("已执行的不能再确认: %v", err)
	}
	if _, err := s.Discard(ctx, "src_wiki", "hold_none", "dana", "", t0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("不存在的批次: %v", err)
	}
}

func TestClosedHoldsArePruned(t *testing.T) {
	ctx, s := context.Background(), store(t)
	for i := range keepClosed + 5 {
		h, err := s.Observe(ctx, "src_wiki", []string{string(rune('a' + i))}, 10, t0.Add(time.Duration(i)*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Discard(ctx, "src_wiki", h.ID, "dana", "", t0); err != nil {
			t.Fatal(err)
		}
	}
	if hs, _ := s.List(ctx, "src_wiki"); len(hs) != keepClosed {
		t.Fatalf("已结束的只保留 %d 批，实际 %d", keepClosed, len(hs))
	}
}
