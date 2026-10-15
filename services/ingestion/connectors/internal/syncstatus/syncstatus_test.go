package syncstatus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func store(t *testing.T) Store {
	t.Helper()
	mr := miniredis.RunT(t)
	return Store{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
}

func TestRecordKeepsLastSuccessAcrossFailures(t *testing.T) {
	ctx, s := context.Background(), store(t)
	t0 := time.Date(2026, 10, 8, 1, 0, 0, 0, time.UTC)
	ok := Run{Command: "sync-wiki", StartedAt: t0, Upserts: 3, DeletesPaused: true, DeletesPausedReason: "over_threshold"}
	ok.Finish(nil, t0.Add(time.Minute))
	if err := s.Record(ctx, "src_wiki", ok); err != nil {
		t.Fatal(err)
	}
	bad := Run{Command: "sync-wiki", StartedAt: t0.Add(time.Hour)}
	bad.Finish(errors.New(strings.Repeat("坏", 300)), t0.Add(time.Hour))
	if err := s.Record(ctx, "src_wiki", bad); err != nil {
		t.Fatal(err)
	}
	st, err := s.Get(ctx, "src_wiki")
	if err != nil || !st.Found || st.Pause != nil {
		t.Fatalf("%+v %v", st, err)
	}
	if st.Last.Result != Failed || len([]rune(st.Last.Error)) != 200 {
		t.Fatalf("最近一轮: %+v", st.Last)
	}
	if st.LastSuccess.Result != Succeeded || st.LastSuccess.Upserts != 3 || !st.LastSuccess.FinishedAt.Equal(t0.Add(time.Minute)) ||
		st.LastSuccess.DeletesPausedReason != "over_threshold" {
		t.Fatalf("最近成功: %+v", st.LastSuccess)
	}
	if none, _ := s.Get(ctx, "src_none"); none.Found {
		t.Fatal("没有记录的数据源应为 Found=false")
	}
}

// 连续失败次数在成功后清零，健康度按数据源列出（连接器连续失败的告警依据）。
func TestHealthsCountConsecutiveFailures(t *testing.T) {
	ctx, s := context.Background(), store(t)
	t0 := time.Date(2026, 10, 8, 1, 0, 0, 0, time.UTC)
	record := func(src string, err error, at time.Time) {
		t.Helper()
		r := Run{Command: "sync-wiki", StartedAt: at}
		r.Finish(err, at)
		if e := s.Record(ctx, src, r); e != nil {
			t.Fatal(e)
		}
	}
	record("src_wiki", nil, t0)
	record("src_wiki", errors.New("x"), t0.Add(time.Hour))
	record("src_wiki", errors.New("x"), t0.Add(2*time.Hour))
	record("src_logs", errors.New("x"), t0)
	record("src_logs", nil, t0.Add(time.Hour))
	hs, err := s.Healths(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Health{}
	for _, h := range hs {
		got[h.SourceID] = h
	}
	if w := got["src_wiki"]; w.ConsecutiveFailures != 2 || !w.LastSuccess.Equal(t0) {
		t.Fatalf("src_wiki: %+v", w)
	}
	if l := got["src_logs"]; l.ConsecutiveFailures != 0 || !l.LastSuccess.Equal(t0.Add(time.Hour)) {
		t.Fatalf("src_logs: %+v", l)
	}
}

func TestPauseIsIdempotentAndResumable(t *testing.T) {
	ctx, s := context.Background(), store(t)
	p := Pause{By: "olga", At: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC), Reason: "源系统维护"}
	got, changed, err := s.SetPause(ctx, "src_wiki", p)
	if err != nil || !changed || got.By != "olga" {
		t.Fatalf("%+v %v %v", got, changed, err)
	}
	// 再暂停不覆盖原记录
	got, changed, err = s.SetPause(ctx, "src_wiki", Pause{By: "oscar", Reason: "又一次"})
	if err != nil || changed || got.By != "olga" {
		t.Fatalf("%+v %v %v", got, changed, err)
	}
	if paused, _ := s.Paused(ctx, "src_wiki"); !paused {
		t.Fatal("应为暂停")
	}
	if changed, _ := s.Resume(ctx, "src_wiki"); !changed {
		t.Fatal("恢复应返回 changed")
	}
	if changed, _ := s.Resume(ctx, "src_wiki"); changed {
		t.Fatal("没暂停时恢复应返回 false")
	}
	if paused, _ := s.Paused(ctx, "src_wiki"); paused {
		t.Fatal("应已恢复")
	}
}
