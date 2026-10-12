package cache

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/events/v1"
	"github.com/hei-/bambuflow/services/retrieval/internal/search"
)

var quiet = slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

func client(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	mr := miniredis.RunT(t)
	return mr, redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func TestResultsPutGetInvalidate(t *testing.T) {
	mr, rdb := client(t)
	ctx := context.Background()
	r := &Results{Client: rdb, Log: quiet}
	res := &search.CachedResult{RerankModel: "bge-reranker", Items: []search.CachedItem{{ChunkID: "wiki:loan#1", Score: 0.9}}}
	start := time.Now()
	r.Put(ctx, "k1", res, []string{"loan", "rate"}, start)
	got, ok := r.Get(ctx, "k1")
	if !ok || got.RerankModel != "bge-reranker" || got.Items[0].ChunkID != "wiki:loan#1" {
		t.Fatalf("%v %v", got, ok)
	}
	if ttl := mr.TTL(resultKey("k1")); ttl != ResultTTL {
		t.Fatalf("TTL: %v", ttl)
	}
	// rate 的权限变了：引用它的结果都清掉，并记下变化时间
	if err := r.Invalidate(ctx, []string{"rate"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Get(ctx, "k1"); ok {
		t.Fatal("应已清掉")
	}
	// 在权限变化之前开始的请求不写缓存（详设 8.3）；之后开始的照常写
	r.Put(ctx, "k2", res, []string{"rate"}, start)
	if _, ok := r.Get(ctx, "k2"); ok {
		t.Fatal("过时的结果不应写入")
	}
	r.Put(ctx, "k3", res, []string{"rate"}, time.Now().Add(time.Second))
	if _, ok := r.Get(ctx, "k3"); !ok {
		t.Fatal("权限变化之后开始的请求应能写入")
	}
}

func TestVectorsRoundTrip(t *testing.T) {
	_, rdb := client(t)
	ctx := context.Background()
	v := &Vectors{Client: rdb, Log: quiet}
	v.Put(ctx, "bge-m3", []string{"额度"}, []search.QueryVector{{Dense: []float32{0.25, -1.5, 3}, Sparse: map[uint32]float32{7: 0.5}}})
	// 换一个实例（没有进程内缓存）也能从 Redis 读到；模型不同的不命中
	w := &Vectors{Client: rdb, Log: quiet}
	got := w.Get(ctx, "bge-m3", []string{"利率", "额度"})
	if len(got) != 1 || len(got[1].Dense) != 3 || got[1].Dense[1] != -1.5 || got[1].Sparse[7] != 0.5 {
		t.Fatalf("%v", got)
	}
	if len(w.Get(ctx, "bge-m4", []string{"额度"})) != 0 {
		t.Fatal("模型版本不同不应命中")
	}
}

func TestInvalidatorConsumesChunkAclChanged(t *testing.T) {
	_, events := client(t)
	_, cache := client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &Results{Client: cache, Log: quiet}
	r.Put(ctx, "k1", &search.CachedResult{}, []string{"loan"}, time.Now().Add(-time.Minute))
	done := make(chan struct{})
	go func() {
		(&Invalidator{Events: events, Results: r, Log: quiet}).Run(ctx)
		close(done)
	}()
	// 从启动时的最新处读：等它开始阻塞读之后再发
	time.Sleep(200 * time.Millisecond)
	pb, _ := proto.Marshal(&eventsv1.ChunkAclChanged{EventId: "evt_1", DocIds: []string{"loan"}, ChangeType: "acl", OccurredAt: timestamppb.Now()})
	if err := events.XAdd(ctx, &redis.XAddArgs{Stream: Stream, Values: map[string]any{"pb": pb}}).Err(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := r.Get(ctx, "k1"); !ok {
			cancel()
			<-done
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("收到 ChunkAclChanged 后应清掉相关缓存")
}
