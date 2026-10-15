package access

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/revocation"
)

type fakeWriter struct {
	mu        sync.Mutex
	failFirst int
	tombs     []*ingestionv1.TombstoneDocumentRequest
	acls      []*ingestionv1.UpdateAclRequest
	parts     []*ingestionv1.SetPartitionGrantsRequest
}

func (f *fakeWriter) SetPartitionGrants(_ context.Context, r *ingestionv1.SetPartitionGrantsRequest) (*ingestionv1.SetPartitionGrantsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.GetPartition() == "" {
		return nil, status.Error(codes.InvalidArgument, "partition 必填")
	}
	f.parts = append(f.parts, r)
	return &ingestionv1.SetPartitionGrantsResponse{}, nil
}

func (f *fakeWriter) TombstoneDocument(_ context.Context, r *ingestionv1.TombstoneDocumentRequest) (*ingestionv1.TombstoneDocumentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFirst > 0 {
		f.failFirst--
		return nil, status.Error(codes.Unavailable, "写入失败")
	}
	f.tombs = append(f.tombs, r)
	return &ingestionv1.TombstoneDocumentResponse{}, nil
}

func (f *fakeWriter) UpdateAcl(_ context.Context, r *ingestionv1.UpdateAclRequest) (*ingestionv1.UpdateAclResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.GetSourceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "source_id 必填")
	}
	f.acls = append(f.acls, r)
	return &ingestionv1.UpdateAclResponse{}, nil
}

func add(t *testing.T, rdb *redis.Client, evt *ingestionv1.DocumentChanged) {
	t.Helper()
	pb, err := proto.Marshal(evt)
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: Stream, Values: map[string]any{"pb": pb}}).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestFastPathAppliesDeletesAndAclChangesAndRetries(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	at := timestamppb.Now()
	add(t, rdb, &ingestionv1.DocumentChanged{SourceId: "src_a", DocId: "d1", ChangeType: ingestionv1.ChangeType_CHANGE_TYPE_DELETE, OccurredAt: at})
	add(t, rdb, &ingestionv1.DocumentChanged{SourceId: "src_a", DocId: "d2", ChangeType: ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY,
		AclTokens: []string{"group:a"}, Classification: commonv1.Classification_CLASSIFICATION_INTERNAL, OccurredAt: at})
	// 缺密级：按无人可见、受限级处理
	add(t, rdb, &ingestionv1.DocumentChanged{SourceId: "src_a", DocId: "d3", ChangeType: ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY,
		AclTokens: []string{"group:all"}, OccurredAt: at})
	// 不合法的事件确认后丢弃，不挡住后面的
	add(t, rdb, &ingestionv1.DocumentChanged{DocId: "d4", ChangeType: ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY,
		Classification: commonv1.Classification_CLASSIFICATION_PUBLIC, OccurredAt: at})
	add(t, rdb, &ingestionv1.DocumentChanged{SourceId: "src_a", DocId: "d5", ChangeType: ingestionv1.ChangeType_CHANGE_TYPE_UPSERT})

	w := &fakeWriter{failFirst: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		(&Consumer{Redis: rdb, Writer: w, Name: "indexing-0", Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(ctx)
		close(done)
	}()
	for ctx.Err() == nil {
		if n, err := rdb.XPending(ctx, Stream, Group).Result(); err == nil && n.Count == 0 {
			w.mu.Lock()
			applied := len(w.acls)
			w.mu.Unlock()
			if applied == 2 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	w.mu.Lock()
	defer w.mu.Unlock()
	// 删除第一次写入失败，退避后重试成功
	if len(w.tombs) != 1 || w.tombs[0].GetDocId() != "d1" {
		t.Fatalf("删除: %v", w.tombs)
	}
	if len(w.acls) != 2 || w.acls[0].GetClassification() != commonv1.Classification_CLASSIFICATION_INTERNAL {
		t.Fatalf("权限: %v", w.acls)
	}
	if w.acls[1].GetClassification() != commonv1.Classification_CLASSIFICATION_RESTRICTED || len(w.acls[1].GetAclTokens()) != 0 {
		t.Fatalf("缺密级应按无人可见: %v", w.acls[1])
	}
}

func TestFastPathAppliesPartitionGrants(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	at := timestamppb.Now()
	for _, evt := range []*ingestionv1.PartitionGrantChanged{
		{SourceId: "src_rules", Partition: "制度库/风险管理", Grantees: []string{"dept:风险管理部"}, OccurredAt: at},
		// 不合法的丢弃，不挡住后面的
		{SourceId: "src_rules", OccurredAt: at},
		{SourceId: "src_rules", Partition: "制度库/信贷", OccurredAt: at},
	} {
		pb, _ := proto.Marshal(evt)
		if err := rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: Stream, Values: map[string]any{"kind": KindPartitionGrant, "pb": pb}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	w := &fakeWriter{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		(&Consumer{Redis: rdb, Writer: w, Name: "indexing-0", Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(ctx)
		close(done)
	}()
	for ctx.Err() == nil {
		if n, err := rdb.XPending(ctx, Stream, Group).Result(); err == nil && n.Count == 0 {
			w.mu.Lock()
			applied := len(w.parts)
			w.mu.Unlock()
			if applied == 2 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.parts) != 2 || w.parts[0].GetGrantees()[0] != "dept:风险管理部" || len(w.parts[1].GetGrantees()) != 0 || len(w.acls) != 0 {
		t.Fatalf("分区授权: %v", w.parts)
	}
}

func TestBacklogIsHeldBeforeProcessing(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	at := timestamppb.Now()
	if err := rdb.XGroupCreateMkStream(ctx, Stream, Group, "0").Err(); err != nil {
		t.Fatal(err)
	}
	add(t, rdb, &ingestionv1.DocumentChanged{SourceId: "src_a", DocId: "d1", ChangeType: ingestionv1.ChangeType_CHANGE_TYPE_DELETE, OccurredAt: at})
	// 第一条已被读走（卡在主库写不进上重试），后面积压的还没读到
	if err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: Group, Consumer: "indexing-0", Streams: []string{Stream, ">"}, Count: 1}).Err(); err != nil {
		t.Fatal(err)
	}
	add(t, rdb, &ingestionv1.DocumentChanged{SourceId: "src_a", DocId: "d2", ChangeType: ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY, OccurredAt: at})
	pb, _ := proto.Marshal(&ingestionv1.PartitionGrantChanged{SourceId: "src_rules", Partition: "信贷", OccurredAt: at})
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: Stream, Values: map[string]any{"kind": KindPartitionGrant, "pb": pb}})
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: Stream, Values: map[string]any{"pb": "坏数据"}})

	pending := &revocation.Store{Redis: rdb}
	c := &Consumer{Redis: rdb, Name: "indexing-0", Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Pending: pending}
	if err := c.lookahead(ctx); err != nil {
		t.Fatal(err)
	}
	d1, d2, part := revocation.DocMember("d1"), revocation.DocMember("d2"), revocation.PartitionMember("src_rules", "信贷")
	v, _ := pending.Read(ctx, []string{d1, d2, part})
	// 已读走的那条由处理时登记（写入方的 Hold），这里只登记还没读到的
	if v.Held[d1] || !v.Held[d2] || !v.Held[part] {
		t.Fatalf("积压的撤权应先进名单: %v", v.Held)
	}
}
