package revoke

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/outbox"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/syncstatus"
)

var quiet = slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

type docStream struct {
	grpc.ClientStream
	batches [][]*ingestionv1.IndexedDocument
}

func (s *docStream) Recv() (*ingestionv1.ListSourceDocumentsResponse, error) {
	if len(s.batches) == 0 {
		return nil, io.EOF
	}
	b := s.batches[0]
	s.batches = s.batches[1:]
	return &ingestionv1.ListSourceDocumentsResponse{Documents: b}, nil
}

type fakeIndex struct {
	docs      []*ingestionv1.IndexedDocument
	tombstone []*ingestionv1.TombstoneDocumentRequest
	failList  error
}

func (f *fakeIndex) ListSourceDocuments(_ context.Context, in *ingestionv1.ListSourceDocumentsRequest,
	_ ...grpc.CallOption) (grpc.ServerStreamingClient[ingestionv1.ListSourceDocumentsResponse], error) {
	if f.failList != nil {
		return nil, f.failList
	}
	// 分两批返回，验证跨批读取
	return &docStream{batches: [][]*ingestionv1.IndexedDocument{f.docs[:1], f.docs[1:]}}, nil
}

func (f *fakeIndex) TombstoneDocument(_ context.Context, in *ingestionv1.TombstoneDocumentRequest,
	_ ...grpc.CallOption) (*ingestionv1.TombstoneDocumentResponse, error) {
	f.tombstone = append(f.tombstone, in)
	return &ingestionv1.TombstoneDocumentResponse{Result: ingestionv1.TombstoneResult_TOMBSTONE_RESULT_TOMBSTONED}, nil
}

type memAudit struct {
	actions []string
	fail    bool
}

func (m *memAudit) Write(_ context.Context, action, objectID string, _ any) error {
	if m.fail {
		return errors.New("审计桶不可用")
	}
	m.actions = append(m.actions, action+":"+objectID)
	return nil
}

func setup(t *testing.T) (*Consumer, *fakeIndex, *redis.Client, syncstatus.Store) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	idx := &fakeIndex{docs: []*ingestionv1.IndexedDocument{
		{DocId: "d1"}, {DocId: "d2", Tombstoned: true}, {DocId: "d3"},
	}}
	st := syncstatus.Store{Client: rdb}
	c := &Consumer{Log: quiet, Revoker: &Revoker{Index: idx, Status: st, Redis: rdb, Audit: &memAudit{}, Log: quiet}}
	return c, idx, rdb, st
}

func event(t *testing.T, typ, object string, at time.Time) map[string]any {
	t.Helper()
	_, pb, err := outbox.New(typ, "", object, at)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{"pb": string(pb)}
}

func TestRevokedSourceIsTombstonedAndStopsSyncing(t *testing.T) {
	c, idx, rdb, st := setup(t)
	ctx := context.Background()
	at := time.Date(2026, 10, 9, 8, 0, 0, 0, time.UTC)
	rdb.HSet(ctx, "bf:connector:src_wiki:state", "d1", "h|a")
	rdb.HSet(ctx, "bf:connector:src_other:state", "x", "h|a")

	if !c.Process(ctx, event(t, outbox.SourceRevoked, "src_wiki", at)) {
		t.Fatal("撤销应处理成功")
	}
	var ids []string
	for _, r := range idx.tombstone {
		ids = append(ids, r.GetDocId())
		if r.GetSourceId() != "src_wiki" || !r.GetOccurredAt().AsTime().Equal(at) {
			t.Fatalf("删除时间取撤销时间: %v", r)
		}
	}
	if !slices.Equal(ids, []string{"d1", "d3"}) {
		t.Fatalf("只给在用文档写删除标记: %v", ids)
	}
	if a := c.Revoker.Audit.(*memAudit).actions; !slices.Equal(a, []string{"source.revoke_applied:src_wiki"}) {
		t.Fatalf("撤销执行完写变更审计: %v", a)
	}
	if revoked, _ := st.Revoked(ctx, "src_wiki"); !revoked {
		t.Fatal("应标为已撤销")
	}
	if n, _ := rdb.Exists(ctx, "bf:connector:src_wiki:state").Result(); n != 0 {
		t.Fatal("已发出状态应清掉")
	}
	if n, _ := rdb.Exists(ctx, "bf:connector:src_other:state").Result(); n != 1 {
		t.Fatal("别的数据源不受影响")
	}
	// 重复处理同一事件：幂等，撤销时间保留最早的
	if !c.Process(ctx, event(t, outbox.SourceRevoked, "src_wiki", at.Add(time.Hour))) {
		t.Fatal("重复处理应成功")
	}
	s, _ := st.Get(ctx, "src_wiki")
	if s.RevokedAt == nil || !s.RevokedAt.Equal(at) {
		t.Fatalf("撤销时间: %v", s.RevokedAt)
	}
}

func TestOtherEventsAndFailures(t *testing.T) {
	c, idx, _, st := setup(t)
	ctx := context.Background()
	if !c.Process(ctx, event(t, outbox.IndexVersionLive, "ix-1", time.Now())) || len(idx.tombstone) != 0 {
		t.Fatal("其他事件直接确认")
	}
	if !c.Process(ctx, map[string]any{"pb": "not-proto"}) {
		t.Fatal("解析不了的事件确认后丢弃")
	}
	if !c.Process(ctx, event(t, outbox.SourceRevoked, "wiki", time.Now())) || len(idx.tombstone) != 0 {
		t.Fatal("非法的数据源 ID 不处理")
	}
	idx.failList = errors.New("indexing 不可用")
	if c.Process(ctx, event(t, outbox.SourceRevoked, "src_wiki", time.Now())) {
		t.Fatal("indexing 不可用时不确认，之后重试")
	}
	// 先标了已撤销：即使删除标记还没写完，之后的同步也不会把文档重新入库
	if revoked, _ := st.Revoked(ctx, "src_wiki"); !revoked {
		t.Fatal("应先标为已撤销")
	}
}

func TestRevokeIsRetriedWhenAuditFails(t *testing.T) {
	c, _, rdb, _ := setup(t)
	ctx := context.Background()
	c.Revoker.Audit = &memAudit{fail: true}
	rdb.HSet(ctx, "bf:connector:src_wiki:state", "d1", "h|a")
	if c.Process(ctx, event(t, outbox.SourceRevoked, "src_wiki", time.Now())) {
		t.Fatal("审计写不进去不应确认事件")
	}
	if n, _ := rdb.Exists(ctx, "bf:connector:src_wiki:state").Result(); n != 1 {
		t.Fatal("没完成时不清已发出状态，重做时照常比对")
	}
}
