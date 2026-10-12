package push

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/publish"
)

var quiet = slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

type staticVerifier struct{ p *identityv1.Principal }

func (v staticVerifier) Verify(context.Context, string) (*identityv1.Principal, error) {
	return v.p, nil
}

func as(t *testing.T, roles ...string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer x"))
	var out context.Context
	if _, err := grpcmw.UnaryServer(staticVerifier{&identityv1.Principal{Sub: "service:crm-sync", Roles: roles}}, quiet)(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/x/y"}, func(ctx context.Context, _ any) (any, error) { out = ctx; return nil, nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

type event struct {
	id, doc string
	change  ingestionv1.ChangeType
	d       *publish.Document
}

type fakePublisher struct{ events []event }

func (f *fakePublisher) PublishWithID(_ context.Context, id string, d *publish.Document, change ingestionv1.ChangeType, _ time.Time) error {
	f.events = append(f.events, event{id, d.DocID, change, d})
	return nil
}

func (f *fakePublisher) DeleteWithID(_ context.Context, id, docID string, _ time.Time) error {
	f.events = append(f.events, event{id, docID, ingestionv1.ChangeType_CHANGE_TYPE_DELETE, nil})
	return nil
}

type fakeStatus struct{ revoked, paused bool }

func (f fakeStatus) Revoked(context.Context, string) (bool, error) { return f.revoked, nil }
func (f fakeStatus) Paused(context.Context, string) (bool, error)  { return f.paused, nil }

func doc(id, text string, acl ...string) *ingestionv1.PushedDocument {
	return &ingestionv1.PushedDocument{DocId: id, Title: "客户服务手册", MimeType: "text/markdown", Text: text, AclTokens: acl,
		Classification: commonv1.Classification_CLASSIFICATION_INTERNAL, SourceUpdatedAt: timestamppb.New(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))}
}

func newServer(t *testing.T) (*Server, *fakePublisher, *fakeStatus) {
	mr := miniredis.RunT(t)
	pub := &fakePublisher{}
	st := &fakeStatus{}
	return &Server{Publisher: func(string) Publisher { return pub }, State: RedisState{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})},
		Status: st, Log: quiet, Now: time.Now}, pub, st
}

func TestPutDetectsDuplicatesAndACLOnly(t *testing.T) {
	s, pub, _ := newServer(t)
	ctx := as(t, "source_pusher:src_crm")
	put := func(d *ingestionv1.PushedDocument) (*ingestionv1.PutDocumentResponse, error) {
		return s.PutDocument(ctx, &ingestionv1.PutDocumentRequest{SourceId: "src_crm", Document: d})
	}
	r, err := put(doc("handbook", "正文", "user:alice", "group:credit-l2"))
	if err != nil || r.GetDuplicate() || r.GetEventId() == "" || len(pub.events) != 1 || pub.events[0].change != ingestionv1.ChangeType_CHANGE_TYPE_UPSERT ||
		pub.events[0].d.SourceType != "wiki" || pub.events[0].d.Classification != "internal" {
		t.Fatalf("首次推送: %v %v %+v", r, err, pub.events)
	}
	// 同样的内容与权限（顺序不同）：不重复处理
	if r, err := put(doc("handbook", "正文", "group:credit-l2", "user:alice")); err != nil || !r.GetDuplicate() || len(pub.events) != 1 {
		t.Fatalf("重复推送: %v %v", r, err)
	}
	// 只改权限：走快速通道
	if _, err := put(doc("handbook", "正文", "user:alice")); err != nil || pub.events[1].change != ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY {
		t.Fatalf("只改权限: %v %+v", err, pub.events)
	}
	// 只改标题：按内容变化重新入库
	d := doc("handbook", "正文", "user:alice")
	d.Title = "客户服务手册（2026 版）"
	if _, err := put(d); err != nil || pub.events[2].change != ingestionv1.ChangeType_CHANGE_TYPE_UPSERT {
		t.Fatalf("改标题: %v %+v", err, pub.events)
	}
	// 二进制原文：文件名按类型带扩展名
	pdf := &ingestionv1.PushedDocument{DocId: "contract-1", Title: "合同", MimeType: "application/pdf", Raw: []byte("%PDF-1.7"),
		AclTokens: []string{"group:credit-l2"}, Classification: commonv1.Classification_CLASSIFICATION_CONFIDENTIAL,
		SourceUpdatedAt: timestamppb.Now()}
	if _, err := put(pdf); err != nil || pub.events[3].d.FileName != "contract-1.pdf" || pub.events[3].d.SourceType != "file" {
		t.Fatalf("二进制原文: %v %+v", err, pub.events[3].d)
	}
	for name, d := range map[string]*ingestionv1.PushedDocument{
		"没有 ACL":       doc("x", "正文"),
		"ACL 格式不对":     doc("x", "正文", "dept:信贷部"),
		"markdown 没正文": doc("x", "", "user:alice"),
	} {
		if _, err := put(d); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestPushRequiresRoleAndLiveSource(t *testing.T) {
	s, pub, st := newServer(t)
	req := &ingestionv1.PutDocumentRequest{SourceId: "src_crm", Document: doc("handbook", "正文", "user:alice")}
	if _, err := s.PutDocument(as(t, "source_pusher:src_other"), req); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("角色不是这个数据源: %v", err)
	}
	st.paused = true
	if _, err := s.PutDocument(as(t, "source_pusher:src_crm"), req); grpcmw.ReasonOf(err) != "conflict" || len(pub.events) != 0 {
		t.Fatalf("暂停的数据源: %v", err)
	}
}

func TestDeleteAndBatch(t *testing.T) {
	s, pub, _ := newServer(t)
	ctx := as(t, "source_pusher:src_crm")
	if _, err := s.DeleteDocument(ctx, &ingestionv1.DeleteDocumentRequest{SourceId: "src_crm", DocId: "handbook"}); status.Code(err) != codes.NotFound {
		t.Fatalf("没推送过的文档: %v", err)
	}
	r, err := s.BatchDocuments(ctx, &ingestionv1.BatchDocumentsRequest{SourceId: "src_crm", Items: []*ingestionv1.BatchItem{
		{Op: &ingestionv1.BatchItem_Upsert{Upsert: doc("a", "甲", "user:alice")}},
		{Op: &ingestionv1.BatchItem_Upsert{Upsert: doc("b", "乙")}},
		{Op: &ingestionv1.BatchItem_Delete{Delete: "a"}},
		{Op: &ingestionv1.BatchItem_Delete{Delete: "zzz"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	res := r.GetResults()
	if len(res) != 4 || res[0].GetEventId() == "" || res[1].GetErrorType() != "invalid_request" || res[2].GetEventId() == "" ||
		res[3].GetErrorType() != "not_found" || len(pub.events) != 2 || pub.events[1].change != ingestionv1.ChangeType_CHANGE_TYPE_DELETE {
		t.Fatalf("批量逐份结果: %v %+v", res, pub.events)
	}
	// 删除后重推同样的内容：重新入库
	if r, err := s.PutDocument(ctx, &ingestionv1.PutDocumentRequest{SourceId: "src_crm", Document: doc("a", "甲", "user:alice")}); err != nil || r.GetDuplicate() {
		t.Fatalf("删除后重推: %v %v", r, err)
	}
}
