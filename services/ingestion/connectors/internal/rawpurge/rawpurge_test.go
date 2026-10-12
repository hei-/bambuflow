package rawpurge

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

type fakeObjects struct {
	prefixes []string
	err      error
}

func (f *fakeObjects) DeletePrefix(_ context.Context, prefix string) (int, error) {
	f.prefixes = append(f.prefixes, prefix)
	return 2, f.err
}

type fakeIndex struct {
	got []*ingestionv1.RecordPurgeRequest
}

func (f *fakeIndex) RecordPurge(_ context.Context, in *ingestionv1.RecordPurgeRequest, _ ...grpc.CallOption) (*ingestionv1.RecordPurgeResponse, error) {
	f.got = append(f.got, in)
	return &ingestionv1.RecordPurgeResponse{}, nil
}

func msg(t *testing.T, e *ingestionv1.DocumentPurged) map[string]any {
	t.Helper()
	b, err := proto.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{"pb": string(b)}
}

func TestPurgeDeletesObjectsThenRecordsReceipt(t *testing.T) {
	at := time.Date(2026, 10, 10, 3, 0, 0, 0, time.UTC)
	objs, idx := &fakeObjects{}, &fakeIndex{}
	p := &Purger{Objects: objs, Index: idx, Log: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), Now: func() time.Time { return at }}
	ctx := context.Background()
	if !p.Process(ctx, msg(t, &ingestionv1.DocumentPurged{SourceId: "src_wiki", DocId: "credit-loan", ReceiptId: "del_1"})) {
		t.Fatal("应处理成功")
	}
	if len(objs.prefixes) != 1 || objs.prefixes[0] != "raw/src_wiki/credit-loan/" {
		t.Fatalf("按文档前缀删除: %v", objs.prefixes)
	}
	if r := idx.got[0]; r.GetReceiptId() != "del_1" || r.GetStore() != "object_store" || !r.GetCompletedAt().AsTime().Equal(at) {
		t.Fatalf("补记凭证: %v", r)
	}
	// 文档 ID 里带路径分隔符的事件不处理（不能借它删到别的前缀）
	if !p.Process(ctx, msg(t, &ingestionv1.DocumentPurged{SourceId: "src_wiki", DocId: "../x", ReceiptId: "del_2"})) || len(objs.prefixes) != 1 {
		t.Fatal("非法事件确认后丢弃")
	}
	objs.err = errors.New("对象存储不可用")
	if p.Process(ctx, msg(t, &ingestionv1.DocumentPurged{SourceId: "src_wiki", DocId: "d2", ReceiptId: "del_3"})) || len(idx.got) != 1 {
		t.Fatal("删除失败时不补记、不确认")
	}
}
