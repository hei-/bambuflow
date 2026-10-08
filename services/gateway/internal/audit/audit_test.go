package audit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type memStore struct {
	objs map[string]string
	fail bool
}

func (m *memStore) Put(_ context.Context, key string, body []byte) error {
	if m.fail {
		return errors.New("对象存储不可用")
	}
	m.objs[key] = string(body)
	return nil
}

func TestShipUploadsSealedSegmentsAndKeepsThemOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	store := &memStore{objs: map[string]string{}, fail: true}
	s := &Shipper{W: w, Store: store, Prefix: "gw-1/"}
	ctx := context.Background()

	if err := w.Write(Record{TraceID: "t1", ShownChunkIDs: []string{"c1"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ship(ctx, time.Unix(1, 0)); err == nil {
		t.Fatal("存储不可用时应返回错误")
	}
	// 上传失败：段留在本地，新记录写进新段
	if err := w.Write(Record{TraceID: "t2"}); err != nil {
		t.Fatal(err)
	}
	store.fail = false
	n, err := s.Ship(ctx, time.Unix(2, 0))
	if err != nil || n != 2 {
		t.Fatalf("应送出两段: n=%d err=%v", n, err)
	}
	if len(store.objs) != 2 || !strings.Contains(store.objs["gw-1/audit.jsonl.0000000001000000000"], `"shown_chunk_ids":["c1"]`) {
		t.Fatalf("对象不符: %v", store.objs)
	}
	if left, _ := w.Sealed(); len(left) != 0 {
		t.Fatalf("送达后本地段应删除: %v", left)
	}
	// 当前段为空时不切出空段
	if n, _ := s.Ship(ctx, time.Unix(3, 0)); n != 0 {
		t.Fatalf("空段不应上传: %d", n)
	}
	if st, err := os.Stat(path); err != nil || st.Size() != 0 {
		t.Fatalf("当前段应为空文件: %v", err)
	}
}

func TestSummarizeRedactsAndTruncates(t *testing.T) {
	got := Summarize([]byte(`{"reason":"误入库","signing_secret":"s3cr3t","nested":[{"api_key":"k","doc_id":"d1"}]}`))
	if strings.Contains(got, "s3cr3t") || strings.Contains(got, `"k"`) || !strings.Contains(got, "误入库") || !strings.Contains(got, "d1") {
		t.Fatalf("脱敏不对: %s", got)
	}
	long := Summarize([]byte(`{"reason":"` + strings.Repeat("长", 2000) + `"}`))
	if len(long) > MaxSummary+len("…") || !strings.HasSuffix(long, "…") || !utf8.ValidString(long) {
		t.Fatalf("截断不对: %d 字节", len(long))
	}
	if got := Summarize([]byte("not json")); got != "<非 JSON，8 字节>" {
		t.Fatalf("非 JSON 不应记原文: %s", got)
	}
}
