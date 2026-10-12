package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/hei-/bambuflow/libs/platform-go/tenancy"
)

func (e *env) get(t *testing.T, path, token, tenant string) resp {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, e.srv.URL+path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	if tenant != "" {
		req.Header.Set(tenancy.Header, tenant)
	}
	r, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Body.Close() }()
	out := resp{status: r.StatusCode}
	_ = json.NewDecoder(r.Body).Decode(&out.body)
	return out
}

func TestPreviewReturnsExcerptWithHighlights(t *testing.T) {
	e := newEnv(t)
	tok := e.authz.token(t, "alice", "console")
	q := url.Values{"chunk_id": {"wiki:doc-1#a1"}, "quote": {"30 万元"}}
	r := e.get(t, "/v1/documents/doc-1/preview?"+q.Encode(), tok, "")
	if r.status != 200 || r.body["chunk_id"] != "wiki:doc-1#a1" || r.body["title"] != "授信管理办法" {
		t.Fatalf("预览: %d %v", r.status, r.body)
	}
	hl := r.body["highlights"].([]any)
	// “额度不超过 30 万元。”：“30 万元”从第 6 个字符起，按字符而不是字节计
	if len(hl) != 1 || hl[0].(map[string]any)["start"] != float64(6) || hl[0].(map[string]any)["end"] != float64(11) {
		t.Fatalf("高亮: %v", hl)
	}
	recs := e.audit.snapshot()
	if last := recs[len(recs)-1]; last.Endpoint != "preview" || len(last.ShownChunkIDs) != 1 {
		t.Fatalf("预览要按展示的块写审计: %+v", last)
	}

	// 无权与不存在由 retrieval 一律返回 NOT_FOUND
	if r := e.get(t, "/v1/documents/doc-2/preview?chunk_id=wiki:doc-2%23a1", tok, ""); r.status != 404 {
		t.Fatalf("不可见的片段应 404: %d", r.status)
	}
	if r := e.get(t, "/v1/documents/doc-1/preview", tok, ""); r.status != 400 {
		t.Fatalf("缺 chunk_id 应 400: %d", r.status)
	}
}

func TestHighlightsAreRuneOffsetsAndEmptyWhenAbsent(t *testing.T) {
	got := highlights("甲乙甲乙", "甲乙")
	if len(got) != 2 || got[1] != (highlightJSON{2, 4}) {
		t.Fatalf("多处高亮: %v", got)
	}
	if got := highlights("正文已脱敏", "张三"); len(got) != 0 {
		t.Fatalf("找不到时不高亮: %v", got)
	}
}
