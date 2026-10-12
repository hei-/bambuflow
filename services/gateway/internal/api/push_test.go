package api

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

type fakePush struct {
	ingestionv1.PushServiceClient
	mu      sync.Mutex
	put     *ingestionv1.PutDocumentRequest
	batch   *ingestionv1.BatchDocumentsRequest
	deleted *ingestionv1.DeleteDocumentRequest
	auth    string
}

func (f *fakePush) seen(ctx context.Context) {
	md, _ := metadata.FromOutgoingContext(ctx)
	f.auth = strings.Join(md.Get("authorization"), "")
}

func (f *fakePush) PutDocument(ctx context.Context, in *ingestionv1.PutDocumentRequest, _ ...grpc.CallOption) (*ingestionv1.PutDocumentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.put = in
	f.seen(ctx)
	return &ingestionv1.PutDocumentResponse{EventId: "evt_1"}, nil
}

func (f *fakePush) DeleteDocument(ctx context.Context, in *ingestionv1.DeleteDocumentRequest, _ ...grpc.CallOption) (*ingestionv1.DeleteDocumentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = in
	if in.GetDocId() == "gone" {
		return nil, status.Error(codes.NotFound, "没有推送过这份文档")
	}
	return &ingestionv1.DeleteDocumentResponse{EventId: "evt_2"}, nil
}

func (f *fakePush) BatchDocuments(_ context.Context, in *ingestionv1.BatchDocumentsRequest, _ ...grpc.CallOption) (*ingestionv1.BatchDocumentsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batch = in
	out := &ingestionv1.BatchDocumentsResponse{}
	for _, it := range in.GetItems() {
		id := it.GetDelete()
		if u := it.GetUpsert(); u != nil {
			id = u.GetDocId()
		}
		out.Results = append(out.Results, &ingestionv1.BatchItemResult{DocId: id, EventId: "evt_" + id})
	}
	return out, nil
}

func pushEnv(t *testing.T) (*env, *fakePush) {
	e := newEnv(t)
	f := &fakePush{}
	e.server.Push = f
	e.server.PushSources = func(id string) (*configv1.PushSource, bool) {
		if id != "src_crm" {
			return nil, false
		}
		return &configv1.PushSource{SourceId: id, PushClient: "crm-sync", DefaultClassification: commonv1.Classification_CLASSIFICATION_INTERNAL}, true
	}
	return e, f
}

const pushDoc = `{"title":"客户服务手册","mime_type":"text/markdown","content":"# 手册\n正文","acl":{"users":["alice"],"groups":["credit-l2"]},` +
	`"source_updated_at":"2026-10-01T08:00:00Z"}`

func TestPushDocument(t *testing.T) {
	e, f := pushEnv(t)
	svc := e.authz.token(t, "service-account-crm-sync", "crm-sync")
	r := e.request(t, http.MethodPut, "/v1/sources/src_crm/documents/handbook", svc, pushDoc, nil)
	if r.status != 202 || r.body["event_id"] != "evt_1" {
		t.Fatalf("推送: %d %s", r.status, r.raw)
	}
	d := f.put.GetDocument()
	if f.put.GetSourceId() != "src_crm" || d.GetText() != "# 手册\n正文" || strings.Join(d.GetAclTokens(), ",") != "user:alice,group:credit-l2" ||
		d.GetClassification() != commonv1.Classification_CLASSIFICATION_INTERNAL || !strings.HasPrefix(f.auth, "Bearer ") {
		t.Fatalf("转给 connectors 的请求: %v %s", f.put, f.auth)
	}
	// 审计记推送客户端与请求体摘要，不记正文
	last := e.audit.snapshot()[len(e.audit.snapshot())-1]
	if last.Actor != "crm-sync" || last.Admin == nil || len(last.Admin.RequestSHA) != 64 || strings.Contains(last.Admin.Request, "正文") {
		t.Fatalf("审计: %+v %+v", last, last.Admin)
	}

	bin := `{"title":"合同","mime_type":"application/pdf","content_base64":"` + base64.StdEncoding.EncodeToString([]byte("%PDF-1.7")) +
		`","acl":{"groups":["credit-l2"]},"classification":"confidential","source_updated_at":"2026-10-01T08:00:00Z"}`
	if r := e.request(t, http.MethodPut, "/v1/sources/src_crm/documents/contract-1", svc, bin, nil); r.status != 202 ||
		string(f.put.GetDocument().GetRaw()) != "%PDF-1.7" || f.put.GetDocument().GetClassification() != commonv1.Classification_CLASSIFICATION_CONFIDENTIAL {
		t.Fatalf("二进制原文: %d %s", r.status, r.raw)
	}

	for name, c := range map[string]struct {
		token, path, body string
		status            int
		typ               string
	}{
		"终端用户的令牌":   {e.authz.token(t, "alice", "crm-sync"), "/v1/sources/src_crm/documents/handbook", pushDoc, 403, "service_identity_required"},
		"不是登记的客户端":  {e.authz.token(t, "service-account-other", "other"), "/v1/sources/src_crm/documents/handbook", pushDoc, 403, "forbidden"},
		"不接受推送的数据源": {svc, "/v1/sources/src_wiki/documents/handbook", pushDoc, 403, "forbidden"},
		"没有令牌":      {"", "/v1/sources/src_crm/documents/handbook", pushDoc, 401, "unauthenticated"},
		"没有 ACL": {svc, "/v1/sources/src_crm/documents/handbook",
			`{"title":"x","mime_type":"text/markdown","content":"x","source_updated_at":"2026-10-01T08:00:00Z"}`, 400, "invalid_request"},
		"ACL 里的用户不存在": {svc, "/v1/sources/src_crm/documents/handbook",
			strings.Replace(pushDoc, `"alice"`, `"nobody"`, 1), 400, "invalid_request"},
		"密级低于数据源默认": {svc, "/v1/sources/src_crm/documents/handbook",
			strings.Replace(pushDoc, `"acl"`, `"classification":"public","acl"`, 1), 400, "invalid_request"},
		"doc_id 不合法": {svc, "/v1/sources/src_crm/documents/Bad_ID", pushDoc, 400, "invalid_request"},
		"类型不支持": {svc, "/v1/sources/src_crm/documents/handbook",
			strings.Replace(pushDoc, "text/markdown", "text/html", 1), 400, "invalid_request"},
	} {
		r := e.request(t, http.MethodPut, c.path, c.token, c.body, nil)
		if r.status != c.status || r.body["type"] != c.typ {
			t.Errorf("%s: %d %s", name, r.status, r.raw)
		}
	}
	if r := e.request(t, http.MethodPut, "/v1/sources/src_crm/documents/handbook", svc,
		strings.Replace(pushDoc, `"alice"`, `"nobody"`, 1), nil); !strings.Contains(r.raw, "user:nobody") {
		t.Errorf("应说明哪个用户不存在: %s", r.raw)
	}

	del := e.request(t, http.MethodDelete, "/v1/sources/src_crm/documents/handbook", svc, "", nil)
	if del.status != 202 || del.body["event_id"] != "evt_2" || f.deleted.GetDocId() != "handbook" {
		t.Fatalf("删除: %d %s", del.status, del.raw)
	}
	if r := e.request(t, http.MethodDelete, "/v1/sources/src_crm/documents/gone", svc, "", nil); r.status != 404 {
		t.Fatalf("没推送过的文档: %d", r.status)
	}
}

func TestPushBatch(t *testing.T) {
	e, f := pushEnv(t)
	svc := e.authz.token(t, "service-account-crm-sync", "crm-sync")
	body := `{"items":[{"op":"upsert","doc_id":"a","document":` + pushDoc + `},{"op":"upsert","doc_id":"b","document":{"title":"x"}},` +
		`{"op":"delete","doc_id":"c"},{"op":"rename","doc_id":"d"}]}`
	r := e.request(t, http.MethodPost, "/v1/sources/src_crm/documents/batch", svc, body, nil)
	if r.status != 200 {
		t.Fatalf("%d %s", r.status, r.raw)
	}
	res := r.body["results"].([]any)
	get := func(i int, k string) any { return res[i].(map[string]any)[k] }
	if len(res) != 4 || get(0, "event_id") != "evt_a" || get(1, "error_type") != "invalid_request" || get(2, "event_id") != "evt_c" ||
		get(3, "error_type") != "invalid_request" || len(f.batch.GetItems()) != 2 {
		t.Fatalf("逐份结果按请求顺序，不合法的不转给 connectors: %s %v", r.raw, f.batch)
	}
	if r := e.request(t, http.MethodPost, "/v1/sources/src_crm/documents/batch", svc, `{"items":[]}`, nil); r.status != 400 {
		t.Fatalf("空批次: %d", r.status)
	}
}
