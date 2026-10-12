package api

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

type fakeAuditQuery struct {
	adminv1.AuditAdminServiceClient
	got *adminv1.QueryAuditRequest
}

func (f *fakeAuditQuery) QueryAudit(_ context.Context, in *adminv1.QueryAuditRequest, _ ...grpc.CallOption) (*adminv1.QueryAuditResponse, error) {
	f.got = in
	return &adminv1.QueryAuditResponse{NextPageToken: "n", Records: []*adminv1.AuditRecord{{
		Time: timestamppb.New(time.Date(2026, 10, 7, 1, 2, 3, 0, time.UTC)), TraceId: "t1", Sub: "alice", Endpoint: "answers", Status: 200,
		ShownChunkIds: []string{"src_wiki:loan#a1"}, MatchedIn: []string{"shown"}, Kind: "access", Source: "gateway",
	}, {
		Time: timestamppb.New(time.Date(2026, 10, 7, 1, 3, 0, 0, time.UTC)), Kind: "change", Source: "indexing", Sub: "service:indexing",
		Action: "document.purge", ObjectId: "loan", DetailJson: `{"reason":"源端已删"}`, MatchedIn: []string{"object"},
	}}}, nil
}

func TestAuditQueryRoute(t *testing.T) {
	e := newEnv(t)
	f := &fakeAuditQuery{}
	e.server.AuditQuery = f
	q := url.Values{"sub": {"alice"}, "doc_id": {"loan"}, "from": {"2026-10-07T00:00:00Z"}, "to": {"2026-10-07T12:00:00+08:00"}, "page_size": {"20"},
		"kind": {"access", "change"}}
	r := e.request(t, http.MethodGet, "/v1/admin/audit?"+q.Encode(), e.authz.token(t, "audra", "console"), "", nil)
	recs, _ := r.body["records"].([]any)
	if r.status != 200 || len(recs) != 2 || r.body["next_page_token"] != "n" {
		t.Fatalf("审计查询: %d %s", r.status, r.raw)
	}
	rec := recs[0].(map[string]any)
	if rec["time"] != "2026-10-07T01:02:03Z" || rec["matched_in"].([]any)[0] != "shown" || len(rec["context_chunk_ids"].([]any)) != 0 {
		t.Fatalf("记录: %v", rec)
	}
	chg := recs[1].(map[string]any)
	if chg["kind"] != "change" || chg["action"] != "document.purge" || chg["detail"].(map[string]any)["reason"] != "源端已删" {
		t.Fatalf("变更审计: %v", chg)
	}
	if f.got.GetSub() != "alice" || f.got.GetDocId() != "loan" || f.got.GetPageSize() != 20 || len(f.got.GetKinds()) != 2 ||
		!f.got.GetTo().AsTime().Equal(time.Date(2026, 10, 7, 4, 0, 0, 0, time.UTC)) {
		t.Fatalf("转给 governance 的条件不对: %v", f.got)
	}
	// 只有审计岗；时间格式不对 400
	if r := e.request(t, http.MethodGet, "/v1/admin/audit?"+q.Encode(), e.authz.token(t, "olga", "console"), "", nil); r.status != 403 {
		t.Fatalf("平台运维不能查审计: %d", r.status)
	}
	bad := url.Values{"from": {"昨天"}, "to": {"2026-10-07T00:00:00Z"}}
	if r := e.request(t, http.MethodGet, "/v1/admin/audit?"+bad.Encode(), e.authz.token(t, "audra", "console"), "", nil); r.status != 400 {
		t.Fatalf("时间格式不对应 400: %d", r.status)
	}
	// 查询本身记管理审计
	recs2 := e.audit.snapshot()
	if last := recs2[len(recs2)-1]; last.Admin == nil || last.Endpoint != "admin-audit-query" || last.Sub != "audra" {
		t.Fatalf("审计查询应记管理审计: %+v", last)
	}
}
