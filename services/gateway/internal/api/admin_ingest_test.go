package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

type fakeIngest struct {
	adminv1.IngestAdminServiceClient
	list   *adminv1.ListDeadLettersRequest
	replay *adminv1.ReplayRequest
}

func (f *fakeIngest) GetIngestStatus(_ context.Context, in *adminv1.GetIngestStatusRequest, _ ...grpc.CallOption) (*adminv1.GetIngestStatusResponse, error) {
	return &adminv1.GetIngestStatusResponse{DocId: in.GetDocId(), SourceId: "src_wiki", EventId: "evt_1", Stage: "parse",
		State: "dead_letter", LastError: &adminv1.IngestError{Code: "bad_document", Message: "表格损坏"}, Attempts: 1,
		UpdatedAt: timestamppb.Now()}, nil
}

func (f *fakeIngest) ListDeadLetters(_ context.Context, in *adminv1.ListDeadLettersRequest, _ ...grpc.CallOption) (*adminv1.ListDeadLettersResponse, error) {
	f.list = in
	return &adminv1.ListDeadLettersResponse{DeadLetters: []*adminv1.DeadLetter{{DocId: "a", SourceId: "src_wiki", Stage: "parse",
		Error: &adminv1.IngestError{Code: "bad_document"}, Quarantined: true, FirstFailedAt: timestamppb.Now(),
		LastFailedAt: timestamppb.Now()}}, NextPageToken: "YQ=="}, nil
}

func (f *fakeIngest) Replay(_ context.Context, in *adminv1.ReplayRequest, _ ...grpc.CallOption) (*adminv1.ReplayResponse, error) {
	f.replay = in
	return &adminv1.ReplayResponse{IngestBatchId: "ing_ab12", Replayed: 3, SkippedQuarantined: 1}, nil
}

func TestIngestAdminRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeIngest{}
	e.server.Ingest = f
	olga := e.authz.token(t, "olga", "console")

	r := e.request(t, http.MethodGet, "/v1/admin/documents/wiki:credit-1/ingest-status", olga, "", nil)
	if r.status != 200 || r.body["state"] != "dead_letter" || r.body["last_error"].(map[string]any)["code"] != "bad_document" {
		t.Fatalf("入库状态: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodGet, "/v1/admin/dead-letters?stage=parse&quarantined=true&page_size=10", olga, "", nil)
	if r.status != 200 || r.body["next_page_token"] != "YQ==" || f.list.GetStage() != "parse" || !f.list.GetQuarantinedOnly() || f.list.GetPageSize() != 10 {
		t.Fatalf("死信列表: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/dead-letters?quarantined=yes", olga, "", nil); r.status != 400 {
		t.Fatalf("quarantined 只认 true/false: %d", r.status)
	}
	key := map[string]string{"Idempotency-Key": "r1"}
	r = e.request(t, http.MethodPost, "/v1/admin/replays", olga, `{"error_code":"bad_document","reason":"解析器已修复"}`, key)
	if r.status != 200 || r.body["ingest_batch_id"] != "ing_ab12" || f.replay.GetErrorCode() != "bad_document" || f.replay.GetReason() != "解析器已修复" {
		t.Fatalf("重放: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/replays", olga, `{"reason":"x","everything":true}`, map[string]string{"Idempotency-Key": "r2"}); r.status != 400 {
		t.Fatalf("未知字段应 400: %d", r.status)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/replays", e.authz.token(t, "paul", "console"), `{"error_code":"x","reason":"x"}`,
		map[string]string{"Idempotency-Key": "r3"}); r.status != 403 {
		t.Fatalf("平台管理员不是运维，应 403: %d", r.status)
	}
}
