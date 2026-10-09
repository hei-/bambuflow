package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

type fakeRebuilds struct {
	adminv1.RebuildAdminServiceClient
	start  *adminv1.StartRebuildRequest
	cancel *adminv1.CancelRebuildRequest
}

func (f *fakeRebuilds) StartRebuild(_ context.Context, in *adminv1.StartRebuildRequest, _ ...grpc.CallOption) (*adminv1.StartRebuildResponse, error) {
	f.start = in
	return &adminv1.StartRebuildResponse{Created: true, Operation: &adminv1.RebuildOperation{OperationId: "op_ab", EmbeddingModel: in.GetEmbeddingModel(),
		Status: adminv1.OperationStatus_OPERATION_STATUS_PENDING, RequestedAt: timestamppb.Now(), ExpiresAt: timestamppb.Now()}}, nil
}

func (f *fakeRebuilds) ApproveRebuild(_ context.Context, in *adminv1.ApproveRebuildRequest, _ ...grpc.CallOption) (*adminv1.ApproveRebuildResponse, error) {
	return &adminv1.ApproveRebuildResponse{Operation: &adminv1.RebuildOperation{OperationId: in.GetOperationId(),
		Status: adminv1.OperationStatus_OPERATION_STATUS_EXECUTED, RebuildId: "rb_cd"}}, nil
}

func (f *fakeRebuilds) GetRebuild(_ context.Context, in *adminv1.GetRebuildRequest, _ ...grpc.CallOption) (*adminv1.GetRebuildResponse, error) {
	return &adminv1.GetRebuildResponse{Rebuild: &adminv1.Rebuild{RebuildId: in.GetRebuildId(), Status: "running", WrittenDocs: 40, TotalDocs: 270,
		StartedAt: timestamppb.Now()}}, nil
}

func (f *fakeRebuilds) CancelRebuild(_ context.Context, in *adminv1.CancelRebuildRequest, _ ...grpc.CallOption) (*adminv1.CancelRebuildResponse, error) {
	f.cancel = in
	return &adminv1.CancelRebuildResponse{Rebuild: &adminv1.Rebuild{RebuildId: in.GetRebuildId(), Status: "cancelled"}}, nil
}

func TestRebuildRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeRebuilds{}
	e.server.Rebuilds = f
	olga, oscar := e.authz.token(t, "olga", "console"), e.authz.token(t, "oscar", "console")
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }

	r := e.request(t, http.MethodPost, "/v1/admin/index-rebuilds", olga, `{"embedding_model":"bge-m3@v2","reason":"换嵌入模型"}`, key("a"))
	if r.status != 202 || r.body["status"] != "pending" || f.start.GetEmbeddingModel() != "bge-m3@v2" {
		t.Fatalf("申请: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/index-rebuilds/operations/op_ab/approve", oscar, "", key("b"))
	if r.status != 200 || r.body["rebuild_id"] != "rb_cd" {
		t.Fatalf("复核: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodGet, "/v1/admin/index-rebuilds/rb_cd", olga, "", nil)
	if r.status != 200 || r.body["written_docs"] != float64(40) || r.body["total_docs"] != float64(270) {
		t.Fatalf("进度: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/index-rebuilds/xx", olga, "", nil); r.status != 404 {
		t.Fatalf("ID 格式不对应 404: %d", r.status)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/index-rebuilds/rb_cd/cancel", olga, `{"reason":"评测不过"}`, key("c"))
	if r.status != 200 || r.body["status"] != "cancelled" || f.cancel.GetReason() != "评测不过" {
		t.Fatalf("取消: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/index-rebuilds", e.authz.token(t, "paul", "console"), `{"reason":"x"}`, key("d")); r.status != 403 {
		t.Fatalf("平台管理员不是运维: %d", r.status)
	}
}
