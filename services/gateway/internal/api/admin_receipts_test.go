package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

type fakeReceipts struct {
	adminv1.DeletionReceiptAdminServiceClient
	got *adminv1.ListDeletionReceiptsRequest
}

func (f *fakeReceipts) ListDeletionReceipts(_ context.Context, in *adminv1.ListDeletionReceiptsRequest, _ ...grpc.CallOption) (*adminv1.ListDeletionReceiptsResponse, error) {
	f.got = in
	return &adminv1.ListDeletionReceiptsResponse{Receipts: []*adminv1.DeletionReceipt{{ReceiptId: "del_1", DocId: in.GetDocId(),
		SourceId: "src_wiki", Stores: []*adminv1.StoreDeletion{{Store: "chunks", CompletedAt: timestamppb.Now()}},
		PendingStores: []string{"object_store"}, CreatedAt: timestamppb.Now()}}}, nil
}

func TestDeletionReceiptRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeReceipts{}
	e.server.Receipts = f
	audra, olga := e.authz.token(t, "audra", "console"), e.authz.token(t, "olga", "console")

	r := e.request(t, http.MethodGet, "/v1/admin/deletion-receipts?doc_id=credit-loan", audra, "", nil)
	rc := r.body["receipts"].([]any)[0].(map[string]any)
	if r.status != 200 || f.got.GetDocId() != "credit-loan" || rc["pending_stores"].([]any)[0] != "object_store" {
		t.Fatalf("审计岗查凭证: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/deletion-receipts", audra, "", nil); r.status != 400 {
		t.Fatalf("没有查询条件应 400: %d", r.status)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/deletion-receipts?doc_id=x", olga, "", nil); r.status != 403 {
		t.Fatalf("平台运维不能查: %d", r.status)
	}
}
