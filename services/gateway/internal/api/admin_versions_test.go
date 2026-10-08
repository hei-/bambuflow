package api

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
)

// fakeVersions 模拟 indexing：同一版本第二次申请返回已有的操作；复核总是成功（自批由 indexing 判断）。
type fakeVersions struct {
	adminv1.IndexVersionAdminServiceClient
	activated *adminv1.ActivateIndexVersionRequest
	approved  string
}

var t0 = timestamppb.New(time.Date(2026, 10, 7, 1, 0, 0, 0, time.UTC))

func (f *fakeVersions) ListIndexVersions(context.Context, *adminv1.ListIndexVersionsRequest, ...grpc.CallOption) (*adminv1.ListIndexVersionsResponse, error) {
	return &adminv1.ListIndexVersionsResponse{Versions: []*adminv1.IndexVersion{{
		Version: "ix-2", Status: adminv1.IndexVersionStatus_INDEX_VERSION_STATUS_RETIRED, EmbeddingModel: "bge-m3@1",
		ChunkerVersion: "c1", DocCount: 12, MissingDocCount: 1, CreatedAt: t0,
	}}}, nil
}

func (f *fakeVersions) ActivateIndexVersion(_ context.Context, in *adminv1.ActivateIndexVersionRequest, _ ...grpc.CallOption) (*adminv1.ActivateIndexVersionResponse, error) {
	if in.GetVersion() == "ix-broken" {
		return nil, grpcmw.Reason(codes.FailedPrecondition, "activation_refused", "清单不全")
	}
	created := f.activated == nil
	f.activated = in
	return &adminv1.ActivateIndexVersionResponse{Created: created, Operation: &adminv1.Operation{
		OperationId: "op_1a", Action: "activate_index_version", TargetVersion: in.GetVersion(), LiveVersion: "ix-3",
		Reason: in.GetReason(), RequestedBy: "olga", RequestedAt: t0, ExpiresAt: t0, Status: adminv1.OperationStatus_OPERATION_STATUS_PENDING,
	}}, nil
}

func (f *fakeVersions) ApproveOperation(ctx context.Context, in *adminv1.ApproveOperationRequest, _ ...grpc.CallOption) (*adminv1.ApproveOperationResponse, error) {
	f.approved = in.GetOperationId()
	return &adminv1.ApproveOperationResponse{Operation: &adminv1.Operation{
		OperationId: in.GetOperationId(), Status: adminv1.OperationStatus_OPERATION_STATUS_EXECUTED, ApprovedBy: "oscar", ApprovedAt: t0,
		RequestedAt: t0, ExpiresAt: t0,
	}}, nil
}

func TestIndexVersionRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeVersions{}
	e.server.IndexVersions = f
	olga, oscar := e.authz.token(t, "olga", "console"), e.authz.token(t, "oscar", "console")
	l := e.request(t, http.MethodGet, "/v1/admin/index-versions", olga, "", nil)
	vs, _ := l.body["versions"].([]any)
	if l.status != 200 || len(vs) != 1 || vs[0].(map[string]any)["status"] != "retired" || vs[0].(map[string]any)["missing_doc_count"] != 1.0 {
		t.Fatalf("版本列表: %d %s", l.status, l.raw)
	}
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }
	a := e.request(t, http.MethodPost, "/v1/admin/index-versions/ix-2/activate", olga, `{"reason":"回滚误发布"}`, key("a"))
	if a.status != 202 || a.body["status"] != "pending" || a.body["operation_id"] != "op_1a" || f.activated.GetReason() != "回滚误发布" {
		t.Fatalf("申请切换应只创建待复核操作: %d %s", a.status, a.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/index-versions/ix-2/activate", olga, `{"reason":"再来一次"}`, key("b")); r.status != 200 {
		t.Fatalf("已有待复核操作应返回 200: %d", r.status)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/index-versions/ix-broken/activate", olga, `{"reason":"x"}`, key("c")); r.status != 409 ||
		r.body["type"] != "activation_refused" {
		t.Fatalf("清单不全应 409 activation_refused: %d %s", r.status, r.raw)
	}
	ap := e.request(t, http.MethodPost, "/v1/admin/index-versions/operations/op_1a/approve", oscar, "", key("d"))
	if ap.status != 200 || ap.body["status"] != "executed" || ap.body["approved_by"] != "oscar" || f.approved != "op_1a" {
		t.Fatalf("复核: %d %s", ap.status, ap.raw)
	}
	n := 0
	for name, c := range map[string]struct {
		method, path, token string
		status              int
	}{
		"审计岗不能看":       {http.MethodGet, "/v1/admin/index-versions", e.authz.token(t, "audra", "console"), 403},
		"非法的操作 ID":     {http.MethodPost, "/v1/admin/index-versions/operations/op_XYZ/approve", oscar, 404},
		"非法的版本号":       {http.MethodPost, "/v1/admin/index-versions/a%20b/activate", olga, 404},
		"多余字段":         {http.MethodPost, "/v1/admin/index-versions/operations/op_1a/approve", oscar, 400},
		"未登记的 rebuild": {http.MethodPost, "/v1/admin/index-rebuilds", olga, 404},
	} {
		n++
		body := ""
		if name == "多余字段" {
			body = `{"force":true}`
		}
		if r := e.request(t, c.method, c.path, c.token, body, key(fmt.Sprintf("k-%d", n))); r.status != c.status {
			t.Errorf("%s: 得到 %d %s", name, r.status, r.raw)
		}
	}
}
