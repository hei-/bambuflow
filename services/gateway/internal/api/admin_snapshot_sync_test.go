package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

type fakeSnapshotSync struct {
	adminv1.SnapshotSyncAdminServiceClient
}

func (fakeSnapshotSync) ListSnapshotSync(context.Context, *adminv1.ListSnapshotSyncRequest, ...grpc.CallOption) (*adminv1.ListSnapshotSyncResponse, error) {
	return &adminv1.ListSnapshotSyncResponse{Services: []*adminv1.ServiceSync{{Service: "gateway", CurrentVersion: "tenants:abc",
		Instances: []*adminv1.InstanceSync{{InstanceId: "gw-2", SnapshotVersion: "tenants:old", Loaded: true, AckedAt: timestamppb.Now()}}}}}, nil
}

func TestSnapshotSyncRoute(t *testing.T) {
	e := newEnv(t)
	e.server.SnapshotSync = fakeSnapshotSync{}
	olga, audra := e.authz.token(t, "olga", "console"), e.authz.token(t, "audra", "console")

	r := e.request(t, http.MethodGet, "/v1/admin/snapshot-sync", olga, "", nil)
	inst := r.body["services"].([]any)[0].(map[string]any)["instances"].([]any)[0].(map[string]any)
	if r.status != 200 || inst["in_sync"] != false || inst["snapshot_version"] != "tenants:old" {
		t.Fatalf("平台运维查看: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/snapshot-sync", audra, "", nil); r.status != 403 {
		t.Fatalf("审计岗不能看: %d", r.status)
	}
}
