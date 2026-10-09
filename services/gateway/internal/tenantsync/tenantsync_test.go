package tenantsync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/grpc"

	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

type fakeClient struct {
	Client
	resp *configv1.GetSnapshotResponse
	acks []*configv1.AckRequest
}

func (f *fakeClient) GetSnapshot(context.Context, *configv1.GetSnapshotRequest, ...grpc.CallOption) (*configv1.GetSnapshotResponse, error) {
	return f.resp, nil
}

func (f *fakeClient) Ack(_ context.Context, in *configv1.AckRequest, _ ...grpc.CallOption) (*configv1.AckResponse, error) {
	f.acks = append(f.acks, in)
	return &configv1.AckResponse{}, nil
}

func snap(version string, tenants ...*configv1.TenantConfig) *configv1.GetSnapshotResponse {
	return &configv1.GetSnapshotResponse{SnapshotVersion: version, Tenants: tenants}
}

func TestRefreshSwapsRegistryAndKeepsOldOnBadConfig(t *testing.T) {
	f := &fakeClient{resp: snap("tenants:1", &configv1.TenantConfig{Tenant: "credit", VersionId: "cfv_1",
		Content: "tenant: credit\nrollout: general\nmember_groups: [credit-l2]\n"})}
	var opened bool
	w := &Watcher{Client: f, PlatformClients: []string{"console"}, Instance: "gw-1", Log: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		Open: func(_ context.Context, r *configv1.GetSnapshotResponse) (*configv1.GetSnapshotResponse, error) {
			opened = true
			return r, nil
		}}
	if w.Registry() != nil {
		t.Fatal("刷新之前没有注册表")
	}
	if err := w.Refresh(context.Background()); err != nil || !opened {
		t.Fatalf("%v", err)
	}
	if got, err := w.Registry().Resolve("console", "credit", []string{"credit-l2"}); err != nil || got.Name != "credit" {
		t.Fatalf("%v %v", got, err)
	}
	// 不合法的快照整份不加载，继续用上一份，并上报原因
	f.resp = snap("tenants:2", &configv1.TenantConfig{Tenant: "credit", VersionId: "cfv_2", Content: "tenant: credit\nrollout: beta\n"})
	if err := w.Refresh(context.Background()); err == nil {
		t.Fatal("不合法的配置应报错")
	}
	if _, err := w.Registry().Resolve("console", "credit", []string{"credit-l2"}); err != nil {
		t.Fatalf("应保留上一份: %v", err)
	}
	last := f.acks[len(f.acks)-1]
	if last.GetLoaded() || last.GetReason() != "invalid_config" || last.GetService() != "gateway" {
		t.Fatalf("%v", last)
	}
	// 验签不过同样不加载
	w.Open = func(context.Context, *configv1.GetSnapshotResponse) (*configv1.GetSnapshotResponse, error) {
		return nil, errors.New("签名不对")
	}
	if err := w.Refresh(context.Background()); err == nil || f.acks[len(f.acks)-1].GetReason() != "signature_invalid" {
		t.Fatalf("%v", err)
	}
}
