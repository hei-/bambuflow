package grants

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"

	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
)

type fakeConfig struct {
	Client
	resp *configv1.GetSnapshotResponse
	err  error
	asks []string
	acks []*configv1.AckRequest
}

func (f *fakeConfig) Ack(_ context.Context, in *configv1.AckRequest, _ ...grpc.CallOption) (*configv1.AckResponse, error) {
	f.acks = append(f.acks, in)
	return &configv1.AckResponse{}, nil
}

// opened 当作验签通过；带 SigningKeyId "bad" 的当作验签失败
func opened(_ context.Context, r *configv1.GetSnapshotResponse) (*configv1.GetSnapshotResponse, error) {
	if r.GetSigningKeyId() == "bad" {
		return nil, errors.New("签名无效")
	}
	return r, nil
}

func (f *fakeConfig) GetSnapshot(_ context.Context, in *configv1.GetSnapshotRequest, _ ...grpc.CallOption) (*configv1.GetSnapshotResponse, error) {
	f.asks = append(f.asks, in.GetService())
	return f.resp, f.err
}

func TestSourcesFailClosed(t *testing.T) {
	now := time.Date(2026, 10, 7, 9, 0, 0, 0, time.UTC)
	f := &fakeConfig{resp: &configv1.GetSnapshotResponse{SnapshotVersion: "grants:1", SourceGrants: []*configv1.SourceGrant{
		{Tenant: "credit", SourceId: "src_a"}, {Tenant: "credit", SourceId: "src_b"}, {Tenant: "risk", SourceId: "src_a"},
	}}}
	w := &Watcher{Client: f, Open: opened, Instance: "r-1", Every: time.Second, MaxAge: 5 * time.Minute, Log: logging.New(&bytes.Buffer{}, "t", slog.LevelInfo),
		Now: func() time.Time { return now }}
	if _, _, err := w.Sources("credit"); !errors.Is(err, ErrUnavailable) || w.Ready() {
		t.Fatal("还没拿到快照时应拒绝")
	}
	if err := w.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, v, err := w.Sources("credit")
	if err != nil || v != "grants:1" || !slices.Equal(got, []string{"src_a", "src_b"}) || f.asks[0] != "retrieval" {
		t.Fatalf("授权: %v %s %v", got, v, err)
	}
	if got, _, _ := w.Sources("hr"); len(got) != 0 {
		t.Fatalf("没有授权的租户应为空: %v", got)
	}
	// 取消授权：下一次刷新后立即生效
	f.resp = &configv1.GetSnapshotResponse{SnapshotVersion: "grants:2", SourceGrants: []*configv1.SourceGrant{{Tenant: "credit", SourceId: "src_a"}}}
	if err := w.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := w.Sources("credit"); !slices.Equal(got, []string{"src_a"}) {
		t.Fatalf("取消授权应生效: %v", got)
	}
	// 刷新失败：期限内继续用上一份，超过期限拒绝
	f.err = errors.New("governance 不可用")
	if w.Refresh(context.Background()) == nil {
		t.Fatal("应返回错误")
	}
	now = now.Add(4 * time.Minute)
	if _, _, err := w.Sources("credit"); err != nil {
		t.Fatalf("期限内应继续可用: %v", err)
	}
	now = now.Add(2 * time.Minute)
	if _, _, err := w.Sources("credit"); !errors.Is(err, ErrUnavailable) || w.Ready() {
		t.Fatal("超过期限应拒绝")
	}
}

func TestUnsignedSnapshotIsNotLoadedAndAcked(t *testing.T) {
	now := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)
	f := &fakeConfig{resp: &configv1.GetSnapshotResponse{SnapshotVersion: "grants:1",
		SourceGrants: []*configv1.SourceGrant{{Tenant: "credit", SourceId: "src_a"}}}}
	w := &Watcher{Client: f, Open: opened, Instance: "r-1", Every: time.Second, MaxAge: 5 * time.Minute,
		Log: logging.New(&bytes.Buffer{}, "t", slog.LevelInfo), Now: func() time.Time { return now }}
	if err := w.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.acks) != 1 || !f.acks[0].GetLoaded() || f.acks[0].GetSnapshotVersion() != "grants:1" || f.acks[0].GetInstanceId() != "r-1" {
		t.Fatalf("加载成功应上报: %v", f.acks)
	}
	// 验签不过：保留旧授权，上报失败原因
	f.resp = &configv1.GetSnapshotResponse{SnapshotVersion: "grants:2", SigningKeyId: "bad"}
	if w.Refresh(context.Background()) == nil {
		t.Fatal("验签不过应返回错误")
	}
	if got, v, _ := w.Sources("credit"); v != "grants:1" || len(got) != 1 {
		t.Fatalf("应保留旧授权: %v %s", got, v)
	}
	if last := f.acks[len(f.acks)-1]; last.GetLoaded() || last.GetReason() != "signature_invalid" {
		t.Fatalf("应上报验签失败: %v", last)
	}
}
