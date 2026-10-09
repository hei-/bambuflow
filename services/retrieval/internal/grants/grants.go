// Package grants 持有租户可访问的数据源（租户授权）：以 governance 为唯一事实来源，从 ConfigService 取 retrieval 的
// 快照、验签后整份替换（governance 详设 6.2、7.1、9.2；retrieval 详设 10.2）。订阅 Watch，版本一变立即刷新；
// 另按固定间隔拉取兜底（Watch 断线时）。每次加载的结果经 Ack 上报。
//
// fail-closed：启动后还没拿到过快照、或快照超过最长使用期限仍未刷新成功时，Sources 返回错误，调用方拒绝请求，
// readiness 为 false。刷新失败但还在期限内时继续用上一份并记错误日志（取消授权最迟在下一次成功刷新后生效）。
package grants

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"

	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

// ErrUnavailable：没有可用的授权数据。
var ErrUnavailable = errors.New("租户授权不可用")

// Client 是 ConfigService 的接口（取快照、订阅通知、上报加载结果）。
type Client interface {
	GetSnapshot(ctx context.Context, in *configv1.GetSnapshotRequest, opts ...grpc.CallOption) (*configv1.GetSnapshotResponse, error)
	Watch(ctx context.Context, in *configv1.WatchRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[configv1.WatchResponse], error)
	Ack(ctx context.Context, in *configv1.AckRequest, opts ...grpc.CallOption) (*configv1.AckResponse, error)
}

type Watcher struct {
	Client Client
	// Open 验签并返回快照本体（libs/platform-go/snapshot）；验签不过的快照不加载
	Open func(ctx context.Context, resp *configv1.GetSnapshotResponse) (*configv1.GetSnapshotResponse, error)
	// Instance 是上报加载结果时的实例名
	Instance string
	// Every 是刷新间隔；MaxAge 是最长使用期限：超过它仍没刷新成功就不再放行
	Every, MaxAge time.Duration
	Log           *slog.Logger
	Now           func() time.Time

	mu       sync.RWMutex
	byTenant map[string][]string
	version  string
	loadedAt time.Time
}

// Refresh 拉一次快照并整份替换。
func (w *Watcher) Refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	signed, err := w.Client.GetSnapshot(ctx, &configv1.GetSnapshotRequest{Service: "retrieval"})
	if err != nil {
		return err
	}
	resp, err := w.Open(ctx, signed)
	if err != nil {
		w.ack(ctx, signed.GetSnapshotVersion(), "signature_invalid")
		return err
	}
	if resp.GetSnapshotVersion() == "" {
		return errors.New("快照没有版本号")
	}
	defer w.ack(ctx, resp.GetSnapshotVersion(), "")
	by := map[string][]string{}
	for _, g := range resp.GetSourceGrants() {
		by[g.GetTenant()] = append(by[g.GetTenant()], g.GetSourceId())
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.version != resp.GetSnapshotVersion() {
		w.Log.InfoContext(ctx, "租户授权已更新", slog.String("snapshot_version", resp.GetSnapshotVersion()), slog.Int("tenants", len(by)))
	}
	w.byTenant, w.version, w.loadedAt = by, resp.GetSnapshotVersion(), w.Now()
	return nil
}

// ack 上报加载结果；reason 为空表示已加载。上报失败只记日志，不影响授权的使用。
func (w *Watcher) ack(ctx context.Context, version, reason string) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := w.Client.Ack(ctx, &configv1.AckRequest{Service: "retrieval", InstanceId: w.Instance, SnapshotVersion: version,
		Loaded: reason == "", Reason: reason}); err != nil {
		w.Log.WarnContext(ctx, "上报快照加载结果失败", slog.Any("err", err))
	}
}

// Watch 订阅版本变化的通知，收到就刷新；断线后按退避重连（上限 30 秒），直到 ctx 结束。
func (w *Watcher) Watch(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		w.mu.RLock()
		loaded := w.version
		w.mu.RUnlock()
		stream, err := w.Client.Watch(ctx, &configv1.WatchRequest{Service: "retrieval", InstanceId: w.Instance, Loaded: loaded})
		if err == nil {
			for {
				if _, err = stream.Recv(); err != nil {
					break
				}
				backoff = time.Second
				if err := w.Refresh(ctx); err != nil {
					w.Log.ErrorContext(ctx, "按通知刷新租户授权失败", slog.Any("err", err))
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		w.Log.WarnContext(ctx, "快照通知断开，稍后重连", slog.Any("err", err), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

// Run 立即刷新一次，之后每隔 Every 刷新（兜底），直到 ctx 结束。
func (w *Watcher) Run(ctx context.Context) {
	t := time.NewTicker(w.Every)
	defer t.Stop()
	for {
		if err := w.Refresh(ctx); err != nil && ctx.Err() == nil {
			w.Log.ErrorContext(ctx, "刷新租户授权失败，继续用上一份", slog.Any("err", err), slog.Time("loaded_at", w.loaded()))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (w *Watcher) loaded() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.loadedAt
}

// Sources 返回租户被授权的数据源与快照版本；没有授权的租户返回空列表。
func (w *Watcher) Sources(tenant string) ([]string, string, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.byTenant == nil || w.Now().Sub(w.loadedAt) > w.MaxAge {
		return nil, "", ErrUnavailable
	}
	return w.byTenant[tenant], w.version, nil
}

// Ready 判断授权数据是否可用（readiness）。
func (w *Watcher) Ready() bool {
	_, _, err := w.Sources("")
	return err == nil
}
