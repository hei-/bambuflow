// Package tenantsync 持有 gateway 当前生效的租户配置：以 governance 为唯一事实来源，从 ConfigService 取 gateway 的
// 快照、验签、用与 governance 同一份校验代码（libs/platform-go/tenancy）解析后整份原子替换（governance 详设 4、7.1、8.1）。
// 订阅 Watch，版本一变立即刷新；另按固定间隔拉取兜底。每次加载的结果经 Ack 上报。
//
// 启动时拿不到快照就不启动（设计 8.1：新实例拿不到快照不接流量）。之后刷新失败继续用上一份：governance 故障不影响问答
// （设计 1.4）；收紧类变更（移出成员群组）在下一次成功刷新后生效。本地磁盘缓存上一份快照还没有做。
package tenantsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/tenancy"
)

const service = "gateway"

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
	// PlatformClients 是平台级客户端（BF_PLATFORM_CLIENTS），不属于任何租户
	PlatformClients []string
	Instance        string
	Log             *slog.Logger

	live    atomic.Pointer[tenancy.Registry]
	mu      sync.Mutex
	version string
}

// Registry 返回当前生效的租户配置；Refresh 成功之前为 nil（启动时先 Refresh 成功才对外服务）。
func (w *Watcher) Registry() *tenancy.Registry { return w.live.Load() }

// Refresh 拉一次快照，校验通过后整份替换；任何一份配置不合法就整份不加载，继续用上一份。
func (w *Watcher) Refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	signed, err := w.Client.GetSnapshot(ctx, &configv1.GetSnapshotRequest{Service: service})
	if err != nil {
		return err
	}
	resp, err := w.Open(ctx, signed)
	if err != nil {
		w.ack(ctx, signed.GetSnapshotVersion(), "signature_invalid")
		return err
	}
	version := resp.GetSnapshotVersion()
	if version == "" {
		return errors.New("快照没有版本号")
	}
	reg, err := build(resp.GetTenants(), w.PlatformClients)
	if err != nil {
		w.ack(ctx, version, "invalid_config")
		return err
	}
	w.mu.Lock()
	changed := w.version != version
	w.version = version
	w.live.Store(reg)
	w.mu.Unlock()
	if changed {
		w.Log.InfoContext(ctx, "租户配置已更新", slog.String("snapshot_version", version), slog.Int("tenants", len(resp.GetTenants())))
	}
	w.ack(ctx, version, "")
	return nil
}

func build(cfgs []*configv1.TenantConfig, platformClients []string) (*tenancy.Registry, error) {
	tenants := make([]tenancy.Tenant, 0, len(cfgs))
	for _, c := range cfgs {
		t, err := tenancy.Parse(c.GetTenant(), []byte(c.GetContent()))
		if err != nil {
			return nil, fmt.Errorf("配置版本 %s: %w", c.GetVersionId(), err)
		}
		tenants = append(tenants, t)
	}
	return tenancy.New(tenants, platformClients)
}

// ack 上报加载结果；reason 为空表示已加载。上报失败只记日志。
func (w *Watcher) ack(ctx context.Context, version, reason string) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := w.Client.Ack(ctx, &configv1.AckRequest{Service: service, InstanceId: w.Instance, SnapshotVersion: version,
		Loaded: reason == "", Reason: reason}); err != nil {
		w.Log.WarnContext(ctx, "上报快照加载结果失败", slog.Any("err", err))
	}
}

// Watch 订阅版本变化的通知，收到就刷新；断线后按退避重连（上限 30 秒），直到 ctx 结束。
func (w *Watcher) Watch(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		w.mu.Lock()
		loaded := w.version
		w.mu.Unlock()
		stream, err := w.Client.Watch(ctx, &configv1.WatchRequest{Service: service, InstanceId: w.Instance, Loaded: loaded})
		if err == nil {
			for {
				if _, err = stream.Recv(); err != nil {
					break
				}
				backoff = time.Second
				if err := w.Refresh(ctx); err != nil {
					w.Log.ErrorContext(ctx, "按通知刷新租户配置失败，继续用上一份", slog.Any("err", err))
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

// Run 每隔 every 刷新一次（Watch 断线时兜底），直到 ctx 结束。
func (w *Watcher) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := w.Refresh(ctx); err != nil && ctx.Err() == nil {
			w.Log.ErrorContext(ctx, "刷新租户配置失败，继续用上一份", slog.Any("err", err))
		}
	}
}
