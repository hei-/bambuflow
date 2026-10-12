// Package grants 持有 retrieval 快照里的配置：租户可访问的数据源（租户授权）、租户的检索计划（pipeline）与
// 检索权限策略包。以 governance 为唯一事实来源，从 ConfigService 取 retrieval 的快照、验签后整份替换
// （governance 详设 5、6.2、7.1、9.2；retrieval 详设 10.2）。订阅 Watch，版本一变立即刷新；
// 另按固定间隔拉取兜底（Watch 断线时）。每次加载的结果经 Ack 上报。
//
// 策略包随快照签名下发，验签后在这里编译；新包编译失败时整份快照都不换，继续用当前的（governance 详设 5.3），
// 上报 policy_invalid。快照里没有策略包（没有生效的全局基线）时 Decide 返回错误，检索一律拒绝。
//
// fail-closed：启动后还没拿到过快照、或快照超过最长使用期限仍未刷新成功时，Sources 返回错误，调用方拒绝请求，
// readiness 为 false。刷新失败但还在期限内时继续用上一份并记错误日志（取消授权最迟在下一次成功刷新后生效）。
// 加载成功的快照写本地磁盘（Cache）；重启时先用盘上的一份（Restore），期限从它最后一次确认生效的时间算起。
package grants

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/hei-/bambuflow/libs/platform-go/policy"
)

// ErrUnavailable：没有可用的授权数据。
var ErrUnavailable = errors.New("租户授权不可用")

// ErrNoPolicy：快照里没有可用的策略包。
var ErrNoPolicy = errors.New("策略包缺失")

// Cache 是快照的本地磁盘缓存（libs/platform-go/snapshot.Cache）。
type Cache interface {
	Save(signed *configv1.GetSnapshotResponse) error
	Load() (*configv1.GetSnapshotResponse, time.Time, error)
}

// Client 是 ConfigService 的接口（取快照、订阅通知、上报加载结果）。
type Client interface {
	GetSnapshot(ctx context.Context, in *configv1.GetSnapshotRequest, opts ...grpc.CallOption) (*configv1.GetSnapshotResponse, error)
	Watch(ctx context.Context, in *configv1.WatchRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[configv1.WatchResponse], error)
	Ack(ctx context.Context, in *configv1.AckRequest, opts ...grpc.CallOption) (*configv1.AckResponse, error)
	// GetCandidate 只在评测副本上用（candidate.go）
	GetCandidate(ctx context.Context, in *configv1.GetCandidateRequest, opts ...grpc.CallOption) (*configv1.GetCandidateResponse, error)
}

type Watcher struct {
	Client Client
	// Open 验签并返回快照本体（libs/platform-go/snapshot）；验签不过的快照不加载
	Open func(ctx context.Context, resp *configv1.GetSnapshotResponse) (*configv1.GetSnapshotResponse, error)
	// Cache 为 nil 时不写盘（BF_SNAPSHOT_CACHE_DIR 为空）
	Cache Cache
	// Instance 是上报加载结果时的实例名
	Instance string
	// Every 是刷新间隔；MaxAge 是最长使用期限：超过它仍没刷新成功就不再放行
	Every, MaxAge time.Duration
	Log           *slog.Logger
	Now           func() time.Time

	mu       sync.RWMutex
	byTenant map[string][]string
	// 各租户生效的 pipeline（检索计划）；没有的租户按默认计划
	plans map[string]*configv1.PipelineConfig
	// 当前的策略包；nil 表示没有（检索一律拒绝）
	engine *policy.Engine
	// 灰度中的候选版本（governance 详设 3.2）：按租户的 pipeline 与策略包，各自带分流规则
	planCanaries   map[string]*configv1.Canary
	policyCanaries []policyCanary
	// 运行中 A/B 实验的检索计划变体，按租户（每个租户至多一个实验）；orchestrator 经 plan_version 选用
	planVariants map[string]*configv1.PipelineConfig
	version      string
	loadedAt     time.Time

	// 评测副本按请求取的候选配置（candidate.go），按版本缓存
	candMu sync.Mutex
	cands  map[string]candidate
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
	engines, err := w.compile(ctx, resp)
	if err != nil {
		w.ack(ctx, resp.GetSnapshotVersion(), "policy_invalid")
		return err
	}
	defer w.ack(ctx, resp.GetSnapshotVersion(), "")
	if w.set(resp, engines, w.Now()) {
		w.Log.InfoContext(ctx, "租户授权已更新", slog.String("snapshot_version", resp.GetSnapshotVersion()))
	}
	if w.Cache != nil {
		if err := w.Cache.Save(signed); err != nil {
			w.Log.WarnContext(ctx, "快照写本地磁盘失败", slog.Any("err", err))
		}
	}
	return nil
}

type policyCanary struct {
	canary *configv1.Canary
	engine *policy.Engine
}

// InCanary 判断用户是否分到灰度的候选版本：sha256(release_id + "\x00" + 用户) 的前 8 字节取模 100 小于 percent。
// orchestrator 用同样的规则（plans.in_canary），两边对同一用户得出相同的结论，不用在请求里互相告知。
func InCanary(releaseID, user string, percent int32) bool {
	sum := sha256.Sum256([]byte(releaseID + "\x00" + user))
	return binary.BigEndian.Uint64(sum[:8])%100 < uint64(max(percent, 0))
}

// compiled 是一次快照里编好的策略包：当前的与灰度中的候选包。
type compiled struct {
	engine   *policy.Engine
	canaries []policyCanary
}

// compile 编译快照里的策略包与灰度中的候选包；与当前已编译的版本相同时沿用，不重新编译。
func (w *Watcher) compile(ctx context.Context, resp *configv1.GetSnapshotResponse) (compiled, error) {
	w.mu.RLock()
	have := map[string]*policy.Engine{}
	if w.engine != nil {
		have[w.engine.Version()] = w.engine
	}
	for _, c := range w.policyCanaries {
		have[c.engine.Version()] = c.engine
	}
	w.mu.RUnlock()
	load := func(pb *configv1.PolicyBundle) (*policy.Engine, error) {
		if e, ok := have[pb.GetVersion()]; ok {
			return e, nil
		}
		b := policy.Bundle{Version: pb.GetVersion()}
		for _, m := range pb.GetModules() {
			b.Modules = append(b.Modules, policy.Module{Path: m.GetPath(), Source: m.GetSource()})
		}
		e, err := policy.Compile(ctx, b)
		if err != nil {
			w.Log.ErrorContext(ctx, "新策略包编译失败，继续用当前的", slog.String("policy_version", pb.GetVersion()), slog.Any("err", err))
			return nil, err
		}
		w.Log.InfoContext(ctx, "已加载策略包", slog.String("policy_version", e.Version()))
		return e, nil
	}
	var out compiled
	if pb := resp.GetPolicy(); pb != nil {
		e, err := load(pb)
		if err != nil {
			return compiled{}, err
		}
		out.engine = e
	}
	for _, c := range resp.GetCanaries() {
		if pb := c.GetPolicy(); pb != nil {
			e, err := load(pb)
			if err != nil {
				return compiled{}, err
			}
			out.canaries = append(out.canaries, policyCanary{c, e})
		}
	}
	return out, nil
}

// set 整份替换授权数据、检索计划、策略包与灰度，返回版本是否变了。
func (w *Watcher) set(resp *configv1.GetSnapshotResponse, c compiled, confirmed time.Time) bool {
	by := map[string][]string{}
	for _, g := range resp.GetSourceGrants() {
		by[g.GetTenant()] = append(by[g.GetTenant()], g.GetSourceId())
	}
	plans := map[string]*configv1.PipelineConfig{}
	for _, p := range resp.GetPipelines() {
		plans[p.GetTenant()] = p
	}
	planCanaries := map[string]*configv1.Canary{}
	for _, c := range resp.GetCanaries() {
		if c.GetPipeline() != nil {
			planCanaries[c.GetTenant()] = c
		}
	}
	variants := map[string]*configv1.PipelineConfig{}
	for _, e := range resp.GetExperiments() {
		if e.GetPipeline() != nil {
			variants[e.GetTenant()] = e.GetPipeline()
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	changed := w.version != resp.GetSnapshotVersion()
	w.planVariants = variants
	w.byTenant, w.plans, w.engine, w.version, w.loadedAt = by, plans, c.engine, resp.GetSnapshotVersion(), confirmed
	w.planCanaries, w.policyCanaries = planCanaries, c.canaries
	return changed
}

// Restore 用本地磁盘上的快照（启动时、Run 第一次刷新之前调用）；超过最长使用期限的照样载入，但 Sources 仍拒绝。
func (w *Watcher) Restore(ctx context.Context) error {
	if w.Cache == nil {
		return nil
	}
	resp, confirmed, err := w.Cache.Load()
	if err != nil {
		return err
	}
	engines, err := w.compile(ctx, resp)
	if err != nil {
		return err
	}
	w.set(resp, engines, confirmed)
	w.Log.InfoContext(ctx, "已载入本地磁盘上的租户授权", slog.String("snapshot_version", resp.GetSnapshotVersion()),
		slog.Time("confirmed_at", confirmed))
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

// Plan 是租户对这个用户生效的 pipeline，依次是：评测请求指定的 pipeline 候选；调用方要的版本（version，A/B 实验组）
// 是该租户运行中实验的变体时用它；分到灰度的用户用候选版本；生效的版本；都没有时为 nil（按默认计划）。
// 不认识的 version（实验刚停止、快照还没到）按后面几项，实际用的版本由调用方从响应的 plan_version 得知。
// 与 Sources 来自同一份快照：快照不可用时 Sources 已经拒绝了请求，这里不再重复判断期限。
func (w *Watcher) Plan(ctx context.Context, tenant, user, version string) *configv1.PipelineConfig {
	if c, ok := candidateFrom(ctx); ok && c.canary.GetPipeline() != nil && (c.canary.GetTenant() == "" || c.canary.GetTenant() == tenant) {
		return c.canary.GetPipeline()
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	if v, ok := w.planVariants[tenant]; ok && version != "" && v.GetVersionId() == version {
		return v
	}
	if c, ok := w.planCanaries[tenant]; ok && InCanary(c.GetReleaseId(), user, c.GetPercent()) {
		return c.GetPipeline()
	}
	return w.plans[tenant]
}

// Decide 用当前的策略包评估（search.Policy）：评测请求指定了策略包候选时用它，分到灰度的用户用候选包。
// 没有策略包时返回错误，调用方拒绝请求（fail-closed）。
func (w *Watcher) Decide(ctx context.Context, in policy.Input, granted []string) (policy.Decision, error) {
	if c, ok := candidateFrom(ctx); ok && c.engine != nil {
		return c.engine.Decide(ctx, in, granted)
	}
	w.mu.RLock()
	e := w.engine
	for _, c := range w.policyCanaries {
		if c.canary.GetTenant() == in.Tenant && InCanary(c.canary.GetReleaseId(), in.Sub, c.canary.GetPercent()) {
			e = c.engine
			break
		}
	}
	w.mu.RUnlock()
	if e == nil {
		return policy.Decision{}, ErrNoPolicy
	}
	return e.Decide(ctx, in, granted)
}

// Version 是当前策略包的版本；没有时为空。
func (w *Watcher) Version() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.engine == nil {
		return ""
	}
	return w.engine.Version()
}

// Ready 判断授权数据与策略包是否可用（readiness）。
func (w *Watcher) Ready() bool {
	_, _, err := w.Sources("")
	return err == nil && w.Version() != ""
}
