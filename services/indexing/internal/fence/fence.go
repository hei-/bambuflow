// Package fence 是撤权写入方在读副本下要做的几步（元数据库可用性详设 4.2、5.2）：
//
//  1. 收到权限或删除变更先登记待生效撤权名单（Hold）；
//  2. 在主库提交（调用方的事务）；
//  3. 把撤权位点 W 抬到提交位点 P，4. 等权限读副本回放到 P（Settle）；
//  5. 索引已在第 2 步的同一事务里更新（P0 的索引与元数据同库）；6. 调用方发 ChunkAclChanged；
//  7. 移出名单（Release）。
//
// 第 3 步成功时，超过 10 秒仍未追上的副本不再等：它的 r < W，retrieval 读到它会拒绝。第 3 步失败（撤权位点所在的
// Redis 不可用）时必须等全部副本追上，并且至少等 300 ms（本机单调时钟），保证 retrieval 兜底用的同步备库位点样本都晚于提交；
// 等不到就返回错误，事件不确认、之后重做。
package fence

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hei-/bambuflow/libs/platform-go/revocation"
	"github.com/hei-/bambuflow/libs/platform-go/telemetry"
)

// Querier 是能查一行的连接（*pgxpool.Pool）。
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Replica 是一个权限读副本。
type Replica struct {
	Name string
	DB   Querier
}

type Fence struct {
	// Primary 是主库（读提交位点）
	Primary  Querier
	Replicas []Replica
	Store    *revocation.Store
	Log      *slog.Logger

	// 以下为零时取详设第 9 节的值
	Timeout  time.Duration // 抬位点成功时等副本的上限，10 s
	Poll     time.Duration // 查副本回放位点的间隔，50 ms
	Settle   time.Duration // 抬位点失败时至少等的时长，300 ms
	MaxBlock time.Duration // 抬位点失败时等全部副本的上限，超过返回错误、事件重做，2 min

	lag     atomic.Uint64
	pending atomic.Int64
	oldest  atomic.Uint64
}

func (f *Fence) dur(v, def time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return def
}

// Hold 登记名单（已有的续期）。写不进时只记日志：名单只是额外一道拦截，正确性靠撤权位点与等待（详设 5.2）。
func (f *Fence) Hold(ctx context.Context, members ...string) {
	if f == nil {
		return
	}
	if err := f.Store.Hold(ctx, members...); err != nil {
		f.Log.WarnContext(ctx, "登记待生效撤权名单失败", slog.Any("err", err))
		failures.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "hold")))
	}
}

// Release 移出名单。失败时条目到期自然消失，期间这些文档多隐藏一会儿（按更严处理）。
func (f *Fence) Release(ctx context.Context, members ...string) {
	if f == nil {
		return
	}
	if err := f.Store.Release(context.WithoutCancel(ctx), members...); err != nil {
		f.Log.WarnContext(ctx, "移出待生效撤权名单失败，条目到期后自然消失", slog.Any("err", err))
		failures.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "release")))
	}
}

// Revoke 按顺序做一次权限或删除变更：登记名单 → commit（主库事务）→ 抬撤权位点并等权限读副本 → notify → 移出名单。
// 提交或等待失败时条目留在名单里（重做时续期），返回错误、事件之后重做；重做时结果多半是“未变化”，等待照样做一遍。
// f 为 nil 时只做 commit 与 notify（测试）。
func (f *Fence) Revoke(ctx context.Context, members []string, commit func() error, notify func()) error {
	start := time.Now()
	f.Hold(ctx, members...)
	if err := commit(); err != nil {
		return err
	}
	if err := f.Commit(ctx, true); err != nil {
		return err
	}
	notify()
	f.Release(ctx, members...)
	revokeDuration.Record(ctx, time.Since(start).Seconds())
	return nil
}

// ErrNotSettled 表示撤权位点没抬上，且没等到全部副本追上：调用方返回不可用，事件之后重做。
var ErrNotSettled = errors.New("撤权未能确认在全部权限读副本上可见")

// Commit 在主库提交之后调用：抬撤权位点，wait 为 true 时再等副本回放（快速通道）；内容写入传 false，
// 只抬位点不等（被采纳的读取已满足 r ≥ W ≥ P，等待只为可用性）。抬位点失败时两种都按兜底等待。
func (f *Fence) Commit(ctx context.Context, wait bool) error {
	if f == nil {
		return nil
	}
	start := time.Now()
	p, err := lsn(ctx, f.Primary, revocation.CommitSQL)
	if err != nil {
		return fmt.Errorf("读主库提交位点: %w", err)
	}
	raised := true
	if err := f.Store.Raise(ctx, p); err != nil {
		raised = false
		f.Log.ErrorContext(ctx, "抬撤权位点失败，改为等全部权限读副本追上", slog.Any("err", err))
		failures.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "raise")))
	}
	if raised && !wait {
		return nil
	}
	outcome, err := f.await(ctx, p, raised, start)
	waited.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("outcome", outcome)))
	return err
}

func (f *Fence) await(ctx context.Context, p revocation.LSN, raised bool, start time.Time) (string, error) {
	// 等待上限从开始等算起（抬位点失败时 Redis 的连接重试可能已经花了一些时间）；兜底的最短等待从提交算起
	limit, waitFrom := f.dur(f.MaxBlock, 2*time.Minute), time.Now()
	if raised {
		limit = f.dur(f.Timeout, 10*time.Second)
	}
	tick := time.NewTicker(f.dur(f.Poll, 50*time.Millisecond))
	defer tick.Stop()
	for {
		behind := f.behind(ctx, p)
		if len(behind) == 0 {
			if raised {
				return "caught_up", nil
			}
			// 兜底：至少等够 300 ms 再继续，retrieval 用到的同步备库位点样本都取自提交之后
			if rest := f.dur(f.Settle, 300*time.Millisecond) - time.Since(start); rest > 0 {
				select {
				case <-ctx.Done():
					return "canceled", ctx.Err()
				case <-time.After(rest):
				}
			}
			return "fallback", nil
		}
		if time.Since(waitFrom) > limit {
			if raised {
				// 落后的副本 r < W，retrieval 会拒绝它的读取，不必再等
				f.Log.WarnContext(ctx, "权限读副本 10 秒内没有追上撤权位点，不再等待", slog.String("replicas", strings.Join(behind, ",")),
					slog.String("lsn", p.String()))
				return "timeout", nil
			}
			return "not_settled", fmt.Errorf("%w：%s", ErrNotSettled, strings.Join(behind, ","))
		}
		select {
		case <-ctx.Done():
			return "canceled", ctx.Err()
		case <-tick.C:
		}
	}
}

// behind 返回回放位点还没到 p 的副本；读不到位点的也算没追上。
func (f *Fence) behind(ctx context.Context, p revocation.LSN) []string {
	var out []string
	for _, r := range f.Replicas {
		qctx, cancel := context.WithTimeout(ctx, time.Second)
		got, err := lsn(qctx, r.DB, revocation.ReplaySQL)
		cancel()
		if err != nil || got < p {
			out = append(out, r.Name)
		}
	}
	return out
}

// Run 每秒采一次权限读副本的回放延迟（供 Lag 用），每 5 秒检查一次撤权位点：缺失时（Redis 故障切换丢了）用主库当前位点重建
// （详设 4.3），在此之前 retrieval 按兜底条件读。
func (f *Fence) Run(ctx context.Context) {
	for i := 0; ; i++ {
		if i%5 == 0 {
			f.ensure(ctx)
		}
		f.sampleLag(ctx)
		if n, oldest, err := f.Store.Stats(ctx); err == nil {
			f.pending.Store(n)
			f.oldest.Store(math.Float64bits(oldest.Seconds()))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (f *Fence) ensure(ctx context.Context) {
	p, err := lsn(ctx, f.Primary, revocation.CommitSQL)
	if err != nil {
		return
	}
	ok, err := f.Store.Init(ctx, p)
	switch {
	case err != nil:
		f.Log.WarnContext(ctx, "检查撤权位点失败", slog.Any("err", err))
	case ok:
		f.Log.WarnContext(ctx, "撤权位点缺失，已用主库当前位点重建", slog.String("lsn", p.String()))
	}
}

func (f *Fence) sampleLag(ctx context.Context) {
	var lag float64
	for _, r := range f.Replicas {
		qctx, cancel := context.WithTimeout(ctx, time.Second)
		var v float64
		if err := r.DB.QueryRow(qctx, revocation.LagSQL).Scan(&v); err == nil {
			lag = max(lag, v)
		}
		cancel()
	}
	f.lag.Store(math.Float64bits(lag))
}

// Lag 返回最近一次采到的各权限读副本中最大的回放延迟（秒，数据库报告的值；读不到的副本不计）。
// 只用于写入限速（详设 7.1），不参与正确性判断。
func (f *Fence) Lag() float64 {
	if f == nil {
		return 0
	}
	return math.Float64frombits(f.lag.Load())
}

func lsn(ctx context.Context, db Querier, sql string) (revocation.LSN, error) {
	var s *string
	if err := db.QueryRow(ctx, sql).Scan(&s); err != nil {
		return 0, err
	}
	if s == nil {
		return 0, errors.New("数据库没有给出复制位点")
	}
	return revocation.ParseLSN(*s)
}

var (
	meter     = telemetry.Meter("bambuflow/indexing")
	waited, _ = meter.Float64Histogram("indexing.revocation.wait", metric.WithUnit("s"),
		metric.WithDescription("撤权提交后等权限读副本回放到提交位点的时间"))
	failures, _ = meter.Int64Counter("indexing.revocation.failures",
		metric.WithDescription("撤权位点与待生效撤权名单的读写失败次数"))
	revokeDuration, _ = meter.Float64Histogram("indexing.revocation.duration", metric.WithUnit("s"),
		metric.WithDescription("快速通道处理一条权限或删除变更（登记名单到移出名单）的时间"))
)

// Gauges 注册待生效撤权名单的条目数与最老条目年龄（详设 7.2：最老条目超过 5 分钟说明撤权写入卡住）。
func (f *Fence) Gauges() {
	_, _ = meter.Int64ObservableGauge("indexing.revocation.pending", metric.WithDescription("待生效撤权名单的条目数"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(f.pending.Load())
			return nil
		}))
	_, _ = meter.Float64ObservableGauge("indexing.revocation.pending_oldest", metric.WithUnit("s"),
		metric.WithDescription("待生效撤权名单里最老条目从登记起的时长"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(math.Float64frombits(f.oldest.Load()))
			return nil
		}))
}
