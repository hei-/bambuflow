package store

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/hei-/bambuflow/libs/platform-go/revocation"
	"github.com/hei-/bambuflow/libs/platform-go/telemetry"
)

// 权限读取（元数据库可用性详设 3.3、4.1）：只读权限读副本组，每次批量读按下面的顺序：
//
//  1. 在同一次 Redis 往返里读撤权位点 W 与待生效撤权名单；
//  2. 在副本上开一个读已提交的只读事务，先读回放位点 r，再读数据（第二条语句的快照晚于 r）；
//  3. 只有 r ≥ W 才采纳，否则换一个副本；全部不满足时返回 ErrNoFreshReplica（fail-closed，不改读主库）。
//
// W 读不到（Redis 不可用，或故障切换后缺失）时改用更严的 r ≥ S，S 是同步备库的已接收位点（Standbys）；
// S 也没有时 fail-closed。名单读不到时只少一道拦截，正确性仍由位点保证。

// ErrNoFreshReplica 表示没有回放位点满足要求的权限读副本（或副本都不可用）。
var ErrNoFreshReplica = errors.New("没有追上撤权位点的权限读副本")

// beginner 是能开事务的连接池（*pgxpool.Pool）。
type beginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

type replica struct {
	name string
	db   beginner
	// 最近一次读到的回放位点与当时要求的位点，供指标用
	replay, need atomic.Uint64
}

// floor 给出兜底条件的 S；ok 为 false 时没有可用的 S。
type floor interface {
	LSN() (revocation.LSN, bool)
}

type permissions struct {
	replicas []*replica
	rev      *revocation.Store
	standby  floor
	log      *slog.Logger
}

// read 按上面的顺序读一次。docs 是这次涉及的文档（名单按它们点名查），fn 在采纳的副本事务里读数据，
// held 是读取前取到的名单（文档与全部分区）。
func (p *permissions) read(ctx context.Context, docs []string, fn func(q pgx.Tx, held map[string]bool) error) error {
	start := time.Now()
	members := make([]string, len(docs))
	for i, d := range docs {
		members[i] = revocation.DocMember(d)
	}
	view, err := p.rev.Read(ctx, members)
	need, held := view.Fence, view.Held
	if err != nil {
		if !errors.Is(err, revocation.ErrNoFence) {
			p.log.WarnContext(ctx, "读撤权位点与待生效撤权名单失败，改用同步备库位点", slog.Any("err", err))
			held = map[string]bool{}
		}
		s, ok := p.standby.LSN()
		if !ok {
			permReads.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "no_fence")))
			return ErrNoFreshReplica
		}
		need = s
		permReads.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "fallback")))
	}
	n := len(p.replicas)
	off := sticky(ctx, n)
	for i := range n {
		r := p.replicas[(off+i)%n]
		ok, err := r.run(ctx, need, func(tx pgx.Tx) error { return fn(tx, held) })
		switch {
		case err != nil:
			p.log.WarnContext(ctx, "权限读副本读取失败，换一个", slog.String("replica", r.name), slog.Any("err", err))
			permReads.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "error")))
		case !ok:
			permReads.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "stale")))
		default:
			permReads.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "accepted")))
			permDuration.Record(ctx, time.Since(start).Seconds())
			return nil
		}
	}
	permReads.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "unavailable")))
	return ErrNoFreshReplica
}

// run 在副本上开读已提交的只读事务：第一条语句读回放位点 r，r ≥ need 时第二条起读数据。
// 读已提交下每条语句各取快照，数据快照晚于 r，至少包含到 r 为止的全部提交。
func (r *replica) run(ctx context.Context, need revocation.LSN, fn func(pgx.Tx) error) (bool, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var raw *string
	if err := tx.QueryRow(ctx, revocation.ReplaySQL).Scan(&raw); err != nil {
		return false, err
	}
	if raw == nil {
		return false, errors.New("副本没有给出回放位点")
	}
	got, err := revocation.ParseLSN(*raw)
	if err != nil {
		return false, err
	}
	r.replay.Store(uint64(got))
	r.need.Store(uint64(need))
	if got < need {
		return false, nil
	}
	return true, fn(tx)
}

// sticky 让同一请求里的几次权限读取尽量落在同一副本上（按 trace_id 哈希），避免在进度不同的副本之间跳转（详设 3.3）。
func sticky(ctx context.Context, n int) int {
	if n <= 1 {
		return 0
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return 0
	}
	idx := 0
	for _, b := range sc.TraceID() {
		idx = (idx*31 + int(b)) % n
	}
	return idx
}

// Standbys 每 100 ms 读一次各同步备库的已接收位点（兜底条件的 S）。样本按发起查询时的本机单调时钟计时，
// 超过 300 ms 的不用；要求每个同步备库都有新鲜样本：任何一个提交都至少被其中一个确认，取最大值才覆盖全部已提交的撤权。
type Standbys struct {
	DBs    []Querier
	Every  time.Duration
	MaxAge time.Duration

	mu      sync.Mutex
	samples []sample
}

// Querier 是能查一行的连接。
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type sample struct {
	lsn revocation.LSN
	at  time.Time
}

func (s *Standbys) Run(ctx context.Context) {
	s.mu.Lock()
	s.samples = make([]sample, len(s.DBs))
	s.mu.Unlock()
	every := s.Every
	if every <= 0 {
		every = 100 * time.Millisecond
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		s.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (s *Standbys) poll(ctx context.Context) {
	var wg sync.WaitGroup
	for i, db := range s.DBs {
		wg.Go(func() {
			at := time.Now()
			qctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			defer cancel()
			var raw *string
			if err := db.QueryRow(qctx, revocation.ReceiveSQL).Scan(&raw); err != nil || raw == nil {
				return
			}
			l, err := revocation.ParseLSN(*raw)
			if err != nil {
				return
			}
			s.mu.Lock()
			s.samples[i] = sample{lsn: l, at: at}
			s.mu.Unlock()
		})
	}
	wg.Wait()
}

func (s *Standbys) LSN() (revocation.LSN, bool) {
	maxAge := s.MaxAge
	if maxAge <= 0 {
		maxAge = 300 * time.Millisecond
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) == 0 {
		return 0, false
	}
	var out revocation.LSN
	for _, sm := range s.samples {
		if sm.at.IsZero() || time.Since(sm.at) > maxAge {
			return 0, false
		}
		out = max(out, sm.lsn)
	}
	return out, true
}

// primaryFloor 用于没有单独读副本、权限直接读主库的部署：主库的回放位点取插入位点，天然不低于任何已提交的撤权，
// 撤权位点读不到时不需要另找 S。
type primaryFloor struct{}

func (primaryFloor) LSN() (revocation.LSN, bool) { return 0, true }

// noFloor 用于配置了读副本、没有配置同步备库的部署：撤权位点读不到就 fail-closed。
type noFloor struct{}

func (noFloor) LSN() (revocation.LSN, bool) { return 0, false }

// docCandidates 从块 ID（{source}:{doc_id}#{anchor}）列出可能的文档 ID：doc_id 与 anchor 里都可能有 #、: ，
// 读名单时把每种切法都点上（多点的只会让名单多拦，不会漏）。
func docCandidates(chunkID string) []string {
	var out []string
	for i := 0; i < len(chunkID) && len(out) < 16; i++ {
		if chunkID[i] != ':' {
			continue
		}
		rest := chunkID[i+1:]
		for j := 0; j < len(rest) && len(out) < 16; j++ {
			if rest[j] == '#' {
				out = append(out, rest[:j])
			}
		}
	}
	return out
}

var (
	permMeter = telemetry.Meter("bambuflow/retrieval")
	// outcome：accepted、stale（这个副本没追上，换下一个）、error、unavailable（全部不满足，fail-closed）、
	// fallback（撤权位点读不到，改用同步备库位点）、no_fence（撤权位点与同步备库位点都没有）
	permReads, _    = permMeter.Int64Counter("retrieval.permission.reads", metric.WithDescription("权限读副本的读取结果"))
	permDuration, _ = permMeter.Float64Histogram("retrieval.permission.duration", metric.WithUnit("s"),
		metric.WithDescription("一次权限批量读（含撤权位点校验）的耗时"))
)

// gauges 注册各副本落后于撤权位点的字节数（详设 7.2），按最近一次读取时的观测值；为 0 的就是满足条件的副本。
func (p *permissions) gauges() {
	_, _ = permMeter.Int64ObservableGauge("retrieval.permission.replica_lag",
		metric.WithUnit("By"), metric.WithDescription("权限读副本最近一次读到的回放位点落后于撤权位点的字节数"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			for _, r := range p.replicas {
				lag := int64(0)
				if need, got := r.need.Load(), r.replay.Load(); need > got {
					lag = int64(min(need-got, math.MaxInt64))
				}
				o.Observe(lag, metric.WithAttributes(attribute.String("replica", r.name)))
			}
			return nil
		}))
}
