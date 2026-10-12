package writer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	evaluationv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/evaluation/v1"
)

// ErrGateFailed：待发布版本没有通过评测门禁（或没有在时间预算内得到结论），不发布。
var ErrGateFailed = errors.New("评测门禁没有通过")

// GateClient 是 evaluation 的 EvalService。
type GateClient interface {
	RunGate(ctx context.Context, in *evaluationv1.RunGateRequest, opts ...grpc.CallOption) (*evaluationv1.RunGateResponse, error)
	GetGate(ctx context.Context, in *evaluationv1.GetGateRequest, opts ...grpc.CallOption) (*evaluationv1.GetGateResponse, error)
}

// Gate 在增量发布前对待发布版本跑轻量门禁（入库管道详设第 10 节：冒烟集，预算 5 分钟，超时按不通过）。
type Gate struct {
	Client GateClient
	// Every 是轮询间隔；Budget 是等结论的上限（比门禁自身的 5 分钟预算略长，留出排队时间）
	Every, Budget time.Duration
	Log           *slog.Logger
}

// Pending 是一个待发布版本与它清单里的文档数。
type Pending struct {
	Version string
	Docs    int64
}

// PendingVersions 返回增量的待发布版本（线上模型）与全量重建的待发布版本，没有的为 nil。
func PendingVersions(ctx context.Context, pool *pgxpool.Pool) (incremental, rebuild *Pending, err error) {
	rows, err := pool.Query(ctx, `SELECT v.version, (SELECT count(*) FROM idx.version_docs vd WHERE vd.version = v.version),
		EXISTS (SELECT 1 FROM idx.rebuilds rb WHERE rb.version = v.version AND rb.status = 'running')
		FROM idx.index_versions v WHERE v.status = 'pending'`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var p Pending
		var isRebuild bool
		if err := rows.Scan(&p.Version, &p.Docs, &isRebuild); err != nil {
			return nil, nil, err
		}
		if isRebuild {
			rebuild = &p
		} else {
			incremental = &p
		}
	}
	return incremental, rebuild, rows.Err()
}

// CheckRebuild 对全量重建的版本跑完整评测集门禁（入库管道详设第 10 节第 4 步，预算 4 小时，由 evaluation 按触发方给出）。
// 不在这里等：门禁按版本幂等，每次定时发布查一次结论，还在跑时 passed 为 false、err 为 nil。
// 门禁不通过就不再发布这个版本，换模型要取消重建后重新申请。
func (g *Gate) CheckRebuild(ctx context.Context, version string) (bool, error) {
	run, err := g.Client.RunGate(ctx, &evaluationv1.RunGateRequest{
		Trigger: "index_rebuild", Candidate: &evaluationv1.EvalConfig{IndexVersion: version}, Ruleset: "full",
		IdempotencyKey: "index_rebuild:" + version,
	}, grpc.WaitForReady(false))
	if err != nil {
		return false, fmt.Errorf("%w：发起门禁失败: %w", ErrGateFailed, err)
	}
	r, err := g.Client.GetGate(ctx, &evaluationv1.GetGateRequest{GateId: run.GetGateId()})
	switch {
	case err != nil:
		return false, fmt.Errorf("%w：查询门禁 %s 失败: %w", ErrGateFailed, run.GetGateId(), err)
	case r.GetVerdict() == evaluationv1.Verdict_VERDICT_PASSED:
		g.Log.InfoContext(ctx, "全量重建的评测门禁通过", slog.String("gate_id", run.GetGateId()), slog.String("version", version))
		return true, nil
	case r.GetVerdict() == evaluationv1.Verdict_VERDICT_RUNNING:
		g.Log.InfoContext(ctx, "全量重建的评测门禁进行中", slog.String("gate_id", run.GetGateId()), slog.String("version", version))
		return false, nil
	}
	return false, fmt.Errorf("%w：门禁 %s 结论 %s：%s（报告 %s）", ErrGateFailed, run.GetGateId(), r.GetVerdict(),
		strings.Join(r.GetViolations(), "；"), r.GetReportUri())
}

// HasLive 返回是否已有线上版本：第一次发布的门禁没有可对照的基线，要在请求里声明。
func HasLive(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var live bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM idx.aliases WHERE alias = 'prod')`).Scan(&live)
	return live, err
}

// Check 发起门禁并等结论；通过返回 nil。同一个待发布版本、同样的文档数重复调用复用同一个门禁（幂等键）。
// first 表示还没有线上版本（第一次发布）。
func (g *Gate) Check(ctx context.Context, version string, docs int64, first bool) error {
	ctx, cancel := context.WithTimeout(ctx, g.Budget)
	defer cancel()
	run, err := g.Client.RunGate(ctx, &evaluationv1.RunGateRequest{
		Trigger: "index_publish", Candidate: &evaluationv1.EvalConfig{IndexVersion: version},
		Deadline: durationpb.New(5 * time.Minute), IdempotencyKey: fmt.Sprintf("index_publish:%s:%d", version, docs),
		FirstRelease: first,
	}, grpc.WaitForReady(false))
	if err != nil {
		return fmt.Errorf("%w：发起门禁失败: %w", ErrGateFailed, err)
	}
	g.Log.InfoContext(ctx, "评测门禁已发起", slog.String("gate_id", run.GetGateId()), slog.String("version", version), slog.Int64("docs", docs))
	t := time.NewTicker(g.Every)
	defer t.Stop()
	for {
		r, err := g.Client.GetGate(ctx, &evaluationv1.GetGateRequest{GateId: run.GetGateId()})
		switch {
		case err != nil && ctx.Err() == nil:
			// 查询失败不等于不通过：继续等到时间预算用完
			g.Log.WarnContext(ctx, "查询门禁失败，继续等", slog.Any("err", err))
		case err == nil && r.GetVerdict() == evaluationv1.Verdict_VERDICT_PASSED:
			g.Log.InfoContext(ctx, "评测门禁通过", slog.String("gate_id", run.GetGateId()))
			return nil
		case err == nil && r.GetVerdict() != evaluationv1.Verdict_VERDICT_RUNNING:
			return fmt.Errorf("%w：门禁 %s 结论 %s：%s（报告 %s）", ErrGateFailed, run.GetGateId(), r.GetVerdict(),
				strings.Join(r.GetViolations(), "；"), r.GetReportUri())
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w：门禁 %s 在 %s 内没有结论", ErrGateFailed, run.GetGateId(), g.Budget)
		case <-t.C:
		}
	}
}
