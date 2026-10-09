package writer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	evaluationv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/evaluation/v1"
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

// PendingVersion 返回待发布版本与它清单里的文档数；没有待发布版本时为 ErrNoPending。
func PendingVersion(ctx context.Context, pool *pgxpool.Pool) (string, int64, error) {
	var version string
	var docs int64
	err := pool.QueryRow(ctx, `SELECT v.version, (SELECT count(*) FROM idx.version_docs vd WHERE vd.version = v.version)
		FROM idx.index_versions v WHERE v.status = 'pending'`).Scan(&version, &docs)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, ErrNoPending
	}
	return version, docs, err
}

// Check 发起门禁并等结论；通过返回 nil。同一个待发布版本、同样的文档数重复调用复用同一个门禁（幂等键）。
func (g *Gate) Check(ctx context.Context, version string, docs int64) error {
	ctx, cancel := context.WithTimeout(ctx, g.Budget)
	defer cancel()
	run, err := g.Client.RunGate(ctx, &evaluationv1.RunGateRequest{
		Trigger: "index_publish", Candidate: &evaluationv1.EvalConfig{IndexVersion: version},
		Deadline: durationpb.New(5 * time.Minute), IdempotencyKey: fmt.Sprintf("index_publish:%s:%d", version, docs),
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
