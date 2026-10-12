// Package worker 逐条执行批量任务（技术设计文档 13.5）：每条先凭委托向 gateway 换发 Principal，
// 再以调用目的 batch 调 orchestrator。并发有自己的上限，不占在线配额（gateway 详设 5.1）；
// orchestrator 过载时（RESOURCE_EXHAUSTED 等）有上限地退避重试，把容量让给在线请求（orchestrator 详设：batch 优先级最低）。
package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/hei-/bambuflow/services/qa/batch/internal/store"
)

// Store 是执行所需的条目读写（store.PG）。
type Store interface {
	Claim(ctx context.Context, n int, lease time.Duration) ([]store.Task, error)
	Finish(ctx context.Context, o store.Outcome) (bool, error)
	Retry(ctx context.Context, t store.Task, at time.Time) error
	FailRemaining(ctx context.Context, batchID, errType, errTitle string) error
}

// Config 是执行参数（部署配置清单）。
type Config struct {
	// 同时执行的条目数（每个实例）
	Concurrency int
	// 每条最多尝试的次数（含第一次）
	MaxAttempts int
	// 单条问答的截止时间：批量问答可用更慢的精排方式（技术设计文档 6.7），比在线的 4 s 宽
	AnswerTimeout time.Duration
	// 重试退避的起点与上限，按 2 的幂增长并加 20% 以内的随机量
	BackoffBase, BackoffMax time.Duration
	// 没有到期条目时的轮询间隔
	PollInterval time.Duration
}

type Worker struct {
	Store        Store
	Principals   identityv1.PrincipalServiceClient
	Orchestrator answerv1.AnswerServiceClient
	Config       Config
	Log          *slog.Logger
	Now          func() time.Time
}

const exchangeTimeout = 5 * time.Second

// Run 启动 Concurrency 个执行循环，直到 ctx 结束；正在执行的条目做完再返回。
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for range w.Config.Concurrency {
		wg.Go(func() { w.loop(ctx) })
	}
	wg.Wait()
}

func (w *Worker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		did, err := w.Step(ctx)
		if err != nil {
			w.Log.WarnContext(ctx, "领取批量条目失败", slog.Any("err", err))
		}
		if did {
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(w.Config.PollInterval):
		}
	}
}

// Step 领取并执行一个条目，返回是否领到了。
func (w *Worker) Step(ctx context.Context) (bool, error) {
	// 租约覆盖换发与问答的最长时间并留出余量；超过租约仍未结束的条目会被别的实例重新领取
	tasks, err := w.Store.Claim(ctx, 1, exchangeTimeout+w.Config.AnswerTimeout+time.Minute)
	if err != nil || len(tasks) == 0 {
		return false, err
	}
	// 领到的条目做完再退出：用不随 ctx 取消的上下文，截止时间由各次调用自己设
	w.execute(context.WithoutCancel(ctx), tasks[0])
	return true, nil
}

// itemError 是条目失败时写进结果的对外错误类型与说明（技术设计文档 13.10）。
type itemError struct{ typ, title string }

var (
	errForbidden   = itemError{"forbidden", "提交人已无权执行该批次"}
	errExhausted   = itemError{"service_unavailable", "多次重试后仍未成功"}
	errInvalid     = itemError{"invalid_request", "问题或过滤条件不合法"}
	errDenied      = itemError{"forbidden", "无权使用该租户或功能"}
	errNotFound    = itemError{"not_found", "资源不存在"}
	errDeadline    = itemError{"deadline_exceeded", "超过截止时间"}
	errUnavailable = itemError{"service_unavailable", "服务暂时不可用"}
)

// retryable 的错误按退避重试，次数用完才失败。
var retryable = map[codes.Code]bool{
	codes.Unavailable: true, codes.ResourceExhausted: true, codes.DeadlineExceeded: true, codes.Aborted: true,
	// Principal 刚换发出来就被拒，多半是下游还没取到新的验签公钥
	codes.Unauthenticated: true,
}

func finalError(c codes.Code) itemError {
	switch c {
	case codes.InvalidArgument:
		return errInvalid
	case codes.PermissionDenied:
		return errDenied
	case codes.NotFound:
		return errNotFound
	case codes.DeadlineExceeded:
		return errDeadline
	}
	return errUnavailable
}

func (w *Worker) execute(ctx context.Context, t store.Task) {
	log := w.Log.With(slog.String("batch_id", t.BatchID), slog.String("custom_id", t.CustomID), slog.Int("attempt", t.Attempts))
	if t.Attempts > w.Config.MaxAttempts {
		// 前几次执行到一半实例退出、租约过期又被领取，次数已用完
		w.fail(ctx, log, t, errExhausted)
		return
	}
	ectx, cancel := context.WithTimeout(ctx, exchangeTimeout)
	ex, err := w.Principals.Exchange(ectx, &identityv1.ExchangeRequest{Delegation: t.Delegation, BatchId: t.BatchID})
	cancel()
	if status.Code(err) == codes.PermissionDenied {
		// 提交人已停用、被移出租户或委托过期：撤权对后续条目立即生效，剩余条目一并失败（gateway 详设 3.3）
		log.WarnContext(ctx, "换发被拒，批次剩余条目失败")
		if err := w.Store.FailRemaining(ctx, t.BatchID, errForbidden.typ, errForbidden.title); err != nil {
			log.ErrorContext(ctx, "标记剩余条目失败时出错", slog.Any("err", err))
		}
		w.fail(ctx, log, t, errForbidden)
		return
	}
	if err != nil {
		w.retryOrFail(ctx, log, t, err, errUnavailable)
		return
	}

	req := &answerv1.AnswerRequest{Question: t.Question}
	if len(t.Filters) > 0 {
		req.Filters = &commonv1.Filters{}
		if err := proto.Unmarshal(t.Filters, req.Filters); err != nil {
			log.ErrorContext(ctx, "过滤条件无法解析", slog.Any("err", err))
			w.fail(ctx, log, t, errInvalid)
			return
		}
	}
	answerID, traceID := newID("ans_"), newID("")
	actx, cancel := context.WithTimeout(ctx, w.Config.AnswerTimeout)
	defer cancel()
	actx = metadata.AppendToOutgoingContext(actx, "authorization", "Bearer "+ex.GetPrincipal(),
		"x-call-purpose", "batch", "x-answer-id", answerID)
	resp, err := w.Orchestrator.Answer(actx, req)
	if err != nil {
		w.retryOrFail(ctx, log, t, err, finalError(status.Code(err)))
		return
	}
	// 回答 ID 由 qa/batch 生成（与 gateway 生成在线回答的 answer_id 一样），结果文件按它回放与反馈
	resp.AnswerId, resp.TraceId = answerID, traceID
	pb, err := proto.Marshal(resp)
	if err != nil {
		log.ErrorContext(ctx, "序列化回答失败", slog.Any("err", err))
		w.fail(ctx, log, t, errUnavailable)
		return
	}
	if _, err := w.Store.Finish(ctx, store.Outcome{BatchID: t.BatchID, Seq: t.Seq, Attempts: t.Attempts, AnswerID: answerID, Result: pb}); err != nil {
		// 结果没写进去：条目仍在执行中，租约过期后重新执行
		log.ErrorContext(ctx, "写入结果失败", slog.Any("err", err))
	}
}

func (w *Worker) retryOrFail(ctx context.Context, log *slog.Logger, t store.Task, err error, final itemError) {
	code := status.Code(err)
	if !retryable[code] || t.Attempts >= w.Config.MaxAttempts {
		log.WarnContext(ctx, "条目失败", slog.String("code", code.String()))
		w.fail(ctx, log, t, final)
		return
	}
	at := w.Now().Add(w.Backoff(t.Attempts))
	log.InfoContext(ctx, "稍后重试", slog.String("code", code.String()), slog.Time("at", at))
	if err := w.Store.Retry(ctx, t, at); err != nil {
		log.ErrorContext(ctx, "放回队列失败", slog.Any("err", err))
	}
}

func (w *Worker) fail(ctx context.Context, log *slog.Logger, t store.Task, e itemError) {
	if _, err := w.Store.Finish(ctx, store.Outcome{BatchID: t.BatchID, Seq: t.Seq, Attempts: t.Attempts, ErrType: e.typ, ErrTitle: e.title}); err != nil {
		log.ErrorContext(ctx, "写入失败结果出错", slog.Any("err", err))
	}
}

// Backoff 是第 attempt 次失败后的等待时间：起点按 2 的幂增长，不超过上限，再加 20% 以内的随机量，
// 避免同一批的条目在 orchestrator 恢复时同时涌入。
func (w *Worker) Backoff(attempt int) time.Duration {
	d := w.Config.BackoffBase
	for i := 1; i < attempt && d < w.Config.BackoffMax; i++ {
		d *= 2
	}
	d = min(d, w.Config.BackoffMax)
	if jitter := int64(d) / 5; jitter > 0 {
		n, err := rand.Int(rand.Reader, big.NewInt(jitter))
		if err == nil {
			d += time.Duration(n.Int64())
		}
	}
	return d
}

func newID(prefix string) string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	if prefix == "ans_" {
		b = b[:12]
	}
	return prefix + hex.EncodeToString(b)
}
