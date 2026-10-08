package worker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	answerv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
	"github.com/bambu-ai/bambuflow/services/qa/batch/internal/store"
)

type fakeStore struct {
	mu        sync.Mutex
	tasks     []store.Task
	finished  []store.Outcome
	retried   map[int64]time.Time
	failedAll []string
}

func (f *fakeStore) Claim(context.Context, int, time.Duration) ([]store.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.tasks) == 0 {
		return nil, nil
	}
	t := f.tasks[0]
	f.tasks = f.tasks[1:]
	return []store.Task{t}, nil
}

func (f *fakeStore) Finish(_ context.Context, o store.Outcome) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finished = append(f.finished, o)
	return true, nil
}

func (f *fakeStore) Retry(_ context.Context, t store.Task, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retried[t.Seq] = at
	return nil
}

func (f *fakeStore) FailRemaining(_ context.Context, id, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failedAll = append(f.failedAll, id)
	return nil
}

type fakePrincipals struct {
	err   error
	calls int
}

func (f *fakePrincipals) Exchange(_ context.Context, r *identityv1.ExchangeRequest, _ ...grpc.CallOption) (*identityv1.ExchangeResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &identityv1.ExchangeResponse{Principal: "p-for-" + r.GetBatchId()}, nil
}

type fakeOrchestrator struct {
	answerv1.AnswerServiceClient
	err   error
	calls int
	md    metadata.MD
	req   *answerv1.AnswerRequest
}

func (f *fakeOrchestrator) Answer(ctx context.Context, r *answerv1.AnswerRequest, _ ...grpc.CallOption) (*answerv1.AnswerResponse, error) {
	f.calls++
	f.md, _ = metadata.FromOutgoingContext(ctx)
	f.req = r
	if f.err != nil {
		return nil, f.err
	}
	return &answerv1.AnswerResponse{AnswerId: "orchestrator-own-id", Status: answerv1.Status_STATUS_ANSWERED, Answer: "答"}, nil
}

var now = time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)

func newWorker(task store.Task, exErr, ansErr error) (*Worker, *fakeStore, *fakePrincipals, *fakeOrchestrator) {
	st := &fakeStore{tasks: []store.Task{task}, retried: map[int64]time.Time{}}
	ps := &fakePrincipals{err: exErr}
	orch := &fakeOrchestrator{err: ansErr}
	w := &Worker{
		Store: st, Principals: ps, Orchestrator: orch, Log: logging.New(&bytes.Buffer{}, "t", slog.LevelInfo),
		Config: Config{Concurrency: 1, MaxAttempts: 3, AnswerTimeout: time.Second, BackoffBase: 2 * time.Second, BackoffMax: time.Minute, PollInterval: time.Millisecond},
		Now:    func() time.Time { return now },
	}
	return w, st, ps, orch
}

func task(attempts int) store.Task {
	f, _ := proto.Marshal(&commonv1.Filters{SourceIds: []string{"src_wikidemo"}})
	return store.Task{BatchID: "bat_01", Seq: 7, CustomID: "q7", Question: "额度上限？", Filters: f, Attempts: attempts, Delegation: "d"}
}

func step(t *testing.T, w *Worker) {
	t.Helper()
	did, err := w.Step(context.Background())
	if err != nil || !did {
		t.Fatalf("应领到条目: %v %v", did, err)
	}
}

func TestSuccessStoresAnswerWithBatchPurpose(t *testing.T) {
	w, st, _, orch := newWorker(task(1), nil, nil)
	step(t, w)
	if got := orch.md.Get("x-call-purpose"); len(got) != 1 || got[0] != "batch" {
		t.Fatalf("调用目的应为 batch: %v", orch.md)
	}
	if got := orch.md.Get("authorization"); len(got) != 1 || got[0] != "Bearer p-for-bat_01" {
		t.Fatalf("应带换发出的 Principal: %v", got)
	}
	aid := orch.md.Get("x-answer-id")
	if len(aid) != 1 || !strings.HasPrefix(aid[0], "ans_") {
		t.Fatalf("应带 ans_ 前缀的 x-answer-id: %v", aid)
	}
	if orch.req.GetFilters().GetSourceIds()[0] != "src_wikidemo" || orch.req.GetQuestion() != "额度上限？" {
		t.Fatalf("问题与过滤条件应原样传递: %v", orch.req)
	}
	if len(st.finished) != 1 || st.finished[0].ErrType != "" || st.finished[0].AnswerID != aid[0] || st.finished[0].Attempts != 1 {
		t.Fatalf("应记成功: %+v", st.finished)
	}
	var a answerv1.AnswerResponse
	if err := proto.Unmarshal(st.finished[0].Result, &a); err != nil || a.GetAnswerId() != aid[0] || a.GetTraceId() == "" {
		t.Fatalf("结果里的 answer_id 应是 qa/batch 生成的: %v %v", a.GetAnswerId(), err)
	}
}

func TestOverloadRetriesWithBackoff(t *testing.T) {
	w, st, _, _ := newWorker(task(1), nil, status.Error(codes.ResourceExhausted, "排队超时"))
	step(t, w)
	at, ok := st.retried[7]
	if !ok || len(st.finished) != 0 {
		t.Fatalf("过载应稍后重试而不是失败: %+v %+v", st.retried, st.finished)
	}
	if d := at.Sub(now); d < 2*time.Second || d > 2400*time.Millisecond {
		t.Fatalf("第一次重试应在 2 秒（加 20%% 以内随机量）后，得到 %s", d)
	}
}

func TestRetriesAreBounded(t *testing.T) {
	w, st, _, _ := newWorker(task(3), nil, status.Error(codes.Unavailable, "不可用"))
	step(t, w)
	if len(st.retried) != 0 || len(st.finished) != 1 || st.finished[0].ErrType != "service_unavailable" {
		t.Fatalf("次数用完应失败: %+v %+v", st.retried, st.finished)
	}
}

func TestNonRetryableFailsImmediately(t *testing.T) {
	w, st, _, _ := newWorker(task(1), nil, status.Error(codes.InvalidArgument, "问题过长"))
	step(t, w)
	if len(st.retried) != 0 || len(st.finished) != 1 || st.finished[0].ErrType != "invalid_request" {
		t.Fatalf("参数错误不重试: %+v %+v", st.retried, st.finished)
	}
}

func TestRevokedSubmitterFailsRemainingItems(t *testing.T) {
	w, st, _, orch := newWorker(task(1), status.Error(codes.PermissionDenied, "提交人已停用"), nil)
	step(t, w)
	if orch.calls != 0 {
		t.Fatal("换发被拒后不应再调 orchestrator")
	}
	if len(st.failedAll) != 1 || st.failedAll[0] != "bat_01" {
		t.Fatalf("剩余条目应一并失败: %v", st.failedAll)
	}
	if len(st.finished) != 1 || st.finished[0].ErrType != "forbidden" {
		t.Fatalf("当前条目应以 forbidden 失败: %+v", st.finished)
	}
}

func TestExchangeUnavailableRetries(t *testing.T) {
	w, st, _, orch := newWorker(task(1), status.Error(codes.Unavailable, "身份同步不新鲜"), nil)
	step(t, w)
	if orch.calls != 0 || len(st.retried) != 1 || len(st.failedAll) != 0 {
		t.Fatalf("换发暂时不可用应重试: calls=%d %+v", orch.calls, st.retried)
	}
}

func TestReclaimedPastLimitFailsWithoutCalls(t *testing.T) {
	w, st, ps, orch := newWorker(task(4), nil, nil)
	step(t, w)
	if ps.calls != 0 || orch.calls != 0 || len(st.finished) != 1 || st.finished[0].ErrType != "service_unavailable" {
		t.Fatalf("超过次数的条目直接失败: %d %d %+v", ps.calls, orch.calls, st.finished)
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	w, _, _, _ := newWorker(task(1), nil, nil)
	for attempt, base := range map[int]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second, 10: time.Minute} {
		d := w.Backoff(attempt)
		if d < base || d > base+base/5 {
			t.Errorf("第 %d 次：%s 不在 [%s, %s] 内", attempt, d, base, base+base/5)
		}
	}
}

func TestStepWithNothingDue(t *testing.T) {
	w, _, _, _ := newWorker(task(1), nil, nil)
	step(t, w)
	if did, err := w.Step(context.Background()); did || err != nil {
		t.Fatalf("没有到期条目时应返回 false: %v %v", did, err)
	}
}
