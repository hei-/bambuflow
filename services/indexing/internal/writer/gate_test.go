package writer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	evaluationv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/evaluation/v1"
)

type fakeGate struct {
	verdicts []evaluationv1.Verdict
	run      *evaluationv1.RunGateRequest
	polls    int
}

func (f *fakeGate) RunGate(_ context.Context, in *evaluationv1.RunGateRequest, _ ...grpc.CallOption) (*evaluationv1.RunGateResponse, error) {
	f.run = in
	return &evaluationv1.RunGateResponse{GateId: "gate_1"}, nil
}

func (f *fakeGate) GetGate(context.Context, *evaluationv1.GetGateRequest, ...grpc.CallOption) (*evaluationv1.GetGateResponse, error) {
	v := f.verdicts[min(f.polls, len(f.verdicts)-1)]
	f.polls++
	return &evaluationv1.GetGateResponse{GateId: "gate_1", Verdict: v, Violations: []string{"credit groundedness 下降 3 个点"}}, nil
}

func TestGateWaitsForVerdict(t *testing.T) {
	ok := &fakeGate{verdicts: []evaluationv1.Verdict{evaluationv1.Verdict_VERDICT_RUNNING, evaluationv1.Verdict_VERDICT_PASSED}}
	g := &Gate{Client: ok, Every: time.Millisecond, Budget: time.Second, Log: quiet}
	if err := g.Check(context.Background(), "ix-2", 12); err != nil {
		t.Fatal(err)
	}
	if ok.run.GetTrigger() != "index_publish" || ok.run.GetCandidate().GetIndexVersion() != "ix-2" || ok.run.GetIdempotencyKey() != "index_publish:ix-2:12" {
		t.Fatalf("%v", ok.run)
	}
	failed := &fakeGate{verdicts: []evaluationv1.Verdict{evaluationv1.Verdict_VERDICT_FAILED}}
	g.Client = failed
	if err := g.Check(context.Background(), "ix-2", 12); !errors.Is(err, ErrGateFailed) || !strings.Contains(err.Error(), "下降") {
		t.Fatalf("不通过不发布: %v", err)
	}
	stuck := &fakeGate{verdicts: []evaluationv1.Verdict{evaluationv1.Verdict_VERDICT_RUNNING}}
	g.Client, g.Budget = stuck, 20*time.Millisecond
	if err := g.Check(context.Background(), "ix-2", 12); !errors.Is(err, ErrGateFailed) {
		t.Fatalf("超时按不通过: %v", err)
	}
}
