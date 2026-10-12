package refetch

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

type fakeStream struct {
	pending, fresh [][]redis.XMessage
	acked          []string
}

func (f *fakeStream) Read(_ context.Context, pending bool) ([]redis.XMessage, error) {
	q := &f.fresh
	if pending {
		q = &f.pending
	}
	if len(*q) == 0 {
		return nil, nil
	}
	out := (*q)[0]
	*q = (*q)[1:]
	return out, nil
}

func (f *fakeStream) Ack(_ context.Context, id string) error {
	f.acked = append(f.acked, id)
	return nil
}

type fakeState struct {
	forgot []string
	err    error
}

func (f *fakeState) Forget(_ context.Context, ids []string) error {
	if f.err != nil {
		return f.err
	}
	f.forgot = append(f.forgot, ids...)
	return nil
}

func msg(t *testing.T, id, src string, docs ...string) redis.XMessage {
	t.Helper()
	pb, err := proto.Marshal(&ingestionv1.RefetchRequested{EventId: "evt_" + id, SourceId: src, DocIds: docs,
		Reason: ingestionv1.RefetchReason_REFETCH_REASON_TOMBSTONE_REVOKED})
	if err != nil {
		t.Fatal(err)
	}
	return redis.XMessage{ID: id, Values: map[string]any{"pb": string(pb)}}
}

func TestApplyForgetsOwnSourceAndAcksEverything(t *testing.T) {
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	s := &fakeStream{
		pending: [][]redis.XMessage{{msg(t, "1-0", "src_wiki", "a")}},
		fresh: [][]redis.XMessage{
			{msg(t, "2-0", "src_files", "x"), {ID: "3-0", Values: map[string]any{"pb": "\xff"}}},
			{msg(t, "4-0", "src_wiki", "b", "c")},
		},
	}
	st := &fakeState{}
	n, err := Apply(context.Background(), s, st, "src_wiki", log)
	if err != nil || n != 3 || !slices.Equal(st.forgot, []string{"a", "b", "c"}) {
		t.Fatalf("n=%d forgot=%v err=%v", n, st.forgot, err)
	}
	// 别的数据源与格式不对的也确认
	if !slices.Equal(s.acked, []string{"1-0", "2-0", "3-0", "4-0"}) {
		t.Fatalf("acked=%v", s.acked)
	}
}

func TestApplyLeavesMessageUnackedWhenStateFails(t *testing.T) {
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	s := &fakeStream{fresh: [][]redis.XMessage{{msg(t, "1-0", "src_wiki", "a")}}}
	_, err := Apply(context.Background(), s, &fakeState{err: errors.New("redis down")}, "src_wiki", log)
	if err == nil || len(s.acked) != 0 {
		t.Fatalf("状态没改成功不能确认: err=%v acked=%v", err, s.acked)
	}
}
