package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/hei-/bambuflow/libs/platform-go/revocation"
)

// fakeDB 是一个权限读副本：回放位点固定为 lsn，记录语句顺序。
type fakeDB struct {
	name string
	lsn  revocation.LSN
	log  *[]string
	fail bool
}

func (d *fakeDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	if d.fail {
		return nil, errors.New("连接失败")
	}
	return &fakeTx{db: d}, nil
}

type fakeTx struct {
	pgx.Tx
	db *fakeDB
}

func (t *fakeTx) Rollback(context.Context) error { return nil }

func (t *fakeTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if sql == revocation.ReplaySQL {
		*t.db.log = append(*t.db.log, t.db.name+":replay")
		return lsnRow(t.db.lsn.String())
	}
	*t.db.log = append(*t.db.log, t.db.name+":data")
	return lsnRow("")
}

type lsnRow string

func (r lsnRow) Scan(dest ...any) error {
	if len(dest) == 0 {
		return nil
	}
	if d, ok := dest[0].(**string); ok {
		v := string(r)
		*d = &v
	}
	return nil
}

type fixedFloor struct {
	lsn revocation.LSN
	ok  bool
}

func (f fixedFloor) LSN() (revocation.LSN, bool) { return f.lsn, f.ok }

func newPerm(t *testing.T, standby floor, dbs ...*fakeDB) (*permissions, *miniredis.Miniredis, *[]string) {
	mr := miniredis.RunT(t)
	log := &[]string{}
	p := &permissions{rev: &revocation.Store{Redis: redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})},
		standby: standby, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, d := range dbs {
		d.log = log
		p.replicas = append(p.replicas, &replica{name: d.name, db: d})
	}
	return p, mr, log
}

func readOnce(p *permissions, docs ...string) (map[string]bool, error) {
	var got map[string]bool
	err := p.read(context.Background(), docs, func(q pgx.Tx, held map[string]bool) error {
		got = held
		return q.QueryRow(context.Background(), "SELECT data").Scan()
	})
	return got, err
}

func TestReadOnlyAcceptsReplicaAtOrPastFence(t *testing.T) {
	p, _, log := newPerm(t, noFloor{}, &fakeDB{name: "a", lsn: 90}, &fakeDB{name: "b", lsn: 100})
	ctx := context.Background()
	_ = p.rev.Raise(ctx, 100)
	_ = p.rev.Hold(ctx, revocation.DocMember("d2"))
	held, err := readOnce(p, "d1", "d2")
	if err != nil {
		t.Fatal(err)
	}
	// a 落后被拒，换 b；每个副本都先读位点、再读数据
	if !slices.Equal(*log, []string{"a:replay", "b:replay", "b:data"}) {
		t.Fatalf("读取顺序: %v", *log)
	}
	if !held[revocation.DocMember("d2")] || held[revocation.DocMember("d1")] {
		t.Fatalf("名单: %v", held)
	}
}

func TestReadFailsClosedWhenNoReplicaIsFresh(t *testing.T) {
	p, _, log := newPerm(t, fixedFloor{0, true}, &fakeDB{name: "a", lsn: 90}, &fakeDB{name: "b", fail: true})
	_ = p.rev.Raise(context.Background(), 100)
	if _, err := readOnce(p); !errors.Is(err, ErrNoFreshReplica) {
		t.Fatalf("全部副本不满足时应拒绝: %v", err)
	}
	if slices.Contains(*log, "a:data") {
		t.Fatal("落后的副本不应读数据")
	}
}

func TestMissingFenceFallsBackToStandbyLSN(t *testing.T) {
	// 撤权位点缺失（故障切换丢了）：用同步备库位点 S，名单照常读
	p, _, _ := newPerm(t, fixedFloor{150, true}, &fakeDB{name: "a", lsn: 120}, &fakeDB{name: "b", lsn: 160})
	_ = p.rev.Hold(context.Background(), revocation.DocMember("d1"))
	held, err := readOnce(p, "d1")
	if err != nil || !held[revocation.DocMember("d1")] {
		t.Fatalf("兜底读取: %v %v", held, err)
	}
	// 没有 S 时拒绝
	p.standby = noFloor{}
	if _, err := readOnce(p, "d1"); !errors.Is(err, ErrNoFreshReplica) {
		t.Fatalf("撤权位点与同步备库位点都没有时应拒绝: %v", err)
	}
}

func TestRedisDownFallsBackToStandbyLSN(t *testing.T) {
	p, mr, log := newPerm(t, fixedFloor{150, true}, &fakeDB{name: "a", lsn: 120}, &fakeDB{name: "b", lsn: 160})
	mr.Close()
	if _, err := readOnce(p, "d1"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(*log, []string{"a:replay", "b:replay", "b:data"}) {
		t.Fatalf("Redis 不可用时按 r ≥ S 采纳: %v", *log)
	}
}

func TestPrimaryOnlyNeedsNoStandby(t *testing.T) {
	// 没有单独读副本、直接读主库：主库的位点不低于任何已提交的撤权
	p, mr, _ := newPerm(t, primaryFloor{}, &fakeDB{name: "primary", lsn: 10})
	mr.Close()
	if _, err := readOnce(p); err != nil {
		t.Fatal(err)
	}
}

func TestStandbysNeedFreshSampleFromEach(t *testing.T) {
	s := &Standbys{MaxAge: 300 * time.Millisecond}
	if _, ok := s.LSN(); ok {
		t.Fatal("还没有样本")
	}
	now := time.Now()
	s.samples = []sample{{lsn: 100, at: now}, {lsn: 200, at: now}}
	if l, ok := s.LSN(); !ok || l != 200 {
		t.Fatalf("取最大值: %v %v", l, ok)
	}
	// 任何一个同步备库的样本过期都不用：那条提交可能只被它确认过
	s.samples[1].at = now.Add(-time.Second)
	if _, ok := s.LSN(); ok {
		t.Fatal("有过期样本时不应给出 S")
	}
}

func TestDocCandidates(t *testing.T) {
	got := docCandidates("src_wiki:a#b#c")
	if !slices.Equal(got, []string{"a", "a#b"}) {
		t.Fatalf("候选: %v", got)
	}
	if got := docCandidates("src_x:ns:doc#1"); !slices.Contains(got, "ns:doc") {
		t.Fatalf("doc_id 含冒号: %v", got)
	}
}
