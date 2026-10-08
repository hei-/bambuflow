package identitysync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

type fakeDir struct {
	users  []Entry
	groups []Group
	err    error
	since  []string // 每次调用收到的 since，用于断言增量 / 全量
}

func (d *fakeDir) Users(_ context.Context, since string) ([]Entry, error) {
	d.since = append(d.since, "users:"+since)
	if d.err != nil {
		return nil, d.err
	}
	var out []Entry
	for _, u := range d.users {
		if u.Modified >= since {
			out = append(out, u)
		}
	}
	return out, nil
}

func (d *fakeDir) Groups(_ context.Context, since string) ([]Group, error) {
	d.since = append(d.since, "groups:"+since)
	var out []Group
	for _, g := range d.groups {
		if g.Modified >= since {
			out = append(out, g)
		}
	}
	return out, nil
}

type fakeStore struct {
	state    State
	applied  []Batch
	failures []error
}

func (s *fakeStore) State(context.Context, string) (State, error) { return s.state, nil }

func (s *fakeStore) Apply(_ context.Context, _ string, b Batch) error {
	s.applied = append(s.applied, b)
	s.state.UsersCursor, s.state.GroupsCursor = b.UsersCursor, b.GroupsCursor
	if b.Full {
		s.state.LastFullAt = time.Unix(1000, 0)
	}
	return nil
}

func (s *fakeStore) RecordFailure(_ context.Context, _ string, cause error) error {
	s.failures = append(s.failures, cause)
	return nil
}

func testConfig() *Config {
	c := &Config{Source: "corp-ldap", Interval: time.Minute, FullEvery: 24 * time.Hour}
	c.Mapping.Username, c.Mapping.Dept, c.Mapping.Grade = "uid", "departmentNumber", "employeeType"
	c.Mapping.ClearanceByGrade = map[string]string{"C1": "public", "C2": "internal", "C3": "confidential"}
	c.Mapping.Status.Attr, c.Mapping.Status.DisabledValues = "employeeStatus", []string{"left"}
	return c
}

func newSyncer(dir *fakeDir, store *fakeStore, at time.Time) *Syncer {
	s := NewSyncer(testConfig(), dir, store, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)))
	s.now = func() time.Time { return at }
	return s
}

func TestFirstRunIsFullThenIncremental(t *testing.T) {
	dir := &fakeDir{
		users: []Entry{
			{DN: "uid=alice", Attrs: map[string]string{"uid": "alice", "departmentNumber": "信贷部", "employeeType": "C2"}, Modified: "20261004010000Z"},
			{DN: "uid=bob", Attrs: map[string]string{"uid": "bob", "employeeType": "C9"}, Modified: "20261004020000Z"},
			{DN: "uid=carol", Attrs: map[string]string{"uid": "carol", "employeeStatus": "left"}, Modified: "20261004020000Z"},
			{DN: "cn=no-uid", Attrs: map[string]string{}, Modified: "20261004020000Z"},
		},
		groups: []Group{{Name: "credit-l2", Members: []string{"alice"}, Modified: "20261004030000Z"}},
	}
	store := &fakeStore{}
	s := newSyncer(dir, store, time.Unix(2000, 0))
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := store.applied[0]
	if !b.Full || len(b.Identities) != 3 || len(b.Groups) != 1 {
		t.Fatalf("首次应全量，3 个有账号的用户、1 个群组: %+v", b)
	}
	want := map[string]Identity{
		"alice": {Clearance: 2, Active: true, Dept: "信贷部"},
		"bob":   {Clearance: 1, Active: true}, // 映射表里没有的等级按最低处理
		"carol": {Clearance: 1, Active: false},
	}
	for _, id := range b.Identities {
		w := want[id.Username]
		if id.Clearance != w.Clearance || id.Active != w.Active || id.Dept != w.Dept {
			t.Errorf("%s: 得到 %+v", id.Username, id)
		}
	}
	if b.UsersCursor != "20261004020000Z" || b.GroupsCursor != "20261004030000Z" {
		t.Errorf("游标应推进到最大修改时间: %q %q", b.UsersCursor, b.GroupsCursor)
	}

	// 第二次：一小时后，增量；只拿到游标之后（含）修改的条目
	s.now = func() time.Time { return time.Unix(1000, 0).Add(time.Hour) }
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := dir.since[len(dir.since)-2:]; got[0] != "users:20261004020000Z" || got[1] != "groups:20261004030000Z" {
		t.Errorf("第二次应按游标增量拉取，得到 %v", got)
	}
	if store.applied[1].Full {
		t.Error("第二次不应是全量")
	}
}

func TestFullReconcileAfterFullEvery(t *testing.T) {
	dir := &fakeDir{}
	store := &fakeStore{state: State{LastFullAt: time.Unix(0, 0), UsersCursor: "x", GroupsCursor: "y"}}
	s := newSyncer(dir, store, time.Unix(0, 0).Add(24*time.Hour))
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !store.applied[0].Full || dir.since[0] != "users:" {
		t.Fatalf("超过 full_every 应全量对账: %+v %v", store.applied[0], dir.since)
	}
}

func TestDirectoryFailureRecordedAndNotApplied(t *testing.T) {
	dir := &fakeDir{err: errors.New("ldap: connection refused")}
	store := &fakeStore{}
	s := newSyncer(dir, store, time.Unix(2000, 0))
	if err := s.RunOnce(context.Background()); err == nil {
		t.Fatal("目录不可用应返回错误")
	}
	if len(store.applied) != 0 || len(store.failures) != 1 {
		t.Fatalf("失败时不应写入，只记录失败: applied=%d failures=%d", len(store.applied), len(store.failures))
	}
}
