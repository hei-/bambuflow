package logs

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/publish"
)

func TestDrainMergesVariablePositions(t *testing.T) {
	d := NewDrain()
	a := d.Add("payment timeout order=ORD000001 user=[已脱敏:phone] after 3000ms")
	b := d.Add("payment timeout order=ORD000002 user=[已脱敏:phone] after 4500ms")
	c := d.Add("channel unionpay retry 3")
	if a != b || a == c {
		t.Fatalf("同一模板的消息应归到一起: %v %v %v", a, b, c)
	}
	if got := a.String(); got != "payment timeout order=<*> user=<*> after <*>" {
		t.Fatalf("模板: %q", got)
	}
	// 词数相同、首词相同但大部分位置不同：不是同一模板
	if d.Add("payment gateway restarted on node7 successfully") == a {
		t.Fatal("相似度不够时应新建模板")
	}
}

func cfg() *Config {
	return &Config{Topic: "logs:app", RetentionDays: 30, Classification: "internal", Services: map[string]Service{
		"payments-svc": {Groups: []string{"group:sre-oncall"}, Levels: []string{"ERROR", "WARN"}},
		"core-batch":   {Groups: []string{"group:sre-core"}, Levels: []string{"ERROR"}},
	}}
}

var now = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

func msg(id string, at time.Time, service, level, text string) Message {
	return Message{ID: id, Time: at, Service: service, Level: level, Text: text}
}

func day(d int, hh, mm, ss int) time.Time {
	return time.Date(2026, 10, d, hh, mm, ss, 0, time.UTC)
}

func TestRenderGroupsByWindowAndTemplate(t *testing.T) {
	entries := []Entry{
		{ID: "1-0", Time: day(5, 8, 1, 0), Level: "ERROR", Text: "payment timeout order=ORD1 after 30ms"},
		{ID: "2-0", Time: day(5, 8, 3, 0), Level: "ERROR", Text: "payment timeout order=ORD2 after 40ms"},
		{ID: "3-0", Time: day(5, 8, 7, 0), Level: "WARN", Text: "channel unionpay retry 3"},
	}
	got := Render("payments-svc", day(5, 0, 0, 0), entries)
	want := "# payments-svc 运行日志 2026-10-05（UTC）\n\n" +
		"## 08:00–08:05\n\n- [ERROR] payment timeout order=<*> after <*>（2 次，08:01:00–08:03:00）\n\n" +
		"## 08:05–08:10\n\n- [WARN] channel unionpay retry <*>（1 次，08:07:00–08:07:00）\n"
	if got != want {
		t.Fatalf("正文:\n%s\n应为:\n%s", got, want)
	}
}

type fakeTopic struct {
	queued []Message
	acked  []string
}

func (f *fakeTopic) Read(_ context.Context, max int) ([]Message, error) {
	n := min(max, len(f.queued))
	out := f.queued[:n]
	f.queued = f.queued[n:]
	return out, nil
}

func (f *fakeTopic) Ack(_ context.Context, ids []string) error {
	f.acked = append(f.acked, ids...)
	return nil
}

type fakeStore map[string]*DayState

func (f fakeStore) Load(_ context.Context, id string) (*DayState, error) {
	if s, ok := f[id]; ok {
		cp := *s
		cp.Entries = slices.Clone(s.Entries)
		return &cp, nil
	}
	return &DayState{}, nil
}
func (f fakeStore) Save(_ context.Context, id string, s *DayState) error { f[id] = s; return nil }
func (f fakeStore) Delete(_ context.Context, id string) error            { delete(f, id); return nil }
func (f fakeStore) List(context.Context) ([]string, error) {
	var out []string
	for k := range f {
		out = append(out, k)
	}
	slices.Sort(out)
	return out, nil
}

type fakePublished map[string]string

func (f fakePublished) All(context.Context) (map[string]string, error) {
	out := map[string]string{}
	for k, v := range f {
		out[k] = v
	}
	return out, nil
}
func (f fakePublished) Set(_ context.Context, id, fp string) error { f[id] = fp; return nil }
func (f fakePublished) Del(_ context.Context, id string) error     { delete(f, id); return nil }

type event struct {
	change ingestionv1.ChangeType
	doc    *publish.Document
	del    string
}

type fakePublisher struct{ events []event }

func (f *fakePublisher) Publish(_ context.Context, d *publish.Document, c ingestionv1.ChangeType, _ time.Time) error {
	f.events = append(f.events, event{change: c, doc: d})
	return nil
}

func (f *fakePublisher) Delete(_ context.Context, id string, _ time.Time) error {
	f.events = append(f.events, event{del: id})
	return nil
}

type env struct {
	topic *fakeTopic
	store fakeStore
	pub   fakePublished
	out   *fakePublisher
	s     *Syncer
}

func newEnv(c *Config) *env {
	e := &env{topic: &fakeTopic{}, store: fakeStore{}, pub: fakePublished{}, out: &fakePublisher{}}
	e.s = &Syncer{Config: c, Source: e.topic, Store: e.store, Published: e.pub, Publisher: e.out,
		BaseURI: "https://logs.example/", Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now }}
	return e
}

func (e *env) run(t *testing.T, msgs ...Message) Stats {
	t.Helper()
	e.topic.queued = append(e.topic.queued, msgs...)
	e.out.events = nil
	st, err := e.s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestSyncFiltersRedactsAndMapsACL(t *testing.T) {
	e := newEnv(cfg())
	st := e.run(t,
		msg("1-0", day(6, 8, 1, 0), "payments-svc", "ERROR", "payment timeout user=13812345678 after 30ms"),
		msg("2-0", day(6, 8, 2, 0), "payments-svc", "INFO", "request served"),         // 级别不入库
		msg("3-0", day(6, 8, 3, 0), "marketing-web", "ERROR", "campaign failed"),      // 未配置的服务
		msg("4-0", day(6, 8, 4, 0), "core-batch", "ERROR", "batch job failed step 4"), // 另一个运维组
		msg("5-0", now.AddDate(0, 0, -40), "payments-svc", "ERROR", "too old"),        // 过了保留期
	)
	if st.Read != 5 || st.Accepted != 2 || st.Upserts != 2 {
		t.Fatalf("统计: %+v", st)
	}
	if !slices.Equal(e.topic.acked, []string{"1-0", "2-0", "3-0", "4-0", "5-0"}) {
		t.Fatalf("读到的消息都要确认（含丢弃的）: %v", e.topic.acked)
	}
	byID := map[string]*publish.Document{}
	for _, ev := range e.out.events {
		byID[ev.doc.DocID] = ev.doc
	}
	p := byID["payments-svc-20261006"]
	if p == nil || byID["core-batch-20261006"] == nil || len(byID) != 2 {
		t.Fatalf("应按服务一天一份文档: %v", byID)
	}
	// 先脱敏再进入管道：连接器自己保存的状态里也只有脱敏后的文字，正文里是模板
	if st := e.store["payments-svc-20261006"]; strings.Contains(p.Text, "13812345678") ||
		!strings.Contains(st.Entries[0].Text, "user=[已脱敏:phone]") {
		t.Fatalf("应先脱敏再入库: %s %+v", p.Text, st)
	}
	if !slices.Equal(p.ACLTokens, []string{"group:sre-oncall"}) || p.SourceType != "log" || p.Classification != "internal" {
		t.Fatalf("权限按服务归属: %+v", p.Meta)
	}
	if !p.UpdatedAt.Equal(day(6, 8, 1, 0)) || p.SourceURI != "https://logs.example/payments-svc?date=2026-10-06" {
		t.Fatalf("发生时间与地址: %v %s", p.UpdatedAt, p.SourceURI)
	}
}

func TestRedeliveryIsIdempotentAndNewMessagesUpdateTheDay(t *testing.T) {
	e := newEnv(cfg())
	first := msg("1-0", day(6, 8, 1, 0), "payments-svc", "ERROR", "payment timeout after 30ms")
	e.run(t, first)
	// 同一条消息再投递一次：偏移量已经记过，正文不变，不发事件
	st := e.run(t, first)
	if st.Duplicates != 1 || len(e.out.events) != 0 {
		t.Fatalf("重复消费应幂等: %+v %v", st, e.out.events)
	}
	// 同一天来了新消息：整份文档按新正文重发
	e.run(t, msg("2-0", day(6, 8, 7, 0), "payments-svc", "WARN", "channel retry 2"))
	if len(e.out.events) != 1 || e.out.events[0].change != ingestionv1.ChangeType_CHANGE_TYPE_UPSERT ||
		!strings.Contains(e.out.events[0].doc.Text, "## 08:05–08:10") {
		t.Fatalf("新消息应更新当天的文档: %v", e.out.events)
	}
}

func TestOwnershipChangeIsACLOnly(t *testing.T) {
	c := cfg()
	e := newEnv(c)
	e.run(t, msg("1-0", day(6, 8, 1, 0), "payments-svc", "ERROR", "payment timeout"))
	svc := c.Services["payments-svc"]
	svc.Groups = []string{"group:sre-oncall", "group:sre-pay"}
	c.Services["payments-svc"] = svc
	st := e.run(t)
	if st.ACLOnly != 1 || e.out.events[0].change != ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY {
		t.Fatalf("只有归属变化应走权限快速通道: %+v", e.out.events)
	}
}

func TestExpiredDaysAreDeletedByPartition(t *testing.T) {
	e := newEnv(cfg())
	e.run(t, msg("1-0", day(6, 8, 1, 0), "payments-svc", "ERROR", "payment timeout"))
	// 时间过去 31 天：这一天整份到期删除，状态一并清掉
	e.s.Now = func() time.Time { return now.AddDate(0, 0, 31) }
	st := e.run(t)
	if st.Expired != 1 || len(e.out.events) != 1 || e.out.events[0].del != "payments-svc-20261006" {
		t.Fatalf("到期应写删除标记: %+v %v", st, e.out.events)
	}
	if len(e.store) != 0 || len(e.pub) != 0 {
		t.Fatalf("到期后状态应清理: %v %v", e.store, e.pub)
	}
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	write := func(s string) string {
		p := filepath.Join(dir, "c.yaml")
		if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	c, err := LoadConfig(write("topic: logs:app\nservices:\n  payments-svc: {groups: [group:sre-oncall], levels: [ERROR]}\n"))
	if err != nil || c.RetentionDays != 30 || c.Classification != "internal" {
		t.Fatalf("默认保留 30 天、内部级: %+v %v", c, err)
	}
	for name, bad := range map[string]string{
		"没有归属":  "topic: t\nservices:\n  a: {levels: [ERROR]}\n",
		"组格式不对": "topic: t\nservices:\n  a: {groups: [sre], levels: [ERROR]}\n",
		"级别不对":  "topic: t\nservices:\n  a: {groups: [group:x], levels: [TRACE]}\n",
		"服务名不对": "topic: t\nservices:\n  A_B: {groups: [group:x], levels: [ERROR]}\n",
		"缺主题":   "services:\n  a: {groups: [group:x], levels: [ERROR]}\n",
	} {
		if _, err := LoadConfig(write(bad)); err == nil {
			t.Errorf("%s 应拒绝", name)
		}
	}
}
