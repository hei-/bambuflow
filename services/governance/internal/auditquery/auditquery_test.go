package auditquery

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/logging"
)

var quiet = logging.New(&bytes.Buffer{}, "t", slog.LevelInfo)

type staticVerifier struct{ p *identityv1.Principal }

func (v staticVerifier) Verify(context.Context, string) (*identityv1.Principal, error) {
	return v.p, nil
}

func as(t *testing.T, roles ...string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer x"))
	var out context.Context
	if _, err := grpcmw.UnaryServer(staticVerifier{&identityv1.Principal{Sub: "audra", Roles: roles}}, quiet)(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/x/y"}, func(ctx context.Context, _ any) (any, error) { out = ctx; return nil, nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

type memBucket struct {
	objs map[string][]byte
	err  error
	gets int
}

func (m *memBucket) List(_ context.Context, prefix string) ([]string, error) {
	var out []string
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, m.err
}

func (m *memBucket) Get(_ context.Context, key string) ([]byte, error) {
	m.gets++
	return m.objs[key], m.err
}

// memIndex 是内存里的审计索引，查询语义与 PG 相同
type memIndex struct {
	rows    map[string]Row
	objects map[string]bool
	state   map[string]string
}

func newIndex() *memIndex {
	return &memIndex{rows: map[string]Row{}, objects: map[string]bool{}, state: map[string]string{}}
}

func (m *memIndex) Indexed(_ context.Context, keys []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, k := range keys {
		if m.objects[k] {
			out[k] = true
		}
	}
	return out, nil
}

func (m *memIndex) Add(_ context.Context, key string, rows []Row) error {
	for _, r := range rows {
		if _, ok := m.rows[r.RecordID]; !ok {
			m.rows[r.RecordID] = r
		}
	}
	m.objects[key] = true
	return nil
}

func (m *memIndex) Find(_ context.Context, f Filter) ([]Row, error) {
	var out []Row
	for _, r := range m.rows {
		doc := f.DocID == "" || slices.Contains(r.ShownDocIDs, f.DocID) || slices.Contains(r.ContextDocIDs, f.DocID) ||
			slices.Contains(r.ListedDocIDs, f.DocID) || r.ObjectID == f.DocID
		after := f.After == nil || r.TS.After(f.After.TS) || (r.TS.Equal(f.After.TS) && r.RecordID > f.After.RecordID)
		if !r.TS.Before(f.From) && !r.TS.After(f.To) && slices.Contains(f.Kinds, r.Kind) && (f.Sub == "" || r.UserID == f.Sub) && doc && after {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, func(a, b Row) int { return cmp.Or(a.TS.Compare(b.TS), strings.Compare(a.RecordID, b.RecordID)) })
	return out[:min(len(out), f.Limit)], nil
}

func (m *memIndex) State(_ context.Context, name string) (string, error) { return m.state[name], nil }
func (m *memIndex) SetState(_ context.Context, name, value string) error {
	m.state[name] = value
	return nil
}
func (m *memIndex) Exclusive(ctx context.Context, fn func(context.Context) error) (bool, error) {
	return true, fn(ctx)
}

type memAudit struct {
	details []any
	fail    bool
}

func (a *memAudit) Write(_ context.Context, action, _ string, detail any) error {
	if a.fail {
		return errors.New("审计桶不可用")
	}
	a.details = append(a.details, action, detail)
	return nil
}

var base = time.Date(2026, 10, 7, 8, 0, 0, 0, time.UTC)

func segmentKey(host string, sealed time.Time) string {
	return fmt.Sprintf("gateway/%s/%s/audit.jsonl.%019d", sealed.Format("2006/01/02"), host, sealed.UnixNano())
}

func lines(recs ...map[string]any) []byte {
	var b bytes.Buffer
	for _, r := range recs {
		x, _ := json.Marshal(r)
		b.Write(append(x, '\n'))
	}
	return b.Bytes()
}

func rec(min int, sub, trace string, extra map[string]any) map[string]any {
	r := map[string]any{"time": base.Add(time.Duration(min) * time.Minute), "trace_id": trace, "sub": sub, "endpoint": "answers", "status": 200}
	for k, v := range extra {
		r[k] = v
	}
	return r
}

func objects() map[string][]byte {
	return map[string][]byte{
		segmentKey("gw-1", base.Add(10*time.Minute)): lines(
			rec(1, "alice", "t1", map[string]any{"shown_chunk_ids": []string{"src_wiki:loan#a1"}, "context_chunk_ids": []string{"src_wiki:loan#a1"}}),
			rec(2, "bob", "t2", nil),
			rec(3, "alice", "t3", map[string]any{"candidate_chunk_ids": []string{"src_wiki:loan#b2"}})),
		// 日期目录之前的旧布局：首次补录时收进来
		"gateway/gw-2/audit.jsonl.1": lines(
			rec(4, "alice", "t4", map[string]any{"listed_doc_ids": []string{"loan"}}),
			rec(-30, "alice", "t-early", nil)),
		segmentKey("gw-1", base.Add(20*time.Minute)): append(lines(
			rec(12, "olga", "t5", map[string]any{"endpoint": "admin-index-versions-list", "admin": map[string]any{"method": "GET", "path": "/v1/admin/index-versions"}})),
			[]byte("{坏行\n")...),
		// 两个月前的记录：跨度不再限 31 天
		segmentKey("gw-1", base.AddDate(0, -2, 0)): lines(rec(-60*24*61, "alice", "t-old", map[string]any{"shown_chunk_ids": []string{"src_wiki:loan#a1"}})),
		"indexing/changes/2026/10/07/0001-a.json": lines(map[string]any{"time": base.Add(5 * time.Minute), "actor": "service:indexing",
			"action": "document.purge", "object_id": "loan", "detail": map[string]any{"reason": "源端已删"}}),
		"governance/changes/2026/10/07/0002-b.json": lines(map[string]any{"time": base.Add(6 * time.Minute), "actor": "ivan",
			"roles": []string{"infosec"}, "action": "source.approve", "object_id": "src_wiki", "trace_id": "t-chg"}),
		"evaluation/replay/2026/10/07/0003-c.jsonl": lines(rec(7, "eval-asker", "t-eval", map[string]any{"eval": true, "gate_id": "gate_1",
			"shown_chunk_ids": []string{"src_wiki:loan#a1"}})),
		// 门禁报告等不是审计记录的对象不进索引
		"evaluation/reports/gate_1.json": []byte(`{}`),
	}
}

func newServer(t *testing.T) (*Server, *memAudit) {
	t.Helper()
	b := &memBucket{objs: objects()}
	ix := newIndex()
	if _, err := (&Indexer{Bucket: b, Index: ix, Log: quiet}).Round(context.Background(), base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	a := &memAudit{}
	return &Server{Bucket: b, Index: ix, Audit: a, Log: quiet}, a
}

func query(sub, doc string, size int32, token string, kinds ...string) *adminv1.QueryAuditRequest {
	return &adminv1.QueryAuditRequest{Sub: sub, DocId: doc, From: timestamppb.New(base.AddDate(0, -3, 0)), To: timestamppb.New(base.Add(time.Hour)),
		PageSize: size, PageToken: token, Kinds: kinds}
}

func traces(r *adminv1.QueryAuditResponse) []string {
	var out []string
	for _, x := range r.GetRecords() {
		out = append(out, cmp.Or(x.GetTraceId(), x.GetAction()))
	}
	return out
}

func TestQueryByUserDocAndTime(t *testing.T) {
	s, a := newServer(t)
	ctx := as(t, roleAuditor)
	all, err := s.QueryAudit(ctx, query("", "", 0, ""))
	// 按时间先后；坏行跳过；评测回放默认不返回
	if err != nil || strings.Join(traces(all), ",") != "t-old,t-early,t1,t2,t3,t4,document.purge,t-chg,t5" {
		t.Fatalf("按时间: %v %v", traces(all), err)
	}
	if adm := all.GetRecords()[8]; adm.GetAdminPath() != "/v1/admin/index-versions" || adm.GetKind() != "access" || adm.GetSource() != "gateway" {
		t.Fatalf("管理审计字段: %v", adm)
	}
	chg := all.GetRecords()[7]
	if chg.GetKind() != "change" || chg.GetSource() != "governance" || chg.GetSub() != "ivan" || chg.GetRoles()[0] != "infosec" ||
		chg.GetObjectId() != "src_wiki" {
		t.Fatalf("变更审计字段: %v", chg)
	}
	byUser, _ := s.QueryAudit(ctx, query("alice", "", 0, ""))
	if strings.Join(traces(byUser), ",") != "t-old,t-early,t1,t3,t4" {
		t.Fatalf("按用户: %v", traces(byUser))
	}
	// 按文档：展示、上下文、列出的文档与变更审计的对象；只在候选里的（t3）不进索引
	byDoc, _ := s.QueryAudit(ctx, query("", "loan", 0, ""))
	if strings.Join(traces(byDoc), ",") != "t-old,t1,t4,document.purge" {
		t.Fatalf("按文档: %v", traces(byDoc))
	}
	in := map[string]string{}
	for _, r := range byDoc.GetRecords() {
		in[cmp.Or(r.GetTraceId(), r.GetAction())] = strings.Join(r.GetMatchedIn(), "+")
	}
	if in["t1"] != "context+shown" || in["t4"] != "listed" || in["document.purge"] != "object" {
		t.Fatalf("出现位置: %v", in)
	}
	if purge := byDoc.GetRecords()[3]; purge.GetDetailJson() != `{"reason":"源端已删"}` || purge.GetSub() != "service:indexing" {
		t.Fatalf("变更审计的依据: %v", purge)
	}
	// 评测回放要显式指定
	ev, _ := s.QueryAudit(ctx, query("", "loan", 0, "", "eval"))
	if strings.Join(traces(ev), ",") != "t-eval" || ev.GetRecords()[0].GetGateId() != "gate_1" || ev.GetRecords()[0].GetSource() != "evaluation" {
		t.Fatalf("评测回放: %v", ev.GetRecords())
	}
	// 每次查询都写变更审计，记下查询条件
	if len(a.details) != 8 || a.details[0] != "audit.query" || a.details[5].(map[string]any)["doc_id"] != "loan" {
		t.Fatalf("查询应写变更审计: %v", a.details)
	}
}

func TestQueryPages(t *testing.T) {
	s, _ := newServer(t)
	ctx := as(t, roleAuditor)
	var got []string
	token := ""
	for range 10 {
		r, err := s.QueryAudit(ctx, query("", "", 2, token))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, traces(r)...)
		if token = r.GetNextPageToken(); token == "" {
			break
		}
	}
	if strings.Join(got, ",") != "t-old,t-early,t1,t2,t3,t4,document.purge,t-chg,t5" {
		t.Fatalf("分页拼起来应与一次查全相同: %v", got)
	}
}

func TestIndexerIsIncrementalAndIdempotent(t *testing.T) {
	b := &memBucket{objs: objects()}
	ix := newIndex()
	x := &Indexer{Bucket: b, Index: ix, Log: quiet}
	ctx := context.Background()
	now := base.Add(time.Hour)
	if n, err := x.Round(ctx, now); err != nil || n != 7 || ix.state[backfillState] == "" {
		t.Fatalf("首次补录全部审计对象：%d %v", n, err)
	}
	rows := len(ix.rows)
	// 之后只看今天与昨天的日期目录；已索引的不再读
	b.gets = 0
	if n, err := x.Round(ctx, now); err != nil || n != 0 || b.gets != 0 {
		t.Fatalf("没有新对象：%d %v，读了 %d 次", n, err, b.gets)
	}
	// 段在另一天重送（本地删除失败）：键不同，记录相同，不重复
	again := segmentKey("gw-1", base.Add(10*time.Minute))
	b.objs[strings.Replace(again, "2026/10/07", "2026/10/08", 1)] = b.objs[again]
	if n, err := x.Round(ctx, now.Add(24*time.Hour)); err != nil || n != 1 || len(ix.rows) != rows {
		t.Fatalf("重送的段：%d %v，%d 行", n, err, len(ix.rows))
	}
	// 换一个实例（新的索引任务，没有内存里的记录）：查库跳过
	if n, err := (&Indexer{Bucket: b, Index: ix, Log: quiet}).Round(ctx, now); err != nil || n != 0 {
		t.Fatalf("已索引的对象不再写：%d %v", n, err)
	}
}

func TestQueryRejections(t *testing.T) {
	s, a := newServer(t)
	if _, err := s.QueryAudit(as(t, "platform_operator"), query("", "", 0, "")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("只有审计岗能查: %v", err)
	}
	ctx := as(t, roleAuditor)
	reversed := query("", "", 0, "")
	reversed.From, reversed.To = reversed.To, reversed.From
	for name, r := range map[string]*adminv1.QueryAuditRequest{
		"缺时间范围": {Sub: "alice"}, "from 晚于 to": reversed, "page_size 越界": query("", "", 201, ""),
		"page_token 不合法": query("", "", 0, "!!"), "类别不认识": query("", "", 0, "", "debug"),
	} {
		if _, err := s.QueryAudit(ctx, r); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}
	a.fail = true
	if _, err := s.QueryAudit(ctx, query("", "", 0, "")); status.Code(err) != codes.Unavailable {
		t.Fatalf("变更审计写不进去就不查: %v", err)
	}
}

func TestDocOfChunk(t *testing.T) {
	for in, want := range map[string]string{"src_wiki:loan#a1": "loan", "src_x:a:b#c#d": "a:b#c", "nocolon": "", "src_x:doc": "doc"} {
		if got := DocOfChunk(in); got != want {
			t.Errorf("%s → %s，期望 %s", in, got, want)
		}
	}
}

func TestRestrictedAccessIsItsOwnKind(t *testing.T) {
	b := &memBucket{objs: map[string][]byte{segmentKey("gw-1", base.Add(10*time.Minute)): lines(
		rec(1, "rita", "t-r", map[string]any{"shown_chunk_ids": []string{"src_aml:list#a1", "src_wiki:loan#a1"}}),
		rec(1, "rita", "t-r", map[string]any{"event": "restricted_access", "shown_chunk_ids": []string{"src_aml:list#a1"}}),
	)}}
	ix := newIndex()
	if _, err := (&Indexer{Bucket: b, Index: ix, Log: quiet}).Round(context.Background(), base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	s := &Server{Bucket: b, Index: ix, Audit: &memAudit{}, Log: quiet}
	ctx := as(t, roleAuditor)
	// 默认只有同一请求的入口审计；受限级访问要显式指定
	def, _ := s.QueryAudit(ctx, query("", "", 0, ""))
	if len(def.GetRecords()) != 1 || def.GetRecords()[0].GetKind() != "access" {
		t.Fatalf("默认: %v", def.GetRecords())
	}
	r, err := s.QueryAudit(ctx, query("", "list", 0, "", "restricted"))
	if err != nil || len(r.GetRecords()) != 1 || r.GetRecords()[0].GetKind() != "restricted" ||
		strings.Join(r.GetRecords()[0].GetShownChunkIds(), ",") != "src_aml:list#a1" {
		t.Fatalf("受限级访问: %v %v", r.GetRecords(), err)
	}
}
