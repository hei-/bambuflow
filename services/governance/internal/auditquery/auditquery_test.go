package auditquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
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

type memSegments struct {
	objs map[string][]byte
	err  error
}

func (m *memSegments) List(_ context.Context, prefix string) ([]string, error) {
	var out []string
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, m.err
}

func (m *memSegments) Get(_ context.Context, key string) ([]byte, error) { return m.objs[key], m.err }

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
	return fmt.Sprintf("gateway/%s/audit.jsonl.%019d", host, sealed.UnixNano())
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

func newServer() (*Server, *memAudit) {
	st := &memSegments{objs: map[string][]byte{
		// 两个实例的段，封存时刻交错；范围之前、范围之后很久的段不扫
		segmentKey("gw-1", base.Add(10*time.Minute)): lines(
			rec(1, "alice", "t1", map[string]any{"shown_chunk_ids": []string{"src_wiki:loan#a1"}, "context_chunk_ids": []string{"src_wiki:loan#a1"}}),
			rec(2, "bob", "t2", nil),
			rec(3, "alice", "t3", map[string]any{"candidate_chunk_ids": []string{"src_wiki:loan#b2"}})),
		segmentKey("gw-2", base.Add(5*time.Minute)): lines(
			rec(4, "alice", "t4", map[string]any{"listed_doc_ids": []string{"loan"}}),
			rec(-30, "alice", "t-early", nil)),
		segmentKey("gw-1", base.Add(20*time.Minute)): append(lines(
			rec(12, "olga", "t5", map[string]any{"endpoint": "admin-index-versions-list", "admin": map[string]any{"method": "GET", "path": "/v1/admin/index-versions"}})),
			[]byte("{坏行\n")...),
		segmentKey("gw-1", base.Add(-2*time.Hour)): lines(rec(-121, "alice", "t-old", nil)),
		segmentKey("gw-1", base.Add(72*time.Hour)): lines(rec(72*60, "alice", "t-late", nil)),
		"governance/changes/2026/10/07/x.json":     []byte(`{"actor":"ivan"}`),
	}}
	a := &memAudit{}
	return &Server{Store: st, Audit: a, Log: quiet}, a
}

func query(sub, doc string, size int32, token string) *adminv1.QueryAuditRequest {
	return &adminv1.QueryAuditRequest{Sub: sub, DocId: doc, From: timestamppb.New(base), To: timestamppb.New(base.Add(time.Hour)),
		PageSize: size, PageToken: token}
}

func traces(r *adminv1.QueryAuditResponse) []string {
	var out []string
	for _, x := range r.GetRecords() {
		out = append(out, x.GetTraceId())
	}
	return out
}

func TestQueryByUserDocAndTime(t *testing.T) {
	s, a := newServer()
	ctx := as(t, roleAuditor)
	all, err := s.QueryAudit(ctx, query("", "", 0, ""))
	// 按段的封存时刻排序：gw-2 的段（5 分）在 gw-1 的段（10 分）之前；范围外的记录不返回，坏行跳过
	if err != nil || strings.Join(traces(all), ",") != "t4,t1,t2,t3,t5" {
		t.Fatalf("按时间: %v %v", traces(all), err)
	}
	if adm := all.GetRecords()[4]; adm.GetAdminPath() != "/v1/admin/index-versions" || adm.GetAdminMethod() != "GET" {
		t.Fatalf("管理审计字段: %v", adm)
	}
	byUser, _ := s.QueryAudit(ctx, query("alice", "", 0, ""))
	if strings.Join(traces(byUser), ",") != "t4,t1,t3" {
		t.Fatalf("按用户: %v", traces(byUser))
	}
	byDoc, _ := s.QueryAudit(ctx, query("", "loan", 0, ""))
	if strings.Join(traces(byDoc), ",") != "t4,t1,t3" {
		t.Fatalf("按文档: %v", traces(byDoc))
	}
	in := map[string]string{}
	for _, r := range byDoc.GetRecords() {
		in[r.GetTraceId()] = strings.Join(r.GetMatchedIn(), "+")
	}
	if in["t1"] != "context+shown" || in["t3"] != "candidate" || in["t4"] != "listed" {
		t.Fatalf("出现位置: %v", in)
	}
	// 每次查询都写变更审计，记下查询条件
	if len(a.details) != 6 || a.details[0] != "audit.query" || a.details[5].(map[string]string)["doc_id"] != "loan" {
		t.Fatalf("查询应写变更审计: %v", a.details)
	}
}

func TestQueryPages(t *testing.T) {
	s, _ := newServer()
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
	if strings.Join(got, ",") != "t4,t1,t2,t3,t5" {
		t.Fatalf("分页拼起来应与一次查全相同: %v", got)
	}
}

func TestQueryRejections(t *testing.T) {
	s, a := newServer()
	if _, err := s.QueryAudit(as(t, "platform_operator"), query("", "", 0, "")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("只有审计岗能查: %v", err)
	}
	ctx := as(t, roleAuditor)
	wide := query("", "", 0, "")
	wide.To = timestamppb.New(base.Add(32 * 24 * time.Hour))
	for name, r := range map[string]*adminv1.QueryAuditRequest{
		"缺时间范围": {Sub: "alice"}, "跨度超过 31 天": wide, "page_size 越界": query("", "", 201, ""),
		"page_token 不合法": query("", "", 0, "!!"), "page_token 指向不存在的段": query("", "", 0, "eyJrIjoibm9wZSIsImwiOjB9"),
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
