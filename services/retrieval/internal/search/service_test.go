package search

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/policy"
	"github.com/hei-/bambuflow/services/retrieval/internal/guard"
)

const model = "bge-m3@test"

// fakeStore 记录被调用的方法；errs 按方法名注入故障。Dense 按过滤条件过滤内存中的块，模拟后端下推。
type fakeStore struct {
	chunks map[string]Candidate // chunk_id → 块
	acl    map[string]ACL       // doc_id → 召回时的权限
	now    map[string]ACL       // doc_id → 组装时的最新权限（模拟召回后权限变化）；缺省同 acl
	groups map[string][]string
	// doc_id → 文档元数据；没有的文档各字段都缺失
	meta map[string]*commonv1.DocMetadata
	errs map[string]error
	// 被紧急拒绝的租户
	deniedTenants map[string]bool
	// 分区授权：grantee token → 可见的分区 token
	partitions map[string][]string
	// 图谱：实体与边（边的证据块是 chunks 里的块）
	entities []Entity
	edges    []fakeEdge
	// 两路召回并发调用存储，记录调用要加锁（真实的 pgx 连接池本身并发安全）
	mu    sync.Mutex
	calls []string
}

func (s *fakeStore) call(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, name)
	return s.errs[name]
}

func (s *fakeStore) LiveVersion(context.Context) (string, string, error) {
	return "ix-1", model, s.call("LiveVersion")
}

func (s *fakeStore) VersionModel(_ context.Context, version string) (string, error) {
	if err := s.call("VersionModel:" + version); err != nil {
		return "", err
	}
	if version == "ix-gone" {
		return "", ErrNoLiveIndex
	}
	return model, nil
}

func (s *fakeStore) Dense(_ context.Context, _ string, f Filter, _ []float32, k int) ([]Candidate, error) {
	if err := s.call("Dense"); err != nil {
		return nil, err
	}
	return s.recallAll(f, k), nil
}

// recallAll 是按过滤条件可见的全部块，按 chunk_id 排序取前 k 个（稠密、稀疏两路的模拟）。
func (s *fakeStore) recallAll(f Filter, k int) []Candidate {
	var ids []string
	for id, c := range s.chunks {
		if f.Check(s.acl[c.DocID]) == retrievalv1.AccessReason_ACCESS_REASON_ALLOWED && metaMatch(f.Meta, s.meta[c.DocID]) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	var out []Candidate
	for _, id := range ids[:min(k, len(ids))] {
		out = append(out, s.chunks[id])
	}
	return out
}

// Keyword 与 Dense 用同样的过滤，再要求正文含任一词。
func (s *fakeStore) Keyword(_ context.Context, _ string, f Filter, terms []string, k int) ([]Candidate, error) {
	if err := s.call("Keyword"); err != nil {
		return nil, err
	}
	var ids []string
	for id, c := range s.chunks {
		hit := slices.ContainsFunc(terms, func(t string) bool { return strings.Contains(c.Text, t) })
		if hit && f.Check(s.acl[c.DocID]) == retrievalv1.AccessReason_ACCESS_REASON_ALLOWED && metaMatch(f.Meta, s.meta[c.DocID]) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	var out []Candidate
	for _, id := range ids[:min(k, len(ids))] {
		out = append(out, s.chunks[id])
	}
	return out, nil
}

// Identifier 与 Dense 用同样的过滤，再要求正文或标题含任一标识符（不区分大小写）。
func (s *fakeStore) Identifier(_ context.Context, _ string, f Filter, ids []string, k int) ([]Candidate, error) {
	if err := s.call("Identifier"); err != nil {
		return nil, err
	}
	var hits []string
	for id, c := range s.chunks {
		hit := slices.ContainsFunc(ids, func(x string) bool {
			x = strings.ToUpper(x)
			return strings.Contains(strings.ToUpper(c.Text), x) || strings.Contains(strings.ToUpper(c.Title), x)
		})
		if hit && f.Check(s.acl[c.DocID]) == retrievalv1.AccessReason_ACCESS_REASON_ALLOWED && metaMatch(f.Meta, s.meta[c.DocID]) {
			hits = append(hits, id)
		}
	}
	slices.Sort(hits)
	var out []Candidate
	for _, id := range hits[:min(k, len(hits))] {
		c := s.chunks[id]
		c.RouteScore = 1
		out = append(out, c)
	}
	return out, nil
}

func (s *fakeStore) latest(docID string) (ACL, bool) {
	a, ok := s.now[docID]
	if !ok {
		a, ok = s.acl[docID]
	}
	a.DocID = docID
	return a, ok
}

func (s *fakeStore) DocACL(_ context.Context, docIDs []string) (map[string]ACL, error) {
	if err := s.call("DocACL"); err != nil {
		return nil, err
	}
	out := map[string]ACL{}
	for _, id := range docIDs {
		if a, ok := s.latest(id); ok {
			out[id] = a
		}
	}
	return out, nil
}

func (s *fakeStore) ChunkACL(_ context.Context, chunkIDs []string) (map[string]ACL, error) {
	if err := s.call("ChunkACL"); err != nil {
		return nil, err
	}
	out := map[string]ACL{}
	for _, id := range chunkIDs {
		if c, ok := s.chunks[id]; ok {
			if a, ok := s.latest(c.DocID); ok {
				out[id] = a
			}
		}
	}
	return out, nil
}

// visibleDocs 按过滤条件取可见文档，按标题（这里用 doc_id 代替）排序。
func (s *fakeStore) visibleDocs(f Filter) []DocSummary {
	var out []DocSummary
	for id, a := range s.acl {
		md := s.meta[id]
		if f.Check(a) == retrievalv1.AccessReason_ACCESS_REASON_ALLOWED && metaMatch(f.Meta, md) {
			out = append(out, DocSummary{DocID: id, Title: id, SourceID: a.SourceID, DocVersion: 1, Metadata: md})
		}
	}
	slices.SortFunc(out, func(x, y DocSummary) int { return strings.Compare(x.DocID, y.DocID) })
	return out
}

func (s *fakeStore) CountDocs(_ context.Context, _ string, f Filter, limit int) (int64, error) {
	return int64(min(len(s.visibleDocs(f)), limit)), s.call("CountDocs")
}

func (s *fakeStore) ListDocs(_ context.Context, _ string, f Filter, _ bool, offset, limit int) ([]DocSummary, error) {
	docs := s.visibleDocs(f)
	return docs[min(offset, len(docs)):min(offset+limit, len(docs))], s.call("ListDocs")
}

func (s *fakeStore) CountGroups(_ context.Context, _ string, f Filter, by string, _ int) ([]Group, error) {
	counts := map[string]int64{}
	for _, d := range s.visibleDocs(f) {
		v := ""
		if by == "doc_type" {
			v = d.Metadata.GetDocType()
		}
		counts[v]++
	}
	var out []Group
	for v, n := range counts {
		out = append(out, Group{v, n})
	}
	slices.SortFunc(out, func(a, b Group) int { return strings.Compare(a.Value, b.Value) })
	return out, s.call("CountGroups")
}

// metaMatch 是元数据条件在内存里的等价判定（真实的 SQL 在 store.metaWhere）。
func metaMatch(m MetaFilter, md *commonv1.DocMetadata) bool {
	in := func(want []string, v string) bool { return len(want) == 0 || slices.Contains(want, v) }
	if !in(m.DocTypes, md.GetDocType()) || !in(m.Depts, md.GetDept()) {
		return false
	}
	if len(m.TopicCodes) > 0 && !slices.ContainsFunc(md.GetTopicCodes(), func(c string) bool { return slices.Contains(m.TopicCodes, c) }) {
		return false
	}
	missing := md == nil || slices.Contains(md.GetMissing(), m.Missing)
	return m.Missing == "" || missing
}

// Excerpt 与 Dense 用同样的必含条件判定（真实的 SQL 在 store.Excerpt）。
func (s *fakeStore) Excerpt(_ context.Context, _ string, f Filter, docID, chunkID string) (Candidate, bool, error) {
	if err := s.call("Excerpt"); err != nil {
		return Candidate{}, false, err
	}
	c, ok := s.chunks[chunkID]
	a, _ := s.latest(docID)
	if !ok || c.DocID != docID || f.Check(a) != retrievalv1.AccessReason_ACCESS_REASON_ALLOWED {
		return Candidate{}, false, nil
	}
	return c, true, nil
}

func (s *fakeStore) TenantDenied(_ context.Context, tenant string) (bool, error) {
	return s.deniedTenants[tenant], s.call("TenantDenied")
}

func (s *fakeStore) Sparse(_ context.Context, _ string, f Filter, _ map[uint32]float32, k int) ([]Candidate, error) {
	if err := s.call("Sparse"); err != nil {
		return nil, err
	}
	return s.recallAll(f, k), nil
}

func (s *fakeStore) Chunks(_ context.Context, _ string, f Filter, ids []string) (map[string]Candidate, error) {
	if err := s.call("Chunks"); err != nil {
		return nil, err
	}
	out := map[string]Candidate{}
	for _, id := range ids {
		c, ok := s.chunks[id]
		if !ok {
			continue
		}
		if a, _ := s.latest(c.DocID); f.Check(a) == retrievalv1.AccessReason_ACCESS_REASON_ALLOWED {
			out[id] = c
		}
	}
	return out, nil
}

func (s *fakeStore) PartitionTokens(_ context.Context, tokens []string) ([]string, error) {
	var out []string
	for _, t := range tokens {
		out = append(out, s.partitions[t]...)
	}
	return out, s.call("PartitionTokens")
}

func (s *fakeStore) Groups(_ context.Context, username string) ([]string, error) {
	return s.groups[username], s.call("Groups")
}

type fakeEmbedder struct {
	model string
	err   error
	// 每个查询的稀疏向量；为 nil 表示模型不给稀疏向量
	sparse map[uint32]float32
}

func (e fakeEmbedder) Embed(_ context.Context, texts []string) ([]QueryVector, string, error) {
	out := make([]QueryVector, len(texts))
	for i := range out {
		out[i].Sparse = e.sparse
	}
	return out, e.model, e.err
}

// 固定数据：wiki 源三份文档，mail 源一份（不属于 credit 租户）。
func newStore() *fakeStore {
	s := &fakeStore{chunks: map[string]Candidate{}, acl: map[string]ACL{}, now: map[string]ACL{}, groups: map[string][]string{}, errs: map[string]error{}}
	add := func(doc, source string, class int32, tokens ...string) {
		s.acl[doc] = ACL{SourceID: source, Tokens: tokens, Classification: class, Active: true}
		for _, n := range []string{"1", "2"} {
			id := source + ":" + doc + "#" + n
			s.chunks[id] = Candidate{ChunkID: id, DocID: doc, DocVersion: 1, SourceID: source, Text: doc + n}
		}
	}
	add("loan", "wiki", 2, "group:credit-l2")
	add("risk", "wiki", 3, "group:credit-l2")
	add("handbook", "wiki", 1, "group:all-staff")
	add("mail", "mail", 1, "group:credit-l2")
	return s
}

func realPolicy(t *testing.T) *policy.Engine {
	t.Helper()
	src, err := os.ReadFile("../../../../config/policies/global.rego")
	if err != nil {
		t.Fatal(err)
	}
	b, err := policy.Build([]policy.Layer{{Scope: policy.ScopeGlobal, Body: string(src)}, {Scope: "tenant:credit", Body: "deny := set()\n"}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := policy.Compile(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// fakeGrants 是 governance 下发的租户授权；nil 表示拿不到授权数据
type fakeGrants map[string][]string

func (g fakeGrants) Plan(context.Context, string, string, string) *configv1.PipelineConfig {
	return nil
}

// planGrants 在 fakeGrants 之上给出一个租户的 pipeline
type planGrants struct {
	fakeGrants
	plan *configv1.PipelineConfig
}

func (g planGrants) Plan(context.Context, string, string, string) *configv1.PipelineConfig {
	return g.plan
}

func (g fakeGrants) Sources(tenant string) ([]string, string, error) {
	if g == nil {
		return nil, "", errors.New("租户授权不可用")
	}
	return g[tenant], "grants:t", nil
}

func alice() *identityv1.Principal {
	return &identityv1.Principal{Sub: "alice", Tenant: "credit", Dept: "信贷部", Clearance: internal, Groups: []string{"credit-l2"}}
}

type fixedVerifier struct{ p *identityv1.Principal }

func (v fixedVerifier) Verify(context.Context, string) (*identityv1.Principal, error) {
	return v.p, nil
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// invoke 经真实的拦截器调用处理函数，Principal 由 fixedVerifier 给出。
func invoke[Req, Resp any](t *testing.T, p *identityv1.Principal, purpose string, req Req, h func(context.Context, Req) (Resp, error)) (Resp, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	md := metadata.Pairs("authorization", "Bearer x")
	if purpose != "" {
		md.Append("x-call-purpose", purpose)
	}
	ctx = metadata.NewIncomingContext(ctx, md)
	var out Resp
	_, err := grpcmw.UnaryServer(fixedVerifier{p}, quiet)(ctx, req, &grpc.UnaryServerInfo{FullMethod: "/test"},
		func(ctx context.Context, req any) (any, error) {
			r, err := h(ctx, req.(Req))
			out = r
			return r, err
		})
	return out, err
}

func search(t *testing.T, s *Server, p *identityv1.Principal, req *retrievalv1.SearchRequest) (*retrievalv1.SearchResponse, error) {
	t.Helper()
	return invoke(t, p, "answer", req, s.Search)
}

func query(texts ...string) *retrievalv1.SearchRequest {
	r := &retrievalv1.SearchRequest{}
	for i, t := range texts {
		r.Queries = append(r.Queries, &retrievalv1.SubQuery{Id: string(rune('a' + i)), Text: t})
	}
	return r
}

func docs(resp *retrievalv1.SearchResponse) []string {
	var out []string
	for _, h := range resp.GetHits() {
		if !slices.Contains(out, h.GetDocId()) {
			out = append(out, h.GetDocId())
		}
	}
	slices.Sort(out)
	return out
}

func newServer(t *testing.T, st *fakeStore) *Server {
	return &Server{
		Store: st, Embedder: fakeEmbedder{model: model}, Policy: realPolicy(t), Grants: fakeGrants{"credit": {"wiki"}},
		Tokenize: strings.Fields,
		Reranker: &fakeReranker{}, Log: quiet,
		// 与部署默认一致：稀疏召回不开
		Disabled: map[string]bool{"sparse": true},
	}
}

func TestSearchReturnsOnlyVisible(t *testing.T) {
	st := newStore()
	resp, err := search(t, newServer(t, st), alice(), query("额度"))
	if err != nil {
		t.Fatal(err)
	}
	// risk 密级高于 alice，handbook 不在她的群组，mail 不是 credit 租户的数据源
	if got := docs(resp); !slices.Equal(got, []string{"loan"}) {
		t.Fatalf("可见文档 %v", got)
	}
	if resp.GetIndexVersion() != "ix-1" || resp.GetPolicyVersion() == "" || resp.GetEmbeddingModel() != model {
		t.Fatalf("版本号缺失: %q %q", resp.GetIndexVersion(), resp.GetPolicyVersion())
	}
	for _, h := range resp.GetHits() {
		if h.GetClassification() != internal {
			t.Fatalf("密级应取权限表的值: %v", h.GetClassification())
		}
	}
}

func TestInjectionFlagIsReturned(t *testing.T) {
	st := newStore()
	c := st.chunks["wiki:loan#1"]
	c.InjectionFlag = true
	st.chunks["wiki:loan#1"] = c
	s := newServer(t, st)
	resp, err := search(t, s, alice(), query("额度"))
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range resp.GetHits() {
		if h.GetInjectionFlag() != (h.GetChunkId() == "wiki:loan#1") {
			t.Fatalf("命中应原样带可疑指令标记: %s %v", h.GetChunkId(), h.GetInjectionFlag())
		}
	}
	e, err := invoke(t, alice(), "mcp", &retrievalv1.GetExcerptRequest{DocId: "loan", ChunkId: "wiki:loan#1"}, s.GetExcerpt)
	if err != nil || !e.GetInjectionFlag() {
		t.Fatalf("片段应带可疑指令标记: %v %v", e, err)
	}
}

func TestAssemblyRecheckDropsRevokedDocs(t *testing.T) {
	st := newStore()
	st.acl["handbook"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true}
	// 召回之后：loan 被撤权、handbook 打了删除标记，组装复核必须拦下
	st.now["loan"] = ACL{SourceID: "wiki", Tokens: []string{"group:risk-dept"}, Classification: 2, Active: true}
	st.now["handbook"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: false}
	resp, err := search(t, newServer(t, st), alice(), query("额度"))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetHits()) != 0 {
		t.Fatalf("撤权后的块不能返回: %v", docs(resp))
	}
}

// 租户被紧急拒绝：召回与返回前复核都看不到任何文档（governance 详设 4.2）
func TestTenantEmergencyDenial(t *testing.T) {
	st := newStore()
	s := newServer(t, st)
	st.deniedTenants = map[string]bool{"credit": true}
	resp, err := search(t, s, alice(), query("额度"))
	if err != nil || len(resp.GetHits()) != 0 {
		t.Fatalf("被拒绝的租户不应检索到文档: %v %v", docs(resp), err)
	}
	check, err := invoke(t, alice(), "answer", &retrievalv1.CheckAccessRequest{ChunkIds: []string{"wiki:loan#1"}}, s.CheckAccess)
	if err != nil || check.GetResults()[0].GetAllowed() {
		t.Fatalf("返回前复核也应拒绝: %v %v", check, err)
	}
	st.deniedTenants = map[string]bool{"risk": true}
	if resp, err := search(t, s, alice(), query("额度")); err != nil || len(resp.GetHits()) == 0 {
		t.Fatalf("别的租户被拒绝不影响本租户: %v", err)
	}
}

// 分区级数据源（ADR-14）：文档只带分区 token，按分区授权表展开后才可见；授权一撤，召回与返回前复核同时不再放行
func TestPartitionGrantsExpandTokens(t *testing.T) {
	st := newStore()
	st.acl["loan"] = ACL{SourceID: "wiki", Tokens: []string{"part:wiki/制度库"}, Classification: 2, Active: true}
	s := newServer(t, st)
	if resp, err := search(t, s, alice(), query("loan")); err != nil || slices.Contains(docs(resp), "loan") {
		t.Fatalf("没有分区授权时不可见: %v %v", docs(resp), err)
	}
	st.partitions = map[string][]string{"dept:信贷部": {"part:wiki/制度库"}}
	resp, err := search(t, s, alice(), query("loan"))
	if err != nil || !slices.Contains(docs(resp), "loan") {
		t.Fatalf("部门有分区授权时可见: %v %v", docs(resp), err)
	}
	check := func() bool {
		r, err := invoke(t, alice(), "answer", &retrievalv1.CheckAccessRequest{ChunkIds: []string{"wiki:loan#1"}}, s.CheckAccess)
		if err != nil {
			t.Fatal(err)
		}
		return r.GetResults()[0].GetAllowed()
	}
	if !check() {
		t.Fatal("返回前复核按同一张表放行")
	}
	st.partitions = nil
	if check() {
		t.Fatal("撤销分区授权后返回前复核应拒绝")
	}
	st.errs["PartitionTokens"] = errors.New("连不上")
	if _, err := search(t, s, alice(), query("loan")); status.Code(err) != codes.Unavailable {
		t.Fatalf("读不到分区授权时拒绝: %v", err)
	}
}

type localDenials struct{ sources, docs, tenants map[string]bool }

func (l localDenials) Source(id string) bool { return l.sources[id] }
func (l localDenials) Doc(id string) bool    { return l.docs[id] }
func (l localDenials) Tenant(t string) bool  { return l.tenants[t] }

// 备用通道收到的规则（索引库的拒绝名单里没有）：按租户、数据源、文档分别在召回范围与返回前复核生效（governance 详设 8.2）
func TestBackupChannelDenials(t *testing.T) {
	st := newStore()
	st.acl["handbook"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true}
	s := newServer(t, st)
	check := func() []bool {
		t.Helper()
		resp, err := invoke(t, alice(), "answer", &retrievalv1.CheckAccessRequest{ChunkIds: []string{"wiki:loan#1", "wiki:handbook#1"}}, s.CheckAccess)
		if err != nil {
			t.Fatal(err)
		}
		return []bool{resp.GetResults()[0].GetAllowed(), resp.GetResults()[1].GetAllowed()}
	}

	s.Local = localDenials{docs: map[string]bool{"loan": true}}
	resp, err := search(t, s, alice(), query("额度"))
	if err != nil || !slices.Equal(docs(resp), []string{"handbook"}) {
		t.Fatalf("按文档拒绝后只剩 handbook: %v %v", docs(resp), err)
	}
	if got := check(); !slices.Equal(got, []bool{false, true}) {
		t.Fatalf("返回前复核应拦下被拒绝的文档: %v", got)
	}
	list, err := invoke(t, alice(), "answer", &retrievalv1.ListDocumentsRequest{}, s.ListDocuments)
	if err != nil || slices.ContainsFunc(list.GetDocuments(), func(d *retrievalv1.DocumentSummary) bool { return d.GetDocId() == "loan" }) {
		t.Fatalf("列举不应包含被拒绝的文档: %v %v", list, err)
	}

	s.Local = localDenials{sources: map[string]bool{"wiki": true}}
	if resp, err := search(t, s, alice(), query("额度")); err != nil || len(resp.GetHits()) != 0 {
		t.Fatalf("按数据源拒绝后应检索不到: %v %v", docs(resp), err)
	}
	if got := check(); !slices.Equal(got, []bool{false, false}) {
		t.Fatalf("按数据源拒绝，复核应全部拦下: %v", got)
	}

	s.Local = localDenials{tenants: map[string]bool{"credit": true}}
	if resp, err := search(t, s, alice(), query("额度")); err != nil || len(resp.GetHits()) != 0 {
		t.Fatalf("按租户拒绝后应检索不到: %v %v", docs(resp), err)
	}
}

// 取消授权：授权里没有的数据源召回与复核都看不到；policy_version 带上授权快照的版本
func TestOnlyGrantedSourcesAreSearched(t *testing.T) {
	st := newStore()
	s := newServer(t, st)
	resp, err := search(t, s, alice(), query("额度"))
	if err != nil || len(resp.GetHits()) == 0 || !strings.HasSuffix(resp.GetPolicyVersion(), "+grants:t") {
		t.Fatalf("授权的数据源应可检索: %v %v", resp, err)
	}
	s.Grants = fakeGrants{"credit": {}}
	if resp, err := search(t, s, alice(), query("额度")); err != nil || len(resp.GetHits()) != 0 {
		t.Fatalf("取消授权后不应检索到: %v %v", docs(resp), err)
	}
	check, err := invoke(t, alice(), "answer", &retrievalv1.CheckAccessRequest{ChunkIds: []string{"wiki:loan#1"}}, s.CheckAccess)
	if err != nil || check.GetResults()[0].GetAllowed() {
		t.Fatalf("返回前复核也应拒绝: %v %v", check, err)
	}
}

func TestTopKAppliedAfterRecheck(t *testing.T) {
	st := newStore()
	st.acl["handbook"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true}
	st.now["handbook"] = ACL{SourceID: "wiki", Tokens: nil, Classification: 1, Active: true}
	req := query("额度")
	req.TopK = 2
	resp, err := search(t, newServer(t, st), alice(), req)
	if err != nil {
		t.Fatal(err)
	}
	// handbook 的两块排在前面但被拦下，top_k 仍由 loan 的两块补满
	if len(resp.GetHits()) != 2 || !slices.Equal(docs(resp), []string{"loan"}) {
		t.Fatalf("得到 %d 条 %v", len(resp.GetHits()), docs(resp))
	}
}

func TestCallerFiltersOnlyNarrow(t *testing.T) {
	req := query("额度")
	req.Filters = &commonv1.Filters{SourceIds: []string{"mail"}}
	resp, err := search(t, newServer(t, newStore()), alice(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetHits()) != 0 {
		t.Fatalf("调用方指定未授权的数据源不能扩大范围: %v", docs(resp))
	}
}

func TestGroupsRefExpandedFromIdentityStore(t *testing.T) {
	st := newStore()
	st.groups["alice"] = []string{"credit-l2", "all-staff"}
	p := alice()
	p.Groups, p.GroupsRef = nil, "alice"
	resp, err := search(t, newServer(t, st), p, query("额度"))
	if err != nil {
		t.Fatal(err)
	}
	if got := docs(resp); !slices.Equal(got, []string{"handbook", "loan"}) {
		t.Fatalf("应按身份库群组展开: %v", got)
	}

	p.GroupsRef = "bob"
	if _, err := search(t, newServer(t, st), p, query("额度")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("借用他人的群组引用应当拒绝: %v", err)
	}
}

// 所有 fail-closed 场景都只返回错误，不返回任何块。
func TestFailClosed(t *testing.T) {
	boom := errors.New("boom")
	noDept := alice()
	noDept.Dept = ""
	otherTenant := alice()
	otherTenant.Tenant = "hr"
	refAlice := alice()
	refAlice.Groups, refAlice.GroupsRef = nil, "alice"
	cases := []struct {
		name string
		edit func(*Server, *fakeStore)
		p    *identityv1.Principal
		req  *retrievalv1.SearchRequest
		want codes.Code
	}{
		{"策略包缺失", func(s *Server, _ *fakeStore) { s.Policy = nil }, alice(), nil, codes.Unavailable},
		{"策略越权给出更高密级", func(s *Server, _ *fakeStore) { s.Policy = raisingPolicy{s.Policy} }, alice(), nil, codes.PermissionDenied},
		{"策略评估出错", func(s *Server, _ *fakeStore) { s.Policy = brokenPolicy{} }, alice(), nil, codes.Unavailable},
		{"Principal 缺部门", nil, noDept, nil, codes.PermissionDenied},
		{"未配置的租户", nil, otherTenant, nil, codes.PermissionDenied},
		{"读线上版本失败", func(_ *Server, st *fakeStore) { st.errs["LiveVersion"] = boom }, alice(), nil, codes.Unavailable},
		{"还没有线上版本", func(_ *Server, st *fakeStore) { st.errs["LiveVersion"] = ErrNoLiveIndex }, alice(), nil, codes.Unavailable},
		{"两路召回都失败", func(s *Server, st *fakeStore) {
			st.errs["Dense"], st.errs["Keyword"] = boom, boom
		}, alice(), nil, codes.Unavailable},
		{"组装复核读权限表失败", func(_ *Server, st *fakeStore) { st.errs["DocACL"] = boom }, alice(), nil, codes.Unavailable},
		{"身份库不可用", func(_ *Server, st *fakeStore) { st.errs["Groups"] = boom }, refAlice, nil, codes.Unavailable},
		{"读不到紧急拒绝名单", func(_ *Server, st *fakeStore) { st.errs["TenantDenied"] = boom }, alice(), nil, codes.Unavailable},
		{"拿不到租户授权", func(s *Server, _ *fakeStore) { s.Grants = fakeGrants(nil) }, alice(), nil, codes.Unavailable},
		{"生效日期格式不对", nil, alice(), &retrievalv1.SearchRequest{
			Queries: []*retrievalv1.SubQuery{{Id: "a", Text: "额度"}}, Filters: &commonv1.Filters{EffectiveTo: "去年"},
		}, codes.InvalidArgument},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := newStore()
			s := newServer(t, st)
			if c.edit != nil {
				c.edit(s, st)
			}
			req := c.req
			if req == nil {
				req = query("额度")
			}
			resp, err := search(t, s, c.p, req)
			if status.Code(err) != c.want || resp != nil {
				t.Fatalf("得到 %v（响应 %v），应为 %v", err, resp, c.want)
			}
		})
	}
}

type raisingPolicy struct{ Policy }

func (p raisingPolicy) Decide(ctx context.Context, in policy.Input, granted []string) (policy.Decision, error) {
	d, err := p.Policy.Decide(ctx, in, granted)
	d.Filter.MaxClassification = 4
	return d, err
}

type brokenPolicy struct{}

func (brokenPolicy) Decide(context.Context, policy.Input, []string) (policy.Decision, error) {
	return policy.Decision{}, errors.New("eval error")
}
func (brokenPolicy) Version() string { return "broken" }

func TestInvalidRequests(t *testing.T) {
	s := newServer(t, newStore())
	long := make([]rune, maxQueryLen+1)
	for i := range long {
		long[i] = '字'
	}
	cases := map[string]*retrievalv1.SearchRequest{
		"没有子查询":    {},
		"子查询过多":    query("a", "b", "c", "d", "e"),
		"空子查询":     query("  "),
		"子查询过长":    query(string(long)),
		"top_k 越界": {Queries: query("a").Queries, TopK: 51},
		"top_k 为负": {Queries: query("a").Queries, TopK: -1},
		"计划版本不合法":  {Queries: query("a").Queries, PlanVersion: "v2"},
	}
	for name, req := range cases {
		if _, err := search(t, s, alice(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: 得到 %v", name, err)
		}
	}
	if _, err := invoke(t, alice(), "", query("a"), s.Search); status.Code(err) != codes.InvalidArgument {
		t.Errorf("缺少 x-call-purpose: 得到 %v", err)
	}
	if _, err := invoke(t, alice(), "batch-export", query("a"), s.Search); status.Code(err) != codes.InvalidArgument {
		t.Errorf("未知的 x-call-purpose: 得到 %v", err)
	}
}

func TestCheckAccess(t *testing.T) {
	st := newStore()
	st.acl["gone"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: false}
	st.chunks["wiki:gone#1"] = Candidate{ChunkID: "wiki:gone#1", DocID: "gone"}
	s := newServer(t, st)
	ids := []string{"wiki:loan#1", "wiki:risk#1", "wiki:handbook#1", "mail:mail#1", "wiki:gone#1", "wiki:nope#1"}
	resp, err := invoke(t, alice(), "answer", &retrievalv1.CheckAccessRequest{ChunkIds: ids}, s.CheckAccess)
	if err != nil {
		t.Fatal(err)
	}
	want := []retrievalv1.AccessReason{
		retrievalv1.AccessReason_ACCESS_REASON_ALLOWED,
		retrievalv1.AccessReason_ACCESS_REASON_CLASSIFICATION_EXCEEDED,
		retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED,
		retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED,
		retrievalv1.AccessReason_ACCESS_REASON_TOMBSTONED,
		retrievalv1.AccessReason_ACCESS_REASON_NOT_FOUND,
	}
	for i, r := range resp.GetResults() {
		if r.GetChunkId() != ids[i] || r.GetReason() != want[i] || r.GetAllowed() != (i == 0) {
			t.Errorf("%s: 得到 %v allowed=%v", ids[i], r.GetReason(), r.GetAllowed())
		}
	}
	if resp.GetCheckedAt() == nil || resp.GetPolicyVersion() == "" {
		t.Fatal("缺少 checked_at 或 policy_version")
	}

	st.errs["ChunkACL"] = errors.New("boom")
	if _, err := invoke(t, alice(), "answer", &retrievalv1.CheckAccessRequest{ChunkIds: ids}, s.CheckAccess); status.Code(err) != codes.Unavailable {
		t.Fatalf("权限表不可用应当拒绝: %v", err)
	}
	s.Policy = nil
	if _, err := invoke(t, alice(), "answer", &retrievalv1.CheckAccessRequest{ChunkIds: ids}, s.CheckAccess); status.Code(err) != codes.Unavailable {
		t.Fatalf("策略包缺失应当拒绝: %v", err)
	}
	for name, ids := range map[string][]string{"空列表": nil, "超过 50 个": make([]string, 51), "空 ID": {""}} {
		if _, err := invoke(t, alice(), "answer", &retrievalv1.CheckAccessRequest{ChunkIds: ids}, s.CheckAccess); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: 得到 %v", name, err)
		}
	}
}

// 按文档复核与按块复核的判定相同；不存在的文档为 not_found，读不到权限表时整批拒绝。
func TestCheckDocumentAccess(t *testing.T) {
	st := newStore()
	st.acl["gone"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: false}
	st.acl["denied"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true, Denied: true}
	s := newServer(t, st)
	ids := []string{"loan", "risk", "handbook", "mail", "gone", "nope", "denied"}
	resp, err := invoke(t, alice(), "batch", &retrievalv1.CheckDocumentAccessRequest{DocIds: ids}, s.CheckDocumentAccess)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := invoke(t, alice(), "batch", &retrievalv1.CheckAccessRequest{ChunkIds: []string{
		"wiki:loan#1", "wiki:risk#1", "wiki:handbook#1", "mail:mail#1",
	}}, s.CheckAccess)
	if err != nil {
		t.Fatal(err)
	}
	want := []retrievalv1.AccessReason{
		retrievalv1.AccessReason_ACCESS_REASON_ALLOWED,
		retrievalv1.AccessReason_ACCESS_REASON_CLASSIFICATION_EXCEEDED,
		retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED,
		retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED,
		retrievalv1.AccessReason_ACCESS_REASON_TOMBSTONED,
		retrievalv1.AccessReason_ACCESS_REASON_NOT_FOUND,
	}
	for i, r := range resp.GetResults() {
		if r.GetDocId() != ids[i] || r.GetAllowed() != (i == 0) {
			t.Errorf("%s: 得到 %v allowed=%v", ids[i], r.GetReason(), r.GetAllowed())
		}
		if i < len(want) && r.GetReason() != want[i] {
			t.Errorf("%s: 得到 %v，应为 %v", ids[i], r.GetReason(), want[i])
		}
		if i < len(chunks.GetResults()) && chunks.GetResults()[i].GetReason() != r.GetReason() {
			t.Errorf("%s: 按文档与按块复核的判定不一致", ids[i])
		}
	}
	if resp.GetCheckedAt() == nil || resp.GetPolicyVersion() == "" {
		t.Fatal("缺少 checked_at 或 policy_version")
	}

	st.errs["DocACL"] = errors.New("boom")
	if _, err := invoke(t, alice(), "batch", &retrievalv1.CheckDocumentAccessRequest{DocIds: ids}, s.CheckDocumentAccess); status.Code(err) != codes.Unavailable {
		t.Fatalf("权限表不可用应当拒绝: %v", err)
	}
	delete(st.errs, "DocACL")
	if _, err := invoke(t, alice(), "", &retrievalv1.CheckDocumentAccessRequest{DocIds: ids}, s.CheckDocumentAccess); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("缺少 x-call-purpose 应拒绝: %v", err)
	}
	for name, ids := range map[string][]string{"空列表": nil, "超过 50 个": make([]string, 51), "空 ID": {""}} {
		if _, err := invoke(t, alice(), "batch", &retrievalv1.CheckDocumentAccessRequest{DocIds: ids}, s.CheckDocumentAccess); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: 得到 %v", name, err)
		}
	}
	s.Policy = nil
	if _, err := invoke(t, alice(), "batch", &retrievalv1.CheckDocumentAccessRequest{DocIds: ids}, s.CheckDocumentAccess); status.Code(err) != codes.Unavailable {
		t.Fatalf("策略包缺失应当拒绝: %v", err)
	}
}

// 单路召回失败只降级：另一路的结果照常返回，降级原因写进响应。
func TestSingleRouteFailureDegrades(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name   string
		edit   func(*Server, *fakeStore)
		stage  string
		reason string
	}{
		{"稠密后端失败", func(_ *Server, st *fakeStore) { st.errs["Dense"] = boom }, "dense", "backend_error"},
		{"嵌入模型不可用", func(s *Server, _ *fakeStore) { s.Embedder = fakeEmbedder{err: boom} }, "dense", "embed_unavailable"},
		{"嵌入模型版本不一致", func(s *Server, _ *fakeStore) { s.Embedder = fakeEmbedder{model: "bge-m3@other"} }, "dense", "model_mismatch"},
		{"关键词后端失败", func(_ *Server, st *fakeStore) { st.errs["Keyword"] = boom }, "keyword", "backend_error"},
		{"分词器不可用", func(s *Server, _ *fakeStore) { s.Tokenize = nil }, "keyword", "segmenter_unavailable"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := newStore()
			s := newServer(t, st)
			c.edit(s, st)
			resp, err := search(t, s, alice(), query("loan1"))
			if err != nil {
				t.Fatal(err)
			}
			d := resp.GetDegraded()
			if len(d) != 1 || d[0].GetStage() != c.stage || d[0].GetReason() != c.reason {
				t.Fatalf("降级标记 %v", d)
			}
			if !slices.Equal(docs(resp), []string{"loan"}) {
				t.Fatalf("另一路的结果应照常返回且仍只含可见文档: %v", docs(resp))
			}
		})
	}
}

func TestKeywordRouteIsFilteredAndFused(t *testing.T) {
	st := newStore()
	resp, err := search(t, newServer(t, st), alice(), query("risk1 loan2"))
	if err != nil {
		t.Fatal(err)
	}
	// risk 文档密级高于 alice：关键词命中也不能返回；两路结果融合后只剩可见块
	if !slices.Equal(docs(resp), []string{"loan"}) || len(resp.GetDegraded()) != 0 {
		t.Fatalf("得到 %v，降级 %v", docs(resp), resp.GetDegraded())
	}
	if !slices.Contains(st.calls, "Keyword") || !slices.Contains(st.calls, "Dense") {
		t.Fatalf("两路都应被调用: %v", st.calls)
	}
}

// fakeReranker 默认保持输入顺序（分数递减）；prefer 非空时含该文本的段落得高分。
type fakeReranker struct {
	prefer string
	err    error
	calls  int
}

func (r *fakeReranker) Rerank(_ context.Context, queries, passages []string) ([]float64, string, error) {
	r.calls++
	if r.err != nil {
		return nil, "", r.err
	}
	out := make([]float64, len(passages))
	for i, p := range passages {
		out[i] = 0.5 - float64(i)/float64(4*len(passages))
		if r.prefer != "" && strings.Contains(p, r.prefer) {
			out[i] = 0.99
		}
	}
	return out, "rerank@test", nil
}

func TestRerank(t *testing.T) {
	st := newStore()
	st.acl["handbook"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true}
	s := newServer(t, st)
	rr := &fakeReranker{prefer: "handbook2"}
	s.Reranker = rr
	resp, err := search(t, s, alice(), query("额度"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetHits()[0].GetChunkId() != "wiki:handbook#2" || resp.GetHits()[0].GetScore() != 0.99 {
		t.Fatalf("精排应决定顺序与分数: %v", resp.GetHits()[0])
	}
	if resp.GetRerankModel() != "rerank@test" || len(resp.GetDegraded()) != 0 {
		t.Fatalf("应记录精排模型版本: %q %v", resp.GetRerankModel(), resp.GetDegraded())
	}

	// 仅检索默认不精排；显式要求时精排
	calls := rr.calls
	if _, err := invoke(t, alice(), "search", query("额度"), s.Search); err != nil || rr.calls != calls {
		t.Fatalf("search 默认不应精排: %v", err)
	}
	yes := true
	req := query("额度")
	req.Rerank = &yes
	if _, err := invoke(t, alice(), "search", req, s.Search); err != nil || rr.calls != calls+1 {
		t.Fatalf("显式 rerank=true 应精排: %v", err)
	}
	// 批量条目的回答与同步问答一样默认精排
	if _, err := invoke(t, alice(), "batch", query("额度"), s.Search); err != nil || rr.calls != calls+2 {
		t.Fatalf("batch 默认应精排: %v", err)
	}

	// 精排不可用：按融合顺序返回，标记降级，仍只含可见文档
	s.Reranker = &fakeReranker{err: errors.New("down")}
	resp, err = search(t, s, alice(), query("额度"))
	if err != nil {
		t.Fatal(err)
	}
	d := resp.GetDegraded()
	if len(d) != 1 || d[0].GetStage() != "rerank" || d[0].GetReason() != "unavailable" || resp.GetRerankModel() != "" {
		t.Fatalf("降级标记 %v", d)
	}
}

func TestRerankSkippedNearDeadline(t *testing.T) {
	s := newServer(t, newStore())
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, model, reason := s.rerank(ctx, query("a").GetQueries(), []scored{{Candidate: Candidate{ChunkID: "x"}}})
	if reason != "skipped_deadline" || model != "" {
		t.Fatalf("剩余时间不足应跳过精排: %q", reason)
	}
}

func TestHitTextIsMaskedByDefault(t *testing.T) {
	st := newStore()
	c := st.chunks["wiki:loan#1"]
	c.Text = "客户手机 13812345678 已登记"
	st.chunks["wiki:loan#1"] = c
	resp, err := search(t, newServer(t, st), alice(), query("额度"))
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range resp.GetHits() {
		if strings.Contains(h.GetText(), "13812345678") {
			t.Fatalf("PII 应掩码: %q", h.GetText())
		}
	}
}

func TestOpenCircuitSkipsRouteWithoutCallingBackend(t *testing.T) {
	st := newStore()
	s := newServer(t, st)
	s.Breakers = guard.NewBreakers(150 * time.Millisecond)
	st.errs["Dense"] = errors.New("boom")
	for range 20 {
		if _, err := search(t, s, alice(), query("loan1")); err != nil {
			t.Fatal(err)
		}
	}
	st.calls = nil
	resp, err := search(t, s, alice(), query("loan1"))
	if err != nil {
		t.Fatal(err)
	}
	d := resp.GetDegraded()
	if len(d) != 1 || d[0].GetStage() != "dense" || d[0].GetReason() != "circuit_open" {
		t.Fatalf("降级标记 %v", d)
	}
	if slices.Contains(st.calls, "Dense") || !slices.Equal(docs(resp), []string{"loan"}) {
		t.Fatalf("熔断后不应再调用稠密后端，关键词路照常: %v %v", st.calls, docs(resp))
	}
}

func TestAdmissionRejectsWithResourceExhausted(t *testing.T) {
	s := newServer(t, newStore())
	s.Admission = &guard.Admission{TenantRate: rate.Limit(0.001), TenantBurst: 1, MaxInflight: 10}
	if _, err := search(t, s, alice(), query("额度")); err != nil {
		t.Fatal(err)
	}
	_, err := search(t, s, alice(), query("额度"))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("超出租户限流应返回 RESOURCE_EXHAUSTED: %v", err)
	}
}

func TestCountIgnoresDocsTheCallerCannotSee(t *testing.T) {
	st := newStore()
	s := newServer(t, st)
	count := func() *retrievalv1.CountDocumentsResponse {
		t.Helper()
		resp, err := invoke(t, alice(), "answer", &retrievalv1.CountDocumentsRequest{}, s.CountDocuments)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	before := count()
	if before.GetTotalCount() != 1 || !before.GetTotalIsExact() || before.GetPolicyVersion() == "" {
		t.Fatalf("alice 只能看到 loan: %v", before)
	}
	// 无权文档（密级过高、不在群组、不是租户的数据源、已删除、在拒绝名单）对计数的影响为 0
	st.acl["secret"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 3, Active: true}
	st.acl["other"] = ACL{SourceID: "wiki", Tokens: []string{"group:risk-dept"}, Classification: 1, Active: true}
	st.acl["mail2"] = ACL{SourceID: "mail", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true}
	st.acl["gone"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: false}
	st.acl["denied"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true, Denied: true}
	if after := count(); after.GetTotalCount() != 1 {
		t.Fatalf("无权文档不能影响计数: %d", after.GetTotalCount())
	}
	st.acl["visible"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true}
	if after := count(); after.GetTotalCount() != 2 {
		t.Fatalf("可见文档应计入: %d", after.GetTotalCount())
	}
}

func TestListDocumentsPagesWithinVersion(t *testing.T) {
	st := newStore()
	for _, id := range []string{"a1", "a2"} {
		st.acl[id] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true}
	}
	s := newServer(t, st)
	var got []string
	token := ""
	for range 5 {
		resp, err := invoke(t, alice(), "search", &retrievalv1.ListDocumentsRequest{PageSize: 2, PageToken: token}, s.ListDocuments)
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetTotalCount() != 3 {
			t.Fatalf("总数应与计数同口径: %d", resp.GetTotalCount())
		}
		for _, d := range resp.GetDocuments() {
			got = append(got, d.GetDocId())
		}
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	if !slices.Equal(got, []string{"a1", "a2", "loan"}) {
		t.Fatalf("分页结果 %v", got)
	}
	stale := encodePageToken("ix-0", 2)
	_, err := invoke(t, alice(), "search", &retrievalv1.ListDocumentsRequest{PageToken: stale}, s.ListDocuments)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("索引版本变化后旧令牌应失效: %v", err)
	}
	_, err = invoke(t, alice(), "search", &retrievalv1.CountDocumentsRequest{Filters: &commonv1.Filters{EffectiveFrom: "2025/01/01"}}, s.CountDocuments)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("日期格式不对应拒绝: %v", err)
	}
}

func TestCountByMetadataWithGroupsAndGaps(t *testing.T) {
	st := newStore()
	for _, id := range []string{"p1", "p2", "n1", "x1"} {
		st.acl[id] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true}
	}
	st.meta = map[string]*commonv1.DocMetadata{
		"p1":   {DocType: "DT-POLICY", Missing: []string{"dept"}},
		"p2":   {DocType: "DT-POLICY", Dept: "DEPT-CREDIT"},
		"n1":   {DocType: "DT-NOTICE"},
		"x1":   {Missing: []string{"doc_type"}},
		"loan": {Missing: []string{"doc_type"}},
		// alice 看不到 risk（密级）：它缺类型也不能计入缺口
		"risk": {Missing: []string{"doc_type"}},
	}
	s := newServer(t, st)
	req := &retrievalv1.CountDocumentsRequest{
		Filters: &commonv1.Filters{DocTypes: []string{"DT-POLICY"}}, GroupBy: retrievalv1.CountGroupBy_COUNT_GROUP_BY_DOC_TYPE,
	}
	resp, err := invoke(t, alice(), "answer", req, s.CountDocuments)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetTotalCount() != 2 {
		t.Fatalf("制度类可见 2 份: %d", resp.GetTotalCount())
	}
	if g := resp.GetGroups(); len(g) != 1 || g[0].GetValue() != "DT-POLICY" || g[0].GetCount() != 2 {
		t.Fatalf("分组: %v", g)
	}
	// 另有 2 份可见文档没有类型（x1、loan），可能也属于制度；无权的 risk 不算
	if gaps := resp.GetMetadataGaps(); len(gaps) != 1 || gaps[0].GetField() != "doc_type" || gaps[0].GetCount() != 2 {
		t.Fatalf("缺口: %v", gaps)
	}
}

func TestSearchAppliesMetadataFilters(t *testing.T) {
	st := newStore()
	st.meta = map[string]*commonv1.DocMetadata{"loan": {DocType: "DT-POLICY"}}
	s := newServer(t, st)
	req := query("额度 loan1")
	req.Filters = &commonv1.Filters{DocTypes: []string{"DT-NOTICE"}}
	resp, err := search(t, s, alice(), req)
	if err != nil || len(resp.GetHits()) != 0 {
		t.Fatalf("类型不符的文档不能返回: %v %v", docs(resp), err)
	}
	req.Filters = &commonv1.Filters{DocTypes: []string{"DT-POLICY"}}
	if resp, err = search(t, s, alice(), req); err != nil || !slices.Equal(docs(resp), []string{"loan"}) {
		t.Fatalf("类型相符的文档照常返回: %v %v", docs(resp), err)
	}
}

// 稀疏召回（详设 6.2）：部署开启了这一路才执行，与稠密共用一次嵌入调用；模型不给稀疏向量时这一路降级
func TestSparseRoute(t *testing.T) {
	st := newStore()
	s := newServer(t, st)
	s.Disabled = map[string]bool{"sparse": true}
	if _, err := search(t, s, alice(), query("额度")); err != nil || st.count("Sparse") != 0 {
		t.Fatalf("默认不开: %v", err)
	}
	s.Disabled = nil
	emb := &countingEmbedder{fakeEmbedder: fakeEmbedder{model: model, sparse: map[uint32]float32{7: 0.5}}}
	s.Embedder = emb
	resp, err := search(t, s, alice(), query("额度"))
	if err != nil || st.count("Sparse") != 1 || len(emb.texts) != 1 || len(resp.GetDegraded()) != 0 {
		t.Fatalf("开启后执行、只嵌入一次: %v %d %v %v", err, st.count("Sparse"), emb.texts, resp.GetDegraded())
	}
	s.Embedder = fakeEmbedder{model: model}
	resp, err = search(t, s, alice(), query("额度"))
	if err != nil || len(resp.GetDegraded()) != 1 || resp.GetDegraded()[0].GetStage() != "sparse" ||
		resp.GetDegraded()[0].GetReason() != "sparse_unavailable" {
		t.Fatalf("模型不给稀疏向量时降级: %v %v", err, resp.GetDegraded())
	}
}
