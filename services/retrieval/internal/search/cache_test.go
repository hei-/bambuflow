package search

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	"github.com/hei-/bambuflow/libs/platform-go/policy"
)

type memResults struct {
	mu   sync.Mutex
	m    map[string]*CachedResult
	puts int
}

func (r *memResults) Get(_ context.Context, key string) (*CachedResult, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.m[key]
	return v, ok
}

func (r *memResults) Put(_ context.Context, key string, res *CachedResult, _ []string, _ time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]*CachedResult{}
	}
	r.m[key] = res
	r.puts++
}

func (s *fakeStore) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if c == name {
			n++
		}
	}
	return n
}

func TestResultCacheSkipsRecallAndStillRechecksAccess(t *testing.T) {
	st := newStore()
	s := newServer(t, st)
	rc := &memResults{}
	s.Results = rc
	first, err := search(t, s, alice(), query("额度"))
	if err != nil || rc.puts != 1 || !slices.Equal(docs(first), []string{"loan"}) {
		t.Fatalf("首次检索应写缓存: %v %v %d", docs(first), err, rc.puts)
	}
	dense := st.count("Dense")
	second, err := search(t, s, alice(), query("  额度 "))
	if err != nil || st.count("Dense") != dense || st.count("Chunks") != 1 {
		t.Fatalf("规范化后同一子查询应命中缓存、不再召回: %v %d %d", err, st.count("Dense"), st.count("Chunks"))
	}
	if !slices.Equal(docs(second), docs(first)) || second.GetHits()[0].GetText() != first.GetHits()[0].GetText() {
		t.Fatalf("命中缓存的结果应与原结果一致: %v", docs(second))
	}
	// 权限在缓存之后被收回：命中缓存也按当前权限取块，看不到
	st.acl["loan"] = ACL{SourceID: "wiki", Tokens: []string{"group:other"}, Classification: 2, Active: true}
	if third, err := search(t, s, alice(), query("额度")); err != nil || len(third.GetHits()) != 0 {
		t.Fatalf("缓存滞后也不能越权: %v %v", docs(third), err)
	}
}

func TestRestrictedClearanceAndDegradedResultsAreNotCached(t *testing.T) {
	st := newStore()
	s := newServer(t, st)
	rc := &memResults{}
	s.Results = rc
	p := alice()
	p.Clearance = commonv1.Classification_CLASSIFICATION_RESTRICTED
	if _, err := search(t, s, p, query("额度")); err != nil || rc.puts != 0 {
		t.Fatalf("可能看到受限级内容的请求不缓存: %v %d", err, rc.puts)
	}
	s.Reranker = nil
	if _, err := search(t, s, alice(), query("额度")); err != nil || rc.puts != 0 {
		t.Fatalf("降级（没精排）的结果不缓存: %v %d", err, rc.puts)
	}
}

type countingPolicy struct {
	Policy
	n int
}

func (c *countingPolicy) Decide(ctx context.Context, in policy.Input, granted []string) (policy.Decision, error) {
	c.n++
	return c.Policy.Decide(ctx, in, granted)
}

type memVectors struct {
	m    map[string]QueryVector
	gets int
}

func (v *memVectors) Get(_ context.Context, model string, texts []string) map[int]QueryVector {
	v.gets++
	out := map[int]QueryVector{}
	for i, t := range texts {
		if vec, ok := v.m[model+"|"+t]; ok {
			out[i] = vec
		}
	}
	return out
}

func (v *memVectors) Put(_ context.Context, model string, texts []string, vecs []QueryVector) {
	for i, t := range texts {
		v.m[model+"|"+t] = vecs[i]
	}
}

type countingEmbedder struct {
	fakeEmbedder
	texts []string
}

func (e *countingEmbedder) Embed(ctx context.Context, texts []string) ([]QueryVector, string, error) {
	e.texts = append(e.texts, texts...)
	return e.fakeEmbedder.Embed(ctx, texts)
}

func TestDecisionAndQueryVectorCaches(t *testing.T) {
	st := newStore()
	s := newServer(t, st)
	pol := &countingPolicy{Policy: s.Policy}
	emb := &countingEmbedder{fakeEmbedder: fakeEmbedder{model: model}}
	s.Policy, s.Embedder, s.Vectors = pol, emb, &memVectors{m: map[string]QueryVector{}}
	for range 2 {
		if _, err := search(t, s, alice(), query("额度")); err != nil {
			t.Fatal(err)
		}
	}
	if pol.n != 1 {
		t.Fatalf("同一主体 60 秒内只评估一次策略: %d", pol.n)
	}
	if len(emb.texts) != 1 {
		t.Fatalf("同一查询只算一次向量: %v", emb.texts)
	}
	// 主体属性变了（换了群组）：重新评估
	p := alice()
	p.Groups = append(p.Groups, "risk")
	if _, err := search(t, s, p, query("额度")); err != nil || pol.n != 2 {
		t.Fatalf("主体属性变了应重新评估: %v %d", err, pol.n)
	}
}
