package search

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/policy"
)

// 缓存（详设第 8 节）只用于提速，不承担正确性：
//   - 策略决定：进程内，按主体属性指纹 + 租户授权版本，60 秒。分区展开与拒绝名单不在缓存里，每次照查，撤权立即生效；
//   - 查询向量：进程内 + Redis（internal/cache），按查询文本 + 嵌入模型版本，7 天；
//   - 检索结果：Redis，融合精排后的块 ID 与分数，不含正文，10 分钟；键含租户、规范化子查询、过滤条件（含展开后的权限 token）、
//     索引版本、检索计划版本与是否精排。命中时按块 ID 带全部必含条件重新取块，再照常组装复核，缓存滞后也不会越权；
//     ChunkAclChanged 按文档清掉相关条目（internal/cache）。可能看到受限级内容的请求、评测副本指定版本的请求、降级的结果不缓存。

// ResultCache 是检索结果缓存（internal/cache.Redis）。
type ResultCache interface {
	Get(ctx context.Context, key string) (*CachedResult, bool)
	// Put 写入并按文档建反向索引；docs 里有文档在 since 之后变过权限时放弃写入（详设 8.3 的竞态）
	Put(ctx context.Context, key string, r *CachedResult, docs []string, since time.Time)
}

// VectorCache 是查询向量缓存（internal/cache.Vectors），稠密与稀疏向量一起存。
type VectorCache interface {
	// Get 返回命中的向量，按 texts 的下标
	Get(ctx context.Context, model string, texts []string) map[int]QueryVector
	Put(ctx context.Context, model string, texts []string, vecs []QueryVector)
}

// CachedResult 是一次检索融合精排后的候选（不含正文）。
type CachedResult struct {
	RerankModel string       `json:"rerank_model,omitempty"`
	Items       []CachedItem `json:"items"`
}

// CachedItem 是一个候选的排序信息与分数解释；正文、标题等在命中时按块 ID 重新取。
type CachedItem struct {
	ChunkID     string             `json:"chunk_id"`
	Score       float64            `json:"score"`
	Query       int                `json:"query"`
	RRF         float64            `json:"rrf"`
	Rerank      *float64           `json:"rerank,omitempty"`
	RouteScore  float64            `json:"route_score"`
	RouteScores map[string]float64 `json:"route_scores,omitempty"`
	Matches     []CachedMatch      `json:"matches,omitempty"`
	Pinned      bool               `json:"pinned,omitempty"`
	IdentRank   int32              `json:"ident_rank,omitempty"`
	GraphPath   []PathStep         `json:"graph_path,omitempty"`
	PathDocs    []string           `json:"path_docs,omitempty"`
}

type CachedMatch struct {
	Route string `json:"route"`
	Query int    `json:"query"`
	Rank  int32  `json:"rank"`
}

// maxCachedItems：缓存的候选上限，与送精排的候选数一致
const maxCachedItems = poolSize

// resultKey 是检索结果缓存的键；不该缓存时为空。
func (s *Server) resultKey(ctx context.Context, f Filter, version, planVersion string, r *retrievalv1.SearchRequest, rerank bool) string {
	if _, pinned := pinnedVersion(ctx); s.Results == nil || pinned ||
		f.MaxClassification >= int32(commonv1.Classification_CLASSIFICATION_RESTRICTED) {
		return ""
	}
	type q struct{ ID, Text string }
	qs := make([]q, 0, len(r.GetQueries()))
	for _, x := range r.GetQueries() {
		qs = append(qs, q{x.GetId(), strings.ToLower(strings.Join(strings.Fields(x.GetText()), " "))})
	}
	sorted := func(xs []string) []string { out := slices.Clone(xs); slices.Sort(out); return out }
	b, err := json.Marshal(struct {
		Tenant, Version, Plan string
		Queries               []q
		Sources, Tokens       []string
		MaxClass              int32
		Meta                  MetaFilter
		Intent                retrievalv1.SearchIntent
		Rerank                bool
	}{grpcmw.PrincipalFrom(ctx).GetTenant(), version, planVersion, qs, sorted(f.Sources), sorted(f.Tokens), f.MaxClassification, f.Meta, r.GetIntent(), rerank})
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// cachedResult 取缓存的候选：按块 ID 带全部必含条件重新取块（现在已不可见的块就此去掉），分数与解释取缓存里的。
func (s *Server) cachedResult(ctx context.Context, key string, f Filter, version string) ([]scored, string, []*commonv1.Degradation, bool) {
	if key == "" {
		return nil, "", nil, false
	}
	r, ok := s.Results.Get(ctx, key)
	if !ok {
		return nil, "", nil, false
	}
	ids := make([]string, 0, len(r.Items))
	for _, it := range r.Items {
		ids = append(ids, it.ChunkID)
	}
	chunks, err := s.Store.Chunks(ctx, version, f, ids)
	if err != nil {
		s.Log.WarnContext(ctx, "按缓存的块 ID 取块失败，改为照常检索", slog.Any("err", err))
		return nil, "", nil, false
	}
	out := make([]scored, 0, len(r.Items))
	for _, it := range r.Items {
		c, ok := chunks[it.ChunkID]
		if !ok {
			continue
		}
		c.RouteScore, c.GraphPath, c.PathDocs = it.RouteScore, it.GraphPath, it.PathDocs
		sc := scored{Candidate: c, score: it.Score, query: it.Query, rrf: it.RRF, rerank: it.Rerank, routeScores: it.RouteScores,
			pinned: it.Pinned, identRank: it.IdentRank}
		for _, m := range it.Matches {
			sc.matches = append(sc.matches, match{route: m.Route, query: m.Query, rank: m.Rank})
		}
		out = append(out, sc)
	}
	return out, r.RerankModel, nil, true
}

// storeResult 写检索结果缓存；key 为空时不写。
func (s *Server) storeResult(ctx context.Context, key string, cands []scored, rerankModel string, since time.Time) {
	if key == "" {
		return
	}
	r := &CachedResult{RerankModel: rerankModel}
	var docs []string
	for _, c := range cands[:min(len(cands), maxCachedItems)] {
		it := CachedItem{ChunkID: c.ChunkID, Score: c.score, Query: c.query, RRF: c.rrf, Rerank: c.rerank, RouteScore: c.RouteScore,
			RouteScores: c.routeScores, Pinned: c.pinned, IdentRank: c.identRank, GraphPath: c.GraphPath, PathDocs: c.PathDocs}
		for _, m := range c.matches {
			it.Matches = append(it.Matches, CachedMatch{Route: m.route, Query: m.query, Rank: m.rank})
		}
		r.Items = append(r.Items, it)
		docs = append(docs, c.DocID)
		docs = append(docs, c.PathDocs...)
	}
	slices.Sort(docs)
	s.Results.Put(ctx, key, r, slices.Compact(docs), since)
}

// embed 先查查询向量缓存，只把没命中的送嵌入模型；模型版本对得上才写回缓存。
func (s *Server) embed(ctx context.Context, model string, texts []string) ([]QueryVector, string, error) {
	if s.Vectors == nil {
		return s.Embedder.Embed(ctx, texts)
	}
	hit := s.Vectors.Get(ctx, model, texts)
	if len(hit) == len(texts) {
		out := make([]QueryVector, len(texts))
		for i := range texts {
			out[i] = hit[i]
		}
		return out, model, nil
	}
	var missing []string
	var at []int
	for i, t := range texts {
		if _, ok := hit[i]; !ok {
			missing, at = append(missing, t), append(at, i)
		}
	}
	vecs, qModel, err := s.Embedder.Embed(ctx, missing)
	if err != nil {
		return nil, "", err
	}
	if qModel != model {
		// 嵌入模型与索引不一致：交给调用方按 model_mismatch 降级，不写缓存
		return nil, qModel, nil
	}
	s.Vectors.Put(ctx, model, missing, vecs)
	out := make([]QueryVector, len(texts))
	for i, v := range hit {
		out[i] = v
	}
	for j, i := range at {
		out[i] = vecs[j]
	}
	return out, model, nil
}

// ---- 策略决定缓存 ----

const (
	decisionTTL = 60 * time.Second
	// 超过就整个清空：按主体缓存，条目数与活跃用户数同量级
	maxDecisions = 20000
)

type decisionEntry struct {
	d   policy.Decision
	exp time.Time
}

// decisions 是进程内的策略决定缓存（详设 8.1）。
type decisions struct {
	mu sync.Mutex
	m  map[string]decisionEntry
}

func decisionKey(in policy.Input, granted []string, grantsVersion string) string {
	b, err := json.Marshal(struct {
		In      policy.Input
		Granted []string
		Version string
	}{in, granted, grantsVersion})
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// decide 评估策略，按主体属性指纹与租户授权版本缓存 60 秒（策略包换版最多晚 60 秒生效，详设 8.1 与总体设计 8.6）。
func (s *Server) decide(ctx context.Context, in policy.Input, granted []string, grantsVersion string) (policy.Decision, error) {
	key := decisionKey(in, granted, grantsVersion)
	now := time.Now()
	s.decisions.mu.Lock()
	if e, ok := s.decisions.m[key]; ok && key != "" && now.Before(e.exp) {
		s.decisions.mu.Unlock()
		return e.d, nil
	}
	s.decisions.mu.Unlock()
	d, err := s.Policy.Decide(ctx, in, granted)
	if err != nil || key == "" {
		return d, err
	}
	s.decisions.mu.Lock()
	defer s.decisions.mu.Unlock()
	if s.decisions.m == nil || len(s.decisions.m) >= maxDecisions {
		s.decisions.m = map[string]decisionEntry{}
	}
	s.decisions.m[key] = decisionEntry{d, now.Add(decisionTTL)}
	return d, nil
}
