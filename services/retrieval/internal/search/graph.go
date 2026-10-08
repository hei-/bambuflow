package search

import (
	"cmp"
	"context"
	"slices"
	"strings"

	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

// 图召回的参数（详设 6.2 graph 插件）
const (
	// 每个子查询最多链接的实体数
	maxLinked = 5
	// 每一跳最多取的边（一条边的每个证据块算一条）
	edgesPerHop = 60
	// 第二跳最多从多少个实体出发
	maxFrontier = 20
	// 每个子查询最多返回的证据块
	graphK = 30
	// 每走一跳分数乘以它
	hopDecay = 0.5
)

// Entity 是图谱里的一个实体。
type Entity struct {
	ID, Name string
	Type     commonv1.EntityType
}

// Edge 是一条关系的一个证据块：同一关系被多个块证明时有多条 Edge。
type Edge struct {
	From, To   Entity
	Relation   commonv1.RelationType
	Confidence float64
	Evidence   Candidate
}

// PathStep 是图路径上的一条边（实体 → 关系 → 实体）。
type PathStep struct {
	From, To Entity
	Relation commonv1.RelationType
}

// graph 是图召回路：从子查询里链接到的实体出发走 1–2 跳，收集边上的证据块（详设 6.2）。
// 子查询里没有可链接的实体时这一条不走图召回，不算失败。
func (s *Server) graph(ctx context.Context, version string, f Filter, queries []*retrievalv1.SubQuery) route {
	r := route{name: "graph", lists: make([][]Candidate, len(queries)), idle: true}
	for i, q := range queries {
		linked, err := s.Store.LinkEntities(ctx, q.GetText(), maxLinked)
		if err != nil {
			r.reason, r.err, r.idle = "backend_error", err, false
			return r
		}
		if len(linked) == 0 {
			continue
		}
		r.idle = false
		list, err := s.walk(ctx, version, f, linked)
		if err != nil {
			r.reason, r.err, r.idle = "backend_error", err, false
			return r
		}
		r.lists[i] = list
	}
	return r
}

type graphHit struct {
	Candidate
	score float64
}

// walk 走两跳。第二跳只从第一跳返回的边出发，而这些边都是证据可见的边，
// 所以遍历不会经过不可见的关系，也不会把不可见文档里的实体当成跳板（详设 13 节用例 4）。
// 评分 = 关系置信度 × 0.5^跳数，同一块取最高分，按分数取前 graphK 个。
func (s *Server) walk(ctx context.Context, version string, f Filter, linked []Entity) ([]Candidate, error) {
	start := map[string]bool{}
	var ids []string
	for _, e := range linked {
		start[e.ID] = true
		ids = append(ids, e.ID)
	}
	first, err := s.Store.Edges(ctx, version, f, ids, edgesPerHop)
	if err != nil {
		return nil, err
	}
	best := map[string]graphHit{}
	add := func(c Candidate, score float64) {
		if b, ok := best[c.ChunkID]; !ok || score > b.score {
			best[c.ChunkID] = graphHit{c, score}
		}
	}
	// 第二跳的起点：第一跳到达的、不在起点里的实体，记下到达它的那条边（取置信度最高的）
	via := map[string]Edge{}
	var frontier []string
	for _, e := range first {
		c := e.Evidence
		c.GraphPath = []PathStep{step(e)}
		c.PathDocs = nil
		add(c, e.Confidence*hopDecay)
		for _, next := range []Entity{e.From, e.To} {
			if start[next.ID] {
				continue
			}
			if _, seen := via[next.ID]; !seen && len(frontier) < maxFrontier {
				via[next.ID] = e
				frontier = append(frontier, next.ID)
			}
		}
	}
	if len(frontier) > 0 {
		second, err := s.Store.Edges(ctx, version, f, frontier, edgesPerHop)
		if err != nil {
			return nil, err
		}
		for _, e := range second {
			prev, ok := via[e.From.ID]
			if !ok {
				prev = via[e.To.ID]
			}
			if samePair(prev, e) {
				// 同一对实体之间的边：已经在第一跳里，不算第二跳
				continue
			}
			c := e.Evidence
			c.GraphPath = []PathStep{step(prev), step(e)}
			c.PathDocs = []string{prev.Evidence.DocID}
			add(c, e.Confidence*hopDecay*hopDecay)
		}
	}
	hits := make([]graphHit, 0, len(best))
	for _, h := range best {
		hits = append(hits, h)
	}
	slices.SortFunc(hits, func(a, b graphHit) int {
		if c := cmp.Compare(b.score, a.score); c != 0 {
			return c
		}
		return cmp.Compare(a.ChunkID, b.ChunkID)
	})
	out := make([]Candidate, 0, min(len(hits), graphK))
	for _, h := range hits[:min(len(hits), graphK)] {
		out = append(out, h.Candidate)
	}
	return out, nil
}

func step(e Edge) PathStep {
	return PathStep{From: e.From, To: e.To, Relation: e.Relation}
}

func samePair(a, b Edge) bool {
	return (a.From.ID == b.From.ID && a.To.ID == b.To.ID) || (a.From.ID == b.To.ID && a.To.ID == b.From.ID)
}

// LinkNames 从候选实体里去掉被更长的已选名称包含的短名称（“核心服务”不再单独算，当问题里写的是“清算核心服务”），
// 按名称从长到短最多保留 limit 个。存储适配器取到候选后调用它，各后端的链接结果因此一致。
func LinkNames(cands []Entity, limit int) []Entity {
	sorted := slices.Clone(cands)
	slices.SortStableFunc(sorted, func(a, b Entity) int {
		if c := cmp.Compare(len([]rune(NormName(b.Name))), len([]rune(NormName(a.Name)))); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	var out []Entity
	for _, e := range sorted {
		n := NormName(e.Name)
		if slices.ContainsFunc(out, func(o Entity) bool { return o.ID == e.ID || strings.Contains(NormName(o.Name), n) }) {
			continue
		}
		out = append(out, e)
		if len(out) == limit {
			break
		}
	}
	return out
}

// NormName 与 indexing 生成实体主键时的归一化相同：去掉空白、转小写。
func NormName(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), ""))
}

// graphPath 把路径转成响应字段；实体名称和正文一样按策略义务脱敏。
func graphPath(f Filter, path []PathStep) []*retrievalv1.GraphEdge {
	var out []*retrievalv1.GraphEdge
	for _, p := range path {
		out = append(out, &retrievalv1.GraphEdge{
			FromEntity: text(f, p.From.Name), FromType: p.From.Type, RelationType: p.Relation,
			ToEntity: text(f, p.To.Name), ToType: p.To.Type,
		})
	}
	return out
}
