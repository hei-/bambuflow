package search

import (
	"cmp"
	"hash/fnv"
	"math"
	"math/bits"
	"slices"
	"strings"
	"unicode"

	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

const (
	// rrfK 是 RRF 的平滑常数（详设 7.1）。融合只看名次：各路分数不可直接比较。
	rrfK = 60
	// poolSize 是送精排的候选上限（详设 7.4）
	poolSize = 50
	// 同一文档最多保留的块数（详设 7.3）
	perDocCap = 3
	// SimHash 汉明距离不超过它视为近重复（详设 7.3）
	nearDupBits = 3
	// MMR 在相关性与多样性之间的权衡（详设 7.3）
	mmrLambda = 0.7
	// relationGraphWeight 是关系类问题里图召回在 RRF 中的权重（orchestrator 详设 5.3“提高 graph 路权重”），
	// 其余情况各路都是 1.0（详设 7.1）。租户 pipeline 的检索计划接入前先用常量，取值待评测集调优。
	relationGraphWeight = 2.0
)

// ranking 是一路召回在一个子查询上的结果列表及这一路在 RRF 中的权重。
type ranking struct {
	list   []Candidate
	weight float64
}

// routeWeight 按意图给出一路召回的融合权重。
func routeWeight(route string, intent retrievalv1.SearchIntent) float64 {
	if route == "graph" && intent == retrievalv1.SearchIntent_SEARCH_INTENT_RELATION {
		return relationGraphWeight
	}
	return 1
}

type scored struct {
	Candidate
	score float64
	// 给出最高分的子查询下标，精排时用它作为查询文本（详设 7.4）
	query int
}

// fuse 先在每个子查询内跨召回路做加权 RRF，再跨子查询合并：同一块取最大分（详设 7.1、7.2）。
// perQuery[q] 是第 q 个子查询在各召回路上的结果列表。同分按 chunk_id 排，结果可复现。
func fuse(perQuery [][]ranking) []scored {
	best := map[string]*scored{}
	ranked := make([][]string, len(perQuery))
	for q, lists := range perQuery {
		sum := map[string]float64{}
		cand := map[string]Candidate{}
		for _, l := range lists {
			for rank, c := range l.list {
				sum[c.ChunkID] += l.weight / float64(rrfK+rank+1)
				cand[c.ChunkID] = withPath(c, cand[c.ChunkID])
			}
		}
		for id, sc := range sum {
			b, ok := best[id]
			switch {
			case !ok:
				best[id] = &scored{Candidate: cand[id], score: sc, query: q}
			case sc > b.score:
				best[id] = &scored{Candidate: withPath(cand[id], b.Candidate), score: sc, query: q}
			default:
				b.Candidate = withPath(b.Candidate, cand[id])
			}
		}
		ranked[q] = sortedIDs(sum)
	}
	out := make([]scored, 0, len(best))
	for _, s := range best {
		out = append(out, *s)
	}
	byScore(out)
	return withQuota(out, ranked)
}

// withPath 合并同一块的两条记录时保留图路径：同一块可能先被稠密或关键词路召回、再被图召回，
// 只有图召回带路径，丢掉它回答就无法解释“为什么找到这条证据”。
func withPath(c, other Candidate) Candidate {
	if len(c.GraphPath) == 0 && len(other.GraphPath) > 0 {
		c.GraphPath, c.PathDocs = other.GraphPath, other.PathDocs
	}
	return c
}

func sortedIDs(sum map[string]float64) []string {
	ids := make([]string, 0, len(sum))
	for id := range sum {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b string) int {
		if c := cmp.Compare(sum[b], sum[a]); c != 0 {
			return c
		}
		return cmp.Compare(a, b)
	})
	return ids
}

func byScore(xs []scored) {
	slices.SortFunc(xs, func(a, b scored) int {
		if c := cmp.Compare(b.score, a.score); c != 0 {
			return c
		}
		return cmp.Compare(a.ChunkID, b.ChunkID)
	})
}

// withQuota 保证每个子查询在前 poolSize 个名额里至少有 ⌈poolSize / 子查询数⌉ 个（详设 7.2），
// 避免多跳问题中某个子问题的证据被另一个挤掉。先按配额从各子查询自己的排名里取，再按总分补足。
func withQuota(all []scored, ranked [][]string) []scored {
	if len(ranked) <= 1 || len(all) <= poolSize {
		return all
	}
	quota := (poolSize + len(ranked) - 1) / len(ranked)
	byID := map[string]scored{}
	for _, s := range all {
		byID[s.ChunkID] = s
	}
	picked := map[string]bool{}
	var pool []scored
	for _, ids := range ranked {
		for _, id := range ids[:min(quota, len(ids))] {
			if !picked[id] {
				picked[id] = true
				pool = append(pool, byID[id])
			}
		}
	}
	var rest []scored
	for _, s := range all {
		switch {
		case picked[s.ChunkID]:
		case len(pool) < poolSize:
			picked[s.ChunkID] = true
			pool = append(pool, s)
		default:
			rest = append(rest, s)
		}
	}
	// 只在名额内部按分数排序；整体重排会把靠配额保住的证据又挤出前 poolSize 名
	byScore(pool)
	return append(pool, rest...)
}

// diversify 依次做近重复去重、同文档限量、MMR，取出最多 poolSize 个候选送精排（详设 7.3）。
func diversify(cands []scored) []scored {
	var kept []scored
	var hashes []uint64
	perDoc := map[string]int{}
	for _, c := range cands {
		h := simhash(c.Text)
		if slices.ContainsFunc(hashes, func(x uint64) bool { return bits.OnesCount64(x^h) <= nearDupBits }) {
			continue
		}
		if perDoc[c.DocID] == perDocCap {
			continue
		}
		hashes = append(hashes, h)
		perDoc[c.DocID]++
		kept = append(kept, c)
	}
	return mmr(kept, poolSize)
}

// mmr 每次选 λ·相关性 − (1−λ)·与已选结果的最大相似度 最大的候选。相关性是归一化后的融合分，
// 相似度用字二元组集合的 Jaccard（候选里没有块向量）。
func mmr(cands []scored, n int) []scored {
	if len(cands) <= 1 {
		return cands
	}
	top := cands[0].score
	grams := make([]map[string]bool, len(cands))
	for i, c := range cands {
		grams[i] = bigrams(c.Text)
	}
	used := make([]bool, len(cands))
	var out []scored
	var chosen []int
	for len(out) < min(n, len(cands)) {
		bestI, bestV := -1, math.Inf(-1)
		for i := range cands {
			if used[i] {
				continue
			}
			sim := 0.0
			for _, j := range chosen {
				sim = max(sim, jaccard(grams[i], grams[j]))
			}
			v := mmrLambda*cands[i].score/top - (1-mmrLambda)*sim
			if v > bestV {
				bestI, bestV = i, v
			}
		}
		used[bestI] = true
		chosen = append(chosen, bestI)
		out = append(out, cands[bestI])
	}
	return out
}

func bigrams(text string) map[string]bool {
	var rs []rune
	for _, r := range strings.ToLower(text) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			rs = append(rs, r)
		}
	}
	out := map[string]bool{}
	for i := 0; i+1 < len(rs); i++ {
		out[string(rs[i:i+2])] = true
	}
	return out
}

func jaccard(a, b map[string]bool) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	inter := 0
	for k := range a {
		if b[k] {
			inter++
		}
	}
	return float64(inter) / float64(len(a)+len(b)-inter)
}

// simhash 是字二元组的 64 位 SimHash；文本规范化（小写、去标点空白）后计算。
func simhash(text string) uint64 {
	var v [64]int
	for g := range bigrams(text) {
		h := fnv.New64a()
		_, _ = h.Write([]byte(g))
		x := h.Sum64()
		for i := range 64 {
			if x&(1<<i) != 0 {
				v[i]++
			} else {
				v[i]--
			}
		}
	}
	var out uint64
	for i := range 64 {
		if v[i] > 0 {
			out |= 1 << i
		}
	}
	return out
}
