package search

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

func TestIdentifiers(t *testing.T) {
	cases := map[string][]string{
		"银发〔2023〕12 号文件里对额度怎么规定":         {"银发〔2023〕12号"},
		"工单 INC-20240312 处理到哪一步了":        {"INC-20240312"},
		"POL-2024-001 与 CR2024001 有什么区别": {"POL-2024-001", "CR2024001"},
		"CRM 系统由哪个团队负责":                  {"CRM"},
		"API 文档在哪里":                      nil, // 单独的英文简称不算标识符
		"2025 年的额度是多少":                   nil, // 纯数字（年份、金额）不算
	}
	for q, want := range cases {
		if got := identifiers(q); !slices.Equal(got, want) {
			t.Errorf("%q：得到 %q，应为 %q", q, got, want)
		}
	}
}

func TestIdentifierHitIsPinnedAndExplained(t *testing.T) {
	st := newStore()
	c := st.chunks["wiki:loan#2"]
	c.Text = "按 INC-20240312 的处理结论，额度调整为 20 万元"
	st.chunks["wiki:loan#2"] = c
	s := newServer(t, st)
	req := query("INC-20240312 的处理结论")
	req.IncludeScoreDetails = true
	resp, err := search(t, s, alice(), req)
	if err != nil {
		t.Fatal(err)
	}
	top := resp.GetHits()[0]
	if top.GetChunkId() != "wiki:loan#2" {
		t.Fatalf("编号命中的块应置顶: %v", resp.GetHits())
	}
	routes := map[string]bool{}
	for _, m := range top.GetMatchedBy() {
		routes[m.GetRoute()] = true
		if m.GetQueryId() != "a" || m.GetRank() < 1 {
			t.Fatalf("命中记录 %v", m)
		}
	}
	if !routes["identifier"] || !routes["dense"] {
		t.Fatalf("应记录全部命中的召回路: %v", top.GetMatchedBy())
	}
	d := top.GetScoreDetails()
	if d.GetIdentifier() != 1 || d.Rerank == nil || d.Dense == nil || d.GetRrf() <= 0 {
		t.Fatalf("分数解释 %v", d)
	}
	// 不要求分数解释时不返回；命中记录总是返回
	resp, err = search(t, s, alice(), query("INC-20240312 的处理结论"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetHits()[0].GetScoreDetails() != nil || len(resp.GetHits()[0].GetMatchedBy()) == 0 {
		t.Fatalf("score_details 应只按请求返回: %v", resp.GetHits()[0])
	}
}

func TestLowRerankScoreUnpinsIdentifierHit(t *testing.T) {
	cands := []scored{
		{Candidate: Candidate{ChunkID: "id", Text: "id"}, pinned: true},
		{Candidate: Candidate{ChunkID: "x", Text: "x"}},
	}
	s := newServer(t, newStore())
	s.Reranker = scoreBy{"id": 0.05, "x": 0.6}
	out, _, _ := s.rerank(t.Context(), query("q").GetQueries(), cands)
	if out[0].ChunkID != "x" || out[1].pinned {
		t.Fatalf("精排分低于 %.1f 的编号命中块不再置顶: %v", pinFloor, out)
	}
	s.Reranker = scoreBy{"id": 0.2, "x": 0.6}
	out, _, _ = s.rerank(t.Context(), query("q").GetQueries(), cands)
	if out[0].ChunkID != "id" {
		t.Fatalf("精排分不低于 %.1f 时仍置顶: %v", pinFloor, out)
	}
}

func TestIdentifierRouteFailureDegrades(t *testing.T) {
	st := newStore()
	st.errs["Identifier"] = errBackend
	resp, err := search(t, newServer(t, st), alice(), &retrievalv1.SearchRequest{
		Queries: []*retrievalv1.SubQuery{{Id: "a", Text: "INC-20240312 额度"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := resp.GetDegraded()
	if len(d) != 1 || d[0].GetStage() != "identifier" || d[0].GetReason() != "backend_error" || len(resp.GetHits()) == 0 {
		t.Fatalf("编号匹配失败只降级: %v %v", d, resp.GetHits())
	}
}

var errBackend = errors.New("backend down")

// scoreBy 按块正文里出现的 chunk_id 给精排分（rerank 测试的段落就是正文）。
type scoreBy map[string]float64

func (m scoreBy) Rerank(_ context.Context, _, passages []string) ([]float64, string, error) {
	out := make([]float64, len(passages))
	for i, p := range passages {
		for id, sc := range m {
			if strings.HasSuffix(p, id) {
				out[i] = sc
			}
		}
	}
	return out, "rerank@test", nil
}

func TestTenantPlanDisablesRoutesAndRerank(t *testing.T) {
	st := newStore()
	c := st.chunks["wiki:loan#2"]
	c.Text = "按 INC-20240312 的处理结论"
	st.chunks["wiki:loan#2"] = c
	s := newServer(t, st)
	rr := &fakeReranker{}
	s.Reranker = rr
	off := false
	s.Grants = planGrants{fakeGrants{"credit": {"wiki"}}, &configv1.PipelineConfig{
		Tenant: "credit", VersionId: "cfv_1",
		Retrieval: &configv1.RetrievalPlan{DisabledRoutes: []string{"identifier", "keyword"}, Rerank: &off},
	}}
	resp, err := search(t, s, alice(), query("INC-20240312 的处理结论"))
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(st.calls, "Identifier") || slices.Contains(st.calls, "Keyword") || rr.calls != 0 {
		t.Fatalf("计划关闭的召回路与精排不应执行: %v 精排 %d 次", st.calls, rr.calls)
	}
	if resp.GetPlanVersion() != "cfv_1" || resp.GetRerankModel() != "" || len(resp.GetDegraded()) != 0 {
		t.Fatalf("计划版本写进响应，关闭不算降级: %v", resp)
	}
}

func TestPlanWeightsMultiplyIntentWeight(t *testing.T) {
	plan := &configv1.RetrievalPlan{RouteWeights: map[string]float64{"graph": 1.5, "dense": 0.5}}
	if w := routeWeight("graph", retrievalv1.SearchIntent_SEARCH_INTENT_RELATION, plan); w != 1.5*relationGraphWeight {
		t.Fatalf("图召回: %v", w)
	}
	if routeWeight("dense", retrievalv1.SearchIntent_SEARCH_INTENT_UNSPECIFIED, plan) != 0.5 ||
		routeWeight("keyword", retrievalv1.SearchIntent_SEARCH_INTENT_UNSPECIFIED, plan) != 1 {
		t.Fatal("没列出的召回路权重为 1")
	}
}
