package search

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

type fakeEdge struct {
	from, to Entity
	rel      commonv1.RelationType
	conf     float64
	chunk    string
}

// LinkEntities 按名称包含关系链接，与 PG 适配器一样交给 LinkNames 去掉被包含的短名称。
func (s *fakeStore) LinkEntities(_ context.Context, text string, limit int) ([]Entity, error) {
	if err := s.call("LinkEntities"); err != nil {
		return nil, err
	}
	var out []Entity
	for _, e := range s.entities {
		if strings.Contains(NormName(text), NormName(e.Name)) {
			out = append(out, e)
		}
	}
	return LinkNames(out, limit), nil
}

// Edges 只返回证据块按召回时权限可见的边，模拟必含条件下推。
func (s *fakeStore) Edges(_ context.Context, _ string, f Filter, ids []string, k int) ([]Edge, error) {
	if err := s.call("Edges"); err != nil {
		return nil, err
	}
	var out []Edge
	for _, e := range s.edges {
		c := s.chunks[e.chunk]
		if !slices.Contains(ids, e.from.ID) && !slices.Contains(ids, e.to.ID) {
			continue
		}
		if f.Check(s.acl[c.DocID]) != retrievalv1.AccessReason_ACCESS_REASON_ALLOWED || !metaMatch(f.Meta, s.meta[c.DocID]) {
			continue
		}
		out = append(out, Edge{From: e.from, To: e.to, Relation: e.rel, Confidence: e.conf, Evidence: c})
	}
	slices.SortStableFunc(out, func(a, b Edge) int {
		switch {
		case a.Confidence > b.Confidence:
			return -1
		case a.Confidence < b.Confidence:
			return 1
		}
		return 0
	})
	return out[:min(k, len(out))], nil
}

var (
	clearing = Entity{"system:清算核心服务", "清算核心服务", commonv1.EntityType_ENTITY_TYPE_SYSTEM}
	accounts = Entity{"system:账户系统", "账户系统", commonv1.EntityType_ENTITY_TYPE_SYSTEM}
	opsDept  = Entity{"org:运营管理部", "运营管理部", commonv1.EntityType_ENTITY_TYPE_ORG}
	secret   = Entity{"system:秘密系统", "秘密系统", commonv1.EntityType_ENTITY_TYPE_SYSTEM}
)

const (
	dependsOn   = commonv1.RelationType_RELATION_TYPE_DEPENDS_ON
	responsible = commonv1.RelationType_RELATION_TYPE_RESPONSIBLE_FOR
)

// graphStore 在固定数据上给块换成互不相近的正文：默认正文（loan1、loan2）太像，会被近重复去重合并。
func graphStore() *fakeStore {
	st := newStore()
	texts := map[string]string{
		"wiki:loan#1":     "清算核心服务依赖账户系统完成日终记账。",
		"wiki:loan#2":     "运营管理部负责账户系统的日常运维与变更审批。",
		"wiki:handbook#1": "员工手册列出了清算核心服务对账户系统的调用约定。",
		"wiki:risk#1":     "机密附件：清算核心服务依赖秘密系统。",
	}
	for id, text := range texts {
		c := st.chunks[id]
		c.Text = text
		st.chunks[id] = c
	}
	return st
}

// graphOnly 只开图召回路，便于断言图召回自己的结果。
func graphOnly(t *testing.T, st *fakeStore) *Server {
	s := newServer(t, st)
	s.Disabled = map[string]bool{"dense": true, "keyword": true}
	return s
}

func paths(resp *retrievalv1.SearchResponse) map[string][]string {
	out := map[string][]string{}
	for _, h := range resp.GetHits() {
		for _, e := range h.GetGraphPath() {
			out[h.GetChunkId()] = append(out[h.GetChunkId()], e.GetFromEntity()+"-"+e.GetToEntity())
		}
	}
	return out
}

func TestGraphWalksTwoHopsAndReturnsEvidenceWithPath(t *testing.T) {
	st := graphStore()
	st.entities = []Entity{clearing, accounts, opsDept}
	st.edges = []fakeEdge{
		{clearing, accounts, dependsOn, 0.9, "wiki:loan#1"},
		{opsDept, accounts, responsible, 0.8, "wiki:loan#2"},
	}
	resp, err := search(t, graphOnly(t, st), alice(), query("清算核心服务依赖哪些系统？"))
	if err != nil {
		t.Fatal(err)
	}
	got := paths(resp)
	want := map[string][]string{
		"wiki:loan#1": {"清算核心服务-账户系统"},
		"wiki:loan#2": {"清算核心服务-账户系统", "运营管理部-账户系统"},
	}
	if len(got) != 2 || !slices.Equal(got["wiki:loan#1"], want["wiki:loan#1"]) || !slices.Equal(got["wiki:loan#2"], want["wiki:loan#2"]) {
		t.Fatalf("图路径 %v", got)
	}
	// 第一跳（0.9 × 0.5）排在第二跳（0.8 × 0.25）前面
	if resp.GetHits()[0].GetChunkId() != "wiki:loan#1" {
		t.Fatalf("排序 %v", resp.GetHits())
	}
	e := resp.GetHits()[0].GetGraphPath()[0]
	if e.GetRelationType() != dependsOn || e.GetFromType() != commonv1.EntityType_ENTITY_TYPE_SYSTEM {
		t.Fatalf("边的类型 %v", e)
	}
}

// 权限评测集用例 4（详设 13 节）：关系两端实体可见、但证据块不可见时，图召回不得返回该关系的任何信息，
// 也不能经由它走到第二跳。
func TestGraphNeverUsesEdgesWithInvisibleEvidence(t *testing.T) {
	st := graphStore()
	st.entities = []Entity{clearing, secret, opsDept}
	st.edges = []fakeEdge{
		// risk 文档的密级高于 alice：这条边不可见
		{clearing, secret, dependsOn, 0.9, "wiki:risk#1"},
		// 可见的边，但只能经由不可见的边到达
		{opsDept, secret, responsible, 0.9, "wiki:loan#1"},
	}
	s := newServer(t, st) // 三路全开：稠密路会召回 loan 的块，但它们不能带上经由秘密系统的路径
	resp, err := search(t, s, alice(), query("清算核心服务依赖什么"))
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range resp.GetHits() {
		if len(h.GetGraphPath()) > 0 {
			t.Fatalf("不可见的边泄露到路径里: %v", h.GetGraphPath())
		}
		if h.GetDocId() == "risk" {
			t.Fatal("不可见文档的块不能返回")
		}
	}
	if !slices.Contains(st.calls, "Edges") {
		t.Fatalf("应执行图召回: %v", st.calls)
	}
}

// 第二跳的路径用到了另一份文档里的边：召回之后那份文档被撤权，组装复核必须连同路径一起拦下这一块。
func TestGraphPathDocRevokedAfterRecallDropsHit(t *testing.T) {
	st := graphStore()
	st.acl["handbook"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: true}
	st.entities = []Entity{clearing, accounts, opsDept}
	st.edges = []fakeEdge{
		{clearing, accounts, dependsOn, 0.9, "wiki:handbook#1"},
		{opsDept, accounts, responsible, 0.8, "wiki:loan#1"},
	}
	st.now["handbook"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 1, Active: false}
	resp, err := search(t, graphOnly(t, st), alice(), query("清算核心服务"))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetHits()) != 0 {
		t.Fatalf("路径上的文档已删除，不能返回: %v", paths(resp))
	}
}

func TestGraphRouteFailureDegradesAndOtherRoutesStillServe(t *testing.T) {
	st := graphStore()
	st.entities = []Entity{clearing}
	st.errs["Edges"] = errors.New("boom")
	resp, err := search(t, newServer(t, st), alice(), query("清算核心服务 loan1"))
	if err != nil {
		t.Fatal(err)
	}
	d := resp.GetDegraded()
	if len(d) != 1 || d[0].GetStage() != "graph" || d[0].GetReason() != "backend_error" {
		t.Fatalf("降级标记 %v", d)
	}
	if !slices.Equal(docs(resp), []string{"loan"}) {
		t.Fatalf("其他路照常: %v", docs(resp))
	}
}

// 没有链接到实体时图召回无事可做：另外两路都失败仍是全部召回失败，不能当成“没有证据”返回空结果。
func TestIdleGraphRouteDoesNotMaskTotalFailure(t *testing.T) {
	boom := errors.New("boom")
	st := graphStore()
	st.errs["Dense"], st.errs["Keyword"] = boom, boom
	if _, err := search(t, newServer(t, st), alice(), query("额度")); err == nil {
		t.Fatal("应返回 UNAVAILABLE")
	}
	// 链接到了实体、图召回正常：降级为只有图召回的结果
	st.entities = []Entity{clearing, accounts}
	st.edges = []fakeEdge{{clearing, accounts, dependsOn, 0.9, "wiki:loan#1"}}
	resp, err := search(t, newServer(t, st), alice(), query("清算核心服务"))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetDegraded()) != 2 || len(resp.GetHits()) != 1 || len(resp.GetHits()[0].GetGraphPath()) != 1 {
		t.Fatalf("降级 %v，结果 %v", resp.GetDegraded(), resp.GetHits())
	}
}

func TestFusePreservesGraphPath(t *testing.T) {
	plain := Candidate{ChunkID: "a", DocID: "d"}
	withP := plain
	withP.GraphPath = []PathStep{{From: clearing, To: accounts, Relation: dependsOn}}
	withP.PathDocs = []string{"d2"}
	// 稠密路排在后面的列表里也不能覆盖掉图路径
	out := fuse([][]ranking{even([]Candidate{withP}, []Candidate{plain}), even([]Candidate{plain})})
	if len(out) != 1 || len(out[0].GraphPath) != 1 || !slices.Equal(out[0].PathDocs, []string{"d2"}) {
		t.Fatalf("融合后丢了图路径: %+v", out)
	}
}

func TestLinkNamesDropsContainedShorterNames(t *testing.T) {
	core := Entity{"system:核心服务", "核心服务", commonv1.EntityType_ENTITY_TYPE_SYSTEM}
	got := LinkNames([]Entity{core, accounts, clearing}, 5)
	var names []string
	for _, e := range got {
		names = append(names, e.Name)
	}
	if !slices.Equal(names, []string{"清算核心服务", "账户系统"}) {
		t.Fatalf("链接结果 %v", names)
	}
	if got := LinkNames([]Entity{core, accounts, clearing}, 1); len(got) != 1 || got[0].ID != clearing.ID {
		t.Fatalf("上限 %v", got)
	}
}

func TestGraphPathNamesAreMasked(t *testing.T) {
	phone := Entity{"role:x", "客户经理13812345678", commonv1.EntityType_ENTITY_TYPE_ROLE}
	out := graphPath(Filter{}, []PathStep{{From: phone, To: accounts, Relation: responsible}})
	if strings.Contains(out[0].GetFromEntity(), "13812345678") {
		t.Fatalf("实体名称应按默认义务掩码: %v", out[0].GetFromEntity())
	}
}
