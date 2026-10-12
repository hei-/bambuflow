package search

import (
	"fmt"
	"slices"
	"testing"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

var internal = commonv1.Classification_CLASSIFICATION_INTERNAL

func TestValidate(t *testing.T) {
	ok := Filter{Sources: []string{"wiki"}, MaxClassification: 2, Tokens: []string{"user:alice", "group:g"}}
	if err := ok.Validate("alice", internal); err != nil {
		t.Fatal(err)
	}
	many := make([]string, MaxTokens+1)
	for i := range many {
		many[i] = fmt.Sprintf("group:g%d", i)
	}
	many[0] = "user:alice"
	bad := map[string]Filter{
		"密级上限高于 Principal": {MaxClassification: 3, Tokens: []string{"user:alice"}},
		"密级上限为 0":          {MaxClassification: 0, Tokens: []string{"user:alice"}},
		"没有 token":         {MaxClassification: 2},
		"不含本人 token":       {MaxClassification: 2, Tokens: []string{"user:bob", "group:g"}},
		"token 超过上限":       {MaxClassification: 2, Tokens: many},
	}
	for name, f := range bad {
		if err := f.Validate("alice", internal); err == nil {
			t.Errorf("%s: 应当断言失败", name)
		}
	}
}

func TestNarrowOnlyShrinks(t *testing.T) {
	f := Filter{Sources: []string{"wiki", "mail"}}
	got := f.Narrow(&commonv1.Filters{SourceIds: []string{"mail", "secret"}})
	if !slices.Equal(got.Sources, []string{"mail"}) {
		t.Fatalf("调用方不能加入未授权的数据源: %v", got.Sources)
	}
	if got := f.Narrow(&commonv1.Filters{SourceIds: []string{"secret"}}); len(got.Sources) != 0 {
		t.Fatalf("交集为空时应当没有数据源: %v", got.Sources)
	}
	if got := f.Narrow(nil); !slices.Equal(got.Sources, f.Sources) {
		t.Fatalf("没有调用方过滤时不变: %v", got.Sources)
	}
}

func TestCheck(t *testing.T) {
	f := Filter{Sources: []string{"wiki"}, MaxClassification: 2, Tokens: []string{"user:alice", "group:credit"}}
	acl := ACL{SourceID: "wiki", Tokens: []string{"group:credit"}, Classification: 2, Active: true}
	cases := []struct {
		name string
		edit func(*ACL)
		want retrievalv1.AccessReason
	}{
		{"可见", func(*ACL) {}, retrievalv1.AccessReason_ACCESS_REASON_ALLOWED},
		{"密级相等可见", func(a *ACL) { a.Classification = 2 }, retrievalv1.AccessReason_ACCESS_REASON_ALLOWED},
		{"密级低于用户可见", func(a *ACL) { a.Classification = 1 }, retrievalv1.AccessReason_ACCESS_REASON_ALLOWED},
		{"密级高于用户", func(a *ACL) { a.Classification = 3 }, retrievalv1.AccessReason_ACCESS_REASON_CLASSIFICATION_EXCEEDED},
		{"删除标记", func(a *ACL) { a.Active = false }, retrievalv1.AccessReason_ACCESS_REASON_TOMBSTONED},
		{"其他租户的数据源", func(a *ACL) { a.SourceID = "mail" }, retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED},
		{"token 无交集", func(a *ACL) { a.Tokens = []string{"group:risk"} }, retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED},
		{"空 ACL 无人可见", func(a *ACL) { a.Tokens = nil }, retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED},
		{"紧急拒绝名单", func(a *ACL) { a.Denied = true }, retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED},
	}
	for _, c := range cases {
		a := acl
		c.edit(&a)
		if got := f.Check(a); got != c.want {
			t.Errorf("%s: 得到 %v，应为 %v", c.name, got, c.want)
		}
	}
}

func cand(id, doc, text string) Candidate {
	return Candidate{ChunkID: id, DocID: doc, Text: text}
}

func ids(xs []scored) []string {
	var out []string
	for _, x := range xs {
		out = append(out, x.ChunkID)
	}
	return out
}

// even 把各路结果包成权重都是 1.0 的列表。
func even(lists ...[]Candidate) []ranking {
	out := make([]ranking, len(lists))
	for i, l := range lists {
		out[i] = ranking{list: l, weight: 1}
	}
	return out
}

func TestFuse(t *testing.T) {
	a, b, c := cand("a", "d1", "甲"), cand("b", "d2", "乙"), cand("c", "d3", "丙")
	// 子查询 0 的两路：b 在两路都出现，分数相加排第一
	got := fuse([][]ranking{even([]Candidate{a, b}, []Candidate{b}), even([]Candidate{c})})
	if !slices.Equal(ids(got), []string{"b", "a", "c"}) {
		t.Fatalf("融合顺序 %v", ids(got))
	}
	// 同一块被多个子查询命中时取最大分，并记下给出最大分的子查询
	got = fuse([][]ranking{even([]Candidate{a}), even([]Candidate{b, a}, []Candidate{a})})
	if got[0].ChunkID != "a" || got[0].query != 1 {
		t.Fatalf("应取最大分并记录子查询: %+v", got[0])
	}
}

func TestRelationIntentRaisesGraphWeight(t *testing.T) {
	d1, d2, g := cand("d1", "x1", "甲"), cand("d2", "x2", "乙"), cand("g", "x3", "丙")
	// 稠密路第 1、2 名是 d1、d2，图召回第 1 名是 g，另一路（关键词）第 2 名也是 d1
	lists := func(intent retrievalv1.SearchIntent) [][]ranking {
		return [][]ranking{{
			{list: []Candidate{d1, d2}, weight: routeWeight("dense", intent, nil)},
			{list: []Candidate{d2, d1}, weight: routeWeight("keyword", intent, nil)},
			{list: []Candidate{g}, weight: routeWeight("graph", intent, nil)},
		}}
	}
	// 普通问题：两路都命中的块排在只有图召回命中的块前面
	if got := ids(fuse(lists(retrievalv1.SearchIntent_SEARCH_INTENT_UNSPECIFIED))); got[2] != "g" {
		t.Fatalf("普通问题的融合顺序 %v", got)
	}
	// 关系类问题：图召回权重加大，图召回的第 1 名排到最前
	if got := ids(fuse(lists(retrievalv1.SearchIntent_SEARCH_INTENT_RELATION))); got[0] != "g" {
		t.Fatalf("关系类问题应把图召回的结果排在前面: %v", got)
	}
	if routeWeight("dense", retrievalv1.SearchIntent_SEARCH_INTENT_RELATION, nil) != 1 {
		t.Fatal("关系类问题只提高图召回的权重")
	}
}

func TestQuotaKeepsEverySubquery(t *testing.T) {
	var q0, q1 []Candidate
	for i := range 60 {
		q0 = append(q0, cand(fmt.Sprintf("x%02d", i), fmt.Sprintf("dx%02d", i), "甲"))
	}
	q1 = append(q1, cand("y", "dy", "乙"))
	// q0 的前 60 名分数都高于 q1 的第 1 名之外的情况下，q1 的证据仍在前 50 名内
	got := fuse([][]ranking{even(q0, q0), even(q1)})
	if !slices.Contains(ids(got[:poolSize]), "y") {
		t.Fatal("每个子查询都应保留配额")
	}
}

func TestDiversify(t *testing.T) {
	long := "第三条 单户授信额度按借款人近十二个月的平均月收入乘以十倍计算，且不超过三十万元。"
	in := []scored{
		{Candidate: cand("a1", "A", long), score: 1},
		{Candidate: cand("b1", "B", long+"。"), score: 0.9}, // 近重复（只差标点）：去掉
		{Candidate: cand("a2", "A", "第四条 已有负债的扣减未结清余额"), score: 0.8},
		{Candidate: cand("a3", "A", "第五条 审批权限按金额分级"), score: 0.7},
		{Candidate: cand("a4", "A", "第六条 期限最长五年"), score: 0.6}, // 同文档第 4 块：去掉
		{Candidate: cand("c1", "C", "员工请假须提前申请"), score: 0.5},
	}
	got := ids(diversify(in, poolSize))
	if slices.Contains(got, "b1") || slices.Contains(got, "a4") || len(got) != 4 {
		t.Fatalf("去重与同文档限量 %v", got)
	}
	if got[0] != "a1" {
		t.Fatalf("最相关的仍排第一: %v", got)
	}
}
