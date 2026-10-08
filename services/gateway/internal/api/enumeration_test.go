package api

import (
	"testing"

	answerv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
)

func TestScopeOmitsTotalWithoutCompleteCount(t *testing.T) {
	// 计数接口不可用、退回普通检索时没有总数：不能输出“共 0 份”
	if s := scopeOf(&answerv1.Scope{FiltersText: "x", Basis: "仅统计你有权查看的文档"}); s.TotalCount != nil {
		t.Fatalf("没有完整计数时不应给总数: %v", *s.TotalCount)
	}
	if s := scopeOf(&answerv1.Scope{TotalCount: 0, TotalIsExact: true}); s.TotalCount == nil || *s.TotalCount != 0 {
		t.Fatal("精确的 0 应照常输出")
	}
}

func TestSummaryAnswerKindAndBasis(t *testing.T) {
	if k := answerKind(answerv1.AnswerKind_ANSWER_KIND_SUMMARY); k != "summary" {
		t.Fatalf("主题汇总的 answer_kind: %s", k)
	}
	if w := whyOf(&answerv1.Why{BasedOnSummaries: true}); !w.BasedOnSummaries {
		t.Fatal("why 应标明基于文档摘要归纳")
	}
}

func TestFiltersValidityRoundTrip(t *testing.T) {
	f := (&filtersJSON{DocTypes: []string{"DT-POLICY"}, ValidityStatuses: []string{"effective"}}).proto()
	if f == nil || f.GetValidityStatuses()[0] != commonv1.ValidityStatus_VALIDITY_STATUS_EFFECTIVE {
		t.Fatalf("效力状态转换: %v", f)
	}
	if back := filtersOf(f); back.ValidityStatuses[0] != "effective" || back.DocTypes[0] != "DT-POLICY" {
		t.Fatalf("反向转换: %+v", back)
	}
	if (&filtersJSON{ValidityStatuses: []string{"作废"}}).proto() != nil {
		t.Fatal("非法的效力状态应拒绝")
	}
}
