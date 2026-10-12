package store

import (
	"strings"
	"testing"

	"github.com/hei-/bambuflow/services/retrieval/internal/search"
)

func TestMetaWhereNumbersParametersAndNeverInlinesValues(t *testing.T) {
	sql, args := metaWhere(search.MetaFilter{
		DocTypes: []string{"DT-POLICY'; DROP TABLE x; --"}, EffectiveFrom: "2025-01-01", Validity: []int32{1}, Missing: "dept",
	}, 7)
	want := " AND d.doc_type = ANY($7::text[]) AND d.effective_date >= $8::date AND d.validity_status = ANY($9::int[]) AND $10 = ANY(d.metadata_missing)"
	if sql != want || len(args) != 4 || strings.Contains(sql, "DROP") {
		t.Fatalf("得到 %q %v", sql, args)
	}
	if sql, args := metaWhere(search.MetaFilter{}, 5); sql != "" || args != nil {
		t.Fatalf("没有条件时应为空: %q %v", sql, args)
	}
}
