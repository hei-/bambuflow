package reconcile

import (
	"slices"
	"testing"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

func indexed(id, hash string, tokens []string, tombstoned bool) *ingestionv1.IndexedDocument {
	return &ingestionv1.IndexedDocument{DocId: id, ContentHash: hash, AclTokens: tokens,
		Classification: commonv1.Classification_CLASSIFICATION_INTERNAL, Tombstoned: tombstoned}
}

func TestCompare(t *testing.T) {
	src := func(hash string, tokens ...string) Source {
		return Source{Fingerprint: hash + "+f|acl", ContentHash: hash, ACLTokens: tokens, Classification: "internal"}
	}
	source := map[string]Source{
		"same":    src("h1", "g:a", "g:b"),
		"lost":    src("h2", "g:a"),
		"changed": src("h3new", "g:a"),
		"acl":     src("h4", "g:a", "g:c"),
		"expired": src("h5", "g:a"),
	}
	f, c := Compare(source, []*ingestionv1.IndexedDocument{
		// 权限 token 的顺序不同不算不一致
		indexed("same", "h1", []string{"g:b", "g:a"}, false),
		indexed("changed", "h3", []string{"g:a"}, false),
		indexed("acl", "h4", []string{"g:a"}, false),
		indexed("expired", "h5", []string{"g:a"}, true),
		indexed("gone", "h6", []string{"g:a"}, false),
		indexed("gone-already", "h7", nil, true),
	})
	if want := (Counts{Missing: 1, ContentMismatch: 1, ACLMismatch: 1, Orphaned: 1, TombstonedInSource: 1, Indexed: 6}); c != want {
		t.Fatalf("%+v", c)
	}
	if !slices.Equal(f.Del, []string{"changed", "lost"}) {
		t.Fatalf("漏的与内容不一致的重新发出: %v", f.Del)
	}
	if f.Set["acl"] != "h4+f|reconcile" || f.Set["gone"] != "reconcile|reconcile" || len(f.Set) != 2 {
		t.Fatalf("权限摘要作废、源端已删的补进状态: %v", f.Set)
	}
	// 密级不同同样按权限不一致处理
	other := src("h1", "g:a", "g:b")
	other.Classification = "confidential"
	if _, c := Compare(map[string]Source{"same": other}, []*ingestionv1.IndexedDocument{indexed("same", "h1", []string{"g:a", "g:b"}, false)}); c.ACLMismatch != 1 {
		t.Fatalf("%+v", c)
	}
}
