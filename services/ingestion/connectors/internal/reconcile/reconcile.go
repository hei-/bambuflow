// Package reconcile 是每日对账（技术设计文档 5.5，入库管道详设第 5 节）：拿源端全量清单与 indexing 已入库的文档比对，
// 发现漏同步（事件丢失、入库失败）、内容或权限不一致、源端已删而索引仍在用的文档。
//
// 对账不自己发事件，只把 connectors 的“已发出状态”改成与索引的实际情况一致，随后照常跑一轮增量同步：
// 漏的与内容不一致的从状态里去掉（重新发 UPSERT），权限不一致的把权限摘要作废（发 ACL_ONLY），
// 源端已删的补进状态（发 DELETE）。这样补发的删除同样受删除保护与待删批次约束，不另走一条路径。
package reconcile

import (
	"slices"
	"strings"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

// stale 是写进状态的占位摘要：与任何真实摘要都不相等，下一次比较必然判为变化。
const stale = "reconcile"

// Source 是源端清单里的一份文档：已发出状态里用的指纹，以及要与索引比对的内容摘要、权限与密级。
type Source struct {
	Fingerprint    string // 内容摘要+源端字段摘要|权限摘要，与已发出状态的格式相同
	ContentHash    string
	ACLTokens      []string
	Classification string // public | internal | confidential | restricted
}

// Counts 是对账结果，与 adminv1.Reconciliation 一一对应。
type Counts struct {
	Missing, ContentMismatch, ACLMismatch, Orphaned, TombstonedInSource, Indexed int
}

// Fixes 是对已发出状态的修改：Set 写入（覆盖），Del 去掉。
type Fixes struct {
	Set map[string]string
	Del []string
}

// Compare 比对源端清单与已入库文档，返回对已发出状态的修改与计数。已打删除标记的文档不复活：
// 它可能是按保留期到期的，或正等撤销复核（撤销后由 RefetchRequested 重新拉取）。
func Compare(source map[string]Source, indexed []*ingestionv1.IndexedDocument) (Fixes, Counts) {
	f := Fixes{Set: map[string]string{}}
	c := Counts{Indexed: len(indexed)}
	seen := make(map[string]bool, len(indexed))
	for _, d := range indexed {
		id := d.GetDocId()
		seen[id] = true
		s, inSource := source[id]
		switch {
		case d.GetTombstoned():
			if inSource {
				c.TombstonedInSource++
			}
		case !inSource:
			c.Orphaned++
			f.Set[id] = stale + "|" + stale
		case d.GetContentHash() != s.ContentHash:
			c.ContentMismatch++
			f.Del = append(f.Del, id)
		case !sameACL(d, s):
			c.ACLMismatch++
			content, _, _ := strings.Cut(s.Fingerprint, "|")
			f.Set[id] = content + "|" + stale
		}
	}
	for id := range source {
		if !seen[id] {
			c.Missing++
			f.Del = append(f.Del, id)
		}
	}
	slices.Sort(f.Del)
	return f, c
}

func sameACL(d *ingestionv1.IndexedDocument, s Source) bool {
	want := commonv1.Classification(commonv1.Classification_value["CLASSIFICATION_"+strings.ToUpper(s.Classification)])
	if d.GetClassification() != want {
		return false
	}
	a, b := slices.Clone(d.GetAclTokens()), slices.Clone(s.ACLTokens)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(slices.Compact(a), slices.Compact(b))
}
