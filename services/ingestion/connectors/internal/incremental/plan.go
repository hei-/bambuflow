// Package incremental 判断一轮同步要发出哪些变更（入库管道详设第 4、5 节）：只发新增与变化的文档，
// 源端已不存在的文档发删除，删除受保护阈值约束。
package incremental

import (
	"sort"
	"strings"
)

const (
	// 单轮待删超过已同步文档的 5% 或超过 1000 份时暂停删除，等管理员确认（入库管道详设第 5 节，建议值）
	maxDeleteRatio = 20 // 1/20 = 5%
	maxDeletes     = 1000
)

// Plan 是一轮同步的变更计划。Upserts、Deletes 按 doc_id 排序。
type Plan struct {
	Upserts []string
	// AclOnly 是内容未变、只有权限变化的文档：走快速通道，不重算向量（入库管道详设第 9 节）
	AclOnly []string
	Deletes []string
	// DeletesPaused 为 true 时本轮不发删除；Reason 说明原因（empty_listing / over_threshold）
	DeletesPaused bool
	Reason        string
	// Held 是因超过阈值被拦下的待删文档（over_threshold 时才有；清单为空时不推断任何删除，也就没有）
	Held []string
}

// Diff 比较上次已发出的状态与本轮清单。两者都是 doc_id → “内容摘要|权限摘要”。
// 内容摘要变了（或新文档）发 UPSERT；只有权限摘要变了发 ACL_ONLY。confirmDeletes 表示管理员已确认本轮删除。
func Diff(state, current map[string]string, confirmDeletes bool) Plan {
	var p Plan
	for id, fp := range current {
		old, ok := state[id]
		switch {
		case old == fp:
		case ok && content(old) == content(fp):
			p.AclOnly = append(p.AclOnly, id)
		default:
			p.Upserts = append(p.Upserts, id)
		}
	}
	sort.Strings(p.AclOnly)
	var deletes []string
	for id := range state {
		if _, ok := current[id]; !ok {
			deletes = append(deletes, id)
		}
	}
	sort.Strings(p.Upserts)
	sort.Strings(deletes)
	switch {
	case len(deletes) == 0:
	case len(current) == 0:
		// 清单拉取异常或返回空结果：视为本轮失败，不推断任何删除
		p.DeletesPaused, p.Reason = true, "empty_listing"
	case !confirmDeletes && (len(deletes) > maxDeletes || len(deletes)*maxDeleteRatio > len(state)):
		p.DeletesPaused, p.Reason, p.Held = true, "over_threshold", deletes
	default:
		p.Deletes = deletes
	}
	return p
}

// content 取指纹里的内容摘要部分。
func content(fp string) string {
	h, _, _ := strings.Cut(fp, "|")
	return h
}

// Incomplete 用在清单里有文档没能读出的一轮（入库管道详设第 11 节）：读不出的文档不在本轮清单里，
// 不能当成源端已删，所以这一轮不发删除、也不记待删批次，其余变更照常。
func (p *Plan) Incomplete() {
	if len(p.Deletes) > 0 || len(p.Held) > 0 {
		p.Deletes, p.Held, p.DeletesPaused, p.Reason = nil, nil, true, "incomplete_listing"
	}
}
