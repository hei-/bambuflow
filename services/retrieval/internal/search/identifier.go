package search

import (
	"context"
	"regexp"
	"strings"

	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

// 编号精确匹配（详设 6.2 identifier）：问题里出现制度文号、工单号、系统编码这类标识符时按原样匹配正文与标题，
// 弥补稠密与关键词召回对编号不敏感（分词会把“INC-20240312”切碎，向量也分不清相邻的编号）。
const (
	// identifierK 是这一路每个子查询最多召回的块数（详设 6.2）
	identifierK = 10
	// 精排分低于它的编号命中块不再置顶，按精排分与其他块一起排序（详设 7.1）
	pinFloor = 0.1
	// 一个子查询最多取几个标识符：再多说明不是在问某个编号
	maxIdentifiers = 5
)

var identifierPatterns = []*regexp.Regexp{
	// 公文文号：银发〔2023〕12 号、信贷发[2024]3号
	regexp.MustCompile(`\p{Han}{1,8}[〔\[［【(（]\d{4}[〕\]］】)）]\s*\d{1,4}\s*号`),
	// 字母前缀加数字的编码：INC-20240312、POL-2024-001、CR2024001、ISO27001
	regexp.MustCompile(`\b[A-Za-z]{2,10}[-_]?\d{3,}(?:[-_]\d+)*\b`),
	// 系统名的英文简称：CRM 系统、ESB 平台（只认紧跟“系统、平台、服务”的大写简称，单独的 API、PDF 不算）
	regexp.MustCompile(`\b([A-Z][A-Z0-9]{2,9})\s*(?:系统|平台|服务)`),
}

// identifiers 找出问题里的标识符，按出现顺序去重。文号里的空白去掉后再匹配（“12 号”与“12号”是同一个编号）。
func identifiers(text string) []string {
	var out []string
	seen := map[string]bool{}
	for _, p := range identifierPatterns {
		for _, m := range p.FindAllStringSubmatch(text, -1) {
			// 有分组时取分组（系统简称不含后面的“系统”二字），没有时取整个匹配
			id := strings.Join(strings.Fields(m[len(m)-1]), "")
			if key := strings.ToUpper(id); !seen[key] && len(out) < maxIdentifiers {
				seen[key] = true
				out = append(out, id)
			}
		}
	}
	return out
}

// identifier 按子查询里的标识符精确匹配。没有任何子查询含标识符时这一路无事可做（idle）。
func (s *Server) identifier(ctx context.Context, version string, f Filter, queries []*retrievalv1.SubQuery) route {
	r := route{name: "identifier", idle: true, lists: make([][]Candidate, len(queries))}
	for i, q := range queries {
		ids := identifiers(q.GetText())
		if len(ids) == 0 {
			continue
		}
		r.idle = false
		list, err := s.Store.Identifier(ctx, version, f, ids, identifierK)
		if err != nil {
			r.reason, r.err = "backend_error", err
			return r
		}
		r.lists[i] = list
	}
	return r
}
