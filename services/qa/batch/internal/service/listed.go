package service

import (
	"fmt"
	"math"
	"slices"

	"google.golang.org/protobuf/proto"

	answerv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/answer/v1"
)

// 口径句的措辞与 orchestrator 清单路径（bambuflow_orchestrator/listing.py）一致：列表项删掉后按同一模板重写，
// 回答读起来与同步问答里权限刚好变化时的结果相同。
const (
	scopeEmpty = "没有找到符合条件的文档；可以放宽条件后再试。"
	scopeAll   = "以下列出全部 %d 份"
	scopeTop   = "以下列出最新的 %d 份"
)

// dropListed 去掉清单类回答里调用者已无权查看的列表项（K2 详设第 8 节），返回调整后的副本；没有要去掉的项时原样返回。
//
// 去掉的文档执行时已计入总数，口径块里的总数与列出数一起减掉：只减列出数会留下“共 N 份、列出 N−1 份”，
// 等于告诉调用者有一份他看不到的文档。总数不精确（超过上限）时只调列出数。
// 回答文本按句子区间重新拼接，句子位置、块的句子下标、审计里的列出文档同步更新。
// 句子区间不合法（越界、重叠）时无法安全调整，返回错误，由调用方按 fail-closed 处理整条。
func dropListed(a *answerv1.AnswerResponse, allowed map[string]bool) (*answerv1.AnswerResponse, error) {
	drop := map[int]bool{}
	var removed int32
	for i, s := range a.GetSentences() {
		if d := s.GetDocument(); d != nil && !allowed[d.GetDocId()] {
			drop[i] = true
			removed++
		}
	}
	if removed == 0 {
		return a, nil
	}
	out := proto.CloneOf(a)
	replace := map[int]string{}
	if scope := scopeBlock(out); scope != nil {
		sc := scope.GetScope()
		sc.Shown = max(0, sc.GetShown()-removed)
		if sc.GetTotalIsExact() {
			sc.TotalCount = max(0, sc.GetTotalCount()-int64(removed))
		}
		if len(scope.GetSentenceIndexes()) == 1 {
			replace[int(scope.GetSentenceIndexes()[0])] = scopeNote(sc)
		}
		if sc.GetTotalIsExact() && sc.GetTotalCount() == 0 {
			out.Status = answerv1.Status_STATUS_INSUFFICIENT_EVIDENCE
		}
		if au := out.GetAudit(); au != nil {
			au.TotalCount = sc.GetTotalCount()
		}
	}
	if err := rewrite(out, drop, replace); err != nil {
		return nil, err
	}
	if au := out.GetAudit(); au != nil {
		au.ListedDocIds = slices.DeleteFunc(au.ListedDocIds, func(id string) bool { return !allowed[id] })
	}
	return out, nil
}

func scopeBlock(a *answerv1.AnswerResponse) *answerv1.Block {
	for _, b := range a.GetBlocks() {
		if b.GetBlockType() == answerv1.BlockType_BLOCK_TYPE_SCOPE && b.GetScope() != nil {
			return b
		}
	}
	return nil
}

// scopeNote 按调整后的数量重写口径句（只有枚举回答带列表项，所以只需要枚举的写法）。
func scopeNote(sc *answerv1.Scope) string {
	n, shown := sc.GetTotalCount(), int64(sc.GetShown())
	if sc.GetTotalIsExact() && n == 0 {
		return scopeEmpty
	}
	total := fmt.Sprintf("%d 份", n)
	if !sc.GetTotalIsExact() {
		total = fmt.Sprintf("超过 %d 份", n)
	}
	listed := fmt.Sprintf(scopeTop, shown)
	if sc.GetTotalIsExact() && shown == n {
		listed = fmt.Sprintf(scopeAll, shown)
	}
	return fmt.Sprintf("符合条件的文档共 %s，%s（%s）。", total, listed, sc.GetBasis())
}

// rewrite 按句子区间（以字符计，与 orchestrator 一致）重新拼接回答文本：去掉 drop 里的句子及其前面的分隔，
// 用 replace 替换句子文本，再更新各句的位置与各块的句子下标；句子全被去掉的块一并去掉。
func rewrite(a *answerv1.AnswerResponse, drop map[int]bool, replace map[int]string) error {
	old := []rune(a.GetAnswer())
	sents := a.GetSentences()
	order := make([]int, len(sents))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(x, y int) int { return int(sents[x].GetStart() - sents[y].GetStart()) })
	var b []rune
	var err error
	pos, skipGap := 0, false
	for _, i := range order {
		s := sents[i]
		start, end := int(s.GetStart()), int(s.GetEnd())
		if start < pos || end < start || end > len(old) {
			return fmt.Errorf("句子 %d 的区间 [%d,%d) 不合法", i, start, end)
		}
		gap := old[pos:start]
		pos = end
		if drop[i] {
			// 去掉句子前面的分隔；它是第一句时，去掉它后面的分隔
			skipGap = skipGap || len(b) == 0
			continue
		}
		if !skipGap {
			b = append(b, gap...)
		}
		skipGap = false
		text := string(old[start:end])
		if r, ok := replace[i]; ok {
			text = r
			s.Text = r
		}
		if s.Start, err = offset(len(b)); err != nil {
			return err
		}
		b = append(b, []rune(text)...)
		if s.End, err = offset(len(b)); err != nil {
			return err
		}
	}
	b = append(b, old[pos:]...)
	a.Answer = string(b)

	newIndex := map[int]int32{}
	kept := make([]*answerv1.Sentence, 0, len(sents)-len(drop))
	var next int32
	for i, s := range sents {
		if !drop[i] {
			newIndex[i] = next
			next++
			kept = append(kept, s)
		}
	}
	a.Sentences = kept
	blocks := a.GetBlocks()[:0]
	for _, bl := range a.GetBlocks() {
		idx := bl.GetSentenceIndexes()[:0]
		for _, i := range bl.GetSentenceIndexes() {
			if n, ok := newIndex[int(i)]; ok {
				idx = append(idx, n)
			}
		}
		bl.SentenceIndexes = idx
		if len(idx) > 0 || bl.GetBlockType() == answerv1.BlockType_BLOCK_TYPE_SCOPE {
			blocks = append(blocks, bl)
		}
	}
	a.Blocks = blocks
	return nil
}

func offset(n int) (int32, error) {
	if n < 0 || n > math.MaxInt32 {
		return 0, fmt.Errorf("回答过长: %d 字", n)
	}
	return int32(n), nil
}
