package vocab

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"google.golang.org/grpc"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// IndexRemapper 是 indexing 的 IndexAdminService.RemapTerms。
type IndexRemapper interface {
	RemapTerms(ctx context.Context, in *adminv1.RemapTermsRequest, opts ...grpc.CallOption) (*adminv1.RemapTermsResponse, error)
}

// Remapper 把已入库文档元数据里停用的编码改成替代编码（governance 详设 6.1：编码只停用不删除）。
// 停用词条后立即执行一次，并定期兜底重放（indexing 一侧是幂等的 UPDATE）。
type Remapper struct {
	Store Snapshots
	Index IndexRemapper
	Log   *slog.Logger
}

// maxChain 是替代链的最大长度：A 停用指向 B、B 又停用指向 C 时一路跟到 C
const maxChain = 16

// Remaps 由词表算出要改的编码：每个停用编码直接改到替代链末端的在用编码；链断了或成环的跳过。
func Remaps(terms []Term) []*adminv1.TermRemap {
	type key struct{ vocab, code string }
	by := map[key]Term{}
	for _, t := range terms {
		by[key{t.Vocabulary, t.Code}] = t
	}
	var out []*adminv1.TermRemap
	for _, t := range terms {
		if t.Status != "deprecated" || t.ReplacedBy == "" {
			continue
		}
		target, ok := t.ReplacedBy, false
		for range maxChain {
			next, found := by[key{t.Vocabulary, target}]
			if !found {
				break
			}
			if next.Status == "active" {
				ok = true
				break
			}
			target = next.ReplacedBy
		}
		if ok {
			out = append(out, &adminv1.TermRemap{Vocabulary: t.Vocabulary, FromCode: t.Code, ToCode: target})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GetVocabulary() != out[j].GetVocabulary() {
			return out[i].GetVocabulary() < out[j].GetVocabulary()
		}
		return out[i].GetFromCode() < out[j].GetFromCode()
	})
	return out
}

// Apply 按当前词表重放一次。
func (r *Remapper) Apply(ctx context.Context) error {
	_, terms, err := r.Store.Snapshot(ctx)
	if err != nil {
		return err
	}
	remaps := Remaps(terms)
	if len(remaps) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	resp, err := r.Index.RemapTerms(ctx, &adminv1.RemapTermsRequest{Remaps: remaps})
	if err != nil {
		return err
	}
	if resp.GetDocuments() > 0 {
		r.Log.InfoContext(ctx, "已按替代编码重新标注文档", slog.Int64("documents", resp.GetDocuments()))
	}
	return nil
}

// Run 每隔 every 重放一次，直到 ctx 结束。
func (r *Remapper) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := r.Apply(ctx); err != nil && ctx.Err() == nil {
			r.Log.WarnContext(ctx, "重新标注受控词表编码失败，稍后重试", slog.Any("err", err))
		}
	}
}
