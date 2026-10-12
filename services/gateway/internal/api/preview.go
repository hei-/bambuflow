package api

import (
	"net/http"
	"strings"
	"unicode/utf8"

	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/hei-/bambuflow/services/gateway/internal/admission"
)

// ---- GET /v1/documents/{doc_id}/preview ----

// 引用预览（技术设计文档 13.9、gateway 详设 4.1）：按调用者当前权限返回一个块的原文片段与高亮位置。
// 权限在 retrieval 读正文之前判定；无权与不存在一律 404，不让调用方据此探测文档是否存在。

const (
	maxIDLen    = 256
	maxQuoteLen = 2000 // 字符，与问题上限一致
)

type highlightJSON struct {
	// 在 text 中的字符区间（按 Unicode 字符计），左闭右开
	Start int `json:"start"`
	End   int `json:"end"`
}

type previewResult struct {
	TraceID        string          `json:"trace_id"`
	IndexVersion   string          `json:"index_version"`
	DocID          string          `json:"doc_id"`
	DocVersion     int64           `json:"doc_version"`
	ChunkID        string          `json:"chunk_id"`
	Title          string          `json:"title"`
	HeadingPath    []string        `json:"heading_path"`
	Text           string          `json:"text"`
	Span           spanJSON        `json:"span"`
	SourceURI      string          `json:"source_uri"`
	SourceType     string          `json:"source_type"`
	Owner          string          `json:"owner,omitempty"`
	Classification string          `json:"classification"`
	Highlights     []highlightJSON `json:"highlights"`
}

func (s *Server) preview(_ http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, _, err := s.authorize(r, c, access{priority: admission.PrioritySearch})
	if err != nil {
		return nil, err
	}
	docID, q := r.PathValue("doc_id"), r.URL.Query()
	chunkID, quote := q.Get("chunk_id"), q.Get("quote")
	if docID == "" || len(docID) > maxIDLen || chunkID == "" || len(chunkID) > maxIDLen ||
		utf8.RuneCountInString(quote) > maxQuoteLen {
		return nil, problemInvalid
	}
	ctx, cancel := downstream(c.ctx, principal, "search", "", searchDeadline)
	defer cancel()
	e, err := s.Retrieval.GetExcerpt(ctx, &retrievalv1.GetExcerptRequest{DocId: docID, ChunkId: chunkID})
	if err != nil {
		return nil, fromGRPC(err)
	}
	// 用户看到了这块的正文：按展示的块写审计
	c.blocks = &answerv1.AuditBlocks{ShownChunkIds: []string{e.GetChunkId()}}
	c.restrictedChunk(e.GetClassification(), e.GetChunkId())
	return previewResult{
		TraceID: c.traceID, IndexVersion: e.GetIndexVersion(), DocID: e.GetDocId(), DocVersion: e.GetDocVersion(),
		ChunkID: e.GetChunkId(), Title: e.GetTitle(), HeadingPath: nonNil(e.GetHeadingPath()), Text: e.GetText(),
		Span: spanOf(e.GetSpan()), SourceURI: e.GetSourceUri(), SourceType: enumName(e.GetSourceType()),
		Owner: e.GetOwner(), Classification: enumName(e.GetClassification()),
		Highlights: highlights(e.GetText(), quote),
	}, nil
}

// highlights 找出引文在片段里出现的位置（按字符计）。找不到时为空：片段照常返回，只是不高亮。
// 正文已脱敏而引文没有，或引文跨了块，都会找不到，这不是错误。
func highlights(text, quote string) []highlightJSON {
	out := []highlightJSON{}
	quote = strings.TrimSpace(quote)
	if quote == "" {
		return out
	}
	for from := 0; ; {
		i := strings.Index(text[from:], quote)
		if i < 0 {
			return out
		}
		start := utf8.RuneCountInString(text[:from+i])
		out = append(out, highlightJSON{start, start + utf8.RuneCountInString(quote)})
		from += i + len(quote)
	}
}
