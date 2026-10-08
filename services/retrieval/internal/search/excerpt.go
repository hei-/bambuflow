package search

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

var errNoExcerpt = status.Error(codes.NotFound, "片段不存在")

// GetExcerpt 返回线上索引版本里一个块的原文片段（MCP get_document_excerpt，gateway 详设第 7 节）。
// 先用权限表的最新状态判定文档是否可见，可见才读正文；读正文的查询本身也带全部必含条件。
// 无权与不存在返回同一个 NOT_FOUND（技术设计文档 13.10），调用方无法据此探测文档是否存在。
func (s *Server) GetExcerpt(ctx context.Context, r *retrievalv1.GetExcerptRequest) (*retrievalv1.GetExcerptResponse, error) {
	if err := validPurpose(ctx); err != nil {
		return nil, err
	}
	if r.GetDocId() == "" || r.GetChunkId() == "" {
		return nil, status.Error(codes.InvalidArgument, "doc_id 与 chunk_id 都必填")
	}
	done, err := s.admit(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	f, pv, version, err := s.docScope(ctx, nil)
	if err != nil {
		return nil, err
	}
	acls, err := s.Store.DocACL(ctx, []string{r.GetDocId()})
	if err != nil {
		s.Log.ErrorContext(ctx, "读取权限表失败", slog.Any("err", err))
		return nil, errUnavailable
	}
	if a, ok := acls[r.GetDocId()]; !ok || f.Check(a) != retrievalv1.AccessReason_ACCESS_REASON_ALLOWED {
		return nil, errNoExcerpt
	}
	c, ok, err := s.Store.Excerpt(ctx, version, f, r.GetDocId(), r.GetChunkId())
	if err != nil {
		s.Log.ErrorContext(ctx, "读取片段失败", slog.Any("err", err))
		return nil, errUnavailable
	}
	if !ok {
		return nil, errNoExcerpt
	}
	return &retrievalv1.GetExcerptResponse{
		ChunkId: c.ChunkID, DocId: c.DocID, DocVersion: c.DocVersion, Text: text(f, c.Text), Span: c.Span, Title: c.Title,
		HeadingPath: c.HeadingPath, SourceUri: c.SourceURI, SourceType: c.SourceType, Owner: c.Owner,
		Classification: c.Classification, IndexVersion: version, PolicyVersion: pv, InjectionFlag: c.InjectionFlag,
	}, nil
}
