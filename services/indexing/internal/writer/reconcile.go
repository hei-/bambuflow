package writer

import (
	"log/slog"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

// reconcileBatch 是 ListSourceDocuments 每条消息的文档数，也是每次查询的页大小。
const reconcileBatch = 1000

// ListSourceDocuments 列出一个数据源已入库的文档，供 connectors 每日对账（技术设计文档 5.5）。按 doc_id 分页读取，
// 不在一个长事务里读完：对账只求最终一致，分页之间的写入由下一次对账发现。
func (s *Server) ListSourceDocuments(r *ingestionv1.ListSourceDocumentsRequest,
	stream grpc.ServerStreamingServer[ingestionv1.ListSourceDocumentsResponse]) error {
	if r.GetSourceId() == "" || len(r.GetSourceId()) > 128 {
		return status.Error(codes.InvalidArgument, "source_id 必填，不超过 128 个字符")
	}
	ctx := stream.Context()
	after := ""
	for {
		rows, err := s.Pool.Query(ctx, `
			SELECT a.doc_id, d.content_hash, a.acl_tokens, a.classification, a.state = 'tombstoned'
			FROM idx.doc_acl a JOIN idx.documents d USING (doc_id)
			WHERE a.source_id = $1 AND a.doc_id > $2 ORDER BY a.doc_id LIMIT $3`, r.GetSourceId(), after, reconcileBatch)
		if err != nil {
			return s.listFailed(r, err)
		}
		docs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*ingestionv1.IndexedDocument, error) {
			var d ingestionv1.IndexedDocument
			var classification int32
			err := row.Scan(&d.DocId, &d.ContentHash, &d.AclTokens, &classification, &d.Tombstoned)
			d.Classification = commonv1.Classification(classification)
			return &d, err
		})
		if err != nil {
			return s.listFailed(r, err)
		}
		if len(docs) == 0 {
			return nil
		}
		if err := stream.Send(&ingestionv1.ListSourceDocumentsResponse{Documents: docs}); err != nil {
			return err
		}
		if len(docs) < reconcileBatch {
			return nil
		}
		after = docs[len(docs)-1].GetDocId()
	}
}

func (s *Server) listFailed(r *ingestionv1.ListSourceDocumentsRequest, err error) error {
	s.Log.Error("列出已入库文档失败", slog.String("source_id", r.GetSourceId()), slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}
