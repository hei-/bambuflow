package api

import (
	"context"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 删除凭证（技术设计文档 9.2，评估报告 K6）：GET /v1/admin/deletion-receipts?doc_id=&source_id=，由 indexing 实现。

type storeDeletionJSON struct {
	Store       string `json:"store"`
	CompletedAt string `json:"completed_at"`
}

type deletionReceiptJSON struct {
	ReceiptID     string              `json:"receipt_id"`
	DocID         string              `json:"doc_id"`
	SourceID      string              `json:"source_id"`
	TombstonedAt  string              `json:"tombstoned_at,omitempty"`
	Stores        []storeDeletionJSON `json:"stores"`
	PendingStores []string            `json:"pending_stores"`
	CreatedAt     string              `json:"created_at"`
}

type deletionReceiptListJSON struct {
	Receipts      []deletionReceiptJSON `json:"receipts"`
	NextPageToken string                `json:"next_page_token,omitempty"`
}

func (s *Server) listDeletionReceipts(ctx context.Context, in *adminInput) (adminOutput, error) {
	doc, source := in.query.Get("doc_id"), in.query.Get("source_id")
	if doc == "" && source == "" {
		return adminOutput{}, problemInvalid
	}
	size, err := pageSize(in)
	if err != nil {
		return adminOutput{}, err
	}
	r, err := s.Receipts.ListDeletionReceipts(ctx, &adminv1.ListDeletionReceiptsRequest{DocId: doc, SourceId: source, PageSize: size,
		PageToken: in.query.Get("page_token")})
	if err != nil {
		return adminOutput{}, err
	}
	out := deletionReceiptListJSON{Receipts: []deletionReceiptJSON{}, NextPageToken: r.GetNextPageToken()}
	for _, rc := range r.GetReceipts() {
		j := deletionReceiptJSON{ReceiptID: rc.GetReceiptId(), DocID: rc.GetDocId(), SourceID: rc.GetSourceId(),
			TombstonedAt: rfc3339(rc.GetTombstonedAt()), Stores: []storeDeletionJSON{}, PendingStores: nonNil(rc.GetPendingStores()),
			CreatedAt: rfc3339(rc.GetCreatedAt())}
		for _, st := range rc.GetStores() {
			j.Stores = append(j.Stores, storeDeletionJSON{Store: st.GetStore(), CompletedAt: rfc3339(st.GetCompletedAt())})
		}
		out.Receipts = append(out.Receipts, j)
	}
	return adminOutput{body: out}, nil
}
