package api

import (
	"fmt"

	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	"github.com/hei-/bambuflow/services/gateway/internal/audit"
)

// 受限级内容每次访问单独审计（技术设计文档 8.2、8.6）：入口审计一个请求一条，受限级的块或文档在发给调用方之前
// 另写一条 restricted_access 记录，只列这一次发出的受限级内容，写不进去就不发（fail-closed）。
// 一元响应在返回前写一条；SSE 在推送受限级引用或列表项之前各写一条，所以流中途断开时，
// 审计里的受限级访问正好是调用方已经收到的那些。审计索引把它单独归为 restricted 类，审计岗可以只查这一类。

func isRestricted(c commonv1.Classification) bool {
	return c == commonv1.Classification_CLASSIFICATION_RESTRICTED
}

// restrictedChunk 记下一个要发出的块；不是受限级时不记。
func (c *call) restrictedChunk(cls commonv1.Classification, chunkID string) {
	if !isRestricted(cls) {
		return
	}
	if c.restricted == nil {
		c.restricted = &answerv1.AuditBlocks{}
	}
	c.restricted.ShownChunkIds = append(c.restricted.ShownChunkIds, chunkID)
}

// restrictedDoc 记下一个要列出的文档；不是受限级时不记。
func (c *call) restrictedDoc(cls commonv1.Classification, docID string) {
	if !isRestricted(cls) {
		return
	}
	if c.restricted == nil {
		c.restricted = &answerv1.AuditBlocks{}
	}
	c.restricted.ListedDocIds = append(c.restricted.ListedDocIds, docID)
}

// restrictedAnswer 记下回答里受限级的引用与列表项。
func (c *call) restrictedAnswer(a *answerv1.AnswerResponse) {
	for _, x := range a.GetCitations() {
		c.restrictedChunk(x.GetClassification(), x.GetChunkId())
	}
	for _, x := range a.GetSentences() {
		if d := x.GetDocument(); d != nil {
			c.restrictedDoc(d.GetClassification(), d.GetDocId())
		}
	}
}

// writeRestricted 把记下的受限级内容写成一条单独的访问记录，写入后清空；没有时不写。
func (s *Server) writeRestricted(c *call) error {
	if c.restricted == nil {
		return nil
	}
	err := s.Audit.Write(audit.Record{
		Time: s.Now(), TraceID: c.traceID, AnswerID: c.answerID, BatchID: c.batchID, JTI: c.jti, Sub: c.sub, Tenant: c.tenant,
		Actor: c.actor, Endpoint: c.endpoint, Status: 200, Event: audit.EventRestricted,
		ShownChunkIDs: c.restricted.GetShownChunkIds(), ListedDocIDs: c.restricted.GetListedDocIds(), Access: c.access,
	})
	if err != nil {
		return fmt.Errorf("写受限级访问记录: %w", err)
	}
	c.restricted = nil
	return nil
}
