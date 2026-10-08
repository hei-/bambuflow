// Package writer 实现 IndexWriterService：把 processing 交来的文档写入待发布版本（入库管道详设 14.3）。
package writer

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

// Dim 是 P0 嵌入模型（bge-m3 或其模拟服务）的向量维度，与表结构 vector(1024) 一致。
const Dim = 1024

// validate 在写库前检查整份文档；任何一块不合格整份拒收，不写半份文档。
// 内核兜底规则（技术设计文档 12.4）：块必须带原文位置，否则拒收。
func validate(r *ingestionv1.WriteDocumentRequest) error {
	if r.GetSourceId() == "" || r.GetDocId() == "" || r.GetContentHash() == "" {
		return fmt.Errorf("source_id、doc_id、content_hash 必填")
	}
	if r.GetOccurredAt() == nil {
		return fmt.Errorf("occurred_at 必填")
	}
	if r.GetEmbeddingModelVersion() == "" || r.GetChunkerVersion() == "" {
		return fmt.Errorf("embedding_model_version、chunker_version 必填")
	}
	if r.GetClassification() == commonv1.Classification_CLASSIFICATION_UNSPECIFIED {
		return fmt.Errorf("密级必填")
	}
	if d := r.GetMetadata().GetEffectiveDate(); d != "" {
		if _, err := time.Parse(time.DateOnly, d); err != nil {
			return fmt.Errorf("effective_date 应为 YYYY-MM-DD: %q", d)
		}
	}
	if _, ok := commonv1.ValidityStatus_name[int32(r.GetMetadata().GetValidityStatus())]; !ok {
		return fmt.Errorf("效力状态 %d 不合法", r.GetMetadata().GetValidityStatus())
	}
	prefix := r.GetSourceId() + ":" + r.GetDocId() + "#"
	seen := map[string]bool{}
	for i, c := range r.GetChunks() {
		if err := validChunk(c, prefix, seen); err != nil {
			return fmt.Errorf("第 %d 块: %w", i, err)
		}
	}
	if err := validateEnrichment(r.GetEnrichment(), r.GetChunks(), prefix, seen); err != nil {
		return err
	}
	return validateRelations(r)
}

// validChunk 检查一块（正文块或摘要块）；seen 记录已出现的 chunk_id，同一请求里不能重复。
func validChunk(c *ingestionv1.Chunk, prefix string, seen map[string]bool) error {
	switch {
	case !strings.HasPrefix(c.GetChunkId(), prefix):
		return fmt.Errorf("chunk_id 应以 %s 开头", prefix)
	case seen[c.GetChunkId()]:
		return fmt.Errorf("chunk_id 重复")
	case c.GetText() == "" || c.GetTextHash() == "":
		return fmt.Errorf("正文与 text_hash 必填")
	case c.GetSpan() == nil:
		return fmt.Errorf("缺少原文位置（span）")
	case c.GetSpan().GetCharStart() < 0 || c.GetSpan().GetCharEnd() <= c.GetSpan().GetCharStart():
		return fmt.Errorf("原文位置不合法 [%d, %d)", c.GetSpan().GetCharStart(), c.GetSpan().GetCharEnd())
	case int(c.GetSpan().GetCharEnd()-c.GetSpan().GetCharStart()) < utf8.RuneCountInString(c.GetText())/2:
		// 位置区间明显短于正文：多半是位置算错了，引用会定位到错误的原文
		return fmt.Errorf("原文位置与正文长度不符")
	case c.GetSpan().Page != nil && c.GetSpan().GetPage() < 1:
		return fmt.Errorf("页码 %d 不合法，应从 1 开始", c.GetSpan().GetPage())
	case !validBox(c.GetSpan().GetBbox()):
		return fmt.Errorf("区域坐标不合法")
	case len(c.GetEmbedding()) != Dim:
		return fmt.Errorf("向量维度 %d，应为 %d", len(c.GetEmbedding()), Dim)
	}
	seen[c.GetChunkId()] = true
	return nil
}

// validateEnrichment 检查摘要与关键词：摘要块按正文块的规则校验（引用它时同样要能定位原文），
// 章节摘要必须挂在本文档某块的父块上。不合格整份拒收，与块的校验一致。
func validateEnrichment(e *ingestionv1.Enrichment, chunks []*ingestionv1.Chunk, prefix string, seen map[string]bool) error {
	if e == nil {
		return nil
	}
	if e.GetSummarizerVersion() == "" {
		return fmt.Errorf("增强: 缺少摘要生成器版本")
	}
	if s := e.GetSummaryChunk(); s != nil {
		if err := validChunk(s, prefix, seen); err != nil {
			return fmt.Errorf("摘要块: %w", err)
		}
	}
	parents := map[string]bool{}
	for _, c := range chunks {
		parents[c.GetParentChunkId()] = true
	}
	done := map[string]bool{}
	for i, sec := range e.GetSections() {
		switch {
		case !parents[sec.GetParentChunkId()] || sec.GetParentChunkId() == "":
			return fmt.Errorf("第 %d 个章节摘要: 父块 %q 不在本文档的块里", i, sec.GetParentChunkId())
		case done[sec.GetParentChunkId()]:
			return fmt.Errorf("第 %d 个章节摘要: 父块重复", i)
		case sec.GetSummary() == "":
			return fmt.Errorf("第 %d 个章节摘要: 摘要为空", i)
		}
		done[sec.GetParentChunkId()] = true
	}
	return nil
}

// validBox 检查块的页面区域：没有区域时合法（文本类来源）；有区域时必须是左上到右下、坐标非负的矩形，
// 否则引用高亮会落到页面之外或错误的位置。
func validBox(b *commonv1.BoundingBox) bool {
	if b == nil {
		return true
	}
	return b.GetX0() >= 0 && b.GetY0() >= 0 && b.GetX1() > b.GetX0() && b.GetY1() > b.GetY0()
}

// metadataFields 是文档元数据的字段名，与契约 DocMetadata.missing 的取值一致。
var metadataFields = []string{"doc_type", "dept", "topic_codes", "effective_date", "validity_status"}

type docMetadata struct {
	docType, dept, vocabVersion string
	topics, missing             []string
	effectiveDate               *time.Time
	status                      int32
}

// metadataOf 把请求里的元数据转成表的列。missing 按字段是否为空重新算，不信任调用方给的列表：
// 统计 metadata_gaps 用的就是它，必须和字段本身一致。没有元数据的请求（旧版 processing）各字段都算缺失。
func metadataOf(m *commonv1.DocMetadata) docMetadata {
	out := docMetadata{
		docType: m.GetDocType(), dept: m.GetDept(), topics: m.GetTopicCodes(), vocabVersion: m.GetVocabularyVersion(),
		status: int32(m.GetValidityStatus()),
	}
	if out.topics == nil {
		out.topics = []string{}
	}
	if t, err := time.Parse(time.DateOnly, m.GetEffectiveDate()); err == nil {
		out.effectiveDate = &t
	}
	empty := map[string]bool{
		"doc_type": out.docType == "", "dept": out.dept == "", "topic_codes": len(out.topics) == 0,
		"effective_date": out.effectiveDate == nil, "validity_status": out.status == 0,
	}
	out.missing = []string{}
	for _, f := range metadataFields {
		if empty[f] {
			out.missing = append(out.missing, f)
		}
	}
	return out
}
