package writer

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

func validRequest() *ingestionv1.WriteDocumentRequest {
	return &ingestionv1.WriteDocumentRequest{
		SourceId: "src_wiki", DocId: "credit-policy", ContentHash: "h1", OccurredAt: timestamppb.Now(),
		Classification: commonv1.Classification_CLASSIFICATION_INTERNAL, EmbeddingModelVersion: "m1", ChunkerVersion: "c1",
		Chunks: []*ingestionv1.Chunk{{
			ChunkId: "src_wiki:credit-policy#a1", Text: "第一章 总则", TextHash: "t1",
			Span: &commonv1.Span{CharStart: 0, CharEnd: 6}, Embedding: make([]float32, Dim),
		}},
	}
}

func TestValidate(t *testing.T) {
	if err := validate(validRequest()); err != nil {
		t.Fatalf("合法请求被拒: %v", err)
	}
	cases := map[string]func(r *ingestionv1.WriteDocumentRequest){
		"缺 span":  func(r *ingestionv1.WriteDocumentRequest) { r.Chunks[0].Span = nil },
		"span 倒置": func(r *ingestionv1.WriteDocumentRequest) { r.Chunks[0].Span = &commonv1.Span{CharStart: 5, CharEnd: 5} },
		"span 太短": func(r *ingestionv1.WriteDocumentRequest) { r.Chunks[0].Span = &commonv1.Span{CharStart: 0, CharEnd: 1} },
		"页码为 0": func(r *ingestionv1.WriteDocumentRequest) {
			r.Chunks[0].Span.Page = proto.Int32(0)
		},
		"区域倒置": func(r *ingestionv1.WriteDocumentRequest) {
			r.Chunks[0].Span.Bbox = &commonv1.BoundingBox{X0: 100, Y0: 10, X1: 50, Y1: 20}
		},
		"区域坐标为负": func(r *ingestionv1.WriteDocumentRequest) {
			r.Chunks[0].Span.Bbox = &commonv1.BoundingBox{X0: -1, Y0: 10, X1: 50, Y1: 20}
		},
		"向量维度不对":        func(r *ingestionv1.WriteDocumentRequest) { r.Chunks[0].Embedding = make([]float32, 768) },
		"chunk_id 前缀不对": func(r *ingestionv1.WriteDocumentRequest) { r.Chunks[0].ChunkId = "src_other:x#a1" },
		"chunk_id 重复": func(r *ingestionv1.WriteDocumentRequest) {
			r.Chunks = append(r.Chunks, r.Chunks[0])
		},
		"缺密级":   func(r *ingestionv1.WriteDocumentRequest) { r.Classification = 0 },
		"缺发生时间": func(r *ingestionv1.WriteDocumentRequest) { r.OccurredAt = nil },
		"缺嵌入模型": func(r *ingestionv1.WriteDocumentRequest) { r.EmbeddingModelVersion = "" },
		"生效日期格式不对": func(r *ingestionv1.WriteDocumentRequest) {
			r.Metadata = &commonv1.DocMetadata{EffectiveDate: "2025/04/01"}
		},
		"效力状态越界": func(r *ingestionv1.WriteDocumentRequest) { r.Metadata = &commonv1.DocMetadata{ValidityStatus: 9} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := validRequest()
			mutate(r)
			if err := validate(r); err == nil {
				t.Fatal("应拒收")
			}
		})
	}
}

func TestValidateRejectsWholeDocumentOnOneBadChunk(t *testing.T) {
	r := validRequest()
	bad := &ingestionv1.Chunk{ChunkId: "src_wiki:credit-policy#b2", Text: "x", TextHash: "t2", Embedding: make([]float32, Dim)}
	r.Chunks = append(r.Chunks, bad)
	err := validate(r)
	if err == nil || !strings.Contains(err.Error(), "第 1 块") {
		t.Fatalf("一块缺 span 整份拒收，并指出是哪一块: %v", err)
	}
}

func TestValidateAcceptsPageAndBox(t *testing.T) {
	r := validRequest()
	r.Chunks[0].Span.Page = proto.Int32(2)
	r.Chunks[0].Span.Bbox = &commonv1.BoundingBox{X0: 56, Y0: 90, X1: 540, Y1: 160}
	if err := validate(r); err != nil {
		t.Fatalf("带页码与区域的合法块被拒: %v", err)
	}
}

func TestSpanJSON(t *testing.T) {
	raw, err := spanJSON(&commonv1.Span{CharStart: 3, CharEnd: 9, Page: proto.Int32(2),
		Bbox: &commonv1.BoundingBox{X0: 1, Y0: 2, X1: 3, Y1: 4}})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"bbox":[1,2,3,4],"char_end":9,"char_start":3,"line_end":null,"line_start":null,"page":2}`
	if string(raw) != want {
		t.Fatalf("span 列格式变了:\n got %s\nwant %s", raw, want)
	}
}

func TestMetadataMissingFollowsEmptyFields(t *testing.T) {
	md := metadataOf(&commonv1.DocMetadata{
		DocType: "DT-POLICY", EffectiveDate: "2025-04-01",
		// 调用方给的 missing 不作数，按字段是否为空重算
		Missing: []string{"doc_type"},
	})
	if strings.Join(md.missing, ",") != "dept,topic_codes,validity_status" || md.effectiveDate == nil {
		t.Fatalf("缺失字段: %v", md.missing)
	}
	// 没有元数据的请求：五个字段都算缺失
	if got := metadataOf(nil); len(got.missing) != 5 || got.topics == nil {
		t.Fatalf("没有元数据: %+v", got)
	}
}

func withEnrichment() *ingestionv1.WriteDocumentRequest {
	r := validRequest()
	r.Chunks[0].ParentChunkId = "src_wiki:credit-policy#p-1"
	r.Enrichment = &ingestionv1.Enrichment{
		SummarizerVersion: "extractive-1",
		SummaryChunk: &ingestionv1.Chunk{
			ChunkId: "src_wiki:credit-policy#s1", Text: "第一章 总则", TextHash: "t9",
			Span: &commonv1.Span{CharStart: 0, CharEnd: 6}, Embedding: make([]float32, Dim),
		},
		Keywords: []string{"总则"},
		Sections: []*ingestionv1.SectionSummary{{ParentChunkId: "src_wiki:credit-policy#p-1", Summary: "总则"}},
	}
	return r
}

func TestValidateEnrichment(t *testing.T) {
	if err := validate(withEnrichment()); err != nil {
		t.Fatalf("带摘要的合法请求被拒: %v", err)
	}
	cases := map[string]func(r *ingestionv1.WriteDocumentRequest){
		"缺生成器版本":      func(r *ingestionv1.WriteDocumentRequest) { r.Enrichment.SummarizerVersion = "" },
		"摘要块前缀不对":     func(r *ingestionv1.WriteDocumentRequest) { r.Enrichment.SummaryChunk.ChunkId = "src_other:x#s1" },
		"摘要块与正文块同 ID": func(r *ingestionv1.WriteDocumentRequest) { r.Enrichment.SummaryChunk.ChunkId = r.Chunks[0].ChunkId },
		"摘要块缺 span":   func(r *ingestionv1.WriteDocumentRequest) { r.Enrichment.SummaryChunk.Span = nil },
		"摘要块向量维度不对":   func(r *ingestionv1.WriteDocumentRequest) { r.Enrichment.SummaryChunk.Embedding = nil },
		"章节不在本文档":     func(r *ingestionv1.WriteDocumentRequest) { r.Enrichment.Sections[0].ParentChunkId = "src_wiki:x#p-2" },
		"章节摘要为空":      func(r *ingestionv1.WriteDocumentRequest) { r.Enrichment.Sections[0].Summary = "" },
		"章节重复": func(r *ingestionv1.WriteDocumentRequest) {
			r.Enrichment.Sections = append(r.Enrichment.Sections, r.Enrichment.Sections[0])
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := withEnrichment()
			mutate(r)
			if err := validate(r); err == nil {
				t.Fatal("应拒收")
			}
		})
	}
}

func TestTitleKeyMatchesProcessing(t *testing.T) {
	for in, want := range map[string]string{
		" 授信管理办法（2025 年修订） ": "授信管理办法",
		"个人消费贷款管理办法(2020版)":  "个人消费贷款管理办法",
		"信用卡分期管理规定":          "信用卡分期管理规定",
	} {
		if got := TitleKey(in); got != want {
			t.Errorf("TitleKey(%q) = %q，应为 %q", in, got, want)
		}
	}
}
