package api

import (
	"strings"
	"testing"

	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	"github.com/hei-/bambuflow/services/gateway/internal/audit"
)

const restricted = commonv1.Classification_CLASSIFICATION_RESTRICTED

// restrictedEvents 是一次引用了受限级块、又列出一份受限级文档的流式回答。
func restrictedEvents() []*answerv1.StreamAnswerResponse {
	evs := goodEvents()
	evs[1].GetCitation().Classification = restricted
	listed := &answerv1.StreamAnswerResponse{Event: &answerv1.StreamAnswerResponse_Sentence{Sentence: &answerv1.SentenceEvent{
		Sentence: &answerv1.Sentence{Text: "《反洗钱可疑交易名单》", ClaimType: answerv1.ClaimType_CLAIM_TYPE_LIST_ITEM, BlockId: "list",
			Document: &answerv1.ListedDocument{DocId: "doc-r", DocVersion: 2, Title: "反洗钱可疑交易名单", Classification: restricted}},
		BlockType: answerv1.BlockType_BLOCK_TYPE_LIST_ITEM,
	}}}
	return append(evs[:3:3], listed, evs[3])
}

func TestStreamWritesRestrictedAccessBeforeEachItem(t *testing.T) {
	e := newEnv(t)
	e.orchestrator.set(restrictedEvents(), nil, false)
	r := e.do(t, "/v1/answers", e.authz.token(t, "rita", "console"), "", qStream)
	if got := strings.Join(names(parseSSE(t, r.raw)), ","); r.status != 200 || got != "meta,citation,sentence,sentence,final" {
		t.Fatalf("得到 %d %s", r.status, got)
	}
	recs := e.audit.snapshot()
	var kinds []string
	for _, x := range recs {
		kinds = append(kinds, x.Stream+x.Event)
	}
	if strings.Join(kinds, ",") != "open,restricted_access,restricted_access,end" {
		t.Fatalf("受限级引用与列表项各单独写一条访问记录: %v", kinds)
	}
	if a, b := recs[1], recs[2]; strings.Join(a.ShownChunkIDs, ",") != "wiki:doc-1#a1" || len(a.ListedDocIDs) != 0 ||
		strings.Join(b.ListedDocIDs, ",") != "doc-r" || len(b.ShownChunkIDs) != 0 || a.Sub != "rita" || a.AnswerID != recs[0].AnswerID {
		t.Fatalf("访问记录只列这一次发出的受限级内容: %+v %+v", a, b)
	}
}

func TestStreamStopsWhenRestrictedAccessCannotBeAudited(t *testing.T) {
	e := newEnv(t)
	e.orchestrator.set(restrictedEvents(), nil, false)
	e.audit.failEvent = audit.EventRestricted
	r := e.do(t, "/v1/answers", e.authz.token(t, "rita", "console"), "", qStream)
	evs := parseSSE(t, r.raw)
	if got := strings.Join(names(evs), ","); got != "meta,error" || evs[1].data["type"] != "service_unavailable" ||
		strings.Contains(r.raw, "额度") {
		t.Fatalf("写不进访问记录时受限级引用不发出: %s", r.raw)
	}
}

func TestUnaryAnswerWritesRestrictedAccessFirst(t *testing.T) {
	e := newEnv(t)
	e.orchestrator.answer = &answerv1.AnswerResponse{
		Status: answerv1.Status_STATUS_ANSWERED,
		Citations: []*answerv1.Citation{
			{RefId: 1, ChunkId: "wiki:doc-1#a1", DocId: "doc-1", Quote: "受限原文", Classification: restricted},
			{RefId: 2, ChunkId: "wiki:doc-2#a1", DocId: "doc-2", Quote: "内部原文", Classification: commonv1.Classification_CLASSIFICATION_INTERNAL},
		},
		Sentences: []*answerv1.Sentence{{Text: "x [S1][S2]", RefIds: []int32{1, 2}}},
	}
	tok := e.authz.token(t, "rita", "console")
	if r := e.do(t, "/v1/answers", tok, "", q); r.status != 200 {
		t.Fatalf("得到 %d %s", r.status, r.raw)
	}
	recs := e.audit.snapshot()
	if len(recs) != 2 || recs[0].Event != audit.EventRestricted || strings.Join(recs[0].ShownChunkIDs, ",") != "wiki:doc-1#a1" ||
		recs[1].Event != "" {
		t.Fatalf("先写受限级访问记录（只含受限级的块），再写入口审计: %+v", recs)
	}

	e.audit.failEvent = audit.EventRestricted
	r := e.do(t, "/v1/answers", tok, "", q)
	if r.status != 503 || strings.Contains(r.raw, "受限原文") {
		t.Fatalf("写不进访问记录时不返回回答: %d %s", r.status, r.raw)
	}
	if last := e.audit.snapshot()[len(e.audit.snapshot())-1]; last.Status != 503 {
		t.Errorf("入口审计按 503 记录: %+v", last)
	}
}

func TestInternalContentWritesNoRestrictedAccess(t *testing.T) {
	e := newEnv(t)
	e.do(t, "/v1/search", e.authz.token(t, "alice", "console"), "", q)
	for _, x := range e.audit.snapshot() {
		if x.Event != "" {
			t.Fatalf("没有受限级内容时只写入口审计: %+v", x)
		}
	}
}
