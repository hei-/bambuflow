package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/tenancy"
)

// DeleteConversation 记下调用者与会话 ID；conv_down 模拟会话存储不可用。
func (f *fakeOrchestrator) DeleteConversation(ctx context.Context, req *answerv1.DeleteConversationRequest) (*answerv1.DeleteConversationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.GetConversationId() == "conv_down" {
		return nil, status.Error(codes.Unavailable, "down")
	}
	f.deleted = append(f.deleted, grpcmw.PrincipalFrom(ctx).GetSub()+"/"+req.GetConversationId())
	return &answerv1.DeleteConversationResponse{}, nil
}

// GetAnswer 模拟 orchestrator 的判定：alice 是提问人；审计岗凭 as_auditor 可看；其余一律 NOT_FOUND。
func (f *fakeOrchestrator) GetAnswer(ctx context.Context, req *answerv1.GetAnswerRequest) (*answerv1.GetAnswerResponse, error) {
	md := grpcmw.PrincipalFrom(ctx)
	f.mu.Lock()
	f.replayReq = req
	_, f.hasDeadline = ctx.Deadline()
	f.mu.Unlock()
	access := answerv1.ReplayAccess_REPLAY_ACCESS_ASKER
	switch {
	case req.GetAnswerId() != "ans_1":
		return nil, status.Error(codes.NotFound, "回答不存在")
	case md.GetSub() == "alice":
	case req.GetAsAuditor():
		access = answerv1.ReplayAccess_REPLAY_ACCESS_AUDITOR
	default:
		return nil, status.Error(codes.NotFound, "回答不存在")
	}
	return &answerv1.GetAnswerResponse{
		Answer: &answerv1.AnswerResponse{
			AnswerId: "ans_1", TraceId: "trace-original", Status: answerv1.Status_STATUS_ANSWERED, Answer: "甲[S1]。",
			Sentences: []*answerv1.Sentence{{Text: "甲[S1]。", End: 6, RefIds: []int32{1}, ClaimType: answerv1.ClaimType_CLAIM_TYPE_QUOTE}},
			Citations: []*answerv1.Citation{{RefId: 1, ChunkId: "wiki:doc-1#a1", DocId: "doc-1"}},
			Audit:     &answerv1.AuditBlocks{CandidateChunkIds: []string{"不应出现"}},
		},
		Question: "授信额度怎么计算？", CreatedAt: timestamppb.New(time.Date(2026, 10, 6, 1, 2, 3, 0, time.UTC)), Access: access,
	}, nil
}

func (f *fakeOrchestrator) CreateFeedback(ctx context.Context, req *answerv1.CreateFeedbackRequest) (*answerv1.CreateFeedbackResponse, error) {
	f.mu.Lock()
	f.feedbackReq = req
	f.mu.Unlock()
	if req.GetIdempotencyKey() == "used-with-other-body" {
		return nil, status.Error(codes.AlreadyExists, "冲突")
	}
	if grpcmw.PrincipalFrom(ctx).GetSub() != "alice" {
		return nil, status.Error(codes.NotFound, "回答不存在")
	}
	return &answerv1.CreateFeedbackResponse{Feedback: &answerv1.Feedback{
		FeedbackId: "fb_1", AnswerId: req.GetAnswerId(), Rating: req.GetRating(), SentenceIndex: req.SentenceIndex,
		CreatedAt: timestamppb.New(time.Date(2026, 10, 6, 1, 2, 3, 0, time.UTC)),
	}}, nil
}

func TestReplayByAskerAndAuditor(t *testing.T) {
	e := newEnv(t)
	r := e.request(t, http.MethodGet, "/v1/answers/ans_1", e.authz.token(t, "alice", "console"), "", nil)
	if r.status != 200 || r.body["access"] != "asker" || r.body["trace_id"] != "trace-original" ||
		r.body["question"] != "授信额度怎么计算？" || r.body["created_at"] != "2026-10-06T01:02:03Z" || r.body["body_withheld"] != false {
		t.Fatalf("提问人回放: %d %v", r.status, r.body)
	}
	if _, ok := r.body["audit"]; ok {
		t.Fatal("审计字段不能返回给调用者")
	}
	if req := e.orchestrator.replayReq; req.GetAsAuditor() || !e.orchestrator.hasDeadline {
		t.Fatalf("非审计岗不应带 as_auditor，且应带 deadline: %v", req)
	}
	recs := e.audit.snapshot()
	if last := recs[len(recs)-1]; last.Endpoint != "answer-replay" || last.AnswerID != "ans_1" || last.Access != "asker" ||
		len(last.ShownChunkIDs) != 1 || len(last.CandidateChunkIDs) != 0 {
		t.Fatalf("回放的审计: %+v", last)
	}

	r = e.request(t, http.MethodGet, "/v1/answers/ans_1", e.authz.token(t, "audrey", "console"), "", nil)
	if r.status != 200 || r.body["access"] != "auditor" || !e.orchestrator.replayReq.GetAsAuditor() {
		t.Fatalf("审计岗回放: %d %v", r.status, r.body)
	}
	recs = e.audit.snapshot()
	if last := recs[len(recs)-1]; last.Sub != "audrey" || last.Access != "auditor" {
		t.Fatalf("审计岗查看要写审计: %+v", last)
	}
}

func TestReplayNotFoundForOthers(t *testing.T) {
	e := newEnv(t)
	for name, path := range map[string]string{
		"别人的回答":   "/v1/answers/ans_1",
		"不存在":     "/v1/answers/ans_2",
		"ID 格式不对": "/v1/answers/bat_1",
	} {
		user := "dual"
		if name != "别人的回答" {
			user = "alice"
		}
		r := e.request(t, http.MethodGet, path, e.authz.token(t, user, "biz-credit"), "", nil)
		if r.status != 404 || r.body["type"] != "not_found" {
			t.Errorf("%s: 得到 %d %v", name, r.status, r.body)
		}
	}
}

func TestFeedback(t *testing.T) {
	e := newEnv(t)
	tok := e.authz.token(t, "alice", "console")
	key := map[string]string{"Idempotency-Key": "k-1"}
	r := e.request(t, http.MethodPost, "/v1/answers/ans_1/feedback", tok,
		`{"rating":"bad_citation","sentence_index":0,"comment":"引用的是旧版"}`, key)
	if r.status != 200 || r.body["feedback_id"] != "fb_1" || r.body["rating"] != "bad_citation" || r.body["sentence_index"] != float64(0) {
		t.Fatalf("反馈: %d %v", r.status, r.body)
	}
	req := e.orchestrator.feedbackReq
	if req.GetIdempotencyKey() != "k-1" || req.GetComment() != "引用的是旧版" || req.GetSentenceIndex() != 0 || req.SentenceIndex == nil {
		t.Fatalf("转给 orchestrator 的请求: %v", req)
	}
	recs := e.audit.snapshot()
	if last := recs[len(recs)-1]; last.Endpoint != "answer-feedback" || last.AnswerID != "ans_1" {
		t.Fatalf("反馈的审计: %+v", last)
	}

	cases := []struct {
		name, user, path, body string
		headers                map[string]string
		status                 int
		typ                    string
	}{
		{"缺幂等键", "alice", "/v1/answers/ans_1/feedback", `{"rating":"useful"}`, nil, 400, "invalid_request"},
		{"幂等键过长", "alice", "/v1/answers/ans_1/feedback", `{"rating":"useful"}`, map[string]string{"Idempotency-Key": strings.Repeat("k", 129)}, 400, "invalid_request"},
		{"取值不对", "alice", "/v1/answers/ans_1/feedback", `{"rating":"great"}`, key, 400, "invalid_request"},
		{"说明过长", "alice", "/v1/answers/ans_1/feedback", `{"rating":"wrong","comment":"` + strings.Repeat("长", 501) + `"}`, key, 400, "invalid_request"},
		{"句子下标为负", "alice", "/v1/answers/ans_1/feedback", `{"rating":"wrong","sentence_index":-1}`, key, 400, "invalid_request"},
		{"未知字段", "alice", "/v1/answers/ans_1/feedback", `{"rating":"wrong","score":1}`, key, 400, "invalid_request"},
		{"ID 格式不对", "alice", "/v1/answers/x/feedback", `{"rating":"wrong"}`, key, 404, "not_found"},
		{"不是提问人", "audrey", "/v1/answers/ans_1/feedback", `{"rating":"wrong"}`, key, 404, "not_found"},
		{"同键不同内容", "alice", "/v1/answers/ans_1/feedback", `{"rating":"wrong"}`, map[string]string{"Idempotency-Key": "used-with-other-body"}, 409, "idempotency_conflict"},
	}
	for _, c := range cases {
		r := e.request(t, http.MethodPost, c.path, e.authz.token(t, c.user, "console"), c.body, c.headers)
		if r.status != c.status || r.body["type"] != c.typ {
			t.Errorf("%s: 得到 %d %v，期望 %d %s", c.name, r.status, r.body["type"], c.status, c.typ)
		}
	}
}

func TestDeleteConversation(t *testing.T) {
	e := newEnv(t)
	tok := e.authz.token(t, "alice", "console")
	r := e.request(t, http.MethodDelete, "/v1/conversations/conv_abc123", tok, "", nil)
	if r.status != 204 || r.raw != "" {
		t.Fatalf("删除会话: %d %q", r.status, r.raw)
	}
	e.orchestrator.mu.Lock()
	deleted := e.orchestrator.deleted
	e.orchestrator.mu.Unlock()
	if len(deleted) != 1 || deleted[0] != "alice/conv_abc123" {
		t.Fatalf("应以调用者身份转给 orchestrator: %v", deleted)
	}
	recs := e.audit.snapshot()
	if last := recs[len(recs)-1]; last.Endpoint != "conversation-delete" || last.Status != 204 {
		t.Fatalf("删除会话的审计: %+v", last)
	}
	if r := e.request(t, http.MethodDelete, "/v1/conversations/abc", tok, "", nil); r.status != 404 {
		t.Errorf("ID 格式不对应为 404: %d", r.status)
	}
	if r := e.request(t, http.MethodDelete, "/v1/conversations/conv_down", tok, "", nil); r.status != 503 {
		t.Errorf("会话存储不可用应为 503: %d", r.status)
	}
	if r := e.request(t, http.MethodDelete, "/v1/conversations/conv_abc123", "", "", nil); r.status != 401 {
		t.Errorf("未认证应为 401: %d", r.status)
	}
}

func TestFeedbackNotOfferedOverMCP(t *testing.T) {
	for _, name := range tenancy.MCPTools {
		if strings.Contains(name, "feedback") {
			t.Fatalf("MCP 保持只读，不开放反馈: %s", name)
		}
	}
}
