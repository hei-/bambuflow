package api

import (
	"net/http"
	"regexp"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	answerv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/admission"
)

// 回放与反馈（技术设计文档 13.1，gateway 详设 4.1）：转到 orchestrator，回答记录存在那里。
// 谁能看、谁能反馈由 orchestrator 按记录里的提问人判定，其他人一律 404；gateway 只负责鉴权、
// 按租户配置判定审计岗、写审计。反馈是写操作，不对 MCP 开放（gateway 详设 7.1，部署配置清单 P16）。

const (
	replayDeadline   = 2 * time.Second
	feedbackDeadline = time.Second
	maxFeedbackNote  = 500
)

var answerIDPattern = regexp.MustCompile(`^ans_[A-Za-z0-9]{1,64}$`)

var feedbackRatings = map[string]answerv1.FeedbackRating{
	"useful":       answerv1.FeedbackRating_FEEDBACK_RATING_USEFUL,
	"wrong":        answerv1.FeedbackRating_FEEDBACK_RATING_WRONG,
	"bad_citation": answerv1.FeedbackRating_FEEDBACK_RATING_BAD_CITATION,
}

// replayResult 是回答契约加上回放才有的字段。
type replayResult struct {
	answerResult
	// 已做 PII 掩码；涉及受限级数据、不保存正文时为空
	Question     string `json:"question,omitempty"`
	CreatedAt    string `json:"created_at"`
	BodyWithheld bool   `json:"body_withheld"`
	// asker 或 auditor
	Access string `json:"access"`
}

// answerTarget 认证后取路径里的回答 ID；格式不对按不存在处理。
func (s *Server) answerTarget(r *http.Request, c *call) (string, bool, error) {
	// 回放与反馈只读写回答记录、不调用大模型，过载时与仅检索同一优先级
	principal, t, err := s.authorize(r, c, access{priority: admission.PrioritySearch})
	if err != nil {
		return "", false, err
	}
	id := r.PathValue("answer_id")
	if !answerIDPattern.MatchString(id) {
		return "", false, problemNotFound
	}
	c.answerID = id
	return principal, t.Auditor(c.groups), nil
}

// ---- GET /v1/answers/{answer_id} ----

// getAnswer 回放一次回答。引用与列表项已由 orchestrator 按调用者当前的权限复核；
// 返回给调用者的块按“展示”写审计：审计岗查看别人的回答也是一次看到（技术设计文档 8.3）。
func (s *Server) getAnswer(_ http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, auditor, err := s.answerTarget(r, c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := downstream(c.ctx, principal, "answer", "", replayDeadline)
	defer cancel()
	resp, err := s.Orchestrator.GetAnswer(ctx, &answerv1.GetAnswerRequest{AnswerId: c.answerID, AsAuditor: auditor})
	if err != nil {
		return nil, fromGRPC(err)
	}
	a := resp.GetAnswer()
	c.access = enumName(resp.GetAccess())
	shown := &answerv1.AuditBlocks{}
	for _, x := range a.GetCitations() {
		shown.ShownChunkIds = append(shown.ShownChunkIds, x.GetChunkId())
	}
	for _, x := range a.GetSentences() {
		if d := x.GetDocument(); d != nil {
			shown.ListedDocIds = append(shown.ListedDocIds, d.GetDocId())
		}
	}
	c.blocks = shown
	return replayResult{
		answerResult: answerJSON(&call{answerID: c.answerID, traceID: a.GetTraceId()}, a),
		Question:     resp.GetQuestion(), CreatedAt: timeOf(resp.GetCreatedAt()),
		BodyWithheld: resp.GetBodyWithheld(), Access: c.access,
	}, nil
}

// ---- POST /v1/answers/{answer_id}/feedback ----

type feedbackRequest struct {
	Rating string `json:"rating"`
	// 指向原回答 sentences 中的一句
	SentenceIndex *int32 `json:"sentence_index,omitempty"`
	Comment       string `json:"comment,omitempty"`
}

type feedbackJSON struct {
	FeedbackID    string `json:"feedback_id"`
	AnswerID      string `json:"answer_id"`
	Rating        string `json:"rating"`
	SentenceIndex *int32 `json:"sentence_index,omitempty"`
	CreatedAt     string `json:"created_at"`
}

// createFeedback 只有提问人本人可以提交；必须带 Idempotency-Key，同键同内容返回原结果，内容不同 409。
// 幂等键由 orchestrator 与反馈同库判重，不经 Redis：判重与写入在同一处完成，Redis 不可用时也不会重复。
func (s *Server) createFeedback(_ http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, _, err := s.answerTarget(r, c)
	if err != nil {
		return nil, err
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > maxIdempotencyKey {
		return nil, problemInvalid
	}
	var req feedbackRequest
	if err := decode(r, &req); err != nil {
		return nil, err
	}
	rating, ok := feedbackRatings[req.Rating]
	if !ok || utf8.RuneCountInString(req.Comment) > maxFeedbackNote || (req.SentenceIndex != nil && *req.SentenceIndex < 0) {
		return nil, problemInvalid
	}
	ctx, cancel := downstream(c.ctx, principal, "answer", "", feedbackDeadline)
	defer cancel()
	resp, err := s.Orchestrator.CreateFeedback(ctx, &answerv1.CreateFeedbackRequest{
		AnswerId: c.answerID, Rating: rating, SentenceIndex: req.SentenceIndex, Comment: req.Comment, IdempotencyKey: key,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil, problemIdempotency
		}
		return nil, fromGRPC(err)
	}
	f := resp.GetFeedback()
	return feedbackJSON{
		FeedbackID: f.GetFeedbackId(), AnswerID: f.GetAnswerId(), Rating: enumName(f.GetRating()),
		SentenceIndex: f.SentenceIndex, CreatedAt: timeOf(f.GetCreatedAt()),
	}, nil
}
