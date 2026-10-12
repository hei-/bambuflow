package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	"github.com/hei-/bambuflow/services/gateway/internal/audit"
)

// POST /v1/answers 的 SSE 形式（stream = true，gateway 详设第 6 节）：把 orchestrator 的服务端流逐个转成 SSE 事件。
// 事件顺序由 orchestrator 保证（meta → citation 先于引用它的 sentence → final），这里只透传、不重排、不合并；
// 流开始后的下游错误按 fromGRPC 映射成 error 事件，发送后结束。

type metaJSON struct {
	AnswerID       string `json:"answer_id"`
	TraceID        string `json:"trace_id"`
	ConversationID string `json:"conversation_id,omitempty"`
}

type streamSentenceJSON struct {
	sentenceJSON
	BlockType string     `json:"block_type"`
	Scope     *scopeJSON `json:"scope,omitempty"`
}

type finalJSON struct {
	Status     string          `json:"status"`
	Why        whyJSON         `json:"why"`
	AnswerKind string          `json:"answer_kind"`
	Confidence *confidenceJSON `json:"confidence,omitempty"`
}

type recvResult struct {
	ev  *answerv1.StreamAnswerResponse
	err error
}

// frameFunc 把一个对外事件（meta、citation、sentence、final、error）写成 SSE 帧。
// /v1/answers 用具名事件；兼容端点改写成 chat.completion.chunk（compat.go）。
type frameFunc func(event string, v any) string

// namedEvent 是 /v1/answers 的 SSE 帧：事件名在 event 字段，data 为 JSON。
func namedEvent(event string, v any) string {
	b, _ := json.Marshal(v)
	return fmt.Sprintf("event: %s\ndata: %s\n\n", event, b)
}

// streamAnswer 在收到 meta 之前出错时返回对外错误（由 wrap 按普通 HTTP 错误返回）；收到 meta 后自己写响应与审计。
func (s *Server) streamAnswer(ctx context.Context, w http.ResponseWriter, c *call, req *answerv1.AnswerRequest, frame frameFunc) error {
	c.orchStart = s.Now()
	stream, err := s.Orchestrator.StreamAnswer(ctx, &answerv1.StreamAnswerRequest{Request: req})
	if err != nil {
		return s.orchestratorErr(c, err)
	}
	first, err := stream.Recv()
	if err != nil {
		return s.orchestratorErr(c, err)
	}
	if first.GetMeta() == nil {
		s.Log.ErrorContext(c.ctx, "orchestrator 的流没有以 meta 开头")
		return problemUnavailable
	}
	// 放行前写入口审计：写不进去就不开始输出（fail-closed）
	if err := s.writeAudit(c, http.StatusOK, "", "open"); err != nil {
		s.Log.ErrorContext(c.ctx, "入口审计写入失败", slog.Any("err", err))
		return problemUnavailable
	}
	c.streamed = true

	// Recv 会阻塞，放到单独的 goroutine 里，主循环才能同时发心跳
	events := make(chan recvResult)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			ev, err := stream.Recv()
			select {
			case events <- recvResult{ev, err}:
			case <-done:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	// 关闭入口代理的响应缓冲，每个事件立即送达
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	send := func(event string, v any) {
		_, _ = io.WriteString(w, frame(event, v))
		_ = rc.Flush()
	}
	send("meta", metaJSON{AnswerID: c.answerID, TraceID: c.traceID, ConversationID: first.GetMeta().GetConversationId()})

	heartbeat := time.NewTicker(sseHeartbeat)
	defer heartbeat.Stop()
	reason := s.relay(c, events, heartbeat.C, send, func() {
		_, _ = io.WriteString(w, ": keep-alive\n\n")
		_ = rc.Flush()
	})
	if c.answerSHA == "" {
		// 没等到 final（客户端断开、出错）：摘要只算已推送的句子
		c.answerSHA = audit.AnswerDigest(c.pushed)
	}
	// 内容已经发出，结束记录写不进去只能记日志
	if err := s.writeAudit(c, http.StatusOK, reason, "end"); err != nil {
		s.Log.ErrorContext(c.ctx, "入口审计写入失败", slog.Any("err", err))
	}
	return nil
}

// relay 转发事件直到 final、出错或客户端断开；返回结束原因：空为正常结束，cancelled 为客户端断开，其余为 error 事件的类型。
func (s *Server) relay(c *call, events <-chan recvResult, beat <-chan time.Time, send func(string, any), ping func()) string {
	fail := func(p problem) string {
		send("error", problemBody(c.traceID, p))
		return p.typ
	}
	for {
		var r recvResult
		select {
		case <-beat:
			ping()
			continue
		case r = <-events:
		}
		if r.err != nil {
			switch {
			case c.ctx.Err() != nil:
				// c.ctx 随客户端连接结束；下游 deadline 在另一个上下文上，到期按错误处理
				return "cancelled"
			case errors.Is(r.err, io.EOF):
				// 流在 final 之前结束：客户端不能把已收到的句子当成完整回答
				s.Log.ErrorContext(c.ctx, "orchestrator 的流没有 final 就结束了")
				return fail(problemUnavailable)
			default:
				return fail(s.orchestratorErr(c, r.err))
			}
		}
		switch ev := r.ev.GetEvent().(type) {
		case *answerv1.StreamAnswerResponse_Citation:
			c.restrictedChunk(ev.Citation.GetClassification(), ev.Citation.GetChunkId())
			if err := s.writeRestricted(c); err != nil {
				s.Log.ErrorContext(c.ctx, "受限级访问记录写入失败", slog.Any("err", err))
				return fail(problemUnavailable)
			}
			send("citation", citationOf(ev.Citation))
		case *answerv1.StreamAnswerResponse_Sentence:
			if d := ev.Sentence.GetSentence().GetDocument(); d != nil {
				c.restrictedDoc(d.GetClassification(), d.GetDocId())
				if err := s.writeRestricted(c); err != nil {
					s.Log.ErrorContext(c.ctx, "受限级访问记录写入失败", slog.Any("err", err))
					return fail(problemUnavailable)
				}
			}
			c.pushed = append(c.pushed, ev.Sentence.GetSentence().GetText())
			send("sentence", streamSentenceJSON{sentenceOf(ev.Sentence.GetSentence()), enumName(ev.Sentence.GetBlockType()), scopeOf(ev.Sentence.GetScope())})
		case *answerv1.StreamAnswerResponse_Final:
			c.blocks, c.usage = ev.Final.GetAudit(), ev.Final.GetUsage()
			c.recordAnswer(ev.Final.GetStatus(), ev.Final.GetWhy(), c.pushed)
			s.observe(c, ev.Final.GetWhy().GetIntent())
			send("final", finalJSON{Status: enumName(ev.Final.GetStatus()), Why: whyOf(ev.Final.GetWhy()), AnswerKind: answerKind(ev.Final.GetAnswerKind()),
				Confidence: confidenceOf(ev.Final.GetConfidence())})
			return ""
		}
		// 其他事件（重复的 meta、以后新增的类型）不转发：对外事件只按 OpenAPI 列出的类型发送
	}
}
