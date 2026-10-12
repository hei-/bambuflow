package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	"github.com/hei-/bambuflow/services/gateway/internal/admission"
)

// POST /v1/compat/chat/completions（技术设计文档 13.9、gateway 详设 4.1）：让已有常见 chat completions 客户端的系统
// 快速接入。只是协议适配层：身份、权限、逐句校验、审计与 /v1/answers 完全相同，校验不通过的句子同样不会出现在 content 里。
//
// 映射：最后一条 user 消息作为 question；完整的回答契约（citations、status、why、trace_id 等）放在扩展字段 rag 里。
// 前面的消息（含 system）不转给 orchestrator：回答契约还没有会话上下文的入口，system 消息也不能覆盖平台规则。
// 参数分三类：
//   - 忽略：model（模型由租户配置与密级路由决定）、采样与长度类参数（temperature、top_p、max_tokens 等）、user、metadata 等；
//   - 拒绝（400）：工具调用（tools、functions 等）、n > 1、非文本的 response_format、logprobs、多模态输入与输出；
//   - 其他未知参数按参数不合法拒绝。

// compatModel 是响应里的 model：实际使用的生成模型在 rag.why.generation_model 里。
const compatModel = "bambuflow"

type compatMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
	Name    string          `json:"name,omitempty"`
}

type compatRequest struct {
	Messages []compatMessage `json:"messages"`
	Stream   bool            `json:"stream,omitempty"`

	// 忽略的参数：只为通过未知字段检查而声明
	Model               json.RawMessage `json:"model,omitempty"`
	Temperature         json.RawMessage `json:"temperature,omitempty"`
	TopP                json.RawMessage `json:"top_p,omitempty"`
	MaxTokens           json.RawMessage `json:"max_tokens,omitempty"`
	MaxCompletionTokens json.RawMessage `json:"max_completion_tokens,omitempty"`
	PresencePenalty     json.RawMessage `json:"presence_penalty,omitempty"`
	FrequencyPenalty    json.RawMessage `json:"frequency_penalty,omitempty"`
	LogitBias           json.RawMessage `json:"logit_bias,omitempty"`
	Stop                json.RawMessage `json:"stop,omitempty"`
	Seed                json.RawMessage `json:"seed,omitempty"`
	User                json.RawMessage `json:"user,omitempty"`
	Metadata            json.RawMessage `json:"metadata,omitempty"`
	Store               json.RawMessage `json:"store,omitempty"`
	StreamOptions       json.RawMessage `json:"stream_options,omitempty"`
	ParallelToolCalls   json.RawMessage `json:"parallel_tool_calls,omitempty"`
	ReasoningEffort     json.RawMessage `json:"reasoning_effort,omitempty"`
	ServiceTier         json.RawMessage `json:"service_tier,omitempty"`

	// 拒绝的参数
	N              *int            `json:"n,omitempty"`
	Tools          json.RawMessage `json:"tools,omitempty"`
	ToolChoice     json.RawMessage `json:"tool_choice,omitempty"`
	Functions      json.RawMessage `json:"functions,omitempty"`
	FunctionCall   json.RawMessage `json:"function_call,omitempty"`
	ResponseFormat json.RawMessage `json:"response_format,omitempty"`
	Logprobs       *bool           `json:"logprobs,omitempty"`
	TopLogprobs    json.RawMessage `json:"top_logprobs,omitempty"`
	Modalities     []string        `json:"modalities,omitempty"`
	Audio          json.RawMessage `json:"audio,omitempty"`
	Prediction     json.RawMessage `json:"prediction,omitempty"`
}

func unsupported(param, why string) problem {
	return problem{http.StatusBadRequest, "invalid_request", "不支持的参数 " + param + "：" + why}
}

// rejectUnsupported 检查会改变回答形态、平台无法满足的参数：宁可明确拒绝，也不让调用方误以为生效了。
func (r *compatRequest) rejectUnsupported() error {
	switch {
	case r.Tools != nil || r.ToolChoice != nil || r.Functions != nil || r.FunctionCall != nil:
		return unsupported("tools", "兼容端点只做知识库问答，不支持工具调用")
	case r.N != nil && *r.N != 1:
		return unsupported("n", "每个请求只返回一个回答")
	case r.ResponseFormat != nil && !textFormat(r.ResponseFormat):
		return unsupported("response_format", "回答结构由回答契约决定，见扩展字段 rag")
	case (r.Logprobs != nil && *r.Logprobs) || r.TopLogprobs != nil:
		return unsupported("logprobs", "回答经逐句校验后输出，没有逐 token 概率")
	case r.Audio != nil || (len(r.Modalities) > 0 && (len(r.Modalities) != 1 || r.Modalities[0] != "text")):
		return unsupported("modalities", "只支持文本")
	case r.Prediction != nil:
		return unsupported("prediction", "回答不能预填")
	}
	return nil
}

func textFormat(raw json.RawMessage) bool {
	var f struct {
		Type string `json:"type"`
	}
	return json.Unmarshal(raw, &f) == nil && f.Type == "text"
}

// textContent 取消息的文本：字符串，或只含 text 部分的数组。
func textContent(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", problemInvalid
	}
	var b strings.Builder
	for _, p := range parts {
		if p.Type != "text" {
			return "", unsupported("messages[].content", "只支持文本内容")
		}
		b.WriteString(p.Text)
	}
	return b.String(), nil
}

// question 校验消息列表并取最后一条 user 消息作为问题。
func (r *compatRequest) question() (string, error) {
	if len(r.Messages) == 0 {
		return "", problemInvalid
	}
	for _, m := range r.Messages {
		switch m.Role {
		case "system", "developer", "user", "assistant":
		case "tool", "function":
			return "", unsupported("messages[].role", "不支持工具调用的消息")
		default:
			return "", problemInvalid
		}
	}
	last := r.Messages[len(r.Messages)-1]
	if last.Role != "user" {
		return "", problem{http.StatusBadRequest, "invalid_request", "最后一条消息必须是 user 消息"}
	}
	return textContent(last.Content)
}

type compatMessageOut struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type compatChoice struct {
	Index        int              `json:"index"`
	Message      compatMessageOut `json:"message"`
	FinishReason string           `json:"finish_reason"`
}

type compatCompletion struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []compatChoice `json:"choices"`
	// 扩展字段：完整的回答契约
	RAG answerResult `json:"rag"`
}

func (s *Server) compatChatCompletions(w http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, _, err := s.authorize(r, c, access{answers: true, priority: admission.PriorityCompat})
	if err != nil {
		return nil, err
	}
	var req compatRequest
	if err := decode(r, &req); err != nil {
		return nil, err
	}
	if err := req.rejectUnsupported(); err != nil {
		return nil, err
	}
	q, err := req.question()
	if err != nil {
		return nil, err
	}
	c.question = q
	if err := validQuestion(q); err != nil {
		return nil, err
	}
	if req.Stream {
		if err := s.acquireStream(c); err != nil {
			return nil, err
		}
	}
	c.answerID = newAnswerID()
	ctx, cancel := downstream(c.ctx, principal, "answer", c.answerID, answerDeadline)
	defer cancel()
	areq := &answerv1.AnswerRequest{Question: q, MaxCitations: 8}
	if req.Stream {
		return nil, s.streamAnswer(ctx, w, c, areq, s.compatFrames(c))
	}
	resp, err := s.answer(ctx, c, areq)
	if err != nil {
		return nil, err
	}
	res := answerJSON(c, resp)
	return compatCompletion{
		ID: c.answerID, Object: "chat.completion", Created: c.start.Unix(), Model: compatModel,
		Choices: []compatChoice{{Index: 0, Message: compatMessageOut{Role: "assistant", Content: res.Answer}, FinishReason: "stop"}},
		RAG:     res,
	}, nil
}

// ---- 流式：每个对外事件改写成一个 chat.completion.chunk ----

type compatDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type compatChunkChoice struct {
	Index        int         `json:"index"`
	Delta        compatDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

// compatStreamRAG 是流式块的扩展字段，每块只带一种：meta、引用、句子或 final。
type compatStreamRAG struct {
	Meta     *metaJSON           `json:"meta,omitempty"`
	Citation *citationJSON       `json:"citation,omitempty"`
	Sentence *streamSentenceJSON `json:"sentence,omitempty"`
	Final    *finalJSON          `json:"final,omitempty"`
}

type compatChunk struct {
	ID      string              `json:"id"`
	Object  string              `json:"object"`
	Created int64               `json:"created"`
	Model   string              `json:"model"`
	Choices []compatChunkChoice `json:"choices"`
	RAG     compatStreamRAG     `json:"rag"`
}

// compatFrames 返回兼容端点的 SSE 帧：data 为 chat.completion.chunk，正常结束时追加 [DONE]；
// 出错时发 {"error": problem} 后结束，不发 [DONE]，客户端据此知道回答未完成。
// 句子之间按块换行，与回答契约里 answer 的拼接方式一致（orchestrator 在段落之间插入换行）。
func (s *Server) compatFrames(c *call) frameFunc {
	lastBlock := ""
	return func(event string, v any) string {
		chunk := compatChunk{ID: c.answerID, Object: "chat.completion.chunk", Created: c.start.Unix(), Model: compatModel,
			Choices: []compatChunkChoice{{Index: 0}}}
		switch x := v.(type) {
		case metaJSON:
			chunk.Choices[0].Delta.Role = "assistant"
			chunk.RAG.Meta = &x
		case citationJSON:
			chunk.RAG.Citation = &x
		case streamSentenceJSON:
			if lastBlock != "" && x.BlockID != lastBlock {
				chunk.Choices[0].Delta.Content = "\n"
			}
			lastBlock = x.BlockID
			chunk.Choices[0].Delta.Content += x.Text
			chunk.RAG.Sentence = &x
		case finalJSON:
			stop := "stop"
			chunk.Choices[0].FinishReason = &stop
			chunk.RAG.Final = &x
		default:
			// error 事件
			b, _ := json.Marshal(map[string]any{"error": v})
			return fmt.Sprintf("data: %s\n\n", b)
		}
		b, _ := json.Marshal(chunk)
		out := fmt.Sprintf("data: %s\n\n", b)
		if event == "final" {
			out += "data: [DONE]\n\n"
		}
		return out
	}
}
