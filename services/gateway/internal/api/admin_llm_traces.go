package api

import (
	"context"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// GET /v1/admin/llm-traces/{trace_id}：一次回答的全部模型调用与逐句校验结果（技术设计文档 14.2，orchestrator 详设 13.3）。
// 内容是完整的提示词与模型输出，管理审计照常记下谁在什么时候看了哪一条（不记内容）。

type llmCallJSON struct {
	Use              string `json:"use"`
	Model            string `json:"model"`
	Level            string `json:"level"`
	SystemPrompt     string `json:"system_prompt"`
	UserPrompt       string `json:"user_prompt"`
	Output           string `json:"output"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
}

type sentenceVerdictJSON struct {
	Text    string  `json:"text"`
	Passed  bool    `json:"passed"`
	Reason  string  `json:"reason"`
	Support float64 `json:"support"`
	Refs    []int32 `json:"refs"`
}

type llmTraceJSON struct {
	TraceID   string                `json:"trace_id"`
	AnswerID  string                `json:"answer_id"`
	Tenant    string                `json:"tenant"`
	CreatedAt string                `json:"created_at,omitempty"`
	Calls     []llmCallJSON         `json:"calls"`
	Verdicts  []sentenceVerdictJSON `json:"verdicts"`
}

func (s *Server) getLLMTrace(ctx context.Context, in *adminInput) (adminOutput, error) {
	r, err := s.LLMTraces.GetLlmTrace(ctx, &adminv1.GetLlmTraceRequest{TraceId: in.params["trace_id"]})
	if err != nil {
		return adminOutput{}, err
	}
	t := r.GetTrace()
	out := llmTraceJSON{TraceID: t.GetTraceId(), AnswerID: t.GetAnswerId(), Tenant: t.GetTenant(), CreatedAt: rfc3339(t.GetCreatedAt()),
		Calls: []llmCallJSON{}, Verdicts: []sentenceVerdictJSON{}}
	for _, c := range t.GetCalls() {
		out.Calls = append(out.Calls, llmCallJSON{Use: c.GetUse(), Model: c.GetModel(), Level: enumName(c.GetLevel()),
			SystemPrompt: c.GetSystemPrompt(), UserPrompt: c.GetUserPrompt(), Output: c.GetOutput(),
			PromptTokens: c.GetPromptTokens(), CompletionTokens: c.GetCompletionTokens()})
	}
	for _, v := range t.GetVerdicts() {
		out.Verdicts = append(out.Verdicts, sentenceVerdictJSON{Text: v.GetText(), Passed: v.GetPassed(), Reason: v.GetReason(),
			Support: v.GetSupport(), Refs: nonNil(v.GetRefs())})
	}
	return adminOutput{body: out}, nil
}
