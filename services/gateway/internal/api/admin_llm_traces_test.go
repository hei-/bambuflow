package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
)

type fakeLLMTraces struct {
	adminv1.LlmTraceAdminServiceClient
	got *adminv1.GetLlmTraceRequest
}

func (f *fakeLLMTraces) GetLlmTrace(ctx context.Context, in *adminv1.GetLlmTraceRequest, _ ...grpc.CallOption) (*adminv1.GetLlmTraceResponse, error) {
	f.got = in
	if in.GetTraceId() != "0123456789abcdef0123456789abcdef" {
		return nil, status.Error(codes.NotFound, "追踪记录不存在")
	}
	return &adminv1.GetLlmTraceResponse{Trace: &adminv1.LlmTrace{TraceId: in.GetTraceId(), AnswerId: "ans_1", Tenant: "credit",
		Calls: []*adminv1.LlmCall{{Use: "generate", Model: "qwen", Level: commonv1.Classification_CLASSIFICATION_CONFIDENTIAL,
			SystemPrompt: "系统", UserPrompt: "问题", Output: "回答 [S1]", PromptTokens: 12, CompletionTokens: 3}},
		Verdicts: []*adminv1.SentenceVerdict{{Text: "回答[S1]", Passed: true, Reason: "passed", Support: 0.9, Refs: []int32{1}}}}}, nil
}

// 只有 LLM 调试人员能看；每次查看都写管理审计，审计里不带提示词与输出。
func TestLLMTraceRoute(t *testing.T) {
	e := newEnv(t)
	f := &fakeLLMTraces{}
	e.server.LLMTraces = f
	dora, audra := e.authz.token(t, "dora", "console"), e.authz.token(t, "audra", "console")

	r := e.request(t, http.MethodGet, "/v1/admin/llm-traces/0123456789abcdef0123456789abcdef", dora, "", nil)
	call := r.body["calls"].([]any)[0].(map[string]any)
	if r.status != 200 || call["level"] != "confidential" || call["output"] != "回答 [S1]" || r.body["answer_id"] != "ans_1" {
		t.Fatalf("调试人员查看: %d %s", r.status, r.raw)
	}
	recs := e.audit.snapshot()
	last := recs[len(recs)-1]
	if last.Endpoint != "admin-llm-traces-get" || last.Sub != "dora" || last.Admin == nil {
		t.Fatalf("查看应写管理审计: %+v", last)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/llm-traces/ffffffffffffffffffffffffffffffff", dora, "", nil); r.status != 404 {
		t.Fatalf("不存在应 404: %d", r.status)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/llm-traces/0123456789abcdef0123456789abcdef", audra, "", nil); r.status != 403 {
		t.Fatalf("审计岗不能看追踪内容: %d", r.status)
	}
}
