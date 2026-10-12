package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	"github.com/hei-/bambuflow/libs/platform-go/tenancy"
	"github.com/hei-/bambuflow/services/gateway/internal/admission"
)

// 运行时保护（gateway 详设 5.1、5.2、第 6 节）：租户并发与每日 token 配额、每用户并发 SSE 流、全局自适应并发。

// used 返回 tenant 当日（测试环境按 UTC 计）已计入配额的 token 数。
func (e *env) used(t *testing.T, tenant string) int64 {
	t.Helper()
	v, err := e.redis.Get("bf:quota:tokens:" + tenant + ":" + time.Now().UTC().Format(time.DateOnly))
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *env) setAnswer(resp *answerv1.AnswerResponse, err error) {
	e.orchestrator.mu.Lock()
	defer e.orchestrator.mu.Unlock()
	e.orchestrator.answer, e.orchestrator.answerErr = resp, err
}

func TestQuotaReservesAndSettlesActualUsage(t *testing.T) {
	e := newEnv(t)
	tok := e.authz.token(t, "tina", "console")

	// 回报了用量：按实际用量结算（预占 8800，退回多出的部分）
	e.setAnswer(&answerv1.AnswerResponse{Status: answerv1.Status_STATUS_ANSWERED,
		Usage: &answerv1.TokenUsage{PromptTokens: 3000, CompletionTokens: 1000}}, nil)
	r := e.do(t, "/v1/answers", tok, "", q)
	if r.status != 200 || e.used(t, "tight") != 4000 {
		t.Fatalf("应按实际用量 4000 结算，得到 %d，已用 %d", r.status, e.used(t, "tight"))
	}
	if strings.Contains(r.raw, "tokens") || strings.Contains(r.raw, "usage") {
		t.Errorf("用量不对外返回: %s", r.raw)
	}
	recs := e.audit.snapshot()
	if last := recs[len(recs)-1]; last.PromptTokens != 3000 || last.CompletionTokens != 1000 {
		t.Errorf("用量应写入审计: %+v", last)
	}

	// 没有调用 orchestrator（参数不合法），或 orchestrator 在生成之前拒绝：预占全部退回
	if r := e.do(t, "/v1/answers", tok, "", `{"question":"  "}`); r.status != 400 || e.used(t, "tight") != 4000 {
		t.Fatalf("参数不合法应退回预占，得到 %d，已用 %d", r.status, e.used(t, "tight"))
	}
	e.setAnswer(nil, status.Error(codes.InvalidArgument, "bad"))
	if r := e.do(t, "/v1/answers", tok, "", q); r.status != 400 || e.used(t, "tight") != 4000 {
		t.Fatalf("orchestrator 拒绝应退回预占，得到 %d，已用 %d", r.status, e.used(t, "tight"))
	}

	// 生成中出错、没有回报用量：按预占量计，宁可多计
	e.setAnswer(nil, status.Error(codes.Internal, "boom"))
	if r := e.do(t, "/v1/answers", tok, "", q); r.status != 503 || e.used(t, "tight") != 4000+admission.EstimateTokens {
		t.Fatalf("没有用量时应按预占量计，得到 %d，已用 %d", r.status, e.used(t, "tight"))
	}

	// 剩余 7200，不够再预占一次：问答类接口一律 quota_exceeded，不带 Retry-After；仅检索照常
	e.setAnswer(nil, nil)
	r = e.do(t, "/v1/answers", tok, "", q)
	if r.status != 429 || r.body["type"] != "quota_exceeded" || r.header.Get("Retry-After") != "" {
		t.Fatalf("配额用尽应 429 quota_exceeded 且不带 Retry-After，得到 %d %s %v", r.status, r.raw, r.header)
	}
	if recs := e.audit.snapshot(); recs[len(recs)-1].Reason != "quota_exceeded" {
		t.Errorf("被拒绝的请求也要写审计: %+v", recs[len(recs)-1])
	}
	if r := e.do(t, "/v1/answers", tok, "", qStream); r.status != 429 || r.body["type"] != "quota_exceeded" {
		t.Errorf("流式问答同样受配额限制，得到 %d %s", r.status, r.raw)
	}
	if r := e.do(t, "/v1/compat/chat/completions", tok, "", compatQ); r.status != 429 || r.body["type"] != "quota_exceeded" {
		t.Errorf("兼容端点同样受配额限制，得到 %d %s", r.status, r.raw)
	}
	bot := e.authz.token(t, "tina", "tight-bot")
	if r := e.mcp(t, bot, toolCall("ask", `{"question":"授信额度怎么计算？"}`), nil); r.status != 429 || r.body["type"] != "quota_exceeded" {
		t.Errorf("MCP 的 ask 同样受配额限制，得到 %d %s", r.status, r.raw)
	}
	if r := e.mcp(t, bot, toolCall("search_documents", `{"question":"授信"}`), nil); r.status != 200 || strings.Contains(r.raw, "quota_exceeded") {
		t.Errorf("配额用尽后 MCP 的检索工具照常可用，得到 %d %s", r.status, r.raw)
	}
	if r := e.do(t, "/v1/search", tok, "", q); r.status != 200 {
		t.Errorf("配额用尽后仅检索照常可用，得到 %d %s", r.status, r.raw)
	}
	if got := e.used(t, "tight"); got != 4000+admission.EstimateTokens {
		t.Errorf("被拒绝的请求不计入配额，已用 %d", got)
	}
}

const compatQ = `{"messages":[{"role":"user","content":"授信额度怎么计算？"}]}`

func TestQuotaStreamSettlesOnFinalAndChargesCancelled(t *testing.T) {
	e := newEnv(t)
	tok := e.authz.token(t, "tina", "console")
	evs := goodEvents()
	evs[3].GetFinal().Usage = &answerv1.TokenUsage{PromptTokens: 600, CompletionTokens: 400}
	e.orchestrator.set(evs, nil, false)
	if r := e.do(t, "/v1/answers", tok, "", qStream); r.status != 200 || e.used(t, "tight") != 1000 {
		t.Fatalf("流式问答应按 final 回报的用量结算，得到 %d，已用 %d", r.status, e.used(t, "tight"))
	}
	if recs := e.audit.snapshot(); recs[len(recs)-1].Stream != "end" || recs[len(recs)-1].PromptTokens != 600 {
		t.Errorf("结束审计应带用量: %+v", recs[len(recs)-1])
	}

	// 客户端中途断开：下游被取消、拿不到用量，已产生的用量照常计入（按预占量）
	e.orchestrator.set([]*answerv1.StreamAnswerResponse{metaEvent()}, nil, true)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.srv.URL+"/v1/answers", strings.NewReader(qStream))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 512)
	_, _ = resp.Body.Read(buf)
	cancel()
	_ = resp.Body.Close()
	want := 1000 + int64(admission.EstimateTokens)
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if e.used(t, "tight") == want {
			return
		}
	}
	t.Fatalf("客户端断开后应按预占量计入，已用 %d，期望 %d", e.used(t, "tight"), want)
}

func TestTenantConcurrencyLimit(t *testing.T) {
	e := newEnv(t)
	tok := e.authz.token(t, "tina", "console")
	ctx := context.Background()
	// 租户 tight 的并发上限为 2：占满后在线请求一律 429 rate_limited
	var held []func()
	for range 2 {
		release, ok := e.server.Slots.Acquire(ctx, "tenant", "tight", 2)
		if !ok {
			t.Fatal("应能占到名额")
		}
		held = append(held, release)
	}
	r := e.do(t, "/v1/search", tok, "", q)
	if r.status != 429 || r.body["type"] != "rate_limited" || r.header.Get("Retry-After") == "" {
		t.Fatalf("租户并发超限应 429 rate_limited 并带 Retry-After，得到 %d %s", r.status, r.raw)
	}
	if r := e.do(t, "/v1/answers", tok, "", q); r.status != 429 || e.used(t, "tight") != 0 {
		t.Fatalf("并发超限的问答不预占配额，得到 %d，已用 %d", r.status, e.used(t, "tight"))
	}
	// 其他租户不受影响
	if r := e.do(t, "/v1/search", e.authz.token(t, "alice", "console"), "", q); r.status != 200 {
		t.Fatalf("单租户超限只影响自己，得到 %d", r.status)
	}
	// 归还一个名额后可用；每个请求结束后归还自己的名额，连续请求不会累积
	held[0]()
	for i := range 3 {
		if r := e.do(t, "/v1/search", tok, "", q); r.status != 200 {
			t.Fatalf("第 %d 次请求应成功（请求结束应归还名额），得到 %d", i+1, r.status)
		}
	}
	held[1]()
}

func TestBatchOutsideOnlineConcurrencyAndQuota(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	// credit 未配置 quota：取默认值。占满并发、用尽配额后，在线请求被拒，批量提交照常
	for range tenancy.DefaultMaxConcurrency {
		if _, ok := e.server.Slots.Acquire(ctx, "tenant", "credit", tenancy.DefaultMaxConcurrency); !ok {
			t.Fatal("应能占到名额")
		}
	}
	if r := e.do(t, "/v1/search", e.authz.token(t, "alice", "console"), "", q); r.status != 429 {
		t.Fatalf("租户并发占满后在线请求应 429，得到 %d", r.status)
	}
	if code, out := e.submit(t, "alice", "", twoLines, map[string]string{"Idempotency-Key": "k-q"}); code != 200 {
		t.Fatalf("批量不进在线并发，提交应成功，得到 %d %v", code, out)
	}
}

func TestUserStreamLimit(t *testing.T) {
	e := newEnv(t)
	tok := e.authz.token(t, "rita", "console")
	for range 3 {
		if _, ok := e.server.Slots.Acquire(context.Background(), "user-streams", "rita", 3); !ok {
			t.Fatal("应能占到名额")
		}
	}
	e.orchestrator.set(goodEvents(), nil, false)
	if r := e.do(t, "/v1/answers", tok, "", qStream); r.status != 429 || r.body["type"] != "rate_limited" {
		t.Fatalf("每用户并发流超过 3 路应 429，得到 %d %s", r.status, r.raw)
	}
	if r := e.do(t, "/v1/compat/chat/completions", tok, "", `{"stream":true,"messages":[{"role":"user","content":"x"}]}`); r.status != 429 {
		t.Fatalf("兼容端点的流同样计入，得到 %d %s", r.status, r.raw)
	}
	if r := e.do(t, "/v1/answers", tok, "", q); r.status != 200 {
		t.Fatalf("非流式问答不受流数限制，得到 %d %s", r.status, r.raw)
	}
	// 别的用户不受影响
	if r := e.do(t, "/v1/answers", e.authz.token(t, "alice", "console"), "", qStream); r.status != 200 {
		t.Fatalf("其他用户的流不受影响，得到 %d", r.status)
	}
}

func TestOverloadRejectsLowPriorityFirst(t *testing.T) {
	e := newEnv(t)
	e.server.Overload = admission.NewAdaptive(10, time.Now)
	for range 8 {
		if _, ok := e.server.Overload.Admit(admission.PriorityAnswer); !ok {
			t.Fatal("应放行")
		}
	}
	tok := e.authz.token(t, "rita", "console")
	// 在途 8 个、上限 10：批量（60%）与兼容端点 / MCP（80%）先被拒，仅检索（90%）与同步问答照常
	if s, raw := e.send(t, http.MethodPost, "/v1/batches/bat_x/cancel", e.authz.token(t, "alice", "console"), "", nil, nil); s.StatusCode != 503 ||
		s.Header.Get("Retry-After") == "" {
		t.Fatalf("批量应先被拒并带 Retry-After，得到 %d %s", s.StatusCode, raw)
	}
	if r := e.do(t, "/v1/compat/chat/completions", tok, "", compatQ); r.status != 503 || r.body["type"] != "service_unavailable" {
		t.Fatalf("兼容端点应被拒，得到 %d %s", r.status, r.raw)
	}
	if r := e.mcp(t, e.authz.token(t, "rita", "risk-bot"), toolCall("search_documents", `{"question":"x"}`), nil); r.status != 503 {
		t.Fatalf("MCP 应被拒，得到 %d %s", r.status, r.raw)
	}
	if r := e.do(t, "/v1/search", tok, "", q); r.status != 200 {
		t.Fatalf("仅检索应放行，得到 %d %s", r.status, r.raw)
	}
	if r := e.do(t, "/v1/answers", tok, "", q); r.status != 200 {
		t.Fatalf("同步问答应放行，得到 %d %s", r.status, r.raw)
	}
	// 再多一个在途：仅检索也被拒，同步问答仍放行
	if _, ok := e.server.Overload.Admit(admission.PriorityAnswer); !ok {
		t.Fatal("应放行")
	}
	if r := e.do(t, "/v1/search", tok, "", q); r.status != 503 {
		t.Fatalf("仅检索应被拒，得到 %d", r.status)
	}
	if r := e.do(t, "/v1/answers", tok, "", q); r.status != 200 {
		t.Fatalf("同步问答优先级最高，应放行，得到 %d %s", r.status, r.raw)
	}
}

func TestResourceExhaustedTurnsInto503(t *testing.T) {
	e := newEnv(t)
	e.setAnswer(nil, status.Error(codes.ResourceExhausted, "orchestrator 满载"))
	r := e.do(t, "/v1/answers", e.authz.token(t, "tina", "console"), "", q)
	if r.status != 503 || r.body["type"] != "service_unavailable" || r.header.Get("Retry-After") == "" || strings.Contains(r.raw, "orchestrator") {
		t.Fatalf("下游 RESOURCE_EXHAUSTED 应立即转 503 并带 Retry-After，得到 %d %s", r.status, r.raw)
	}
	if got := e.server.Overload.Limit(); got >= 1000 {
		t.Errorf("下游满载应立即收紧全局并发，上限仍为 %d", got)
	}
	if got := e.used(t, "tight"); got != 0 {
		t.Errorf("下游在生成前拒绝，预占应退回，已用 %d", got)
	}
}
