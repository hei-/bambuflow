package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	batchv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/batch/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/tenancy"
	"github.com/hei-/bambuflow/services/gateway/internal/issuer"
)

// ---- 假的 qa/batch：用 gateway 发布的 JWKS 验签 Principal，只对提交人 alice 返回批次 ----

type fakeBatches struct {
	batchv1.UnimplementedBatchServiceServer
	mu      sync.Mutex
	created *batchv1.CreateBatchRequest
	sub     string
	// ListResults 第二页返回的错误；为空时第二页正常返回
	page2Err error
}

func (f *fakeBatches) CreateBatch(ctx context.Context, req *batchv1.CreateBatchRequest) (*batchv1.CreateBatchResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.GetIdempotencyKey() == "used-with-other-body" {
		return nil, status.Error(codes.AlreadyExists, "幂等键冲突")
	}
	f.created, f.sub = req, grpcmw.PrincipalFrom(ctx).GetSub()
	return &batchv1.CreateBatchResponse{Batch: &batchv1.Batch{
		BatchId: req.GetBatchId(), Status: batchv1.BatchStatus_BATCH_STATUS_QUEUED, Mode: req.GetMode(),
		Counts: &batchv1.BatchCounts{Total: int64(len(req.GetItems()))}, CreatedAt: timestamppb.Now(),
	}}, nil
}

func (f *fakeBatches) last() (*batchv1.CreateBatchRequest, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.created, f.sub
}

func (f *fakeBatches) reset(page2Err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created, f.sub, f.page2Err = nil, "", page2Err
}

func owner(ctx context.Context) error {
	if grpcmw.PrincipalFrom(ctx).GetSub() != "alice" {
		return status.Error(codes.NotFound, "批次不存在")
	}
	return nil
}

func (f *fakeBatches) GetBatch(ctx context.Context, req *batchv1.GetBatchRequest) (*batchv1.GetBatchResponse, error) {
	if err := owner(ctx); err != nil {
		return nil, err
	}
	return &batchv1.GetBatchResponse{Batch: &batchv1.Batch{
		BatchId: req.GetBatchId(), Status: batchv1.BatchStatus_BATCH_STATUS_COMPLETED, Mode: batchv1.BatchMode_BATCH_MODE_ANSWER,
		Counts:    &batchv1.BatchCounts{Total: 2, Succeeded: 1, Failed: 1},
		CreatedAt: timestamppb.New(time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)), CompletedAt: timestamppb.New(time.Date(2026, 10, 6, 8, 5, 0, 0, time.UTC)),
		ExpiresAt: timestamppb.New(time.Date(2026, 10, 13, 8, 5, 0, 0, time.UTC)),
	}}, nil
}

func (f *fakeBatches) CancelBatch(ctx context.Context, req *batchv1.CancelBatchRequest) (*batchv1.CancelBatchResponse, error) {
	if err := owner(ctx); err != nil {
		return nil, err
	}
	return &batchv1.CancelBatchResponse{Batch: &batchv1.Batch{BatchId: req.GetBatchId(), Status: batchv1.BatchStatus_BATCH_STATUS_CANCELLED,
		Mode: batchv1.BatchMode_BATCH_MODE_ANSWER, Counts: &batchv1.BatchCounts{Total: 2, Cancelled: 2}, CreatedAt: timestamppb.Now()}}, nil
}

func (f *fakeBatches) ListResults(ctx context.Context, req *batchv1.ListResultsRequest) (*batchv1.ListResultsResponse, error) {
	if err := owner(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.GetPageToken() == "" {
		return &batchv1.ListResultsResponse{NextPageToken: "p2", Results: []*batchv1.BatchResult{{
			CustomId: "q1", Outcome: &batchv1.BatchResult_Answer{Answer: &answerv1.AnswerResponse{
				AnswerId: "ans_batch1", TraceId: "t-1", Status: answerv1.Status_STATUS_ANSWERED, Answer: "额度上限为 50 万元[S1]。",
				Audit: &answerv1.AuditBlocks{CandidateChunkIds: []string{"c1", "c2"}, ContextChunkIds: []string{"c1"}, ShownChunkIds: []string{"c1"}},
			}},
		}}}, nil
	}
	if f.page2Err != nil {
		return nil, f.page2Err
	}
	return &batchv1.ListResultsResponse{Results: []*batchv1.BatchResult{{
		CustomId: "q2", Outcome: &batchv1.BatchResult_Error{Error: &batchv1.ItemError{Type: "forbidden", Title: "引用的文档已无权访问"}},
	}}}, nil
}

// ---- 请求辅助 ----

func batchBody(t *testing.T, mode, file string) (string, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if mode != "" {
		if err := mw.WriteField("mode", mode); err != nil {
			t.Fatal(err)
		}
	}
	fw, err := mw.CreateFormFile("input_file", "batch.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fw.Write([]byte(file))
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	return mw.FormDataContentType(), &buf
}

// sent 是一次请求的结果（响应体已读完并关闭）。
type sent struct {
	StatusCode int
	Header     http.Header
}

func (e *env) send(t *testing.T, method, path, token, contentType string, body io.Reader, header map[string]string) (sent, string) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), method, e.srv.URL+path, body)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(tenancy.Header, "credit")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	r, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Body.Close() }()
	b, _ := io.ReadAll(r.Body)
	return sent{r.StatusCode, r.Header}, string(b)
}

func (e *env) submit(t *testing.T, user, mode, file string, header map[string]string) (int, map[string]any) {
	t.Helper()
	ct, body := batchBody(t, mode, file)
	r, raw := e.send(t, http.MethodPost, "/v1/batches", e.authz.token(t, user, "console"), ct, body, header)
	var out map[string]any
	_ = json.Unmarshal([]byte(raw), &out)
	return r.StatusCode, out
}

const twoLines = `{"custom_id":"q1","question":"个人消费贷款的额度上限是多少？"}
{"custom_id":"q2","question":"授信审批流程？","filters":{"source_ids":["src_wikidemo"]}}
`

func TestCreateBatch(t *testing.T) {
	e := newEnv(t)
	// 用户的在线问答额度用完了也能提交批次：批量不进在线配额（gateway 详设 5.1）
	e.limits.deny["user-answers:alice"] = true
	code, out := e.submit(t, "alice", "", twoLines, map[string]string{"Idempotency-Key": "k-1"})
	if code != 200 {
		t.Fatalf("提交应成功，得到 %d %v", code, out)
	}
	id, _ := out["batch_id"].(string)
	if !strings.HasPrefix(id, "bat_") || out["status"] != "queued" || out["mode"] != "answer" {
		t.Fatalf("批次不对: %v", out)
	}
	if out["counts"].(map[string]any)["total"] != float64(2) {
		t.Fatalf("total 应为 2: %v", out)
	}
	req, sub := e.batches.last()
	if sub != "alice" || req.GetBatchId() != id || req.GetIdempotencyKey() != "k-1" || req.GetRequestDigest() == "" {
		t.Fatalf("转给 qa/batch 的请求不对: sub=%s %v", sub, req)
	}
	if len(req.GetItems()) != 2 || req.GetItems()[1].GetFilters().GetSourceIds()[0] != "src_wikidemo" {
		t.Fatalf("条目不对: %v", req.GetItems())
	}
	// 委托绑定提交人、租户、客户端与这个批次，gateway 自己能验
	d, err := e.server.Issuer.VerifyDelegation(req.GetDelegation(), id)
	if err != nil || d.Username != "alice" || d.Tenant != "credit" || d.Actor != "console" {
		t.Fatalf("委托不对: %+v %v", d, err)
	}
	recs := e.audit.snapshot()
	if last := recs[len(recs)-1]; last.Endpoint != "batches" || last.BatchID != id || last.Status != 200 {
		t.Fatalf("入口审计不对: %+v", last)
	}
}

func TestCreateBatchDigestFollowsContent(t *testing.T) {
	e := newEnv(t)
	digest := func(file string) string {
		if code, out := e.submit(t, "alice", "answer", file, nil); code != 200 {
			t.Fatalf("提交应成功，得到 %d %v", code, out)
		}
		req, _ := e.batches.last()
		return req.GetRequestDigest()
	}
	a, b := digest(twoLines), digest(twoLines)
	c := digest(strings.Replace(twoLines, "q2", "q3", 1))
	if a != b || a == c {
		t.Fatalf("同样内容摘要应相同、不同内容应不同: %s %s %s", a, b, c)
	}
}

func TestCreateBatchRejections(t *testing.T) {
	e := newEnv(t)
	var many strings.Builder
	for i := 0; i <= maxBatchItems; i++ {
		fmt.Fprintf(&many, `{"custom_id":"q%d","question":"问题"}`+"\n", i)
	}
	cases := []struct {
		name, user, mode, file string
		header                 map[string]string
		status                 int
		typ                    string
	}{
		{"空文件", "alice", "", "\n\n", nil, 400, "invalid_request"},
		{"custom_id 重复", "alice", "", `{"custom_id":"q1","question":"a"}` + "\n" + `{"custom_id":"q1","question":"b"}`, nil, 400, "invalid_request"},
		{"custom_id 不合法", "alice", "", `{"custom_id":"有空格 的","question":"a"}`, nil, 400, "invalid_request"},
		{"未知字段", "alice", "", `{"custom_id":"q1","question":"a","tenant":"risk"}`, nil, 400, "invalid_request"},
		{"不是 JSON", "alice", "", `custom_id=q1`, nil, 400, "invalid_request"},
		{"过滤条件不合法", "alice", "", `{"custom_id":"q1","question":"a","filters":{"source_ids":["wiki"]}}`, nil, 400, "invalid_request"},
		{"未知模式", "alice", "fast", twoLines, nil, 400, "invalid_request"},
		{"仅检索模式尚未开放", "alice", "evidence_only", twoLines, nil, 400, "invalid_request"},
		{"问题过长", "alice", "", `{"custom_id":"q1","question":"` + strings.Repeat("长", 2001) + `"}`, nil, 413, "payload_too_large"},
		{"超过 1 万条", "alice", "", many.String(), nil, 413, "payload_too_large"},
		{"幂等键过长", "alice", "", twoLines, map[string]string{"Idempotency-Key": strings.Repeat("k", 129)}, 400, "invalid_request"},
		{"幂等键对应不同内容", "alice", "", twoLines, map[string]string{"Idempotency-Key": "used-with-other-body"}, 409, "idempotency_conflict"},
		{"不是该租户的成员", "bob", "", twoLines, nil, 403, "forbidden"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e.batches.reset(nil)
			code, out := e.submit(t, tc.user, tc.mode, tc.file, tc.header)
			if code != tc.status || out["type"] != tc.typ {
				t.Fatalf("应返回 %d %s，得到 %d %v", tc.status, tc.typ, code, out)
			}
			if req, _ := e.batches.last(); tc.status != 409 && req != nil {
				t.Fatal("被拒绝的提交不应转给 qa/batch")
			}
		})
	}
	// 不是 multipart 表单
	r, raw := e.send(t, http.MethodPost, "/v1/batches", e.authz.token(t, "alice", "console"), "application/json", strings.NewReader(twoLines), nil)
	if r.StatusCode != 400 {
		t.Fatalf("非 multipart 应返回 400，得到 %d %s", r.StatusCode, raw)
	}
}

func TestBatchStatusAndCancelOnlyForSubmitter(t *testing.T) {
	e := newEnv(t)
	tok := e.authz.token(t, "alice", "console")
	r, raw := e.send(t, http.MethodGet, "/v1/batches/bat_0a1b", tok, "", nil, nil)
	if r.StatusCode != 200 {
		t.Fatalf("提交人查询应成功，得到 %d %s", r.StatusCode, raw)
	}
	var b map[string]any
	_ = json.Unmarshal([]byte(raw), &b)
	if b["status"] != "completed" || b["completed_at"] != "2026-10-06T08:05:00Z" || b["expires_at"] != "2026-10-13T08:05:00Z" {
		t.Fatalf("状态不对: %s", raw)
	}
	if b["counts"].(map[string]any)["failed"] != float64(1) {
		t.Fatalf("计数不对: %s", raw)
	}
	if r, raw := e.send(t, http.MethodPost, "/v1/batches/bat_0a1b/cancel", tok, "", nil, nil); r.StatusCode != 200 || !strings.Contains(raw, `"cancelled"`) {
		t.Fatalf("提交人取消应成功，得到 %d %s", r.StatusCode, raw)
	}
	// 同租户的其他人与不存在的批次一样是 404
	other := e.authz.token(t, "dual", "console")
	for _, p := range []string{"/v1/batches/bat_0a1b", "/v1/batches/bat_0a1b/results"} {
		if r, raw := e.send(t, http.MethodGet, p, other, "", nil, nil); r.StatusCode != 404 {
			t.Fatalf("%s 非提交人应得到 404，得到 %d %s", p, r.StatusCode, raw)
		}
	}
	if r, _ := e.send(t, http.MethodPost, "/v1/batches/bat_0a1b/cancel", other, "", nil, nil); r.StatusCode != 404 {
		t.Fatalf("非提交人取消应得到 404，得到 %d", r.StatusCode)
	}
	if r, _ := e.send(t, http.MethodGet, "/v1/batches/ans_0a1b", tok, "", nil, nil); r.StatusCode != 404 {
		t.Fatalf("格式不对的批次 ID 应得到 404，得到 %d", r.StatusCode)
	}
	if r, _ := e.send(t, http.MethodGet, "/v1/batches/bat_0a1b", "", "", nil, nil); r.StatusCode != 401 {
		t.Fatalf("没有令牌应得到 401，得到 %d", r.StatusCode)
	}
}

func TestBatchResults(t *testing.T) {
	e := newEnv(t)
	r, raw := e.send(t, http.MethodGet, "/v1/batches/bat_0a1b/results", e.authz.token(t, "alice", "console"), "", nil, nil)
	if r.StatusCode != 200 || r.Header.Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("应返回 200 JSONL，得到 %d %s %s", r.StatusCode, r.Header.Get("Content-Type"), raw)
	}
	sc := bufio.NewScanner(strings.NewReader(raw))
	var lines []map[string]any
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("每行应是 JSON: %q", sc.Text())
		}
		lines = append(lines, m)
	}
	if len(lines) != 2 || lines[0]["custom_id"] != "q1" || lines[1]["custom_id"] != "q2" {
		t.Fatalf("应按顺序返回两行: %s", raw)
	}
	a := lines[0]["answer"].(map[string]any)
	if a["answer_id"] != "ans_batch1" || a["status"] != "answered" || a["audit"] != nil {
		t.Fatalf("第一行应是回答契约（不含审计块）: %v", a)
	}
	if lines[1]["error"].(map[string]any)["type"] != "forbidden" || lines[1]["answer"] != nil {
		t.Fatalf("第二行应是错误: %v", lines[1])
	}
	// 每条回答的三类块进审计，流开始与结束各一条
	var item, open, end bool
	for _, rec := range e.audit.snapshot() {
		switch {
		case rec.Endpoint == "batch-result-item":
			item = rec.AnswerID == "ans_batch1" && rec.BatchID == "bat_0a1b" && rec.Sub == "alice" && len(rec.ShownChunkIDs) == 1 && len(rec.CandidateChunkIDs) == 2
		case rec.Endpoint == "batch-results" && rec.Stream == "open":
			open = true
		case rec.Endpoint == "batch-results" && rec.Stream == "end" && rec.Reason == "":
			end = true
		}
	}
	if !item || !open || !end {
		t.Fatalf("审计不全: item=%v open=%v end=%v %+v", item, open, end, e.audit.snapshot())
	}
}

func TestBatchResultsAuditFailureWithholdsAnswers(t *testing.T) {
	e := newEnv(t)
	e.audit.fail = true
	r, raw := e.send(t, http.MethodGet, "/v1/batches/bat_0a1b/results", e.authz.token(t, "alice", "console"), "", nil, nil)
	if r.StatusCode != 503 || strings.Contains(raw, "ans_batch1") {
		t.Fatalf("审计不可用时不应输出结果，得到 %d %s", r.StatusCode, raw)
	}
}

func TestBatchResultsInterruptedMidway(t *testing.T) {
	e := newEnv(t)
	e.batches.reset(status.Error(codes.Unavailable, "下游不可用"))
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, e.srv.URL+"/v1/batches/bat_0a1b/results", nil)
	req.Header.Set("Authorization", "Bearer "+e.authz.token(t, "alice", "console"))
	req.Header.Set(tenancy.Header, "credit")
	r, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Body.Close() }()
	if _, err := io.ReadAll(r.Body); err == nil {
		t.Fatal("中途出错时连接应被中断，调用方读到错误而不是完整的结果")
	}
	var aborted bool
	for _, rec := range e.audit.snapshot() {
		aborted = aborted || (rec.Endpoint == "batch-results" && rec.Stream == "end" && rec.Reason == "service_unavailable")
	}
	if !aborted {
		t.Fatal("中断应记入审计")
	}
}

// ---- PrincipalService.Exchange ----

// delegation 直接签发一份委托（与 createBatch 签发的相同），不经 HTTP：之后的用例要在同一个 goroutine 里改身份库。
func (e *env) delegation(t *testing.T, user string) (string, string) {
	t.Helper()
	id := newBatchID()
	raw, err := e.server.Issuer.IssueDelegation(issuer.Delegation{
		Username: user, Tenant: "credit", Actor: "console", BatchID: id, Expires: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw, id
}

func TestExchangeIssuesPrincipalWithCurrentAttributes(t *testing.T) {
	e := newEnv(t)
	deleg, id := e.delegation(t, "alice")
	// 提交后用户调了部门：换发出的 Principal 用当前属性
	u := e.identities.users["alice"]
	u.Dept = "风险部"
	e.identities.users["alice"] = u
	ps := &PrincipalServer{S: e.server}
	resp, err := ps.Exchange(context.Background(), &identityv1.ExchangeRequest{Delegation: deleg, BatchId: id})
	if err != nil {
		t.Fatal(err)
	}
	claims := jwt.MapClaims{}
	if _, _, err := jwt.NewParser().ParseUnverified(resp.GetPrincipal(), claims); err != nil {
		t.Fatal(err)
	}
	if claims["sub"] != "alice" || claims["tenant"] != "credit" || claims["dept"] != "风险部" || claims["actor"] != "console" {
		t.Fatalf("Principal 内容不对: %v", claims)
	}
	// 受限级禁止批量导出：即使提交人有受限级权限，批量条目的身份密级也只到机密
	u.Clearance = 4
	e.identities.users["alice"] = u
	resp, err = ps.Exchange(context.Background(), &identityv1.ExchangeRequest{Delegation: deleg, BatchId: id})
	if err != nil {
		t.Fatal(err)
	}
	claims = jwt.MapClaims{}
	if _, _, err := jwt.NewParser().ParseUnverified(resp.GetPrincipal(), claims); err != nil {
		t.Fatal(err)
	}
	if claims["clearance"] != "CLASSIFICATION_CONFIDENTIAL" {
		t.Fatalf("批量身份的密级应封顶为机密: %v", claims["clearance"])
	}
	if time.Until(resp.GetExpiresAt().AsTime()) > 5*time.Minute+time.Second {
		t.Fatal("换发的 Principal 有效期不应超过 5 分钟")
	}
	recs := e.audit.snapshot()
	if last := recs[len(recs)-1]; last.Endpoint != "batch-exchange" || last.Status != 200 || last.BatchID != id || last.JTI == "" {
		t.Fatalf("换发应写审计: %+v", last)
	}
}

func TestExchangeRefusals(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(e *env, deleg, id *string)
		code   codes.Code
	}{
		{"提交人已停用", func(e *env, _, _ *string) {
			u := e.identities.users["alice"]
			u.Active = false
			e.identities.users["alice"] = u
		}, codes.PermissionDenied},
		{"提交人被移出租户", func(e *env, _, _ *string) {
			u := e.identities.users["alice"]
			u.Groups = []string{"risk-dept"}
			e.identities.users["alice"] = u
		}, codes.PermissionDenied},
		{"提交人已从身份库删除", func(e *env, _, _ *string) { delete(e.identities.users, "alice") }, codes.PermissionDenied},
		{"委托不属于该批次", func(_ *env, _, id *string) { *id = "bat_ffff" }, codes.PermissionDenied},
		{"委托被篡改", func(_ *env, deleg, _ *string) { *deleg += "x" }, codes.PermissionDenied},
		{"身份同步不新鲜", func(e *env, _, _ *string) { e.identities.lastSync = time.Now().Add(-time.Hour) }, codes.Unavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			deleg, id := e.delegation(t, "alice")
			tc.mutate(e, &deleg, &id)
			_, err := (&PrincipalServer{S: e.server}).Exchange(context.Background(), &identityv1.ExchangeRequest{Delegation: deleg, BatchId: id})
			if status.Code(err) != tc.code {
				t.Fatalf("应返回 %s，得到 %v", tc.code, err)
			}
			recs := e.audit.snapshot()
			if last := recs[len(recs)-1]; last.Endpoint != "batch-exchange" || last.Status == 200 {
				t.Fatalf("拒绝也要写审计: %+v", last)
			}
		})
	}
}

func TestExchangeFailsClosedWithoutAudit(t *testing.T) {
	e := newEnv(t)
	deleg, id := e.delegation(t, "alice")
	e.audit.fail = true
	resp, err := (&PrincipalServer{S: e.server}).Exchange(context.Background(), &identityv1.ExchangeRequest{Delegation: deleg, BatchId: id})
	if status.Code(err) != codes.Unavailable || resp != nil {
		t.Fatalf("审计不可用时不应签发，得到 %v %v", resp, err)
	}
}
