package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/principal"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/audit"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/roles"
)

// request 发一个任意方法的请求，headers 里可以带 Idempotency-Key 等；token 为空时不带 Authorization。
func (e *env) request(t *testing.T, method, path, token, body string, headers map[string]string) resp {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), method, e.srv.URL+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	r, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Body.Close() }()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r.Body)
	out := resp{status: r.StatusCode, raw: buf.String(), header: r.Header}
	_ = json.Unmarshal(buf.Bytes(), &out.body)
	return out
}

// fakeImpl 记录实现方收到的调用：管理 Principal（按 gateway 发布的 JWKS 验签）、trace_id 与请求。
type fakeImpl struct {
	mu     sync.Mutex
	calls  int
	seen   *identityv1.Principal
	trace  string
	params map[string]string
	body   string
	err    error
	verify *principal.Verifier
}

func (f *fakeImpl) call(ctx context.Context, in *adminInput) (adminOutput, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	raw := strings.TrimPrefix(strings.Join(md.Get("authorization"), ""), "Bearer ")
	p, err := f.verify.Verify(ctx, raw)
	if err != nil {
		return adminOutput{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.seen, f.trace, f.params, f.body = p, strings.Join(md.Get(grpcmw.TraceIDKey), ""), in.params, string(in.body)
	if f.err != nil {
		return adminOutput{}, f.err
	}
	return adminOutput{status: http.StatusCreated, body: map[string]any{"id": "op_1", "n": f.calls}}, nil
}

func newAdminEnv(t *testing.T) (*env, *fakeImpl) {
	e := newEnv(t)
	jwks := httptest.NewServer(http.HandlerFunc(e.server.Issuer.JWKS))
	t.Cleanup(jwks.Close)
	f := &fakeImpl{verify: principal.NewOperatorVerifier(principal.NewJWKS(jwks.URL, jwks.Client()))}
	e.server.adminTable = []adminRoute{
		{name: "admin-things-list", method: http.MethodGet, pattern: "/v1/admin/things", owner: "governance",
			roles: []string{roles.Auditor, roles.PlatformOperator}, call: f.call},
		{name: "admin-things-activate", method: http.MethodPost, pattern: "/v1/admin/things/{thing}/activate", owner: "indexing",
			roles: []string{roles.PlatformOperator}, call: f.call},
	}
	return e, f
}

func TestAdminDispatchAndRBAC(t *testing.T) {
	e, f := newAdminEnv(t)
	tok := func(user, client string) string { return e.authz.token(t, user, client) }
	key := map[string]string{"Idempotency-Key": "k-1"}
	cases := []struct {
		name, method, path, token string
		headers                   map[string]string
		status                    int
		typ                       string
	}{
		{"未登记的路径", http.MethodGet, "/v1/admin/tenants", "", nil, 404, "not_found"},
		{"已登记路径的未登记方法", http.MethodDelete, "/v1/admin/things", tok("olga", "console"), nil, 404, "not_found"},
		{"多一段路径", http.MethodPost, "/v1/admin/things/v1/activate/now", tok("olga", "console"), key, 404, "not_found"},
		{"没有令牌", http.MethodGet, "/v1/admin/things", "", nil, 401, "unauthenticated"},
		{"只有服务身份", http.MethodGet, "/v1/admin/things", tok("service-account-ci", "ci"), nil, 401, "user_identity_required"},
		{"没有平台角色", http.MethodGet, "/v1/admin/things", tok("alice", "console"), nil, 403, "forbidden"},
		{"角色不在这个接口允许的范围", http.MethodPost, "/v1/admin/things/v1/activate", tok("audra", "console"), key, 403, "forbidden"},
		{"经业务系统客户端调用", http.MethodGet, "/v1/admin/things", tok("olga", "biz-credit"), nil, 403, "forbidden"},
		{"写操作没有幂等键", http.MethodPost, "/v1/admin/things/v1/activate", tok("olga", "console"), nil, 400, "invalid_request"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := e.request(t, c.method, c.path, c.token, "", c.headers)
			if r.status != c.status || r.body["type"] != c.typ {
				t.Fatalf("得到 %d %v，期望 %d %s", r.status, r.body["type"], c.status, c.typ)
			}
		})
	}
	if f.calls != 0 {
		t.Fatalf("被拒绝的请求不应到达实现方，实际 %d 次", f.calls)
	}

	r := e.request(t, http.MethodGet, "/v1/admin/things", tok("audra", "console"), "", nil)
	if r.status != 201 || r.body["id"] != "op_1" {
		t.Fatalf("应转发到实现方: %d %s", r.status, r.raw)
	}
	if f.seen.GetSub() != "audra" || f.seen.GetTenant() != "" || len(f.seen.GetRoles()) != 1 || f.seen.GetRoles()[0] != roles.Auditor ||
		f.seen.GetDept() != "审计部" || f.seen.GetActor() != "console" {
		t.Fatalf("管理 Principal 不对: %v", f.seen)
	}
	recs := e.audit.snapshot()
	last := recs[len(recs)-1]
	if last.Endpoint != "admin-things-list" || last.Admin == nil || last.Admin.Owner != "governance" || last.Admin.Request != "" ||
		last.TraceID != f.trace || last.Sub != "audra" || last.Tenant != "" {
		t.Fatalf("读操作的管理审计不对: %+v %+v", last, last.Admin)
	}
	// 被拒绝的请求也有管理审计
	if rec := recs[0]; rec.Status != 404 || rec.Admin != nil {
		t.Errorf("未登记路径应记入口审计、不当作管理操作: %+v", rec)
	}
	if rec := recs[5]; rec.Status != 403 || rec.Admin == nil || rec.Admin.Path != "/v1/admin/things" || rec.Sub != "alice" {
		t.Errorf("没有角色的请求应记管理审计: %+v", rec)
	}
}

func TestAdminWriteIdempotencyAndAudit(t *testing.T) {
	e, f := newAdminEnv(t)
	olga := e.authz.token(t, "olga", "console")
	body := `{"reason":"回滚误发布","signing_secret":"top-secret"}`
	key := map[string]string{"Idempotency-Key": "k-1"}
	first := e.request(t, http.MethodPost, "/v1/admin/things/v42/activate", olga, body, key)
	if first.status != 201 || f.calls != 1 || f.params["thing"] != "v42" || f.body != body {
		t.Fatalf("第一次应执行: %d %s %v", first.status, first.raw, f.params)
	}
	again := e.request(t, http.MethodPost, "/v1/admin/things/v42/activate", olga, body, key)
	if again.status != 201 || again.raw != first.raw || f.calls != 1 {
		t.Fatalf("同键同内容应返回第一次的结果、不再执行: %d %s（实现方 %d 次）", again.status, again.raw, f.calls)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/things/v43/activate", olga, body, key); r.status != 409 || r.body["type"] != "idempotency_conflict" {
		t.Fatalf("同键不同请求应冲突: %d %s", r.status, r.raw)
	}
	// 别人用同一个键互不影响
	oscar := e.authz.token(t, "oscar", "console")
	if r := e.request(t, http.MethodPost, "/v1/admin/things/v42/activate", oscar, body, key); r.status != 201 || f.calls != 2 {
		t.Fatalf("不同用户的同名键应各自执行: %d", r.status)
	}

	var writes []audit.Record
	for _, r := range e.audit.snapshot() {
		if r.Admin != nil && r.Admin.Method == http.MethodPost {
			writes = append(writes, r)
		}
	}
	a := writes[0].Admin
	if strings.Contains(a.Request, "top-secret") || !strings.Contains(a.Request, "回滚误发布") || a.RequestSHA == "" ||
		!strings.Contains(a.Result, "op_1") || a.IdempotencyKey != "k-1" || a.Owner != "indexing" || a.Replayed {
		t.Fatalf("写操作的管理审计不对: %+v", a)
	}
	if !writes[1].Admin.Replayed || writes[1].Status != 201 {
		t.Fatalf("重复请求应记为重放: %+v", writes[1].Admin)
	}
}

func TestAdminImplementerErrors(t *testing.T) {
	e, f := newAdminEnv(t)
	olga := e.authz.token(t, "olga", "console")
	key := map[string]string{"Idempotency-Key": "k-err"}
	cases := []struct {
		err    error
		status int
		typ    string
	}{
		{grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "x"), 403, "self_approval_forbidden"},
		{grpcmw.Reason(codes.FailedPrecondition, "activation_refused", "x"), 409, "activation_refused"},
		{grpcmw.Reason(codes.FailedPrecondition, "not_registered_reason", "内部细节"), 503, "service_unavailable"},
		{status.Error(codes.NotFound, "x"), 404, "not_found"},
		{status.Error(codes.Unavailable, "x"), 503, "service_unavailable"},
	}
	for i, c := range cases {
		f.err = c.err
		r := e.request(t, http.MethodPost, "/v1/admin/things/v1/activate", olga, `{}`, key)
		if r.status != c.status || r.body["type"] != c.typ || strings.Contains(r.raw, "内部细节") {
			t.Fatalf("第 %d 种错误: 得到 %d %s", i, r.status, r.raw)
		}
	}
	// 失败不占用幂等键：同一个键重试会再次执行
	f.err = nil
	if r := e.request(t, http.MethodPost, "/v1/admin/things/v1/activate", olga, `{}`, key); r.status != 201 || f.calls != len(cases)+1 {
		t.Fatalf("失败后同键重试应执行: %d（实现方 %d 次）", r.status, f.calls)
	}
}
