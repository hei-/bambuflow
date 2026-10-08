package api

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	answerv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	batchv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/batch/v1"
	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
	"github.com/bambu-ai/bambuflow/libs/platform-go/principal"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/admission"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/audit"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/authn"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/idempotency"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/identity"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/issuer"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/ratelimit"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/roles"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/tenancy"
)

const platformIssuer = "http://keycloak:8080/realms/bambuflow"

// ---- 假的平台授权服务：签发平台令牌并提供 JWKS ----

type fakeAuthz struct {
	key *ecdsa.PrivateKey
	srv *httptest.Server
}

func newFakeAuthz(t *testing.T) *fakeAuthz {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	a := &fakeAuthz{key: k}
	a.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		b, _ := k.PublicKey.Bytes()
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "EC", "crv": "P-256", "kid": "platform-k1",
			"x": base64.RawURLEncoding.EncodeToString(b[1:33]), "y": base64.RawURLEncoding.EncodeToString(b[33:]),
		}}})
	}))
	t.Cleanup(a.srv.Close)
	return a
}

func (a *fakeAuthz) token(t *testing.T, username, client string) string {
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss": platformIssuer, "aud": authn.Audience, "azp": client, "preferred_username": username,
		"iat": now.Unix(), "exp": now.Add(15 * time.Minute).Unix(), "auth_time": now.Add(-time.Minute).Unix(),
	})
	tok.Header["kid"] = "platform-k1"
	s, err := tok.SignedString(a.key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// ---- 内存身份库 ----

type fakeIdentities struct {
	users    map[string]identity.User
	lastSync time.Time
	err      error
}

func (f *fakeIdentities) Lookup(_ context.Context, u string) (identity.User, bool, error) {
	if f.err != nil {
		return identity.User{}, false, f.err
	}
	x, ok := f.users[u]
	return x, ok, nil
}

func (f *fakeIdentities) LastSync(context.Context) (time.Time, error) { return f.lastSync, f.err }

// ---- 审计 ----

type fakeAudit struct {
	mu      sync.Mutex
	records []audit.Record
	fail    bool
}

func (f *fakeAudit) Write(r audit.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return errors.New("磁盘已满")
	}
	f.records = append(f.records, r)
	return nil
}

func (f *fakeAudit) snapshot() []audit.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]audit.Record(nil), f.records...)
}

// ---- 假的 retrieval：用 gateway 发布的 JWKS 验签 Principal，并把看到的身份写回响应 ----

type echoRetrieval struct {
	retrievalv1.UnimplementedRetrievalServiceServer
	mu sync.Mutex
	// 最近一次调用看到的调用目的与 Principal 的 actor
	purpose, actor string
}

func (e *echoRetrieval) record(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.purpose, e.actor = strings.Join(md.Get("x-call-purpose"), ","), grpcmw.PrincipalFrom(ctx).GetActor()
}

func (e *echoRetrieval) seen() (string, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.purpose, e.actor
}

func (e *echoRetrieval) Search(ctx context.Context, req *retrievalv1.SearchRequest) (*retrievalv1.SearchResponse, error) {
	e.record(ctx)
	p := grpcmw.PrincipalFrom(ctx)
	seen := fmt.Sprintf("%s@%s|%s|groups=%d|ref=%s|actor=%s", p.GetSub(), p.GetTenant(), p.GetClearance(),
		len(p.GetGroups()), p.GetGroupsRef(), p.GetActor())
	return &retrievalv1.SearchResponse{
		IndexVersion: seen,
		Hits: []*retrievalv1.Hit{{
			ChunkId: "wiki:doc-1#a1", DocId: "doc-1", DocVersion: 3, Title: "授信管理办法", Text: req.GetQueries()[0].GetText(),
			Span: &commonv1.Span{CharStart: 0, CharEnd: 10}, Score: 0.9,
			SourceType: commonv1.SourceType_SOURCE_TYPE_WIKI, Classification: commonv1.Classification_CLASSIFICATION_INTERNAL,
			GraphPath: []*retrievalv1.GraphEdge{{
				FromEntity: "清算核心服务", FromType: commonv1.EntityType_ENTITY_TYPE_SYSTEM,
				RelationType: commonv1.RelationType_RELATION_TYPE_BELONGS_TO, ToEntity: "支付清算部", ToType: commonv1.EntityType_ENTITY_TYPE_ORG,
			}},
		}},
	}, nil
}

type fakeOrchestrator struct {
	answerv1.UnimplementedAnswerServiceServer
	// 流式调用依次发送的事件；err 非空时发完事件后以它结束，block 为真时发完事件后等到调用被取消
	events      []*answerv1.StreamAnswerResponse
	err         error
	block       bool
	mu          sync.Mutex
	seenMD      metadata.MD
	hasDeadline bool
	// 非空时一元调用返回它
	answer *answerv1.AnswerResponse
	// 非空时一元调用返回这个错误
	answerErr error
	// 回放与反馈收到的请求（replay_test.go）
	replayReq   *answerv1.GetAnswerRequest
	feedbackReq *answerv1.CreateFeedbackRequest
}

func (f *fakeOrchestrator) set(events []*answerv1.StreamAnswerResponse, err error, block bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events, f.err, f.block = events, err, block
}

func (f *fakeOrchestrator) StreamAnswer(req *answerv1.StreamAnswerRequest, stream grpc.ServerStreamingServer[answerv1.StreamAnswerResponse]) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	f.mu.Lock()
	f.seenMD = md
	_, f.hasDeadline = stream.Context().Deadline()
	events, err, block := f.events, f.err, f.block
	f.mu.Unlock()
	if req.GetRequest().GetQuestion() == "" {
		return status.Error(codes.InvalidArgument, "问题为空")
	}
	for _, ev := range events {
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	if block {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return err
}

// Answer 记下调用元数据；有预设回答时返回它，否则把请求里的会话 ID 原样带回（确认 gateway 透传了它）。
func (f *fakeOrchestrator) Answer(ctx context.Context, req *answerv1.AnswerRequest) (*answerv1.AnswerResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.mu.Lock()
	f.seenMD = md
	resp, answerErr := f.answer, f.answerErr
	f.mu.Unlock()
	if answerErr != nil {
		return nil, answerErr
	}
	if resp != nil {
		return resp, nil
	}
	return &answerv1.AnswerResponse{
		Status: answerv1.Status_STATUS_INSUFFICIENT_EVIDENCE, AnswerId: "ignored-by-gateway", ConversationId: req.GetConversationId(),
		Sentences: []*answerv1.Sentence{{Text: "x", ClaimType: answerv1.ClaimType_CLAIM_TYPE_QUOTE}},
	}, nil
}

type env struct {
	srv          *httptest.Server
	authz        *fakeAuthz
	identities   *fakeIdentities
	audit        *fakeAudit
	orchestrator *fakeOrchestrator
	retrieval    *echoRetrieval
	limits       *fakeLimits
	batches      *fakeBatches
	server       *Server
	redis        *miniredis.Miniredis
}

func groups(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%03d", prefix, i)
	}
	return out
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name, "tenant.yaml"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("credit", "tenant: credit\nrollout: general\nmember_groups: [credit-l2, project-000]\nbound_clients: [biz-credit]\n"+
		"auditor_groups: [credit-audit]\n"+
		"mcp_agents: [{client_id: credit-bot, purpose: 信贷问答助手, owner: 信贷部, rps: 7}]\n")
	// 配额只够两次预占（每次 8800），并发上限 2：专门用来测租户这一层，不影响其他用例
	write("tight", "tenant: tight\nrollout: general\nmember_groups: [tight-g]\n"+
		"mcp_agents: [{client_id: tight-bot, purpose: 配额测试, owner: 平台组}]\n"+
		"quota: {max_concurrency: 2, daily_tokens: 20000}\n")
	write("risk", "tenant: risk\nrollout: pilot\nmember_groups: [risk-dept]\npilot_groups: [risk-pilot]\n"+
		"mcp_agents: [{client_id: risk-bot, purpose: 风险问答助手, owner: 风险管理部}]\nmcp_tools: [ask, search_documents]\n")
	tenants, err := tenancy.Load(dir, []string{"console"})
	if err != nil {
		t.Fatal(err)
	}

	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	iss, err := issuer.New(k)
	if err != nil {
		t.Fatal(err)
	}
	jwks := httptest.NewServer(http.HandlerFunc(iss.JWKS))
	t.Cleanup(jwks.Close)

	// 下游按 gateway 发布的 JWKS 验签，与生产路径一致
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(grpc.UnaryInterceptor(grpcmw.UnaryServer(
		principal.NewVerifier(principal.NewJWKS(jwks.URL, jwks.Client())), logging.New(&bytes.Buffer{}, "t", slog.LevelInfo))))
	retrieval := &echoRetrieval{}
	retrievalv1.RegisterRetrievalServiceServer(gs, retrieval)
	orch := &fakeOrchestrator{}
	answerv1.RegisterAnswerServiceServer(gs, orch)
	batches := &fakeBatches{}
	batchv1.RegisterBatchServiceServer(gs, batches)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	quiet := logging.New(&bytes.Buffer{}, "gateway", slog.LevelInfo)

	e := &env{redis: mr, authz: newFakeAuthz(t), audit: &fakeAudit{}, orchestrator: orch, retrieval: retrieval, batches: batches, limits: &fakeLimits{deny: map[string]bool{}}, identities: &fakeIdentities{
		lastSync: time.Now().Add(-time.Minute),
		users: map[string]identity.User{
			"alice":  {Username: "alice", Dept: "信贷部", Clearance: 2, Active: true, Groups: []string{"credit-l2"}},
			"audrey": {Username: "audrey", Clearance: 2, Active: true, Groups: []string{"credit-l2", "credit-audit"}},
			"bob":    {Username: "bob", Clearance: 3, Active: true, Groups: []string{"risk-dept"}},
			"rita":   {Username: "rita", Clearance: 3, Active: true, Groups: []string{"risk-dept", "risk-pilot"}},
			"dual":   {Username: "dual", Clearance: 2, Active: true, Groups: []string{"credit-l2", "risk-dept"}},
			"eve":    {Username: "eve", Clearance: 1, Active: true, Groups: groups("project", 250)},
			"gone":   {Username: "gone", Clearance: 2, Active: false, Groups: []string{"credit-l2"}},
			"tina":   {Username: "tina", Clearance: 2, Active: true, Groups: []string{"tight-g"}},
			// 管理接口的测试用户：平台运维两人、审计岗、数据属主（admin_test.go）
			"olga":  {Username: "olga", Dept: "平台部", Clearance: 2, Active: true, Groups: []string{"platform-ops"}},
			"oscar": {Username: "oscar", Dept: "平台部", Clearance: 2, Active: true, Groups: []string{"platform-ops"}},
			"audra": {Username: "audra", Dept: "审计部", Clearance: 2, Active: true, Groups: []string{"platform-audit"}},
			"dana":  {Username: "dana", Dept: "信贷部", Clearance: 3, Active: true, Groups: []string{"credit-l2", "data-owners"}},
			"paul":  {Username: "paul", Dept: "平台部", Clearance: 2, Active: true, Groups: []string{"platform-admins"}},
			"ivan":  {Username: "ivan", Dept: "信息安全部", Clearance: 2, Active: true, Groups: []string{"infosec"}},
		},
	}}
	s := &Server{
		Verifier:   authn.NewVerifier(principal.NewJWKS(e.authz.srv.URL, e.authz.srv.Client()), platformIssuer, "service-account-"),
		Identities: e.identities, Tenants: tenants, Issuer: iss,
		Retrieval: retrievalv1.NewRetrievalServiceClient(conn), Orchestrator: answerv1.NewAnswerServiceClient(conn),
		Batches: batchv1.NewBatchServiceClient(conn),
		Audit:   e.audit, Limits: e.limits,
		ClientRule: ratelimit.Rule{Name: "client", Rate: 20, Burst: 20}, UserAnswerRule: ratelimit.Rule{Name: "user-answers"},
		Slots:    &admission.Slots{Redis: rdb, Instances: 1, Lease: admission.DefaultLease, Log: quiet},
		Quota:    &admission.Quota{Redis: rdb, Instances: 1, Location: time.UTC, Log: quiet, Now: time.Now},
		Overload: admission.NewAdaptive(1000, time.Now), UserStreams: 3,
		PublicURL: "https://rag.example", AuthorizationServer: platformIssuer,
		AdminRoles: roles.Map{
			roles.PlatformOperator: {"platform-ops"}, roles.Auditor: {"platform-audit"}, roles.DataOwner: {"data-owners"},
			roles.InfoSec: {"infosec"}, roles.PlatformAdmin: {"platform-admins"},
		},
		Idempotency: &idempotency.Store{Redis: rdb},
		Log:         logging.New(&bytes.Buffer{}, "gateway", slog.LevelInfo), Now: time.Now,
	}
	e.server = s
	e.srv = httptest.NewServer(s.Handler())
	t.Cleanup(e.srv.Close)
	return e
}

type resp struct {
	status int
	body   map[string]any
	raw    string
	header http.Header
}

func (e *env) do(t *testing.T, path, token, tenant, body string) resp {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, e.srv.URL+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if tenant != "" {
		req.Header.Set(tenancy.Header, tenant)
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

const (
	q       = `{"question":"授信额度怎么计算？"}`
	qStream = `{"question":"授信额度怎么计算？","stream":true}`
)

func TestRejections(t *testing.T) {
	e := newEnv(t)
	tok := func(user, client string) string { return e.authz.token(t, user, client) }
	cases := []struct {
		name, path, token, tenant, body string
		status                          int
		typ                             string
	}{
		{"没有令牌", "/v1/search", "", "", q, 401, "unauthenticated"},
		{"伪造的令牌", "/v1/search", newFakeAuthz(t).token(t, "alice", "console"), "", q, 401, "unauthenticated"},
		{"只有服务身份", "/v1/search", tok("service-account-biz-credit", "biz-credit"), "", q, 401, "user_identity_required"},
		{"身份库里没有该用户", "/v1/search", tok("mallory", "console"), "", q, 403, "forbidden"},
		{"用户已停用", "/v1/search", tok("gone", "console"), "", q, 403, "forbidden"},
		{"请求头指定非成员租户", "/v1/search", tok("alice", "console"), "risk", q, 403, "forbidden"},
		{"请求头指定不存在的租户", "/v1/search", tok("alice", "console"), "nope", q, 403, "forbidden"},
		{"绑定客户端与请求头不一致", "/v1/search", tok("dual", "biz-credit"), "risk", q, 403, "forbidden"},
		{"未登记的客户端", "/v1/search", tok("alice", "unknown-app"), "", q, 403, "forbidden"},
		{"属于多个租户但没指定", "/v1/search", tok("dual", "console"), "", q, 400, "invalid_request"},
		{"试点租户的非试点用户问答", "/v1/answers", tok("bob", "console"), "", q, 403, "answers_not_enabled"},
		{"试点租户的非试点用户流式问答", "/v1/answers", tok("bob", "console"), "", qStream, 403, "answers_not_enabled"},
		{"会话 ID 格式不对", "/v1/answers", tok("rita", "console"), "", `{"question":"x","conversation_id":"abc"}`, 400, "invalid_request"},
		{"stream 不是布尔值", "/v1/answers", tok("rita", "console"), "", `{"question":"x","stream":"yes"}`, 400, "invalid_request"},
		{"未知字段", "/v1/search", tok("alice", "console"), "", `{"question":"x","tenant":"risk"}`, 400, "invalid_request"},
		{"问题为空", "/v1/search", tok("alice", "console"), "", `{"question":"  "}`, 400, "invalid_request"},
		{"问题超过 2000 字", "/v1/search", tok("alice", "console"), "", `{"question":"` + strings.Repeat("长", 2001) + `"}`, 413, "payload_too_large"},
		{"请求体超过 64 KB", "/v1/search", tok("alice", "console"), "", `{"question":"` + strings.Repeat("a", 70<<10) + `"}`, 413, "payload_too_large"},
		{"top_k 越界", "/v1/search", tok("alice", "console"), "", `{"question":"x","top_k":51}`, 400, "invalid_request"},
		{"未登记的路径", "/v1/admin/x", tok("alice", "console"), "", q, 404, "not_found"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := e.do(t, c.path, c.token, c.tenant, c.body)
			if r.status != c.status || r.body["type"] != c.typ {
				t.Fatalf("得到 %d %v，期望 %d %s", r.status, r.body["type"], c.status, c.typ)
			}
			if r.body["trace_id"] == "" || strings.Contains(r.raw, "retrieval") {
				t.Errorf("错误响应应带 trace_id、不含内部服务名: %s", r.raw)
			}
		})
	}
}

func TestStaleIdentitySyncFailsClosed(t *testing.T) {
	e := newEnv(t)
	e.identities.lastSync = time.Now().Add(-16 * time.Minute)
	if r := e.do(t, "/v1/search", e.authz.token(t, "alice", "console"), "", q); r.status != 503 || r.body["type"] != "service_unavailable" {
		t.Fatalf("身份同步超过 15 分钟未成功应 503，得到 %d %v", r.status, r.body)
	}
	e.identities.lastSync = time.Time{}
	if r := e.do(t, "/v1/search", e.authz.token(t, "alice", "console"), "", q); r.status != 503 {
		t.Fatalf("从未同步成功也应 503，得到 %d", r.status)
	}
	e.identities.lastSync, e.identities.err = time.Now(), errors.New("connection refused")
	if r := e.do(t, "/v1/search", e.authz.token(t, "alice", "console"), "", q); r.status != 503 {
		t.Fatalf("身份库不可用应 503，得到 %d", r.status)
	}
}

func TestSearchCarriesVerifiablePrincipal(t *testing.T) {
	e := newEnv(t)
	r := e.do(t, "/v1/search", e.authz.token(t, "alice", "console"), "", q)
	if r.status != 200 {
		t.Fatalf("得到 %d %s", r.status, r.raw)
	}
	if got := r.body["index_version"]; got != "alice@credit|CLASSIFICATION_INTERNAL|groups=1|ref=|actor=console" {
		t.Errorf("下游应拿到验签通过、属性来自身份库的 Principal，得到 %v", got)
	}
	hit := r.body["hits"].([]any)[0].(map[string]any)
	if hit["source_type"] != "wiki" || hit["classification"] != "internal" {
		t.Errorf("枚举应转成小写对外值: %v", hit)
	}
	// 图召回找到的块带路径，枚举同样转成小写对外值
	edge := hit["graph_path"].([]any)[0].(map[string]any)
	if edge["from_entity"] != "清算核心服务" || edge["from_type"] != "system" || edge["relation_type"] != "belongs_to" ||
		edge["to_entity"] != "支付清算部" || edge["to_type"] != "org" {
		t.Errorf("图路径映射不对: %v", edge)
	}

	// 业务系统客户端绑定租户，无需请求头
	r = e.do(t, "/v1/search", e.authz.token(t, "dual", "biz-credit"), "", q)
	if r.status != 200 || !strings.HasPrefix(r.body["index_version"].(string), "dual@credit|") {
		t.Errorf("绑定客户端应落到 credit: %d %v", r.status, r.body["index_version"])
	}

	// 群组超过 200 个时只放引用
	r = e.do(t, "/v1/search", e.authz.token(t, "eve", "console"), "credit", q)
	if r.status != 200 || !strings.Contains(r.body["index_version"].(string), "groups=0|ref=eve") {
		t.Errorf("大群组应只放引用: %d %v", r.status, r.body["index_version"])
	}
}

func TestGroupRemovalTakesEffectOnNextRequest(t *testing.T) {
	e := newEnv(t)
	if r := e.do(t, "/v1/search", e.authz.token(t, "alice", "console"), "", q); r.status != 200 {
		t.Fatalf("移出群组前应可用，得到 %d", r.status)
	}
	// 身份同步把 alice 移出 credit-l2：gateway 不缓存属性，下一个请求即按新群组判断
	u := e.identities.users["alice"]
	u.Groups = nil
	e.identities.users["alice"] = u
	if r := e.do(t, "/v1/search", e.authz.token(t, "alice", "console"), "credit", q); r.status != 403 {
		t.Fatalf("移出群组后应 403，得到 %d", r.status)
	}
}

func TestAnswersPilotAndAnswerID(t *testing.T) {
	e := newEnv(t)
	r := e.do(t, "/v1/answers", e.authz.token(t, "rita", "console"), "", q)
	if r.status != 200 {
		t.Fatalf("试点群组成员应可问答，得到 %d %s", r.status, r.raw)
	}
	if id, _ := r.body["answer_id"].(string); !strings.HasPrefix(id, "ans_") || r.body["status"] != "insufficient_evidence" {
		t.Errorf("answer_id 由 gateway 生成且带 ans_ 前缀，status 转小写: %v", r.body)
	}
	if s := r.body["sentences"].([]any)[0].(map[string]any); s["claim_type"] != "quote" {
		t.Errorf("claim_type 应为 quote: %v", s)
	}
	if _, ok := r.body["conversation_id"]; ok {
		t.Errorf("orchestrator 没给会话 ID 时不输出空字段: %v", r.body)
	}
	r = e.do(t, "/v1/answers", e.authz.token(t, "rita", "console"), "", `{"question":"x","conversation_id":"conv_abc123"}`)
	if r.status != 200 || r.body["conversation_id"] != "conv_abc123" {
		t.Errorf("会话 ID 应透传给 orchestrator 并返回: %d %v", r.status, r.body)
	}
	// 试点租户的仅检索对全部成员开放
	if r := e.do(t, "/v1/search", e.authz.token(t, "bob", "console"), "", q); r.status != 200 {
		t.Errorf("试点租户的仅检索应对成员开放，得到 %d", r.status)
	}
}

// fakeLimits 按“规则名:键”拒绝指定的桶，并记下每个桶最近一次用的规则。
type fakeLimits struct {
	mu    sync.Mutex
	deny  map[string]bool
	rules map[string]ratelimit.Rule
}

func (f *fakeLimits) Allow(_ context.Context, rule ratelimit.Rule, key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rules == nil {
		f.rules = map[string]ratelimit.Rule{}
	}
	f.rules[rule.Name+":"+key] = rule
	return !f.deny[rule.Name+":"+key]
}

func (f *fakeLimits) rule(key string) ratelimit.Rule {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rules[key]
}

func TestRateLimitsReturn429BeforeIdentityLookup(t *testing.T) {
	e := newEnv(t)
	e.limits.deny["user-answers:rita"] = true
	r := e.do(t, "/v1/answers", e.authz.token(t, "rita", "console"), "", q)
	if r.status != 429 || r.body["type"] != "rate_limited" {
		t.Fatalf("用户问答超限应返回 429 rate_limited，得到 %d %s", r.status, r.raw)
	}
	// 用户层只限问答；仅检索照常
	if r := e.do(t, "/v1/search", e.authz.token(t, "rita", "console"), "", q); r.status != 200 {
		t.Fatalf("用户问答限流不影响仅检索，得到 %d", r.status)
	}
	e.limits.deny["client:console"] = true
	if r := e.do(t, "/v1/search", e.authz.token(t, "bob", "console"), "", q); r.status != 429 {
		t.Fatalf("调用方应用超限应返回 429，得到 %d", r.status)
	}
	recs := e.audit.snapshot()
	if last := recs[len(recs)-1]; last.Status != 429 || last.Reason != "rate_limited" || last.Sub != "bob" {
		t.Fatalf("被限流的请求也要写审计: %+v", last)
	}
}

func TestAuditEveryRequestWithoutQuestionText(t *testing.T) {
	e := newEnv(t)
	e.do(t, "/v1/search", "", "", q)
	e.do(t, "/v1/answers", e.authz.token(t, "rita", "console"), "", q)
	if len(e.audit.snapshot()) != 2 {
		t.Fatalf("被拒绝和成功的请求都应写审计，得到 %d 条", len(e.audit.snapshot()))
	}
	recs := e.audit.snapshot()
	rej, ok := recs[0], recs[1]
	if rej.Status != 401 || rej.Reason != "unauthenticated" {
		t.Errorf("拒绝记录: %+v", rej)
	}
	if ok.Sub != "rita" || ok.Tenant != "risk" || ok.Actor != "console" || ok.JTI == "" || !strings.HasPrefix(ok.AnswerID, "ans_") ||
		ok.QuestionLen != 9 || len(ok.QuestionSHA) != 64 {
		t.Errorf("成功记录字段不全: %+v", ok)
	}
	line, _ := json.Marshal(recs)
	if strings.Contains(string(line), "授信额度") {
		t.Error("审计不应包含问题原文")
	}

	e.audit.fail = true
	r := e.do(t, "/v1/search", e.authz.token(t, "alice", "console"), "", q)
	if r.status != 503 || r.body["hits"] != nil {
		t.Fatalf("审计写入失败时应 503 且不返回结果，得到 %d %s", r.status, r.raw)
	}
}

// ---- SSE（gateway 详设第 6 节）----

type sseEvent struct {
	name string
	data map[string]any
}

func parseSSE(t *testing.T, body string) []sseEvent {
	t.Helper()
	var out []sseEvent
	for _, block := range strings.Split(strings.TrimSpace(body), "\n\n") {
		var ev sseEvent
		for _, l := range strings.Split(block, "\n") {
			switch {
			case strings.HasPrefix(l, "event: "):
				ev.name = strings.TrimPrefix(l, "event: ")
			case strings.HasPrefix(l, "data: "):
				if err := json.Unmarshal([]byte(strings.TrimPrefix(l, "data: ")), &ev.data); err != nil {
					t.Fatalf("data 不是 JSON: %q", l)
				}
			}
		}
		if ev.name != "" {
			out = append(out, ev)
		}
	}
	return out
}

func names(evs []sseEvent) []string {
	out := []string{}
	for _, e := range evs {
		out = append(out, e.name)
	}
	return out
}

func metaEvent() *answerv1.StreamAnswerResponse {
	return &answerv1.StreamAnswerResponse{Event: &answerv1.StreamAnswerResponse_Meta{Meta: &answerv1.AnswerMeta{AnswerId: "ignored-by-gateway", ConversationId: "conv_m1"}}}
}

func goodEvents() []*answerv1.StreamAnswerResponse {
	return []*answerv1.StreamAnswerResponse{
		metaEvent(),
		{Event: &answerv1.StreamAnswerResponse_Citation{Citation: &answerv1.Citation{
			RefId: 1, ChunkId: "wiki:doc-1#a1", DocId: "doc-1", DocVersion: 3, Quote: "额度不超过 30 万元", Span: &commonv1.Span{CharEnd: 10},
			SourceType: commonv1.SourceType_SOURCE_TYPE_WIKI, Classification: commonv1.Classification_CLASSIFICATION_INTERNAL,
		}}},
		{Event: &answerv1.StreamAnswerResponse_Sentence{Sentence: &answerv1.SentenceEvent{
			Sentence:  &answerv1.Sentence{Text: "额度不超过 30 万元。[S1]", End: 15, ClaimType: answerv1.ClaimType_CLAIM_TYPE_QUOTE, RefIds: []int32{1}, Support: 0.9, BlockId: "b1"},
			BlockType: answerv1.BlockType_BLOCK_TYPE_PARAGRAPH,
		}}},
		{Event: &answerv1.StreamAnswerResponse_Final{Final: &answerv1.AnswerFinal{
			Status: answerv1.Status_STATUS_ANSWERED, Why: &answerv1.Why{GenerationModel: "chat@t"},
		}}},
	}
}

func TestAnswersStreamRelaysEventsInOrder(t *testing.T) {
	e := newEnv(t)
	e.orchestrator.set(goodEvents(), nil, false)
	r := e.do(t, "/v1/answers", e.authz.token(t, "rita", "console"), "", qStream)
	if r.status != 200 {
		t.Fatalf("得到 %d %s", r.status, r.raw)
	}
	evs := parseSSE(t, r.raw)
	if got := names(evs); strings.Join(got, ",") != "meta,citation,sentence,final" {
		t.Fatalf("事件顺序应原样透传，得到 %v", got)
	}
	recs := e.audit.snapshot()
	if len(recs) != 2 || recs[0].Stream != "open" || recs[1].Stream != "end" || recs[1].Reason != "" {
		t.Fatalf("SSE 请求开始输出前、结束时各写一条审计: %+v", recs)
	}
	if id := evs[0].data["answer_id"]; id != recs[0].AnswerID || !strings.HasPrefix(id.(string), "ans_") || evs[0].data["trace_id"] != recs[0].TraceID {
		t.Errorf("meta 的 answer_id 由 gateway 生成，与审计一致: %v", evs[0].data)
	}
	if evs[0].data["conversation_id"] != "conv_m1" {
		t.Errorf("meta 应带 orchestrator 给出的会话 ID: %v", evs[0].data)
	}
	if c := evs[1].data; c["source_type"] != "wiki" || c["classification"] != "internal" || c["ref_id"] != float64(1) {
		t.Errorf("citation 字段与非流式一致: %v", c)
	}
	if st := evs[2].data; st["block_type"] != "paragraph" || st["claim_type"] != "quote" || st["block_id"] != "b1" || st["end"] != float64(15) {
		t.Errorf("sentence 应带 block_type 与非流式的句子字段: %v", st)
	}
	if f := evs[3].data; f["status"] != "answered" || f["why"].(map[string]any)["generation_model"] != "chat@t" {
		t.Errorf("final: %v", f)
	}
	e.orchestrator.mu.Lock()
	md, hasDeadline := e.orchestrator.seenMD, e.orchestrator.hasDeadline
	e.orchestrator.mu.Unlock()
	if got := md.Get("x-answer-id"); len(got) != 1 || got[0] != recs[0].AnswerID {
		t.Errorf("x-answer-id 应与审计一致: %v", got)
	}
	if got := md.Get("x-call-purpose"); len(got) != 1 || got[0] != "answer" {
		t.Errorf("x-call-purpose: %v", got)
	}
	if got := md.Get("authorization"); len(got) != 1 || !strings.HasPrefix(got[0], "Bearer ") {
		t.Error("下游应拿到 gateway 签发的 Principal")
	}
	if !hasDeadline {
		t.Error("流式调用同样带 deadline")
	}
}

func TestAnswersStreamErrors(t *testing.T) {
	e := newEnv(t)
	tok := e.authz.token(t, "rita", "console")

	// 流开始后出错：按 fromGRPC 映射成 error 事件，不透出下游细节
	e.orchestrator.set([]*answerv1.StreamAnswerResponse{metaEvent()}, status.Error(codes.PermissionDenied, "retrieval 拒绝"), false)
	r := e.do(t, "/v1/answers", tok, "", qStream)
	evs := parseSSE(t, r.raw)
	if r.status != 200 || strings.Join(names(evs), ",") != "meta,error" || evs[1].data["type"] != "forbidden" ||
		evs[1].data["trace_id"] == "" || strings.Contains(r.raw, "retrieval") {
		t.Fatalf("应以 error 事件结束且不含内部细节: %d %s", r.status, r.raw)
	}
	if recs := e.audit.snapshot(); recs[len(recs)-1].Reason != "forbidden" {
		t.Errorf("结束审计应记错误类型: %+v", recs[len(recs)-1])
	}

	// 没有 final 就结束：客户端不能把已收到的句子当成完整回答
	e.orchestrator.set(goodEvents()[:3], nil, false)
	evs = parseSSE(t, e.do(t, "/v1/answers", tok, "", qStream).raw)
	if strings.Join(names(evs), ",") != "meta,citation,sentence,error" || evs[3].data["type"] != "service_unavailable" {
		t.Fatalf("缺 final 应补 error 事件: %v", names(evs))
	}

	// meta 之前出错：还没开始输出，按普通 HTTP 错误返回
	e.orchestrator.set(nil, status.Error(codes.Unavailable, "down"), false)
	r = e.do(t, "/v1/answers", tok, "", qStream)
	if r.status != 503 || r.body["type"] != "service_unavailable" {
		t.Fatalf("meta 之前出错应返回 503 problem，得到 %d %s", r.status, r.raw)
	}

	// 审计写不进去就不开始输出
	e.orchestrator.set(goodEvents(), nil, false)
	e.audit.mu.Lock()
	e.audit.fail = true
	e.audit.mu.Unlock()
	r = e.do(t, "/v1/answers", tok, "", qStream)
	if r.status != 503 || strings.Contains(r.raw, "event:") {
		t.Fatalf("审计写入失败时应 503 且不输出事件，得到 %d %s", r.status, r.raw)
	}
}

func TestAnswersStreamClientDisconnectCancelsDownstream(t *testing.T) {
	e := newEnv(t)
	e.orchestrator.set([]*answerv1.StreamAnswerResponse{metaEvent()}, nil, true)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.srv.URL+"/v1/answers", strings.NewReader(qStream))
	req.Header.Set("Authorization", "Bearer "+e.authz.token(t, "rita", "console"))
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	// 读到 meta 后断开
	buf := make([]byte, 512)
	if n, _ := resp.Body.Read(buf); !strings.Contains(string(buf[:n]), "event: meta") {
		t.Fatalf("应先收到 meta: %q", buf[:n])
	}
	cancel()
	_ = resp.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if recs := e.audit.snapshot(); len(recs) == 2 {
			if recs[1].Stream != "end" || recs[1].Reason != "cancelled" {
				t.Fatalf("客户端断开应记 cancelled: %+v", recs[1])
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("客户端断开后下游没有被取消，流没有结束")
}

func TestRelaySendsHeartbeatWhileWaiting(t *testing.T) {
	s := &Server{Log: logging.New(&bytes.Buffer{}, "gateway", slog.LevelInfo), Overload: admission.NewAdaptive(10, time.Now), Now: time.Now}
	c := &call{ctx: context.Background(), traceID: "t1"}
	events, beat := make(chan recvResult), make(chan time.Time)
	pings := 0
	var sent []string
	go func() {
		beat <- time.Now()
		beat <- time.Now()
		events <- recvResult{ev: goodEvents()[3]}
	}()
	reason := s.relay(c, events, beat, func(name string, _ any) { sent = append(sent, name) }, func() { pings++ })
	if reason != "" || pings != 2 || strings.Join(sent, ",") != "final" {
		t.Fatalf("等待下游时应发心跳: reason=%q pings=%d sent=%v", reason, pings, sent)
	}
}
