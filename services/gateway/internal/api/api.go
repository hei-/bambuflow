// Package api 是对外 HTTP 接口（技术设计文档 13.9，contracts/openapi/v1/openapi.yaml）。
//
// 处理链（gateway 详设第 2 节）：接入限制 → 认证 → 限流 → 构造 Principal → 功能授权 → 路由。
// 任一步失败就在入口返回，不把半成品请求交给下游；每个请求（含被拒绝的）都写入口审计。
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	batchv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/batch/v1"
	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/hei-/bambuflow/libs/platform-go/tenancy"
	"github.com/hei-/bambuflow/services/gateway/internal/admission"
	"github.com/hei-/bambuflow/services/gateway/internal/audit"
	"github.com/hei-/bambuflow/services/gateway/internal/authn"
	"github.com/hei-/bambuflow/services/gateway/internal/idempotency"
	"github.com/hei-/bambuflow/services/gateway/internal/identity"
	"github.com/hei-/bambuflow/services/gateway/internal/issuer"
	"github.com/hei-/bambuflow/services/gateway/internal/ratelimit"
	"github.com/hei-/bambuflow/services/gateway/internal/roles"
)

const (
	maxBody = 64 << 10 // 请求体上限 64 KB（gateway 详设第 9 节）
	// 批次输入文件的上限：10,000 条（技术设计文档 13.5）按每条平均约 2 KB 估算
	maxBatchBody = 20 << 20
	// 身份同步超过这么久没有成功就拒绝请求：撤权时效已无法保证（gateway 详设 3.5）
	maxIdentityStaleness = 15 * time.Minute
	searchDeadline       = time.Second
	// 按多跳的 10 s 设：调用前不知道问题走哪条路径，单跳的 4 s 由 orchestrator 按意图路由的结果在内部收紧
	// （orchestrator 详设 5.3、11.1）
	answerDeadline = 10 * time.Second
	// orchestrator 的延迟预算（orchestrator 详设 11.1）：多跳 10 s，其余 4 s；全局自适应并发按耗时占预算的比例判断过载
	multiHopBudget  = 10 * time.Second
	singleHopBudget = 4 * time.Second
	// SSE 心跳间隔：防止中间代理断开空闲连接（gateway 详设第 6 节）
	sseHeartbeat = 15 * time.Second
)

type IdentityStore interface {
	Lookup(ctx context.Context, username string) (identity.User, bool, error)
	LastSync(ctx context.Context) (time.Time, error)
	// Unknown 返回平台身份库里不存在的用户与群组
	Unknown(ctx context.Context, users, groups []string) ([]string, []string, error)
}

type AuditWriter interface {
	Write(audit.Record) error
}

// RateLimiter 从某一层限流的某个桶里取一个令牌（internal/ratelimit）。
type RateLimiter interface {
	Allow(ctx context.Context, rule ratelimit.Rule, key string) bool
}

type Server struct {
	Verifier   *authn.Verifier
	Identities IdentityStore
	// Tenants 返回当前生效的租户配置：由 governance 的配置快照下发，更新时原子替换（internal/tenantsync）
	Tenants      func() *tenancy.Registry
	Issuer       *issuer.Issuer
	Retrieval    retrievalv1.RetrievalServiceClient
	Orchestrator answerv1.AnswerServiceClient
	Batches      batchv1.BatchServiceClient
	// 管理接口的实现方（gateway 详设 4.2）
	Denials       adminv1.EmergencyDenialAdminServiceClient
	IndexVersions adminv1.IndexVersionAdminServiceClient
	AuditQuery    adminv1.AuditAdminServiceClient
	Sources       adminv1.SourceAdminServiceClient
	Experiments   adminv1.ExperimentAdminServiceClient
	Vocabularies  adminv1.VocabularyAdminServiceClient
	Plugins       adminv1.PluginAdminServiceClient
	Configs       adminv1.ConfigAdminServiceClient
	Ingest        adminv1.IngestAdminServiceClient
	Tombstones    adminv1.TombstoneAdminServiceClient
	Receipts      adminv1.DeletionReceiptAdminServiceClient
	SnapshotSync  adminv1.SnapshotSyncAdminServiceClient
	Rebuilds      adminv1.RebuildAdminServiceClient
	Connectors    adminv1.ConnectorAdminServiceClient
	// Push 是 connectors 的推送入库（push.go）；PushSources 按数据源取推送登记（与租户配置同一份快照，internal/tenantsync）
	Push        ingestionv1.PushServiceClient
	PushSources func(sourceID string) (*configv1.PushSource, bool)
	Analysis    adminv1.ExperimentAnalysisServiceClient
	Trends      adminv1.QualityTrendServiceClient
	Gates       adminv1.EvalGateAdminServiceClient
	Golden      adminv1.GoldenSetAdminServiceClient
	Audit       AuditWriter
	Limits      RateLimiter
	// 限流规则（gateway 详设 5.1，部署配置清单）：每个调用方应用一个桶；用户的问答一个桶
	ClientRule, UserAnswerRule ratelimit.Rule
	// 租户并发与每用户并发 SSE 流（gateway 详设 5.1、第 6 节）
	Slots *admission.Slots
	// 租户每日 token 配额（gateway 详设 5.1）
	Quota *admission.Quota
	// 全局自适应并发（gateway 详设 5.2）
	Overload *admission.Adaptive
	// UserStreams 是每个用户同时打开的 SSE 流上限（部署配置清单：3 路）
	UserStreams int
	// PublicURL 是 gateway 对外的根地址（如 https://rag.corp.example），MCP 的资源标识为 PublicURL + "/mcp"；
	// AuthorizationServer 是平台授权服务的签发方地址，写进受保护资源元数据（gateway 详设 7.2）
	PublicURL, AuthorizationServer string
	// ClientCertHeader 是入口代理放校验过的客户端证书主题 DN 的请求头（部署配置）；为空时不接受业务系统代用户调用
	ClientCertHeader string
	// AdminRoles 把平台身份库的群组映射成平台角色，管理接口按它做 RBAC（gateway 详设 4.2）
	AdminRoles roles.Map
	// StepUp 是审批类管理操作要求的加强认证（governance 详设 9.1，部署配置清单 P7）
	StepUp StepUp
	// Idempotency 记管理写操作的幂等键（gateway 详设 4.3）
	Idempotency *idempotency.Store
	Log         *slog.Logger
	Now         func() time.Time
	// adminTable 非空时代替 adminRoutes()，只给单元测试用
	adminTable []adminRoute
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/search", s.wrap("search", maxBody, s.search))
	mux.HandleFunc("POST /v1/answers", s.wrap("answers", maxBody, s.answers))
	mux.HandleFunc("POST /v1/compat/chat/completions", s.wrap("compat_chat_completions", maxBody, s.compatChatCompletions))
	mux.HandleFunc("POST /mcp", s.wrap("mcp", maxBody, s.mcp))
	// 推送入库（push.go）：只接受登记过的推送客户端的服务身份
	mux.HandleFunc("PUT /v1/sources/{source_id}/documents/{doc_id}", s.wrap("push_document", maxPushBody, s.putDocument))
	mux.HandleFunc("DELETE /v1/sources/{source_id}/documents/{doc_id}", s.wrap("push_delete", maxBody, s.deletePushedDocument))
	mux.HandleFunc("POST /v1/sources/{source_id}/documents/batch", s.wrap("push_batch", maxPushBatchBody, s.batchPush))
	mux.HandleFunc("GET /mcp", mcpMethodNotAllowed)
	mux.HandleFunc("DELETE /mcp", mcpMethodNotAllowed)
	mux.HandleFunc("GET "+mcpResourcePath, s.resourceMetadata)
	mux.HandleFunc("GET /v1/documents/{doc_id}/preview", s.wrap("preview", maxBody, s.preview))
	mux.HandleFunc("GET /v1/answers/{answer_id}", s.wrap("answer-replay", maxBody, s.getAnswer))
	mux.HandleFunc("POST /v1/answers/{answer_id}/feedback", s.wrap("answer-feedback", maxBody, s.createFeedback))
	mux.HandleFunc("DELETE /v1/conversations/{conversation_id}", s.wrap("conversation-delete", maxBody, s.deleteConversation))
	mux.HandleFunc("POST /v1/batches", s.wrap("batches", maxBatchBody, s.createBatch))
	mux.HandleFunc("GET /v1/batches/{batch_id}", s.wrap("batches", maxBody, s.getBatch))
	mux.HandleFunc("POST /v1/batches/{batch_id}/cancel", s.wrap("batches", maxBody, s.cancelBatch))
	mux.HandleFunc("GET /v1/batches/{batch_id}/results", s.wrap("batch-results", maxBody, s.batchResults))
	// 管理接口按登记的路由表分发，未登记的方法与路径在 admin 里返回 404
	mux.HandleFunc("/v1/admin/", s.wrap("admin", maxBody, s.admin))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, newTraceID(), problemNotFound)
	})
	return mux
}

// call 记录一次请求在处理链上积累的信息，最后写入审计。
type call struct {
	traceID, answerID, jti string
	batchID                string
	authTime               int64
	sub, tenant, actor     string
	// 用户的群组（来自平台身份库），判定审计岗用
	groups []string
	// 回放时调用者凭什么看到这条回答，写审计
	access   string
	question string
	blocks   *answerv1.AuditBlocks // 问答的三类块，只进审计
	// 将要发出、还没写单独访问记录的受限级块与文档（restricted.go）
	restricted *answerv1.AuditBlocks
	// 问答或检索用到的版本、问答的状态与已推送句子的摘要，只进审计
	versions     *audit.Versions
	answerStatus string
	answerSHA    string
	// 流式问答已推送的句子，算回答摘要用
	pushed   []string
	endpoint string
	start    time.Time
	ctx      context.Context
	// 处理函数已经自己写了响应和审计（SSE），wrap 不再处理
	streamed bool
	// 成功响应的 HTTP 状态码，0 为 200（MCP 的通知返回 202，JSON-RPC 格式错误返回 400）
	httpStatus int
	// MCP 工具执行失败：HTTP 照常返回 200 与 JSON-RPC 结果，审计按这个错误记录
	toolErr *problem
	// 准入时占用的名额（全局并发、租户并发、SSE 流），请求结束时归还
	release []func()
	// 问答类请求预占的 token 配额，请求结束时按 usage 结算
	quota *admission.Hold
	// orchestrator 回报的实际用量，只用于配额与审计，不对外返回
	usage *answerv1.TokenUsage
	// orchestrator 在生成之前就拒绝了请求（参数、权限、下游不可用等）：预占的配额全部退回
	rejected bool
	// 调用 orchestrator 的开始时刻，用于计算延迟信号
	orchStart time.Time
	// 管理接口的审计内容（gateway 详设 8.2）
	admin *audit.Admin
}

// problem 是一个对外错误（技术设计文档 13.10）。
type problem struct {
	status int
	typ    string
	title  string
}

func (p problem) Error() string { return p.typ }

var (
	problemInvalid      = problem{http.StatusBadRequest, "invalid_request", "参数不合法"}
	problemTenant       = problem{http.StatusBadRequest, "invalid_request", "请用请求头 " + tenancy.Header + " 指定租户"}
	problemUnauth       = problem{http.StatusUnauthorized, "unauthenticated", "令牌缺失或无效"}
	problemUserRequired = problem{http.StatusUnauthorized, "user_identity_required", "需要代表终端用户的令牌"}
	problemForbidden    = problem{http.StatusForbidden, "forbidden", "无权使用该租户或功能"}
	// 审批类管理操作要求最近做过加强认证（RFC 9470），响应带 WWW-Authenticate 告诉客户端要求的 acr 与时限
	problemStepUp      = problem{http.StatusUnauthorized, "insufficient_user_authentication", "该操作需要重新认证"}
	problemNotEnabled  = problem{http.StatusForbidden, "answers_not_enabled", "该租户的问答尚未对你开放"}
	problemNotFound    = problem{http.StatusNotFound, "not_found", "资源不存在"}
	problemTooLarge    = problem{http.StatusRequestEntityTooLarge, "payload_too_large", "请求超出上限"}
	problemRateLimited = problem{http.StatusTooManyRequests, "rate_limited", "请求过于频繁，请稍后重试"}
	// 不带 Retry-After：当日不会恢复，调用方不应自动重试（技术设计文档 13.10）
	problemQuota       = problem{http.StatusTooManyRequests, "quota_exceeded", "租户当日的问答配额已用尽，仅检索照常可用"}
	problemOverloaded  = problem{http.StatusServiceUnavailable, "service_unavailable", "服务繁忙，请稍后重试"}
	problemUnavailable = problem{http.StatusServiceUnavailable, "service_unavailable", "服务暂时不可用"}
	problemDeadline    = problem{http.StatusGatewayTimeout, "deadline_exceeded", "超过截止时间"}
)

type handlerFunc func(w http.ResponseWriter, r *http.Request, c *call) (any, error)

func (s *Server) wrap(endpoint string, limit int64, h handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := s.Now()
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("gateway").Start(ctx, "gateway."+endpoint)
		defer span.End()
		c := &call{ctx: ctx, traceID: span.SpanContext().TraceID().String(), endpoint: endpoint, start: start}
		if !span.SpanContext().IsValid() {
			c.traceID = newTraceID()
		}
		// SSE 由处理函数自己写完响应后才返回，所以这里覆盖了流的全过程
		defer s.finish(c)

		r.Body = http.MaxBytesReader(w, r.Body, limit)
		body, err := h(w, r, c)
		if c.streamed {
			return
		}
		if err == nil {
			// 受限级内容先写单独的访问记录，写不进去就不返回（fail-closed）
			if rerr := s.writeRestricted(c); rerr != nil {
				s.Log.ErrorContext(ctx, "受限级访问记录写入失败", slog.Any("err", rerr))
				err = problemUnavailable
			}
		}

		status := http.StatusOK
		if c.httpStatus != 0 {
			status = c.httpStatus
		}
		var p problem
		if err != nil {
			if !errors.As(err, &p) {
				s.Log.ErrorContext(ctx, "请求处理出错", slog.String("endpoint", endpoint), slog.Any("err", err))
				p = problemUnavailable
			}
			status = p.status
		}
		reason := ""
		if err != nil {
			reason = p.typ
		}
		auditStatus, auditReason := status, reason
		if c.toolErr != nil {
			auditStatus, auditReason = c.toolErr.status, c.toolErr.typ
		}
		if aerr := s.writeAudit(c, auditStatus, auditReason, ""); aerr != nil {
			// 审计写不进去就不返回结果（fail-closed）
			s.Log.ErrorContext(ctx, "入口审计写入失败", slog.Any("err", aerr))
			writeProblem(w, c.traceID, problemUnavailable)
			return
		}
		if err != nil {
			writeProblem(w, c.traceID, p)
			return
		}
		if body == nil {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// writeAudit 写一条入口审计。stream 只在 SSE 请求上填写：open 为开始输出前（写不进去就不输出），end 为流结束时。
func (s *Server) writeAudit(c *call, status int, reason, stream string) error {
	sha, n := audit.QuestionDigest(c.question)
	return s.Audit.Write(audit.Record{
		Time: c.start, TraceID: c.traceID, AnswerID: c.answerID, BatchID: c.batchID, JTI: c.jti, Sub: c.sub, Tenant: c.tenant,
		Actor: c.actor, Endpoint: c.endpoint, Status: status, Reason: reason, Stream: stream,
		LatencyMS: s.Now().Sub(c.start).Milliseconds(), QuestionSHA: sha, QuestionLen: n,
		CandidateChunkIDs: c.blocks.GetCandidateChunkIds(), ContextChunkIDs: c.blocks.GetContextChunkIds(),
		ShownChunkIDs: c.blocks.GetShownChunkIds(), ListedDocIDs: c.blocks.GetListedDocIds(), TotalCount: c.blocks.GetTotalCount(),
		PromptTokens: c.usage.GetPromptTokens(), CompletionTokens: c.usage.GetCompletionTokens(),
		Versions: c.versions, AnswerStatus: c.answerStatus, AnswerSHA: c.answerSHA,
		Access: c.access, Admin: c.admin,
	})
}

// finish 在请求结束时结算配额、归还名额。
//
// 结算口径：orchestrator 回报了用量就按实际用量；没有调用 orchestrator，或它在生成之前就拒绝了，全部退回；
// 其余情况（客户端断开、超时、流中途出错、旧版 orchestrator 不回报用量）按预占量计，
// 已产生的用量照常计入（gateway 详设第 6 节），宁可多计不少计。
func (s *Server) finish(c *call) {
	if c.quota != nil {
		actual := int64(admission.EstimateTokens)
		switch {
		case c.usage != nil:
			actual = c.usage.GetPromptTokens() + c.usage.GetCompletionTokens()
		case c.answerID == "" || (c.rejected && !c.streamed):
			// 流已经开始输出（收到 meta）后再出错，生成已经发生，不退回
			actual = 0
		}
		c.quota.Settle(c.ctx, actual)
	}
	for _, release := range c.release {
		release()
	}
}

// orchestratorErr 把 orchestrator 的错误转成对外错误，同时记下它对配额与过载判断的影响。
func (s *Server) orchestratorErr(c *call, err error) problem {
	switch status.Code(err) {
	case codes.ResourceExhausted:
		// 下游已经满载：立即收紧全局并发，不等 p95（gateway 详设 5.2）
		s.Overload.Exhausted()
		c.rejected = true
	case codes.InvalidArgument, codes.PermissionDenied, codes.NotFound, codes.FailedPrecondition, codes.Unauthenticated, codes.Unavailable:
		c.rejected = true
	case codes.DeadlineExceeded:
		s.observe(c, answerv1.Intent_INTENT_UNSPECIFIED)
	}
	return fromGRPC(err)
}

// observe 把一次 orchestrator 调用的耗时折算成占其延迟预算的比例，作为全局自适应并发的信号。
// 预算按回答的意图取：多跳 10 s，其余（包括不知道意图的超时）按单跳 4 s，宁可早一点收紧。
func (s *Server) observe(c *call, intent answerv1.Intent) {
	budget := singleHopBudget
	if intent == answerv1.Intent_INTENT_MULTI_HOP {
		budget = multiHopBudget
	}
	s.Overload.Observe(float64(s.Now().Sub(c.orchStart)) / float64(budget))
}

// acquireStream 为一个 SSE 流占用该用户的名额（gateway 详设第 6 节：每用户 3 路）。
func (s *Server) acquireStream(c *call) error {
	release, ok := s.Slots.Acquire(c.ctx, "user-streams", c.sub, s.UserStreams)
	if !ok {
		return problemRateLimited
	}
	c.release = append(c.release, release)
	return nil
}

// access 是一个接口在处理链上的差别。除 priority 外，零值是仅检索：租户成员即可。
type access struct {
	// priority：全局过载时的优先级（gateway 详设 5.2），每个接口都必须明确给出
	priority admission.Priority
	// answers：问答类接口，另受用户问答限流，试点租户只对试点群组开放
	answers bool
	// batch：提交批量问答。和问答一样要求租户已对用户开放问答，但不占在线问答的限流额度（gateway 详设 5.1）
	batch bool
	// mcp：MCP 端点，客户端必须是登记过的 agent，按登记的额度限流（gateway 详设 7.2）
	mcp bool
}

// authorize 依次完成认证、读取身份、确定租户，返回签好的 Principal 与租户。
func (s *Server) authorize(r *http.Request, c *call, a access) (string, *tenancy.Tenant, error) {
	caller, err := s.Verifier.Verify(c.ctx, r.Header.Get("Authorization"))
	switch {
	case errors.Is(err, authn.ErrUserIdentityRequired):
		return "", nil, problemUserRequired
	case err != nil:
		s.Log.InfoContext(c.ctx, "平台令牌校验失败", slog.Any("err", err))
		return "", nil, problemUnauth
	}
	c.sub, c.actor, c.authTime = caller.Username, caller.ClientID, caller.AuthTime
	if err := s.checkOnBehalf(r, c, caller); err != nil {
		return "", nil, err
	}
	clientRule := s.ClientRule
	if a.mcp {
		if caller.Act != "" {
			// MCP 的 agent 走授权码流程，不做令牌交换
			return "", nil, problemForbidden
		}
		agent, ok := s.Tenants().Agent(caller.ClientID)
		if !ok {
			// 未登记的 agent 即使拿到了用户令牌也拒绝（gateway 详设 7.2），不再查身份库
			return "", nil, problemForbidden
		}
		if agent.RPS > 0 {
			clientRule.Rate, clientRule.Burst = float64(agent.RPS), agent.RPS
		}
	}
	// 限流放在查身份库之前：被限流的请求不再占用数据库
	if !s.Limits.Allow(c.ctx, clientRule, caller.ClientID) ||
		(a.answers && !s.Limits.Allow(c.ctx, s.UserAnswerRule, caller.Username)) {
		return "", nil, problemRateLimited
	}
	// 全局过载保护同样在查身份库之前：过载时不再给数据库与下游加压，按优先级从低往高拒绝
	done, ok := s.Overload.Admit(a.priority)
	if !ok {
		return "", nil, problemOverloaded
	}
	c.release = append(c.release, done)

	last, err := s.Identities.LastSync(c.ctx)
	if err != nil {
		return "", nil, fmt.Errorf("读取身份同步状态: %w", err)
	}
	if s.Now().Sub(last) > maxIdentityStaleness {
		s.Log.ErrorContext(c.ctx, "身份同步超过 15 分钟未成功，拒绝请求", slog.Time("last_success", last))
		return "", nil, problemUnavailable
	}
	user, ok, err := s.Identities.Lookup(c.ctx, caller.Username)
	if err != nil {
		return "", nil, fmt.Errorf("读取平台身份库: %w", err)
	}
	if !ok || !user.Active {
		return "", nil, problemForbidden
	}

	reg := s.Tenants()
	resolve := reg.Resolve
	if a.mcp {
		resolve = reg.ResolveAgent
	}
	t, err := resolve(caller.ClientID, r.Header.Get(tenancy.Header), user.Groups)
	switch {
	case errors.Is(err, tenancy.ErrTenantRequired):
		return "", nil, problemTenant
	case err != nil:
		return "", nil, problemForbidden
	}
	c.tenant, c.groups = t.Name, user.Groups
	if (a.answers || a.batch) && !t.AnswersAllowed(user.Groups) {
		return "", nil, problemNotEnabled
	}
	if err := s.admitTenant(c, t, a); err != nil {
		return "", nil, err
	}

	token, jti, err := s.Issuer.Issue(issuer.Subject{
		Username: user.Username, Tenant: t.Name, Dept: user.Dept, Title: user.Title, Region: user.Region,
		Clearance: user.Clearance, Groups: user.Groups, Actor: caller.ClientID, AuthTime: caller.AuthTime,
	})
	if err != nil {
		return "", nil, fmt.Errorf("签发 Principal: %w", err)
	}
	c.jti = jti
	return token, t, nil
}

// admitTenant 检查租户这一层（gateway 详设 5.1）：在线请求占一个租户并发名额；问答类请求再预占当日 token 配额，
// 配额用尽时问答类接口返回 quota_exceeded，仅检索不受影响。批量不进在线并发与配额（qa/batch 有自己的并发上限）。
func (s *Server) admitTenant(c *call, t *tenancy.Tenant, a access) error {
	if a.priority == admission.PriorityBatch {
		return nil
	}
	limits := t.Limits()
	release, ok := s.Slots.Acquire(c.ctx, "tenant", t.Name, limits.MaxConcurrency)
	if !ok {
		return problemRateLimited
	}
	c.release = append(c.release, release)
	if a.answers {
		hold, ok := s.Quota.Reserve(c.ctx, t.Name, limits.DailyTokens, admission.EstimateTokens)
		if !ok {
			return problemQuota
		}
		c.quota = hold
	}
	return nil
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return problemTooLarge
		}
		return problemInvalid
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return problemInvalid
	}
	return nil
}

func validQuestion(q string) error {
	n := utf8.RuneCountInString(strings.TrimSpace(q))
	switch {
	case n == 0:
		return problemInvalid
	case n > 2000:
		return problemTooLarge
	}
	return nil
}

// downstream 返回调下游 gRPC 的上下文：带 Principal、调用目的与 deadline。deadline 只能缩短。
func downstream(ctx context.Context, principal, purpose, answerID string, d time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, d)
	md := []string{"authorization", "Bearer " + principal, "x-call-purpose", purpose}
	if answerID != "" {
		md = append(md, "x-answer-id", answerID)
	}
	return metadata.AppendToOutgoingContext(ctx, md...), cancel
}

func writeProblem(w http.ResponseWriter, traceID string, p problem) {
	w.Header().Set("Content-Type", "application/problem+json")
	if p.status == http.StatusServiceUnavailable || p.typ == problemRateLimited.typ {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(p.status)
	_ = json.NewEncoder(w).Encode(problemBody(traceID, p))
}

// problemBody 是 problem+json 的内容；SSE 的 error 事件用同样的内容。
func problemBody(traceID string, p problem) map[string]any {
	return map[string]any{"type": p.typ, "title": p.title, "status": p.status, "trace_id": traceID}
}

func newTraceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func newAnswerID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "ans_" + hex.EncodeToString(b)
}
