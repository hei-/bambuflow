package search

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/policy"
	"github.com/hei-/bambuflow/services/retrieval/internal/guard"
	"github.com/hei-/bambuflow/services/retrieval/internal/redact"
)

const (
	defaultTopK = 20
	maxTopK     = 50
	maxQueries  = 4
	maxQueryLen = 2000 // 字符
	// 每路召回的候选数（详设 6.2），融合后再取 top_k
	recallK     = 50
	maxCheckIDs = 50
	// 剩余时间不足以完成最少一路召回时直接返回（详设 9.1）
	minBudget = 120 * time.Millisecond
)

// Candidate 是召回到的一个块，带组装响应所需的全部字段。
type Candidate struct {
	ChunkID, DocID, ParentChunkID, SourceID string
	DocVersion                              int64
	Text, Title, SourceURI, Owner           string
	HeadingPath                             []string
	Span                                    *commonv1.Span
	SourceType                              commonv1.SourceType
	Classification                          commonv1.Classification
	// 入库时检测到可疑指令（retrieval 详设 3.3），原样返回给调用方
	InjectionFlag bool
	// Text 里被掩码的 PII 原值的密文（入库时信封加密）；没有 PII 时 Ciphertext 为空
	PII SealedPII
	// 图召回找到它时的路径，以及路径上各条边的证据块所属文档：组装复核时这些文档也必须仍然可见
	GraphPath []PathStep
	PathDocs  []string
	// RouteScore 是召回它的那一路的原始分数，归一化到 0–1（分数解释，详设 7.5）：
	// 稠密为 1 − 余弦距离，关键词为 r / (r + 1)，图为关系置信度 × 跳数衰减，编号精确匹配为 1
	RouteScore float64
}

// SealedPII 是一个块里 PII 原值的密文与被主密钥包裹的数据密钥（contracts SealedPII）。
type SealedPII struct {
	Ciphertext        []byte
	WrappedKey, KeyID string
}

// DocSummary 是列举结果里的一份文档。
type DocSummary struct {
	// SummaryChunkID 是版本清单里这个内容版本的摘要块，没有时为空
	DocID, Title, SourceID, SourceURI, SummaryChunkID string
	DocVersion                                        int64
	SourceType                                        commonv1.SourceType
	Classification                                    commonv1.Classification
	Metadata                                          *commonv1.DocMetadata
}

// Group 是分组计数的一组；字段为空的文档归入 Value 为空的一组。
type Group struct {
	Value string
	Count int64
}

// ErrNoLiveIndex 表示 prod 别名还没有指向任何版本。
var ErrNoLiveIndex = errors.New("还没有线上索引版本")

// Store 是对索引与元数据库的只读访问。
type Store interface {
	// LiveVersion 返回 prod 别名指向的索引版本及其嵌入模型版本。
	LiveVersion(ctx context.Context) (version, model string, err error)
	// VersionModel 返回给定索引版本的嵌入模型（评测模式指定版本时用）；不存在或已丢弃时为 ErrNoLiveIndex。
	VersionModel(ctx context.Context, version string) (model string, err error)
	// Dense 在给定索引版本里按过滤条件召回与 vec 最近的 k 个块，按相似度降序。
	Dense(ctx context.Context, version string, f Filter, vec []float32, k int) ([]Candidate, error)
	// Sparse 按学习型稀疏向量的内积召回 k 个块（没有稀疏向量的块不参与），按内积降序。
	Sparse(ctx context.Context, version string, f Filter, vec map[uint32]float32, k int) ([]Candidate, error)
	// Keyword 在给定索引版本里按过滤条件召回含任一 terms 的 k 个块，按相关度降序。
	Keyword(ctx context.Context, version string, f Filter, terms []string, k int) ([]Candidate, error)
	// Identifier 在给定索引版本里按过滤条件召回正文或标题里原样出现任一 ids 的 k 个块（不区分大小写），
	// 标题命中的在前（编号精确匹配，详设 6.2）。
	Identifier(ctx context.Context, version string, f Filter, ids []string, k int) ([]Candidate, error)
	// DocACL 读文档的最新权限状态；不存在的文档不在结果里。
	DocACL(ctx context.Context, docIDs []string) (map[string]ACL, error)
	// ChunkACL 读块所属文档的最新权限状态，键是 chunk_id；不存在的块不在结果里。
	ChunkACL(ctx context.Context, chunkIDs []string) (map[string]ACL, error)
	// CountDocs 统计给定索引版本里按过滤条件可见的文档数，最多数到 limit。
	CountDocs(ctx context.Context, version string, f Filter, limit int) (int64, error)
	// ListDocs 列出给定索引版本里按过滤条件可见的文档；byDate 为 true 时按生效日期倒序，否则按标题。
	ListDocs(ctx context.Context, version string, f Filter, byDate bool, offset, limit int) ([]DocSummary, error)
	// CountGroups 按 by（doc_type、dept、validity_status、year）分组计数，按数量倒序，最多 limit 组。
	CountGroups(ctx context.Context, version string, f Filter, by string, limit int) ([]Group, error)
	// Excerpt 读给定索引版本里属于 docID 的块 chunkID；查询带全部必含条件，不可见或不存在时 ok 为 false。
	Excerpt(ctx context.Context, version string, f Filter, docID, chunkID string) (c Candidate, ok bool, err error)
	// Chunks 按块 ID 取给定索引版本里的块（检索结果缓存命中时用），带全部必含条件：现在不可见的块不在结果里。
	Chunks(ctx context.Context, version string, f Filter, chunkIDs []string) (map[string]Candidate, error)
	// TenantDenied 判断租户是否在紧急拒绝名单里（governance 的紧急拒绝规则，按租户）。
	TenantDenied(ctx context.Context, tenant string) (bool, error)
	// PartitionTokens 按主体的 token（user:、group:、dept:）从分区授权表展开出可见的分区 token（part:<数据源>/<分区>，
	// 总体设计 ADR-14、详设 5.1）
	PartitionTokens(ctx context.Context, tokens []string) ([]string, error)
	// Groups 从平台身份库读用户的完整群组列表（Principal 只带 groups_ref 时用）。
	Groups(ctx context.Context, username string) ([]string, error)
	// LinkEntities 找出名称或别名出现在 text 里的实体，最多 limit 个，名称长的在前（实体链接）。
	// 实体本身不带文档信息，不需要按权限过滤；它们只作为遍历的起点。
	LinkEntities(ctx context.Context, text string, limit int) ([]Entity, error)
	// Edges 在给定索引版本里取与 entityIDs 中任一实体相连的边，按置信度降序最多 k 条，一条边的每个证据块各一行。
	// 必含条件按证据块所属文档下推：证据块对调用者不可见的边不返回，图上的权限由此生效（详设 6.2）。
	Edges(ctx context.Context, version string, f Filter, entityIDs []string, k int) ([]Edge, error)
}

// QueryVector 是一个子查询的向量：稠密向量，以及模型给出时的学习型稀疏向量（词元 ID → 权重，详设 6.2）。
type QueryVector struct {
	Dense  []float32
	Sparse map[uint32]float32
}

// Embedder 批量计算查询向量，同时返回模型版本（如 bge-m3@mock-hash-1）。稠密与稀疏向量出自同一次调用。
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([]QueryVector, string, error)
}

// Reranker 是交叉编码器精排：对每对（查询，段落）给出 0–1 的分数，同时返回模型版本。
type Reranker interface {
	Rerank(ctx context.Context, queries, passages []string) ([]float64, string, error)
}

// Policy 是已加载的策略包。
type Policy interface {
	// Decide 评估策略；Decision.Version 是给出决定的策略包版本
	Decide(ctx context.Context, in policy.Input, granted []string) (policy.Decision, error)
}

// Grants 是租户可访问的数据源（internal/grants，governance 下发）：拿不到时返回错误，请求 fail-closed。
type Grants interface {
	// Plan 是租户对这个用户生效的 pipeline（检索计划，governance 下发，灰度中按用户分流，评测请求可指定候选）；
	// 没有时为 nil，按默认计划
	Plan(ctx context.Context, tenant, user, version string) *configv1.PipelineConfig
	Sources(tenant string) (sources []string, version string, err error)
}

// Server 实现 RetrievalService。Policy 为 nil 表示策略包缺失：所有请求 fail-closed。
type Server struct {
	retrievalv1.UnimplementedRetrievalServiceServer
	Store    Store
	Embedder Embedder
	Policy   Policy
	Grants   Grants
	// Tokenize 是关键词检索的分词（libs/textseg，与入库侧相同）；为 nil 时关键词路降级
	Tokenize func(string) []string
	// Reranker 为 nil 时不精排，按融合顺序返回并标记降级
	Reranker Reranker
	// EvalMode 为 true 时接受 EvalIndexHeader 指定的索引版本（evaluation 详设 4.2）；只在评测副本上开启
	EvalMode bool
	// Disabled 是检索计划关闭的召回路（如对照评测只开稠密路）；关闭的路不执行、不算降级
	Disabled map[string]bool
	// Breakers 按召回路熔断，Admission 做入口限流与过载保护（详设 9.2）；为 nil 时不启用（单元测试）
	Breakers  *guard.Breakers
	Admission *guard.Admission
	// PII 为能看 PII 原文的调用者还原入库时掩码的原值（internal/pii）；为 nil 时一律返回掩码文本
	PII PIIRestorer
	// Local 是经备用通道收到的紧急拒绝规则（governance 详设 8.2，internal/denials），与索引库的拒绝名单同时生效；为 nil 表示没有
	Local LocalDenials
	// Results、Vectors 是检索结果与查询向量缓存（cache.go，internal/cache）；为 nil 时不缓存
	Results ResultCache
	Vectors VectorCache
	Log     *slog.Logger

	decisions decisions
}

// LocalDenials 是经备用通道（事件流的独立主题、配置卷里的规则文件）收到、验签通过的紧急拒绝规则，只增不减。
type LocalDenials interface {
	Source(id string) bool
	Doc(id string) bool
	Tenant(name string) bool
}

// PIIRestorer 解开一个块的 PII 原值并回填到掩码正文里。
type PIIRestorer interface {
	Restore(ctx context.Context, chunkID, text string, ciphertext []byte, wrapped, keyID string) (string, error)
}

var (
	errUnavailable = status.Error(codes.Unavailable, "检索暂不可用")
	errDenied      = status.Error(codes.PermissionDenied, "无权检索")
	planVersionRe  = regexp.MustCompile(`^cfv_[0-9a-f]{24}$`)
)

func (s *Server) Search(ctx context.Context, r *retrievalv1.SearchRequest) (*retrievalv1.SearchResponse, error) {
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < minBudget {
		return nil, status.Error(codes.DeadlineExceeded, "剩余时间不足")
	}
	if err := validateSearch(ctx, r); err != nil {
		return nil, err
	}
	// 只在 Search 入口准入：CheckAccess 是同一次问答返回前的复核，拒绝它只会让已经放行的问答失败
	done, err := s.admit(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	f, pv, err := s.authorize(ctx)
	if err != nil {
		return nil, err
	}
	f = f.Narrow(r.GetFilters())
	p := grpcmw.PrincipalFrom(ctx)
	plan := s.Grants.Plan(ctx, p.GetTenant(), p.GetSub(), r.GetPlanVersion())
	start := time.Now()
	indexVersion, model, err := s.liveIndex(ctx)
	if err != nil {
		return nil, err
	}
	// 检索计划关闭精排时不精排，即使请求要求（计划只能在平台能力范围内收窄，retrieval 详设 10.1）
	rp := plan.GetRetrieval()
	planRerank := rp == nil || rp.Rerank == nil || rp.GetRerank()
	doRerank := wantRerank(ctx, r) && planRerank
	// 检索结果缓存（详设 8.1）：命中时跳过召回与精排，按块 ID 带全部必含条件重新取块，再照常组装复核
	key := s.resultKey(ctx, f, indexVersion, plan.GetVersionId(), r, doRerank)
	cands, rerankModel, degraded, hit := s.cachedResult(ctx, key, f, indexVersion)
	if !hit {
		cands, degraded, err = s.recall(ctx, f, indexVersion, model, r.GetQueries(), r.GetIntent(), rp)
		if err != nil {
			return nil, err
		}
		if doRerank && len(cands) > 0 {
			var reason string
			cands, rerankModel, reason = s.rerank(ctx, r.GetQueries(), cands)
			if reason != "" {
				degraded = append(degraded, &commonv1.Degradation{Stage: "rerank", Reason: reason})
			}
		}
		// 降级的结果不缓存：下一次可能就是完整的
		if len(degraded) == 0 {
			s.storeResult(ctx, key, cands, rerankModel, start)
		}
	}
	topK := int(r.GetTopK())
	if topK == 0 {
		topK = defaultTopK
	}
	hits, err := s.assemble(ctx, f, cands, topK, r.GetQueries(), r.GetIncludeScoreDetails())
	if err != nil {
		return nil, err
	}
	return &retrievalv1.SearchResponse{
		Hits: hits, IndexVersion: indexVersion, PolicyVersion: pv, EmbeddingModel: model,
		RerankModel: rerankModel, Degraded: degraded, PlanVersion: plan.GetVersionId(),
	}, nil
}

func (s *Server) CheckAccess(ctx context.Context, r *retrievalv1.CheckAccessRequest) (*retrievalv1.CheckAccessResponse, error) {
	reasons, pv, err := s.checkIDs(ctx, "chunk_ids", r.GetChunkIds(), s.Store.ChunkACL)
	if err != nil {
		return nil, err
	}
	resp := &retrievalv1.CheckAccessResponse{CheckedAt: timestamppb.Now(), PolicyVersion: pv}
	for i, id := range r.GetChunkIds() {
		resp.Results = append(resp.Results, &retrievalv1.AccessResult{
			ChunkId: id, Allowed: reasons[i] == retrievalv1.AccessReason_ACCESS_REASON_ALLOWED, Reason: reasons[i],
		})
	}
	return resp, nil
}

// CheckDocumentAccess 按文档复核：清单类回答的列表项引用的是文档本身（K2 详设第 8 节）。
// 与 CheckAccess 只差在按 doc_id 读权限表，判定完全相同。
func (s *Server) CheckDocumentAccess(ctx context.Context, r *retrievalv1.CheckDocumentAccessRequest) (*retrievalv1.CheckDocumentAccessResponse, error) {
	reasons, pv, err := s.checkIDs(ctx, "doc_ids", r.GetDocIds(), s.Store.DocACL)
	if err != nil {
		return nil, err
	}
	resp := &retrievalv1.CheckDocumentAccessResponse{CheckedAt: timestamppb.Now(), PolicyVersion: pv}
	for i, id := range r.GetDocIds() {
		resp.Results = append(resp.Results, &retrievalv1.DocumentAccessResult{
			DocId: id, Allowed: reasons[i] == retrievalv1.AccessReason_ACCESS_REASON_ALLOWED, Reason: reasons[i],
		})
	}
	return resp, nil
}

// checkIDs 是返回前复核的公共部分：校验参数、展开主体，用 read 读这批 ID 的最新权限状态，逐个判定。
// 复核不经入口准入：它是已经放行的请求在返回前的最后一步，拒绝它只会让请求失败。
// 读不到权限表时整批拒绝（fail-closed）；不存在的 ID 判为 not_found。
func (s *Server) checkIDs(ctx context.Context, field string, ids []string,
	read func(context.Context, []string) (map[string]ACL, error)) ([]retrievalv1.AccessReason, string, error) {
	if err := validPurpose(ctx); err != nil {
		return nil, "", err
	}
	if len(ids) == 0 || len(ids) > maxCheckIDs || slices.Contains(ids, "") {
		return nil, "", status.Errorf(codes.InvalidArgument, "%s 需要 1–%d 个非空值", field, maxCheckIDs)
	}
	f, pv, err := s.authorize(ctx)
	if err != nil {
		return nil, "", err
	}
	acls, err := read(ctx, ids)
	if err != nil {
		s.Log.ErrorContext(ctx, "读取权限表失败", slog.Any("err", err))
		return nil, "", errUnavailable
	}
	reasons := make([]retrievalv1.AccessReason, len(ids))
	for i, id := range ids {
		reasons[i] = retrievalv1.AccessReason_ACCESS_REASON_NOT_FOUND
		if a, ok := acls[id]; ok {
			reasons[i] = f.Check(a)
		}
	}
	return reasons, pv, nil
}

// authorize 展开主体、评估策略并断言必含条件，返回过滤条件和策略版本。任何一步拿不到确定结果都拒绝。
func (s *Server) authorize(ctx context.Context) (Filter, string, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if p == nil {
		return Filter{}, "", status.Error(codes.Unauthenticated, "缺少 Principal")
	}
	if s.Policy == nil {
		s.Log.ErrorContext(ctx, "策略包缺失，拒绝请求")
		return Filter{}, "", errUnavailable
	}
	groups := p.GetGroups()
	if ref := p.GetGroupsRef(); ref != "" {
		// gateway 只会把本人账号作为引用；指向别人的引用一律拒绝，防止借用他人的群组
		if ref != p.GetSub() {
			s.Log.WarnContext(ctx, "groups_ref 与 sub 不一致", slog.String("sub", p.GetSub()))
			return Filter{}, "", errDenied
		}
		var err error
		if groups, err = s.Store.Groups(ctx, ref); err != nil {
			s.Log.ErrorContext(ctx, "读取平台身份库群组失败", slog.Any("err", err))
			return Filter{}, "", errUnavailable
		}
	}
	// 租户授权只来自 governance（技术设计文档 9.1）；拿不到就拒绝，不退回到任何默认值
	granted, grantsVersion, err := s.Grants.Sources(p.GetTenant())
	if err != nil {
		s.Log.ErrorContext(ctx, "租户授权不可用，拒绝请求", slog.Any("err", err))
		return Filter{}, "", errUnavailable
	}
	d, err := s.decide(ctx, policy.Input{
		Sub: p.GetSub(), Tenant: p.GetTenant(), Dept: p.GetDept(), Clearance: int32(p.GetClearance()),
		Region: p.GetRegion(), Groups: groups,
	}, granted, grantsVersion)
	if err != nil {
		s.Log.ErrorContext(ctx, "策略评估失败", slog.Any("err", err))
		return Filter{}, "", errUnavailable
	}
	if !d.Allow {
		s.Log.InfoContext(ctx, "策略拒绝", slog.String("reason", d.Reason), slog.String("tenant", p.GetTenant()))
		return Filter{}, "", errDenied
	}
	f := Filter{
		Sources: d.Filter.Sources, MaxClassification: d.Filter.MaxClassification, Tokens: d.Filter.ACLTokens,
		ShowPII: d.Obligations.Redaction == "none",
	}
	if err := f.Validate(p.GetSub(), p.GetClearance()); err != nil {
		s.Log.ErrorContext(ctx, "过滤条件未通过必含条件断言", slog.Any("err", err), slog.String("policy_version", d.Version))
		return Filter{}, "", errDenied
	}
	// 分区级数据源（ADR-14）：文档只带分区 token，按主体从分区授权表展开后与文档级 token 合并成同一个必含条件；
	// 召回、计数、列举与返回前复核都用它，分区授权一变下一个请求就按新的判定。读不到授权表时拒绝（fail-closed）
	parts, err := s.Store.PartitionTokens(ctx, f.Tokens)
	if err != nil {
		s.Log.ErrorContext(ctx, "读取分区授权失败", slog.Any("err", err))
		return Filter{}, "", errUnavailable
	}
	f.Tokens = append(slices.Clone(f.Tokens), parts...)
	if len(f.Tokens) > MaxTokens {
		s.Log.ErrorContext(ctx, "展开分区后 token 超出上限", slog.Int("tokens", len(f.Tokens)))
		return Filter{}, "", errDenied
	}
	// 租户被紧急拒绝：可检索的数据源清空。召回与返回前复核都经这里，两处同时生效；读不到名单时拒绝（fail-closed）
	denied, err := s.Store.TenantDenied(ctx, p.GetTenant())
	if err != nil {
		s.Log.ErrorContext(ctx, "读取紧急拒绝名单失败", slog.Any("err", err))
		return Filter{}, "", errUnavailable
	}
	if denied || s.Local != nil && s.Local.Tenant(p.GetTenant()) {
		s.Log.WarnContext(ctx, "租户在紧急拒绝名单里", slog.String("tenant", p.GetTenant()))
		f.Sources = nil
	}
	if s.Local != nil {
		// 备用通道按数据源的拒绝：从可检索的数据源里去掉，召回、计数、列举与返回前复核都不再包含它
		f.Sources = slices.DeleteFunc(slices.Clone(f.Sources), s.Local.Source)
		f.DeniedDoc = s.Local.Doc
	}
	// 授权快照的版本也影响能看到什么，与策略包版本一起写进 policy_version（进入 why 与审计）
	// 策略包版本取给出这次决定的那个包（灰度时可能是候选包）
	return f, d.Version + "+" + grantsVersion, nil
}

// route 是一路召回在各子查询上的结果（lists[q] 对应第 q 个子查询，未执行为 nil）；
// reason 非空表示这一路被跳过或失败（写入 degraded）。
type route struct {
	name   string
	lists  [][]Candidate
	reason string
	err    error
	// idle 表示这一路对本次请求无事可做（如图召回在问题里没有链接到任何实体）：既不算成功也不算降级，
	// 否则另外两路都失败时会把“没有召回”当成“没有证据”返回
	idle bool
}

// recall 并发执行稠密向量、关键词、图与编号精确匹配四路召回，按加权 RRF 融合（详设第 4 节第 6–8 步）；
// 权重随意图变化（routeWeight），编号精确匹配命中的块置顶（pinFirst）。
// 单路失败只降级、不影响其他路；全部失败才返回 UNAVAILABLE（详设 9.4）。
func (s *Server) recall(ctx context.Context, f Filter, version, model string, queries []*retrievalv1.SubQuery,
	intent retrievalv1.SearchIntent, plan *configv1.RetrievalPlan) ([]scored, []*commonv1.Degradation, error) {
	// 部署时关闭的（Disabled）与租户检索计划关闭的召回路都不执行、不算降级
	off := func(route string) bool { return s.Disabled[route] || slices.Contains(plan.GetDisabledRoutes(), route) }
	if len(f.Sources) == 0 {
		// 没有可检索的数据源：结果必然为空，不必访问后端
		return nil, nil, nil
	}
	routes := make([]route, 0, 5)
	results := make([]route, 5)
	vectors := s.lazyVectors(ctx, model, queries)
	var wg sync.WaitGroup
	if !off("dense") {
		wg.Go(func() {
			results[0] = s.guarded("dense", func() route { return s.dense(ctx, version, model, f, vectors) })
		})
	}
	if !off("sparse") {
		wg.Go(func() {
			results[4] = s.guarded("sparse", func() route { return s.sparse(ctx, version, model, f, vectors) })
		})
	}
	if !off("keyword") {
		wg.Go(func() {
			results[1] = s.guarded("keyword", func() route { return s.keyword(ctx, version, f, queries) })
		})
	}
	if !off("graph") {
		wg.Go(func() {
			results[2] = s.guarded("graph", func() route { return s.graph(ctx, version, f, queries) })
		})
	}
	if !off("identifier") {
		wg.Go(func() {
			results[3] = s.guarded("identifier", func() route { return s.identifier(ctx, version, f, queries) })
		})
	}
	wg.Wait()
	for _, r := range results {
		if r.name != "" && !r.idle {
			routes = append(routes, r)
		}
	}
	perQuery := make([][]ranking, len(queries))
	var degraded []*commonv1.Degradation
	for _, r := range routes {
		if r.reason == "" {
			for q, list := range r.lists {
				perQuery[q] = append(perQuery[q], ranking{route: r.name, list: list, weight: routeWeight(r.name, intent, plan)})
			}
			continue
		}
		s.Log.WarnContext(ctx, "召回路降级", slog.String("route", r.name), slog.String("reason", r.reason), slog.Any("err", r.err))
		degraded = append(degraded, &commonv1.Degradation{Stage: r.name, Reason: r.reason})
	}
	if len(degraded) == len(routes) {
		s.Log.ErrorContext(ctx, "召回不可用", slog.String("reason", "all_routes_failed"))
		return nil, nil, errUnavailable
	}
	return pinFirst(fuse(perQuery)), degraded, nil
}

// liveIndex 取这次检索用的索引版本与嵌入模型（评测副本可指定版本）；拿不到时按不可用处理。
func (s *Server) liveIndex(ctx context.Context) (string, string, error) {
	version, model, err := s.indexVersion(ctx)
	if errors.Is(err, ErrNoLiveIndex) {
		s.Log.ErrorContext(ctx, "召回不可用", slog.String("reason", "no_live_index"))
		return "", "", errUnavailable
	}
	if _, ok := status.FromError(err); ok && err != nil {
		return "", "", err
	}
	if err != nil {
		s.Log.ErrorContext(ctx, "召回不可用", slog.String("reason", "metadata_unavailable"), slog.Any("err", err))
		return "", "", errUnavailable
	}
	return version, model, nil
}

// guarded 经熔断器执行一路召回：熔断打开时不调用后端，直接标记 circuit_open（详设 9.4）。
// 出错（含超时）的调用计为失败；没有出错但被跳过的（如 model_mismatch）不计。
func (s *Server) guarded(name string, run func() route) route {
	if s.Breakers == nil {
		return run()
	}
	if !s.Breakers.Allow(name) {
		return route{name: name, reason: "circuit_open"}
	}
	start := time.Now()
	r := run()
	s.Breakers.Record(name, r.err != nil, time.Since(start))
	return r
}

// admit 做入口准入：按租户限流、按调用目的分级拒绝。拒绝时返回 RESOURCE_EXHAUSTED，调用方按退避重试（详设 9.3）。
func (s *Server) admit(ctx context.Context) (func(), error) {
	p := grpcmw.PrincipalFrom(ctx)
	if s.Admission == nil || p == nil {
		// 没有 Principal 的请求由 authorize 拒绝
		return func() {}, nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	done, reject := s.Admission.Enter(p.GetTenant(), md.Get("x-call-purpose")[0])
	if reject != "" {
		s.Log.WarnContext(ctx, "入口拒绝", slog.String("reason", string(reject)), slog.String("tenant", p.GetTenant()))
		return nil, status.Error(codes.ResourceExhausted, string(reject))
	}
	return done, nil
}

// queryVectors 是一次检索的查询向量：稠密与稀疏两路共用一次嵌入调用（详设 6.2），哪一路先用到就由它触发。
type queryVectors func() ([]QueryVector, string, error)

func (s *Server) lazyVectors(ctx context.Context, model string, queries []*retrievalv1.SubQuery) queryVectors {
	texts := make([]string, len(queries))
	for i, q := range queries {
		texts[i] = q.GetText()
	}
	type result struct {
		vecs  []QueryVector
		model string
		err   error
	}
	once := sync.OnceValue(func() result {
		vecs, qModel, err := s.embed(ctx, model, texts)
		return result{vecs, qModel, err}
	})
	return func() ([]QueryVector, string, error) {
		r := once()
		return r.vecs, r.model, r.err
	}
}

// vectorsFor 取查询向量并核对模型版本；不能用时返回降级原因。
func vectorsFor(vectors queryVectors, model string) ([]QueryVector, string, error) {
	vecs, qModel, err := vectors()
	switch {
	case err != nil:
		return nil, "embed_unavailable", err
	case qModel != model:
		// 用不同模型的向量去查只会得到错配结果（详设 6.2）
		return nil, "model_mismatch", nil
	}
	return vecs, "", nil
}

func (s *Server) dense(ctx context.Context, version, model string, f Filter, vectors queryVectors) route {
	r := route{name: "dense"}
	vecs, reason, err := vectorsFor(vectors, model)
	if reason != "" {
		r.reason, r.err = reason, err
		return r
	}
	r.lists = make([][]Candidate, len(vecs))
	for q, v := range vecs {
		list, err := s.Store.Dense(ctx, version, f, v.Dense, recallK)
		if err != nil {
			r.reason, r.err = "backend_error", err
			return r
		}
		r.lists[q] = list
	}
	return r
}

// sparse 是学习型稀疏向量召回路（详设 6.2）：按内积排序。嵌入模型不给稀疏向量时降级 sparse_unavailable
// （部署时显式开启了这一路，却没接上能给稀疏向量的模型，要让人看得见）。
func (s *Server) sparse(ctx context.Context, version, model string, f Filter, vectors queryVectors) route {
	r := route{name: "sparse"}
	vecs, reason, err := vectorsFor(vectors, model)
	if reason != "" {
		r.reason, r.err = reason, err
		return r
	}
	r.lists = make([][]Candidate, len(vecs))
	for q, v := range vecs {
		if v.Sparse == nil {
			r.reason = "sparse_unavailable"
			return r
		}
		if len(v.Sparse) == 0 {
			continue
		}
		list, err := s.Store.Sparse(ctx, version, f, v.Sparse, recallK)
		if err != nil {
			r.reason, r.err = "backend_error", err
			return r
		}
		r.lists[q] = list
	}
	return r
}

func (s *Server) keyword(ctx context.Context, version string, f Filter, queries []*retrievalv1.SubQuery) route {
	r := route{name: "keyword"}
	if s.Tokenize == nil {
		r.reason = "segmenter_unavailable"
		return r
	}
	r.lists = make([][]Candidate, len(queries))
	for i, q := range queries {
		terms := uniq(s.Tokenize(q.GetText()))
		if len(terms) == 0 {
			// 子查询里没有可检索的词（如全是标点）：这一条不走关键词路，不算失败
			continue
		}
		list, err := s.Store.Keyword(ctx, version, f, terms, recallK)
		if err != nil {
			r.reason, r.err = "backend_error", err
			return r
		}
		r.lists[i] = list
	}
	return r
}

func uniq(xs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

// assemble 用权限表的最新状态再过滤一次，再截取 top_k：覆盖“召回之后权限才变化”的窗口（详设第 4 节第 10 步）。
// 先复核再截取，被拦下的块不会让结果少于 top_k。
func (s *Server) assemble(ctx context.Context, f Filter, cands []scored, topK int,
	queries []*retrievalv1.SubQuery, details bool) ([]*retrievalv1.Hit, error) {
	if len(cands) == 0 {
		return []*retrievalv1.Hit{}, nil
	}
	var docIDs []string
	for _, c := range cands {
		for _, d := range append([]string{c.DocID}, c.PathDocs...) {
			if !slices.Contains(docIDs, d) {
				docIDs = append(docIDs, d)
			}
		}
	}
	acls, err := s.Store.DocACL(ctx, docIDs)
	if err != nil {
		s.Log.ErrorContext(ctx, "组装复核读取权限表失败", slog.Any("err", err))
		return nil, errUnavailable
	}
	hits := []*retrievalv1.Hit{}
	for _, c := range cands {
		if !visible(f, acls, c.DocID) {
			continue
		}
		// 图路径上任何一条边的证据在召回之后变得不可见，整块不返回：路径本身会泄露那份文档里的关系（详设 13 节用例 4）
		if slices.ContainsFunc(c.PathDocs, func(d string) bool { return !visible(f, acls, d) }) {
			continue
		}
		a := acls[c.DocID]
		hits = append(hits, &retrievalv1.Hit{
			ChunkId: c.ChunkID, DocId: c.DocID, DocVersion: c.DocVersion, ParentChunkId: c.ParentChunkID,
			Text: s.text(ctx, f, c.Candidate), Span: c.Span, Title: c.Title, HeadingPath: c.HeadingPath, SourceUri: c.SourceURI,
			SourceType: c.SourceType, Owner: c.Owner, Classification: commonv1.Classification(a.Classification), Score: c.score,
			GraphPath: graphPath(f, c.GraphPath), InjectionFlag: c.InjectionFlag, MatchedBy: matchedBy(c, queries),
		})
		if details {
			hits[len(hits)-1].ScoreDetails = scoreDetails(c)
		}
		if len(hits) == topK {
			break
		}
	}
	return hits, nil
}

// matchedBy 按召回路、子查询排序，结果可复现。
func matchedBy(c scored, queries []*retrievalv1.SubQuery) []*retrievalv1.RouteMatch {
	ms := slices.Clone(c.matches)
	slices.SortFunc(ms, func(a, b match) int {
		return cmp.Or(cmp.Compare(a.route, b.route), cmp.Compare(a.query, b.query), cmp.Compare(a.rank, b.rank))
	})
	out := make([]*retrievalv1.RouteMatch, 0, len(ms))
	for _, m := range ms {
		out = append(out, &retrievalv1.RouteMatch{Route: m.route, QueryId: queries[m.query].GetId(), Rank: m.rank})
	}
	return out
}

func scoreDetails(c scored) *retrievalv1.ScoreDetails {
	d := &retrievalv1.ScoreDetails{Rrf: c.rrf, Rerank: c.rerank}
	opt := func(route string) *float64 {
		if v, ok := c.routeScores[route]; ok {
			return &v
		}
		return nil
	}
	d.Keyword, d.Dense, d.Graph, d.Identifier, d.Sparse = opt("keyword"), opt("dense"), opt("graph"), opt("identifier"), opt("sparse")
	return d
}

func visible(f Filter, acls map[string]ACL, docID string) bool {
	a, ok := acls[docID]
	return ok && f.Check(a) == retrievalv1.AccessReason_ACCESS_REASON_ALLOWED
}

// text 按策略义务处理正文（技术设计文档 8.2、8.6）。索引里只有等长掩码的正文：
// 默认再按查询侧规则掩码一次（第二道防线，防入库漏识别）；义务允许看原文时解开入库时加密的原值并回填，
// 解不开（密钥服务不可用、密文损坏）就返回掩码正文，不阻断检索。
// maskUnlessShown 处理没有加密原值的短文本（如图路径上的实体名）：义务不允许看原文时掩码。
func maskUnlessShown(f Filter, s string) string {
	if f.ShowPII {
		return s
	}
	return redact.Mask(s)
}

func (s *Server) text(ctx context.Context, f Filter, c Candidate) string {
	if !f.ShowPII {
		return redact.Mask(c.Text)
	}
	if len(c.PII.Ciphertext) == 0 || s.PII == nil {
		return c.Text
	}
	out, err := s.PII.Restore(ctx, c.ChunkID, c.Text, c.PII.Ciphertext, c.PII.WrappedKey, c.PII.KeyID)
	if err != nil {
		s.Log.WarnContext(ctx, "还原 PII 原值失败，返回掩码正文", slog.String("chunk_id", c.ChunkID), slog.Any("err", err))
		return c.Text
	}
	return out
}

var purposes = []string{"answer", "search", "agent_round", "mcp", "batch"}

// rerankBudget 是留给精排的最少剩余时间：不足时按融合顺序返回（详设 9.1 第 4 条）。
const rerankBudget = 300 * time.Millisecond

// wantRerank 取请求里的 rerank；不设时 answer、agent_round 精排，search、mcp 不精排（详设 3.2）。
// batch 只来自 orchestrator 为批量条目生成回答，与 answer 一样精排：批量回答的质量不能低于同步问答。
func wantRerank(ctx context.Context, r *retrievalv1.SearchRequest) bool {
	if r.Rerank != nil {
		return r.GetRerank()
	}
	md, _ := metadata.FromIncomingContext(ctx)
	p := md.Get("x-call-purpose")
	return len(p) == 1 && (p[0] == "answer" || p[0] == "agent_round" || p[0] == "batch")
}

// rerank 把候选（最多 poolSize 个）与命中它的子查询成对送交叉编码器，按精排分重新排序（详设 7.4）。
// 不可用或时间不足时保留融合顺序，返回降级原因；精排分写进 score。
func (s *Server) rerank(ctx context.Context, queries []*retrievalv1.SubQuery, cands []scored) ([]scored, string, string) {
	if s.Reranker == nil {
		return cands, "", "unavailable"
	}
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < rerankBudget {
		return cands, "", "skipped_deadline"
	}
	qs := make([]string, len(cands))
	ps := make([]string, len(cands))
	for i, c := range cands {
		qs[i] = queries[c.query].GetText()
		// 输入是“标题路径 + 正文”，截断由模型服务按最大长度处理
		ps[i] = strings.Join(append(slices.Clone(c.HeadingPath), c.Text), "\n")
	}
	scores, model, err := s.Reranker.Rerank(ctx, qs, ps)
	if err != nil || len(scores) != len(cands) {
		s.Log.WarnContext(ctx, "精排不可用，按融合顺序返回", slog.Any("err", err))
		return cands, "", "unavailable"
	}
	out := slices.Clone(cands)
	for i := range out {
		out[i].score = scores[i]
		out[i].rerank = &scores[i]
		// 精排分过低的编号命中块不再置顶（详设 7.1）
		out[i].pinned = out[i].pinned && scores[i] >= pinFloor
	}
	slices.SortStableFunc(out, func(a, b scored) int {
		if a.pinned != b.pinned {
			if a.pinned {
				return -1
			}
			return 1
		}
		if c := cmp.Compare(b.score, a.score); c != 0 {
			return c
		}
		return cmp.Compare(a.ChunkID, b.ChunkID)
	})
	return out, model, ""
}

func validPurpose(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("x-call-purpose")
	if len(v) != 1 || !slices.Contains(purposes, v[0]) {
		return status.Error(codes.InvalidArgument, "x-call-purpose 必须是 answer、search、agent_round、mcp、batch 之一")
	}
	return nil
}

func validateSearch(ctx context.Context, r *retrievalv1.SearchRequest) error {
	if err := validPurpose(ctx); err != nil {
		return err
	}
	if err := ValidateMeta(r.GetFilters()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	qs := r.GetQueries()
	if len(qs) == 0 || len(qs) > maxQueries {
		return status.Errorf(codes.InvalidArgument, "需要 1–%d 个子查询", maxQueries)
	}
	for _, q := range qs {
		if strings.TrimSpace(q.GetText()) == "" || utf8.RuneCountInString(q.GetText()) > maxQueryLen {
			return status.Errorf(codes.InvalidArgument, "子查询不能为空，且不超过 %d 字", maxQueryLen)
		}
	}
	if r.GetTopK() < 0 || r.GetTopK() > maxTopK {
		return status.Errorf(codes.InvalidArgument, "top_k 需在 1–%d 之间", maxTopK)
	}
	if v := r.GetPlanVersion(); v != "" && !planVersionRe.MatchString(v) {
		return status.Error(codes.InvalidArgument, "plan_version 须为 cfv_ 版本")
	}
	return nil
}
