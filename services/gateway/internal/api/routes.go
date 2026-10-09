package api

import (
	"context"
	"net/http"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

	answerv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/admission"
)

// 对外 JSON 结构，与 contracts/openapi/v1/openapi.yaml 一致：字段 snake_case，枚举值为 proto 枚举名去掉前缀后的小写形式。

type filtersJSON struct {
	SourceIDs        []string `json:"source_ids,omitempty"`
	DocTypes         []string `json:"doc_types,omitempty"`
	Depts            []string `json:"depts,omitempty"`
	TopicCodes       []string `json:"topic_codes,omitempty"`
	EffectiveFrom    string   `json:"effective_from,omitempty"`
	EffectiveTo      string   `json:"effective_to,omitempty"`
	ValidityStatuses []string `json:"validity_statuses,omitempty"`
}

// 对外的效力状态写法（OpenAPI）与契约枚举的对应
var validityNames = map[string]commonv1.ValidityStatus{
	"effective": commonv1.ValidityStatus_VALIDITY_STATUS_EFFECTIVE,
	"pending":   commonv1.ValidityStatus_VALIDITY_STATUS_PENDING,
	"repealed":  commonv1.ValidityStatus_VALIDITY_STATUS_REPEALED,
}

// proto 转成契约里的过滤条件；不合法时返回 nil（调用方按参数错误处理）。日期格式由 retrieval 校验。
func (f *filtersJSON) proto() *commonv1.Filters {
	if f == nil {
		return nil
	}
	for _, id := range f.SourceIDs {
		if !strings.HasPrefix(id, "src_") {
			return nil
		}
	}
	out := &commonv1.Filters{
		SourceIds: f.SourceIDs, DocTypes: f.DocTypes, Depts: f.Depts, TopicCodes: f.TopicCodes,
		EffectiveFrom: f.EffectiveFrom, EffectiveTo: f.EffectiveTo,
	}
	for _, v := range f.ValidityStatuses {
		st, ok := validityNames[v]
		if !ok {
			return nil
		}
		out.ValidityStatuses = append(out.ValidityStatuses, st)
	}
	return out
}

func filtersOf(f *commonv1.Filters) *filtersJSON {
	if f == nil {
		return nil
	}
	out := &filtersJSON{
		SourceIDs: f.GetSourceIds(), DocTypes: f.GetDocTypes(), Depts: f.GetDepts(), TopicCodes: f.GetTopicCodes(),
		EffectiveFrom: f.GetEffectiveFrom(), EffectiveTo: f.GetEffectiveTo(),
	}
	for _, v := range f.GetValidityStatuses() {
		out.ValidityStatuses = append(out.ValidityStatuses, enumName(v))
	}
	return out
}

type spanJSON struct {
	CharStart int32  `json:"char_start"`
	CharEnd   int32  `json:"char_end"`
	LineStart *int32 `json:"line_start,omitempty"`
	LineEnd   *int32 `json:"line_end,omitempty"`
	Page      *int32 `json:"page,omitempty"`
	// PDF、扫描件页面上的区域（点，原点在左上角），前端据此在原文上高亮
	Bbox *bboxJSON `json:"bbox,omitempty"`
}

type bboxJSON struct {
	X0 float32 `json:"x0"`
	Y0 float32 `json:"y0"`
	X1 float32 `json:"x1"`
	Y1 float32 `json:"y1"`
}

func spanOf(s *commonv1.Span) spanJSON {
	// 回放受限级回答时引用只留标识、不带原文位置
	if s == nil {
		return spanJSON{}
	}
	out := spanJSON{CharStart: s.GetCharStart(), CharEnd: s.GetCharEnd(), LineStart: s.LineStart, LineEnd: s.LineEnd, Page: s.Page}
	if b := s.GetBbox(); b != nil {
		out.Bbox = &bboxJSON{b.GetX0(), b.GetY0(), b.GetX1(), b.GetY1()}
	}
	return out
}

type degradationJSON struct {
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
}

func degradedOf(ds []*commonv1.Degradation) []degradationJSON {
	out := []degradationJSON{}
	for _, d := range ds {
		out = append(out, degradationJSON{d.GetStage(), d.GetReason()})
	}
	return out
}

// enumName 把 STATUS_ANSWERED 这类枚举值转成对外的 answered。
func enumName(e protoreflect.Enum) string {
	full := string(e.Descriptor().Values().ByNumber(e.Number()).Name())
	prefix := string(e.Descriptor().Values().ByNumber(0).Name()) // XXX_UNSPECIFIED
	return strings.ToLower(strings.TrimPrefix(full, strings.TrimSuffix(prefix, "UNSPECIFIED")))
}

// ---- POST /v1/search ----

type searchRequest struct {
	Question string       `json:"question"`
	Filters  *filtersJSON `json:"filters,omitempty"`
	TopK     *int32       `json:"top_k,omitempty"`
}

type hitJSON struct {
	ChunkID        string   `json:"chunk_id"`
	DocID          string   `json:"doc_id"`
	DocVersion     int64    `json:"doc_version"`
	Title          string   `json:"title"`
	Text           string   `json:"text"`
	Span           spanJSON `json:"span"`
	Score          float64  `json:"score"`
	SourceURI      string   `json:"source_uri"`
	SourceType     string   `json:"source_type"`
	Owner          string   `json:"owner,omitempty"`
	Classification string   `json:"classification"`
	// 只有图召回找到的块才有路径
	GraphPath []graphEdgeJSON `json:"graph_path,omitempty"`
	// 入库时检测到可疑指令（技术设计文档 8.5）：只在为真时输出
	InjectionFlag bool `json:"injection_flag,omitempty"`
}

type graphEdgeJSON struct {
	FromEntity   string `json:"from_entity"`
	FromType     string `json:"from_type"`
	RelationType string `json:"relation_type"`
	ToEntity     string `json:"to_entity"`
	ToType       string `json:"to_type"`
}

// edge 是回答契约与检索契约里图路径上一条边的共同读法：两个契约各自定义了同样字段的 GraphEdge。
type edge interface {
	GetFromEntity() string
	GetFromType() commonv1.EntityType
	GetRelationType() commonv1.RelationType
	GetToEntity() string
	GetToType() commonv1.EntityType
}

func graphEdgesOf[E edge](es []E) []graphEdgeJSON {
	var out []graphEdgeJSON
	for _, e := range es {
		out = append(out, graphEdgeJSON{e.GetFromEntity(), enumName(e.GetFromType()), enumName(e.GetRelationType()),
			e.GetToEntity(), enumName(e.GetToType())})
	}
	return out
}

type searchResult struct {
	TraceID      string            `json:"trace_id"`
	IndexVersion string            `json:"index_version"`
	Degraded     []degradationJSON `json:"degraded"`
	Hits         []hitJSON         `json:"hits"`
}

func (s *Server) search(w http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, _, err := s.authorize(r, c, access{priority: admission.PrioritySearch})
	if err != nil {
		return nil, err
	}
	var req searchRequest
	if err := decode(r, &req); err != nil {
		return nil, err
	}
	c.question = req.Question
	if err := validQuestion(req.Question); err != nil {
		return nil, err
	}
	topK := int32(20)
	if req.TopK != nil {
		topK = *req.TopK
	}
	filters := req.Filters.proto()
	if topK < 1 || topK > 50 || (req.Filters != nil && filters == nil) {
		return nil, problemInvalid
	}
	return s.runSearch(c, principal, "search", req.Question, filters, topK)
}

// runSearch 调 retrieval.Search 并转成对外结构；/v1/search 与 MCP 的 search_documents 共用，只是调用目的不同。
func (s *Server) runSearch(c *call, principal, purpose, question string, filters *commonv1.Filters, topK int32) (searchResult, error) {
	ctx, cancel := downstream(c.ctx, principal, purpose, "", searchDeadline)
	defer cancel()
	resp, err := s.Retrieval.Search(ctx, &retrievalv1.SearchRequest{
		Queries: []*retrievalv1.SubQuery{{Id: "q1", Text: question}}, Filters: filters, TopK: topK,
	})
	if err != nil {
		return searchResult{}, fromGRPC(err)
	}
	out := searchResult{TraceID: c.traceID, IndexVersion: resp.GetIndexVersion(), Degraded: degradedOf(resp.GetDegraded()), Hits: []hitJSON{}}
	for _, h := range resp.GetHits() {
		out.Hits = append(out.Hits, hitJSON{
			ChunkID: h.GetChunkId(), DocID: h.GetDocId(), DocVersion: h.GetDocVersion(), Title: h.GetTitle(),
			Text: h.GetText(), Span: spanOf(h.GetSpan()), Score: h.GetScore(), SourceURI: h.GetSourceUri(),
			SourceType: enumName(h.GetSourceType()), Owner: h.GetOwner(), Classification: enumName(h.GetClassification()),
			GraphPath: graphEdgesOf(h.GetGraphPath()), InjectionFlag: h.GetInjectionFlag(),
		})
	}
	return out, nil
}

// ---- POST /v1/answers ----

type answerRequest struct {
	Question     string       `json:"question"`
	Filters      *filtersJSON `json:"filters,omitempty"`
	MaxCitations *int32       `json:"max_citations,omitempty"`
	// true 时以 SSE 按句返回（技术设计文档 13.9、13.11：用 stream 参数开关，不单独设流式接口）
	Stream bool `json:"stream,omitempty"`
	// 多轮时带上一轮返回的会话 ID；会话归属由 orchestrator 校验（orchestrator 详设 10.4）
	ConversationID string `json:"conversation_id,omitempty"`
}

// 与 OpenAPI 的 conversation_id 格式一致；其余格式在边界处拒绝，不往下游传
var conversationIDRe = regexp.MustCompile(`^conv_[A-Za-z0-9]{1,64}$`)

type sentenceJSON struct {
	Text      string  `json:"text"`
	Start     int32   `json:"start"`
	End       int32   `json:"end"`
	ClaimType string  `json:"claim_type"`
	RefIDs    []int32 `json:"ref_ids"`
	Support   float64 `json:"support"`
	BlockID   string  `json:"block_id"`
	// 只有计算句带算式
	Calc *calcJSON `json:"calc,omitempty"`
	// 只有列表项带所列的文档
	Document *listedDocJSON `json:"document,omitempty"`
}

type listedDocJSON struct {
	DocID          string   `json:"doc_id"`
	DocVersion     int64    `json:"doc_version"`
	Title          string   `json:"title"`
	SourceURI      string   `json:"source_uri"`
	DocType        string   `json:"doc_type,omitempty"`
	Dept           string   `json:"dept,omitempty"`
	TopicCodes     []string `json:"topic_codes,omitempty"`
	EffectiveDate  string   `json:"effective_date,omitempty"`
	ValidityStatus string   `json:"validity_status,omitempty"`
}

type scopeJSON struct {
	FiltersText  string `json:"filters_text"`
	TotalCount   *int64 `json:"total_count,omitempty"`
	TotalIsExact bool   `json:"total_is_exact"`
	Shown        int32  `json:"shown"`
	MetadataGaps string `json:"metadata_gaps,omitempty"`
	Basis        string `json:"basis"`
}

type calcJSON struct {
	Inputs  []calcInputJSON  `json:"inputs"`
	Results []calcResultJSON `json:"results"`
}

type calcInputJSON struct {
	Var   string `json:"var"`
	RefID int32  `json:"ref_id"`
	Value string `json:"value"`
	Unit  string `json:"unit"`
}

type calcResultJSON struct {
	Name  string `json:"name"`
	Expr  string `json:"expr"`
	Value string `json:"value"`
}

type blockJSON struct {
	BlockID         string     `json:"block_id"`
	BlockType       string     `json:"block_type"`
	ParentBlockID   string     `json:"parent_block_id,omitempty"`
	Order           int32      `json:"order"`
	SentenceIndexes []int32    `json:"sentence_indexes"`
	Scope           *scopeJSON `json:"scope,omitempty"`
}

type citationJSON struct {
	RefID          int32    `json:"ref_id"`
	ChunkID        string   `json:"chunk_id"`
	DocID          string   `json:"doc_id"`
	DocVersion     int64    `json:"doc_version"`
	Title          string   `json:"title"`
	Quote          string   `json:"quote"`
	Span           spanJSON `json:"span"`
	SourceURI      string   `json:"source_uri"`
	Owner          string   `json:"owner,omitempty"`
	Classification string   `json:"classification"`
	SourceType     string   `json:"source_type"`
	// 所引内容入库时检测到可疑指令；它不会是任何一句的唯一依据
	InjectionFlag bool `json:"injection_flag,omitempty"`
}

type whyJSON struct {
	Retrievers        []string         `json:"retrievers"`
	IndexVersion      string           `json:"index_version"`
	PolicyVersion     string           `json:"policy_version"`
	EmbeddingModel    string           `json:"embedding_model"`
	GenerationModel   string           `json:"generation_model"`
	PromptVersion     string           `json:"prompt_version"`
	NLIModel          string           `json:"nli_model"`
	SupportThreshold  float64          `json:"support_threshold"`
	RerankModel       string           `json:"rerank_model"`
	CalculatorVersion string           `json:"calculator_version"`
	InferredFilters   *filtersJSON     `json:"inferred_filters,omitempty"`
	Intent            string           `json:"intent,omitempty"`
	AgentRounds       []agentRoundJSON `json:"agent_rounds,omitempty"`
	GraphPath         []graphPathJSON  `json:"graph_path,omitempty"`
	// 主题汇总：结论基于文档摘要归纳（K2 详设第 6 节）
	BasedOnSummaries bool `json:"based_on_summaries,omitempty"`
	// A/B 实验与分组（orchestrator 详设 12.3）；租户没有运行中的实验时不输出
	ExperimentID  string            `json:"experiment_id,omitempty"`
	ExperimentArm string            `json:"experiment_arm,omitempty"`
	Degraded      []degradationJSON `json:"degraded"`
}

type graphPathJSON struct {
	RefID   int32           `json:"ref_id"`
	ChunkID string          `json:"chunk_id"`
	Edges   []graphEdgeJSON `json:"edges"`
}

type agentRoundJSON struct {
	Round int32           `json:"round"`
	Calls []agentCallJSON `json:"calls"`
}

type agentCallJSON struct {
	SubQuestionID string   `json:"sub_question_id"`
	SubQuestion   string   `json:"sub_question"`
	Tool          string   `json:"tool"`
	ChunkIDs      []string `json:"chunk_ids"`
	Covered       bool     `json:"covered"`
	ElapsedMS     int32    `json:"elapsed_ms"`
}

type answerResult struct {
	AnswerID  string         `json:"answer_id"`
	TraceID   string         `json:"trace_id"`
	Status    string         `json:"status"`
	Answer    string         `json:"answer"`
	Sentences []sentenceJSON `json:"sentences"`
	Blocks    []blockJSON    `json:"blocks"`
	Citations []citationJSON `json:"citations"`
	Why       whyJSON        `json:"why"`
	// fact / enumeration / count / summary（K2 详设第 7 节）
	AnswerKind     string `json:"answer_kind"`
	ConversationID string `json:"conversation_id,omitempty"`
	// 回答整体的置信度（技术设计文档 7.8）；拒答、仅证据与清单类回答没有
	Confidence *confidenceJSON `json:"confidence,omitempty"`
}

type confidenceJSON struct {
	Score           float64 `json:"score"`
	Level           string  `json:"level"`
	WeightsVersion  string  `json:"weights_version"`
	Coverage        float64 `json:"coverage"`
	MeanSupport     float64 `json:"mean_support"`
	TopRerank       float64 `json:"top_rerank"`
	SourceDiversity float64 `json:"source_diversity"`
	Conflict        bool    `json:"conflict"`
}

func confidenceOf(c *answerv1.Confidence) *confidenceJSON {
	if c == nil {
		return nil
	}
	return &confidenceJSON{Score: c.GetScore(), Level: enumName(c.GetLevel()), WeightsVersion: c.GetWeightsVersion(),
		Coverage: c.GetCoverage(), MeanSupport: c.GetMeanSupport(), TopRerank: c.GetTopRerank(),
		SourceDiversity: c.GetSourceDiversity(), Conflict: c.GetConflict()}
}

func (s *Server) answers(w http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, _, err := s.authorize(r, c, access{answers: true, priority: admission.PriorityAnswer})
	if err != nil {
		return nil, err
	}
	var req answerRequest
	if err := decode(r, &req); err != nil {
		return nil, err
	}
	c.question = req.Question
	if err := validQuestion(req.Question); err != nil {
		return nil, err
	}
	maxCitations := int32(8)
	if req.MaxCitations != nil {
		maxCitations = *req.MaxCitations
	}
	filters := req.Filters.proto()
	if maxCitations < 1 || maxCitations > 20 || (req.Filters != nil && filters == nil) ||
		(req.ConversationID != "" && !conversationIDRe.MatchString(req.ConversationID)) {
		return nil, problemInvalid
	}
	if req.Stream {
		if err := s.acquireStream(c); err != nil {
			return nil, err
		}
	}
	// answer_id 由 gateway 生成（gateway 详设 1.1），经 metadata 交给 orchestrator 关联审计
	c.answerID = newAnswerID()
	ctx, cancel := downstream(c.ctx, principal, "answer", c.answerID, answerDeadline)
	defer cancel()
	areq := &answerv1.AnswerRequest{Question: req.Question, Filters: filters, MaxCitations: maxCitations, ConversationId: req.ConversationID}
	if req.Stream {
		return nil, s.streamAnswer(ctx, w, c, areq, namedEvent)
	}
	resp, err := s.answer(ctx, c, areq)
	if err != nil {
		return nil, err
	}
	return answerJSON(c, resp), nil
}

// answer 是一元问答调用：/v1/answers、兼容端点与 MCP 的 ask 共用。记下审计块与用量，并把耗时交给全局自适应并发。
func (s *Server) answer(ctx context.Context, c *call, req *answerv1.AnswerRequest) (*answerv1.AnswerResponse, error) {
	c.orchStart = s.Now()
	resp, err := s.Orchestrator.Answer(ctx, req)
	if err != nil {
		return nil, s.orchestratorErr(c, err)
	}
	s.observe(c, resp.GetWhy().GetIntent())
	c.blocks, c.usage = resp.GetAudit(), resp.GetUsage()
	return resp, nil
}

// answerKind 未设置时按普通事实问答（旧版 orchestrator 不填这个字段）
func answerKind(k answerv1.AnswerKind) string {
	if k == answerv1.AnswerKind_ANSWER_KIND_UNSPECIFIED {
		return "fact"
	}
	return enumName(k)
}

func answerJSON(c *call, resp *answerv1.AnswerResponse) answerResult {
	out := answerResult{
		AnswerID: c.answerID, TraceID: c.traceID, Status: enumName(resp.GetStatus()), Answer: resp.GetAnswer(),
		Sentences: []sentenceJSON{}, Blocks: []blockJSON{}, Citations: []citationJSON{}, Why: whyOf(resp.GetWhy()),
		AnswerKind: answerKind(resp.GetAnswerKind()), ConversationID: resp.GetConversationId(), Confidence: confidenceOf(resp.GetConfidence()),
	}
	for _, x := range resp.GetSentences() {
		out.Sentences = append(out.Sentences, sentenceOf(x))
	}
	for _, b := range resp.GetBlocks() {
		out.Blocks = append(out.Blocks, blockJSON{b.GetBlockId(), enumName(b.GetBlockType()), b.GetParentBlockId(), b.GetOrder(),
			nonNil(b.GetSentenceIndexes()), scopeOf(b.GetScope())})
	}
	for _, x := range resp.GetCitations() {
		out.Citations = append(out.Citations, citationOf(x))
	}
	return out
}

func sentenceOf(x *answerv1.Sentence) sentenceJSON {
	return sentenceJSON{x.GetText(), x.GetStart(), x.GetEnd(), enumName(x.GetClaimType()), nonNil(x.GetRefIds()), x.GetSupport(), x.GetBlockId(),
		calcOf(x.GetCalc()), listedDocOf(x.GetDocument())}
}

func listedDocOf(d *answerv1.ListedDocument) *listedDocJSON {
	if d == nil {
		return nil
	}
	m := d.GetMetadata()
	out := &listedDocJSON{DocID: d.GetDocId(), DocVersion: d.GetDocVersion(), Title: d.GetTitle(), SourceURI: d.GetSourceUri(),
		DocType: m.GetDocType(), Dept: m.GetDept(), TopicCodes: m.GetTopicCodes(), EffectiveDate: m.GetEffectiveDate()}
	if m.GetValidityStatus() != commonv1.ValidityStatus_VALIDITY_STATUS_UNSPECIFIED {
		out.ValidityStatus = enumName(m.GetValidityStatus())
	}
	return out
}

// scopeOf 转换统计口径。没有完整计数时不输出总数：前端据此不显示“共 N 份”（K2 详设第 9 节）。
func scopeOf(s *answerv1.Scope) *scopeJSON {
	if s == nil {
		return nil
	}
	out := &scopeJSON{FiltersText: s.GetFiltersText(), TotalIsExact: s.GetTotalIsExact(), Shown: s.GetShown(),
		MetadataGaps: s.GetMetadataGaps(), Basis: s.GetBasis()}
	if s.GetTotalIsExact() || s.GetTotalCount() > 0 {
		n := s.GetTotalCount()
		out.TotalCount = &n
	}
	return out
}

func calcOf(c *answerv1.Calculation) *calcJSON {
	if c == nil {
		return nil
	}
	out := &calcJSON{Inputs: []calcInputJSON{}, Results: []calcResultJSON{}}
	for _, i := range c.GetInputs() {
		out.Inputs = append(out.Inputs, calcInputJSON{i.GetVar(), i.GetRefId(), i.GetValue(), i.GetUnit()})
	}
	for _, r := range c.GetResults() {
		out.Results = append(out.Results, calcResultJSON{r.GetName(), r.GetExpr(), r.GetValue()})
	}
	return out
}

func citationOf(x *answerv1.Citation) citationJSON {
	return citationJSON{x.GetRefId(), x.GetChunkId(), x.GetDocId(), x.GetDocVersion(), x.GetTitle(),
		x.GetQuote(), spanOf(x.GetSpan()), x.GetSourceUri(), x.GetOwner(), enumName(x.GetClassification()), enumName(x.GetSourceType()),
		x.GetInjectionFlag()}
}

func whyOf(why *answerv1.Why) whyJSON {
	out := whyJSON{Retrievers: nonNil(why.GetRetrievers()), IndexVersion: why.GetIndexVersion(), PolicyVersion: why.GetPolicyVersion(),
		EmbeddingModel: why.GetEmbeddingModel(), GenerationModel: why.GetGenerationModel(), PromptVersion: why.GetPromptVersion(),
		NLIModel: why.GetNliModel(), SupportThreshold: why.GetSupportThreshold(), RerankModel: why.GetRerankModel(),
		CalculatorVersion: why.GetCalculatorVersion(), InferredFilters: filtersOf(why.GetInferredFilters()),
		BasedOnSummaries: why.GetBasedOnSummaries(), Degraded: degradedOf(why.GetDegraded())}
	if why.GetExperimentId() != "" {
		out.ExperimentID, out.ExperimentArm = why.GetExperimentId(), enumName(why.GetExperimentArm())
	}
	// 旧版 orchestrator 不填意图：不输出，而不是输出 unspecified
	if why.GetIntent() != answerv1.Intent_INTENT_UNSPECIFIED {
		out.Intent = enumName(why.GetIntent())
	}
	for _, r := range why.GetAgentRounds() {
		round := agentRoundJSON{Round: r.GetRound(), Calls: []agentCallJSON{}}
		for _, c := range r.GetCalls() {
			round.Calls = append(round.Calls, agentCallJSON{c.GetSubQuestionId(), c.GetSubQuestion(), c.GetTool(),
				nonNil(c.GetChunkIds()), c.GetCovered(), c.GetElapsedMs()})
		}
		out.AgentRounds = append(out.AgentRounds, round)
	}
	for _, p := range why.GetGraphPath() {
		out.GraphPath = append(out.GraphPath, graphPathJSON{p.GetRefId(), p.GetChunkId(), nonNil(graphEdgesOf(p.GetEdges()))})
	}
	return out
}

func nonNil[T any](xs []T) []T {
	if xs == nil {
		return []T{}
	}
	return xs
}

// fromGRPC 把下游的 gRPC 状态转成对外错误；不透出下游服务名与细节（技术设计文档 13.10）。
func fromGRPC(err error) problem {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return problemInvalid
	case codes.PermissionDenied:
		return problemForbidden
	case codes.NotFound:
		return problemNotFound
	case codes.DeadlineExceeded:
		return problemDeadline
	case codes.ResourceExhausted:
		// 下游已经满载：立即转 503，由调用方按 Retry-After 退避（gateway 详设 5.2）
		return problemOverloaded
	default:
		// 包括 UNAUTHENTICATED：下游不认 gateway 签的 Principal 是平台内部故障，对外按不可用处理
		return problemUnavailable
	}
}
