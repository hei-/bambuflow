package api

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 审计查询（技术设计文档 13.2，governance 详设 7.2）：GET /v1/admin/audit?sub=&doc_id=&from=&to=&kind=，由 governance 实现。

type auditRecordJSON struct {
	Kind              string   `json:"kind"`
	Source            string   `json:"source"`
	Time              string   `json:"time"`
	TraceID           string   `json:"trace_id"`
	AnswerID          string   `json:"answer_id,omitempty"`
	BatchID           string   `json:"batch_id,omitempty"`
	Sub               string   `json:"sub,omitempty"`
	Tenant            string   `json:"tenant,omitempty"`
	Actor             string   `json:"actor,omitempty"`
	Endpoint          string   `json:"endpoint"`
	Status            int32    `json:"status"`
	Reason            string   `json:"reason,omitempty"`
	LatencyMS         int64    `json:"latency_ms"`
	CandidateChunkIDs []string `json:"candidate_chunk_ids"`
	ContextChunkIDs   []string `json:"context_chunk_ids"`
	ShownChunkIDs     []string `json:"shown_chunk_ids"`
	ListedDocIDs      []string `json:"listed_doc_ids"`
	// 按文档查询时，文档出现在哪几类里
	MatchedIn   []string `json:"matched_in,omitempty"`
	AdminMethod string   `json:"admin_method,omitempty"`
	AdminPath   string   `json:"admin_path,omitempty"`
	// 问答与检索用到的版本、问答状态与回答摘要（技术设计文档 8.3）
	Versions     *auditVersionsJSON `json:"versions,omitempty"`
	AnswerStatus string             `json:"answer_status,omitempty"`
	AnswerSHA256 string             `json:"answer_sha256,omitempty"`
	// 变更审计才有
	Roles    []string        `json:"roles,omitempty"`
	Action   string          `json:"action,omitempty"`
	ObjectID string          `json:"object_id,omitempty"`
	Detail   json.RawMessage `json:"detail,omitempty"`
	// 评测回放才有
	GateID string `json:"gate_id,omitempty"`
}

type auditVersionsJSON struct {
	IndexVersion      string   `json:"index_version,omitempty"`
	PolicyVersion     string   `json:"policy_version,omitempty"`
	EmbeddingModel    string   `json:"embedding_model,omitempty"`
	RerankModel       string   `json:"rerank_model,omitempty"`
	GenerationModel   string   `json:"generation_model,omitempty"`
	PromptVersion     string   `json:"prompt_version,omitempty"`
	NLIModel          string   `json:"nli_model,omitempty"`
	CalculatorVersion string   `json:"calculator_version,omitempty"`
	ExperimentID      string   `json:"experiment_id,omitempty"`
	ExperimentArm     string   `json:"experiment_arm,omitempty"`
	PipelineVersion   string   `json:"pipeline_version,omitempty"`
	RoutingVersion    string   `json:"routing_version,omitempty"`
	Degraded          []string `json:"degraded,omitempty"`
}

func auditVersionsOf(v *adminv1.AuditVersions) *auditVersionsJSON {
	if v == nil {
		return nil
	}
	return &auditVersionsJSON{IndexVersion: v.GetIndexVersion(), PolicyVersion: v.GetPolicyVersion(), EmbeddingModel: v.GetEmbeddingModel(),
		RerankModel: v.GetRerankModel(), GenerationModel: v.GetGenerationModel(), PromptVersion: v.GetPromptVersion(),
		NLIModel: v.GetNliModel(), CalculatorVersion: v.GetCalculatorVersion(), ExperimentID: v.GetExperimentId(),
		ExperimentArm: v.GetExperimentArm(), PipelineVersion: v.GetPipelineVersion(), RoutingVersion: v.GetRoutingVersion(),
		Degraded: v.GetDegraded()}
}

type auditListJSON struct {
	Records       []auditRecordJSON `json:"records"`
	NextPageToken string            `json:"next_page_token,omitempty"`
}

// detailOf 原样转出变更审计的依据；governance 给的不是合法 JSON 时不输出（不把坏数据拼进响应）。
func detailOf(s string) json.RawMessage {
	if s == "" || !json.Valid([]byte(s)) {
		return nil
	}
	return json.RawMessage(s)
}

func (s *Server) queryAudit(ctx context.Context, in *adminInput) (adminOutput, error) {
	size, err := pageSize(in)
	if err != nil {
		return adminOutput{}, err
	}
	from, err1 := time.Parse(time.RFC3339, in.query.Get("from"))
	to, err2 := time.Parse(time.RFC3339, in.query.Get("to"))
	if err1 != nil || err2 != nil {
		return adminOutput{}, problemInvalid
	}
	resp, err := s.AuditQuery.QueryAudit(ctx, &adminv1.QueryAuditRequest{
		Sub: in.query.Get("sub"), DocId: in.query.Get("doc_id"), From: timestamppb.New(from), To: timestamppb.New(to),
		PageSize: size, PageToken: in.query.Get("page_token"), Kinds: in.query["kind"],
	})
	if err != nil {
		return adminOutput{}, err
	}
	out := auditListJSON{Records: []auditRecordJSON{}, NextPageToken: resp.GetNextPageToken()}
	for _, r := range resp.GetRecords() {
		out.Records = append(out.Records, auditRecordJSON{
			Time: r.GetTime().AsTime().UTC().Format(time.RFC3339Nano), TraceID: r.GetTraceId(), AnswerID: r.GetAnswerId(),
			BatchID: r.GetBatchId(), Sub: r.GetSub(), Tenant: r.GetTenant(), Actor: r.GetActor(), Endpoint: r.GetEndpoint(),
			Status: r.GetStatus(), Reason: r.GetReason(), LatencyMS: r.GetLatencyMs(),
			CandidateChunkIDs: nonNil(r.GetCandidateChunkIds()), ContextChunkIDs: nonNil(r.GetContextChunkIds()),
			ShownChunkIDs: nonNil(r.GetShownChunkIds()), ListedDocIDs: nonNil(r.GetListedDocIds()), MatchedIn: r.GetMatchedIn(),
			AdminMethod: r.GetAdminMethod(), AdminPath: r.GetAdminPath(),
			Versions: auditVersionsOf(r.GetVersions()), AnswerStatus: r.GetAnswerStatus(), AnswerSHA256: r.GetAnswerSha256(),
			Kind: r.GetKind(), Source: r.GetSource(), Roles: r.GetRoles(), Action: r.GetAction(), ObjectID: r.GetObjectId(),
			GateID: r.GetGateId(), Detail: detailOf(r.GetDetailJson()),
		})
	}
	return adminOutput{body: out}, nil
}
