// Package auditquery 是审计查询 /v1/admin/audit（governance 详设 1.1、2）与审计索引（index.go）：按用户、文档、时间范围、
// 记录类别查审计索引，再从审计桶读出原文返回。时间跨度不限。只有审计岗能查，每次查询先写变更审计再返回结果（fail-closed）。
package auditquery

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

const roleAuditor = "auditor"

var (
	// restricted 是 gateway 为受限级内容单独写的访问记录（技术设计文档 8.6）；同一次请求另有一条 access，默认不重复列出
	kinds          = []string{"access", "change", "eval", "restricted"}
	defaultKinds   = []string{"access", "change"}
	errLineMissing = errors.New("原文里没有这一行")
)

// Auditor 写变更审计。
type Auditor interface {
	Write(ctx context.Context, action, objectID string, detail any) error
}

type Server struct {
	adminv1.UnimplementedAuditAdminServiceServer
	Bucket Bucket
	Index  Index
	Audit  Auditor
	Log    *slog.Logger
}

// record 是入口审计与评测回放的一行（gateway internal/audit.Record 的 JSON 形式，只取查询要返回的字段）。
type record struct {
	Time              time.Time `json:"time"`
	TraceID           string    `json:"trace_id"`
	AnswerID          string    `json:"answer_id"`
	BatchID           string    `json:"batch_id"`
	Sub               string    `json:"sub"`
	Tenant            string    `json:"tenant"`
	Actor             string    `json:"actor"`
	Endpoint          string    `json:"endpoint"`
	Status            int32     `json:"status"`
	Reason            string    `json:"reason"`
	LatencyMS         int64     `json:"latency_ms"`
	CandidateChunkIDs []string  `json:"candidate_chunk_ids"`
	ContextChunkIDs   []string  `json:"context_chunk_ids"`
	ShownChunkIDs     []string  `json:"shown_chunk_ids"`
	ListedDocIDs      []string  `json:"listed_doc_ids"`
	Versions          *struct {
		IndexVersion      string   `json:"index_version"`
		PolicyVersion     string   `json:"policy_version"`
		EmbeddingModel    string   `json:"embedding_model"`
		RerankModel       string   `json:"rerank_model"`
		GenerationModel   string   `json:"generation_model"`
		PromptVersion     string   `json:"prompt_version"`
		NLIModel          string   `json:"nli_model"`
		CalculatorVersion string   `json:"calculator_version"`
		PipelineVersion   string   `json:"pipeline_version"`
		RoutingVersion    string   `json:"routing_version"`
		ExperimentID      string   `json:"experiment_id"`
		ExperimentArm     string   `json:"experiment_arm"`
		Degraded          []string `json:"degraded"`
	} `json:"versions"`
	AnswerStatus string `json:"answer_status"`
	AnswerSHA    string `json:"answer_sha256"`
	Admin        *struct {
		Method string `json:"method"`
		Path   string `json:"path"`
	} `json:"admin"`
	// 评测回放才有
	GateID string `json:"gate_id"`
	// restricted_access 为受限级内容的单独访问记录
	Event string `json:"event"`
}

// change 是一条变更审计（libs/platform-go/changeaudit.Record 的 JSON 形式）。
type change struct {
	Time     time.Time       `json:"time"`
	TraceID  string          `json:"trace_id"`
	Actor    string          `json:"actor"`
	Roles    []string        `json:"roles"`
	Action   string          `json:"action"`
	ObjectID string          `json:"object_id"`
	Detail   json.RawMessage `json:"detail"`
}

func (s *Server) QueryAudit(ctx context.Context, r *adminv1.QueryAuditRequest) (*adminv1.QueryAuditResponse, error) {
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), roleAuditor) {
		return nil, status.Error(codes.PermissionDenied, "需要审计岗")
	}
	f := Filter{From: r.GetFrom().AsTime(), To: r.GetTo().AsTime(), Sub: r.GetSub(), DocID: r.GetDocId(), Kinds: r.GetKinds(),
		Limit: int(r.GetPageSize())}
	if f.Limit == 0 {
		f.Limit = 50
	}
	if len(f.Kinds) == 0 {
		f.Kinds = defaultKinds
	}
	switch {
	case r.GetFrom() == nil || r.GetTo() == nil || f.To.Before(f.From):
		return nil, status.Error(codes.InvalidArgument, "from、to 必填，from 不晚于 to")
	case f.Limit < 1 || f.Limit > 200:
		return nil, status.Error(codes.InvalidArgument, "page_size 应为 1–200")
	case len(f.Sub) > 256 || len(f.DocID) > 512 || !utf8.ValidString(f.Sub) || !utf8.ValidString(f.DocID):
		return nil, status.Error(codes.InvalidArgument, "sub 或 doc_id 不合法")
	case slices.ContainsFunc(f.Kinds, func(k string) bool { return !slices.Contains(kinds, k) }):
		return nil, status.Error(codes.InvalidArgument, "kinds 只能是 access、change、eval、restricted")
	}
	if t := r.GetPageToken(); t != "" {
		var c Cursor
		raw, err := base64.RawURLEncoding.DecodeString(t)
		if err != nil || json.Unmarshal(raw, &c) != nil || c.RecordID == "" {
			return nil, status.Error(codes.InvalidArgument, "page_token 不合法")
		}
		f.After = &c
	}

	// 先写变更审计：查询行为本身被审计（谁在什么时候查了谁、哪份文档），写不进去就不查
	if err := s.Audit.Write(ctx, "audit.query", "", map[string]any{
		"sub": f.Sub, "doc_id": f.DocID, "from": f.From.UTC().Format(time.RFC3339), "to": f.To.UTC().Format(time.RFC3339),
		"kinds": f.Kinds, "page_token": r.GetPageToken(),
	}); err != nil {
		s.Log.ErrorContext(ctx, "写审计查询的变更审计失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}

	// 多取一行判断还有没有下一页
	want := f.Limit
	f.Limit++
	rows, err := s.Index.Find(ctx, f)
	if err != nil {
		s.Log.ErrorContext(ctx, "查审计索引失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	out := &adminv1.QueryAuditResponse{}
	if len(rows) > want {
		rows = rows[:want]
		last := rows[want-1]
		tok, _ := json.Marshal(Cursor{TS: last.TS, RecordID: last.RecordID})
		out.NextPageToken = base64.RawURLEncoding.EncodeToString(tok)
	}
	objects := map[string][][]byte{}
	for _, row := range rows {
		lines, ok := objects[row.ObjectKey]
		if !ok {
			body, err := s.Bucket.Get(ctx, row.ObjectKey)
			if err != nil {
				s.Log.ErrorContext(ctx, "读取审计原文失败", slog.String("key", row.ObjectKey), slog.Any("err", err))
				return nil, status.Error(codes.Unavailable, "暂时不可用")
			}
			lines = splitLines(body)
			objects[row.ObjectKey] = lines
		}
		rec, err := row.proto(lines, f.DocID)
		if err != nil {
			// 索引与原文对不上：原文只追加、不会变，说明索引写错了；不返回拼凑的结果
			s.Log.ErrorContext(ctx, "审计原文与索引不符", slog.String("key", row.ObjectKey), slog.Int("line", row.Line), slog.Any("err", err))
			return nil, status.Error(codes.Internal, "审计索引与原文不符")
		}
		out.Records = append(out.Records, rec)
	}
	return out, nil
}

func splitLines(body []byte) [][]byte {
	var out [][]byte
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	for sc.Scan() {
		out = append(out, slices.Clone(sc.Bytes()))
	}
	return out
}

// proto 从原文行组装返回的记录。
func (row Row) proto(lines [][]byte, docID string) (*adminv1.AuditRecord, error) {
	if row.Line > len(lines) {
		return nil, errLineMissing
	}
	raw := lines[row.Line-1]
	if row.Kind == "change" {
		var c change
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		out := &adminv1.AuditRecord{Kind: row.Kind, Source: row.Source, Time: timestamppb.New(c.Time), TraceId: c.TraceID, Sub: c.Actor,
			Roles: c.Roles, Action: c.Action, ObjectId: c.ObjectID}
		if len(c.Detail) > 0 && string(c.Detail) != "null" {
			out.DetailJson = string(c.Detail)
		}
		if docID != "" && c.ObjectID == docID {
			out.MatchedIn = []string{"object"}
		}
		return out, nil
	}
	var r record
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	out := r.proto(matchDoc(r, docID))
	out.Kind, out.Source, out.GateId = row.Kind, row.Source, r.GateID
	return out, nil
}

// DocOfChunk 从块 ID（{source}:{doc_id}#{anchor}，总体设计 4.1）取出文档 ID。
func DocOfChunk(chunkID string) string {
	_, rest, ok := strings.Cut(chunkID, ":")
	if !ok {
		return ""
	}
	if i := strings.LastIndexByte(rest, '#'); i >= 0 {
		return rest[:i]
	}
	return rest
}

// matchDoc 返回文档出现在哪几类块（或列出的文档）里；docID 为空时返回空。
func matchDoc(r record, docID string) []string {
	if docID == "" {
		return nil
	}
	var out []string
	for _, c := range []struct {
		name string
		ids  []string
	}{{"candidate", r.CandidateChunkIDs}, {"context", r.ContextChunkIDs}, {"shown", r.ShownChunkIDs}} {
		if slices.ContainsFunc(c.ids, func(id string) bool { return DocOfChunk(id) == docID }) {
			out = append(out, c.name)
		}
	}
	if slices.Contains(r.ListedDocIDs, docID) {
		out = append(out, "listed")
	}
	return out
}

func (r record) proto(matched []string) *adminv1.AuditRecord {
	out := &adminv1.AuditRecord{
		Time: timestamppb.New(r.Time), TraceId: r.TraceID, AnswerId: r.AnswerID, BatchId: r.BatchID, Sub: r.Sub, Tenant: r.Tenant,
		Actor: r.Actor, Endpoint: r.Endpoint, Status: r.Status, Reason: r.Reason, LatencyMs: r.LatencyMS,
		CandidateChunkIds: r.CandidateChunkIDs, ContextChunkIds: r.ContextChunkIDs, ShownChunkIds: r.ShownChunkIDs,
		ListedDocIds: r.ListedDocIDs, MatchedIn: matched, AnswerStatus: r.AnswerStatus, AnswerSha256: r.AnswerSHA,
	}
	if v := r.Versions; v != nil {
		out.Versions = &adminv1.AuditVersions{IndexVersion: v.IndexVersion, PolicyVersion: v.PolicyVersion,
			EmbeddingModel: v.EmbeddingModel, RerankModel: v.RerankModel, GenerationModel: v.GenerationModel,
			PromptVersion: v.PromptVersion, NliModel: v.NLIModel, CalculatorVersion: v.CalculatorVersion,
			ExperimentId: v.ExperimentID, ExperimentArm: v.ExperimentArm, PipelineVersion: v.PipelineVersion,
			RoutingVersion: v.RoutingVersion, Degraded: v.Degraded}
	}
	if r.Admin != nil {
		out.AdminMethod, out.AdminPath = r.Admin.Method, r.Admin.Path
	}
	return out
}
