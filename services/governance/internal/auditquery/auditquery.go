// Package auditquery 是审计查询 /v1/admin/audit（governance 详设 1.1、2）：按用户、文档、时间范围查 gateway 的入口审计。
//
// 数据来自 gateway 送进审计桶的入口审计段（键 gateway/<实例>/audit.jsonl.<封存时刻纳秒>，每行一条 JSON），
// 不另建索引：设计里的审计索引表（总体设计 4.1 audit_index）还没有，先按时间范围挑出可能相关的段逐行扫描，
// 所以查询必须带时间范围、跨度不超过 31 天。只有审计岗能查，每次查询先写变更审计再返回结果（fail-closed）。
package auditquery

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
)

const (
	// SegmentPrefix 是 gateway 入口审计段的键前缀
	SegmentPrefix = "gateway/"
	maxSpan       = 31 * 24 * time.Hour
	// segmentSlack：段的封存时刻晚于段里最后一条记录，网关停机或审计桶不可用时可能晚很久；
	// 时间范围之后这么久之内封存的段也要扫
	segmentSlack = 24 * time.Hour
	roleAuditor  = "auditor"
)

// Segments 是审计桶的只读访问。
type Segments interface {
	List(ctx context.Context, prefix string) ([]string, error)
	Get(ctx context.Context, key string) ([]byte, error)
}

// Auditor 写变更审计。
type Auditor interface {
	Write(ctx context.Context, action, objectID string, detail any) error
}

type Server struct {
	adminv1.UnimplementedAuditAdminServiceServer
	Store Segments
	Audit Auditor
	Log   *slog.Logger
}

// record 是入口审计的一行（gateway internal/audit.Record 的 JSON 形式，只取查询要返回的字段）。
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
	Admin             *struct {
		Method string `json:"method"`
		Path   string `json:"path"`
	} `json:"admin"`
}

// segment 是一个审计段：键与封存时刻。
type segment struct {
	key    string
	sealed int64
}

// cursor 是分页位置：从第 seg 个段（按封存时刻排序后，以键标识）的第 line 行开始。
type cursor struct {
	Key  string `json:"k"`
	Line int    `json:"l"`
}

func (s *Server) QueryAudit(ctx context.Context, r *adminv1.QueryAuditRequest) (*adminv1.QueryAuditResponse, error) {
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), roleAuditor) {
		return nil, status.Error(codes.PermissionDenied, "需要审计岗")
	}
	from, to := r.GetFrom().AsTime(), r.GetTo().AsTime()
	size := int(r.GetPageSize())
	if size == 0 {
		size = 50
	}
	switch {
	case r.GetFrom() == nil || r.GetTo() == nil || to.Before(from) || to.Sub(from) > maxSpan:
		return nil, status.Error(codes.InvalidArgument, "from、to 必填，跨度不超过 31 天")
	case size < 1 || size > 200:
		return nil, status.Error(codes.InvalidArgument, "page_size 应为 1–200")
	case len(r.GetSub()) > 256 || len(r.GetDocId()) > 512 || !utf8.ValidString(r.GetSub()) || !utf8.ValidString(r.GetDocId()):
		return nil, status.Error(codes.InvalidArgument, "sub 或 doc_id 不合法")
	}
	var start cursor
	if t := r.GetPageToken(); t != "" {
		raw, err := base64.RawURLEncoding.DecodeString(t)
		if err != nil || json.Unmarshal(raw, &start) != nil || start.Line < 0 {
			return nil, status.Error(codes.InvalidArgument, "page_token 不合法")
		}
	}

	// 先写变更审计：查询行为本身被审计（谁在什么时候查了谁、哪份文档），写不进去就不查
	if err := s.Audit.Write(ctx, "audit.query", "", map[string]string{
		"sub": r.GetSub(), "doc_id": r.GetDocId(), "from": from.UTC().Format(time.RFC3339), "to": to.UTC().Format(time.RFC3339),
		"page_token": r.GetPageToken(),
	}); err != nil {
		s.Log.ErrorContext(ctx, "写审计查询的变更审计失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}

	segs, err := s.segments(ctx, from, to)
	if err != nil {
		s.Log.ErrorContext(ctx, "列出审计段失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	i := 0
	if start.Key != "" {
		i = slices.IndexFunc(segs, func(g segment) bool { return g.key == start.Key })
		if i < 0 {
			return nil, status.Error(codes.InvalidArgument, "page_token 不合法")
		}
	}
	out := &adminv1.QueryAuditResponse{}
	for ; i < len(segs); i++ {
		body, err := s.Store.Get(ctx, segs[i].key)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取审计段失败", slog.String("key", segs[i].key), slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "暂时不可用")
		}
		line := 0
		sc := bufio.NewScanner(bytes.NewReader(body))
		sc.Buffer(make([]byte, 64<<10), 16<<20)
		for sc.Scan() {
			line++
			if segs[i].key == start.Key && line <= start.Line {
				continue
			}
			var rec record
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				// 审计段由 gateway 写入、对象锁保护，不会是坏数据；万一读到，跳过并告警，不让整个查询失败
				s.Log.ErrorContext(ctx, "审计段里有无法解析的行", slog.String("key", segs[i].key), slog.Int("line", line))
				continue
			}
			if rec.Time.Before(from) || rec.Time.After(to) || (r.GetSub() != "" && rec.Sub != r.GetSub()) {
				continue
			}
			matched := matchDoc(rec, r.GetDocId())
			if r.GetDocId() != "" && len(matched) == 0 {
				continue
			}
			if len(out.Records) == size {
				tok, _ := json.Marshal(cursor{Key: segs[i].key, Line: line - 1})
				out.NextPageToken = base64.RawURLEncoding.EncodeToString(tok)
				return out, nil
			}
			out.Records = append(out.Records, rec.proto(matched))
		}
		if err := sc.Err(); err != nil {
			s.Log.ErrorContext(ctx, "读取审计段失败", slog.String("key", segs[i].key), slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "暂时不可用")
		}
	}
	return out, nil
}

// segments 列出可能含有 [from, to] 内记录的审计段，按封存时刻排序：封存时刻不早于 from、不晚于 to + segmentSlack。
func (s *Server) segments(ctx context.Context, from, to time.Time) ([]segment, error) {
	keys, err := s.Store.List(ctx, SegmentPrefix)
	if err != nil {
		return nil, err
	}
	var out []segment
	for _, k := range keys {
		dot := strings.LastIndexByte(k, '.')
		if dot < 0 {
			continue
		}
		ns, err := strconv.ParseInt(k[dot+1:], 10, 64)
		if err != nil {
			continue
		}
		if ns >= from.UnixNano() && ns <= to.Add(segmentSlack).UnixNano() {
			out = append(out, segment{key: k, sealed: ns})
		}
	}
	slices.SortFunc(out, func(a, b segment) int {
		return cmp.Or(cmp.Compare(a.sealed, b.sealed), strings.Compare(a.key, b.key))
	})
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
		ListedDocIds: r.ListedDocIDs, MatchedIn: matched,
	}
	if r.Admin != nil {
		out.AdminMethod, out.AdminPath = r.Admin.Method, r.Admin.Path
	}
	return out
}
