// Package service 实现 BatchService（contracts/bambuflow/batch/v1），由 gateway 带调用者的 Principal 调用。
//
// 批次绑定提交人：查询、取消、取结果只对同一租户的提交人本人开放，其他人一律 NOT_FOUND（不区分无权与不存在）。
// 取结果时用取结果者当前的身份复核引用与列表项（gateway 详设 4.1）：执行之后才被撤权的文档不会经结果文件流出。
package service

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	answerv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	batchv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/batch/v1"
	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/services/qa/batch/internal/store"
)

const (
	maxItems       = 10_000
	maxQuestion    = 2000
	defaultPage    = 100
	maxPage        = 100
	maxCheckAccess = 50 // retrieval CheckAccess、CheckDocumentAccess 一次最多 50 个
)

var (
	batchIDPattern  = regexp.MustCompile(`^bat_[a-f0-9]{1,64}$`)
	customIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)

	errNotFound    = status.Error(codes.NotFound, "批次不存在")
	errUnavailable = status.Error(codes.Unavailable, "暂时不可用")
)

// Store 是 BatchService 用到的读写（store.PG）。
type Store interface {
	Create(ctx context.Context, b store.NewBatch, items []*batchv1.BatchItem) (*batchv1.Batch, error)
	Get(ctx context.Context, id string) (store.Record, error)
	Cancel(ctx context.Context, id string) error
	Results(ctx context.Context, id string, after int64, limit int) ([]store.ResultRow, error)
}

type Server struct {
	batchv1.UnimplementedBatchServiceServer
	Store     Store
	Retrieval retrievalv1.RetrievalServiceClient
	Log       *slog.Logger
}

func (s *Server) CreateBatch(ctx context.Context, r *batchv1.CreateBatchRequest) (*batchv1.CreateBatchResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if err := validCreate(r); err != nil {
		return nil, err
	}
	b, err := s.Store.Create(ctx, store.NewBatch{
		ID: r.GetBatchId(), Tenant: p.GetTenant(), Submitter: p.GetSub(), Actor: p.GetActor(), Delegation: r.GetDelegation(),
		IdempotencyKey: r.GetIdempotencyKey(), Digest: r.GetRequestDigest(), Mode: r.GetMode(),
	}, r.GetItems())
	switch {
	case errors.Is(err, store.ErrConflict):
		return nil, status.Error(codes.AlreadyExists, "同一幂等键对应了不同的请求内容")
	case err != nil:
		s.Log.ErrorContext(ctx, "写入批次失败", slog.Any("err", err))
		return nil, errUnavailable
	}
	s.Log.InfoContext(ctx, "已受理批次", slog.String("batch_id", b.GetBatchId()), slog.Int("items", len(r.GetItems())))
	return &batchv1.CreateBatchResponse{Batch: b}, nil
}

// validCreate 校验提交（gateway 已校验过一遍，这里不信任调用方，按契约再查）。
func validCreate(r *batchv1.CreateBatchRequest) error {
	if !batchIDPattern.MatchString(r.GetBatchId()) || r.GetDelegation() == "" || r.GetRequestDigest() == "" ||
		len(r.GetIdempotencyKey()) > 128 {
		return status.Error(codes.InvalidArgument, "batch_id、delegation、request_digest 不合法")
	}
	// 仅检索模式的结果格式还没有实现
	if r.GetMode() != batchv1.BatchMode_BATCH_MODE_ANSWER {
		return status.Error(codes.InvalidArgument, "只支持问答模式")
	}
	if n := len(r.GetItems()); n == 0 || n > maxItems {
		return status.Errorf(codes.InvalidArgument, "条目数应为 1–%d", maxItems)
	}
	seen := make(map[string]bool, len(r.GetItems()))
	for _, it := range r.GetItems() {
		q := strings.TrimSpace(it.GetQuestion())
		if !customIDPattern.MatchString(it.GetCustomId()) || seen[it.GetCustomId()] || q == "" || utf8.RuneCountInString(q) > maxQuestion {
			return status.Error(codes.InvalidArgument, "custom_id 重复或不合法，或问题为空、过长")
		}
		seen[it.GetCustomId()] = true
	}
	return nil
}

// owned 取批次并核对调用者是提交人本人。
func (s *Server) owned(ctx context.Context, id string) (store.Record, error) {
	if !batchIDPattern.MatchString(id) {
		return store.Record{}, errNotFound
	}
	rec, err := s.Store.Get(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return store.Record{}, errNotFound
	}
	if err != nil {
		s.Log.ErrorContext(ctx, "读取批次失败", slog.Any("err", err))
		return store.Record{}, errUnavailable
	}
	p := grpcmw.PrincipalFrom(ctx)
	if rec.Tenant != p.GetTenant() || rec.Submitter != p.GetSub() {
		return store.Record{}, errNotFound
	}
	return rec, nil
}

func (s *Server) GetBatch(ctx context.Context, r *batchv1.GetBatchRequest) (*batchv1.GetBatchResponse, error) {
	rec, err := s.owned(ctx, r.GetBatchId())
	if err != nil {
		return nil, err
	}
	return &batchv1.GetBatchResponse{Batch: rec.Batch}, nil
}

func (s *Server) CancelBatch(ctx context.Context, r *batchv1.CancelBatchRequest) (*batchv1.CancelBatchResponse, error) {
	if _, err := s.owned(ctx, r.GetBatchId()); err != nil {
		return nil, err
	}
	if err := s.Store.Cancel(ctx, r.GetBatchId()); err != nil {
		s.Log.ErrorContext(ctx, "取消批次失败", slog.Any("err", err))
		return nil, errUnavailable
	}
	rec, err := s.owned(ctx, r.GetBatchId())
	if err != nil {
		return nil, err
	}
	return &batchv1.CancelBatchResponse{Batch: rec.Batch}, nil
}

func (s *Server) ListResults(ctx context.Context, r *batchv1.ListResultsRequest) (*batchv1.ListResultsResponse, error) {
	if _, err := s.owned(ctx, r.GetBatchId()); err != nil {
		return nil, err
	}
	size := int(r.GetPageSize())
	if size == 0 {
		size = defaultPage
	}
	after := int64(-1)
	if tok := r.GetPageToken(); tok != "" {
		n, err := strconv.ParseInt(tok, 10, 64)
		if err != nil || n < 0 {
			return nil, status.Error(codes.InvalidArgument, "page_token 不合法")
		}
		after = n
	}
	if size < 1 || size > maxPage {
		return nil, status.Errorf(codes.InvalidArgument, "page_size 应为 1–%d", maxPage)
	}
	rows, err := s.Store.Results(ctx, r.GetBatchId(), after, size)
	if err != nil {
		s.Log.ErrorContext(ctx, "读取结果失败", slog.Any("err", err))
		return nil, errUnavailable
	}
	results, err := s.recheck(ctx, rows)
	if err != nil {
		return nil, err
	}
	out := &batchv1.ListResultsResponse{Results: results}
	if len(rows) == size {
		out.NextPageToken = strconv.FormatInt(rows[len(rows)-1].Seq, 10)
	}
	return out, nil
}

var (
	errRevoked = &batchv1.ItemError{Type: "forbidden", Title: "引用的文档已无权访问"}
	// 句子区间不合法，无法按权限去掉列表项：整条不返回（fail-closed）
	errMalformed = &batchv1.ItemError{Type: "service_unavailable", Title: "结果无法按当前权限调整"}
)

// recheck 把结果转成契约，并用调用者当前的 Principal 复核：
//   - 引用的块任一已无权访问，整条改为错误（句子与引用互相依赖，不能只删引用）；
//   - 清单类回答的列表项引用的是文档本身，按文档复核，无权的项去掉、口径块的数量同步调整（K2 详设第 8 节）。
//
// 复核失败时整页不返回（fail-closed）。
func (s *Server) recheck(ctx context.Context, rows []store.ResultRow) ([]*batchv1.BatchResult, error) {
	out := make([]*batchv1.BatchResult, len(rows))
	answers := map[int]*answerv1.AnswerResponse{}
	var chunks, docs []string
	for i, row := range rows {
		out[i] = &batchv1.BatchResult{CustomId: row.CustomID}
		switch row.Status {
		case "succeeded":
			a := &answerv1.AnswerResponse{}
			if err := proto.Unmarshal(row.Result, a); err != nil {
				s.Log.ErrorContext(ctx, "结果无法解析", slog.Int64("seq", row.Seq), slog.Any("err", err))
				return nil, errUnavailable
			}
			answers[i] = a
			for _, c := range a.GetCitations() {
				chunks = append(chunks, c.GetChunkId())
			}
			for _, sent := range a.GetSentences() {
				if d := sent.GetDocument(); d != nil {
					docs = append(docs, d.GetDocId())
				}
			}
		case "cancelled":
			out[i].Outcome = &batchv1.BatchResult_Error{Error: &batchv1.ItemError{Type: "cancelled", Title: "条目已取消"}}
		default:
			out[i].Outcome = &batchv1.BatchResult_Error{Error: &batchv1.ItemError{Type: row.ErrType, Title: row.ErrTitle}}
		}
	}
	ctx = metadata.AppendToOutgoingContext(grpcmw.ForwardPrincipal(ctx), "x-call-purpose", "batch")
	allowed, err := s.checkAccess(ctx, chunks, s.chunkAccess)
	if err != nil {
		return nil, err
	}
	allowedDocs, err := s.checkAccess(ctx, docs, s.docAccess)
	if err != nil {
		return nil, err
	}
	for i, a := range answers {
		if slices.ContainsFunc(a.GetCitations(), func(c *answerv1.Citation) bool { return !allowed[c.GetChunkId()] }) {
			out[i].Outcome = &batchv1.BatchResult_Error{Error: errRevoked}
			continue
		}
		pruned, err := dropListed(a, allowedDocs)
		if err != nil {
			s.Log.ErrorContext(ctx, "清单类回答无法按权限调整", slog.Int64("seq", rows[i].Seq), slog.Any("err", err))
			out[i].Outcome = &batchv1.BatchResult_Error{Error: errMalformed}
			continue
		}
		out[i].Outcome = &batchv1.BatchResult_Answer{Answer: pruned}
	}
	return out, nil
}

// checkAccess 去重后每 50 个一批交给 check 复核，返回允许访问的 ID。ctx 已带调用者的 Principal 与调用目的。
// 复核不经 retrieval 的入口准入，调用目的 batch 只用于审计归类。
func (s *Server) checkAccess(ctx context.Context, ids []string,
	check func(context.Context, []string) (map[string]bool, error)) (map[string]bool, error) {
	allowed := map[string]bool{}
	uniq := slices.Compact(slices.Sorted(slices.Values(ids)))
	for start := 0; start < len(uniq); start += maxCheckAccess {
		got, err := check(ctx, uniq[start:min(start+maxCheckAccess, len(uniq))])
		if err != nil {
			s.Log.WarnContext(ctx, "复核权限失败", slog.Any("err", err))
			return nil, errUnavailable
		}
		for id, ok := range got {
			allowed[id] = allowed[id] || ok
		}
	}
	return allowed, nil
}

func (s *Server) chunkAccess(ctx context.Context, ids []string) (map[string]bool, error) {
	resp, err := s.Retrieval.CheckAccess(ctx, &retrievalv1.CheckAccessRequest{ChunkIds: ids})
	out := map[string]bool{}
	for _, r := range resp.GetResults() {
		out[r.GetChunkId()] = r.GetAllowed()
	}
	return out, err
}

func (s *Server) docAccess(ctx context.Context, ids []string) (map[string]bool, error) {
	resp, err := s.Retrieval.CheckDocumentAccess(ctx, &retrievalv1.CheckDocumentAccessRequest{DocIds: ids})
	out := map[string]bool{}
	for _, r := range resp.GetResults() {
		out[r.GetDocId()] = r.GetAllowed()
	}
	return out, err
}
