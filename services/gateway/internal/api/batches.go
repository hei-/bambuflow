package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"regexp"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	batchv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/batch/v1"
	"github.com/hei-/bambuflow/services/gateway/internal/admission"
	"github.com/hei-/bambuflow/services/gateway/internal/issuer"
)

// /v1/batches 系列接口（技术设计文档 13.5，gateway 详设 4.1）：转到 qa/batch。
// 提交时签发委托（gateway 详设 3.3），qa/batch 每条执行前凭它换发 Principal；查询、取消、取结果都用调用者自己的身份，
// qa/batch 只对提交人本人返回。

const (
	maxBatchItems = 10_000
	// 每行输入的上限：问题不超过 2000 字（UTF-8 最多约 6 KB）加过滤条件，留足余量
	maxBatchLine = 64 << 10
	// 提交要写入最多 1 万条，比查询给更长的时间
	batchCreateDeadline = 10 * time.Second
	batchDeadline       = 2 * time.Second
	// 取结果整体的时间上限：最多 1 万条、每页 100 条逐页复核权限
	batchResultsDeadline = 2 * time.Minute
	maxIdempotencyKey    = 128
)

var (
	customIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)
	batchIDPattern  = regexp.MustCompile(`^bat_[a-f0-9]{1,64}$`)

	problemIdempotency = problem{http.StatusConflict, "idempotency_conflict", "同一幂等键对应了不同的请求内容"}
)

var batchModes = map[string]batchv1.BatchMode{
	"answer":        batchv1.BatchMode_BATCH_MODE_ANSWER,
	"evidence_only": batchv1.BatchMode_BATCH_MODE_EVIDENCE_ONLY,
}

type batchCountsJSON struct {
	Total     int64 `json:"total"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Cancelled int64 `json:"cancelled"`
}

type batchJSON struct {
	BatchID     string          `json:"batch_id"`
	Status      string          `json:"status"`
	Mode        string          `json:"mode"`
	Counts      batchCountsJSON `json:"counts"`
	CreatedAt   string          `json:"created_at"`
	StartedAt   string          `json:"started_at,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
	ExpiresAt   string          `json:"expires_at,omitempty"`
}

type batchInputLine struct {
	CustomID string       `json:"custom_id"`
	Question string       `json:"question"`
	Filters  *filtersJSON `json:"filters,omitempty"`
}

type itemErrorJSON struct {
	Type  string `json:"type"`
	Title string `json:"title"`
}

type batchResultLine struct {
	CustomID string         `json:"custom_id"`
	Answer   *answerResult  `json:"answer,omitempty"`
	Error    *itemErrorJSON `json:"error,omitempty"`
}

func batchJSONOf(b *batchv1.Batch) batchJSON {
	c := b.GetCounts()
	return batchJSON{
		BatchID: b.GetBatchId(), Status: enumName(b.GetStatus()), Mode: enumName(b.GetMode()),
		Counts:    batchCountsJSON{c.GetTotal(), c.GetSucceeded(), c.GetFailed(), c.GetCancelled()},
		CreatedAt: timeOf(b.GetCreatedAt()), StartedAt: timeOf(b.GetStartedAt()),
		CompletedAt: timeOf(b.GetCompletedAt()), ExpiresAt: timeOf(b.GetExpiresAt()),
	}
}

func timeOf(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339)
}

func newBatchID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "bat_" + hex.EncodeToString(b)
}

// batchProblem 在 fromGRPC 之外处理幂等冲突。
func batchProblem(err error) problem {
	if status.Code(err) == codes.AlreadyExists {
		return problemIdempotency
	}
	return fromGRPC(err)
}

// batchForm 是解析后的提交表单。
type batchForm struct {
	mode   batchv1.BatchMode
	items  []*batchv1.BatchItem
	digest string // 模式与输入文件原文的摘要，用于幂等键比对
}

// parseBatchForm 读取 multipart 表单：input_file（JSONL）与可选的 mode。逐行校验，任何一行不合法整批拒绝。
func parseBatchForm(r *http.Request) (batchForm, error) {
	mr, err := r.MultipartReader()
	if err != nil {
		return batchForm{}, problemInvalid
	}
	form := batchForm{mode: batchv1.BatchMode_BATCH_MODE_ANSWER}
	var fileSum []byte
	modeName, seenMode := "answer", false
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return batchForm{}, tooLargeOr(err, problemInvalid)
		}
		switch part.FormName() {
		case "mode":
			if seenMode {
				return batchForm{}, problemInvalid
			}
			seenMode = true
			b, err := io.ReadAll(io.LimitReader(part, 32))
			if err != nil {
				return batchForm{}, tooLargeOr(err, problemInvalid)
			}
			modeName = strings.TrimSpace(string(b))
		case "input_file":
			if fileSum != nil {
				return batchForm{}, problemInvalid
			}
			form.items, fileSum, err = parseBatchInput(part)
			if err != nil {
				return batchForm{}, err
			}
		default:
			return batchForm{}, problemInvalid
		}
	}
	mode, ok := batchModes[modeName]
	if !ok || fileSum == nil || len(form.items) == 0 {
		return batchForm{}, problemInvalid
	}
	// 仅检索模式的结果格式（证据列表）还没有实现，先明确拒绝，不悄悄按问答执行
	if mode != batchv1.BatchMode_BATCH_MODE_ANSWER {
		return batchForm{}, problemInvalid
	}
	form.mode = mode
	sum := sha256.Sum256(append([]byte(modeName+"\n"), fileSum...))
	form.digest = hex.EncodeToString(sum[:])
	return form, nil
}

func parseBatchInput(part *multipart.Part) ([]*batchv1.BatchItem, []byte, error) {
	h := sha256.New()
	sc := bufio.NewScanner(io.TeeReader(part, h))
	sc.Buffer(make([]byte, 0, 4096), maxBatchLine)
	seen := map[string]bool{}
	var items []*batchv1.BatchItem
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if len(items) == maxBatchItems {
			return nil, nil, problemTooLarge
		}
		var in batchInputLine
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil || dec.More() {
			return nil, nil, problemInvalid
		}
		if err := validQuestion(in.Question); err != nil {
			return nil, nil, err
		}
		filters := in.Filters.proto()
		if !customIDPattern.MatchString(in.CustomID) || seen[in.CustomID] || (in.Filters != nil && filters == nil) {
			return nil, nil, problemInvalid
		}
		seen[in.CustomID] = true
		items = append(items, &batchv1.BatchItem{CustomId: in.CustomID, Question: in.Question, Filters: filters})
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return nil, nil, problemTooLarge
		}
		return nil, nil, tooLargeOr(err, problemInvalid)
	}
	return items, h.Sum(nil), nil
}

func tooLargeOr(err error, p problem) problem {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return problemTooLarge
	}
	return p
}

// ---- POST /v1/batches ----

func (s *Server) createBatch(_ http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, _, err := s.authorize(r, c, access{batch: true, priority: admission.PriorityBatch})
	if err != nil {
		return nil, err
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxIdempotencyKey {
		return nil, problemInvalid
	}
	form, err := parseBatchForm(r)
	if err != nil {
		return nil, err
	}
	c.batchID = newBatchID()
	// 委托只记提交人、租户、客户端与批次；用户属性在每次换发时重新读取
	delegation, err := s.Issuer.IssueDelegation(issuer.Delegation{
		Username: c.sub, Tenant: c.tenant, Actor: c.actor, BatchID: c.batchID, AuthTime: c.authTime,
		Expires: s.Now().Add(issuer.MaxDelegationLifetime),
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := downstream(c.ctx, principal, "batch", "", batchCreateDeadline)
	defer cancel()
	resp, err := s.Batches.CreateBatch(ctx, &batchv1.CreateBatchRequest{
		BatchId: c.batchID, Delegation: delegation, Mode: form.mode, Items: form.items,
		IdempotencyKey: key, RequestDigest: form.digest,
	})
	if err != nil {
		return nil, batchProblem(err)
	}
	// 幂等重放时返回的是已有批次
	c.batchID = resp.GetBatch().GetBatchId()
	return batchJSONOf(resp.GetBatch()), nil
}

// batchTarget 认证后取路径里的批次 ID；格式不对按不存在处理。
func (s *Server) batchTarget(r *http.Request, c *call) (string, error) {
	principal, _, err := s.authorize(r, c, access{priority: admission.PriorityBatch})
	if err != nil {
		return "", err
	}
	id := r.PathValue("batch_id")
	if !batchIDPattern.MatchString(id) {
		return "", problemNotFound
	}
	c.batchID = id
	return principal, nil
}

// ---- GET /v1/batches/{batch_id} ----

func (s *Server) getBatch(_ http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, err := s.batchTarget(r, c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := downstream(c.ctx, principal, "batch", "", batchDeadline)
	defer cancel()
	resp, err := s.Batches.GetBatch(ctx, &batchv1.GetBatchRequest{BatchId: c.batchID})
	if err != nil {
		return nil, batchProblem(err)
	}
	return batchJSONOf(resp.GetBatch()), nil
}

// ---- POST /v1/batches/{batch_id}/cancel ----

func (s *Server) cancelBatch(_ http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, err := s.batchTarget(r, c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := downstream(c.ctx, principal, "batch", "", batchDeadline)
	defer cancel()
	resp, err := s.Batches.CancelBatch(ctx, &batchv1.CancelBatchRequest{BatchId: c.batchID})
	if err != nil {
		return nil, batchProblem(err)
	}
	return batchJSONOf(resp.GetBatch()), nil
}

// ---- GET /v1/batches/{batch_id}/results ----

// batchResults 以 JSONL 逐页输出结果。第一页之前的错误按普通 HTTP 错误返回；开始输出后出错只能中断连接，
// 调用方据此知道结果不完整（不在末尾补错误行，免得被当成一条结果）。每条回答的三类块各写一条审计：
// 结果被取走时才算“展示给了用户”（技术设计文档 8.3）。
func (s *Server) batchResults(w http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, err := s.batchTarget(r, c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(c.ctx, batchResultsDeadline)
	defer cancel()
	page := func(token string) (*batchv1.ListResultsResponse, error) {
		pctx, pcancel := downstream(ctx, principal, "batch", "", batchDeadline)
		defer pcancel()
		return s.Batches.ListResults(pctx, &batchv1.ListResultsRequest{BatchId: c.batchID, PageToken: token})
	}
	resp, err := page("")
	if err != nil {
		return nil, batchProblem(err)
	}
	if err := s.writeAudit(c, http.StatusOK, "", "open"); err != nil {
		s.Log.ErrorContext(c.ctx, "入口审计写入失败", slog.Any("err", err))
		return nil, problemUnavailable
	}
	c.streamed = true
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)
	for {
		for _, res := range resp.GetResults() {
			line, err := s.resultLine(c, res)
			if err != nil {
				s.abortResults(c, "service_unavailable", err)
			}
			if err := enc.Encode(line); err != nil {
				// 客户端已断开
				s.endResults(c, "cancelled")
				return nil, nil
			}
		}
		if flusher != nil {
			flusher.Flush()
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		if resp, err = page(resp.GetNextPageToken()); err != nil {
			s.abortResults(c, batchProblem(err).typ, err)
		}
	}
	s.endResults(c, "")
	return nil, nil
}

// resultLine 转换一条结果；回答先写审计，写不进去就不输出（fail-closed）。
func (s *Server) resultLine(c *call, res *batchv1.BatchResult) (batchResultLine, error) {
	line := batchResultLine{CustomID: res.GetCustomId()}
	a := res.GetAnswer()
	if a == nil {
		line.Error = &itemErrorJSON{Type: res.GetError().GetType(), Title: res.GetError().GetTitle()}
		return line, nil
	}
	item := &call{ctx: c.ctx, traceID: c.traceID, answerID: a.GetAnswerId(), batchID: c.batchID, jti: c.jti,
		sub: c.sub, tenant: c.tenant, actor: c.actor, endpoint: "batch-result-item", start: s.Now(), blocks: a.GetAudit()}
	item.recordAnswer(a.GetStatus(), a.GetWhy(), sentenceTexts(a.GetSentences()))
	if err := s.writeAudit(item, http.StatusOK, "", ""); err != nil {
		return line, err
	}
	out := answerJSON(&call{answerID: a.GetAnswerId(), traceID: a.GetTraceId()}, a)
	line.Answer = &out
	return line, nil
}

func (s *Server) endResults(c *call, reason string) {
	if err := s.writeAudit(c, http.StatusOK, reason, "end"); err != nil {
		s.Log.ErrorContext(c.ctx, "入口审计写入失败", slog.Any("err", err))
	}
}

// abortResults 记下中断原因后中断连接：响应头已发出，只能这样让调用方知道结果不完整。
func (s *Server) abortResults(c *call, reason string, err error) {
	s.Log.ErrorContext(c.ctx, "批次结果输出中断", slog.String("batch_id", c.batchID), slog.Any("err", err))
	s.endResults(c, reason)
	panic(http.ErrAbortHandler)
}
