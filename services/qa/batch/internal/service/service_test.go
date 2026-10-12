package service

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	answerv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	batchv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/batch/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/logging"
	"github.com/hei-/bambuflow/services/qa/batch/internal/store"
)

// ---- 假的存储 ----

type fakeStore struct {
	created  *store.NewBatch
	rec      store.Record
	rows     []store.ResultRow
	conflict bool
	after    int64
	limit    int
}

func (f *fakeStore) Create(_ context.Context, b store.NewBatch, _ []*batchv1.BatchItem) (*batchv1.Batch, error) {
	if f.conflict {
		return nil, store.ErrConflict
	}
	f.created = &b
	return &batchv1.Batch{BatchId: b.ID}, nil
}

func (f *fakeStore) Get(_ context.Context, id string) (store.Record, error) {
	if f.rec.Batch == nil || f.rec.Batch.GetBatchId() != id {
		return store.Record{}, store.ErrNotFound
	}
	return f.rec, nil
}

func (f *fakeStore) Cancel(context.Context, string) error { return nil }

func (f *fakeStore) Results(_ context.Context, _ string, after int64, limit int) ([]store.ResultRow, error) {
	f.after, f.limit = after, limit
	return f.rows, nil
}

// ---- 假的 retrieval：只允许 allowed 里的块，并记下调用时带的 Principal ----

type fakeRetrieval struct {
	retrievalv1.RetrievalServiceClient
	allowed  map[string]bool
	err      error
	docErr   error
	batches  [][]string
	docs     [][]string
	auth     []string
	purposes []string
}

func (f *fakeRetrieval) CheckDocumentAccess(ctx context.Context, r *retrievalv1.CheckDocumentAccessRequest, _ ...grpc.CallOption) (*retrievalv1.CheckDocumentAccessResponse, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	f.purposes = append(f.purposes, md.Get("x-call-purpose")...)
	f.docs = append(f.docs, r.GetDocIds())
	if f.docErr != nil {
		return nil, f.docErr
	}
	out := &retrievalv1.CheckDocumentAccessResponse{}
	for _, id := range r.GetDocIds() {
		out.Results = append(out.Results, &retrievalv1.DocumentAccessResult{DocId: id, Allowed: f.allowed[id]})
	}
	return out, nil
}

func (f *fakeRetrieval) CheckAccess(ctx context.Context, r *retrievalv1.CheckAccessRequest, _ ...grpc.CallOption) (*retrievalv1.CheckAccessResponse, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	f.auth = md.Get("authorization")
	f.purposes = append(f.purposes, md.Get("x-call-purpose")...)
	f.batches = append(f.batches, r.GetChunkIds())
	if f.err != nil {
		return nil, f.err
	}
	out := &retrievalv1.CheckAccessResponse{}
	for _, id := range r.GetChunkIds() {
		out.Results = append(out.Results, &retrievalv1.AccessResult{ChunkId: id, Allowed: f.allowed[id]})
	}
	return out, nil
}

// ---- 以某个用户身份调用：经 grpcmw 的拦截器放入 Principal，与生产路径一致 ----

type staticVerifier struct{ p *identityv1.Principal }

func (v staticVerifier) Verify(context.Context, string) (*identityv1.Principal, error) {
	return v.p, nil
}

func as[Req, Resp any](t *testing.T, sub, tenant string, call func(context.Context, Req) (Resp, error), req Req) (Resp, error) {
	t.Helper()
	ic := grpcmw.UnaryServer(staticVerifier{&identityv1.Principal{Sub: sub, Tenant: tenant, Actor: "console"}}, logging.New(&bytes.Buffer{}, "t", slog.LevelInfo))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer raw-"+sub))
	out, err := ic(ctx, req, &grpc.UnaryServerInfo{FullMethod: "/t"}, func(ctx context.Context, r any) (any, error) {
		return call(ctx, r.(Req))
	})
	var zero Resp
	if out == nil {
		return zero, err
	}
	return out.(Resp), err
}

func newServer() (*Server, *fakeStore, *fakeRetrieval) {
	st := &fakeStore{rec: store.Record{Tenant: "credit", Submitter: "alice", Batch: &batchv1.Batch{BatchId: "bat_0a"}}}
	rt := &fakeRetrieval{allowed: map[string]bool{}}
	return &Server{Store: st, Retrieval: rt, Log: logging.New(&bytes.Buffer{}, "t", slog.LevelInfo)}, st, rt
}

func validCreateRequest() *batchv1.CreateBatchRequest {
	return &batchv1.CreateBatchRequest{
		BatchId: "bat_0a", Delegation: "d", RequestDigest: "x", Mode: batchv1.BatchMode_BATCH_MODE_ANSWER,
		Items: []*batchv1.BatchItem{{CustomId: "q1", Question: "额度？"}, {CustomId: "q2", Question: "流程？"}},
	}
}

func TestCreateRecordsSubmitterFromPrincipal(t *testing.T) {
	s, st, _ := newServer()
	if _, err := as(t, "alice", "credit", s.CreateBatch, validCreateRequest()); err != nil {
		t.Fatal(err)
	}
	if st.created.Submitter != "alice" || st.created.Tenant != "credit" || st.created.Actor != "console" {
		t.Fatalf("提交人与租户应取自 Principal: %+v", st.created)
	}
	st.conflict = true
	if _, err := as(t, "alice", "credit", s.CreateBatch, validCreateRequest()); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("幂等键冲突应返回 ALREADY_EXISTS，得到 %v", err)
	}
}

func TestCreateValidation(t *testing.T) {
	s, _, _ := newServer()
	for name, mutate := range map[string]func(r *batchv1.CreateBatchRequest){
		"批次 ID 不合法":    func(r *batchv1.CreateBatchRequest) { r.BatchId = "ans_1" },
		"没有委托":         func(r *batchv1.CreateBatchRequest) { r.Delegation = "" },
		"仅检索模式":        func(r *batchv1.CreateBatchRequest) { r.Mode = batchv1.BatchMode_BATCH_MODE_EVIDENCE_ONLY },
		"没有条目":         func(r *batchv1.CreateBatchRequest) { r.Items = nil },
		"custom_id 重复": func(r *batchv1.CreateBatchRequest) { r.Items[1].CustomId = "q1" },
		"问题为空":         func(r *batchv1.CreateBatchRequest) { r.Items[0].Question = "  " },
	} {
		r := validCreateRequest()
		mutate(r)
		if _, err := as(t, "alice", "credit", s.CreateBatch, r); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: 应返回 INVALID_ARGUMENT，得到 %v", name, err)
		}
	}
}

func TestOnlySubmitterSeesBatch(t *testing.T) {
	s, _, _ := newServer()
	if _, err := as(t, "alice", "credit", s.GetBatch, &batchv1.GetBatchRequest{BatchId: "bat_0a"}); err != nil {
		t.Fatalf("提交人应能查询: %v", err)
	}
	for _, who := range [][2]string{{"bob", "credit"}, {"alice", "risk"}} {
		if _, err := as(t, who[0], who[1], s.GetBatch, &batchv1.GetBatchRequest{BatchId: "bat_0a"}); status.Code(err) != codes.NotFound {
			t.Errorf("%v 应得到 NOT_FOUND，得到 %v", who, err)
		}
		if _, err := as(t, who[0], who[1], s.ListResults, &batchv1.ListResultsRequest{BatchId: "bat_0a"}); status.Code(err) != codes.NotFound {
			t.Errorf("%v 取结果应得到 NOT_FOUND，得到 %v", who, err)
		}
		if _, err := as(t, who[0], who[1], s.CancelBatch, &batchv1.CancelBatchRequest{BatchId: "bat_0a"}); status.Code(err) != codes.NotFound {
			t.Errorf("%v 取消应得到 NOT_FOUND，得到 %v", who, err)
		}
	}
	if _, err := as(t, "alice", "credit", s.GetBatch, &batchv1.GetBatchRequest{BatchId: "bat_ff"}); status.Code(err) != codes.NotFound {
		t.Errorf("不存在的批次应得到 NOT_FOUND，得到 %v", err)
	}
}

func answerRow(seq int64, id string, chunks ...string) store.ResultRow {
	a := &answerv1.AnswerResponse{AnswerId: id, Status: answerv1.Status_STATUS_ANSWERED}
	for i, c := range chunks {
		a.Citations = append(a.Citations, &answerv1.Citation{RefId: int32(i + 1), ChunkId: c})
	}
	pb, _ := proto.Marshal(a)
	return store.ResultRow{Seq: seq, CustomID: fmt.Sprintf("q%d", seq), Status: "succeeded", Result: pb}
}

func TestResultsRecheckedWithCallerPrincipal(t *testing.T) {
	s, st, rt := newServer()
	lpb, _ := proto.Marshal(listingAnswer(3, true, "d1", "d2", "d3"))
	st.rows = []store.ResultRow{
		answerRow(0, "ans_ok", "c1", "c2"),
		answerRow(1, "ans_revoked", "c1", "c3"),
		{Seq: 2, CustomID: "q2", Status: "failed", ErrType: "forbidden", ErrTitle: "提交人已无权执行该批次"},
		{Seq: 3, CustomID: "q3", Status: "cancelled"},
		{Seq: 4, CustomID: "q4", Status: "succeeded", Result: lpb},
	}
	rt.allowed = map[string]bool{"c1": true, "c2": true, "d1": true, "d3": true}
	resp, err := as(t, "alice", "credit", s.ListResults, &batchv1.ListResultsRequest{BatchId: "bat_0a", PageSize: 5})
	if err != nil {
		t.Fatal(err)
	}
	r := resp.GetResults()
	if len(r) != 5 || r[0].GetAnswer().GetAnswerId() != "ans_ok" {
		t.Fatalf("有权的回答应原样返回: %v", r)
	}
	if r[1].GetAnswer() != nil || r[1].GetError().GetType() != "forbidden" {
		t.Fatalf("引用已无权访问的回答应改为错误: %v", r[1])
	}
	if r[2].GetError().GetType() != "forbidden" || r[3].GetError().GetType() != "cancelled" {
		t.Fatalf("失败与取消的条目应带错误: %v %v", r[2], r[3])
	}
	if got := listedDocs(r[4].GetAnswer()); len(got) != 2 || got[0] != "d1" || got[1] != "d3" {
		t.Fatalf("清单类回答应去掉无权的列表项后返回: %v", r[4])
	}
	if len(rt.auth) != 1 || rt.auth[0] != "Bearer raw-alice" {
		t.Fatalf("复核应带取结果者的 Principal: %v", rt.auth)
	}
	if len(rt.purposes) != 2 || rt.purposes[0] != "batch" || rt.purposes[1] != "batch" {
		t.Fatalf("按块与按文档复核的调用目的都应为 batch: %v", rt.purposes)
	}
	// 一页取满时给出下一页令牌，令牌是最后一条的序号
	if resp.GetNextPageToken() != "4" {
		t.Fatalf("下一页令牌应为 4，得到 %q", resp.GetNextPageToken())
	}
	if _, err := as(t, "alice", "credit", s.ListResults, &batchv1.ListResultsRequest{BatchId: "bat_0a", PageToken: "4"}); err != nil || st.after != 4 || st.limit != 100 {
		t.Fatalf("翻页应从序号 4 之后取、默认每页 100: %v %d %d", err, st.after, st.limit)
	}
}

func TestResultsFailClosedWhenRecheckUnavailable(t *testing.T) {
	s, st, rt := newServer()
	st.rows = []store.ResultRow{answerRow(0, "ans_ok", "c1")}
	rt.err = status.Error(codes.Unavailable, "retrieval 不可用")
	if _, err := as(t, "alice", "credit", s.ListResults, &batchv1.ListResultsRequest{BatchId: "bat_0a"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("无法复核时不应返回结果，得到 %v", err)
	}
}

func TestRecheckSplitsIntoBatchesOf50(t *testing.T) {
	s, st, rt := newServer()
	var chunks []string
	for i := range 120 {
		chunks = append(chunks, fmt.Sprintf("c%d", i))
	}
	st.rows = []store.ResultRow{answerRow(0, "ans_1", chunks...), answerRow(1, "ans_2", chunks[:10]...)}
	if _, err := as(t, "alice", "credit", s.ListResults, &batchv1.ListResultsRequest{BatchId: "bat_0a"}); err != nil {
		t.Fatal(err)
	}
	if len(rt.batches) != 3 || len(rt.batches[0]) != 50 || len(rt.batches[2]) != 20 {
		t.Fatalf("120 个不同的块应分 3 次复核: %d", len(rt.batches))
	}
}

func TestBadPageParams(t *testing.T) {
	s, _, _ := newServer()
	for _, r := range []*batchv1.ListResultsRequest{
		{BatchId: "bat_0a", PageToken: "abc"}, {BatchId: "bat_0a", PageToken: "-3"}, {BatchId: "bat_0a", PageSize: 101},
	} {
		if _, err := as(t, "alice", "credit", s.ListResults, r); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%v 应返回 INVALID_ARGUMENT，得到 %v", r, err)
		}
	}
}

// listingAnswer 按 orchestrator 清单路径的格式构造枚举回答：口径句在前（版式上排最后），列表项逐行在后。
func listingAnswer(total int64, exact bool, docs ...string) *answerv1.AnswerResponse {
	scope := &answerv1.Scope{TotalCount: total, TotalIsExact: exact, Basis: "仅统计你有权查看的文档"}
	for range docs {
		scope.Shown++
	}
	a := &answerv1.AnswerResponse{
		AnswerId: "ans_list", Status: answerv1.Status_STATUS_ANSWERED, AnswerKind: answerv1.AnswerKind_ANSWER_KIND_ENUMERATION,
		Audit: &answerv1.AuditBlocks{ListedDocIds: docs, TotalCount: total},
	}
	add := func(text string, doc string) {
		if a.Answer != "" {
			a.Answer += "\n"
		}
		start, _ := offset(len([]rune(a.Answer)))
		a.Answer += text
		end, _ := offset(len([]rune(a.Answer)))
		s := &answerv1.Sentence{Text: text, Start: start, End: end}
		if doc != "" {
			s.Document = &answerv1.ListedDocument{DocId: doc, Title: "文档" + doc}
			s.ClaimType = answerv1.ClaimType_CLAIM_TYPE_LIST_ITEM
		}
		a.Sentences = append(a.Sentences, s)
	}
	add(scopeNote(scope), "")
	list := &answerv1.Block{BlockId: "list", BlockType: answerv1.BlockType_BLOCK_TYPE_LIST_ITEM}
	for i, d := range docs {
		list.SentenceIndexes = append(list.SentenceIndexes, int32(i)+1)
		add("《文档"+d+"》", d)
	}
	a.Blocks = []*answerv1.Block{list, {
		BlockId: "scope", BlockType: answerv1.BlockType_BLOCK_TYPE_SCOPE, Order: 1000, SentenceIndexes: []int32{0}, Scope: scope,
	}}
	return a
}

func listedDocs(a *answerv1.AnswerResponse) []string {
	var out []string
	for _, s := range a.GetSentences() {
		if d := s.GetDocument(); d != nil {
			out = append(out, d.GetDocId())
		}
	}
	return out
}

// 列表项按文档复核：无权的项去掉，口径块的总数与列出数一起减，回答文本、句子位置、块下标与审计同步调整。
func TestDropListedAdjustsScope(t *testing.T) {
	a := listingAnswer(3, true, "d1", "d2", "d3")
	before := proto.Clone(a)
	got, err := dropListed(a, map[string]bool{"d1": true, "d3": true})
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(a, before) {
		t.Fatal("不应改动原回答")
	}
	want := "符合条件的文档共 2 份，以下列出全部 2 份（仅统计你有权查看的文档）。\n《文档d1》\n《文档d3》"
	if got.GetAnswer() != want {
		t.Fatalf("回答文本: %q", got.GetAnswer())
	}
	rs := []rune(got.GetAnswer())
	for _, s := range got.GetSentences() {
		if string(rs[s.GetStart():s.GetEnd()]) != s.GetText() {
			t.Fatalf("句子位置与文本不一致: %v", s)
		}
	}
	list, scope := got.GetBlocks()[0], got.GetBlocks()[1]
	if len(list.GetSentenceIndexes()) != 2 || list.GetSentenceIndexes()[0] != 1 || list.GetSentenceIndexes()[1] != 2 {
		t.Fatalf("列表块下标: %v", list.GetSentenceIndexes())
	}
	if sc := scope.GetScope(); sc.GetShown() != 2 || sc.GetTotalCount() != 2 || scope.GetSentenceIndexes()[0] != 0 {
		t.Fatalf("口径块: %v", scope)
	}
	if au := got.GetAudit(); au.GetTotalCount() != 2 || len(au.GetListedDocIds()) != 2 || au.GetListedDocIds()[1] != "d3" {
		t.Fatalf("审计应只含实际列出的文档: %v", au)
	}
	if got.GetStatus() != answerv1.Status_STATUS_ANSWERED {
		t.Fatalf("状态不应变化: %v", got.GetStatus())
	}
}

func TestDropListedAllRevoked(t *testing.T) {
	got, err := dropListed(listingAnswer(2, true, "d1", "d2"), map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetAnswer() != scopeEmpty || len(got.GetSentences()) != 1 || got.GetStatus() != answerv1.Status_STATUS_INSUFFICIENT_EVIDENCE {
		t.Fatalf("全部无权时只说没有找到: %q %v", got.GetAnswer(), got.GetStatus())
	}
	if len(got.GetBlocks()) != 1 || got.GetBlocks()[0].GetBlockType() != answerv1.BlockType_BLOCK_TYPE_SCOPE {
		t.Fatalf("空的列表块应去掉，口径块保留: %v", got.GetBlocks())
	}
}

// 总数不精确（超过上限）时只调列出数。
func TestDropListedInexactTotal(t *testing.T) {
	got, err := dropListed(listingAnswer(10000, false, "d1", "d2"), map[string]bool{"d2": true})
	if err != nil {
		t.Fatal(err)
	}
	if sc := scopeBlock(got).GetScope(); sc.GetTotalCount() != 10000 || sc.GetShown() != 1 {
		t.Fatalf("口径块: %v", sc)
	}
	if want := "符合条件的文档共 超过 10000 份，以下列出最新的 1 份（仅统计你有权查看的文档）。\n《文档d2》"; got.GetAnswer() != want {
		t.Fatalf("回答文本: %q", got.GetAnswer())
	}
}

// 句子区间不合法时不能安全调整：整条改为错误，不原样放行。
func TestDropListedMalformedFailsClosed(t *testing.T) {
	s, st, rt := newServer()
	a := listingAnswer(2, true, "d1", "d2")
	a.Sentences[2].End = 999
	pb, _ := proto.Marshal(a)
	st.rows = []store.ResultRow{{Seq: 0, CustomID: "q0", Status: "succeeded", Result: pb}}
	rt.allowed = map[string]bool{"d1": true}
	resp, err := as(t, "alice", "credit", s.ListResults, &batchv1.ListResultsRequest{BatchId: "bat_0a"})
	if err != nil {
		t.Fatal(err)
	}
	if r := resp.GetResults()[0]; r.GetAnswer() != nil || r.GetError().GetType() != "service_unavailable" {
		t.Fatalf("应改为错误: %v", r)
	}
}

func TestResultsFailClosedWhenDocumentRecheckUnavailable(t *testing.T) {
	s, st, rt := newServer()
	pb, _ := proto.Marshal(listingAnswer(1, true, "d1"))
	st.rows = []store.ResultRow{{Seq: 0, CustomID: "q0", Status: "succeeded", Result: pb}}
	rt.allowed = map[string]bool{"d1": true}
	rt.docErr = status.Error(codes.Unavailable, "retrieval 不可用")
	if _, err := as(t, "alice", "credit", s.ListResults, &batchv1.ListResultsRequest{BatchId: "bat_0a"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("无法按文档复核时不应返回结果，得到 %v", err)
	}
}
