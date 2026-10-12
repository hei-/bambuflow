package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

type fakeGolden struct {
	adminv1.GoldenSetAdminServiceClient
	list    *adminv1.ListQuestionsRequest
	submit  *adminv1.SubmitCandidateRequest
	resolve *adminv1.ResolveFeedbackCaseRequest
}

func (f *fakeGolden) ListFeedbackCases(_ context.Context, in *adminv1.ListFeedbackCasesRequest, _ ...grpc.CallOption) (*adminv1.ListFeedbackCasesResponse, error) {
	return &adminv1.ListFeedbackCasesResponse{Cases: []*adminv1.FeedbackCase{{AnswerId: "ans_1", Tenant: in.GetTenant(),
		MaskedQuestion: "额度上限？", Ratings: []string{"wrong"}, Status: "open", AnsweredAt: timestamppb.Now()}}}, nil
}

func (f *fakeGolden) ResolveFeedbackCase(_ context.Context, in *adminv1.ResolveFeedbackCaseRequest, _ ...grpc.CallOption) (*adminv1.ResolveFeedbackCaseResponse, error) {
	f.resolve = in
	return &adminv1.ResolveFeedbackCaseResponse{Case: &adminv1.FeedbackCase{AnswerId: in.GetAnswerId(), Status: in.GetResolution(),
		ResolvedBy: "dana", AnsweredAt: timestamppb.Now()}}, nil
}

func TestFeedbackCaseRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeGolden{}
	e.server.Golden = f
	dana, olga := e.authz.token(t, "dana", "console"), e.authz.token(t, "olga", "console")
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }

	r := e.request(t, http.MethodGet, "/v1/admin/golden-sets/credit/feedback-cases", olga, "", nil)
	if r.status != 200 || r.body["cases"].([]any)[0].(map[string]any)["masked_question"] != "额度上限？" {
		t.Fatalf("列表: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/golden-sets/credit/feedback-cases/ans_1/resolve", olga,
		`{"resolution":"dismissed","note":"x"}`, key("a")); r.status != 403 {
		t.Fatalf("平台运维不能处理: %d", r.status)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/golden-sets/credit/feedback-cases/ans_1/resolve", dana,
		`{"resolution":"converted","note":"cand_0001"}`, key("b"))
	if r.status != 200 || r.body["status"] != "converted" || f.resolve.GetNote() != "cand_0001" || f.resolve.GetTenant() != "credit" {
		t.Fatalf("处理: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/golden-sets/credit/feedback-cases/x1/resolve", dana,
		`{"resolution":"converted","note":"x"}`, key("c")); r.status != 404 {
		t.Fatalf("非法回答 ID: %d", r.status)
	}
}

func (f *fakeGolden) ListQuestions(_ context.Context, in *adminv1.ListQuestionsRequest, _ ...grpc.CallOption) (*adminv1.ListQuestionsResponse, error) {
	f.list = in
	return &adminv1.ListQuestionsResponse{GoldenVersions: []string{"wiki-demo@v3"}, Questions: []*adminv1.GoldenQuestion{
		{QuestionId: "q_wiki_001", Status: "in_set", Expected: &adminv1.GoldenExpected{Status: "answered"}}}}, nil
}

func (f *fakeGolden) SubmitCandidate(_ context.Context, in *adminv1.SubmitCandidateRequest, _ ...grpc.CallOption) (*adminv1.SubmitCandidateResponse, error) {
	f.submit = in
	return &adminv1.SubmitCandidateResponse{Question: &adminv1.GoldenQuestion{QuestionId: "cand_000000000000000000000001", Status: "pending"}}, nil
}

func (f *fakeGolden) ReviewCandidate(context.Context, *adminv1.ReviewCandidateRequest, ...grpc.CallOption) (*adminv1.ReviewCandidateResponse, error) {
	return nil, grpcmw.Reason(codes.PermissionDenied, "self_approval_forbidden", "不能复核自己提交的题")
}

func TestGoldenSetRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeGolden{}
	e.server.Golden = f
	dana, olga := e.authz.token(t, "dana", "console"), e.authz.token(t, "olga", "console")
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }

	r := e.request(t, http.MethodGet, "/v1/admin/golden-sets/credit/questions?include_candidates=true", olga, "", nil)
	if r.status != 200 || !f.list.GetIncludeCandidates() || f.list.GetTenant() != "credit" || len(r.body["questions"].([]any)) != 1 {
		t.Fatalf("列表: %d %s", r.status, r.raw)
	}
	body := `{"golden_set":"wiki-demo","bucket":"single_hop","asker":"credit_l2","question":"最长期限？",` +
		`"expected":{"status":"answered","key_points":["5 年"],"must_cite":[],"must_not_cite":[]},"origin":"feedback"}`
	r = e.request(t, http.MethodPost, "/v1/admin/golden-sets/credit/questions", dana, body, key("a"))
	if r.status != 201 || f.submit.GetExpected().GetKeyPoints()[0] != "5 年" || r.body["status"] != "pending" {
		t.Fatalf("提交: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/golden-sets/credit/questions", olga, body, key("b")); r.status != 403 {
		t.Fatalf("平台运维不能提交: %d", r.status)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/golden-sets/credit/questions/cand_000000000000000000000001/review", dana,
		`{"approved":true}`, key("c"))
	if r.status != 403 || r.body["type"] != "self_approval_forbidden" {
		t.Fatalf("自己复核: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/golden-sets/credit/questions/q_wiki_001/review", dana, `{"approved":true}`, key("d")); r.status != 404 {
		t.Fatalf("已入集的题不能复核: %d", r.status)
	}
}
