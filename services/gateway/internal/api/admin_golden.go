package api

import (
	"context"
	"net/http"
	"regexp"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 评测集题目与候选题复核（evaluation 详设 3.4、8.2）：GET/POST /v1/admin/golden-sets/{tenant}/questions、
// POST .../{question_id}/review，由 evaluation 实现。入集需业务专家（数据属主）与平台（平台管理员）各一人复核。

var (
	goldenTenantPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	candidateIDPattern  = regexp.MustCompile(`^cand_[a-f0-9]{24}$`)
)

type goldenExpectedJSON struct {
	Status      string   `json:"status"`
	KeyPoints   []string `json:"key_points"`
	MustCite    []string `json:"must_cite"`
	MustNotCite []string `json:"must_not_cite"`
}

type goldenQuestionJSON struct {
	QuestionID       string             `json:"question_id"`
	GoldenSet        string             `json:"golden_set"`
	Bucket           string             `json:"bucket"`
	Asker            string             `json:"asker"`
	Question         string             `json:"question"`
	Expected         goldenExpectedJSON `json:"expected"`
	Status           string             `json:"status"`
	Origin           string             `json:"origin"`
	SubmittedBy      string             `json:"submitted_by,omitempty"`
	SubmittedAt      string             `json:"submitted_at,omitempty"`
	ExpertReviewer   string             `json:"expert_reviewer,omitempty"`
	PlatformReviewer string             `json:"platform_reviewer,omitempty"`
	RejectReason     string             `json:"reject_reason,omitempty"`
}

func goldenQuestionOf(q *adminv1.GoldenQuestion) goldenQuestionJSON {
	e := q.GetExpected()
	return goldenQuestionJSON{QuestionID: q.GetQuestionId(), GoldenSet: q.GetGoldenSet(), Bucket: q.GetBucket(), Asker: q.GetAsker(),
		Question: q.GetQuestion(), Expected: goldenExpectedJSON{Status: e.GetStatus(), KeyPoints: nonNil(e.GetKeyPoints()),
			MustCite: nonNil(e.GetMustCite()), MustNotCite: nonNil(e.GetMustNotCite())},
		Status: q.GetStatus(), Origin: q.GetOrigin(), SubmittedBy: q.GetSubmittedBy(), SubmittedAt: rfc3339(q.GetSubmittedAt()),
		ExpertReviewer: q.GetExpertReviewer(), PlatformReviewer: q.GetPlatformReviewer(), RejectReason: q.GetRejectReason()}
}

func goldenTenant(in *adminInput) (string, error) {
	t := in.params["tenant"]
	if !goldenTenantPattern.MatchString(t) {
		return "", problemNotFound
	}
	return t, nil
}

type goldenQuestionListJSON struct {
	Questions      []goldenQuestionJSON `json:"questions"`
	GoldenVersions []string             `json:"golden_versions"`
}

func (s *Server) listGoldenQuestions(ctx context.Context, in *adminInput) (adminOutput, error) {
	tenant, err := goldenTenant(in)
	if err != nil {
		return adminOutput{}, err
	}
	include := false
	switch in.query.Get("include_candidates") {
	case "", "false":
	case "true":
		include = true
	default:
		return adminOutput{}, problemInvalid
	}
	r, err := s.Golden.ListQuestions(ctx, &adminv1.ListQuestionsRequest{Tenant: tenant, IncludeCandidates: include})
	if err != nil {
		return adminOutput{}, err
	}
	out := goldenQuestionListJSON{Questions: []goldenQuestionJSON{}, GoldenVersions: nonNil(r.GetGoldenVersions())}
	for _, q := range r.GetQuestions() {
		out.Questions = append(out.Questions, goldenQuestionOf(q))
	}
	return adminOutput{body: out}, nil
}

type submitCandidateJSON struct {
	GoldenSet string             `json:"golden_set"`
	Bucket    string             `json:"bucket"`
	Asker     string             `json:"asker"`
	Question  string             `json:"question"`
	Expected  goldenExpectedJSON `json:"expected"`
	Origin    string             `json:"origin"`
}

func (s *Server) submitCandidate(ctx context.Context, in *adminInput) (adminOutput, error) {
	tenant, err := goldenTenant(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req submitCandidateJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	e := req.Expected
	r, err := s.Golden.SubmitCandidate(ctx, &adminv1.SubmitCandidateRequest{Tenant: tenant, GoldenSet: req.GoldenSet, Bucket: req.Bucket,
		Asker: req.Asker, Question: req.Question, Origin: req.Origin, Expected: &adminv1.GoldenExpected{Status: e.Status,
			KeyPoints: e.KeyPoints, MustCite: e.MustCite, MustNotCite: e.MustNotCite}})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{status: http.StatusCreated, body: goldenQuestionOf(r.GetQuestion())}, nil
}

type reviewCandidateJSON struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason"`
}

func (s *Server) reviewCandidate(ctx context.Context, in *adminInput) (adminOutput, error) {
	tenant, err := goldenTenant(in)
	if err != nil {
		return adminOutput{}, err
	}
	id := in.params["question_id"]
	if !candidateIDPattern.MatchString(id) {
		return adminOutput{}, problemNotFound
	}
	var req reviewCandidateJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Golden.ReviewCandidate(ctx, &adminv1.ReviewCandidateRequest{Tenant: tenant, QuestionId: id, Approved: req.Approved,
		Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: goldenQuestionOf(r.GetQuestion())}, nil
}

// 负反馈案例（技术设计文档 F8，evaluation 详设 3.4）：GET .../feedback-cases、POST .../feedback-cases/{answer_id}/resolve。

type feedbackCaseJSON struct {
	AnswerID       string   `json:"answer_id"`
	Tenant         string   `json:"tenant"`
	AnsweredAt     string   `json:"answered_at"`
	MaskedQuestion string   `json:"masked_question"`
	AnswerStatus   string   `json:"answer_status"`
	Ratings        []string `json:"ratings"`
	Comments       []string `json:"comments"`
	CitedDocIDs    []string `json:"cited_doc_ids"`
	CitedTitles    []string `json:"cited_titles"`
	Status         string   `json:"status"`
	ResolvedBy     string   `json:"resolved_by,omitempty"`
	ResolutionNote string   `json:"resolution_note,omitempty"`
	ResolvedAt     string   `json:"resolved_at,omitempty"`
}

func feedbackCaseOf(c *adminv1.FeedbackCase) feedbackCaseJSON {
	return feedbackCaseJSON{AnswerID: c.GetAnswerId(), Tenant: c.GetTenant(), AnsweredAt: rfc3339(c.GetAnsweredAt()),
		MaskedQuestion: c.GetMaskedQuestion(), AnswerStatus: c.GetAnswerStatus(), Ratings: nonNil(c.GetRatings()),
		Comments: nonNil(c.GetComments()), CitedDocIDs: nonNil(c.GetCitedDocIds()), CitedTitles: nonNil(c.GetCitedTitles()),
		Status: c.GetStatus(), ResolvedBy: c.GetResolvedBy(), ResolutionNote: c.GetResolutionNote(), ResolvedAt: rfc3339(c.GetResolvedAt())}
}

type feedbackCaseListJSON struct {
	Cases []feedbackCaseJSON `json:"cases"`
}

func (s *Server) listFeedbackCases(ctx context.Context, in *adminInput) (adminOutput, error) {
	tenant, err := goldenTenant(in)
	if err != nil {
		return adminOutput{}, err
	}
	r, err := s.Golden.ListFeedbackCases(ctx, &adminv1.ListFeedbackCasesRequest{Tenant: tenant, Status: in.query.Get("status")})
	if err != nil {
		return adminOutput{}, err
	}
	out := feedbackCaseListJSON{Cases: []feedbackCaseJSON{}}
	for _, c := range r.GetCases() {
		out.Cases = append(out.Cases, feedbackCaseOf(c))
	}
	return adminOutput{body: out}, nil
}

type resolveFeedbackCaseJSON struct {
	Resolution string `json:"resolution"`
	Note       string `json:"note"`
}

func (s *Server) resolveFeedbackCase(ctx context.Context, in *adminInput) (adminOutput, error) {
	tenant, err := goldenTenant(in)
	if err != nil {
		return adminOutput{}, err
	}
	id := in.params["answer_id"]
	if !answerIDPattern.MatchString(id) {
		return adminOutput{}, problemNotFound
	}
	var req resolveFeedbackCaseJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	r, err := s.Golden.ResolveFeedbackCase(ctx, &adminv1.ResolveFeedbackCaseRequest{Tenant: tenant, AnswerId: id,
		Resolution: req.Resolution, Note: req.Note})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: feedbackCaseOf(r.GetCase())}, nil
}
