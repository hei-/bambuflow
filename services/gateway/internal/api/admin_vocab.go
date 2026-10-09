package api

import (
	"context"
	"net/http"
	"regexp"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 受控词表（governance 详设 6.1、7.2）：/v1/admin/vocabularies/{vocabulary}/terms，由 governance 实现，平台管理员维护。

var (
	vocabularyNames = map[string]bool{"doc_type": true, "dept": true, "topic": true}
	termCodePattern = regexp.MustCompile(`^[A-Z][A-Z0-9-]{1,63}$`)
)

type termJSON struct {
	Vocabulary string   `json:"vocabulary"`
	Code       string   `json:"code"`
	Name       string   `json:"name"`
	Synonyms   []string `json:"synonyms"`
	Parent     string   `json:"parent,omitempty"`
	Status     string   `json:"status"`
	ReplacedBy string   `json:"replaced_by,omitempty"`
	Owner      string   `json:"owner,omitempty"`
}

func termOf(t *adminv1.Term) termJSON {
	return termJSON{Vocabulary: t.GetVocabulary(), Code: t.GetCode(), Name: t.GetName(), Synonyms: nonNil(t.GetSynonyms()),
		Parent: t.GetParent(), Status: t.GetStatus(), ReplacedBy: t.GetReplacedBy(), Owner: t.GetOwner()}
}

func vocabularyOf(in *adminInput) (string, error) {
	v := in.params["vocabulary"]
	if !vocabularyNames[v] {
		return "", problemNotFound
	}
	return v, nil
}

type termListJSON struct {
	Terms           []termJSON `json:"terms"`
	SnapshotVersion string     `json:"snapshot_version"`
}

func (s *Server) listTerms(ctx context.Context, in *adminInput) (adminOutput, error) {
	v, err := vocabularyOf(in)
	if err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Vocabularies.ListTerms(ctx, &adminv1.ListTermsRequest{Vocabulary: v})
	if err != nil {
		return adminOutput{}, err
	}
	out := termListJSON{Terms: []termJSON{}, SnapshotVersion: resp.GetSnapshotVersion()}
	for _, t := range resp.GetTerms() {
		out.Terms = append(out.Terms, termOf(t))
	}
	return adminOutput{body: out}, nil
}

type upsertTermJSON struct {
	Code     string   `json:"code"`
	Name     string   `json:"name"`
	Synonyms []string `json:"synonyms"`
	Parent   string   `json:"parent"`
	Owner    string   `json:"owner"`
}

func (s *Server) upsertTerm(ctx context.Context, in *adminInput) (adminOutput, error) {
	v, err := vocabularyOf(in)
	if err != nil {
		return adminOutput{}, err
	}
	var req upsertTermJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Vocabularies.UpsertTerm(ctx, &adminv1.UpsertTermRequest{Vocabulary: v, Code: req.Code, Name: req.Name,
		Synonyms: req.Synonyms, Parent: req.Parent, Owner: req.Owner})
	if err != nil {
		return adminOutput{}, err
	}
	code := http.StatusOK
	if resp.GetCreated() {
		code = http.StatusCreated
	}
	return adminOutput{status: code, body: termOf(resp.GetTerm())}, nil
}

type deprecateTermJSON struct {
	ReplacedBy string `json:"replaced_by"`
	Reason     string `json:"reason"`
}

func (s *Server) deprecateTerm(ctx context.Context, in *adminInput) (adminOutput, error) {
	v, err := vocabularyOf(in)
	if err != nil {
		return adminOutput{}, err
	}
	code := in.params["code"]
	if !termCodePattern.MatchString(code) {
		return adminOutput{}, problemNotFound
	}
	var req deprecateTermJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Vocabularies.DeprecateTerm(ctx, &adminv1.DeprecateTermRequest{Vocabulary: v, Code: code,
		ReplacedBy: req.ReplacedBy, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	return adminOutput{body: termOf(resp.GetTerm())}, nil
}
