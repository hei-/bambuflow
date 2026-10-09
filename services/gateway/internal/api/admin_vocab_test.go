package api

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

type fakeVocab struct {
	adminv1.VocabularyAdminServiceClient
	upsert *adminv1.UpsertTermRequest
	dep    *adminv1.DeprecateTermRequest
}

func (f *fakeVocab) ListTerms(_ context.Context, in *adminv1.ListTermsRequest, _ ...grpc.CallOption) (*adminv1.ListTermsResponse, error) {
	return &adminv1.ListTermsResponse{SnapshotVersion: "vocab-1", Terms: []*adminv1.Term{{Vocabulary: in.GetVocabulary(), Code: "DT-POLICY", Name: "制度", Status: "active"}}}, nil
}

func (f *fakeVocab) UpsertTerm(_ context.Context, in *adminv1.UpsertTermRequest, _ ...grpc.CallOption) (*adminv1.UpsertTermResponse, error) {
	f.upsert = in
	return &adminv1.UpsertTermResponse{Created: true, Term: &adminv1.Term{Vocabulary: in.GetVocabulary(), Code: in.GetCode(), Name: in.GetName(), Status: "active"}}, nil
}

func (f *fakeVocab) DeprecateTerm(_ context.Context, in *adminv1.DeprecateTermRequest, _ ...grpc.CallOption) (*adminv1.DeprecateTermResponse, error) {
	f.dep = in
	return &adminv1.DeprecateTermResponse{Term: &adminv1.Term{Vocabulary: in.GetVocabulary(), Code: in.GetCode(), Status: "deprecated", ReplacedBy: in.GetReplacedBy()}}, nil
}

func TestVocabularyRoutes(t *testing.T) {
	e := newEnv(t)
	f := &fakeVocab{}
	e.server.Vocabularies = f
	paul := e.authz.token(t, "paul", "console")
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }
	if r := e.request(t, http.MethodGet, "/v1/admin/vocabularies/doc_type/terms", paul, "", nil); r.status != 200 || r.body["snapshot_version"] != "vocab-1" {
		t.Fatalf("列表: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodGet, "/v1/admin/vocabularies/colors/terms", paul, "", nil); r.status != 404 {
		t.Fatalf("未知词表应 404: %d", r.status)
	}
	r := e.request(t, http.MethodPost, "/v1/admin/vocabularies/topic/terms", paul, `{"code":"T-AML","name":"反洗钱","synonyms":["AML"]}`, key("a"))
	if r.status != 201 || f.upsert.GetVocabulary() != "topic" || f.upsert.GetSynonyms()[0] != "AML" {
		t.Fatalf("新增: %d %s", r.status, r.raw)
	}
	r = e.request(t, http.MethodPost, "/v1/admin/vocabularies/topic/terms/T-AML/deprecate", paul, `{"replaced_by":"T-COMPLIANCE","reason":"合并"}`, key("b"))
	if r.status != 200 || r.body["status"] != "deprecated" || f.dep.GetReplacedBy() != "T-COMPLIANCE" {
		t.Fatalf("停用: %d %s", r.status, r.raw)
	}
	if r := e.request(t, http.MethodPost, "/v1/admin/vocabularies/topic/terms", e.authz.token(t, "audra", "console"), `{"code":"T-X","name":"x"}`, key("c")); r.status != 403 {
		t.Fatalf("审计岗应 403: %d", r.status)
	}
	if r := e.request(t, http.MethodDelete, "/v1/admin/vocabularies/topic/terms/T-AML", paul, "", nil); r.status != 404 {
		t.Fatalf("没有删除接口: %d", r.status)
	}
}
