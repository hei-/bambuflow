package vocab

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
)

type memTerms struct{ terms []Term }

func (m *memTerms) Snapshot(context.Context) (string, []Term, error) {
	return Version(m.terms), append([]Term{}, m.terms...), nil
}

func (m *memTerms) Upsert(_ context.Context, terms []Term, beforeCommit func() error) error {
	if err := beforeCommit(); err != nil {
		return err
	}
	for _, t := range terms {
		replaced := false
		for i := range m.terms {
			if m.terms[i].Vocabulary == t.Vocabulary && m.terms[i].Code == t.Code {
				m.terms[i], replaced = t, true
			}
		}
		if !replaced {
			m.terms = append(m.terms, t)
		}
	}
	return nil
}

type memAudit struct{ actions []string }

func (a *memAudit) Write(_ context.Context, action, _ string, _ any) error {
	a.actions = append(a.actions, action)
	return nil
}

type staticVerifier struct{ p *identityv1.Principal }

func (v staticVerifier) Verify(context.Context, string) (*identityv1.Principal, error) {
	return v.p, nil
}

func as(t *testing.T, roles ...string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer x"))
	var out context.Context
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := grpcmw.UnaryServer(staticVerifier{&identityv1.Principal{Sub: "paul", Roles: roles}}, log)(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/x/y"}, func(ctx context.Context, _ any) (any, error) { out = ctx; return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestVocabularyAdmin(t *testing.T) {
	store := &memTerms{terms: []Term{{Vocabulary: "doc_type", Code: "DT-POLICY", Name: "制度", Status: "active"}}}
	audit := &memAudit{}
	a := &Admin{Store: store, Audit: audit, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx := as(t, RolePlatformAdmin)

	if _, err := a.ListTerms(as(t, "auditor"), &adminv1.ListTermsRequest{Vocabulary: "doc_type"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("非平台管理员应拒绝: %v", err)
	}
	created, err := a.UpsertTerm(ctx, &adminv1.UpsertTermRequest{Vocabulary: "doc_type", Code: "DT-RULE", Name: "规定", Synonyms: []string{"细则"}})
	if err != nil || !created.GetCreated() || created.GetTerm().GetStatus() != "active" {
		t.Fatalf("新增: %v %v", created, err)
	}
	if _, err := a.UpsertTerm(ctx, &adminv1.UpsertTermRequest{Vocabulary: "doc_type", Code: "DT-X", Name: "x", Parent: "NOPE"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("上级不存在应拒绝: %v", err)
	}
	if _, err := a.UpsertTerm(ctx, &adminv1.UpsertTermRequest{Vocabulary: "doc_type", Code: "bad code", Name: "x"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("编码格式不对应拒绝: %v", err)
	}
	if _, err := a.DeprecateTerm(ctx, &adminv1.DeprecateTermRequest{Vocabulary: "doc_type", Code: "DT-RULE", ReplacedBy: "DT-NONE", Reason: "合并"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("替代编码不存在应拒绝: %v", err)
	}
	dep, err := a.DeprecateTerm(ctx, &adminv1.DeprecateTermRequest{Vocabulary: "doc_type", Code: "DT-RULE", ReplacedBy: "DT-POLICY", Reason: "合并为制度"})
	if err != nil || dep.GetTerm().GetStatus() != "deprecated" || dep.GetTerm().GetReplacedBy() != "DT-POLICY" {
		t.Fatalf("停用: %v %v", dep, err)
	}
	// 已停用的不能经修改重新启用
	_, err = a.UpsertTerm(ctx, &adminv1.UpsertTermRequest{Vocabulary: "doc_type", Code: "DT-RULE", Name: "规定"})
	if status.Code(err) != codes.FailedPrecondition || grpcmw.ReasonOf(err) != "conflict" {
		t.Fatalf("重新启用应拒绝: %v", err)
	}
	list, err := a.ListTerms(ctx, &adminv1.ListTermsRequest{Vocabulary: "doc_type"})
	if err != nil || len(list.GetTerms()) != 2 || list.GetSnapshotVersion() == "" {
		t.Fatalf("列表: %v %v", list, err)
	}
	if len(audit.actions) != 2 || audit.actions[0] != "vocabulary.upsert" || audit.actions[1] != "vocabulary.deprecate" {
		t.Fatalf("审计: %v", audit.actions)
	}
}

func TestRemapsFollowReplacementChain(t *testing.T) {
	terms := []Term{
		{Vocabulary: "doc_type", Code: "DT-A", Status: "deprecated", ReplacedBy: "DT-B"},
		{Vocabulary: "doc_type", Code: "DT-B", Status: "deprecated", ReplacedBy: "DT-C"},
		{Vocabulary: "doc_type", Code: "DT-C", Status: "active"},
		{Vocabulary: "topic", Code: "T-X", Status: "deprecated", ReplacedBy: "T-GONE"},
		{Vocabulary: "topic", Code: "T-L1", Status: "deprecated", ReplacedBy: "T-L2"},
		{Vocabulary: "topic", Code: "T-L2", Status: "deprecated", ReplacedBy: "T-L1"},
	}
	got := Remaps(terms)
	if len(got) != 2 || got[0].GetFromCode() != "DT-A" || got[0].GetToCode() != "DT-C" ||
		got[1].GetFromCode() != "DT-B" || got[1].GetToCode() != "DT-C" {
		t.Fatalf("链要跟到在用的编码，断链与成环的跳过: %v", got)
	}
}
