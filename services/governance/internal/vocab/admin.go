package vocab

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
)

// RolePlatformAdmin 维护受控词表（governance 详设 9.1；设计写“各业务文档归口部门”，平台角色里还没有这一类）。
const RolePlatformAdmin = "platform_admin"

// TermStore 是词表的读写（生产为 Store）。
type TermStore interface {
	Snapshot(ctx context.Context) (string, []Term, error)
	Upsert(ctx context.Context, terms []Term, beforeCommit func() error) error
}

// Auditor 写变更审计（internal/changeaudit）。
type Auditor interface {
	Write(ctx context.Context, action, objectID string, detail any) error
}

// Admin 实现 VocabularyAdminService（governance 详设 6.1、7.2）。每次改动都对整套词表重新校验，
// 与变更审计同一事务提交；改动立即进入下一版配置快照。
type Admin struct {
	adminv1.UnimplementedVocabularyAdminServiceServer
	Store TermStore
	Audit Auditor
	Log   *slog.Logger
	// Remap 停用词条后把已入库文档的编码改成替代编码；为 nil 时不做（单元测试）
	Remap *Remapper
}

var codePattern = regexp.MustCompile(`^[A-Z][A-Z0-9-]{1,63}$`)

func (a *Admin) admin(ctx context.Context) error {
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), RolePlatformAdmin) {
		return status.Error(codes.PermissionDenied, "需要平台管理员")
	}
	return nil
}

func (a *Admin) load(ctx context.Context) (string, []Term, error) {
	v, terms, err := a.Store.Snapshot(ctx)
	if err != nil {
		a.Log.ErrorContext(ctx, "读取词表失败", slog.Any("err", err))
		return "", nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	return v, terms, nil
}

func (a *Admin) ListTerms(ctx context.Context, r *adminv1.ListTermsRequest) (*adminv1.ListTermsResponse, error) {
	if err := a.admin(ctx); err != nil {
		return nil, err
	}
	if !slices.Contains(Names, r.GetVocabulary()) {
		return nil, status.Error(codes.NotFound, "词表不存在")
	}
	v, terms, err := a.load(ctx)
	if err != nil {
		return nil, err
	}
	out := &adminv1.ListTermsResponse{SnapshotVersion: v}
	for _, t := range terms {
		if t.Vocabulary == r.GetVocabulary() {
			out.Terms = append(out.Terms, t.proto())
		}
	}
	return out, nil
}

func short(s string, n int) bool {
	c := utf8.RuneCountInString(strings.TrimSpace(s))
	return c > 0 && c <= n
}

func (a *Admin) UpsertTerm(ctx context.Context, r *adminv1.UpsertTermRequest) (*adminv1.UpsertTermResponse, error) {
	if err := a.admin(ctx); err != nil {
		return nil, err
	}
	if !slices.Contains(Names, r.GetVocabulary()) || !codePattern.MatchString(r.GetCode()) || !short(r.GetName(), 100) ||
		len(r.GetSynonyms()) > 50 || (r.GetOwner() != "" && !short(r.GetOwner(), 100)) {
		return nil, status.Error(codes.InvalidArgument, "vocabulary、code、name、synonyms 或 owner 不合法")
	}
	syns := []string{}
	for _, s := range r.GetSynonyms() {
		if !short(s, 100) {
			return nil, status.Error(codes.InvalidArgument, "同义词不能为空或超过 100 字")
		}
		syns = append(syns, strings.TrimSpace(s))
	}
	_, terms, err := a.load(ctx)
	if err != nil {
		return nil, err
	}
	t := Term{Vocabulary: r.GetVocabulary(), Code: r.GetCode(), Name: strings.TrimSpace(r.GetName()), Synonyms: syns,
		Parent: r.GetParent(), Status: "active", Owner: strings.TrimSpace(r.GetOwner())}
	i := slices.IndexFunc(terms, func(x Term) bool { return x.Vocabulary == t.Vocabulary && x.Code == t.Code })
	created := i < 0
	if created {
		terms = append(terms, t)
	} else {
		if terms[i].Status == "deprecated" {
			return nil, grpcmw.Reason(codes.FailedPrecondition, "conflict", ErrReactivate.Error())
		}
		terms[i] = t
	}
	if err := Validate(terms); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := a.Store.Upsert(ctx, []Term{t}, func() error {
		return a.Audit.Write(ctx, "vocabulary.upsert", t.Vocabulary+"/"+t.Code,
			map[string]any{"name": t.Name, "synonyms": t.Synonyms, "parent": t.Parent, "owner": t.Owner, "created": created})
	}); err != nil {
		return nil, a.storeErr(ctx, err)
	}
	return &adminv1.UpsertTermResponse{Term: t.proto(), Created: created}, nil
}

func (a *Admin) DeprecateTerm(ctx context.Context, r *adminv1.DeprecateTermRequest) (*adminv1.DeprecateTermResponse, error) {
	if err := a.admin(ctx); err != nil {
		return nil, err
	}
	if !slices.Contains(Names, r.GetVocabulary()) || !codePattern.MatchString(r.GetCode()) ||
		!codePattern.MatchString(r.GetReplacedBy()) || !short(r.GetReason(), 500) || r.GetReplacedBy() == r.GetCode() {
		return nil, status.Error(codes.InvalidArgument, "vocabulary、code、replaced_by 或 reason 不合法")
	}
	_, terms, err := a.load(ctx)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(terms, func(x Term) bool { return x.Vocabulary == r.GetVocabulary() && x.Code == r.GetCode() })
	if i < 0 {
		return nil, status.Error(codes.NotFound, "词条不存在")
	}
	if terms[i].Status == "deprecated" {
		return &adminv1.DeprecateTermResponse{Term: terms[i].proto()}, nil
	}
	j := slices.IndexFunc(terms, func(x Term) bool { return x.Vocabulary == r.GetVocabulary() && x.Code == r.GetReplacedBy() })
	if j < 0 || terms[j].Status != "active" {
		return nil, status.Error(codes.InvalidArgument, "替代编码必须是同一词表里的有效词条")
	}
	t := terms[i]
	t.Status, t.ReplacedBy = "deprecated", r.GetReplacedBy()
	terms[i] = t
	if err := Validate(terms); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := a.Store.Upsert(ctx, []Term{t}, func() error {
		return a.Audit.Write(ctx, "vocabulary.deprecate", t.Vocabulary+"/"+t.Code,
			map[string]string{"replaced_by": t.ReplacedBy, "reason": strings.TrimSpace(r.GetReason())})
	}); err != nil {
		return nil, a.storeErr(ctx, err)
	}
	if a.Remap != nil {
		// 已入库文档按替代编码重新标注：后台执行，失败由定期重放兜底，不影响停用本身
		go func() {
			if err := a.Remap.Apply(context.WithoutCancel(ctx)); err != nil {
				a.Log.WarnContext(ctx, "停用后重新标注失败，等定期重放", slog.Any("err", err))
			}
		}()
	}
	return &adminv1.DeprecateTermResponse{Term: t.proto()}, nil
}

func (a *Admin) storeErr(ctx context.Context, err error) error {
	if errors.Is(err, ErrReactivate) {
		return grpcmw.Reason(codes.FailedPrecondition, "conflict", ErrReactivate.Error())
	}
	a.Log.ErrorContext(ctx, "写词表失败", slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}

func (t Term) proto() *adminv1.Term {
	return &adminv1.Term{Vocabulary: t.Vocabulary, Code: t.Code, Name: t.Name, Synonyms: nonNil(t.Synonyms), Parent: t.Parent,
		Status: t.Status, ReplacedBy: t.ReplacedBy, Owner: t.Owner}
}
