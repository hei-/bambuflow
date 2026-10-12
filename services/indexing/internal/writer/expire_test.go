package writer

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

func TestParseRetention(t *testing.T) {
	got, err := ParseRetention([]byte("retention_days:\n  src_mail: 2555\n  src_wiki_logs: 30\n"))
	if err != nil || got["src_mail"] != 2555 || got["src_wiki_logs"] != 30 {
		t.Fatalf("%v %v", got, err)
	}
	for _, bad := range []string{"retention_days:\n  src_mail: 0\n", "retention_days:\n  mail: 30\n", "retention_days: [1]\n"} {
		if _, err := ParseRetention([]byte(bad)); err == nil {
			t.Errorf("%q 应拒绝", bad)
		}
	}
	// 参数不合法在读库之前拒绝（Pool 为 nil 也不出错）
	a := &Admin{Log: quiet}
	_, err = a.ExpireDocuments(context.Background(), &adminv1.ExpireDocumentsRequest{RetentionDays: map[string]int32{"src_x": -1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("%v", err)
	}
}

func TestPurgeDeletedRejectsNegativeGrace(t *testing.T) {
	a := &Admin{Log: quiet}
	if _, err := a.PurgeDeleted(context.Background(), &adminv1.PurgeDeletedRequest{GraceHours: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("%v", err)
	}
	// 每一步删除都记进凭证：块（含关键词与向量）、章节、图谱、版本清单、元数据
	var stores []string
	for _, s := range purgeSteps {
		stores = append(stores, s.store)
	}
	if strings.Join(stores, ",") != "graph,sections,manifests,chunks,metadata" {
		t.Fatalf("%v", stores)
	}
}

func TestRemapTermsValidatesBeforeTouchingStore(t *testing.T) {
	a := &Admin{Log: quiet}
	for _, m := range []*adminv1.TermRemap{
		{Vocabulary: "color", FromCode: "DT-A", ToCode: "DT-B"},
		{Vocabulary: "doc_type", FromCode: "dt-a", ToCode: "DT-B"},
		{Vocabulary: "topic", FromCode: "T-A", ToCode: "T-A"},
	} {
		if _, err := a.RemapTerms(context.Background(), &adminv1.RemapTermsRequest{Remaps: []*adminv1.TermRemap{m}}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%v 应拒绝: %v", m, err)
		}
	}
}
