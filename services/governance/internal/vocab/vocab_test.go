package vocab

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

func TestDevSeedIsValid(t *testing.T) {
	terms, err := LoadFile("../../../../tools/test-upstream/vocab.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(terms) < 10 {
		t.Fatalf("词条太少: %d", len(terms))
	}
}

func TestValidateRejectsBrokenTerms(t *testing.T) {
	cases := map[string]string{
		"重复编码":   "doc_type:\n  - {code: A, name: 甲}\n  - {code: A, name: 乙}\n",
		"停用没有替代": "doc_type:\n  - {code: A, name: 甲, status: deprecated}\n",
		"上级不存在":  "topic:\n  - {code: A, name: 甲, parent: X}\n",
		"未知词表":   "color:\n  - {code: A, name: 甲}\n",
	}
	for name, body := range cases {
		path := filepath.Join(t.TempDir(), "v.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadFile(path); err == nil {
			t.Errorf("%s 应当校验失败", name)
		}
	}
}

type fixed []Term

func (f fixed) Snapshot(context.Context) (string, []Term, error) { return Version(f), f, nil }

func TestSnapshotGroupsTermsByVocabulary(t *testing.T) {
	terms := fixed{
		{Vocabulary: "doc_type", Code: "DT-POLICY", Name: "制度", Synonyms: []string{"办法"}, Status: "active"},
		{Vocabulary: "doc_type", Code: "DT-RULE", Name: "规定", Status: "deprecated", ReplacedBy: "DT-POLICY"},
		{Vocabulary: "topic", Code: "T-1", Name: "信贷", Status: "active"},
	}
	s := &Server{Store: terms, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	resp, err := s.GetSnapshot(context.Background(), &configv1.GetSnapshotRequest{Service: "processing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetVocabularies()) != 3 || !strings.HasPrefix(resp.GetSnapshotVersion(), "vocab-") {
		t.Fatalf("快照: %v", resp)
	}
	dt := resp.GetVocabularies()[0]
	if dt.GetName() != "doc_type" || len(dt.GetTerms()) != 2 || dt.GetTerms()[1].GetStatus() != configv1.TermStatus_TERM_STATUS_DEPRECATED {
		t.Fatalf("文档类型词表: %v", dt)
	}
	// 内容不变则版本不变，内容一变版本就变
	if Version(terms) != Version(fixed(append([]Term{}, terms...))) || Version(terms) == Version(terms[:2]) {
		t.Fatal("快照版本应随内容变化")
	}
}
