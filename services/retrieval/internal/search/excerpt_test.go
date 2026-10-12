package search

import (
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

func excerpt(t *testing.T, s *Server, doc, chunk string) (*retrievalv1.GetExcerptResponse, error) {
	t.Helper()
	return invoke(t, alice(), "mcp", &retrievalv1.GetExcerptRequest{DocId: doc, ChunkId: chunk}, s.GetExcerpt)
}

func TestExcerptReturnsVisibleChunkMasked(t *testing.T) {
	st := newStore()
	c := st.chunks["wiki:loan#1"]
	c.Text = "客户手机 13812345678 已登记"
	st.chunks["wiki:loan#1"] = c
	resp, err := excerpt(t, newServer(t, st), "loan", "wiki:loan#1")
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetChunkId() != "wiki:loan#1" || resp.GetIndexVersion() != "ix-1" || resp.GetPolicyVersion() == "" {
		t.Fatalf("片段字段: %v", resp)
	}
	if strings.Contains(resp.GetText(), "13812345678") {
		t.Fatalf("PII 应掩码: %q", resp.GetText())
	}
}

func TestExcerptChecksAccessBeforeReading(t *testing.T) {
	st := newStore()
	s := newServer(t, st)
	cases := map[string][2]string{
		"密级过高":     {"risk", "wiki:risk#1"},
		"不在群组":     {"handbook", "wiki:handbook#1"},
		"不是租户的数据源": {"mail", "mail:mail#1"},
		"文档不存在":    {"nope", "wiki:nope#1"},
		"块不属于该文档":  {"loan", "wiki:risk#1"},
		"块不存在":     {"loan", "wiki:loan#9"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			st.calls = nil
			_, err := excerpt(t, s, c[0], c[1])
			if status.Code(err) != codes.NotFound {
				t.Fatalf("无权与不存在都应 NOT_FOUND: %v", err)
			}
		})
	}
	// 无权的文档在读正文之前就被拦下
	st.calls = nil
	if _, err := excerpt(t, s, "risk", "wiki:risk#1"); status.Code(err) != codes.NotFound || slices.Contains(st.calls, "Excerpt") {
		t.Fatalf("无权时不应读取正文: %v %v", err, st.calls)
	}
	// 打了删除标记、进了拒绝名单的文档同样不可见
	st.now["loan"] = ACL{SourceID: "wiki", Tokens: []string{"group:credit-l2"}, Classification: 2, Active: true, Denied: true}
	if _, err := excerpt(t, s, "loan", "wiki:loan#1"); status.Code(err) != codes.NotFound {
		t.Fatalf("拒绝名单里的文档应 NOT_FOUND: %v", err)
	}
}

func TestExcerptInvalidRequests(t *testing.T) {
	s := newServer(t, newStore())
	if _, err := excerpt(t, s, "", "wiki:loan#1"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("doc_id 为空应 INVALID_ARGUMENT: %v", err)
	}
	if _, err := invoke(t, alice(), "", &retrievalv1.GetExcerptRequest{DocId: "loan", ChunkId: "wiki:loan#1"}, s.GetExcerpt); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("缺少调用目的应 INVALID_ARGUMENT: %v", err)
	}
}
