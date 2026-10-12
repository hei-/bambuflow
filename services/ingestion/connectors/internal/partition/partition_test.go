package partition

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/docmeta"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/publish"
)

var quiet = slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "partitions.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadValidates(t *testing.T) {
	g, err := Load(write(t, "partitions:\n  制度库/风险管理: [dept:风险管理部, group:risk, dept:风险管理部]\n"))
	if err != nil || strings.Join(g["制度库/风险管理"], ",") != "dept:风险管理部,group:risk" {
		t.Fatalf("%v %v", g, err)
	}
	for name, body := range map[string]string{
		"被授权方格式不对": "partitions:\n  制度库: [风险管理部]\n",
		"分区名有空格":   "partitions:\n  \"制度 库\": [dept:x]\n",
		"未知字段":     "partition:\n  制度库: [dept:x]\n",
	} {
		if _, err := Load(write(t, body)); err == nil {
			t.Errorf("%s 应报错", name)
		}
	}
}

func TestApplyUsesPartitionTokenExceptForExplicitACL(t *testing.T) {
	doc := func(id, part string, explicit bool) *publish.Document {
		return &publish.Document{Meta: docmeta.Meta{DocID: id, Classification: "internal", ACLTokens: []string{"user:owner"},
			Partition: part, ExplicitACL: explicit}}
	}
	docs := []*publish.Document{doc("a", "制度库/风险管理", false), doc("b", "制度库/风险管理", true), doc("c", "", false)}
	if err := Apply(docs, "src_rules", Grants{"制度库/风险管理": {"dept:风险管理部"}}, quiet); err != nil {
		t.Fatal(err)
	}
	if strings.Join(docs[0].ACLTokens, ",") != "part:src_rules/制度库/风险管理" || docs[0].ACLFingerprint != docmeta.Fingerprint(docs[0].ACLTokens, "internal") {
		t.Fatalf("分区文档只带分区 token: %+v", docs[0].Meta)
	}
	if docs[1].ACLTokens[0] != "user:owner" || docs[2].ACLTokens[0] != "user:owner" {
		t.Fatalf("自带 ACL 与没写分区的按文档级: %+v %+v", docs[1].Meta, docs[2].Meta)
	}
}

type emitted struct {
	partition string
	grantees  []string
}

type fakeEmitter struct{ got []emitted }

func (f *fakeEmitter) PartitionGrants(_ context.Context, partition string, grantees []string, _ time.Time) error {
	f.got = append(f.got, emitted{partition, grantees})
	return nil
}

func TestSyncEmitsOnlyChanges(t *testing.T) {
	mr := miniredis.RunT(t)
	st := RedisState{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()}), SourceID: "src_rules"}
	ctx := context.Background()
	em := &fakeEmitter{}
	grants := Grants{"制度库/风险管理": {"dept:风险管理部"}, "制度库/信贷": {"group:credit"}}
	if n, err := Sync(ctx, grants, st, em, time.Now()); err != nil || n != 2 {
		t.Fatalf("首次全部发出: %d %v", n, err)
	}
	if n, _ := Sync(ctx, grants, st, em, time.Now()); n != 0 {
		t.Fatalf("没变化不发: %d", n)
	}
	// 收回信贷分区、改风险分区
	grants = Grants{"制度库/风险管理": {"dept:风险管理部", "group:auditors"}}
	if n, err := Sync(ctx, grants, st, em, time.Now()); err != nil || n != 2 {
		t.Fatalf("变化: %d %v", n, err)
	}
	last := em.got[2:]
	if last[0].partition != "制度库/风险管理" || len(last[0].grantees) != 2 || last[1].partition != "制度库/信贷" || last[1].grantees != nil {
		t.Fatalf("删掉的分区发空列表: %+v", last)
	}
}
