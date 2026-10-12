package dbschema

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/publish"
)

func cfg() *Config {
	return &Config{Database: "bankdemo", Classification: "internal",
		Views: []string{"bankdemo.v_loan_balance", "bankdemo.v_branch_daily"},
		Roles: map[string][]string{"report_ops": {"group:sre-oncall"}, "report_finance": {"group:sre-core"}}}
}

func loanView() View {
	return View{Schema: "bankdemo", Name: "v_loan_balance", Comment: "贷款余额视图。",
		Columns:  []Column{{"loan_id", "bigint", "贷款编号"}, {"balance", "numeric(18,2)", "贷款余额（元）"}, {"note", "text", ""}},
		Grantees: []string{"report_ops", "unknown_role"}}
}

func branchView() View {
	return View{Schema: "bankdemo", Name: "v_branch_daily", Columns: []Column{{"branch", "text", "网点"}},
		Grantees: []string{"report_finance", "report_ops"}}
}

func TestDocumentIsStructureOnly(t *testing.T) {
	at := time.Date(2026, 10, 7, 8, 0, 0, 0, time.UTC)
	d := cfg().Document(loanView(), "db://", at)
	want := "# 视图 bankdemo.v_loan_balance\n\n所属库：bankdemo；对象类型：视图（只同步表结构与字段说明，不含行数据）。\n\n" +
		"贷款余额视图。\n\n## 字段\n\n- loan_id（bigint）：贷款编号\n- balance（numeric(18,2)）：贷款余额（元）\n- note（text）\n"
	if d.Text != want {
		t.Fatalf("正文:\n%s", d.Text)
	}
	if d.DocID != "bankdemo-bankdemo-v-loan-balance" || d.SourceType != "db" || d.SourceURI != "db://bankdemo/bankdemo.v_loan_balance" {
		t.Fatalf("标识: %s %s %s", d.DocID, d.SourceType, d.SourceURI)
	}
	if d.Title != "视图 bankdemo.v_loan_balance（bankdemo 库，同步于 2026-10-07 08:00 UTC）" {
		t.Fatalf("标题应带库名、视图名与同步时间: %s", d.Title)
	}
	// 没有映射的角色不给任何人权限
	if !slices.Equal(d.ACLTokens, []string{"group:sre-oncall"}) {
		t.Fatalf("权限来自视图授权: %v", d.ACLTokens)
	}
	// 同步时间只在标题里，不进正文：表结构哈希与它无关
	if d2 := cfg().Document(loanView(), "db://", at.Add(time.Hour)); d2.ContentHash != d.ContentHash {
		t.Fatal("同一表结构的哈希应相同")
	}
}

type fakeCatalog struct {
	views []View
	err   error
}

func (f *fakeCatalog) Views(_ context.Context, q []string) ([]View, error) {
	var out []View
	for _, v := range f.views {
		if slices.Contains(q, v.Schema+"."+v.Name) {
			out = append(out, v)
		}
	}
	return out, f.err
}

type kv map[string]string

func (k kv) All(context.Context) (map[string]string, error) {
	out := map[string]string{}
	for a, b := range k {
		out[a] = b
	}
	return out, nil
}
func (k kv) Set(_ context.Context, id, v string) error { k[id] = v; return nil }
func (k kv) Del(_ context.Context, id string) error    { delete(k, id); return nil }

type event struct {
	change ingestionv1.ChangeType
	doc    *publish.Document
	del    string
}

type fakePublisher struct{ events []event }

func (f *fakePublisher) Publish(_ context.Context, d *publish.Document, c ingestionv1.ChangeType, _ time.Time) error {
	f.events = append(f.events, event{change: c, doc: d})
	return nil
}

func (f *fakePublisher) Delete(_ context.Context, id string, _ time.Time) error {
	f.events = append(f.events, event{del: id})
	return nil
}

func TestSyncLifecycle(t *testing.T) {
	cat := &fakeCatalog{views: []View{loanView(), branchView(),
		// 不在白名单里的视图不同步
		{Schema: "bankdemo", Name: "v_customer_contact", Columns: []Column{{"phone", "text", ""}}}}}
	pub, out := kv{}, &fakePublisher{}
	t0 := time.Date(2026, 10, 7, 8, 0, 0, 0, time.UTC)
	now := t0
	s := &Syncer{Config: cfg(), Catalog: cat, Published: pub, Synced: kv{}, Publisher: out, BaseURI: "db://",
		Now: func() time.Time { return now }}
	run := func() Stats {
		t.Helper()
		out.events = nil
		st, err := s.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	if st := run(); st.Views != 2 || st.Upserts != 2 {
		t.Fatalf("首轮: %+v", st)
	}
	// 一小时后再跑，表结构没变：不发任何事件
	now = t0.Add(time.Hour)
	if st := run(); st.Upserts+st.ACLOnly+st.Deletes != 0 || len(out.events) != 0 {
		t.Fatalf("表结构不变时不应重发: %+v", st)
	}
	// 授权变了：只发 ACL_ONLY，标题里的同步时间仍是这版表结构第一次被发现的时间
	cat.views[0].Grantees = []string{"report_ops", "report_finance"}
	run()
	if len(out.events) != 1 || out.events[0].change != ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY ||
		!strings.Contains(out.events[0].doc.Title, "同步于 2026-10-07 08:00 UTC") {
		t.Fatalf("授权变化: %+v", out.events)
	}
	// 加了一列：按表结构哈希判断为变更
	cat.views[0].Columns = append(cat.views[0].Columns, Column{"rate", "numeric", "利率"})
	if st := run(); st.Upserts != 1 || !strings.Contains(out.events[0].doc.Title, "同步于 2026-10-07 09:00 UTC") {
		t.Fatalf("表结构变更: %+v %v", st, out.events)
	}
	// 视图从系统表中消失：超过删除保护阈值时暂停，管理员确认后删除
	cat.views = cat.views[:1]
	if st := run(); st.Deletes != 0 || st.DeletesPaused != "over_threshold" {
		t.Fatalf("删除保护: %+v", st)
	}
	s.ConfirmDeletes = true
	if st := run(); st.Deletes != 1 || out.events[0].del != "bankdemo-bankdemo-v-branch-daily" {
		t.Fatalf("对象消失即删除: %+v %v", st, out.events)
	}
	// 元数据读取失败：整轮失败，不推断删除
	cat.err = errors.New("down")
	if _, err := s.Run(context.Background()); err == nil {
		t.Fatal("元数据不可用时应失败")
	}
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	write := func(s string) string {
		p := filepath.Join(dir, "c.yaml")
		if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	c, err := LoadConfig(write("database: bankdemo\nviews: [bankdemo.v_a]\nroles: {r: [group:x]}\n"))
	if err != nil || c.Classification != "internal" {
		t.Fatalf("默认内部级: %+v %v", c, err)
	}
	for name, bad := range map[string]string{
		"白名单为空":     "database: d\nviews: []\n",
		"视图名不合法":    "database: d\nviews: [v_a]\n",
		"token 不合法": "database: d\nviews: [s.v]\nroles: {r: [ops]}\n",
		"库名不合法":     "database: D!\nviews: [s.v]\n",
	} {
		if _, err := LoadConfig(write(bad)); err == nil {
			t.Errorf("%s 应拒绝", name)
		}
	}
}
