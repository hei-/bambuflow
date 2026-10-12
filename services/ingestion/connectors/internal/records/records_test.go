package records

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/publish"
)

const cfgYAML = `database: dw
tables:
  - view: dw.v_product_rates
    title: 产品费率
    key: [product_code]
    consent: DATA-12 2026-10-01 信贷部书面同意
    acl_column: acl
    columns:
      - {name: product_code, label: 产品代码}
      - {name: rate, label: 年化费率}
      - {name: contact_phone, label: 联系电话, mask: true}
`

func load(t *testing.T, body string) (*Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "records.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return LoadConfig(p)
}

func TestLoadConfigRequiresConsentAndRowACL(t *testing.T) {
	c, err := load(t, cfgYAML)
	if err != nil || c.Tables[0].Classification != "internal" || c.Tables[0].MaxRows != defaultMaxRows {
		t.Fatalf("%v %v", c, err)
	}
	for name, body := range map[string]string{
		"没有书面同意":     strings.Replace(cfgYAML, "    consent: DATA-12 2026-10-01 信贷部书面同意\n", "", 1),
		"行级权限表达不了":   strings.Replace(cfgYAML, "    acl_column: acl\n", "", 1),
		"主键不在列里":     strings.Replace(cfgYAML, "key: [product_code]", "key: [id]", 1),
		"权限列与整表权限都给": strings.Replace(cfgYAML, "    acl_column: acl\n", "    acl_column: acl\n    acl: [group:all]\n", 1),
	} {
		if _, err := load(t, body); err == nil {
			t.Errorf("%s 应报错", name)
		}
	}
}

func s(v string) *string { return &v }

func TestDocumentIsRecordCard(t *testing.T) {
	c, _ := load(t, cfgYAML)
	tb := c.Tables[0]
	r := Row{Values: map[string]*string{"product_code": s("p001"), "rate": s("3.50%"), "contact_phone": s("13812345678")},
		ACL: []string{"group:credit", "dept:信贷部"}}
	d, ok, err := c.Document(tb, r, "db://", time.Now())
	if err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	if d.DocID != "dw-dw-v-product-rates-p001" || d.SourceType != "db" || d.Title != "产品费率（product_code=p001）" ||
		strings.Join(d.ACLTokens, ",") != "dept:信贷部,group:credit" {
		t.Fatalf("文档: %+v", d.Meta)
	}
	for _, want := range []string{"- 产品代码：p001", "- 年化费率：3.50%", "- 联系电话：*******5678", "dw.v_product_rates"} {
		if !strings.Contains(d.Text, want) {
			t.Errorf("卡片缺 %q：\n%s", want, d.Text)
		}
	}
	if strings.Contains(d.Text, "13812345678") {
		t.Error("掩码列不能出现原值")
	}
	// 主键不是小写字母数字时用摘要
	r.Values["product_code"] = s("产品 A")
	if d, _, _ := c.Document(tb, r, "db://", time.Now()); !strings.HasPrefix(d.DocID, "dw-dw-v-product-rates-k") {
		t.Fatalf("主键摘要: %s", d.DocID)
	}
	// 权限表达不了的行不入库
	r.ACL = []string{"风险管理部"}
	if _, ok, err := c.Document(tb, r, "db://", time.Now()); ok || err != nil {
		t.Fatalf("权限不合法的行: %v %v", ok, err)
	}
}

type fakeRows map[string][]Row

func (f fakeRows) Rows(_ context.Context, t Table, limit int) ([]Row, error) {
	if len(f[t.View]) > limit {
		return nil, ErrTooManyRows
	}
	return f[t.View], nil
}

type kv map[string]string

func (k kv) All(context.Context) (map[string]string, error) { return k, nil }
func (k kv) Set(_ context.Context, id, v string) error      { k[id] = v; return nil }
func (k kv) Del(_ context.Context, id string) error         { delete(k, id); return nil }

type fakePublisher struct{ changes []string }

func (f *fakePublisher) Publish(_ context.Context, d *publish.Document, change ingestionv1.ChangeType, _ time.Time) error {
	f.changes = append(f.changes, change.String()+":"+d.DocID)
	return nil
}

func (f *fakePublisher) Delete(_ context.Context, id string, _ time.Time) error {
	f.changes = append(f.changes, "DELETE:"+id)
	return nil
}

func TestSyncDiffsRows(t *testing.T) {
	c, _ := load(t, cfgYAML)
	row := func(code, rate string, acl ...string) Row {
		return Row{Values: map[string]*string{"product_code": s(code), "rate": s(rate)}, ACL: acl}
	}
	rows := fakeRows{"dw.v_product_rates": {row("p001", "3.5%", "group:credit"), row("p002", "4.0%", "group:credit"), row("p003", "5%")}}
	state := kv{}
	pub := &fakePublisher{}
	sy := &Syncer{Config: c, Rows: rows, Published: state, Publisher: pub, BaseURI: "db://", Now: time.Now}
	st, err := sy.Run(context.Background())
	if err != nil || st.Upserts != 2 || st.WithoutACL != 1 {
		t.Fatalf("首轮: %+v %v", st, err)
	}
	// 费率变了、权限变了、一行消失
	rows["dw.v_product_rates"] = []Row{row("p001", "3.6%", "group:credit"), row("p002", "4.0%", "group:risk")}
	pub.changes = nil
	st, err = sy.Run(context.Background())
	if err != nil || strings.Join(pub.changes, ",") != "CHANGE_TYPE_ACL_ONLY:dw-dw-v-product-rates-p002,CHANGE_TYPE_UPSERT:dw-dw-v-product-rates-p001" {
		t.Fatalf("增量: %+v %v %v", st, pub.changes, err)
	}
	// 超过 max_rows：整轮失败，不推断删除
	c.Tables[0].MaxRows = 1
	if _, err := sy.Run(context.Background()); err == nil {
		t.Fatal("超过 max_rows 应失败")
	}
}
