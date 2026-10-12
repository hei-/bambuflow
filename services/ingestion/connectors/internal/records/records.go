// Package records 是可选的结构化记录入库（入库管道详设第 6 节，技术设计文档 5.2）：默认关系型数据库只同步表结构
// （internal/dbschema），行数据由 SQL 工具实时查询；对确实要和文档一起做语义检索的表（产品参数、费率、制度条款表），
// 按表开启，一行一张记录卡片入库。
//
//   - 启用条件：配置里写明数据属主的书面同意（consent）；行级权限能表达成权限 token（权限列 acl_column 或整表的 acl），
//     某一行的权限表达不了（为空或格式不对）就不入库，已入库的随之删除。
//   - 文档标识：库名-schema-视图名-主键（主键值不是小写字母、数字、连字符时用它的摘要）；一行一份文档。
//   - 内容：表名、主键，再逐行写“字段说明：值”；只含配置里列出的列，mask 的列只存掩码（保留后 4 位）。
//   - 引用：source_type = db，标题带表名与主键，source_uri 指到这一行。
//
// 行经 Rows 端口读取，默认实现用只读账号查白名单视图（PostgresRows）。同步与其他样例源相同：与上次发出的比对，
// 新增与内容变化发 UPSERT、只有权限变化发 ACL_ONLY、消失的行发删除（受删除保护阈值约束）。
package records

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/docmeta"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/publish"
)

// Column 是入库的一列。
type Column struct {
	Name  string `yaml:"name"`
	Label string `yaml:"label"`
	// Mask 为真时只存掩码（保留后 4 位）
	Mask bool `yaml:"mask"`
}

// Table 是一张开启记录入库的表（白名单视图）。
type Table struct {
	// schema.view
	View  string `yaml:"view"`
	Title string `yaml:"title"`
	// 主键列，按顺序
	Key            []string `yaml:"key"`
	Columns        []Column `yaml:"columns"`
	Classification string   `yaml:"classification"`
	// 行级权限：权限列（text[]，元素是 group:/dept:/user: 开头的 token）与整表的 token 二选一
	ACLColumn string   `yaml:"acl_column"`
	ACL       []string `yaml:"acl"`
	// 数据属主书面同意的依据（如工单号与日期），必填
	Consent string `yaml:"consent"`
	// 单表最多入库的行数，超过时整轮失败（不是“只入前 N 行”：那样会把其余的行当成已删除）
	MaxRows int `yaml:"max_rows"`
}

type Config struct {
	Database string  `yaml:"database"`
	Tables   []Table `yaml:"tables"`
}

const defaultMaxRows = 100000

var (
	qualifiedPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$`)
	identPattern     = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	dbPattern        = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	tokenPattern     = regexp.MustCompile(`^(group|dept|user):[^\s:]+$`)
	keyPattern       = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)
	classes          = []string{"public", "internal", "confidential", "restricted"}
)

// LoadConfig 读取并校验配置。
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("记录入库配置: %w", err)
	}
	if !dbPattern.MatchString(c.Database) || len(c.Tables) == 0 {
		return nil, fmt.Errorf("记录入库配置: database 不合法或没有表")
	}
	seen := map[string]bool{}
	for i := range c.Tables {
		t := &c.Tables[i]
		if t.Classification == "" {
			t.Classification = "internal"
		}
		if t.MaxRows == 0 {
			t.MaxRows = defaultMaxRows
		}
		if err := t.validate(); err != nil {
			return nil, fmt.Errorf("记录入库配置 %s: %w", t.View, err)
		}
		if seen[t.View] {
			return nil, fmt.Errorf("记录入库配置: %s 重复", t.View)
		}
		seen[t.View] = true
	}
	return &c, nil
}

func (t *Table) validate() error {
	switch {
	case !qualifiedPattern.MatchString(t.View):
		return fmt.Errorf("view 应为 schema.view")
	case strings.TrimSpace(t.Consent) == "":
		return fmt.Errorf("缺少数据属主的书面同意（consent）")
	case strings.TrimSpace(t.Title) == "":
		return fmt.Errorf("缺少 title")
	case len(t.Key) == 0 || len(t.Columns) == 0:
		return fmt.Errorf("key 与 columns 必填")
	case !slices.Contains(classes, t.Classification):
		return fmt.Errorf("密级 %q 不合法", t.Classification)
	case (t.ACLColumn == "") == (len(t.ACL) == 0):
		return fmt.Errorf("acl_column 与 acl 必须二选一：行级权限表达不了的表不能入库")
	case t.ACLColumn != "" && !identPattern.MatchString(t.ACLColumn):
		return fmt.Errorf("acl_column %q 不合法", t.ACLColumn)
	case t.MaxRows < 1:
		return fmt.Errorf("max_rows 应为正数")
	}
	for _, tok := range t.ACL {
		if !tokenPattern.MatchString(tok) {
			return fmt.Errorf("acl 里的 %q 不合法", tok)
		}
	}
	names := map[string]bool{}
	for _, col := range t.Columns {
		if !identPattern.MatchString(col.Name) || strings.TrimSpace(col.Label) == "" || names[col.Name] {
			return fmt.Errorf("列 %q 不合法、缺少 label 或重复", col.Name)
		}
		names[col.Name] = true
	}
	for _, k := range t.Key {
		if !names[k] {
			return fmt.Errorf("主键列 %q 不在 columns 里", k)
		}
	}
	return nil
}

// Row 是一行：列名 → 文本值（空值为 nil），以及权限列的 token。
type Row struct {
	Values map[string]*string
	ACL    []string
}

// docID 是一行的文档 ID。
func (c *Config) docID(t Table, key string) string {
	return strings.ReplaceAll(strings.ToLower(c.Database+"-"+strings.ReplaceAll(t.View, ".", "-")), "_", "-") + "-" + key
}

// keyOf 是主键在文档 ID 里的写法：本身就是小写字母、数字与连字符时原样用，否则用摘要。
func keyOf(t Table, r Row) (string, string, error) {
	var parts, shown []string
	for _, k := range t.Key {
		v := r.Values[k]
		if v == nil || *v == "" {
			return "", "", fmt.Errorf("主键列 %s 为空", k)
		}
		parts = append(parts, *v)
		shown = append(shown, k+"="+*v)
	}
	joined := strings.ToLower(strings.Join(parts, "-"))
	if !keyPattern.MatchString(joined) {
		sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
		joined = "k" + hex.EncodeToString(sum[:])[:20]
	}
	return joined, strings.Join(shown, "，"), nil
}

// acl 是一行的权限 token；表达不了时返回 nil（这一行不入库）。
func acl(t Table, r Row) []string {
	tokens := t.ACL
	if t.ACLColumn != "" {
		tokens = r.ACL
	}
	if len(tokens) == 0 || slices.ContainsFunc(tokens, func(s string) bool { return !tokenPattern.MatchString(s) }) {
		return nil
	}
	out := slices.Clone(tokens)
	sort.Strings(out)
	return slices.Compact(out)
}

func mask(v string) string {
	r := []rune(v)
	if len(r) <= 4 {
		return strings.Repeat("*", len(r))
	}
	return strings.Repeat("*", len(r)-4) + string(r[len(r)-4:])
}

// Card 是一行的记录卡片正文：只由配置的列与它们的值决定，同样的行总是同样的正文。
func Card(database string, t Table, shownKey string, r Row) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s：%s\n\n来源：%s 库的 %s（结构化记录，一行一张卡片）；主键 %s。\n\n", t.Title, shownKey, database, t.View, shownKey)
	for _, col := range t.Columns {
		v := "（空）"
		if p := r.Values[col.Name]; p != nil {
			v = *p
			if col.Mask {
				v = mask(v)
			}
		}
		fmt.Fprintf(&b, "- %s：%s\n", col.Label, v)
	}
	return b.String()
}

// Document 把一行写成要发布的文档；行的权限表达不了时 ok 为 false。
func (c *Config) Document(t Table, r Row, baseURI string, syncedAt time.Time) (*publish.Document, bool, error) {
	key, shown, err := keyOf(t, r)
	if err != nil {
		return nil, false, err
	}
	tokens := acl(t, r)
	if tokens == nil {
		return nil, false, nil
	}
	body := Card(c.Database, t, shown, r)
	sum := sha256.Sum256([]byte(body))
	return &publish.Document{
		Meta: docmeta.Meta{
			DocID: c.docID(t, key), Title: fmt.Sprintf("%s（%s）", t.Title, shown), Owner: c.Database,
			Classification: t.Classification, ACLTokens: tokens, UpdatedAt: syncedAt,
			ACLFingerprint: docmeta.Fingerprint(tokens, t.Classification),
		},
		SourceURI: baseURI + c.Database + "/" + t.View + "?" + strings.ReplaceAll(shown, "，", "&"), SourceType: "db",
		ContentType: "text/markdown", Text: body, ContentHash: hex.EncodeToString(sum[:]),
	}, true, nil
}
