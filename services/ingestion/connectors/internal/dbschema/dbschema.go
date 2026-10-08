// Package dbschema 是关系型数据库数据源（技术设计文档 5.2、5.5）：只同步白名单视图的表结构和字段说明，
// 行数据不入索引（问答时由 agent 的只读 SQL 工具实时查询，行级权限由视图保证）。
//
// 一个视图一份文档，正文是视图说明与字段列表；变更按表结构哈希判断，视图从系统表中消失（或移出白名单）即删除；
// 权限来自视图的 SELECT 授权，按配置把数据库角色映射成权限 token，没有映射到任何群组的视图谁都看不到。
// 引用展示库名、视图名和同步时间：标题里带“同步于”这一版表结构第一次被发现的时间，表结构不变时标题也不变。
//
// 元数据经 Catalog 端口读取：优先接数据平台的元数据目录，没有时用默认实现定时读系统表（PostgresCatalog，
// 只读账号、只查系统目录，不读任何行数据）。
package dbschema

import (
	"context"
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

	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/docmeta"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/publish"
)

// View 是一个视图的表结构、说明与 SELECT 授权（授权给哪些数据库角色）。
type View struct {
	Schema, Name, Comment string
	Columns               []Column
	Grantees              []string
}

// Column 是视图的一个字段。
type Column struct {
	Name, Type, Comment string
}

// Catalog 是元数据端口：返回白名单里仍然存在的视图（不存在的不返回）。不可用时返回错误，调用方不推断任何删除。
type Catalog interface {
	Views(ctx context.Context, qualified []string) ([]View, error)
}

// Config 是关系型数据库数据源的部署配置。
type Config struct {
	// 展示用的库名，进入标题与 source_uri
	Database       string `yaml:"database"`
	Classification string `yaml:"classification"`
	// 白名单：schema.view
	Views []string `yaml:"views"`
	// 数据库角色 → 权限 token；PUBLIC 表示授权给所有数据库用户
	Roles map[string][]string `yaml:"roles"`
}

var (
	qualifiedPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$`)
	dbPattern        = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	tokenPattern     = regexp.MustCompile(`^(group|dept|user):[^\s:]+$`)
	classes          = []string{"public", "internal", "confidential", "restricted"}
)

// LoadConfig 读取并校验配置；密级不配置时按内部。
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("数据库数据源配置: %w", err)
	}
	if c.Classification == "" {
		c.Classification = "internal"
	}
	switch {
	case !dbPattern.MatchString(c.Database):
		return nil, fmt.Errorf("数据库数据源配置: database %q 不合法", c.Database)
	case !slices.Contains(classes, c.Classification):
		return nil, fmt.Errorf("数据库数据源配置: 密级 %q 不合法", c.Classification)
	case len(c.Views) == 0:
		return nil, fmt.Errorf("数据库数据源配置: 白名单为空")
	}
	for _, v := range c.Views {
		if !qualifiedPattern.MatchString(v) {
			return nil, fmt.Errorf("白名单里的 %q 应为 schema.view", v)
		}
	}
	for role, tokens := range c.Roles {
		for _, t := range tokens {
			if !tokenPattern.MatchString(t) {
				return nil, fmt.Errorf("角色 %s 的权限 token %q 不合法", role, t)
			}
		}
	}
	return &c, nil
}

// DocID 是视图的文档 ID：库名-schema-视图名，下划线换成连字符。
func (c *Config) DocID(v View) string {
	return strings.ReplaceAll(strings.ToLower(c.Database+"-"+v.Schema+"-"+v.Name), "_", "-")
}

// aclTokens 把视图的 SELECT 授权映射成权限 token；没有映射的角色不给任何人权限（权限只收紧不放宽）。
func (c *Config) aclTokens(v View) []string {
	var out []string
	for _, g := range v.Grantees {
		out = append(out, c.Roles[g]...)
	}
	sort.Strings(out)
	return slices.Compact(out)
}

// Render 是视图的正文：说明与字段列表。只由表结构决定，同一结构总是同一正文（表结构哈希按它算）。
func Render(database string, v View) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# 视图 %s.%s\n\n所属库：%s；对象类型：视图（只同步表结构与字段说明，不含行数据）。\n", v.Schema, v.Name, database)
	if v.Comment != "" {
		fmt.Fprintf(&b, "\n%s\n", v.Comment)
	}
	b.WriteString("\n## 字段\n\n")
	for _, col := range v.Columns {
		line := fmt.Sprintf("- %s（%s）", col.Name, col.Type)
		if col.Comment != "" {
			line += "：" + col.Comment
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

// Document 把一个视图写成要发布的文档。syncedAt 是这一版表结构第一次被发现的时间。
func (c *Config) Document(v View, baseURI string, syncedAt time.Time) *publish.Document {
	body := Render(c.Database, v)
	sum := sha256.Sum256([]byte(body))
	tokens := c.aclTokens(v)
	acl := sha256.Sum256([]byte(strings.Join(tokens, "\n") + "\n" + c.Classification))
	return &publish.Document{
		Meta: docmeta.Meta{
			DocID: c.DocID(v),
			Title: fmt.Sprintf("视图 %s.%s（%s 库，同步于 %s）", v.Schema, v.Name, c.Database,
				syncedAt.UTC().Format("2006-01-02 15:04 UTC")),
			Owner: c.Database, Classification: c.Classification, ACLTokens: tokens,
			UpdatedAt: syncedAt, ACLFingerprint: hex.EncodeToString(acl[:]),
		},
		SourceURI: baseURI + c.Database + "/" + v.Schema + "." + v.Name, SourceType: "db",
		ContentType: "text/markdown", Text: body, ContentHash: hex.EncodeToString(sum[:]),
	}
}
