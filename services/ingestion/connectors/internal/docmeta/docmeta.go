// Package docmeta 校验并归一化样例数据源给出的文档元数据（模拟源系统接口返回的 ID、属主、密级与 ACL）。
//
// Wiki 页面放在 front matter 里，共享盘文件放在旁挂的元数据文件里，两者字段相同。
// ACL 在这里归一化为权限 token（技术设计文档 4.1：group:x、dept:x、user:x）。
package docmeta

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type Meta struct {
	DocID          string
	Title          string
	Owner          string
	Classification string // public | internal | confidential | restricted
	ACLTokens      []string
	UpdatedAt      time.Time
	ACLFingerprint string // 权限摘要：归一化 ACL 与密级的哈希，用于发现“仅权限变化”
	Source         *SourceFields
	// 源端字段的摘要：源系统只改了结构化字段（如把制度标为已废止）也要重新入库，元数据才会更新
	SourceFingerprint string
	// Partition 是文档所在的分区（知识库或目录），分区级数据源用它（总体设计 ADR-14）；ExplicitACL 表示源端给了单文档 ACL
	Partition   string
	ExplicitACL bool
}

// SourceFields 是源系统给出的结构化字段（入库管道详设第 7 节：来源优先级最高）。原样传给 processing，
// 由它按受控词表归一；这里只校验格式。都没有时为 nil。
type SourceFields struct {
	DocType       string   `json:"doc_type,omitempty" yaml:"doc_type"`
	Dept          string   `json:"dept,omitempty" yaml:"dept"`
	Topics        []string `json:"topics,omitempty" yaml:"topics"`
	EffectiveDate string   `json:"effective_date,omitempty" yaml:"effective_date"`
	// effective / pending / repealed
	Status string `json:"status,omitempty" yaml:"status"`
	// 这份文档替代的文档（同一数据源里的 doc_id），入库管道详设第 7 节效力信息
	Supersedes []string `json:"supersedes,omitempty" yaml:"supersedes"`
}

type fields struct {
	ID             string    `yaml:"id"`
	Title          string    `yaml:"title"`
	Owner          string    `yaml:"owner"`
	Classification string    `yaml:"classification"`
	ACL            []string  `yaml:"acl"`
	UpdatedAt      time.Time `yaml:"updated_at"`
	Partition      string    `yaml:"partition"`
	SourceFields   `yaml:",inline"`
}

var (
	tokenPattern = regexp.MustCompile(`^(group|dept|user):[^\s:]+$`)
	idPattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	levels       = []string{"public", "internal", "confidential", "restricted"}
)

// Decode 解析并校验一段 YAML 元数据；出现未知字段也报错，避免拼错的 acl 被当作“没有 ACL”。
func Decode(raw []byte) (*Meta, error) {
	var f fields
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("元数据: %w", err)
	}
	if !idPattern.MatchString(f.ID) || f.Title == "" || f.UpdatedAt.IsZero() {
		return nil, errors.New("id（小写字母、数字、连字符）、title、updated_at 必填")
	}
	if !slices.Contains(levels, f.Classification) {
		return nil, fmt.Errorf("密级 %q 不合法", f.Classification)
	}
	tokens, err := aclTokens(f.ACL, f.Owner)
	if err != nil {
		return nil, err
	}
	m := &Meta{
		DocID: f.ID, Title: f.Title, Owner: f.Owner, Classification: f.Classification, ACLTokens: tokens,
		UpdatedAt: f.UpdatedAt, ACLFingerprint: Fingerprint(tokens, f.Classification), Partition: strings.TrimSpace(f.Partition), ExplicitACL: len(f.ACL) > 0,
	}
	for _, id := range f.Supersedes {
		if !idPattern.MatchString(id) || id == f.ID {
			return nil, fmt.Errorf("supersedes 里的 %q 不合法", id)
		}
	}
	if f.DocType != "" || f.Dept != "" || len(f.Topics) > 0 || f.EffectiveDate != "" || f.Status != "" || len(f.Supersedes) > 0 {
		if f.EffectiveDate != "" {
			if _, err := time.Parse(time.DateOnly, f.EffectiveDate); err != nil {
				return nil, fmt.Errorf("effective_date 应为 YYYY-MM-DD: %q", f.EffectiveDate)
			}
		}
		if f.Status != "" && !slices.Contains([]string{"effective", "pending", "repealed"}, f.Status) {
			return nil, fmt.Errorf("status %q 不合法", f.Status)
		}
		src := f.SourceFields
		m.Source = &src
		b, err := json.Marshal(src)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(b)
		m.SourceFingerprint = hex.EncodeToString(sum[:])
	}
	return m, nil
}

// aclTokens 归一化 ACL：去重排序。没有 ACL 时按最保守处理，只给属主可见；连属主都没有则谁都不可见
// （技术设计文档 16.1：ACL 无法完整导出的，按最保守处理）。
func aclTokens(acl []string, owner string) ([]string, error) {
	if len(acl) == 0 {
		if owner == "" {
			return []string{}, nil
		}
		acl = []string{"user:" + owner}
	}
	out := make([]string, 0, len(acl))
	for _, t := range acl {
		t = strings.TrimSpace(t)
		if !tokenPattern.MatchString(t) {
			return nil, fmt.Errorf("ACL 条目 %q 不合法，应为 group:/dept:/user: 开头", t)
		}
		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out, nil
}

// Fingerprint 是权限摘要：归一化 ACL 与密级的哈希。
func Fingerprint(tokens []string, classification string) string {
	fp := sha256.Sum256([]byte(strings.Join(tokens, "\n") + "\n" + classification))
	return hex.EncodeToString(fp[:])
}

// Skipped 是清单里没能读出的一项（文件名与原因）：同步跳过它、照常处理其余文档，这一轮不推断删除（入库管道详设第 11 节）。
type Skipped struct {
	Name, Reason string
}
