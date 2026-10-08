// Package search 是 retrieval 的领域逻辑：必含条件断言、权限判定、多子查询融合与 Search / CheckAccess 流程。
// 存储、嵌入模型和策略引擎都经本包定义的接口接入（retrieval 详设第 2 节）。
package search

import (
	"errors"
	"fmt"
	"slices"
	"time"

	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

// MaxTokens 是主体 token 数量上限。超过时拒绝而不截断：截断会让用户少看到该看的内容，且难以排查（详设 5.1）。
const MaxTokens = 1024

// Filter 是下推到召回的过滤条件。P0 只有一路召回（pgvector），用结构体直接表达四个必含条件
// 与调用方的数据源过滤；多后端时再换成详设 5.3 的表达式树。
type Filter struct {
	// 租户授权的数据源（已减去策略禁止的），再与调用方 source_ids 求交
	Sources           []string
	MaxClassification int32
	Tokens            []string
	// 调用方的元数据条件（受控词表编码，按原样匹配；为空表示不限）。和数据源一样只能收窄范围
	Meta MetaFilter
	// ShowPII 来自策略义务：只有义务明确为 none 时才返回 PII 原文，其余一律掩码（fail-closed）
	ShowPII bool
}

var errMandatory = errors.New("必含条件不成立")

// Validate 在下发前断言必含条件（详设 5.3）。策略包写错也不能放大权限：
// 密级上限不得高于 Principal 的密级，token 集合必须含调用者本人。
func (f Filter) Validate(sub string, clearance commonv1.Classification) error {
	switch {
	case f.MaxClassification < int32(commonv1.Classification_CLASSIFICATION_PUBLIC) || f.MaxClassification > int32(clearance):
		return fmt.Errorf("%w: 密级上限 %d 超出范围（Principal 密级 %d）", errMandatory, f.MaxClassification, clearance)
	case len(f.Tokens) == 0 || len(f.Tokens) > MaxTokens:
		return fmt.Errorf("%w: token 数量 %d 不在 1–%d 之间", errMandatory, len(f.Tokens), MaxTokens)
	case !slices.Contains(f.Tokens, "user:"+sub):
		return fmt.Errorf("%w: token 集合不含 user:%s", errMandatory, sub)
	}
	return nil
}

// Narrow 把调用方 filters 以 AND 拼接到过滤条件上：数据源只能取交集，只会缩小范围。
func (f Filter) Narrow(c *commonv1.Filters) Filter {
	if ids := c.GetSourceIds(); len(ids) > 0 {
		var kept []string
		for _, s := range f.Sources {
			if slices.Contains(ids, s) {
				kept = append(kept, s)
			}
		}
		f.Sources = kept
	}
	f.Meta = MetaFilter{
		DocTypes: c.GetDocTypes(), Depts: c.GetDepts(), TopicCodes: c.GetTopicCodes(),
		EffectiveFrom: c.GetEffectiveFrom(), EffectiveTo: c.GetEffectiveTo(),
	}
	for _, v := range c.GetValidityStatuses() {
		f.Meta.Validity = append(f.Meta.Validity, int32(v))
	}
	return f
}

// MetaFilter 是文档元数据条件（K2 详设第 3 节）。
type MetaFilter struct {
	DocTypes, Depts, TopicCodes []string
	// YYYY-MM-DD，闭区间；没有生效日期的文档不满足任何日期条件
	EffectiveFrom, EffectiveTo string
	Validity                   []int32
	// Missing 非空时只要这个字段缺失的文档：统计 metadata_gaps 用，不来自调用方
	Missing string
}

// Fields 返回被筛选的元数据字段名（与 DocMetadata.missing 的取值一致），用来统计 metadata_gaps。
func (m MetaFilter) Fields() []string {
	var out []string
	add := func(on bool, name string) {
		if on {
			out = append(out, name)
		}
	}
	add(len(m.DocTypes) > 0, "doc_type")
	add(len(m.Depts) > 0, "dept")
	add(len(m.TopicCodes) > 0, "topic_codes")
	add(m.EffectiveFrom != "" || m.EffectiveTo != "", "effective_date")
	add(len(m.Validity) > 0, "validity_status")
	return out
}

// Without 去掉某个字段的条件：统计这个字段的缺口时，其他条件照常生效。
func (m MetaFilter) Without(field string) MetaFilter {
	switch field {
	case "doc_type":
		m.DocTypes = nil
	case "dept":
		m.Depts = nil
	case "topic_codes":
		m.TopicCodes = nil
	case "effective_date":
		m.EffectiveFrom, m.EffectiveTo = "", ""
	case "validity_status":
		m.Validity = nil
	}
	return m
}

// ValidateMeta 检查调用方的元数据条件格式。
func ValidateMeta(c *commonv1.Filters) error {
	for _, d := range []string{c.GetEffectiveFrom(), c.GetEffectiveTo()} {
		if d != "" {
			if _, err := time.Parse(time.DateOnly, d); err != nil {
				return fmt.Errorf("生效日期应为 YYYY-MM-DD: %q", d)
			}
		}
	}
	for _, v := range c.GetValidityStatuses() {
		if v == commonv1.ValidityStatus_VALIDITY_STATUS_UNSPECIFIED || commonv1.ValidityStatus_name[int32(v)] == "" {
			return fmt.Errorf("效力状态 %d 不合法", v)
		}
	}
	return nil
}

// ACL 是一份文档在权限表（idx.doc_acl）里的最新状态。
type ACL struct {
	SourceID       string
	Tokens         []string
	Classification int32
	Active         bool
	// Denied 表示文档在紧急拒绝名单里（K9）：任何人都不可见
	Denied bool
}

// Check 按过滤条件判定一份文档是否可见。召回后的组装复核与 CheckAccess 共用这一个判定，二者不会不一致。
func (f Filter) Check(a ACL) retrievalv1.AccessReason {
	switch {
	case !a.Active:
		return retrievalv1.AccessReason_ACCESS_REASON_TOMBSTONED
	case a.Denied:
		return retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED
	case !slices.Contains(f.Sources, a.SourceID):
		return retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED
	case a.Classification > f.MaxClassification:
		return retrievalv1.AccessReason_ACCESS_REASON_CLASSIFICATION_EXCEEDED
	case !slices.ContainsFunc(a.Tokens, func(t string) bool { return slices.Contains(f.Tokens, t) }):
		return retrievalv1.AccessReason_ACCESS_REASON_ACL_DENIED
	}
	return retrievalv1.AccessReason_ACCESS_REASON_ALLOWED
}
