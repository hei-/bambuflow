# 检索权限策略（retrieval 详设 5.2）：按 Principal 给出允许 / 拒绝和过滤条件。
#
# 输入 input.principal：sub、tenant、dept、clearance（密级编号 1–4）、region、groups（已展开的完整群组列表）；
# input.granted_sources：租户被授权的数据源，以 governance 为唯一事实来源，经配置快照下发（governance 详设 6.2）。
# 数据 data.tenants.<租户>：租户策略，denied_sources（策略禁止的数据源，可省略）、
#   pii_viewer_groups（可看 PII 原文的群组，可省略）；
# data.source_regions.<数据源>：只对这些地域可见（数据源策略，技术设计文档 8.6；可省略）。
# 内核在下发前还会断言必含条件（services/retrieval/internal/search/filter.go），策略写错也不会放大权限。
package bambuflow.authz

import rego.v1

default decision := {"allow": false, "reason": "no_rule"}

# 部门和密级是 ABAC 的必备属性；缺任何一项都无法判定，拒绝而不是按空值放行
complete if {
	input.principal.sub != ""
	input.principal.dept != ""
	input.principal.clearance >= 1
	input.principal.clearance <= 4
}

tenant := data.tenants[input.principal.tenant]

decision := {"allow": false, "reason": "principal_incomplete"} if {
	not complete
} else := {"allow": false, "reason": "tenant_not_configured"} if {
	not tenant
} else := {"allow": true, "reason": "allowed", "filter": filter, "obligations": obligations}

denied := {s | some s in object.get(tenant, "denied_sources", [])}

# 数据源限定了地域、而调用者不在其中：从可检索的数据源里排除（ABAC）
region_blocked contains s if {
	some s in input.granted_sources
	# 不能写 object.get(data, …)：data 里包含本策略包自身，会构成递归
	regions := data.source_regions[s]
	not input.principal.region in regions
}

# 义务：默认对 PII 掩码；只有租户指定的群组看原文
default redaction := "mask"

redaction := "none" if {
	some g in input.principal.groups
	g in object.get(tenant, "pii_viewer_groups", [])
}

obligations := {"redaction": redaction}

filter := {
	"sources": [s | some s in input.granted_sources; not s in denied; not s in region_blocked],
	"max_classification": input.principal.clearance,
	# 与入库侧同一格式的 token（技术设计文档第 4 节“ACL 的表示”）
	"acl_tokens": array.concat(
		array.concat([concat("", ["user:", input.principal.sub])], [concat("", ["group:", g]) | some g in input.principal.groups]),
		[concat("", ["dept:", input.principal.dept])],
	),
}
