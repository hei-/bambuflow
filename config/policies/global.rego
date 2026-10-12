# 全局基线（信息安全审批，governance 详设 5.1）：合成规则与平台级约束，给出检索的 decision。
# package（bambuflow.authz）与 import rego.v1 由 governance 按作用域生成，这里只写规则。
#
# 输入 input.principal：sub、tenant、dept、clearance（密级编号 1–4）、region、groups（已展开的完整群组列表）；
# input.granted_sources：租户被授权的数据源，以 governance 为唯一事实来源，经配置快照下发（governance 详设 6.2）。
#
# 下层只能收紧：合成只从下层读拒绝类结果——
#   数据源层 data.bambuflow.sources[<数据源>].deny：这个主体不能检索该数据源的原因（如地域限定）；
#   租户层 data.bambuflow.tenants[<租户>].deny：整个请求拒绝的原因；deny_sources：租户不检索的数据源；
#   pii_viewer_groups：能看 PII 原文的群组（PII 默认掩码，租户指定谁能看原文）。
# 没有租户层的租户一律拒绝（跨租户默认拒绝）。内核在下发前还会断言必含条件
# （services/retrieval/internal/search/filter.go），策略写错也不会放大权限。

default decision := {"allow": false, "reason": "no_rule"}

# 部门和密级是 ABAC 的必备属性；缺任何一项都无法判定，拒绝而不是按空值放行
complete if {
	input.principal.sub != ""
	input.principal.dept != ""
	input.principal.clearance >= 1
	input.principal.clearance <= 4
}

tenant := data.bambuflow.tenants[input.principal.tenant]

tenant_deny := {r | some r in object.get(tenant, "deny", set())}

decision := {"allow": false, "reason": "principal_incomplete"} if {
	not complete
} else := {"allow": false, "reason": "tenant_not_configured"} if {
	not tenant
} else := {"allow": false, "reason": "tenant_denied"} if {
	count(tenant_deny) > 0
} else := {"allow": true, "reason": "allowed", "filter": filter, "obligations": obligations}

denied_by_tenant := {s | some s in object.get(tenant, "deny_sources", set())}

# 数据源层给出了拒绝原因的数据源：从可检索的数据源里排除
denied_by_source contains s if {
	some s in input.granted_sources
	count(object.get(data.bambuflow.sources, [s, "deny"], set())) > 0
}

# 义务：默认对 PII 掩码；只有租户指定的群组看原文
default redaction := "mask"

redaction := "none" if {
	some g in input.principal.groups
	g in object.get(tenant, "pii_viewer_groups", set())
}

obligations := {"redaction": redaction}

filter := {
	"sources": [s | some s in input.granted_sources; not s in denied_by_tenant; not s in denied_by_source],
	"max_classification": input.principal.clearance,
	# 与入库侧同一格式的 token（技术设计文档第 4 节“ACL 的表示”）
	"acl_tokens": array.concat(
		array.concat([concat("", ["user:", input.principal.sub])], [concat("", ["group:", g]) | some g in input.principal.groups]),
		[concat("", ["dept:", input.principal.dept])],
	),
}

# ---- 测试（发布前由 governance 运行，详设 5.2 第 2 步） ----

alice := {"sub": "alice", "tenant": "credit", "dept": "信贷部", "clearance": 2, "region": "华东", "groups": ["credit-l2"]}

layers := {"credit": {"deny_sources": {"mail"}, "pii_viewer_groups": {"kyc"}}, "locked": {"deny": {"suspended"}}}

test_allowed_with_own_tokens if {
	d := decision with input as {"principal": alice, "granted_sources": ["wiki", "mail"]} with data.bambuflow.tenants as layers
	d.allow
	d.filter.sources == ["wiki"]
	d.filter.max_classification == 2
	d.filter.acl_tokens == ["user:alice", "group:credit-l2", "dept:信贷部"]
	d.obligations.redaction == "mask"
}

test_incomplete_principal_denied if {
	d := decision with input as {"principal": object.union(alice, {"dept": ""}), "granted_sources": ["wiki"]}
		with data.bambuflow.tenants as layers
	not d.allow
	d.reason == "principal_incomplete"
}

test_unconfigured_tenant_denied if {
	d := decision with input as {"principal": object.union(alice, {"tenant": "other"}), "granted_sources": ["wiki"]}
		with data.bambuflow.tenants as layers
	d.reason == "tenant_not_configured"
}

test_tenant_deny_rejects_request if {
	d := decision with input as {"principal": object.union(alice, {"tenant": "locked"}), "granted_sources": ["wiki"]}
		with data.bambuflow.tenants as layers
	d.reason == "tenant_denied"
}

test_source_layer_excludes_source if {
	d := decision with input as {"principal": alice, "granted_sources": ["wiki", "east"]}
		with data.bambuflow.tenants as layers
		with data.bambuflow.sources as {"east": {"deny": {"region"}}}
	d.filter.sources == ["wiki"]
}

test_pii_viewer_sees_original if {
	d := decision with input as {"principal": object.union(alice, {"groups": ["kyc"]}), "granted_sources": ["wiki"]}
		with data.bambuflow.tenants as layers
	d.obligations.redaction == "none"
}
