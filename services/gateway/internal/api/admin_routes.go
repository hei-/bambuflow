package api

import (
	"net/http"
	"time"

	"github.com/bambu-ai/bambuflow/services/gateway/internal/roles"
)

// adminRoutes 是管理接口的路由表（gateway 详设 4.2）：方法 + 路径 → 实现方与允许的平台角色。
// 只登记已经实现的接口；设计里列出、尚未实现的路径不登记，访问时与任何未知路径一样返回 404。
// 角色按 governance 详设 9.1、入库管道详设 14.4；实现方会按管理 Principal 的角色再校验一次。
func (s *Server) adminRoutes() []adminRoute {
	return []adminRoute{
		{name: "admin-emergency-denials-list", method: http.MethodGet, pattern: "/v1/admin/emergency-denials", owner: "governance",
			roles: []string{roles.InfoSec, roles.DataOwner}, call: s.listDenials},
		{name: "admin-emergency-denials-create", method: http.MethodPost, pattern: "/v1/admin/emergency-denials", owner: "governance",
			roles: []string{roles.InfoSec, roles.DataOwner}, call: s.createDenial},
		{name: "admin-index-versions-list", method: http.MethodGet, pattern: "/v1/admin/index-versions", owner: "indexing",
			roles: []string{roles.PlatformOperator}, call: s.listIndexVersions},
		// 切换或回滚：第一次调用只创建待复核的操作，另一位平台运维复核后才执行（入库管道详设 14.4）
		{name: "admin-index-versions-activate", method: http.MethodPost, pattern: "/v1/admin/index-versions/{version}/activate", owner: "indexing",
			roles: []string{roles.PlatformOperator}, call: s.activateIndexVersion},
		{name: "admin-index-versions-approve", method: http.MethodPost, pattern: "/v1/admin/index-versions/operations/{operation_id}/approve",
			owner: "indexing", roles: []string{roles.PlatformOperator}, call: s.approveOperation},
		// 数据源：平台管理员注册，本部门的数据属主审批、撤销与授权（属主范围由 governance 判断）
		{name: "admin-sources-register", method: http.MethodPost, pattern: "/v1/admin/sources", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.registerSource},
		{name: "admin-sources-approve", method: http.MethodPost, pattern: "/v1/admin/sources/{src_id}/approve", owner: "governance",
			roles: []string{roles.DataOwner}, call: s.approveSource},
		{name: "admin-sources-revoke", method: http.MethodPost, pattern: "/v1/admin/sources/{src_id}/revoke", owner: "governance",
			roles: []string{roles.DataOwner}, call: s.revokeSource},
		{name: "admin-sources-grants-list", method: http.MethodGet, pattern: "/v1/admin/sources/{src_id}/grants", owner: "governance",
			roles: []string{roles.DataOwner}, call: s.listGrants},
		{name: "admin-sources-grants-create", method: http.MethodPost, pattern: "/v1/admin/sources/{src_id}/grants", owner: "governance",
			roles: []string{roles.DataOwner}, call: s.createGrant},
		{name: "admin-sources-grants-delete", method: http.MethodDelete, pattern: "/v1/admin/sources/{src_id}/grants", owner: "governance",
			roles: []string{roles.DataOwner}, call: s.deleteGrant},
		// A/B 实验：平台管理员立项、按 5% → 20% → 50% 推进与停止（governance 详设 7.2、9.1）
		{name: "admin-experiments-list", method: http.MethodGet, pattern: "/v1/admin/experiments", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.listExperiments},
		{name: "admin-experiments-create", method: http.MethodPost, pattern: "/v1/admin/experiments", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.createExperiment},
		{name: "admin-experiments-get", method: http.MethodGet, pattern: "/v1/admin/experiments/{exp_id}", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.getExperiment},
		{name: "admin-experiments-ramp", method: http.MethodPost, pattern: "/v1/admin/experiments/{exp_id}/ramp", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.rampExperiment},
		{name: "admin-experiments-stop", method: http.MethodPost, pattern: "/v1/admin/experiments/{exp_id}/stop", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.stopExperiment},
		// A/B 分析由 evaluation 实现（evaluation 详设 8.2）；按用户聚合全部回答记录，给更长的超时
		{name: "admin-experiments-analysis", method: http.MethodGet, pattern: "/v1/admin/experiments/{exp_id}/analysis", owner: "evaluation",
			roles: []string{roles.PlatformAdmin}, deadline: 30 * time.Second, call: s.experimentAnalysis},
		// 在线抽样评审的趋势（evaluation 详设 7.1、8.2）；设计里租户负责人也能看本租户，平台角色里还没有租户负责人
		{name: "admin-quality-trends", method: http.MethodGet, pattern: "/v1/admin/quality-trends", owner: "evaluation",
			roles: []string{roles.PlatformAdmin}, call: s.qualityTrends},
		// 审计查询只给审计岗；查询本身既有这里的管理审计，也有 governance 的变更审计（governance 详设 9.3）
		{name: "admin-audit-query", method: http.MethodGet, pattern: "/v1/admin/audit", owner: "governance",
			roles: []string{roles.Auditor}, call: s.queryAudit},
	}
}
