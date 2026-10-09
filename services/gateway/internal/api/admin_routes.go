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
		// 全量重建：申请与开始双人复核，看进度与取消一人即可（入库管道详设第 10 节、14.4）
		{name: "admin-index-rebuilds-start", method: http.MethodPost, pattern: "/v1/admin/index-rebuilds", owner: "indexing",
			roles: []string{roles.PlatformOperator}, call: s.startRebuild},
		{name: "admin-index-rebuilds-approve", method: http.MethodPost, pattern: "/v1/admin/index-rebuilds/operations/{operation_id}/approve",
			owner: "indexing", roles: []string{roles.PlatformOperator}, call: s.approveRebuild},
		{name: "admin-index-rebuilds-get", method: http.MethodGet, pattern: "/v1/admin/index-rebuilds/{rebuild_id}", owner: "indexing",
			roles: []string{roles.PlatformOperator}, call: s.getRebuild},
		{name: "admin-index-rebuilds-cancel", method: http.MethodPost, pattern: "/v1/admin/index-rebuilds/{rebuild_id}/cancel", owner: "indexing",
			roles: []string{roles.PlatformOperator}, call: s.cancelRebuild},
		// 撤销误写的删除标记：同样双人复核（入库管道详设第 5 节、14.4）
		{name: "admin-tombstone-revocations-create", method: http.MethodPost, pattern: "/v1/admin/tombstone-revocations", owner: "indexing",
			roles: []string{roles.PlatformOperator}, call: s.revokeTombstones},
		{name: "admin-tombstone-revocations-approve", method: http.MethodPost, pattern: "/v1/admin/tombstone-revocations/operations/{operation_id}/approve",
			owner: "indexing", roles: []string{roles.PlatformOperator}, call: s.approveRevocation},
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
		// 评测门禁的列表与报告（evaluation 详设 5.2、8.2）；设计里租户负责人也能看本租户，平台角色里还没有租户负责人
		{name: "admin-eval-gates-list", method: http.MethodGet, pattern: "/v1/admin/eval-gates", owner: "evaluation",
			roles: []string{roles.PlatformAdmin, roles.PlatformOperator}, call: s.listGates},
		{name: "admin-eval-gates-get", method: http.MethodGet, pattern: "/v1/admin/eval-gates/{gate_id}", owner: "evaluation",
			roles: []string{roles.PlatformAdmin, roles.PlatformOperator}, call: s.getGate},
		// 评测集题目与候选题复核（evaluation 详设 3.4、8.2）：设计里的“标注人员”“业务专家”平台角色里还没有，
		// 由数据属主（业务专家）与平台管理员（平台）提交与复核，入集需两类人各一
		{name: "admin-golden-questions-list", method: http.MethodGet, pattern: "/v1/admin/golden-sets/{tenant}/questions", owner: "evaluation",
			roles: []string{roles.PlatformAdmin, roles.PlatformOperator, roles.DataOwner}, call: s.listGoldenQuestions},
		{name: "admin-golden-questions-submit", method: http.MethodPost, pattern: "/v1/admin/golden-sets/{tenant}/questions", owner: "evaluation",
			roles: []string{roles.PlatformAdmin, roles.DataOwner}, call: s.submitCandidate},
		{name: "admin-golden-questions-review", method: http.MethodPost, pattern: "/v1/admin/golden-sets/{tenant}/questions/{question_id}/review",
			owner: "evaluation", roles: []string{roles.PlatformAdmin, roles.DataOwner}, call: s.reviewCandidate},
		// 受控词表：平台管理员维护，编码只能停用不能删除（governance 详设 6.1、7.2）
		{name: "admin-vocabulary-terms-list", method: http.MethodGet, pattern: "/v1/admin/vocabularies/{vocabulary}/terms", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.listTerms},
		{name: "admin-vocabulary-terms-upsert", method: http.MethodPost, pattern: "/v1/admin/vocabularies/{vocabulary}/terms", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.upsertTerm},
		{name: "admin-vocabulary-terms-deprecate", method: http.MethodPost, pattern: "/v1/admin/vocabularies/{vocabulary}/terms/{code}/deprecate",
			owner: "governance", roles: []string{roles.PlatformAdmin}, call: s.deprecateTerm},
		// 数据源同步的运维接口由 connectors 的管理服务实现（入库管道详设 14.4）；暂停会让该源的撤权与删除也停下
		{name: "admin-sources-sync-status", method: http.MethodGet, pattern: "/v1/admin/sources/{src_id}/sync-status", owner: "connectors",
			roles: []string{roles.PlatformOperator, roles.DataOwner}, call: s.syncStatus},
		{name: "admin-sources-pause", method: http.MethodPost, pattern: "/v1/admin/sources/{src_id}/pause", owner: "connectors",
			roles: []string{roles.PlatformOperator}, call: s.pauseSource},
		{name: "admin-sources-resume", method: http.MethodPost, pattern: "/v1/admin/sources/{src_id}/resume", owner: "connectors",
			roles: []string{roles.PlatformOperator}, call: s.resumeSource},
		// 删除保护拦下的待删批次：数据属主确认或丢弃，确认需两位不同的属主（connectors 判断）
		{name: "admin-sources-pending-deletions-list", method: http.MethodGet, pattern: "/v1/admin/sources/{src_id}/pending-deletions",
			owner: "connectors", roles: []string{roles.PlatformOperator, roles.DataOwner}, call: s.listPendingDeletions},
		{name: "admin-sources-pending-deletions-confirm", method: http.MethodPost,
			pattern: "/v1/admin/sources/{src_id}/pending-deletions/{hold_id}/confirm", owner: "connectors",
			roles: []string{roles.DataOwner}, call: s.confirmDeletion},
		{name: "admin-sources-pending-deletions-discard", method: http.MethodPost,
			pattern: "/v1/admin/sources/{src_id}/pending-deletions/{hold_id}/discard", owner: "connectors",
			roles: []string{roles.DataOwner}, call: s.discardDeletion},
		// 入库运维由 processing 的管理服务实现（入库管道详设 11、14.4）；重放只放回死信里的原事件，不重新从源系统拉取
		{name: "admin-documents-ingest-status", method: http.MethodGet, pattern: "/v1/admin/documents/{doc_id}/ingest-status",
			owner: "processing", roles: []string{roles.PlatformOperator}, call: s.ingestStatus},
		{name: "admin-dead-letters-list", method: http.MethodGet, pattern: "/v1/admin/dead-letters", owner: "processing",
			roles: []string{roles.PlatformOperator}, call: s.listDeadLetters},
		{name: "admin-replays-create", method: http.MethodPost, pattern: "/v1/admin/replays", owner: "processing",
			roles: []string{roles.PlatformOperator}, call: s.replay},
		// 插件登记：平台管理员登记，Tool 插件由信息安全评审；评估结果由 CI 经服务间接口写入（governance 详设 3.1）
		{name: "admin-plugins-list", method: http.MethodGet, pattern: "/v1/admin/plugins", owner: "governance",
			roles: []string{roles.PlatformAdmin, roles.InfoSec}, call: s.listPlugins},
		{name: "admin-plugins-register", method: http.MethodPost, pattern: "/v1/admin/plugins", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.registerPlugin},
		{name: "admin-plugins-security-review", method: http.MethodPost, pattern: "/v1/admin/plugins/{plugin_id}/security-review",
			owner: "governance", roles: []string{roles.InfoSec}, call: s.reviewPlugin},
		// 租户配置的版本、变更单与发布（governance 详设 3、4、7.2）：平台管理员发起与审批，作者不能批自己的；
		// 设计里发布由 CI 服务账号发起、租户负责人只读本租户，平台角色里还没有这两类，先都由平台管理员
		{name: "admin-tenants-list", method: http.MethodGet, pattern: "/v1/admin/tenants", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.listTenants},
		{name: "admin-tenants-create", method: http.MethodPost, pattern: "/v1/admin/tenants", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.createTenant},
		{name: "admin-tenants-config-versions", method: http.MethodGet, pattern: "/v1/admin/tenants/{tenant}/config-versions",
			owner: "governance", roles: []string{roles.PlatformAdmin}, call: s.listConfigVersions},
		{name: "admin-change-requests-create", method: http.MethodPost, pattern: "/v1/admin/change-requests", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.createChangeRequest},
		{name: "admin-change-requests-approve", method: http.MethodPost, pattern: "/v1/admin/change-requests/{chg_id}/approve",
			owner: "governance", roles: []string{roles.PlatformAdmin}, call: s.approveChangeRequest},
		{name: "admin-change-requests-reject", method: http.MethodPost, pattern: "/v1/admin/change-requests/{chg_id}/reject",
			owner: "governance", roles: []string{roles.PlatformAdmin}, call: s.rejectChangeRequest},
		{name: "admin-releases-create", method: http.MethodPost, pattern: "/v1/admin/releases", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.createRelease},
		{name: "admin-releases-get", method: http.MethodGet, pattern: "/v1/admin/releases/{rel_id}", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.getRelease},
		{name: "admin-releases-approve", method: http.MethodPost, pattern: "/v1/admin/releases/{rel_id}/approve", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.approveRelease},
		{name: "admin-releases-rollback", method: http.MethodPost, pattern: "/v1/admin/releases/{rel_id}/rollback", owner: "governance",
			roles: []string{roles.PlatformAdmin}, call: s.rollbackRelease},
		// 审计查询只给审计岗；查询本身既有这里的管理审计，也有 governance 的变更审计（governance 详设 9.3）
		{name: "admin-audit-query", method: http.MethodGet, pattern: "/v1/admin/audit", owner: "governance",
			roles: []string{roles.Auditor}, call: s.queryAudit},
	}
}
