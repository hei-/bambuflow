# Wiki 样例源（只用于开发与 CI）

`wiki/` 下是虚构的公司内部制度页面，供 P0 入库与检索测试使用。front matter 模拟源系统接口返回的元数据与 ACL。
部分页面（credit-personal-loan、credit-mortgage-loan-2023、risk-limit-policy）另带源系统的结构化字段
doc_type、dept、topics、effective_date、status，用来覆盖元数据“源端字段优先”的路径；其余页面的元数据靠标题与正文规则抽取，
取不到的记为缺失（入库管道详设第 7 节）。
connectors 只读取 `wiki/` 目录里带 front matter 的页面，所以本说明放在目录外。

| 页面 | 可见范围 | 密级 | 用途 |
| --- | --- | --- | --- |
| credit-personal-loan | group:credit-l2 | 内部 | 信贷业务的常规问答 |
| risk-limit-policy | group:risk-dept | 机密 | 跨部门不可见、密级高于普通员工 |
| staff-handbook | group:all-staff | 公开 | 全员可见 |
| aml-report-procedure | group:risk-dept、group:credit-l2 | 内部 | 多群组授权；含超过 800 字的长节，验证按段落切分 |
| credit-draft-notes | 未写 ACL，只有属主 carl 可见 | 内部 | 没有 ACL 时按最保守处理 |
| legacy-orphan | 未写 ACL 且没有属主，无人可见 | 内部 | 最保守处理的边界情况 |

P1 扩充（2026-10-05，用于检索质量评测，内容均为虚构）：

| 页面 | 可见范围 | 密级 | 用途 |
| --- | --- | --- | --- |
| credit-mortgage-loan、credit-collateral、credit-post-loan、credit-small-business、credit-faq | group:credit-l2 | 内部 | 信贷制度；含表格（首付比例、抵押率、风险分类） |
| credit-mortgage-loan-2023 | group:credit-l2 | 内部 | 已废止的旧版，与新版数值冲突，测检索与回答是否误用旧版 |
| risk-market-risk、risk-operational、risk-stress-test | group:risk-dept | 机密 | 风险管理制度 |
| risk-draft-2026 | 未写 ACL，只有属主 bob 可见 | 机密 | 属主可见的机密草稿 |
| risk-capital-plan | group:risk-dept | 受限 | 密级高于风险经理（机密），任何测试账号都不可见 |
| compliance-kyc、compliance-sanctions | group:credit-l2、group:risk-dept | 内部 | 合规制度，多群组授权 |
| hr-travel、hr-training、hr-performance、it-password、it-remote-work、it-incident | group:all-staff | 公开 | 全员可见；差旅标准含表格 |
| ops-counter、ops-cash | group:all-staff | 内部 | 全员群组但密级为内部：公开级员工（eve）不可见 |

P1-T1b 干扰页面（`tools/test-upstream/gen_distractors.py` 生成，不进仓库，只进 connectors 开发镜像）：30 个分行对 10 项总行制度的补充通知与例会纪要，共 230 页，
主题与评测集考查的制度相近，只引用总行制度、不改其中的数值。ACL 与密级按主题区分：信贷类 group:credit-l2 内部级，通用类 group:all-staff 公开级，风险类 group:risk-dept 机密级。
