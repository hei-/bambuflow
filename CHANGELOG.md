# 版本记录

版本号按语义化版本（Semantic Versioning）写成 `vX.Y.Z`，每个版本在公开仓库是一个提交和一个同名标签。

1.0 之前：

- **次版本号（Y）**：新增一组可以演示或使用的能力（一个里程碑、一批新的对外接口或页面），或者对外契约
  （`/v1` 接口、Webhook 事件、MCP 工具、回答契约字段）有不兼容的改动时递增，修订号归零。不兼容的改动在这里写明迁移方式。
- **修订号（Z）**：只修缺陷、改文档或部署配置，不加新能力、不改对外契约时递增。
- **1.0.0**：可以投入生产时发布：Kubernetes 部署完整（含评测副本与各类定时任务）、至少接入一个真实数据源、
  质量门禁有基线并通过、对外契约冻结。之后按语义化版本的常规规则递增。

## v0.1.0

可以交出去演示的版本：`make demo` 一条命令起环境，在浏览器里演示问答与管理操作，见 `docs/demo.md`。

- 管理控制台（`services/console`）：问答页（流式回答、引用角标与原文预览、why、置信度、反馈、多轮会话）；
  管理页（数据源同步与对账、待删批次、索引版本切换与双人复核、评测门禁报告、紧急拒绝、审计查询）。
- 全量重建期间内容变更同时写入线上版本与新版本；重建版本写完后跑完整评测集门禁，通过才上线。
- 每日对账（`connectors reconcile-*`、`make reconcile`）：与已入库的文档比对，补发漏同步、内容或权限不一致与源端已删的变更；
  `POST /v1/admin/sources/{src_id}/sync` 立即同步（接入 Kubernetes 调度时可用）。
- 管理控制台另有租户配置（变更单提交、审批与回退）、评测集（候选题复核、负反馈案例）、运维状态（配置快照分发、删除凭证）。
- 撤销数据源后对该源全部文档写删除标记；物理删除时一并删掉对象存储里的原文，`GET /v1/admin/deletion-receipts` 查删除凭证。
- 第二个业务系统接入：评测集按租户算接入阶段（试点、全量），切全量时核对；风险管理租户试点；接入手册 `docs/onboarding.md`。
- 用户的负反馈回流评测：`/v1/admin/golden-sets/{tenant}/feedback-cases`；在线质量趋势带反馈计数。
- 入库平面上 Kubernetes（`deploy/helm/bambuflow-ingest`）；processing 常驻，失败按有上限的指数退避重试。
- 配置快照写本地磁盘，governance 不可用时实例重启仍能启动；`GET /v1/admin/snapshot-sync` 查各实例的同步情况。
- 审批类管理操作要求加强认证：令牌的 acr 与登录时间不满足时返回 401 `insufficient_user_authentication`（RFC 9470）。
  部署时须配置 `gateway.stepUp.acrValues`（Helm）或 `BF_STEP_UP_ACR`，gateway 没有这项不启动。
- 评测身份改由 evaluation 自己的签发方签发，只在评测副本上有效；部署时在 Vault 写入 `evaluation/principal-signing`。
- 数据源：`GET /v1/admin/sources` 列出数据源（数据属主只看本部门）；受限级数据源可以注册，要本部门属主与信息安全各审批一次
  （`POST .../approve` 的 `capacity`）才生效。`GET /v1/admin/change-requests`、`GET /v1/admin/releases` 按状态列出变更单与发布。
- 管理控制台补齐页面：数据源（注册、审批、租户授权、撤销与同步运维）、租户配置（待审批变更单与发布列表、新建租户）、
  A/B 实验、受控词表、插件、入库运维（入库状态、死信与重放）。`config.json` 不再需要 `sources`。
- 租户负责人：租户配置新增 `owner_groups`，其中的用户可以只读本租户的配置版本、评测门禁与质量趋势。
- 在线抽样评审读回答原文前先写访问审计，写不进去就不读；部署时在 Vault 的 `bambuflow/evaluation` 加审计桶凭证
  （`BF_AUDIT_S3_ACCESS_KEY`、`BF_AUDIT_S3_SECRET_KEY`）。
- 入库失败处理补全：各事件流的消费者认领空闲的挂起消息（Pod 重建后不再滞留）；processing 的重试时间持久保存；
  解析与 OCR 在子进程里按 10 / 30 分钟超时；增强失败的文档之后补做。
- 配置灰度：pipeline、routing、策略包、回答模板审批后先对一部分用户生效（租户对象为该租户 10% 的用户，全局对象为一个试点租户），
  在控制台全量或终止；回退可以插队并终止进行中的灰度。
- 灰度观察：evaluation 每 5 分钟比较灰度用户与对照组的引用覆盖率、拒答率、越权拦截与 p95，超阈值自动终止灰度，
  观察满 30 分钟且样本够时自动全量；控制台发布列表显示最近一次观察结果。
- 配置发布的评测门禁：pipeline、路由、回答模板与策略包的发布由 governance 自动发起门禁，评测副本按候选版本回放
  （候选只对评测身份开放），通过后才能审批。
- 稀疏向量召回（retrieval 的 sparse 召回路，默认不开，`BF_RETRIEVERS` 列出 sparse 才执行）：嵌入模型在稠密向量之外给出学习型稀疏向量
  （输出名 sparse，同一次调用），入库时写进块记录，检索按内积召回并参与融合；分数解释新增 `sparse`。开启了而模型不给稀疏向量时这一路降级。
  indexing 新增迁移 0021。
- retrieval 缓存（配置 `BF_CACHE_REDIS_ADDR`，Helm `retrieval.cacheRedisAddr` 才开启）：策略决定进程内 60 秒；查询向量进程内加 Redis
  7 天；检索结果只存块 ID 与分数、10 分钟，命中时按块 ID 带全部必含条件重新取块并照常复核，可能看到受限级内容的请求与降级结果不缓存。
  indexing 在权限变化或写删除标记后发 ChunkAclChanged（Redis 流 `retrieval:chunk-acl`），retrieval 各副本按文档清掉相关结果。
- PII 识别在规则之外可接 NER 服务（`BF_NER_URL`，Helm `processing.nerUrl`，`POST /v1/ner`）：人名、地址等同样等长掩码、原值加密；
  NER 失败时文档停在脱敏阶段重试，不跳过脱敏入库。受控元数据新增模型抽取端口，只补规则没取到、置信度不低于 0.8 的字段（默认不接）。
- 解析补全：超过 1500 字的表格除按行组切分外另生成一块文字摘要（章节、行数、各列与首列取值，span 指向整张表）供语义召回；
  有文字层的 PDF 里的内嵌图片（不小于 64 × 64）做 OCR 生成图片块，挂在所属章节下；配置了图片描述服务
  （`BF_IMAGE_DESCRIBER_URL`，Helm `processing.imageDescriberUrl`）时图片块前面加一句图片说明。
- 效力信息的替代关系（评估报告 K3）：文档声明替代的旧文档（源系统字段 `supersedes`，或正文“《…》同时废止”）在同一数据源里关联，
  旧文档记下被谁替代并改为已废止（替代它的待生效时不改），只改文档级字段、不重新向量化；旧文档后到或重新入库都保留替代关系。
  枚举类回答的列表项标出“已被新版替代”。indexing 新增迁移 0020。
- 入库运维：向量化前按（嵌入模型、输入摘要）复用已入库的向量，增量入库只算改动的块；`POST /v1/admin/replays` 新增时间段
  （`from`、`to`）、组件版本（`pipeline_version`）与 `include_succeeded`（修好组件后重新处理已成功的文档，一次最多 1 万份）；
  样例源单份文档读不出或发出失败不中止整轮（跳过的记进同步状态，这一轮不推断删除）；全量重建用的 processing 可放独立节点池
  （Helm `rebuildWorker.nodeSelector`、`tolerations`）并按每秒文档数限速（`rebuildWorker.maxDocsPerSecond`，默认 5）。
  indexing 新增迁移 0019。
- 结构化记录入库（`connectors sync-records`）：按表开启，须写明数据属主的书面同意与行级权限（权限列或整表 token），
  一行一张记录卡片（表名、主键与“字段说明：值”，敏感列只存掩码），权限表达不了的行不入库；增量同步与删除保护同其他数据源。
- 分区级数据源（总体设计 ADR-14）：注册时可声明权限模型为分区级（`permission_model`）；文档只带分区 token，分区授权由连接器
  按 `BF_PARTITIONS_FILE` 同步、indexing 写入分区授权表，retrieval 按调用者展开；改授权不改索引，下一个请求就按新授权判定。
  governance 新增迁移 0025，indexing 新增迁移 0018。
- 推送入库 API（`PUT`/`DELETE /v1/sources/{src_id}/documents/{doc_id}`、`POST /v1/sources/{src_id}/documents/batch`）：
  没有现成连接器的业务系统用服务账号令牌推送文档；只接受数据源注册时登记、随数据源审批的推送客户端（新字段 `push_client`）；
  ACL 必填且用户与群组须在平台身份库里存在，密级不低于数据源默认密级；内容与权限都没变的重复推送不重复处理。
  governance 新增迁移 0024。
- 常驻清单轮询：数据源可改为 `connectors poll <sync-*>` 常驻同步（Helm `connectors.sources[].poll`），间隔 5–15 分钟按源系统的
  限流与响应时间自动调整，当前间隔与下一轮时间在 `GET /v1/admin/sources/{src_id}/sync-status` 与控制台可见；立即同步改为唤醒它。
  `webhook: true` 时接收源系统的变更通知（HMAC 签名，密钥 `BF_WEBHOOK_SECRET`），通知只提前触发一轮同步。
- 租户负责人的写权限：为自己负责的租户申请数据源授权（`POST /v1/admin/grant-applications`），数据源所属部门的数据属主审批
  （要求加强认证，审批即写入授权）或驳回，申请人可撤回，7 天未审批过期；控制台数据源页新增“授权申请”。租户负责人还能查看、
  审批本租户 pipeline 的发布并全量或终止灰度。governance 新增迁移 0023。
- 受限级内容每次访问单独审计：检索、问答（含流式的每条受限级引用与列表项）、回放、预览与 MCP 发出受限级内容之前，
  gateway 另写一条 `restricted_access` 记录，写不进去就不发；审计查询新增类别 `restricted`（`GET /v1/admin/audit?kind=restricted`），
  控制台审计页可选。批量问答的身份密级最高到机密，受限级文档不进批量结果。governance 新增迁移 0022。
- 紧急拒绝规则的备用通道（`docs/emergency-denials.md`）：governance 登记规则时另用单独的密钥签名、发到事件流的独立主题，
  retrieval 直接订阅；governance 不可用时信息安全用 `governance emergency-deny` 签发，发到事件流或写成文件投放到 retrieval 的配置卷
  （Helm `retrieval.emergencyDenials`）。两条通道只接受验签通过的规则、只能增加拒绝。部署时在 Vault 写入
  `governance/emergency-signing`（`private_key`、`public_key`），retrieval 新增 `BF_REDIS_ADDR`。
- Helm Chart 部署身份同步（此前只有 compose 部署）：`identitySync.config` 必须配置，凭证放在 Vault 的 `bambuflow/identity-sync`。
- 身份同步新增两种上游（与 LDAP 三选一，见 `docs/identity-sync.md`）：SCIM 2.0 推送端点（用户与群组的新建、查询、PUT、PATCH、删除，
  Bearer 令牌认证）与导出文件（CSV，内容变了整份替换，超过 `max_age` 没更新算同步失败）。
- 业务系统代用户调用：业务系统拿用户的公司 IdP 令牌向平台授权服务做令牌交换（RFC 8693），得到 act 为自己的平台令牌；
  gateway 要求请求同时出示该业务系统的 mTLS 客户端证书（入口代理校验后把主题 DN 写进 `BF_CLIENT_CERT_HEADER` /
  Helm `gateway.clientCertHeader` 指定的请求头，CN 须与 act 一致），未配置时不接受这类令牌。平台授权服务镜像启用
  `token-exchange` 与 `admin-fine-grained-authz:v1`；业务系统用 `deploy/authz/register-business-client.sh` 登记。
- Principal 签名密钥定期轮换（默认 30 天，`BF_SIGNING_KEY_ROTATE_EVERY` / Helm `gateway.signingKeyRotateEvery`）：gateway 到期时
  生成下一把写回 Vault，先在 JWKS 里与当前一把同时发布，15 分钟后才用它签名，上一把留到下一次轮换。部署时 gateway 改用单独的
  Vault 角色 `vault.gatewayRole`，须能读写 `<kvMount>/gateway/principal-signing`；在线服务的角色不再需要读 `gateway/*`。
- 审计索引：审计查询改走索引，时间跨度不限，覆盖 gateway 的入口审计、各服务的变更审计与评测回放（门禁回放的请求写审计、
  带评测标记），可以反查一份文档展示给过哪些回答和用户。gateway 审计段的对象键改为按送达日期分目录。
- A/B 实验可以替换已过评测门禁的 pipeline（检索计划、agent 预算、阈值）、routing（生成模型与置信度权重）与回答模板版本；
  routing 新增 `generation_model`（须是部署 `BF_LLM_ALT_MODELS` / Helm `orchestrator.llmAltModels` 里的模型），
  pipeline 新增 `agent.max_rounds`、`agent.max_sub_questions`。
- 策略包的发布须两位不同的人审批，其中至少一位是信息安全或数据属主；7 天未审批的发布与变更单自动关闭。
- 管理控制台新增“配置对象”页：修改 pipeline、routing、策略包、回答模板时写成合并请求（GitLab，或开发用的本地目录），
  合并后由 CI 发布生效；部署时用 Helm `governance.gitForge` 配置，不配置时只能经 CI 发布。
- 回答模板经 governance 发布（五分区检查）后生效；租户 pipeline 可写租户规则（业务口径、术语、回答风格）；
  对话摘要进入回答的提示词。内置模板版本升为 `qa-single-hop@4`、`qa-multi-hop@3`。
- 检索权限策略包经 governance 发布：全局基线、数据源、租户三层合成，发布前编译、跑测试、核心约束检查，审批时可看到
  可见范围变大的主体；随配置快照签名下发，retrieval 验签后热加载（新包不合法时继续用当前的）。
  **部署迁移**：Helm 的 `policyData` 改为 `policyLayers`（键 `tenant.<租户>` / `source.<数据源>`，值为该层 rego，至少一个测试），
  只作 governance 首次启动的初始版本；retrieval 不再挂载策略 ConfigMap、不再读 `BF_POLICY_DIR`。
- 受治理的配置对象：租户 pipeline（关闭召回路、融合权重、精排开关、支持度与覆盖率阈值、单跳 LLM 重排）与全局 routing
  （置信度权重）经 governance 发布（须附评测门禁）后下发 retrieval 与 orchestrator，版本写进 why 与审计。
- 多轮会话：会话保存对话摘要（受限级会话不存）与上一轮引用，追问“上面那个制度”时能找到所指的文档；
  `DELETE /v1/conversations/{conversation_id}` 删除自己的会话，控制台问答页可直接删除。
- 检索补全：问题里有制度文号、工单号等编号时精确匹配并置顶；`/v1/search` 返回每条结果命中的召回路与名次（`matched_by`），
  `include_score_details` 时返回各路分数；回答的 why 新增 `evidence_scores` 与 `llm_rerank`（多步检索用 LLM 挑选证据）。
- 证据冲突检测：回答之外的来源与回答矛盾时置信度下调，回答末尾提示核实并引用矛盾的来源；综合句与所引来源矛盾时不放行。
  部署要求：NLI 模型须同时输出 `support` 与 `contradiction`。评测报告新增置信度校准（拟合权重与 ECE）。
- 按密级选模型：外部模型只处理公开级内容，受限级只用专用实例（`BF_LLM_KIND`、`BF_LLM_RESTRICTED_URL/MODEL`，
  Helm `orchestrator.llmKind`、`llmRestrictedUrl`、`llmRestrictedModel`）；不允许调用模型时只返回证据。
- 审计补全：每条问答与检索的审计带上索引、模型、提示词、策略版本与回答摘要，审计页可见；仅检索的返回结果记为“展示”；
  定时发布、按保留期过期、物理删除、撤销数据源后的清理、评测门禁结论写变更审计，写不进去就不执行。
  部署时 Vault 的 `bambuflow/indexing`、`bambuflow/connectors` 要加审计桶凭证（`BF_AUDIT_S3_ACCESS_KEY`、`BF_AUDIT_S3_SECRET_KEY`），
  入库 chart 新增 `audit` 配置。
- 评测加 LLM 评审：Faithfulness 与答案正确率进评测报告，Faithfulness 进在线质量趋势；配置 `BF_JUDGE_URL`、`BF_JUDGE_MODEL`
  （Helm 的 `evaluation.llmJudgeUrl/llmJudgeModel`，密钥 `BF_JUDGE_API_KEY` 放 Vault 的 `bambuflow/evaluation`）后生效。
- 单跳问答加查询理解：多轮追问按前几轮问题改写成完整问题再检索；同义词换成受控词表的规范名称一起检索；
  “去年”“2025 年”“现行”这类说法转成生效日期与效力条件（why 里可见，检索无结果时自动去掉）。
- 删除与仅权限变化改走独立事件流 `ingest:document-access`，由 indexing 直接处理，不再经过 processing。
  升级前请等 processing 把 `ingest:document-changed` 里已有的事件处理完；残留的删除与权限事件会进死信，由每日对账重新发出。
- 修复：第一次发布索引时评测门禁无法判定、`make ingest` 发布失败；增量发布的门禁评测的版本缺少未变更的文档，
  质量指标被误判下降而拦下发布。

## v0.0.2

问答能力（多步检索、图路径、回放与反馈、引用预览、置信度）、运行保护（熔断、配额、自适应并发）、全部管理接口、
异步评测门禁、回答记录信封加密、存储期限与物理删除、受控词表变更的后续处理。

## v0.0.1

第一个公开快照：三个平面的骨架、权限在召回前生效与返回前复核、混合检索与重排、逐句校验与拒答、评测与权限评测集、
Compose 开发环境与 Helm chart。
