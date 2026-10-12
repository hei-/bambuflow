# 接入一个新业务系统

新业务线按“一份配置 + 一套评测集”接入，不改主流程代码。分试点与全量两个阶段上线：试点期问答只对试点群组开放、
检索对全部成员开放；评测集达到全量要求、门禁通过后，经变更单切到全量。开发环境里的 risk 租户（风险管理系统）
就是按本文接入的例子，配置见 `tools/test-upstream/tenants/risk/tenant.yaml`。

管理操作都可以经管理控制台或 `/v1/admin` 接口完成，下面写的是接口；角色见各步的“谁来做”。

## 1. 数据源

| 步骤 | 谁来做 | 接口 |
| --- | --- | --- |
| 注册数据源（类型、归属部门、默认密级） | 平台管理员 | `POST /v1/admin/sources` |
| 审批，写明依据（如工单号） | 归属部门的数据属主 | `POST /v1/admin/sources/{src_id}/approve` |
| 部署这个源的同步任务（connectors，定时同步与每日对账），首次全量入库 | 平台运维 | 部署配置 |
| 申请授权（写明用途） | 新租户的负责人 | `POST /v1/admin/grant-applications` |
| 审批申请，写明依据；审批即授权 | 归属部门的数据属主 | `POST /v1/admin/grant-applications/{application_id}/approve` |

同步方式二选一（Helm `connectors.sources`）：定时任务（`schedule`），或常驻清单轮询（`poll: true`，5–15 分钟按源系统的响应自动调整，
当前间隔在控制台数据源页可见）。源系统能推送变更通知时再加 `webhook: true`：源系统向 `connectors-poll-<数据源>` 的 `/webhook`
发 POST，请求头 `Bambuflow-Webhook-Timestamp`（Unix 秒）与 `Bambuflow-Webhook-Signature`（`v1=` 加 HMAC-SHA256(密钥, 时间戳 + "." + 请求体) 的十六进制），
密钥（至少 32 个字符）放在 Vault 的 `bambuflow/connectors-<数据源>` 的 `BF_WEBHOOK_SECRET`。通知只用来提前触发一轮同步，内容不解析。

注册数据源时声明权限模型（`permission_model`，不填按文档级）。按知识库或目录给部门、群组授权的源（制度库、部门共享盘）
可声明为分区级：文档元数据写 `partition`（知识库或目录路径），分区授权写在 Helm `connectors.sources[].configFiles` 的一份
`partitions.yaml`（`partitions: {制度库/风险管理: [dept:风险管理部, group:risk-auditors]}`），并设
`env.BF_PARTITIONS_FILE: /etc/bambuflow/source/partitions.yaml`。改授权只改这份文件，下一轮同步后生效，不重建索引；
单份文档自带 ACL 的按文档级处理并告警。

关系型数据库默认只同步表结构（`sync-dbschema`），行数据问答时实时查询。确实要和文档一起检索的表（产品参数、费率、制度条款表）
可以按表开启记录入库（`sync-records`，入库管道详设第 6 节）：配置里写明数据属主的书面同意（`consent`）、入库的列与字段说明、
主键，以及行级权限（权限列 `acl_column`，text[] 里放 `group:`/`dept:`/`user:` token；或整表的 `acl`）；
敏感列标 `mask: true` 只存掩码。一行一张记录卡片，权限表达不了的行不入库。

没有现成连接器的业务系统可以推送入库（入库管道详设 14.5）：
1. 在平台授权服务里给业务系统建一个只开服务账号（客户端凭证）的客户端，受众加上 `bambuflow-platform`；
2. 注册数据源时填 `push_client`（这个客户端 ID），随数据源一起审批，不需要部署同步任务；
3. 业务系统用客户端凭证换得的令牌调用 `PUT /v1/sources/{src_id}/documents/{doc_id}`、`DELETE` 同一路径、
   `POST /v1/sources/{src_id}/documents/batch`（每批最多 100 份）。ACL 必填，用户与群组须在平台身份库里存在；
   单份原文最大 10 MB。内容与权限都没变的重复推送不会重复处理。

已有的数据源只需要授权这一步。数据属主也可以不等申请直接授权（`POST /v1/admin/sources/{src_id}/grants`）。数据源下线时由数据属主撤销：在线立即不可见，全部文档写删除标记，观察期后物理删除。

## 2. 租户配置（试点）

由平台管理员提交新租户的配置，另一位平台管理员审批后生效（变更单流程）：

```yaml
tenant: risk
rollout: pilot              # 试点：问答只对 pilot_groups 开放，检索对 member_groups 全部开放
member_groups: [risk-dept]  # 租户成员（公司身份目录里的群组）
pilot_groups: [risk-pilot]  # 试点群组
bound_clients: [risk-portal] # 业务系统的客户端：用它登录的请求固定落在本租户，不用传 Bambuflow-Tenant
quota: {max_concurrency: 50, daily_tokens: 20000000}
```

- `POST /v1/admin/tenants` 提交，`POST /v1/admin/change-requests/{chg_id}/approve` 审批；1 分钟内推到 gateway。
- 业务系统的客户端在平台授权服务（Keycloak 的平台 realm）里登记，客户端 ID 与 `bound_clients` 一致，两种方式选一种：
  - **代用户调用**（业务系统已有用户登录，gateway 详设 3.1）：平台运维执行 `deploy/authz/register-business-client.sh <客户端 ID>`，
    建只做令牌交换的机密客户端并允许它交换。业务系统拿用户的公司 IdP 令牌做令牌交换（RFC 8693）换平台令牌，
    调用 gateway 时出示 mTLS 客户端证书，证书 CN 与客户端 ID 一致（入口代理把证书主题写进 `gateway.clientCertHeader` 指定的请求头）；
  - **用户直接登录平台**：授权码 + PKCE 的公开客户端，回调地址填业务系统的地址。
- 只做检索、自己生成答案的业务系统用 `/v1/search`（仅检索模式）。
- 要经 MCP 接入的 agent 写在 `mcp_agents` 里（客户端 ID、用途、负责人）。

## 3. 评测集

评测集是接入周期的主要工作（评测详设 3.1）：

| 阶段 | 要求（只计已审核的题） |
| --- | --- |
| 试点 | ≥ 100 题，应拒答 ≥ 15%，合成题（LLM 生成后一人审核）≤ 50% |
| 全量 | ≥ 300 题，应拒答 ≥ 15%，合成题 ≤ 50%，双人标注题 ≥ 150 |

- 业务专家提交候选题：`POST /v1/admin/golden-sets/{tenant}/questions`；
  复核要业务专家（数据属主）与平台管理员各一人：`POST /v1/admin/golden-sets/{tenant}/questions/{question_id}/review`。
- 复核通过的题用 `python -m bambuflow_evaluation candidates --tenant <租户>` 导出成 YAML，走合并请求加进 `golden/` 下的评测集；
  每道题带 `provenance`（来源、作者、审核人），据此计算合成题占比与双人标注题数。
- 试点期的线上问题脱敏抽样与用户负反馈是补足 300 题的主要来源。
- 每次评测运行与发布门禁的报告里都有各租户的评测集情况（满足哪个阶段、还差什么），console 的评测门禁页可以看。

## 4. 上线检查与切到全量

1. 试点前：权限评测集零越权（`make eval-perm`），租户评测集 Groundedness ≥ 0.9。
2. 试点期：看质量趋势（`GET /v1/admin/quality-trends`）、用户反馈与拒答率，持续补题。
3. 切到全量：跑一次覆盖该租户的评测门禁（`make eval` 或 CI，门禁记录在 `/v1/admin/eval-gates`），
   再提交把 `rollout` 改为 `general` 的变更单并附上 `gate_id`。governance 核对门禁已通过、覆盖该租户，
   且门禁里该租户的评测集已满足全量要求，否则拒绝（409 `gate_failed`，说明还差什么）；另一位平台管理员审批后生效。
