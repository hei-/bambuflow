# 身份同步的上游配置

平台身份库（`identity.identities`、`identity.identity_groups`）只由身份同步写入，gateway 签发 Principal、retrieval 取大群组都读它
（gateway 详设 3.5）。身份同步是 governance 的子命令 `identity-sync`，配置文件由 `BF_IDENTITY_SYNC_CONFIG` 指定，
上游按部署三选一。各种上游共用 `source`（同步源的名字）、`interval` 与 `mapping`（字段映射与密级映射，映射表里没有的等级按最低密级处理）。

身份同步超过 15 分钟没有成功时 gateway 不再签发 Principal（fail-closed）。

部署：compose 里是 `identity-sync` 服务；Helm 用 `identitySync.config` 写配置文件内容（必须配置），数据库连接串与上游凭证放在 Vault 的
`bambuflow/identity-sync`。LDAP 与导出文件单副本定时拉取；SCIM 两个副本，Service 端口取 `scim.listen`，网络策略只放行入口所在命名空间，
入口代理按路径 `/scim/v2/` 转发；导出文件用 `identitySync.exportClaim` 指定上游写入的 PVC。

## LDAP / Active Directory

每个 `interval` 按修改时间增量拉取，每隔 `full_every` 全量对账（目录里已不存在的用户停用）。开发环境的例子见
`tools/test-upstream/identity-sync.yaml`。

## SCIM 推送

上游目录或 IAM 按 SCIM 2.0 把用户与群组的变化推送到平台，秒级生效。

```yaml
source: corp-scim
scim:
  listen: ":8090"          # 端点：<对外地址>/scim/v2/Users、/scim/v2/Groups，经入口代理对上游开放
  token_env: SCIM_TOKEN    # 上游调用凭证（Bearer 令牌，至少 32 个字符）所在的环境变量，凭证放在 Vault
mapping:
  username: userName
  display_name: displayName
  title: title
  dept: urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:department
  region: addresses.region           # 多值属性取 primary 的一项
  grade: urn:corp:params:scim:User:grade
  clearance_by_grade: {C1: public, C2: internal, C3: confidential}
interval: 1m               # 端点在运行、能写库时每隔这么久记一次同步成功
```

- 映射的属性名：顶层属性用原名，扩展 schema 的属性写成“schema URN:属性”，复合属性写成“属性.子属性”。
- `active` 为 false 即停用；删除用户只标记停用（审计和回放要保留历史身份），删除群组清掉它的全部成员关系。
- 用户的 id 就是平台账号，群组的 id 就是群组名：租户配置与策略按群组名引用，改名等于换了一个用户或群组，所以不支持改名
  （返回 400），上游应先删再建。
- 支持新建、查询（filter 只支持 `id`、`externalId`、`userName` / `displayName` 的 `eq`）、PUT、PATCH（add、replace、remove）
  与删除；不支持批量、排序、ETag 与改密码。
- 推送式的上游停推时平台无从察觉：上游自己的推送失败要在 IAM 的预配日志里告警。

## 导出文件（兜底）

上游定时放置一份 CSV（UTF-8，首行为列名，每行一个用户），平台每个 `interval` 检查一次，内容变了就整份替换
（文件里没有的用户停用、没有的群组清掉），没变只记一次成功。

```yaml
source: hr-export
file:
  path: /var/lib/identity/people.csv
  max_age: 25h             # 文件超过这么久没有更新算同步失败
  groups_column: groups    # 用户所属群组的列
  groups_separator: ";"
mapping:
  username: account
  dept: department
  title: title
  region: region
  grade: grade
  clearance_by_grade: {C1: public, C2: internal, C3: confidential}
interval: 1m
```

- 上游须整份写完再改名放到位，避免读到写了一半的文件；空文件与账号重复的文件整份拒绝。
- 撤权的生效时间取决于导出频率，须在该部署的撤权 SLO 中单独声明；`max_age` 要按导出频率留出余量。
