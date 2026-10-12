# 紧急拒绝规则的备用通道

紧急拒绝规则平时经管理接口（`POST /v1/admin/emergency-denials`）登记，由 governance 写进索引库的拒绝名单
（governance 详设 4.2）。同时 governance 把规则签名后发到事件流的独立主题 `bambuflow.emergency-denials`，retrieval 直接订阅：
索引库的拒绝名单写不进去时，规则照样生效。

governance 整体不可用时，信息安全有两条备用通道（governance 详设 8.2，部署配置清单 P8）。两条通道都只能增加拒绝，
retrieval 只接受验签通过的规则；签名密钥在 Vault 的 `governance/emergency-signing`（`private_key` 只给 governance 与信息安全，
`public_key` 给 retrieval），与快照签名密钥分开。

## 签发

用 governance 镜像里的命令行工具，带信息安全自己的 Vault 凭证（`VAULT_ADDR`、`VAULT_TOKEN`）：

```sh
# 追加到事件流的独立主题（需要 BF_REDIS_ADDR、BF_REDIS_PASSWORD）：各 retrieval 实例几秒内生效
governance emergency-deny -scope source -target src_xxx -reason "工单 SEC-123" -by <平台账号> -publish

# 或写成文件，经部署系统投放到 retrieval 的配置卷（Helm：retrieval.emergencyDenials），30 秒内生效
governance emergency-deny -scope document -target <文档 ID> -reason "工单 SEC-123" -by <平台账号> -out deny.json
```

`-scope` 为 `source`（该数据源的全部文档）、`document` 或 `tenant`（该租户的任何人都检索不到任何文档）。
retrieval 生效时记一条告警级日志（规则 ID、范围、签发人）。

## 生效范围

- 按租户、按数据源：召回、计数、列举与返回前复核都不再包含。
- 按文档：返回前复核拦下（检索结果可能少于 top_k），列举里去掉；计数仍含它（只是数字，不暴露内容）。

## 之后

- governance 恢复后，经管理接口登记同一条规则：写进索引库的拒绝名单，并留下变更审计。
- 规则只增不减。要解除，先经变更单解除正式规则，再删掉事件流里的条目（`XDEL`）或配置卷里的文件，滚动重启 retrieval。
