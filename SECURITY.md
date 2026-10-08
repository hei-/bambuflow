# 安全

## 报告漏洞

请不要在公开 issue 里报告安全漏洞。请通过 GitHub 的 **Security → Report a vulnerability**（私下报告）提交，
写明影响的模块、复现步骤与影响范围。我们会在 7 天内确认收到，并在修复发布后致谢（如你愿意）。

特别关注的问题：越权访问（召回或回答里出现调用者无权看的内容）、绕过逐句校验、Principal 伪造或提权、
审计缺失、提示词注入导致的越权，以及 sql_query 的参数逃逸。

## 开发环境的口令

`deploy/compose/` 与 `tools/` 里形如 `dev-only-*` 的口令、Vault 开发令牌、测试账号与签名密钥只用于本地开发与 CI，
不要用在任何真实环境。生产部署的密钥与连接串一律来自 Vault（见 `deploy/helm/bambuflow/values.yaml` 的说明），
不进 values 与镜像。
