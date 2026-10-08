# 参与开发

## 环境

本机只需要 git、make 和 docker 客户端，不在本机编译：所有检查在工具容器（`deploy/toolbox/Dockerfile`）里执行，
开发环境由 Docker Compose 拉起。

1. 复制 `.env.local.example` 为 `.env.local`（不入库），按需填写：
   - `DOCKER_CONTEXT`：工具容器与开发环境所在的 Docker 上下文，不填就用本机 Docker；
   - `BF_GOPROXY`、`BF_PIP_INDEX_URL`、`BF_DEBIAN_MIRROR`：访问不了官方源时的镜像；
   - 外部 LLM 的地址、模型名与密钥（不填就用模拟服务，e2e 与评测都按模拟服务编写）。
2. 起环境并入库样例数据：`make up PROFILE=core,ingest`，然后 `make ingest`。
3. 跑端到端测试与评测：`make e2e`、`make eval-perm`、`make eval`。

| 命令 | 作用 |
| --- | --- |
| `make fix` | uv lock、ruff format/--fix、go mod tidy、gofmt，并把改动拷回本地 |
| `make check` | lint + test + 依赖规则 + 术语检查 + Helm 检查 |
| `make test M=services/retrieval` | 只测一个模块 |
| `make contracts` | 重新生成契约代码，并做 buf lint、兼容性与 OpenAPI 命名检查 |
| `make up` / `make down [VOLUMES=1]` | 起停开发环境 |
| `make e2e` / `make eval-perm` / `make eval` | 端到端测试 / 权限评测集（越权必须为 0）/ 评测集 |
| `make helm-check` / `make helm-smoke` | Helm 静态检查 / 单节点 k3s 上的冒烟测试 |

## 架构约束

- 一级目录：`contracts/`、`services/`、`libs/`、`config/`、`golden/`、`deploy/`、`tools/`、`docs/`。
  `services/` 下一个目录是一个可独立部署的单元；`libs/` 只放被至少两个模块使用的库。
- 服务之间不互相 import，只通过 `contracts/` 生成的接口通信，不共享数据库（各用自己的 schema）。
  只有 retrieval 读索引、只有 indexing 写索引。`make deps-check` 会检查这些规则。
- 权限在召回前生效、返回前复核；权限与溯源环节一律 fail-closed。未经校验的陈述不离开 orchestrator。
  影响回答的每个版本号都写进 `why`。
- 外部系统一律经端口接入，并自带默认实现或保守默认值。
- 对外契约：路径 kebab-case，字段 snake_case，对外 ID 带类型前缀（`ans_`、`conv_`、`bat_` 等）。
  改了 `contracts/` 必须跑 `make contracts`，兼容性检查不通过的改动需要先讨论。

## 提交前

- 先 `make fix` 再 `make check`；动了契约跑 `make contracts`；动了权限相关代码跑 `make eval-perm`。
- 行为变化要有对应的测试；修 bug 先写能复现的测试。
- 不靠削弱检查来通过：不加 `noqa`、`type: ignore`、`nolint`，不跳过或删除测试，不放宽断言。
- 密钥、令牌和个人信息不能进入代码、日志、错误响应或测试快照。
- 只改完成任务所需的代码；注释只写非显然的“为什么”。

## 中文写作

文档与注释用中文技术读者的通行说法，不逐字直译造词；首次出现的专业词写成“中文（English）”，之后只用中文。
术语以 [`docs/terminology.md`](docs/terminology.md) 为准，`make terms-check` 会扫描“避免使用”的写法。

## 许可

提交即表示你同意以 [Apache License 2.0](LICENSE) 授权你的贡献。
