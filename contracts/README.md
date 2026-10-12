# contracts

模块之间唯一的通信契约（AGENTS.md 第 0 节）。服务之间不互相 import，只依赖这里生成的代码。

| 目录 | 内容 |
| --- | --- |
| `bambuflow/<领域>/v1/` | protobuf：模块间 gRPC 接口与事件。proto 包名与目录一致（`bambuflow.answer.v1` 在 `bambuflow/answer/v1/`） |
| `openapi/v1/openapi.yaml` | 对外 HTTP 接口，字段以技术设计文档 13.9 为准 |
| `openapi/ruleset.yaml` | 对外命名规则（13.11）：路径 kebab-case、字段与枚举值 snake_case、对外 ID 带类型前缀 |
| `gen/go/` | 生成的 Go 代码，独立模块 `github.com/hei-/bambuflow/contracts/gen/go` |
| `gen/python/` | 生成的 Python 包 `bambuflow-contracts`，导入路径 `bambuflow.<领域>.v1` |

## 规则

- **只放已实现的字段**：契约里的字段都是当前代码真正产出或接受的；以后加字段是兼容改动。设计文档里规划了、还没实现的字段不提前写进来。
- **枚举值一次列全**：枚举按设计列出全部取值，即使暂时只用到一部分。调用方会穷举处理枚举，事后加值会让它们出错。
- **兼容性**：proto 由 `buf breaking` 与目标分支比较（FILE 级）；不兼容的改动升大版本（`v2`）并双写过渡（技术设计文档 12.5）。
- **生成代码入库**：改了 proto 运行 `make contracts` 并提交 `gen/` 的变化；CI 会重新生成并比对，不一致即失败。不要手改 `gen/` 下的文件。
- **改了对外字段**：同时改 `openapi/v1/openapi.yaml` 和对应的 proto，两边命名一致（OpenAPI 的枚举值是 proto 枚举名去掉前缀后的小写形式，如 `STATUS_ANSWERED` → `answered`）。
