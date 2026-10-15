# 压测计划

技术设计文档 14.4 的八个场景，P2 在预生产执行。预生产的数据规模不低于生产的 20%，结果按比例外推；
压测产出的每卡吞吐与存储节点实测值替换技术设计文档 11.2 的估算数字。

## 工具

`tools/devtools/cmd/loadtest`（在测试工具镜像 `bambuflow-devtools` 里，命令 `loadtest`）按给定的到达速率对 gateway 的
`/v1/answers` 发请求：开环发送（到点就发，不等前一个返回），按分钟统计耗时分位、错误率、429 与回答状态，
结束时按通过标准判定，报告写到 `-out` 目录（`report.json`、`report.md`），不通过时退出码为 1。

```sh
loadtest -base https://rag-preprod.example -tokens tokens.txt -questions questions.jsonl \
  -rate 50 -duration 2h -out reports/steady
```

| 参数 | 说明 |
| --- | --- |
| `-tokens` | 每行一个用户的 Bearer 令牌，轮流使用。用预生产的测试账号，覆盖各租户与密级；令牌不进报告 |
| `-questions` | JSONL，每行 `{"question": "…", "kind": "single_hop" 或 "multi_hop"}` |
| `-rate`、`-to`、`-ramp` | 每秒请求数；给了 `-to` 时在 `-ramp` 内线性爬升到它，之后保持 |
| `-multihop` | 多跳题的比例，按比例摊匀出题（同样的参数每次发出的题目序列相同） |
| `-batch` | 开始时提交一个这么多条的批次，报告里给出它结束时的进度 |
| `-pass-p95`、`-pass-multihop-p95`、`-pass-error-rate` | 通过标准，默认 4 s、10 s、0.1% |
| `-max-inflight` | 压测端同时在途的上限，超过的请求记为丢弃；有丢弃时结果不能用（检查项“压测端没有丢弃请求”） |

统计口径：耗时只算拿到完整回答（收到 `final`）的请求；错误是 5xx、超时、连接失败、流式的 `error` 事件与没收到 `final`
就断开；429（限流、配额）单独计，不算错误。压测机与 gateway 之间的网络耗时也计入，压测机放在与接入方相近的位置。

**问题集**：按 7 : 3 混合评测集题与线上脱敏样本（线上样本经信息安全审核脱敏后才能用于压测）。评测集题从
`golden/<集合>/questions.yaml` 取 `question` 字段，`bucket` 为多跳的标 `multi_hop`；拒答类题（`should_refuse`）保留，
它们走完整的拒答路径，也是真实负载的一部分。

**LLM**：与 AI Infra 约定时段调用真实推理服务；其余时间 orchestrator 指向模拟服务（compose 里的 `mock-llm`），
单独测量编排链路的开销，结果标明用的是哪一种。

**服务端指标**：工具只看客户端能看到的；扩容、资源、质量这些看 Prometheus（“服务 RED”与“RAG 概览”仪表盘），
下文每个场景列出要看的指标。

## 场景

| # | 场景 | 负载模型 | 命令要点 | 通过标准 |
| --- | --- | --- | --- | --- |
| 1 | 稳态单跳问答 | 50 QPS 持续 2 小时，7 : 3 混合问题 | `-rate 50 -duration 2h` | p95 ≤ 4 s；错误率 < 0.1%；在线抽样 Groundedness 不低于离线基线 |
| 2 | 峰值突增 | 5 分钟内从 10 拉到 100 QPS，之后保持 15 分钟 | `-rate 10 -to 100 -ramp 5m -duration 20m` | 5 分钟内完成扩容；限流生效，不出现连锁 5xx |
| 3 | 多跳混合 | 50 QPS，20% 多跳 | `-rate 50 -multihop 0.2 -duration 1h` | 多跳 p95 ≤ 10 s；单跳 p95 不超标 |
| 4 | 入库高峰叠加 | 全量重建期间 50 QPS | 先发起全量重建（`POST /v1/admin/index-rebuilds`），再 `-rate 50 -duration 2h` | 在线 p95 劣化 ≤ 10%（与场景 1 比）；撤权仍 ≤ 15 分钟 |
| 5 | 批量叠加 | 1 万条批次 + 50 QPS | `-rate 50 -batch 10000 -duration 1h` | 在线不劣化（与场景 1 比）；批次按低优先级让出资源 |
| 6 | 缓存冷启动 | 清空全部缓存后 50 QPS | 清空检索缓存的 Redis（`FLUSHDB`），立即 `-rate 50 -duration 30m` | 10 分钟内恢复到 SLO（按分钟表的 p95） |
| 7 | 故障注入 | 50 QPS 下按 11.6 逐项注入 | `-rate 50`，按技术设计文档 11.6 的故障场景逐项注入 | 降级行为与表格一致；越权 0 条 |
| 8 | 长稳 | 30 QPS 持续 24 小时 | `-rate 30 -duration 24h` | 无内存泄漏，资源使用平稳 |

### 各场景另看的指标

1. **稳态**：`evaluation_online_groundedness` 与离线基线比较（在线抽样的结果第二天出）；`orchestrator_degraded_total` 应接近 0。
2. **峰值突增**：各服务副本数（HPA）在爬升开始后 5 分钟内到位；按分钟表里 429 先于 5xx 出现，5xx 不随 429 一起上升；
   `gateway_requests_total{reason="rate_limited"}` 按租户分布与配额一致。
3. **多跳混合**：工具报告里单跳、多跳分开的 p95；多跳的 `orchestrator_answer_duration_seconds{intent}`。
4. **入库高峰叠加**：重建期间每 10 分钟用测试数据源发一条撤权（改一份文档的权限），看 `indexing_access_lag_seconds` 的 p95；
   `indexing_write_throttled_total` 与 `retrieval_permission_replica_lag_bytes` 说明重建是否按副本回放延迟限速。
5. **批量叠加**：在线 p95 与场景 1 的差距；批次进度（报告末尾）与 qa/batch 的并发，确认批量没有挤占在线并发。
6. **缓存冷启动**：`retrieval_cache_lookups_total` 的命中率回升曲线与按分钟的 p95 一起看。
7. **故障注入**：每项注入期间的按分钟表与降级指标；结束后跑权限评测集，越权必须为 0。
8. **长稳**：各服务的常驻内存与 goroutine / 线程数（容器指标）没有持续上升；连接池、文件句柄平稳；
   结束时的 p95 与开始时相同。

## 产出

- 每个场景的 `report.md`、`report.json` 与对应时段的仪表盘截图，按日期放在压测记录里；
- 每卡吞吐（LLM、嵌入、NLI、重排）与存储节点的实测值，替换技术设计文档 11.2 的估算，并回填部署配置清单里的容量参数；
- 不通过的项开缺陷单，修复后单独复测该场景。
