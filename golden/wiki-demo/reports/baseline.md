# 评测报告：wiki-demo 2026-10-05.2

**模拟服务下的 CI 回归基线**（2026-10-06 由 main 的 CI run 37389298640（提交 b0e0dde）报告生成，新增按租户的指标。与上一版基线（run 37295438845）相比，Recall@10 从 0.967 变为 0.956，下降 1.1 个点，在 2 个点的门槛内；这段时间合并了多格式入库（P1-T2）与 P2-T0～T5，下降来自哪一项尚未排查。模拟服务的检索指标不反映真实质量，真实模型的基线见 baseline-real.md）

**结论：passed**

## 评测配置

- 评测集：`golden/wiki-demo` 版本 2026-10-05.2，109 题；来源 llm_generated_unreviewed，审核人 无（待审核）
- embedding_model：bge-m3@mock-hash-1
- generation_model：mock-chat-1
- index_version：ix-20261005-234516-384704
- nli_model：nli@mock-bigram-1
- policy_version：sha256:f72926859324e111
- prompt_version：qa-single-hop@1
- 判定器：NLI nli@mock-bigram-1，支持度阈值 0.5
- 同一配置连跑 3 次，指标取平均；每次各自对照基线下结论，结论一致才算可复现

## 指标

| 指标 | 本次 | 基线 | 设计门槛 |
| --- | --- | --- | --- |
| Recall@10 | 0.956 | 0.967 | 不低于基线 |
| 引用覆盖率 | 1.000 | 1.000 | ≥ 0.95 |
| Groundedness | 1.000 | 1.000 | ≥ 0.9 |
| 状态正确率 | 0.835 | 0.835 | 不低于基线 |
| 应拒答题正确拒答率 | 0.000 | 0.000 | ≥ 0.9 |
| 误拒率 | 0.000 | 0.000 | — |
| Answer 延迟 p50（ms） | 76 | 33 | — |
| Answer 延迟 p95（ms） | 157 | 88 | — |

## 按租户

| 租户 | 题数 | Recall@10 | 引用覆盖率 | Groundedness | 状态正确率 |
| --- | --- | --- | --- | --- | --- |
| credit | 62 | 0.926（基线 —） | 1.000（基线 —） | 1.000（基线 —） | 0.871（基线 —） |
| ops | 15 | 1.000（基线 —） | 1.000（基线 —） | 1.000（基线 —） | 0.667（基线 —） |
| risk | 32 | 1.000（基线 —） | 1.000（基线 —） | 1.000（基线 —） | 0.844（基线 —） |

## 分桶

| 分桶 | 题数 | 状态正确 |
| --- | --- | --- |
| should_refuse | 18 | 0 |
| single_hop | 91 | 91 |

## 逐题

| 题目 | 期望 | 实际 | 必引进前 10 | 引用块数 | 有支撑句 / 总句 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| q_wiki_001 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_002 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_003 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_004 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_005 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_006 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_007 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_008 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_009 | answered | answered | 0/1 | 4 | 4/4 |  |
| q_wiki_010 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_011 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_012 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_013 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_014 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_015 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_016 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_017 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_018 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_019 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_020 | answered | answered | 0/1 | 4 | 4/4 |  |
| q_wiki_021 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_022 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_023 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_024 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_025 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_026 | answered | answered | 0/1 | 4 | 4/4 |  |
| q_wiki_027 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_028 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_029 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_030 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_031 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_032 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_033 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_034 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_035 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_036 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_037 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_038 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_039 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_040 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_041 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_042 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_043 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_044 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_045 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_046 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_047 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_048 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_049 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_050 | answered | answered | 0/1 | 4 | 4/4 |  |
| q_wiki_051 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_052 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_053 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_054 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_055 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_056 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_057 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_058 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_059 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_060 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_061 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_062 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_063 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_064 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_065 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_066 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_067 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_068 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_069 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_070 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_071 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_072 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_073 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_074 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_075 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_076 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_077 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_078 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_079 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_080 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_081 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_082 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_083 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_084 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_085 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_086 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_087 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_088 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_089 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_090 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_091 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_092 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_093 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_094 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_095 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_096 | answered | answered | 1/1 | 4 | 4/4 |  |
| q_wiki_097 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_098 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_099 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_100 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_101 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_102 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_103 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_104 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_105 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_106 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_107 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_108 | insufficient_evidence | answered | — | 4 | 4/4 |  |
| q_wiki_109 | insufficient_evidence | answered | — | 4 | 4/4 |  |
