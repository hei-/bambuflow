# T2Retrieval 子集（只检索评测）

来源：C-MTEB 的 T2Retrieval（ModelScope 数据集 `MTEB/T2Retrieval`），Apache License 2.0。原始数据来自 T2Ranking，
本目录只存抽样结果的元数据，不存段落正文。

- `queries.jsonl`：500 条查询及其相关段落 ID（标注里分数大于 0 的段落）。
- 语料：全量约 11.8 万段（`meta.json` 的 corpus_all），在 GPU 主机上嵌入；CPU 环境可在脚本里把 CORPUS 设为 5000，此时生成 `corpus_ids.txt` 列出抽样的段落 ID。
- `meta.json`：原文件路径、sha256、抽样参数；`tools/test-upstream/make_t2_subset.py` 生成。
- `reports/baseline.*`：只检索评测的基线（真实嵌入模型），`make eval-retrieval` 与它比较。

入库：`make bench-seed` 下载原文件、校验 sha256、筛出段落，写入数据源 `src_t2retrieval`（只授权给评测租户 bench），
随后 `make ingest` 发布。
