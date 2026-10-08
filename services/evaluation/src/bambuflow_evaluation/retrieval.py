"""只检索评测：按公开评测集的标注计算 Recall@10、nDCG@10、MRR@10。

P1 计划 T1b，待确认 P1-6 的 C。

只调用 retrieval 的 Search，不经过生成，结果是确定的，所以只跑一次。
提问身份是评测租户 bench 的固定模板，它只被授权检索评测集这一个数据源
（tools/test-upstream/policy/data.json）。
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_evaluation.golden import Asker
from bambuflow_evaluation.metrics import MAX_DROP
from bambuflow_evaluation.signer import Signer

if TYPE_CHECKING:
    from bambuflow.retrieval.v1.retrieval_pb2_grpc import RetrievalServiceAsyncStub

K = 10
DOC_PREFIX = "t2-"  # retrieval_bench 写入时的 doc_id 前缀
ASKER = Asker("bench", "bench-user", "bench", "评测", 1, ("bench-readers",))
METRICS = ("recall_at_10", "ndcg_at_10", "mrr_at_10")


@dataclass(frozen=True)
class Query:
    id: str
    text: str
    relevant: frozenset[str]


def load(path: Path) -> tuple[dict[str, Any], list[Query]]:
    meta: dict[str, Any] = json.loads((path / "meta.json").read_text(encoding="utf-8"))
    queries = []
    for line in (path / "queries.jsonl").read_text(encoding="utf-8").splitlines():
        r = json.loads(line)
        queries.append(Query(str(r["id"]), r["text"], frozenset(r["relevant"])))
    return meta, queries


def score(ranked: Sequence[str], relevant: frozenset[str]) -> tuple[float, float, float]:
    """单条查询的 Recall@10、nDCG@10（二值相关）、MRR@10。"""
    top = list(ranked[:K])
    recall = len(relevant & set(top)) / len(relevant)
    dcg = sum(1 / math.log2(i + 2) for i, c in enumerate(top) if c in relevant)
    idcg = sum(1 / math.log2(i + 2) for i in range(min(len(relevant), K)))
    mrr = next((1 / (i + 1) for i, c in enumerate(top) if c in relevant), 0.0)
    return recall, dcg / idcg, mrr


async def evaluate(
    stub: RetrievalServiceAsyncStub, signer: Signer, queries: Sequence[Query], rerank: bool = False
) -> tuple[dict[str, float], str]:
    token = signer.sign(ASKER)
    md = (("authorization", "Bearer " + token), ("x-call-purpose", "search"))
    sums = [0.0, 0.0, 0.0]
    latencies: list[float] = []
    index_version = ""
    for q in queries:
        start = time.perf_counter()
        resp = await stub.Search(
            retrieval_pb2.SearchRequest(
                queries=[retrieval_pb2.SubQuery(id="q1", text=q.text)], top_k=K, rerank=rerank
            ),
            metadata=md,
            timeout=10,
        )
        latencies.append((time.perf_counter() - start) * 1000)
        index_version = resp.index_version
        ranked = [h.doc_id.removeprefix(DOC_PREFIX) for h in resp.hits]
        for i, v in enumerate(score(ranked, q.relevant)):
            sums[i] += v
    latencies.sort()
    metrics = {k: sums[i] / len(queries) for i, k in enumerate(METRICS)}
    metrics["search_p95_ms"] = latencies[max(0, math.ceil(0.95 * len(latencies)) - 1)]
    return metrics, index_version


def judge(metrics: dict[str, float], baseline: dict[str, float] | None) -> tuple[str, list[str]]:
    reasons = [
        f"{k} 从基线 {baseline[k]:.3f} 降到 {metrics[k]:.3f}，超过 2 个点"
        for k in METRICS
        if baseline and k in baseline and baseline[k] - metrics[k] > MAX_DROP
    ]
    return ("failed" if reasons else "passed"), reasons


def markdown(
    meta: dict[str, Any],
    n: int,
    index_version: str,
    metrics: dict[str, float],
    baseline: dict[str, float] | None,
    conclusion: str,
    reasons: Sequence[str],
) -> str:
    labels = {
        "recall_at_10": "Recall@10",
        "ndcg_at_10": "nDCG@10",
        "mrr_at_10": "MRR@10",
        "search_p95_ms": "Search 延迟 p95（ms）",
    }
    out = [
        f"# 检索评测报告：{meta['source']} 子集",
        "",
        f"**结论：{conclusion}**" + ("（" + "；".join(reasons) + "）" if reasons else ""),
        "",
        f"- 评测集：{meta['source']}（{meta['license']}），{n} 条查询、{meta['corpus']} 段语料，"
        f"其中相关段落 {meta['relevant_passages']} 段；抽样种子 {meta['seed']}",
        f"- 索引版本：{index_version}；只检索，不经过生成",
        "",
        "| 指标 | 本次 | 基线 |",
        "| --- | --- | --- |",
    ]
    for k, label in labels.items():
        base = f"{baseline[k]:.3f}" if baseline and k in baseline else "—"
        out.append(f"| {label} | {metrics[k]:.3f} | {base} |")
    return "\n".join(out) + "\n"
