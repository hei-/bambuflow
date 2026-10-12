"""调用嵌入模型（Triton 的 KServe v2 HTTP 接口；开发与 CI 用接口相同的模拟服务）。"""

from __future__ import annotations

import json
from typing import Any

import httpx

BATCH = 64  # 每批块数（入库管道详设第 8 节，建议值）
# 学习型稀疏向量：词元 ID → 权重
Sparse = dict[int, float]


class Embedder:
    def __init__(self, base_url: str, model: str, client: httpx.Client | None = None) -> None:
        self._url = base_url.rstrip("/")
        self._model = model
        self._client = client or httpx.Client(timeout=30)
        meta: dict[str, Any] = self._get(f"/v2/models/{model}")
        versions = meta.get("versions") or []
        if not versions:
            raise RuntimeError(f"嵌入模型 {model} 没有可用版本")
        # 写入索引版本清单与 why：查询向量必须用同一模型版本生成
        self.model_version = f"{model}@{versions[-1]}"
        # 模型元数据里有名为 sparse 的输出时，同一次调用还给学习型稀疏向量
        self.sparse = any(o.get("name") == "sparse" for o in meta.get("outputs") or [])

    def _get(self, path: str) -> dict[str, Any]:
        resp = self._client.get(self._url + path)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return data

    def embed(self, texts: list[str]) -> list[list[float]]:
        return self.embed_full(texts)[0]

    def embed_full(self, texts: list[str]) -> tuple[list[list[float]], list[Sparse] | None]:
        """稠密向量，以及同一次调用里的学习型稀疏向量
        （输出名 sparse，每段文本一个“词元 ID → 权重”的 JSON）；
        模型不给稀疏向量时第二项为 None（retrieval 的 sparse 召回路随之不可用）。"""
        dense: list[list[float]] = []
        sparse: list[Sparse] = []
        has_sparse = True
        for i in range(0, len(texts), BATCH):
            batch = texts[i : i + BATCH]
            resp = self._client.post(
                f"{self._url}/v2/models/{self._model}/infer",
                json={
                    "inputs": [
                        {"name": "text", "shape": [len(batch)], "datatype": "BYTES", "data": batch}
                    ]
                },
            )
            resp.raise_for_status()
            outputs = {o.get("name", ""): o for o in resp.json()["outputs"]}
            output = outputs.get("embedding") or resp.json()["outputs"][0]
            n, dim = output["shape"]
            if n != len(batch):
                raise RuntimeError(f"嵌入模型返回 {n} 条，请求 {len(batch)} 条")
            flat = output["data"]
            dense.extend(flat[j * dim : (j + 1) * dim] for j in range(n))
            if "sparse" not in outputs:
                has_sparse = False
                continue
            items = outputs["sparse"]["data"]
            if len(items) != n:
                raise RuntimeError(f"稀疏向量 {len(items)} 条，请求 {len(batch)} 条")
            sparse.extend(_sparse(x) for x in items)
        return dense, (sparse if has_sparse else None)


def _sparse(raw: str) -> Sparse:
    weights = json.loads(raw)
    if not isinstance(weights, dict):
        raise RuntimeError("稀疏向量格式不对")
    return {int(k): float(v) for k, v in weights.items() if float(v) > 0}
