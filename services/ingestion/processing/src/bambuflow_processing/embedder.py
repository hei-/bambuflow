"""调用嵌入模型（Triton 的 KServe v2 HTTP 接口；开发与 CI 用接口相同的模拟服务）。"""

from __future__ import annotations

from typing import Any

import httpx

BATCH = 64  # 每批块数（入库管道详设第 8 节，建议值）


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

    def _get(self, path: str) -> dict[str, Any]:
        resp = self._client.get(self._url + path)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return data

    def embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
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
            output = resp.json()["outputs"][0]
            n, dim = output["shape"]
            if n != len(batch):
                raise RuntimeError(f"嵌入模型返回 {n} 条，请求 {len(batch)} 条")
            flat = output["data"]
            out.extend(flat[j * dim : (j + 1) * dim] for j in range(n))
        return out
