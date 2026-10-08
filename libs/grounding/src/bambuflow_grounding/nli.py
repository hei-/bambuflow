"""NLI 模型客户端：Triton 的 KServe v2 HTTP 接口（开发与 CI 用接口相同的模拟服务）。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import httpx
from opentelemetry import trace

from bambuflow_grounding.verify import GroundingUnavailableError

BATCH = 64

_tracer = trace.get_tracer(__name__)


class KServeNLI:
    def __init__(self, base_url: str, client: httpx.AsyncClient, model: str = "nli") -> None:
        self._url = f"{base_url.rstrip('/')}/v2/models/{model}/infer"
        self._client = client

    async def support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        try:
            with _tracer.start_as_current_span("nli.infer", attributes={"nli.pairs": len(pairs)}):
                return await self._support(pairs)
        except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as e:
            raise GroundingUnavailableError("NLI 不可用") from e

    async def _support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        scores: list[float] = []
        version = ""
        for start in range(0, len(pairs), BATCH):
            batch = pairs[start : start + BATCH]
            resp = await self._client.post(
                self._url,
                json={
                    "inputs": [
                        {
                            "name": "premise",
                            "shape": [len(batch)],
                            "datatype": "BYTES",
                            "data": [p for p, _ in batch],
                        },
                        {
                            "name": "hypothesis",
                            "shape": [len(batch)],
                            "datatype": "BYTES",
                            "data": [h for _, h in batch],
                        },
                    ]
                },
            )
            resp.raise_for_status()
            body: dict[str, Any] = resp.json()
            data = body["outputs"][0]["data"]
            if len(data) != len(batch):
                raise ValueError(f"NLI 返回 {len(data)} 条，请求 {len(batch)} 条")
            scores.extend(float(x) for x in data)
            # 写入 why：回放时要用同一版本的 NLI 模型
            version = f"{body['model_name']}@{body['model_version']}"
        return scores, version
