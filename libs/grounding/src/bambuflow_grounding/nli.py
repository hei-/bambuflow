"""NLI 模型客户端：Triton 的 KServe v2 HTTP 接口（开发与 CI 用接口相同的模拟服务）。

模型一次推理给出两个输出：support（蕴含概率）与 contradiction（矛盾概率），按名称取用。"""

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
        return await self._call(pairs, "support")

    async def contradiction(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        return await self._call(pairs, "contradiction")

    async def _call(self, pairs: Sequence[tuple[str, str]], output: str) -> tuple[list[float], str]:
        try:
            with _tracer.start_as_current_span(
                "nli.infer", attributes={"nli.pairs": len(pairs), "nli.output": output}
            ):
                return await self._infer(pairs, output)
        except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as e:
            raise GroundingUnavailableError("NLI 不可用") from e

    async def _infer(
        self, pairs: Sequence[tuple[str, str]], output: str
    ) -> tuple[list[float], str]:
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
                    ],
                    "outputs": [{"name": output}],
                },
            )
            resp.raise_for_status()
            body: dict[str, Any] = resp.json()
            # 按名称取输出；模型没有这个输出时 KeyError，按 NLI 不可用处理
            data = {o["name"]: o["data"] for o in body["outputs"]}[output]
            if len(data) != len(batch):
                raise ValueError(f"NLI 返回 {len(data)} 条，请求 {len(batch)} 条")
            scores.extend(float(x) for x in data)
            # 写入 why：回放时要用同一版本的 NLI 模型
            version = f"{body['model_name']}@{body['model_version']}"
        return scores, version
