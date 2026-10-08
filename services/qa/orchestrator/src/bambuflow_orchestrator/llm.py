"""LLM 网关客户端：OpenAI 兼容的 /v1/chat/completions（公司网关或开发用的模拟网关，只换地址）。

一律以 stream=true 读取：模型每输出完一行（orchestrator 详设 8.3 的一个 JSON 单元）
就交给调用方切句校验，不必等全文（详设 9.3）。调用方校验期间模型在网关一侧继续生成，
输出暂存在连接缓冲里。

每次调用都经过本模型的熔断器（circuit.py），并记下实际消耗的 token（Usage），
由服务入口把一次回答里全部调用的用量之和交给 gateway 计入配额（gateway 详设 5.1）。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any

import httpx
from opentelemetry import trace

from bambuflow_orchestrator.circuit import Breaker

# 采样参数（orchestrator 详设 8.3）
TEMPERATURE = 0.1
TOP_P = 0.9
MAX_TOKENS = 800

_tracer = trace.get_tracer(__name__)


class LLMUnavailableError(Exception):
    pass


class CircuitOpenError(LLMUnavailableError):
    """熔断打开：没有向网关发请求。调用方与网关不可用一样降级，只是 why 里的原因不同。"""


@dataclass
class Usage:
    """一次调用实际消耗的 token。

    以网关返回的 usage 为准（流式时在最后一个数据块，需要请求里带 stream_options.include_usage）。
    网关没给（不支持该参数，或连接中途断开、超时）时按字数估算：输入取提示词的字数，
    输出取已读到的字数。中文大致一字一 token、英文按字母计会偏多，宁可多计也不漏计配额。
    网关返回错误状态时请求没有被模型处理，记 0。
    """

    prompt_tokens: int = 0
    completion_tokens: int = 0
    estimated: bool = False


class ChatClient:
    def __init__(
        self,
        base_url: str,
        model: str,
        client: httpx.AsyncClient,
        api_key: str = "",
        extra_body: dict[str, Any] | None = None,
    ) -> None:
        self._url = base_url.rstrip("/") + "/v1/chat/completions"
        self._model = model
        self._client = client
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        # 厂商专有的请求参数（如关闭推理模型的思考过程），由部署配置给出，不写进代码（ADR-13）
        self._extra = extra_body or {}
        # 一个客户端对应一个模型，熔断器也就是按模型的（详设 8.5）
        self.breaker = Breaker(model)

    def generate(self, system: str, user: str, timeout: float) -> ChatGeneration:
        body = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": TEMPERATURE,
            "top_p": TOP_P,
            "max_tokens": MAX_TOKENS,
            "stream": True,
            # 流式时网关只在请求了才在最后一个数据块返回 usage（OpenAI 兼容约定）；
            # 不认这个参数的网关可以在部署配置的 extra_body 里覆盖
            "stream_options": {"include_usage": True},
            **self._extra,
        }
        return ChatGeneration(self._client, self._url, self._headers, body, timeout, self.breaker)


class ChatGeneration:
    """一次生成，按行读取输出（async for）。

    整次生成（含等待首字）不超过 timeout 秒；网关不可用、超时或返回格式不对时，
    在读取过程中抛 LLMUnavailableError；熔断打开时不发请求，抛 CircuitOpenError。
    model 是网关实际使用的模型（写入 why），读到数据块后更新；usage 在读取结束后确定。
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        url: str,
        headers: dict[str, str],
        body: dict[str, Any],
        timeout: float,
        breaker: Breaker,
    ) -> None:
        self._client, self._url, self._headers = client, url, headers
        self._body, self._timeout, self._breaker = body, timeout, breaker
        self.model = str(body["model"])
        self.usage = Usage()
        self._reported: dict[str, Any] | None = None
        self._output_chars = 0

    async def __aiter__(self) -> AsyncGenerator[str, None]:
        if not self._breaker.allow():
            raise CircuitOpenError(f"模型 {self.model} 熔断中")
        # 模型调用单独一个 span（属性按 OTel 的 gen_ai 约定），trace 里能看到生成耗时和所用模型。
        # 不设为当前 span：读取期间调用方还在做校验，校验的 span 不应挂在生成下面
        span = _tracer.start_span("llm.chat", attributes={"gen_ai.request.model": self.model})
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout
        buf = ""
        accepted = False  # 网关已接受请求（2xx）：此后中断也已产生用量
        outcome: bool | None = None  # 计入熔断统计的结果；None 表示不计（如客户端断开）
        try:
            async with self._client.stream(
                "POST", self._url, headers=self._headers, timeout=self._timeout, json=self._body
            ) as resp:
                resp.raise_for_status()
                accepted = True
                events = resp.aiter_lines()
                while True:
                    # 每次读取都以整次生成的截止时间为界，而不是单次读取的超时
                    try:
                        raw = await asyncio.wait_for(anext(events), deadline - loop.time())
                    except StopAsyncIteration:
                        break
                    data = _data(raw)
                    if data is None:
                        continue
                    if data == "[DONE]":
                        break
                    buf += self._delta(data)
                    *done, buf = buf.split("\n")
                    for line in done:
                        try:
                            yield line
                        except GeneratorExit:
                            # 调用方读够了提前结束（如输出无法解析）：网关本身是正常的
                            outcome = True
                            raise
            outcome = True
        except (
            httpx.HTTPError,
            TimeoutError,
            AttributeError,
            KeyError,
            TypeError,
            ValueError,
        ) as e:
            outcome = False
            raise LLMUnavailableError(f"{type(e).__name__}: {e}") from e
        finally:
            if outcome is None:
                self._breaker.release()
            else:
                self._breaker.record(outcome)
            self._settle(accepted)
            span.set_attribute("gen_ai.response.model", self.model)
            span.set_attribute("gen_ai.usage.input_tokens", self.usage.prompt_tokens)
            span.set_attribute("gen_ai.usage.output_tokens", self.usage.completion_tokens)
            span.end()
        if buf:
            yield buf

    def _settle(self, accepted: bool) -> None:
        """读取结束（正常、出错或被取消）时确定本次用量。"""
        if not accepted:
            return
        u = self._reported or {}
        prompt, completion = u.get("prompt_tokens"), u.get("completion_tokens")
        if isinstance(prompt, int) and isinstance(completion, int):
            self.usage = Usage(prompt, completion)
            return
        chars = sum(len(str(m.get("content", ""))) for m in self._body["messages"])
        self.usage = Usage(chars, self._output_chars, estimated=True)

    def _delta(self, data: str) -> str:
        chunk: dict[str, Any] = json.loads(data)
        if chunk.get("model"):
            self.model = str(chunk["model"])
        if isinstance(chunk.get("usage"), dict):
            self._reported = chunk["usage"]
        choices = chunk.get("choices") or []
        # 带 include_usage 时网关在末尾单独发一个只有 usage、没有 choices 的数据块
        if not choices:
            return ""
        content = choices[0].get("delta", {}).get("content") or ""
        if not isinstance(content, str):
            raise TypeError("content 不是字符串")
        self._output_chars += len(content)
        return content


def _data(line: str) -> str | None:
    """SSE 的 data 行；空行、注释与其他字段返回 None。"""
    if not line.startswith("data:"):
        return None
    return line.removeprefix("data:").strip()
