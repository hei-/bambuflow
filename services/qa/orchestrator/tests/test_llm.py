"""LLM 客户端的流式读取：按行交出、跨数据块拼行、整次生成的超时、用量与熔断。"""

import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest

from bambuflow_orchestrator import circuit
from bambuflow_orchestrator.circuit import Breaker, State
from bambuflow_orchestrator.llm import ChatClient, CircuitOpenError, LLMUnavailableError, Usage


def sse(
    *pieces: str, model: str = "chat-real@1", usage: dict[str, int] | None = None
) -> list[bytes]:
    out = []
    for p in pieces:
        chunk = {"model": model, "choices": [{"index": 0, "delta": {"content": p}}]}
        out.append(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
    # 带 include_usage 时网关在末尾单独发 usage，没有 choices；不支持的网关给空的或不给
    tail = {"model": model, "choices": [], "usage": usage or {}}
    out.append(f"data: {json.dumps(tail)}\n\n".encode())
    out.append(b"data: [DONE]\n\n")
    return out


class Body(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes], pause_after: int | None = None) -> None:
        self.chunks, self.pause_after = chunks, pause_after

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for i, c in enumerate(self.chunks):
            if i == self.pause_after:
                await asyncio.sleep(5)
            yield c


def collect(transport: httpx.MockTransport, timeout: float = 2.0) -> tuple[list[str], str]:
    async def go() -> tuple[list[str], str]:
        async with httpx.AsyncClient(transport=transport) as http:
            gen = ChatClient("http://llm", "chat@1", http).generate("系统", "问题", timeout)
            lines = [line async for line in gen]
            return lines, gen.model

    return asyncio.run(go())


def test_lines_are_joined_across_chunks() -> None:
    seen: list[dict[str, object]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(json.loads(req.content))
        return httpx.Response(200, stream=Body(sse('{"a":', '1}\n{"b"', ":2}\n", '{"c":3}')))

    lines, model = collect(httpx.MockTransport(handler))
    assert lines == ['{"a":1}', '{"b":2}', '{"c":3}']
    assert model == "chat-real@1"  # why 记网关实际使用的模型
    assert seen[0]["stream"] is True and seen[0]["max_tokens"] == 800
    assert seen[0]["stream_options"] == {"include_usage": True}


def test_gateway_error_is_unavailable() -> None:
    with pytest.raises(LLMUnavailableError):
        collect(httpx.MockTransport(lambda _: httpx.Response(503)))


def test_whole_generation_is_bounded_by_timeout() -> None:
    lines: list[str] = []

    async def go() -> None:
        body = Body(sse("第一行\n", "第二行\n"), pause_after=1)
        transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=body))
        async with httpx.AsyncClient(transport=transport) as http:
            async for line in ChatClient("http://llm", "chat@1", http).generate("s", "u", 0.3):
                lines.append(line)

    with pytest.raises(LLMUnavailableError):
        asyncio.run(go())
    # 超时前读到的行已经交出；是否采用由调用方按校验结果决定
    assert lines == ["第一行"]


def usage_of(transport: httpx.MockTransport, timeout: float = 2.0) -> Usage:
    async def go() -> Usage:
        async with httpx.AsyncClient(transport=transport) as http:
            gen = ChatClient("http://llm", "chat@1", http).generate("系统", "问题", timeout)
            try:
                async for _ in gen:
                    pass
            except LLMUnavailableError:
                pass
            return gen.usage

    return asyncio.run(go())


def test_usage_comes_from_last_chunk() -> None:
    usage = {"prompt_tokens": 120, "completion_tokens": 7}
    body = Body(sse('{"a":1}\n', usage=usage))
    u = usage_of(httpx.MockTransport(lambda _: httpx.Response(200, stream=body)))
    assert u == Usage(120, 7)


def test_usage_is_estimated_when_gateway_omits_it() -> None:
    body = Body(sse("一二三\n", "四五"))
    u = usage_of(httpx.MockTransport(lambda _: httpx.Response(200, stream=body)))
    # 输入按提示词字数（“系统” + “问题”），输出按读到的字数
    assert u == Usage(4, 6, estimated=True)


def test_usage_counts_what_was_read_before_timeout() -> None:
    usage = {"prompt_tokens": 9, "completion_tokens": 9}
    body = Body(sse("第一行\n", "第二行\n", usage=usage), pause_after=1)
    u = usage_of(httpx.MockTransport(lambda _: httpx.Response(200, stream=body)), timeout=0.3)
    assert u == Usage(4, 4, estimated=True), "中途超时时收不到网关的 usage，按已读到的估算"


def test_gateway_error_costs_nothing() -> None:
    assert usage_of(httpx.MockTransport(lambda _: httpx.Response(503))) == Usage()


def test_breaker_opens_after_gateway_errors_and_skips_requests() -> None:
    requests = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(503)

    async def go() -> tuple[ChatClient, Exception | None]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = ChatClient("http://llm", "chat@1", http)
            last: Exception | None = None
            for _ in range(circuit.MIN_CALLS + 1):
                try:
                    async for _ in client.generate("s", "u", 1.0):
                        pass
                except LLMUnavailableError as e:
                    last = e
            return client, last

    client, last = asyncio.run(go())
    assert client.breaker.state is State.OPEN
    assert isinstance(last, CircuitOpenError)
    assert requests == circuit.MIN_CALLS, "熔断打开后不再向网关发请求"


def test_reader_stopping_early_counts_as_success() -> None:
    async def go() -> ChatClient:
        body = Body(sse("第一行\n第二行\n"))
        transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=body))
        async with httpx.AsyncClient(transport=transport) as http:
            client = ChatClient("http://llm", "chat@1", http)
            now = [0.0]
            client.breaker = Breaker("chat@1", lambda: now[0])
            for _ in range(circuit.MIN_CALLS):
                client.breaker.record(False)
            now[0] += circuit.OPEN_SECONDS  # 进入半开：一次失败就会重新打开
            gen = client.generate("s", "u", 1.0).__aiter__()
            await anext(gen)
            await gen.aclose()  # 调用方读够了（如输出无法解析）提前结束
            return client

    client = asyncio.run(go())
    assert client.breaker.state is State.HALF_OPEN


def test_router_picks_model_by_classification() -> None:
    from bambuflow.common.v1 import types_pb2
    from bambuflow_orchestrator.llm import ModelNotAllowedError, RefusedGeneration, Router

    async def main() -> None:
        async with httpx.AsyncClient() as http:
            private = ChatClient("http://llm", "private@1", http)
            restricted = ChatClient("http://llm-r", "restricted@1", http)
            r = Router(private, restricted=restricted)
            assert r.choose(types_pb2.CLASSIFICATION_PUBLIC) is private
            assert r.choose(types_pb2.CLASSIFICATION_CONFIDENTIAL) is private
            assert r.choose(types_pb2.CLASSIFICATION_UNSPECIFIED) is private
            assert r.choose(types_pb2.CLASSIFICATION_RESTRICTED) is restricted
            # 没配受限级专用实例：受限级不调用
            assert Router(private).choose(types_pb2.CLASSIFICATION_RESTRICTED) is None
            # 默认模型是外部模型：只处理公开级
            ext = Router(private, external=True)
            assert ext.choose(types_pb2.CLASSIFICATION_PUBLIC) is private
            assert ext.choose(types_pb2.CLASSIFICATION_INTERNAL) is None
            # routing 指定的模型：部署允许的才调用，受限级仍用专用实例
            alt = ChatClient("http://llm", "alt@1", http)
            r = Router(private, restricted=restricted, alternatives={"alt@1": alt})
            assert r.choose(types_pb2.CLASSIFICATION_INTERNAL, "alt@1") is alt
            assert r.choose(types_pb2.CLASSIFICATION_INTERNAL, "private@1") is private
            assert r.choose(types_pb2.CLASSIFICATION_PUBLIC, "unknown@1") is None
            assert r.choose(types_pb2.CLASSIFICATION_RESTRICTED, "alt@1") is restricted
            ext = Router(private, external=True, alternatives={"alt@1": alt})
            assert ext.choose(types_pb2.CLASSIFICATION_INTERNAL, "alt@1") is None
            with pytest.raises(ModelNotAllowedError):
                async for _ in RefusedGeneration(types_pb2.CLASSIFICATION_INTERNAL):
                    pass

    asyncio.run(main())
