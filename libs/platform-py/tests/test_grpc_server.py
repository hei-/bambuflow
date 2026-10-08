"""在进程内起 grpc.aio 服务，验证鉴权拦截器、Principal 转发和超时只缩不长。"""

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import grpc
import pytest

from bambuflow.answer.v1 import answer_pb2, answer_pb2_grpc
from bambuflow_platform.grpc_server import AuthInterceptor, current_principal, downstream
from bambuflow_platform.principal import JWKS, Verifier

from .conftest import FakeGateway


class Unused(answer_pb2_grpc.AnswerServiceServicer):
    """这里只用到 Answer 与 StreamAnswer；回放与反馈接口不涉及拦截器行为。"""

    async def GetAnswer(self, request: Any, context: Any) -> answer_pb2.GetAnswerResponse:
        raise NotImplementedError

    async def CreateFeedback(self, request: Any, context: Any) -> answer_pb2.CreateFeedbackResponse:
        raise NotImplementedError


class Leaf(Unused):
    """把看到的调用者和剩余时间写进响应，便于断言。"""

    async def Answer(self, request: Any, context: Any) -> answer_pb2.AnswerResponse:
        p = current_principal()
        assert p is not None
        return answer_pb2.AnswerResponse(
            answer=f"{p.sub}@{p.tenant}", trace_id=f"{context.time_remaining():.1f}"
        )

    async def StreamAnswer(
        self, request: Any, context: Any
    ) -> AsyncIterator[answer_pb2.StreamAnswerResponse]:
        # 每个事件都重新读一次调用者：流处理期间 Principal 一直可用
        for _ in range(2):
            p = current_principal()
            assert p is not None
            yield answer_pb2.StreamAnswerResponse(
                meta=answer_pb2.AnswerMeta(answer_id=f"{p.sub}@{p.tenant}")
            )


class Mid(Unused):
    """中间一跳：转发 Principal，并把超时压到 1 秒以内。"""

    def __init__(self, target: str) -> None:
        self._target = target

    async def Answer(self, request: Any, context: Any) -> answer_pb2.AnswerResponse:
        async with grpc.aio.insecure_channel(self._target) as ch:
            resp: answer_pb2.AnswerResponse = await answer_pb2_grpc.AnswerServiceStub(ch).Answer(
                request, **downstream(context, budget=1.0)
            )
            return resp

    async def StreamAnswer(
        self, request: Any, context: Any
    ) -> AsyncIterator[answer_pb2.StreamAnswerResponse]:
        async with grpc.aio.insecure_channel(self._target) as ch:
            call = answer_pb2_grpc.AnswerServiceStub(ch).StreamAnswer(
                request, **downstream(context, budget=1.0)
            )
            async for event in call:
                yield event


async def _serve(servicer: Any, verifier: Verifier) -> tuple[grpc.aio.Server, str]:
    server = grpc.aio.server(interceptors=[AuthInterceptor(verifier)])
    answer_pb2_grpc.add_AnswerServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


async def _call(target: str, token: str | None, timeout: float | None) -> answer_pb2.AnswerResponse:
    md = (("authorization", "Bearer " + token),) if token else None
    async with grpc.aio.insecure_channel(target) as ch:
        resp: answer_pb2.AnswerResponse = await answer_pb2_grpc.AnswerServiceStub(ch).Answer(
            answer_pb2.AnswerRequest(question="q"), metadata=md, timeout=timeout
        )
        return resp


def test_auth_interceptor(gateway: FakeGateway) -> None:
    verifier = Verifier(JWKS("http://gateway/jwks", fetch=gateway.fetch))
    other = FakeGateway("k1")

    async def scenario() -> list[Any]:
        server, target = await _serve(Leaf(), verifier)
        try:
            results: list[Any] = []
            for token, timeout in [
                (gateway.sign("k1"), None),  # 没有 deadline
                (None, 5.0),  # 没有 Principal
                (other.sign("k1"), 5.0),  # 签名不对
            ]:
                try:
                    await _call(target, token, timeout)
                    results.append(grpc.StatusCode.OK)
                except grpc.aio.AioRpcError as e:
                    results.append((e.code(), e.details()))
            results.append(await _call(target, gateway.sign("k1"), 5.0))
            return results
        finally:
            await server.stop(None)

    no_deadline, no_principal, forged, ok = asyncio.run(scenario())
    assert no_deadline[0] == grpc.StatusCode.INVALID_ARGUMENT
    assert no_principal[0] == grpc.StatusCode.UNAUTHENTICATED
    assert forged == (grpc.StatusCode.UNAUTHENTICATED, "Principal 无效")
    assert ok.answer == "alice@credit"


@pytest.mark.parametrize(("caller_timeout", "expect_max"), [(5.0, 1.0), (0.4, 0.4)])
def test_forward_principal_and_deadline_only_shrinks(
    gateway: FakeGateway, caller_timeout: float, expect_max: float
) -> None:
    verifier = Verifier(JWKS("http://gateway/jwks", fetch=gateway.fetch))

    async def scenario() -> tuple[answer_pb2.AnswerResponse, list[str]]:
        leaf, leaf_target = await _serve(Leaf(), verifier)
        mid, mid_target = await _serve(Mid(leaf_target), verifier)
        try:
            token = gateway.sign("k1")
            return (
                await _call(mid_target, token, caller_timeout),
                await _stream(mid_target, token, caller_timeout),
            )
        finally:
            await mid.stop(None)
            await leaf.stop(None)

    resp, streamed = asyncio.run(scenario())
    assert resp.answer == "alice@credit"  # 下游拿到同一个 Principal
    assert float(resp.trace_id) <= expect_max  # 下游剩余时间不超过 min(上游剩余, 1 秒)
    assert streamed == ["alice@credit", "alice@credit"]  # 流式调用同样转发 Principal


async def _stream(target: str, token: str | None, timeout: float | None) -> list[str]:
    md = (("authorization", "Bearer " + token),) if token else None
    async with grpc.aio.insecure_channel(target) as ch:
        call = answer_pb2_grpc.AnswerServiceStub(ch).StreamAnswer(
            answer_pb2.StreamAnswerRequest(request=answer_pb2.AnswerRequest(question="q")),
            metadata=md,
            timeout=timeout,
        )
        return [e.meta.answer_id async for e in call]


def test_auth_interceptor_server_streaming(gateway: FakeGateway) -> None:
    """服务端流与一元调用的规则相同：缺 deadline、缺 Principal、签名不对都拒绝，

    且不产生任何事件。
    """
    verifier = Verifier(JWKS("http://gateway/jwks", fetch=gateway.fetch))
    other = FakeGateway("k1")

    async def scenario() -> list[Any]:
        server, target = await _serve(Leaf(), verifier)
        try:
            results: list[Any] = []
            for token, timeout in [
                (gateway.sign("k1"), None),
                (None, 5.0),
                (other.sign("k1"), 5.0),
            ]:
                try:
                    results.append(await _stream(target, token, timeout))
                except grpc.aio.AioRpcError as e:
                    results.append((e.code(), e.details()))
            results.append(await _stream(target, gateway.sign("k1"), 5.0))
            return results
        finally:
            await server.stop(None)

    no_deadline, no_principal, forged, ok = asyncio.run(scenario())
    assert no_deadline[0] == grpc.StatusCode.INVALID_ARGUMENT
    assert no_principal[0] == grpc.StatusCode.UNAUTHENTICATED
    assert forged == (grpc.StatusCode.UNAUTHENTICATED, "Principal 无效")
    assert ok == ["alice@credit", "alice@credit"]


def test_internal_services_need_deadline_but_no_principal(gateway: FakeGateway) -> None:
    """internal 里的服务（服务间接口）不要求 Principal，但仍要求 deadline；其他服务照旧鉴权。"""
    verifier = Verifier(JWKS("http://gateway/jwks", fetch=gateway.fetch))

    class Open(Unused):
        async def Answer(self, request: Any, context: Any) -> answer_pb2.AnswerResponse:
            return answer_pb2.AnswerResponse(answer="open")

        async def StreamAnswer(
            self, request: Any, context: Any
        ) -> AsyncIterator[answer_pb2.StreamAnswerResponse]:
            yield answer_pb2.StreamAnswerResponse()

    async def scenario() -> list[Any]:
        server = grpc.aio.server(
            interceptors=[
                AuthInterceptor(verifier, internal=frozenset({"bambuflow.answer.v1.AnswerService"}))
            ]
        )
        answer_pb2_grpc.add_AnswerServiceServicer_to_server(Open(), server)
        target = f"127.0.0.1:{server.add_insecure_port('127.0.0.1:0')}"
        await server.start()
        try:
            out: list[Any] = [(await _call(target, None, 5.0)).answer]
            try:
                await _call(target, None, None)
            except grpc.aio.AioRpcError as e:
                out.append(e.code())
            try:
                async with grpc.aio.insecure_channel(target) as ch:
                    call = answer_pb2_grpc.AnswerServiceStub(ch).StreamAnswer(
                        answer_pb2.StreamAnswerRequest(), timeout=5.0
                    )
                    async for _ in call:
                        pass
            except grpc.aio.AioRpcError as e:
                out.append(e.code())
            return out
        finally:
            await server.stop(None)

    ok, no_deadline, stream = asyncio.run(scenario())
    assert ok == "open"
    assert no_deadline == grpc.StatusCode.INVALID_ARGUMENT
    # 服务间接口只支持一元调用
    assert stream == grpc.StatusCode.UNIMPLEMENTED
