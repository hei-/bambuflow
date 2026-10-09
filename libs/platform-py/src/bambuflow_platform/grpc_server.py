"""服务间 gRPC 的公共拦截器（grpc.aio），规则与 libs/platform-go/grpcmw 一致：

身份只来自 metadata 中的 Principal；请求必须带 deadline；调下游时原样转发 Principal，超时只能缩短。
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import grpc

from bambuflow.identity.v1 import principal_pb2
from bambuflow_platform.principal import InvalidPrincipalError, Verifier

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Caller:
    principal: principal_pb2.Principal
    raw: str


_caller: contextvars.ContextVar[_Caller | None] = contextvars.ContextVar("caller", default=None)


def current_principal() -> principal_pb2.Principal | None:
    """当前请求经过校验的调用者身份；不在请求处理中时返回 None。"""
    c = _caller.get()
    return c.principal if c else None


# 评测指定索引版本的 metadata（evaluation 详设 4.2，与 retrieval 的 search.EvalIndexHeader 相同）：
# 只有评测模式的实例接受（AuthInterceptor 的 eval_mode），并随调用链原样转给下游
EVAL_INDEX_HEADER = "x-eval-index-version"


def _metadata(context: Any, key: str) -> str:
    values = [v for k, v in context.invocation_metadata() or () if k == key]
    return values[0] if len(values) == 1 and isinstance(values[0], str) else ""


def downstream(
    context: grpc.aio.ServicerContext[Any, Any], budget: float | None = None
) -> dict[str, Any]:
    """调下游 gRPC 时的参数：转发当前 Principal（评测请求另转发指定的索引版本），
    超时取“剩余时间”和 budget 中较小的一个。

    用法：await stub.Search(request, **downstream(context, budget=1.5))
    """
    c = _caller.get()
    if c is None:
        raise RuntimeError("当前请求没有经过校验的 Principal")
    remaining = context.time_remaining()
    timeout = remaining if budget is None or remaining is None else min(remaining, budget)
    md: tuple[tuple[str, str], ...] = (("authorization", "Bearer " + c.raw),)
    if pin := _metadata(context, EVAL_INDEX_HEADER):
        md += ((EVAL_INDEX_HEADER, pin),)
    return {"metadata": md, "timeout": timeout}


def _bearer(context: grpc.aio.ServicerContext[Any, Any]) -> str:
    values = [v for k, v in context.invocation_metadata() or () if k == "authorization"]
    if len(values) != 1 or not isinstance(values[0], str) or not values[0].startswith("Bearer "):
        return ""
    return values[0].removeprefix("Bearer ")


class AuthInterceptor(grpc.aio.ServerInterceptor):
    """要求 deadline 并校验 Principal。失败一律返回不带细节的错误，原因只写日志。

    支持一元与服务端流两种调用，规则相同；客户端流与双向流尚无接口使用，显式拒绝，避免绕过鉴权。

    internal 列出的服务（全名，如 bambuflow.answer.v1.AnswerRecordService）是服务间接口：
    只要求 deadline、不要求 Principal，调用方身份由服务网格的 mTLS 与网络策略保证
    （技术设计文档 12.6）；只支持一元调用。
    """

    def __init__(
        self, verifier: Verifier, internal: frozenset[str] = frozenset(), eval_mode: bool = False
    ) -> None:
        self._verifier = verifier
        self._internal = internal
        # 非评测模式的实例收到指定索引版本的请求一律拒绝
        # （evaluation 详设 4.2：否则就是绕过发布门禁的后门）
        self._eval_mode = eval_mode

    async def _authenticate(self, method: str, context: Any) -> _Caller:
        """校验 deadline 与 Principal，不通过时 abort（抛出异常，不会返回）。"""
        if context.time_remaining() is None:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "请求必须带 deadline")
        if not self._eval_mode and _metadata(context, EVAL_INDEX_HEADER):
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED, "只有评测模式的实例接受指定索引版本"
            )
        raw = _bearer(context)
        if not raw:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "缺少 Principal")
        try:
            # 可能需要拉 JWKS，放到线程里，不阻塞事件循环
            principal = await asyncio.to_thread(self._verifier.verify, raw)
        except InvalidPrincipalError as e:
            _log.warning("Principal 校验失败", extra={"method": method, "reason": str(e)})
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Principal 无效")
        return _Caller(principal, raw)

    async def intercept_service(
        self,
        continuation: Callable[
            [grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler[Any, Any] | None]
        ],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler[Any, Any] | None:
        handler = await continuation(handler_call_details)
        if handler is None:
            return handler
        method = handler_call_details.method
        if method.split("/")[1] in self._internal:
            return self._internal_handler(handler)
        # context 标成 Any：grpc 的类型桩把处理函数的上下文写成同步的 ServicerContext
        if handler.unary_unary is not None:
            unary = handler.unary_unary

            async def unary_behavior(request: Any, context: Any) -> Any:
                token = _caller.set(await self._authenticate(method, context))
                try:
                    return await unary(request, context)
                finally:
                    _caller.reset(token)

            return grpc.unary_unary_rpc_method_handler(
                unary_behavior,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if handler.unary_stream is not None:
            streaming = handler.unary_stream

            async def stream_behavior(request: Any, context: Any) -> AsyncIterator[Any]:
                token = _caller.set(await self._authenticate(method, context))
                try:
                    result = streaming(request, context)
                    # 本平台的流式处理函数一律写成异步生成器；其他写法拦截器无法在整个流期间
                    # 保持 Principal，直接拒绝
                    if inspect.isasyncgen(result):
                        async for response in result:
                            yield response
                    else:
                        await context.abort(
                            grpc.StatusCode.UNIMPLEMENTED, "流式处理函数须为异步生成器"
                        )
                finally:
                    _caller.reset(token)

            return grpc.unary_stream_rpc_method_handler(
                stream_behavior,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        async def unsupported(request: Any, context: Any) -> Any:
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, "客户端流与双向流尚未接入鉴权")

        return grpc.unary_unary_rpc_method_handler(unsupported)

    def _internal_handler(
        self, handler: grpc.RpcMethodHandler[Any, Any]
    ) -> grpc.RpcMethodHandler[Any, Any]:
        unary = handler.unary_unary
        if unary is None:

            async def streaming(request: Any, context: Any) -> Any:
                await context.abort(grpc.StatusCode.UNIMPLEMENTED, "服务间接口只支持一元调用")

            return grpc.unary_unary_rpc_method_handler(streaming)

        async def behavior(request: Any, context: Any) -> Any:
            if context.time_remaining() is None:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "请求必须带 deadline")
            return await unary(request, context)

        return grpc.unary_unary_rpc_method_handler(
            behavior,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
