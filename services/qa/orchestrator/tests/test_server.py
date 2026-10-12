"""调 retrieval 时的调用目的：沿用入口的 answer、mcp、batch，agent 轮次固定为 agent_round。

在进程内起一个假的 retrieval，记录每次调用收到的 x-call-purpose 与请求。
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import grpc
import pytest

from bambuflow.common.v1 import types_pb2
from bambuflow.identity.v1 import principal_pb2
from bambuflow.retrieval.v1 import retrieval_pb2, retrieval_pb2_grpc
from bambuflow_orchestrator import server
from bambuflow_orchestrator.budget import Deadline
from bambuflow_platform import grpc_server


class Recorder(retrieval_pb2_grpc.RetrievalServiceServicer):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []

    def _record(self, method: str, request: Any, context: Any) -> None:
        md = dict(context.invocation_metadata())
        self.calls.append((method, md.get("x-call-purpose", ""), request))

    async def Search(self, request: Any, context: Any) -> retrieval_pb2.SearchResponse:
        self._record("Search", request, context)
        return retrieval_pb2.SearchResponse()

    async def CheckAccess(self, request: Any, context: Any) -> retrieval_pb2.CheckAccessResponse:
        self._record("CheckAccess", request, context)
        # revoked 开头的块无权；gone 开头的不在结果里（同样按无权处理）
        return retrieval_pb2.CheckAccessResponse(
            results=[
                retrieval_pb2.AccessResult(chunk_id=c, allowed=not c.startswith("revoked"))
                for c in request.chunk_ids
                if not c.startswith("gone")
            ]
        )

    async def CountDocuments(
        self, request: Any, context: Any
    ) -> retrieval_pb2.CountDocumentsResponse:
        self._record("CountDocuments", request, context)
        return retrieval_pb2.CountDocumentsResponse()

    async def ListDocuments(
        self, request: Any, context: Any
    ) -> retrieval_pb2.ListDocumentsResponse:
        self._record("ListDocuments", request, context)
        return retrieval_pb2.ListDocumentsResponse()

    async def GetExcerpt(self, request: Any, context: Any) -> retrieval_pb2.GetExcerptResponse:
        self._record("GetExcerpt", request, context)
        if request.chunk_id == "missing":
            await context.abort(grpc.StatusCode.NOT_FOUND, "不存在")
        return retrieval_pb2.GetExcerptResponse(chunk_id=request.chunk_id)

    # orchestrator 不调用这个接口

    async def CheckDocumentAccess(
        self, request: Any, context: Any
    ) -> retrieval_pb2.CheckDocumentAccessResponse:
        raise NotImplementedError


class Inbound:
    """入口请求的上下文：只用到元数据与剩余时间。"""

    def __init__(self, purpose: str) -> None:
        self._md = (("x-call-purpose", purpose), ("x-answer-id", "ans_1"))

    def invocation_metadata(self) -> tuple[tuple[str, str], ...]:
        return self._md

    def time_remaining(self) -> float:
        return 5.0


@asynccontextmanager
async def retrieval() -> AsyncIterator[tuple[Recorder, Any]]:
    rec = Recorder()
    srv = grpc.aio.server()
    retrieval_pb2_grpc.add_RetrievalServiceServicer_to_server(rec, srv)
    port = srv.add_insecure_port("127.0.0.1:0")
    await srv.start()
    # downstream 转发当前请求的 Principal；单测里直接放入，等同于经过了鉴权拦截器
    token = grpc_server._caller.set(
        grpc_server._Caller(principal_pb2.Principal(sub="alice", tenant="credit"), "raw")
    )
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
            yield rec, retrieval_pb2_grpc.RetrievalServiceStub(ch)
    finally:
        grpc_server._caller.reset(token)
        await srv.stop(None)


def test_access_records_denied_chunks() -> None:
    """返回前复核拦下的块记进 denied（回答记录的越权拦截数）；sql_query 查出的证据不经复核。"""

    async def run() -> set[str]:
        async with retrieval() as (_, stub):
            denied: set[str] = set()
            ctx: Any = Inbound("answer")
            access = server._Access(stub, ctx, Deadline(5), denied, {"sql:1"})
            got = await access.allowed(["ok#1", "revoked#1", "gone#1", "sql:1"])
            assert got == {"ok#1": True, "revoked#1": False, "sql:1": True}
            return denied

    assert asyncio.run(run()) == {"revoked#1", "gone#1"}


@pytest.mark.parametrize("purpose", ["answer", "mcp", "batch"])
def test_purpose_follows_entry(purpose: str) -> None:
    async def run() -> list[tuple[str, str, Any]]:
        async with retrieval() as (rec, stub):
            ctx: Any = Inbound(purpose)
            deadline = Deadline(5)
            await server._Search(stub, ctx, deadline).search("额度", None)
            denied: set[str] = set()
            await server._Access(stub, ctx, deadline, denied).allowed(["wiki:loan#1"])
            assert not denied
            catalog = server._Catalog(stub, ctx, deadline)
            await catalog.count(types_pb2.Filters(), retrieval_pb2.COUNT_GROUP_BY_UNSPECIFIED)
            await catalog.list(types_pb2.Filters(), 20)
            excerpts = server._Excerpts(stub, ctx, deadline)
            assert (await excerpts.excerpt("d", "s:d#sum")) is not None
            # 无权与不存在都是 NOT_FOUND：主题汇总把它当作没有摘要
            assert (await excerpts.excerpt("d", "missing")) is None
            await server._AgentSearch(stub, ctx).search("额度", None, 2, 1.0)
            return rec.calls

    calls = asyncio.run(run())
    assert [(m, p) for m, p, _ in calls] == [
        ("Search", purpose),
        ("CheckAccess", purpose),
        ("CountDocuments", purpose),
        ("ListDocuments", purpose),
        ("GetExcerpt", purpose),
        ("GetExcerpt", purpose),
        ("Search", "agent_round"),
    ]
    # 回答生成总是精排，不随调用目的的默认值变化（mcp 默认不精排）
    first = calls[0][2]
    assert first.HasField("rerank") and first.rerank
