import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import grpc
import pytest
from bambuflow_processing.admin import IngestAdmin
from bambuflow_processing.state import Failure, IngestState
from bambuflow_processing.worker import STREAM
from test_worker import FakeRedis

from bambuflow.admin.v1 import ingest_admin_pb2, ingest_admin_pb2_grpc
from bambuflow.identity.v1 import principal_pb2
from bambuflow.ingestion.v1 import events_pb2
from bambuflow_platform.grpc_server import AuthInterceptor
from bambuflow_platform.principal import JWKS, Verifier


class FakeVerifier(Verifier):
    def __init__(self) -> None:
        super().__init__(JWKS("http://unused"), operator=True)

    def verify(self, raw: str) -> principal_pb2.Principal:
        return principal_pb2.Principal(sub=raw, roles=[raw])


def evt(doc: str, source: str = "src_wiki") -> events_pb2.DocumentChanged:
    return events_pb2.DocumentChanged(event_id="evt_" + doc, source_id=source, doc_id=doc)


if TYPE_CHECKING:
    from bambuflow.admin.v1.ingest_admin_pb2_grpc import IngestAdminServiceAsyncStub as Stub


def run(state: IngestState, body: "Callable[[Stub], Awaitable[None]]") -> None:
    async def main() -> None:
        server = grpc.aio.server(interceptors=[AuthInterceptor(FakeVerifier())])
        ingest_admin_pb2_grpc.add_IngestAdminServiceServicer_to_server(IngestAdmin(state), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
                await body(ingest_admin_pb2_grpc.IngestAdminServiceStub(ch))
        finally:
            await server.stop(None)

    asyncio.run(main())


def as_(role: str) -> dict[str, Any]:
    return {"metadata": (("authorization", "Bearer " + role),), "timeout": 5}


OPS = as_("platform_operator")


def seeded() -> tuple[FakeRedis, IngestState]:
    rdb = FakeRedis()
    state = IngestState(rdb, "p-test")
    state.failed(evt("a"), Failure("parse", "bad_document", "表格损坏", False))
    state.failed(evt("b"), Failure("fetch", "content_mismatch", "摘要不一致", False))
    state.failed(evt("c", "src_mail"), Failure("parse", "bad_document", "附件损坏", False))
    state.failed(evt("d"), Failure("write", "rpc_unavailable", "indexing 不可用", True))
    return rdb, state


def test_status_and_dead_letters() -> None:
    _, state = seeded()

    async def body(stub: "Stub") -> None:
        s = await stub.GetIngestStatus(ingest_admin_pb2.GetIngestStatusRequest(doc_id="a"), **OPS)
        assert (s.stage, s.state, s.last_error.code, s.attempts) == (
            "parse",
            "dead_letter",
            "bad_document",
            1,
        )
        d = await stub.GetIngestStatus(ingest_admin_pb2.GetIngestStatusRequest(doc_id="d"), **OPS)
        assert d.state == "retrying" and d.last_error.code == "rpc_unavailable"
        with pytest.raises(grpc.aio.AioRpcError) as e:
            await stub.GetIngestStatus(ingest_admin_pb2.GetIngestStatusRequest(doc_id="zz"), **OPS)
        assert e.value.code() == grpc.StatusCode.NOT_FOUND

        page = await stub.ListDeadLetters(
            ingest_admin_pb2.ListDeadLettersRequest(page_size=2), **OPS
        )
        assert [x.doc_id for x in page.dead_letters] == ["a", "b"] and page.next_page_token
        rest = await stub.ListDeadLetters(
            ingest_admin_pb2.ListDeadLettersRequest(page_size=2, page_token=page.next_page_token),
            **OPS,
        )
        assert [x.doc_id for x in rest.dead_letters] == ["c"] and not rest.next_page_token
        parse = await stub.ListDeadLetters(
            ingest_admin_pb2.ListDeadLettersRequest(source_id="src_wiki", stage="parse"), **OPS
        )
        (only,) = parse.dead_letters
        assert (only.doc_id, only.error.message, only.pipeline_version) == (
            "a",
            "表格损坏",
            "p-test",
        )

        with pytest.raises(grpc.aio.AioRpcError) as e:
            await stub.ListDeadLetters(ingest_admin_pb2.ListDeadLettersRequest(), **as_("auditor"))
        assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED

    run(state, body)


def test_replay_by_condition_skips_quarantined_unless_named() -> None:
    rdb, state = seeded()
    # a 已重放 3 次仍失败：隔离
    rdb.hset("ingest:replays", "a", 3)
    state.failed(evt("a"), Failure("parse", "bad_document", "表格损坏", False))

    async def body(stub: "Stub") -> None:
        for bad in (
            ingest_admin_pb2.ReplayRequest(reason="修好了"),  # 没有任何条件
            ingest_admin_pb2.ReplayRequest(error_code="bad_document"),  # 没写原因
            ingest_admin_pb2.ReplayRequest(stage="ocr?", reason="x"),
        ):
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await stub.Replay(bad, **OPS)
            assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT

        r = await stub.Replay(
            ingest_admin_pb2.ReplayRequest(error_code="bad_document", reason="解析器已修复"), **OPS
        )
        assert (r.replayed, r.skipped_quarantined) == (1, 1) and r.ingest_batch_id.startswith(
            "ing_"
        )
        assert {d["doc_id"] for d in state.dead_letters()} == {"a", "b"}
        s = await stub.GetIngestStatus(ingest_admin_pb2.GetIngestStatusRequest(doc_id="c"), **OPS)
        assert (s.state, s.ingest_batch_id) == ("queued", r.ingest_batch_id)

        named = await stub.Replay(
            ingest_admin_pb2.ReplayRequest(doc_ids=["a"], reason="人工确认"), **OPS
        )
        assert (named.replayed, named.skipped_quarantined) == (1, 0)
        assert len(rdb.streams[STREAM]) == 2

        none = await stub.Replay(
            ingest_admin_pb2.ReplayRequest(source_id="src_none", reason="x"), **OPS
        )
        assert (none.replayed, none.ingest_batch_id) == (0, "")

    run(state, body)
