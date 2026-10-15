import asyncio
import os
from collections.abc import Sequence
from typing import Any, cast

import grpc
import pytest

from bambuflow.admin.v1 import llm_trace_admin_pb2
from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.identity.v1 import principal_pb2
from bambuflow.ingestion.v1 import events_pb2
from bambuflow_grounding import Claim, Verdict
from bambuflow_orchestrator import llmtrace
from bambuflow_orchestrator.llm import RefusedGeneration, Usage
from bambuflow_platform import envelope

TRACE_ID = "0123456789abcdef0123456789abcdef"


class FakeKeys:
    """密钥服务的内存实现：包裹后的数据密钥就是它的十六进制。"""

    key_id = "bambuflow-answers"

    async def data_key(self) -> envelope.DataKey:
        dek = os.urandom(32)
        return envelope.DataKey(dek, dek.hex(), self.key_id)

    async def unwrap(self, wrapped: str) -> bytes:
        return bytes.fromhex(wrapped)


class FakeGeneration:
    def __init__(self, system: str, user: str, output: str) -> None:
        self.model = "qwen"
        self.output = output
        self.usage = Usage(120, 30)
        self._prompts = (system, user)

    def prompts(self) -> tuple[str, str]:
        return self._prompts


def _verdict(text: str, passed: bool, reason: str) -> Verdict:
    claim = Claim(text, (1,), ("引文",), "fact", "b1", "paragraph")
    return Verdict(claim, passed, (1,) if passed else (), {}, 0.9 if passed else 0.2, reason)


def _collect() -> llmtrace.Collector:
    """在独立的上下文里记录：模型调用与校验结果经上下文变量记到绑定的 Collector。"""
    c = llmtrace.Collector()

    async def request() -> None:
        llmtrace.bind(c)

        async def generate() -> None:
            llmtrace.note_call(
                "generate",
                types_pb2.CLASSIFICATION_CONFIDENTIAL,
                FakeGeneration("系统", "问题与证据", "回答 [S1]"),
            )

        # 请求里创建的任务也记到同一个 Collector
        await asyncio.create_task(generate())
        llmtrace.note_verdicts(
            [_verdict("通过的句子", True, "passed"), _verdict("丢弃的句子", False, "low_support")]
        )

    asyncio.run(request())
    return c


def test_collect_and_seal_round_trip() -> None:
    c = _collect()
    assert not c.restricted()
    t = llmtrace.build(TRACE_ID, "ans_1", "credit", c)
    sealed = asyncio.run(llmtrace.seal(t, ["src_wiki:doc-1"], FakeKeys()))
    assert "问题与证据".encode() not in sealed.blob
    got = asyncio.run(llmtrace.restore(sealed, FakeKeys()))
    call = got.calls[0]
    assert (call.use, call.model, call.system_prompt, call.user_prompt, call.output) == (
        "generate",
        "qwen",
        "系统",
        "问题与证据",
        "回答 [S1]",
    )
    assert (call.prompt_tokens, call.completion_tokens) == (120, 30)
    assert [(v.text, v.passed, v.reason) for v in got.verdicts] == [
        ("通过的句子[S1]", True, "passed"),
        ("丢弃的句子", False, "low_support"),
    ]
    # 密文绑定 trace_id：换一个 trace_id 解不开
    other = llmtrace.Sealed(
        "f" * 32, "ans_1", "credit", [], sealed.blob, sealed.wrapped_key, sealed.key_id
    )
    with pytest.raises(envelope.DecryptError):
        asyncio.run(llmtrace.restore(other, FakeKeys()))


def test_restricted_input_marks_collector() -> None:
    c = llmtrace.Collector()
    c.calls.append(
        llmtrace._Call(
            "generate",
            types_pb2.CLASSIFICATION_RESTRICTED,
            RefusedGeneration(types_pb2.CLASSIFICATION_RESTRICTED),
        )
    )
    # 受限级内容即使没有调用模型（没配受限级实例）也不进追踪库
    assert c.restricted()


def test_docs_of_covers_context_shown_listed_and_cited() -> None:
    resp = answer_pb2.AnswerResponse(
        audit=answer_pb2.AuditBlocks(
            context_chunk_ids=["src_wiki:doc-1#a", "src_wiki:doc-2#b"],
            shown_chunk_ids=["src_wiki:doc-1#a"],
            listed_doc_ids=["doc-9"],
        ),
        citations=[answer_pb2.Citation(doc_id="doc-1")],
    )
    assert llmtrace.docs_of(resp) == ["doc-1", "doc-9", "src_wiki:doc-1", "src_wiki:doc-2"]


class MemTraces:
    def __init__(self) -> None:
        self.rows: dict[str, llmtrace.Sealed] = {}
        self.deleted: list[Sequence[str]] = []
        self.fail = False

    async def put(self, s: llmtrace.Sealed) -> None:
        self.rows[s.trace_id] = s

    async def get(self, trace_id: str) -> llmtrace.Sealed | None:
        if self.fail:
            raise llmtrace.TracesUnavailableError("down")
        return self.rows.get(trace_id)

    async def delete_doc(self, keys: Sequence[str]) -> int:
        if self.fail:
            raise llmtrace.TracesUnavailableError("down")
        self.deleted.append(keys)
        gone = [k for k, s in self.rows.items() if set(s.docs) & set(keys)]
        for k in gone:
            del self.rows[k]
        return len(gone)


class Aborted(Exception):
    pass


class FakeContext:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None

    async def abort(self, code: grpc.StatusCode, details: str) -> None:
        self.code = code
        raise Aborted(details)


def _get(
    monkeypatch: pytest.MonkeyPatch, traces: MemTraces, roles: list[str], trace_id: str
) -> tuple[grpc.StatusCode | None, Any]:
    p = principal_pb2.Principal(sub="dora", roles=roles)
    monkeypatch.setattr(llmtrace, "current_principal", lambda: p)
    svc = llmtrace.AdminService(traces, FakeKeys())
    ctx = FakeContext()
    try:
        resp = asyncio.run(
            svc.GetLlmTrace(llm_trace_admin_pb2.GetLlmTraceRequest(trace_id=trace_id), ctx)
        )
    except Aborted:
        return ctx.code, None
    return None, resp


def test_admin_only_for_llm_debuggers(monkeypatch: pytest.MonkeyPatch) -> None:
    traces = MemTraces()
    t = llmtrace.build(TRACE_ID, "ans_1", "credit", _collect())
    traces.rows[TRACE_ID] = asyncio.run(llmtrace.seal(t, ["src_wiki:doc-1"], FakeKeys()))

    code, _ = _get(monkeypatch, traces, ["platform_operator"], TRACE_ID)
    assert code == grpc.StatusCode.PERMISSION_DENIED
    code, _ = _get(monkeypatch, traces, ["llm_debugger"], "not-a-trace")
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    code, _ = _get(monkeypatch, traces, ["llm_debugger"], "f" * 32)
    assert code == grpc.StatusCode.NOT_FOUND
    code, resp = _get(monkeypatch, traces, ["llm_debugger"], TRACE_ID)
    assert code is None and resp.trace.calls[0].output == "回答 [S1]"
    traces.fail = True
    code, _ = _get(monkeypatch, traces, ["llm_debugger"], TRACE_ID)
    assert code == grpc.StatusCode.UNAVAILABLE


class FakeReceipts:
    def __init__(self, code: grpc.StatusCode | None = None) -> None:
        self.calls: list[Any] = []
        self.code = code

    async def RecordPurge(self, request: Any, timeout: float) -> Any:
        self.calls.append(request)
        if self.code is not None:
            raise grpc.aio.AioRpcError(
                self.code, grpc.aio.Metadata(), grpc.aio.Metadata(), details="x"
            )
        return None


def _purged(doc: str = "doc-1") -> dict[bytes, bytes]:
    evt = events_pb2.DocumentPurged(source_id="src_wiki", doc_id=doc, receipt_id="del_1")
    return {b"pb": evt.SerializeToString()}


def test_purge_deletes_traces_then_records_receipt() -> None:
    traces = MemTraces()
    t = llmtrace.build(TRACE_ID, "ans_1", "credit", _collect())
    traces.rows[TRACE_ID] = asyncio.run(llmtrace.seal(t, ["src_wiki:doc-1"], FakeKeys()))
    receipts = FakeReceipts()
    consumer = llmtrace.PurgeConsumer(cast(Any, None), traces, cast(Any, receipts), "o-0")

    assert asyncio.run(consumer.handle(_purged()))
    assert traces.rows == {}
    assert traces.deleted == [["src_wiki:doc-1", "doc-1"]]
    assert [(r.receipt_id, r.store) for r in receipts.calls] == [("del_1", "llm_traces")]


def test_purge_retries_when_store_or_receipts_unavailable() -> None:
    traces = MemTraces()
    traces.fail = True
    receipts = FakeReceipts()
    consumer = llmtrace.PurgeConsumer(cast(Any, None), traces, cast(Any, receipts), "c")
    # 删不掉追踪记录就不补记凭证、不确认
    assert not asyncio.run(consumer.handle(_purged()))
    assert receipts.calls == []

    traces.fail = False
    unavailable = llmtrace.PurgeConsumer(
        cast(Any, None), traces, cast(Any, FakeReceipts(grpc.StatusCode.UNAVAILABLE)), "c"
    )
    assert not asyncio.run(unavailable.handle(_purged()))
    # 凭证不存在（已被清理）时确认丢弃，不无限重试
    gone = llmtrace.PurgeConsumer(
        cast(Any, None), traces, cast(Any, FakeReceipts(grpc.StatusCode.NOT_FOUND)), "c"
    )
    assert asyncio.run(gone.handle(_purged()))
