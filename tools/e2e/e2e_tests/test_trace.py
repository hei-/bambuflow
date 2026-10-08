"""一次问答在追踪查看器里能看到完整链路（P0 第 8 步）。

gateway → orchestrator → retrieval → 模型调用（LLM、NLI）。
"""

import os
import time
from typing import Any

import httpx

from .test_answers import QUESTION, ask

JAEGER = os.environ.get("JAEGER_URL", "http://jaeger:16686")
# span 由各服务批量导出，最多等这么久
WAIT_SECONDS = 30


def fetch(trace_id: str) -> tuple[set[str], set[str]]:
    """返回 trace 里出现的服务名与操作名。"""
    r = httpx.get(f"{JAEGER}/api/traces/{trace_id}", timeout=5)
    if r.status_code != 200:
        return set(), set()
    services: set[str] = set()
    ops: set[str] = set()
    for t in r.json().get("data") or []:
        procs: dict[str, Any] = t["processes"]
        for span in t["spans"]:
            services.add(procs[span["processID"]]["serviceName"])
            ops.add(span["operationName"])
    return services, ops


def test_answer_trace_covers_the_whole_chain() -> None:
    a = ask("alice", "credit", QUESTION)
    trace_id = a["trace_id"]
    assert len(trace_id) == 32
    want_services = {"gateway", "orchestrator", "retrieval"}
    deadline = time.monotonic() + WAIT_SECONDS
    services: set[str] = set()
    ops: set[str] = set()
    while time.monotonic() < deadline:
        services, ops = fetch(trace_id)
        if want_services <= services and {"llm.chat", "nli.infer"} <= ops:
            break
        time.sleep(2)
    assert want_services <= services, f"trace 里只有 {sorted(services)}"
    assert "llm.chat" in ops and "nli.infer" in ops, f"缺少模型调用 span: {sorted(ops)}"
    assert any("RetrievalService/Search" in op for op in ops), sorted(ops)
    assert any("RetrievalService/CheckAccess" in op for op in ops), sorted(ops)
