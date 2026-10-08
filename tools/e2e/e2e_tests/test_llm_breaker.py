"""运行时保护的端到端测试：LLM 熔断打开后只返回证据、不再调用网关，冷却后半开探测成功即恢复
（orchestrator 详设 8.5，总体设计 11.6）。

经模拟 LLM 网关的故障注入（POST /mock/faults）让接下来的补全请求都返回 503。熔断按模型统计，
打开期间影响所有问答，所以用例结束前一定清除故障并等熔断关闭，不影响之后的用例。
过载保护（并发上限与排队）需要几十路并发的慢请求，经 gateway 会先被 gateway 自己的并发限制挡住，
由 orchestrator 的单测覆盖（tests/test_admission.py、test_runtime.py）。
"""

import time
import uuid
from typing import Any

import httpx

from .test_answers import LLM, QUESTION, ask, llm_calls

# 与 orchestrator 的 circuit.py 一致
OPEN_SECONDS = 30
CLOSE_AFTER = 10
# 熔断要窗口里错误率超过 20%：之前用例的成功调用也在 1 分钟窗口里，多留余量
MAX_FAILING_ASKS = 60


def degraded(a: dict[str, Any]) -> list[tuple[str, str]]:
    return [(d["stage"], d["reason"]) for d in a["why"].get("degraded", [])]


def faults(fail_next: int) -> None:
    if fail_next:
        r = httpx.post(LLM + "/mock/faults", json={"fail_next": fail_next}, timeout=5)
    else:
        r = httpx.delete(LLM + "/mock/faults", timeout=5)
    r.raise_for_status()


def test_circuit_opens_on_gateway_errors_and_recovers() -> None:
    marker = uuid.uuid4().hex
    opened: tuple[str, dict[str, Any]] | None = None
    try:
        faults(MAX_FAILING_ASKS)
        for i in range(MAX_FAILING_ASKS):
            q = f"{QUESTION} {marker}-{i}"
            a = ask("alice", "credit", q)
            # 网关报错期间每次都只返回证据
            assert a["status"] == "evidence_only" and a["citations"], a
            if ("generation", "circuit_open") in degraded(a):
                opened = (q, a)
                break
    finally:
        faults(0)
    assert opened is not None, "网关持续报错时熔断应打开"
    q, a = opened
    assert a["answer"] == "" and a["sentences"] == []
    assert llm_calls(q) == 0, "熔断打开期间不应再向网关发请求"

    # 冷却期内仍然熔断：网关虽已恢复，也不发请求
    q = f"{QUESTION} {marker}-still-open"
    assert ("generation", "circuit_open") in degraded(ask("alice", "credit", q))
    assert llm_calls(q) == 0

    # 冷却后半开：连续成功 CLOSE_AFTER 次关闭，之后照常回答
    time.sleep(OPEN_SECONDS + 1)
    for i in range(CLOSE_AFTER + 1):
        a = ask("alice", "credit", f"{QUESTION} {marker}-probe-{i}")
        assert a["status"] == "answered", a
        assert ("generation", "circuit_open") not in degraded(a)
