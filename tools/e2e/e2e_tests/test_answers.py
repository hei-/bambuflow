"""问答的端到端测试：gateway → orchestrator → retrieval / 模拟 LLM 网关 / 模拟 NLI（P0 第 7 步）。

模拟网关从证据里逐字摘句作答；问题里带 [mock:<样例>] 时追加用于测试校验的输出
（tools/devtools/cmd/mock-llm）。需要先 make ingest。
"""

import json
import os
import uuid
from typing import Any

import httpx

from .test_gateway import GATEWAY, token

LLM = os.environ.get("BF_LLM_URL", "http://llm:8000")
QUESTION = "个人消费贷款的额度上限是多少？"


def ask(
    user: str, tenant: str, question: str, filters: dict[str, Any] | None = None
) -> dict[str, Any]:
    body: dict[str, Any] = {"question": question}
    if filters:
        body["filters"] = filters
    r = httpx.post(
        GATEWAY + "/v1/answers",
        json=body,
        headers={"Authorization": "Bearer " + token(user), "Bambuflow-Tenant": tenant},
        timeout=10,
    )
    assert r.status_code == 200, r.text
    data: dict[str, Any] = r.json()
    return data


def search_hits(user: str, tenant: str, question: str) -> dict[str, dict[str, Any]]:
    r = httpx.post(
        GATEWAY + "/v1/search",
        json={"question": question, "top_k": 50},
        headers={"Authorization": "Bearer " + token(user), "Bambuflow-Tenant": tenant},
        timeout=10,
    )
    assert r.status_code == 200, r.text
    return {h["chunk_id"]: h for h in r.json()["hits"]}


def llm_calls(marker: str) -> int:
    r = httpx.get(LLM + "/mock/requests", params={"contains": marker}, timeout=5)
    r.raise_for_status()
    count: int = r.json()["count"]
    return count


def test_answer_is_grounded_and_cites_only_this_search() -> None:
    a = ask("alice", "credit", QUESTION)
    assert a["status"] == "answered" and a["answer_id"].startswith("ans_")
    assert a["sentences"] and a["citations"]
    for s in a["sentences"]:
        assert a["answer"][s["start"] : s["end"]] == s["text"]
        assert s["ref_ids"] and s["support"] >= 0.5
    # 同一用户同一问题的检索结果覆盖回答引用的每一块，引文逐字出自块正文
    hits = search_hits("alice", "credit", QUESTION)
    for c in a["citations"]:
        assert c["chunk_id"] in hits, c["chunk_id"]
        assert c["quote"] in hits[c["chunk_id"]]["text"]
        assert c["doc_version"] == hits[c["chunk_id"]]["doc_version"]
    why = a["why"]
    assert why["index_version"].startswith("ix-") and why["policy_version"].startswith("sha256:")
    assert why["embedding_model"] == "bge-m3@mock-hash-1"
    assert why["generation_model"] == "mock-chat-1" and why["nli_model"] == "nli@mock-bigram-2"
    assert why["prompt_version"] and why["support_threshold"] == 0.5
    assert why["retrievers"] == ["dense", "keyword"] and why["rerank_model"]


def test_unverified_sentences_never_reach_the_answer() -> None:
    a = ask("alice", "credit", QUESTION + "[mock:ungrounded][mock:fake-quote][mock:bad-ref]")
    text = a["answer"] + "".join(s["text"] for s in a["sentences"])
    assert "免息" not in text, "无依据的句子不应输出"
    assert "三十年" not in text, "引文不在原文里的句子不应输出"
    assert "[S99]" not in text, "引用不存在证据的句子不应输出"
    # 摘自证据的句子照常输出
    assert a["sentences"]


def test_empty_retrieval_does_not_call_llm() -> None:
    marker = uuid.uuid4().hex
    # 只检索一个不存在的数据源：结果必然为空
    a = ask("alice", "credit", f"{QUESTION} {marker}", {"source_ids": ["src_does_not_exist"]})
    assert a["status"] == "insufficient_evidence"
    assert a["answer"] == "" and a["citations"] == []
    assert llm_calls(marker) == 0
    # 对照：有检索结果时同样的计数会增加，证明上面的 0 不是计数失效
    ask("alice", "credit", f"{QUESTION} {marker}")
    assert llm_calls(marker) == 1


def test_llm_unavailable_returns_evidence_only() -> None:
    a = ask("alice", "credit", QUESTION + "[mock:error]")
    assert a["status"] == "evidence_only" and a["answer"] == "" and a["sentences"] == []
    assert a["citations"]
    assert {"stage": "generation", "reason": "unavailable"} in a["why"]["degraded"]


# ---- 流式（stream = true 时以 SSE 按句返回）----

UNVERIFIED = "[mock:ungrounded][mock:fake-quote][mock:bad-ref]"


def ask_stream(user: str, tenant: str, question: str) -> list[tuple[str, dict[str, Any]]]:
    events: list[tuple[str, dict[str, Any]]] = []
    with httpx.stream(
        "POST",
        GATEWAY + "/v1/answers",
        json={"question": question, "stream": True},
        headers={"Authorization": "Bearer " + token(user), "Bambuflow-Tenant": tenant},
        timeout=10,
    ) as r:
        assert r.status_code == 200, r.read()
        assert r.headers["content-type"].startswith("text/event-stream")
        name = ""
        for line in r.iter_lines():
            if line.startswith("event: "):
                name = line.removeprefix("event: ")
            elif line.startswith("data: "):
                events.append((name, json.loads(line.removeprefix("data: "))))
    return events


def check_order(events: list[tuple[str, dict[str, Any]]]) -> None:
    """meta 最先、final 最后；citation 先于引用它的 sentence；位置按已发送的句子累加。"""
    names = [n for n, _ in events]
    assert names[0] == "meta" and names[-1] == "final" and names.count("final") == 1, names
    assert events[0][1]["answer_id"].startswith("ans_")
    cited: set[int] = set()
    end = 0
    for name, data in events:
        if name == "citation":
            cited.add(data["ref_id"])
        elif name == "sentence":
            assert set(data["ref_ids"]) <= cited, "citation 应先于引用它的 sentence"
            assert data["start"] >= end and data["end"] - data["start"] == len(data["text"])
            end = data["end"]


def without(d: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {k: v for k, v in d.items() if k not in keys}


def test_stream_gives_same_sentences_and_citations_as_unary() -> None:
    for question in (QUESTION, QUESTION + UNVERIFIED):
        unary = ask("alice", "credit", question)
        events = ask_stream("alice", "credit", question)
        check_order(events)
        sentences = [without(d, "block_type") for n, d in events if n == "sentence"]
        citations = sorted((d for n, d in events if n == "citation"), key=lambda c: c["ref_id"])
        assert sentences == unary["sentences"], question
        assert citations == unary["citations"], question
        final = events[-1][1]
        assert final["status"] == unary["status"] and final["why"] == unary["why"]
        # 句子按 start、end 放回去就是非流式的 answer
        for s in sentences:
            assert unary["answer"][s["start"] : s["end"]] == s["text"]


def test_stream_never_carries_unverified_sentences() -> None:
    events = ask_stream("alice", "credit", QUESTION + UNVERIFIED)
    texts = "".join(d["text"] for n, d in events if n == "sentence")
    assert texts, "摘自证据的句子照常输出"
    assert "免息" not in texts, "无依据的句子不应出现在流里"
    assert "三十年" not in texts, "引文不在原文里的句子不应出现在流里"
    assert "[S99]" not in texts, "引用不存在证据的句子不应出现在流里"
    # 有句子被丢弃，覆盖率不足：已发送的句子保留，final 告知证据不足
    assert events[-1][1]["status"] == "insufficient_evidence"


def test_stream_llm_unavailable_ends_with_evidence_only() -> None:
    events = ask_stream("alice", "credit", QUESTION + "[mock:error]")
    check_order(events)
    assert not [d for n, d in events if n == "sentence"]
    assert [d for n, d in events if n == "citation"], "只给证据时证据以 citation 发送"
    final = events[-1][1]
    assert final["status"] == "evidence_only"
    assert {"stage": "generation", "reason": "unavailable"} in final["why"]["degraded"]


def test_stream_rejections_match_unary() -> None:
    # 鉴权在开始输出前判定，与非流式相同：返回 problem+json，不是 SSE
    r = httpx.post(
        GATEWAY + "/v1/answers",
        json={"question": QUESTION, "stream": True},
        headers={"Bambuflow-Tenant": "credit"},
        timeout=10,
    )
    assert r.status_code == 401 and r.headers["content-type"].startswith("application/problem+json")
