"""意图路由、多步检索与会话（P3 C 线，orchestrator 详设 5.3、第 6 节、第 10 节）。

多跳问题经模拟 LLM 的规划样例（[mock-plan:子问题1|子问题2]，tools/devtools/cmd/mock-llm）
拆成确定的子问题；
回答仍是逐句校验过的句子，引用只来自各轮检索命中的块。需要先 make ingest。
"""

import uuid
from typing import Any

import httpx

from .test_answers import ask_stream, check_order, llm_calls
from .test_gateway import GATEWAY, token

OLD_NEW = (
    "新旧两版个人住房贷款管理办法在首付比例上有什么区别？"
    "[mock-plan:现行个人住房贷款管理办法的首付比例|2023 版个人住房贷款管理办法的首付比例]"
)


def post(user: str, body: dict[str, Any], tenant: str = "credit") -> httpx.Response:
    return httpx.post(
        GATEWAY + "/v1/answers",
        json=body,
        headers={"Authorization": "Bearer " + token(user), "Bambuflow-Tenant": tenant},
        timeout=15,
    )


def answer(user: str, question: str, **extra: Any) -> dict[str, Any]:
    r = post(user, {"question": question, **extra})
    assert r.status_code == 200, r.text
    data: dict[str, Any] = r.json()
    return data


def test_multi_hop_answers_from_every_round_with_verified_sentences() -> None:
    a = answer("alice", OLD_NEW)
    why = a["why"]
    assert why["intent"] == "multi_hop" and why["prompt_version"].startswith("qa-multi-hop@")
    rounds = why["agent_rounds"]
    assert 1 <= len(rounds) <= 3 and [r["round"] for r in rounds] == list(range(1, len(rounds) + 1))
    first = rounds[0]["calls"]
    assert [c["sub_question"] for c in first] == [
        "现行个人住房贷款管理办法的首付比例",
        "2023 版个人住房贷款管理办法的首付比例",
    ]
    assert all(c["tool"] == "doc_search" for r in rounds for c in r["calls"])
    assert a["status"] == "answered", a
    # 引用只来自各轮检索命中的块；每句都带引用（固定说明除外）并能在回答里定位
    found = {chunk for r in rounds for c in r["calls"] for chunk in c["chunk_ids"]}
    assert {c["chunk_id"] for c in a["citations"]} <= found
    for s in a["sentences"]:
        assert a["answer"][s["start"] : s["end"]] == s["text"]
        assert s["ref_ids"] or s["claim_type"] == "none"
    # 两个子问题的证据都进了上下文：新旧两版都被引用
    titles = {c["title"] for c in a["citations"]}
    assert any("2023" in t for t in titles) and any("2023" not in t for t in titles), titles


def test_multi_hop_stream_carries_rounds_in_final() -> None:
    events = ask_stream("alice", "credit", OLD_NEW)
    check_order(events)
    assert events[0][1]["conversation_id"].startswith("conv_")
    final = events[-1][1]
    assert final["why"]["intent"] == "multi_hop" and final["why"]["agent_rounds"]


def test_planner_unavailable_falls_back_to_the_question() -> None:
    a = answer("alice", "个人消费贷款的额度上限和期限分别是多少？[mock:plan-error]")
    assert {"stage": "plan", "reason": "unavailable"} in a["why"]["degraded"]
    (call,) = a["why"]["agent_rounds"][0]["calls"]
    assert call["sub_question"].startswith("个人消费贷款的额度上限和期限分别是多少")
    assert a["status"] in ("answered", "insufficient_evidence")


def test_out_of_scope_is_refused_without_retrieval_or_llm() -> None:
    marker = uuid.uuid4().hex
    a = answer("alice", f"帮我推荐一只股票 {marker}")
    assert a["status"] == "refused" and a["why"]["intent"] == "out_of_scope"
    assert a["citations"] == [] and all(not s["ref_ids"] for s in a["sentences"])
    assert llm_calls(marker) == 0


def test_single_hop_question_stays_on_single_hop_path() -> None:
    a = answer("alice", "个人消费贷款的额度上限是多少？")
    assert a["why"]["intent"] == "single_hop" and "agent_rounds" not in a["why"]


def test_conversation_continues_for_owner_only() -> None:
    first = answer("alice", "个人住房贷款的首付比例是多少？")
    conv = first["conversation_id"]
    assert conv.startswith("conv_")
    again = answer("alice", "二套住房呢？", conversation_id=conv)
    assert again["conversation_id"] == conv
    # 同租户的另一个用户拿着 alice 的会话 ID：按新会话处理，不报错、不沿用
    other = answer("carl", "二套住房呢？", conversation_id=conv)
    assert other["conversation_id"].startswith("conv_") and other["conversation_id"] != conv


def test_malformed_conversation_id_is_rejected_at_gateway() -> None:
    r = post("alice", {"question": "首付比例是多少？", "conversation_id": "abc"})
    assert r.status_code == 400


def test_owner_can_delete_conversation() -> None:
    def delete(user: str, conv: str) -> httpx.Response:
        return httpx.delete(
            f"{GATEWAY}/v1/conversations/{conv}",
            headers={"Authorization": "Bearer " + token(user), "Bambuflow-Tenant": "credit"},
            timeout=15,
        )

    conv = answer("alice", "个人住房贷款的首付比例是多少？")["conversation_id"]
    # 别人删除：同样 204，但会话还在
    assert delete("carl", conv).status_code == 204
    assert answer("alice", "二套住房呢？", conversation_id=conv)["conversation_id"] == conv
    # 本人删除后，带原 ID 的提问按新会话处理
    assert delete("alice", conv).status_code == 204
    again = answer("alice", "二套住房呢？", conversation_id=conv)
    assert again["conversation_id"] != conv
    assert delete("alice", "abc").status_code == 404
