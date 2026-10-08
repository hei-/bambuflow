"""MCP 端点与兼容 chat completions 端点的端到端测试（gateway 详设第 7 节、技术设计文档 13.9）。

测试用 agent 客户端经 Keycloak 管理 API 建在平台 realm 里
（只在开发与 CI 的 Keycloak 中，生产 realm 模板不含）：
e2e-agent 在 credit 租户的 mcp_agents 里登记过，e2e-agent-unregistered 没有登记。
需要先 make ingest。
"""

import json
import os
import re
from typing import Any

import httpx
import pytest

from .auth import KC, REDIRECT_URI, TEST_PASSWORD, platform_login
from .test_gateway import CREDIT_VISIBLE, GATEWAY, RISK_ONLY, WIKI, token

AGENT = "e2e-agent"
UNREGISTERED = "e2e-agent-unregistered"
QUESTION = "个人消费贷款的额度上限是多少？"


def _ensure_client(admin: httpx.Client, client_id: str) -> None:
    """建一个与 console 配置相同的公开客户端（授权码 + PKCE，受众为平台）；已存在就不动。"""
    r = admin.get("/admin/realms/bambuflow/clients", params={"clientId": client_id})
    r.raise_for_status()
    if r.json():
        return
    r = admin.post(
        "/admin/realms/bambuflow/clients",
        json={
            "clientId": client_id,
            "enabled": True,
            "publicClient": True,
            "standardFlowEnabled": True,
            "directAccessGrantsEnabled": False,
            "redirectUris": [REDIRECT_URI],
            "attributes": {
                "pkce.code.challenge.method": "S256",
                "access.token.signed.response.alg": "ES256",
            },
            "protocolMappers": [
                {
                    "name": "platform-audience",
                    "protocol": "openid-connect",
                    "protocolMapper": "oidc-audience-mapper",
                    "config": {
                        "included.custom.audience": "bambuflow-platform",
                        "access.token.claim": "true",
                        "id.token.claim": "false",
                    },
                }
            ],
        },
    )
    assert r.status_code in (201, 409), r.text


@pytest.fixture(scope="module", autouse=True)
def agent_clients() -> None:
    tok = httpx.post(
        f"{KC}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": "admin",
            "password": os.environ["KEYCLOAK_ADMIN_PASSWORD"],
        },
        timeout=30,
    )
    tok.raise_for_status()
    with httpx.Client(
        base_url=KC,
        headers={"Authorization": "Bearer " + tok.json()["access_token"]},
        timeout=30,
    ) as admin:
        for c in (AGENT, UNREGISTERED):
            _ensure_client(admin, c)


def agent_token(user: str, client_id: str = AGENT) -> str:
    return platform_login(user, TEST_PASSWORD, client_id)["access_token"]


def rpc(tok: str | None, method: str, params: dict[str, Any] | None = None) -> httpx.Response:
    headers = {
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2025-06-18",
    }
    if tok:
        headers["Authorization"] = "Bearer " + tok
    body: dict[str, Any] = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params is not None:
        body["params"] = params
    return httpx.post(GATEWAY + "/mcp", json=body, headers=headers, timeout=15)


def call_tool(tok: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    r = rpc(tok, "tools/call", {"name": name, "arguments": arguments})
    assert r.status_code == 200, r.text
    result: dict[str, Any] = r.json()["result"]
    return result


def test_unauthenticated_points_to_resource_metadata() -> None:
    r = rpc(None, "tools/list")
    assert r.status_code == 401
    assert "resource_metadata=" in r.headers["WWW-Authenticate"]
    meta = httpx.get(GATEWAY + "/.well-known/oauth-protected-resource/mcp", timeout=5).json()
    assert meta["resource"].endswith("/mcp")
    assert meta["authorization_servers"] == [f"{KC}/realms/bambuflow"]


def test_unregistered_agent_and_non_agent_clients_are_rejected() -> None:
    # 未登记的 agent 即使拿到了用户令牌也拒绝；console 不是 agent，也不能用 MCP
    for tok in (agent_token("alice", UNREGISTERED), token("alice")):
        r = rpc(tok, "tools/list")
        assert r.status_code == 403 and r.json()["type"] == "forbidden", r.text
    # agent 只能用 MCP
    r = httpx.post(
        GATEWAY + "/v1/search",
        json={"question": QUESTION},
        headers={"Authorization": "Bearer " + agent_token("alice")},
        timeout=10,
    )
    assert r.status_code == 403
    # agent 绑定 credit：不是 credit 成员的用户经它调用也拒绝
    assert rpc(agent_token("bob"), "tools/list").status_code == 403


def test_initialize_and_tools_are_read_only() -> None:
    tok = agent_token("alice")
    r = rpc(
        tok,
        "initialize",
        {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "e2e", "version": "1"},
        },
    )
    assert r.status_code == 200 and r.json()["result"]["protocolVersion"] == "2025-06-18"
    tools = rpc(tok, "tools/list").json()["result"]["tools"]
    assert {t["name"] for t in tools} == {
        "ask",
        "search_documents",
        "list_documents",
        "count_documents",
        "get_document_excerpt",
    }
    assert all(t["annotations"]["readOnlyHint"] for t in tools)


def test_search_documents_uses_the_end_user_permissions() -> None:
    res = call_tool(
        agent_token("alice"), "search_documents", {"question": "授信额度怎么计算？", "top_k": 50}
    )
    assert not res["isError"], res
    hits = res["structuredContent"]["hits"]
    pages = {h["doc_id"] for h in hits if h["chunk_id"].startswith(WIKI)}
    # 与 alice 直连 /v1/search 的可见范围一致：看不到风险部的页面
    assert pages and not pages & RISK_ONLY
    assert pages & CREDIT_VISIBLE
    text = res["content"][0]["text"]
    assert '<document_content trust="untrusted"' in text
    assert "hits[].text" in res["structuredContent"]["document_content"]["untrusted_fields"]


def test_excerpt_list_and_count() -> None:
    tok = agent_token("alice")
    hit = call_tool(tok, "search_documents", {"question": QUESTION})["structuredContent"]["hits"][0]
    ex = call_tool(
        tok, "get_document_excerpt", {"doc_id": hit["doc_id"], "chunk_id": hit["chunk_id"]}
    )
    assert not ex["isError"] and ex["structuredContent"]["chunk_id"] == hit["chunk_id"]
    # 无权的片段与不存在的片段一样是 not_found
    denied = call_tool(
        tok,
        "get_document_excerpt",
        {"doc_id": "risk-limit-policy", "chunk_id": WIKI + "risk-limit-policy#0"},
    )
    assert denied["isError"] and denied["structuredContent"]["type"] == "not_found"
    missing = call_tool(
        tok, "get_document_excerpt", {"doc_id": "no-such-doc", "chunk_id": WIKI + "no-such-doc#0"}
    )
    # 错误内容与无权时完全相同，不能据此探测文档是否存在（trace_id 每次请求不同，不比较）
    assert without_trace(missing["structuredContent"]) == without_trace(denied["structuredContent"])
    assert without_trace(missing["content"]) == without_trace(denied["content"])

    listed = call_tool(tok, "list_documents", {"page_size": 50})
    count = call_tool(tok, "count_documents", {})
    assert not listed["isError"] and not count["isError"]
    docs = {d["doc_id"] for d in listed["structuredContent"]["documents"]}
    assert docs and not docs & RISK_ONLY
    assert count["structuredContent"]["total_count"] == listed["structuredContent"]["total_count"]


def test_ask_returns_answer_contract() -> None:
    res = call_tool(agent_token("alice"), "ask", {"question": QUESTION})
    assert not res["isError"], res
    a = res["structuredContent"]
    assert a["status"] == "answered" and a["answer_id"].startswith("ans_") and a["citations"]


def compat(tok: str, body: dict[str, Any]) -> httpx.Response:
    return httpx.post(
        GATEWAY + "/v1/compat/chat/completions",
        json=body,
        headers={"Authorization": "Bearer " + tok, "Bambuflow-Tenant": "credit"},
        timeout=15,
    )


def test_compat_chat_completions_keeps_citations() -> None:
    r = compat(
        token("alice"), {"model": "any", "messages": [{"role": "user", "content": QUESTION}]}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["object"] == "chat.completion" and body["id"].startswith("ans_")
    assert body["choices"][0]["message"]["content"] == body["rag"]["answer"]
    assert body["rag"]["status"] == "answered" and body["rag"]["citations"]

    r = compat(token("alice"), {"messages": [{"role": "user", "content": QUESTION}], "tools": []})
    assert r.status_code == 400 and r.json()["type"] == "invalid_request"


def test_compat_stream_ends_with_done() -> None:
    with httpx.stream(
        "POST",
        GATEWAY + "/v1/compat/chat/completions",
        json={"stream": True, "messages": [{"role": "user", "content": QUESTION}]},
        headers={"Authorization": "Bearer " + token("alice"), "Bambuflow-Tenant": "credit"},
        timeout=15,
    ) as r:
        assert r.status_code == 200
        data = [line.removeprefix("data: ") for line in r.iter_lines() if line.startswith("data: ")]
    assert data[-1] == "[DONE]"
    chunks = [json.loads(d) for d in data[:-1]]
    content = "".join(c["choices"][0]["delta"].get("content", "") for c in chunks)
    assert content and chunks[-1]["choices"][0]["finish_reason"] == "stop"
    assert chunks[-1]["rag"]["final"]["status"] == "answered"


def without_trace(v: Any) -> str:
    """去掉 trace_id 后的规范化文本，用来比较两次请求的错误内容。"""
    return re.sub(r'\\?"trace_id\\?":\s*\\?"[0-9a-f]*\\?"', "", json.dumps(v, sort_keys=True))
