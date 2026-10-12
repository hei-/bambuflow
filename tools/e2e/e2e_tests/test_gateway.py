"""gateway 的端到端测试：真实登录 → gateway → retrieval（P0 第 6 步起为真实服务）。

断言“身份来自平台身份库、租户解析正确、Principal 被 retrieval 验签并用于权限过滤”：
每个测试账号只能检索到 Wiki 样例里它有权看的页面。测试身份与群组见 tools/test-upstream；
需要先 make ingest。
"""

import os
import time
from collections.abc import Callable, Iterator
from typing import cast

import httpx
import pytest
from ldap3 import MODIFY_ADD, MODIFY_DELETE, Connection, Server

from .auth import TEST_PASSWORD, platform_login
from .upstream import visible_distractors

GATEWAY = os.environ.get("GATEWAY_URL", "http://gateway:8080")
QUESTION = {"question": "授信额度怎么计算？", "top_k": 50}
SYNC_DEADLINE_SECONDS = 70  # 身份同步每分钟一次，加上轮询间隔与同步耗时的余量
WIKI = "src_wikidemo:"

# ldap3 的类型桩没有标注 modify，在这里给出签名
Changes = dict[str, list[tuple[str, list[str]]]]
ldap_modify = cast(Callable[[Connection, str, Changes], bool], Connection.modify)


def token(user: str) -> str:
    return platform_login(user, TEST_PASSWORD)["access_token"]


def search(tok: str | None, tenant: str | None = None, path: str = "/v1/search") -> httpx.Response:
    headers = {}
    if tok:
        headers["Authorization"] = "Bearer " + tok
    if tenant:
        headers["Bambuflow-Tenant"] = tenant
    return httpx.post(GATEWAY + path, json=QUESTION, headers=headers, timeout=10)


def wiki_pages(resp: httpx.Response) -> set[str]:
    """检索结果里 Wiki 样例源的页面（top_k 50 足以覆盖样例的全部块）。"""
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["index_version"].startswith("ix-")
    return {h["doc_id"] for h in body["hits"] if h["chunk_id"].startswith(WIKI)}


# 可见范围见 tools/test-upstream/wiki-README.md。页面多于 top_k 能覆盖的块数，所以断言两件事：
# 返回的页面都在可见范围内，且一定不含无权页面。
CREDIT_VISIBLE = {
    "credit-personal-loan",
    "aml-report-procedure",
    "staff-handbook",
    "credit-mortgage-loan",
    "credit-mortgage-loan-2023",
    "credit-collateral",
    "credit-post-loan",
    "credit-small-business",
    "credit-faq",
    "compliance-kyc",
    "compliance-sanctions",
    "hr-travel",
    "hr-training",
    "hr-performance",
    "it-password",
    "it-remote-work",
    "it-incident",
    "ops-counter",
    "ops-cash",
}
RISK_ONLY = {"risk-limit-policy", "risk-market-risk", "risk-operational", "risk-stress-test"}
NOBODY = {"legacy-orphan", "risk-capital-plan"}  # 无主无 ACL / 受限级，测试账号都不可见


CREDIT_TOKENS = {"group:credit-l2", "group:all-staff", "dept:信贷部"}
RISK_TOKENS = {"group:risk-dept", "group:all-staff", "dept:风险管理部", "user:bob"}


def with_distractors(visible: set[str], tokens: set[str], clearance: int) -> set[str]:
    """干扰页面（tools/test-upstream/gen_distractors.py）按各自的 ACL 与密级加入可见范围。"""
    return visible | visible_distractors(tokens, clearance)


@pytest.mark.parametrize(
    ("user", "tenant", "visible", "forbidden"),
    [
        # credit-l2 + all-staff，内部级：不含风险部页面、属主草稿、受限页面
        (
            "alice",
            "credit",
            with_distractors(CREDIT_VISIBLE, CREDIT_TOKENS | {"user:alice"}, 2),
            RISK_ONLY | NOBODY | {"credit-draft-notes", "risk-draft-2026"},
        ),
        # 没有 ACL 的草稿只有属主可见
        (
            "carl",
            "credit",
            with_distractors(
                CREDIT_VISIBLE | {"credit-draft-notes"}, CREDIT_TOKENS | {"user:carl"}, 2
            ),
            RISK_ONLY | NOBODY | {"risk-draft-2026"},
        ),
        # 机密级的风险经理能看到机密页面与自己的草稿，看不到受限页面和信贷 L2 的页面
        (
            "bob",
            "risk",
            with_distractors(
                RISK_ONLY
                | {
                    "risk-draft-2026",
                    "aml-report-procedure",
                    "staff-handbook",
                    "compliance-kyc",
                    "compliance-sanctions",
                    "hr-travel",
                    "hr-training",
                    "hr-performance",
                    "it-password",
                    "it-remote-work",
                    "it-incident",
                    "ops-counter",
                    "ops-cash",
                },
                RISK_TOKENS,
                3,
            ),
            NOBODY | {"credit-personal-loan", "credit-collateral", "credit-draft-notes"},
        ),
    ],
)
def test_search_returns_only_pages_visible_to_caller(
    user: str, tenant: str, visible: set[str], forbidden: set[str]
) -> None:
    pages = wiki_pages(search(token(user), tenant))
    assert pages and pages <= visible, pages - visible
    assert not pages & forbidden


def test_tenant_resolution() -> None:
    alice = token("alice")
    r = search(alice)  # 同时属于 credit 与 ops，必须指定
    assert r.status_code == 400 and r.json()["type"] == "invalid_request"
    r = search(alice, "risk")  # 不是 risk 的成员
    assert r.status_code == 403 and r.json()["type"] == "forbidden"
    r = search(None)
    assert r.status_code == 401 and r.json()["type"] == "unauthenticated"


def test_large_groups_expanded_from_identity_store() -> None:
    # eve 属于 251 个群组，Principal 只带引用；retrieval 从平台身份库展开出 all-staff
    # 才能看到员工手册。eve 只属于 ops，可省略租户
    public = {
        "staff-handbook",
        "hr-travel",
        "hr-training",
        "hr-performance",
        "it-password",
        "it-remote-work",
        "it-incident",
    } | visible_distractors({"group:all-staff", "user:eve"}, 1)
    pages = wiki_pages(search(token("eve")))
    # 运营部的柜面与现金制度虽然对全员群组开放，但是内部级，公开级的 eve 看不到
    assert pages and pages <= public, pages - public


def test_pilot_tenant_closes_answers_to_non_pilot_members() -> None:
    # ruth 是 risk 的成员但不在试点群组：能检索、不能问答；试点群组里的 bob 能问答
    ruth = token("ruth")
    assert search(ruth, "risk").status_code == 200
    r = search(ruth, "risk", path="/v1/answers")
    assert r.status_code == 403 and r.json()["type"] == "answers_not_enabled"
    r = httpx.post(
        GATEWAY + "/v1/answers",
        json={"question": QUESTION["question"]},
        headers={"Authorization": "Bearer " + token("bob"), "Bambuflow-Tenant": "risk"},
        timeout=10,
    )
    assert r.status_code == 200, r.text


@pytest.fixture
def directory() -> Iterator[Connection]:
    conn = Connection(
        Server(os.environ["LDAP_URL"]),
        "cn=admin,dc=corp,dc=example",
        os.environ["LDAP_ADMIN_PASSWORD"],
        auto_bind=True,
    )
    yield conn
    conn.unbind()


def wait_for(tok: str, tenant: str, want: int, deadline: float) -> float:
    start = time.monotonic()
    while time.monotonic() - start < deadline:
        if search(tok, tenant).status_code == want:
            return time.monotonic() - start
        time.sleep(5)
    pytest.fail(f"{deadline:.0f} 秒内没有变成 {want}")


def test_group_removal_takes_effect_within_a_minute(directory: Connection) -> None:
    group = "cn=credit-l2,ou=groups,dc=corp,dc=example"
    alice_dn = "uid=alice,ou=people,dc=corp,dc=example"
    tok = token("alice")
    assert search(tok, "credit").status_code == 200
    try:
        removed = ldap_modify(directory, group, {"member": [(MODIFY_DELETE, [alice_dn])]})
        assert removed, directory.result
        elapsed = wait_for(tok, "credit", 403, SYNC_DEADLINE_SECONDS)
        print(f"移出群组后 {elapsed:.0f} 秒生效")
    finally:
        # 无论成败都把 alice 加回去，保证测试可重复运行
        ldap_modify(directory, group, {"member": [(MODIFY_ADD, [alice_dn])]})
    wait_for(tok, "credit", 200, SYNC_DEADLINE_SECONDS)


def test_preview_highlights_quote_and_hides_invisible_chunks() -> None:
    """引用预览：用问答返回的引用取片段，引文在片段里有高亮；看不到的文档与不存在的文档一样 404。"""
    from .test_answers import QUESTION, ask

    a = ask("alice", "credit", QUESTION)
    cit = a["citations"][0]
    hdrs = {"Authorization": "Bearer " + token("alice"), "Bambuflow-Tenant": "credit"}
    r = httpx.get(
        f"{GATEWAY}/v1/documents/{cit['doc_id']}/preview",
        params={"chunk_id": cit["chunk_id"], "quote": cit["quote"]},
        headers=hdrs,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    p = r.json()
    assert p["chunk_id"] == cit["chunk_id"] and p["highlights"]
    h = p["highlights"][0]
    assert p["text"][h["start"] : h["end"]] == cit["quote"]
    # risk-limit-policy 对 alice 不可见：与不存在的文档返回同样的 404
    hidden = httpx.get(
        f"{GATEWAY}/v1/documents/risk-limit-policy/preview",
        params={"chunk_id": WIKI + "risk-limit-policy#0"},
        headers=hdrs,
        timeout=10,
    )
    missing = httpx.get(
        f"{GATEWAY}/v1/documents/no-such-doc/preview",
        params={"chunk_id": WIKI + "no-such-doc#0"},
        headers=hdrs,
        timeout=10,
    )
    assert hidden.status_code == missing.status_code == 404
    assert hidden.json()["type"] == missing.json()["type"]
