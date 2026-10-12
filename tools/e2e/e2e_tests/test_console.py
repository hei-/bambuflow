"""管理控制台（services/console）的端到端测试：经 console 的 nginx 访问，验证静态页、前端路由回退、
安全头、
运行时配置，以及登录后经同源代理（/v1 → gateway）的问答与流式问答。
页面本身的渲染由 console 的组件测试覆盖。
"""

import json
import os
from typing import Any

import httpx

from .test_answers import QUESTION, check_order
from .test_gateway import token

CONSOLE = os.environ.get("CONSOLE_URL", "http://console:8080")


def test_static_pages_and_spa_fallback() -> None:
    index = httpx.get(CONSOLE + "/", timeout=10)
    assert index.status_code == 200 and '<div id="root">' in index.text
    # 前端路由（刷新管理页、登录回调）都回到 index.html
    for path in ("/admin/sources", "/callback?code=x&state=y"):
        r = httpx.get(CONSOLE + path, timeout=10)
        assert r.status_code == 200 and r.text == index.text, path
        assert r.headers["x-frame-options"] == "DENY"
        assert r.headers["x-content-type-options"] == "nosniff"
    cfg = httpx.get(CONSOLE + "/config.json", timeout=10)
    assert cfg.headers["cache-control"] == "no-store"
    body = cfg.json()
    assert body["clientId"] == "console" and body["authority"].endswith("/realms/bambuflow")
    assert {t["id"] for t in body["tenants"]} >= {"credit", "risk", "ops"}


def test_api_is_proxied_to_gateway_with_problem_responses() -> None:
    r = httpx.post(CONSOLE + "/v1/answers", json={"question": QUESTION}, timeout=10)
    assert r.status_code == 401 and r.json()["type"] == "unauthenticated"


def test_streaming_answer_through_console() -> None:
    events: list[tuple[str, dict[str, Any]]] = []
    with httpx.stream(
        "POST",
        CONSOLE + "/v1/answers",
        json={"question": QUESTION, "stream": True},
        headers={
            "Authorization": "Bearer " + token("alice"),
            "Bambuflow-Tenant": "credit",
            "Accept": "text/event-stream",
        },
        timeout=20,
    ) as r:
        assert r.status_code == 200, r.read()
        assert r.headers["content-type"].startswith("text/event-stream")
        name = ""
        for line in r.iter_lines():
            if line.startswith("event: "):
                name = line.removeprefix("event: ")
            elif line.startswith("data: "):
                events.append((name, json.loads(line.removeprefix("data: "))))
    check_order(events)
    final = events[-1][1]
    assert final["status"] == "answered", final
    answer_id = events[0][1]["answer_id"]
    # 反馈也经同源代理，幂等键必填
    fb = httpx.post(
        f"{CONSOLE}/v1/answers/{answer_id}/feedback",
        json={"rating": "useful"},
        headers={
            "Authorization": "Bearer " + token("alice"),
            "Bambuflow-Tenant": "credit",
            "Idempotency-Key": "console-e2e-" + answer_id,
        },
        timeout=10,
    )
    assert fb.status_code == 200, fb.text
