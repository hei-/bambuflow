"""异步批量与 Webhook 的端到端测试（P3-A，技术设计文档 13.5、13.7）。

批次经 gateway 提交，qa/batch 逐条换发身份后以调用目的 batch 调 orchestrator；结果只有提交人能取。
Webhook 订阅经 governance 的 WebhookAdminService 登记，接收方是本测试进程里起的 HTTP 服务，
校验签名、事件体与事件 ID。需要先 make ingest。
"""

import hashlib
import hmac
import http.server
import json
import os
import queue
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from typing import Any

import grpc
import httpx
import pytest

from bambuflow.admin.v1 import index_admin_pb2, webhook_admin_pb2, webhook_admin_pb2_grpc

from .test_gateway import GATEWAY, token
from .test_index_admin import admin, live_models, write
from .test_listing import visible

WEBHOOK_ADDR = os.environ.get("BF_WEBHOOK_ADDR", "webhook:9090")
QUESTIONS = ["个人消费贷款的额度上限是多少？", "授信审批需要哪些材料？", "贷款逾期怎么处理？"]
DONE = {"completed", "failed", "cancelled"}


def jsonl(lines: list[dict[str, Any]]) -> bytes:
    return "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines).encode()


def headers(user: str, extra: dict[str, str] | None = None) -> dict[str, str]:
    return {"Authorization": "Bearer " + token(user), "Bambuflow-Tenant": "credit", **(extra or {})}


def submit(user: str, lines: list[dict[str, Any]], key: str | None = None) -> httpx.Response:
    return httpx.post(
        GATEWAY + "/v1/batches",
        files={"input_file": ("batch.jsonl", jsonl(lines), "application/x-ndjson")},
        data={"mode": "answer"},
        headers=headers(user, {"Idempotency-Key": key} if key else None),
        timeout=30,
    )


def items(n: int = 3) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = [
        {"custom_id": f"q{i}", "question": QUESTIONS[i % len(QUESTIONS)]} for i in range(n)
    ]
    out[0]["filters"] = {"source_ids": ["src_wikidemo"]}
    return out


def wait_done(batch_id: str, timeout: float = 180) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        r = httpx.get(f"{GATEWAY}/v1/batches/{batch_id}", headers=headers("alice"), timeout=10)
        assert r.status_code == 200, r.text
        b: dict[str, Any] = r.json()
        if b["status"] in DONE:
            return b
        assert time.monotonic() < deadline, f"批次没有在 {timeout} 秒内结束: {b}"
        time.sleep(1)


def results(user: str, batch_id: str) -> httpx.Response:
    return httpx.get(f"{GATEWAY}/v1/batches/{batch_id}/results", headers=headers(user), timeout=60)


def test_batch_runs_and_only_submitter_gets_results() -> None:
    r = submit("alice", items())
    assert r.status_code == 200, r.text
    b = r.json()
    assert b["batch_id"].startswith("bat_") and b["status"] in ("queued", "running")
    assert b["counts"]["total"] == 3

    done = wait_done(b["batch_id"])
    assert done["status"] == "completed", done
    assert done["counts"]["succeeded"] == 3 and done["counts"]["failed"] == 0
    assert done["completed_at"] and done["expires_at"] > done["completed_at"]

    r = results("alice", b["batch_id"])
    assert r.status_code == 200 and r.headers["content-type"] == "application/x-ndjson", r.text
    lines = [json.loads(x) for x in r.text.splitlines()]
    assert [x["custom_id"] for x in lines] == ["q0", "q1", "q2"]
    for x in lines:
        a = x["answer"]
        assert a["answer_id"].startswith("ans_") and a["status"] in (
            "answered",
            "insufficient_evidence",
        )
        assert "audit" not in a
        for s in a["sentences"]:
            assert a["answer"][s["start"] : s["end"]] == s["text"]
    # 同租户的其他人看不到这个批次，与不存在一样是 404
    for path in ("", "/results"):
        r = httpx.get(f"{GATEWAY}/v1/batches/{b['batch_id']}{path}", headers=headers("carl"))
        assert r.status_code == 404, r.text


def listed(a: dict[str, Any]) -> list[str]:
    return [s["document"]["doc_id"] for s in a["sentences"] if s["claim_type"] == "list_item"]


def test_listing_items_rechecked_by_document_when_fetching() -> None:
    """清单类回答取结果时按文档复核（K2 详设第 8 节）：执行之后变得不可见的文档从列表里去掉，
    口径块的总数与列出数同步减少，回答里不提无权文档。用紧急拒绝名单模拟执行后的撤权。"""
    r = submit("alice", [{"custom_id": "list", "question": "信贷管理部有哪些制度？"}])
    assert r.status_code == 200, r.text
    batch_id = r.json()["batch_id"]
    assert wait_done(batch_id)["status"] == "completed"

    r = results("alice", batch_id)
    assert r.status_code == 200, r.text
    (line,) = [json.loads(x) for x in r.text.splitlines()]
    a = line["answer"]
    assert a["answer_kind"] == "enumeration", line
    want = visible(a["why"]["inferred_filters"])
    docs = listed(a)
    assert docs and set(docs) == want
    (scope_block,) = [b for b in a["blocks"] if b["block_type"] == "scope"]
    before = scope_block["scope"]

    gone = docs[0]
    titles = {
        s["document"]["doc_id"]: s["document"]["title"] for s in a["sentences"] if "document" in s
    }
    title = titles[gone]
    added = admin().AddDenyRules(
        index_admin_pb2.AddDenyRulesRequest(doc_ids=[gone], reason="e2e 批量取结果复核"), timeout=10
    )
    assert added.added == 1
    try:
        r = results("alice", batch_id)
        assert r.status_code == 200, r.text
        (line,) = [json.loads(x) for x in r.text.splitlines()]
        a = line["answer"]
        assert listed(a) == docs[1:], "被撤权的文档应从列表里去掉，其余照旧"
        (scope_block,) = [b for b in a["blocks"] if b["block_type"] == "scope"]
        scope = scope_block["scope"]
        assert scope["shown"] == before["shown"] - 1
        assert scope["total_count"] == before["total_count"] - 1
        assert "无权" not in a["answer"] and f"《{title}》" not in a["answer"]
        for s in a["sentences"]:
            assert a["answer"][s["start"] : s["end"]] == s["text"]
    finally:
        admin().RemoveDenyRules(index_admin_pb2.RemoveDenyRulesRequest(doc_ids=[gone]), timeout=10)


def test_batch_idempotency_key() -> None:
    key = "e2e-" + uuid.uuid4().hex
    first = submit("alice", items(1), key)
    again = submit("alice", items(1), key)
    assert first.status_code == 200 and again.status_code == 200, again.text
    assert first.json()["batch_id"] == again.json()["batch_id"]
    other = submit("alice", items(2), key)
    assert other.status_code == 409 and other.json()["type"] == "idempotency_conflict"
    wait_done(first.json()["batch_id"])


def test_batch_rejects_bad_input() -> None:
    dup = [{"custom_id": "q1", "question": "a"}, {"custom_id": "q1", "question": "b"}]
    r = submit("alice", dup)
    assert r.status_code == 400 and r.json()["type"] == "invalid_request"
    r = httpx.post(
        GATEWAY + "/v1/batches",
        files={"input_file": ("b.jsonl", jsonl(items(1)), "application/x-ndjson")},
        data={"mode": "evidence_only"},
        headers=headers("alice"),
    )
    assert r.status_code == 400, r.text


def test_cancel_stops_queued_items() -> None:
    r = submit("alice", items(300))
    assert r.status_code == 200, r.text
    batch_id = r.json()["batch_id"]
    c = httpx.post(f"{GATEWAY}/v1/batches/{batch_id}/cancel", headers=headers("alice"))
    assert c.status_code == 200, c.text
    done = wait_done(batch_id)
    assert done["status"] == "cancelled" and done["counts"]["cancelled"] > 0, done
    lines = [json.loads(x) for x in results("alice", batch_id).text.splitlines()]
    assert (
        sum(1 for x in lines if x.get("error", {}).get("type") == "cancelled")
        == done["counts"]["cancelled"]
    )


# ---- Webhook ----


class _Receiver(http.server.BaseHTTPRequestHandler):
    got: "queue.Queue[tuple[dict[str, str], bytes]]"

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.got.put((dict(self.headers.items()), body))
        self.send_response(204)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture
def receiver() -> Iterator[tuple[str, "queue.Queue[tuple[dict[str, str], bytes]]"]]:
    got: queue.Queue[tuple[dict[str, str], bytes]] = queue.Queue()
    handler = type("Receiver", (_Receiver,), {"got": got})
    # e2e 容器在 compose 网络里的地址：webhook 服务按它回调
    ip = socket.gethostbyname(socket.gethostname())
    srv = http.server.ThreadingHTTPServer((ip, 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://{ip}:{srv.server_address[1]}/hook", got
    srv.shutdown()


def webhooks() -> Any:
    return webhook_admin_pb2_grpc.WebhookAdminServiceStub(grpc.insecure_channel(WEBHOOK_ADDR))


@pytest.fixture
def subscribe() -> Iterator[Any]:
    created: list[str] = []

    def make(url: str, types: list[str]) -> webhook_admin_pb2.CreateSubscriptionResponse:
        resp: webhook_admin_pb2.CreateSubscriptionResponse = webhooks().CreateSubscription(
            webhook_admin_pb2.CreateSubscriptionRequest(
                tenant="credit", url=url, event_types=types, description="e2e"
            ),
            timeout=10,
        )
        created.append(resp.subscription.subscription_id)
        return resp

    yield make
    for sid in created:
        webhooks().DeleteSubscription(
            webhook_admin_pb2.DeleteSubscriptionRequest(subscription_id=sid), timeout=10
        )


def wait_event(
    got: "queue.Queue[tuple[dict[str, str], bytes]]", typ: str, object_id: str, timeout: float = 60
) -> tuple[dict[str, str], bytes]:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        assert remaining > 0, f"{timeout} 秒内没有收到 {typ} {object_id}"
        h, body = got.get(timeout=remaining)
        evt = json.loads(body)
        if evt["type"] == typ and evt["object_id"] == object_id:
            return h, body


def check_signature(secret: str, h: dict[str, str], body: bytes) -> None:
    ts = h["Bambuflow-Webhook-Timestamp"]
    assert abs(time.time() - int(ts)) < 300
    want = "v1=" + hmac.new(secret.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()
    assert want in h["Bambuflow-Webhook-Signature"].split(","), "签名校验失败"


def test_webhook_batch_completed(receiver: Any, subscribe: Any) -> None:
    url, got = receiver
    sub = subscribe(url, ["batch.completed", "batch.failed"])
    assert sub.signing_secret.startswith("whsec_")
    r = submit("alice", items(1))
    assert r.status_code == 200, r.text
    batch_id = r.json()["batch_id"]
    h, body = wait_event(got, "batch.completed", batch_id)
    check_signature(sub.signing_secret, h, body)
    evt = json.loads(body)
    # 事件体只有 ID、类型、时间与对象 ID，不带问答内容
    assert set(evt) == {"event_id", "type", "occurred_at", "object_id"}
    assert evt["event_id"].startswith("evt_") and h["Bambuflow-Webhook-Id"] == evt["event_id"]

    sid = sub.subscription.subscription_id
    deadline = time.monotonic() + 10
    while True:
        ds = (
            webhooks()
            .ListDeliveries(
                webhook_admin_pb2.ListDeliveriesRequest(subscription_id=sid), timeout=10
            )
            .deliveries
        )
        d = next(x for x in ds if x.event_id == evt["event_id"])
        if d.status == webhook_admin_pb2.DELIVERY_STATUS_DELIVERED or time.monotonic() > deadline:
            break
        time.sleep(0.5)
    assert d.status == webhook_admin_pb2.DELIVERY_STATUS_DELIVERED and d.last_status_code == 204

    # 手工重放沿用原事件 ID，接收方按它去重
    webhooks().Redeliver(
        webhook_admin_pb2.RedeliverRequest(subscription_id=sid, event_id=evt["event_id"]),
        timeout=10,
    )
    h2, _ = wait_event(got, "batch.completed", batch_id)
    assert h2["Bambuflow-Webhook-Id"] == evt["event_id"]


def test_webhook_index_version_live(receiver: Any, subscribe: Any) -> None:
    url, got = receiver
    sub = subscribe(url, ["index.version.live"])
    _, model, chunker = live_models()
    doc = "webhook-" + uuid.uuid4().hex[:8]
    write(doc, "Webhook 通知测试 " + doc, "2026-10-06T00:00:00Z", model, chunker)
    pub = admin().Publish(index_admin_pb2.PublishRequest(), timeout=30)
    h, body = wait_event(got, "index.version.live", pub.version)
    check_signature(sub.signing_secret, h, body)


def test_webhook_rejects_unknown_event() -> None:
    with pytest.raises(grpc.RpcError) as e:
        webhooks().CreateSubscription(
            webhook_admin_pb2.CreateSubscriptionRequest(
                tenant="credit", url="https://hooks.corp.example/x", event_types=["doc.deleted"]
            ),
            timeout=10,
        )
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
