"""A/B 实验登记与分组（P3-E1、E2，governance 详设 3.1、7.2，技术设计文档 10.5）：
平台管理员立项后实验立即以 5% 流量开始，并出现在 governance 下发给 orchestrator 的快照里；
流量只能按 5% → 20% → 50% 向上推进；停止后从快照中消失。同一租户同时只能有一个运行中的实验。

用风险租户（问答对所有成员关闭），实验不影响其他用例；用例结束前一定停止实验。
"""

import os
import time
import uuid
from typing import Any

import grpc
import httpx

from bambuflow.answer.v1 import answer_pb2, records_pb2, records_pb2_grpc
from bambuflow.config.v1 import config_pb2, config_pb2_grpc

from .admin import call
from .test_answers import ask
from .test_gateway import GATEWAY, token

PATH = "/v1/admin/experiments"
BODY: dict[str, Any] = {
    "tenant": "risk",
    "hypothesis": "支持度门槛调低后，有用反馈率上升",
    "primary_metric": "useful_rate",
    "baseline_rate": 0.6,
    "min_detectable_effect": 0.05,
    "overrides": {"support_threshold": 0.4},
}


def snapshot() -> config_pb2.GetSnapshotResponse:
    stub = config_pb2_grpc.ConfigServiceStub(grpc.insecure_channel(os.environ["BF_CONFIG_ADDR"]))
    resp: config_pb2.GetSnapshotResponse = stub.GetSnapshot(
        config_pb2.GetSnapshotRequest(service="orchestrator"), timeout=10
    )
    return resp


def running(tenant: str) -> list[config_pb2.RunningExperiment]:
    return [e for e in snapshot().experiments if e.tenant == tenant]


def stop_running(tenant: str) -> None:
    """上次运行中断时可能留下运行中的实验：先停掉。"""
    for e in running(tenant):
        r = call("paul", "POST", f"{PATH}/{e.exp_id}/stop", {"reason": "e2e 清理"})
        assert r.status_code == 200, r.text


def test_experiment_lifecycle_and_snapshot() -> None:
    stop_running("risk")
    before = snapshot().snapshot_version
    r = call("paul", "POST", PATH, BODY)
    assert r.status_code == 201, r.text
    exp = r.json()
    exp_id = exp["exp_id"]
    try:
        assert exp["status"] == "running" and exp["traffic_percent"] == 5
        assert 1400 < exp["sample_size_per_arm"] < 1550 and exp["created_by"] == "paul"

        (snap,) = running("risk")
        assert snap.exp_id == exp_id and snap.traffic_percent == 5
        assert snap.support_threshold == 0.4 and not snap.HasField("min_coverage")
        assert snapshot().snapshot_version != before

        # 同一租户只能有一个运行中的实验；只有平台管理员能立项
        assert call("paul", "POST", PATH, BODY).status_code == 409
        assert call("olga", "POST", PATH, BODY).status_code == 403

        r = call("paul", "POST", f"{PATH}/{exp_id}/ramp", {"traffic_percent": 20})
        assert r.status_code == 200 and r.json()["traffic_percent"] == 20, r.text
        assert running("risk")[0].traffic_percent == 20
        assert (
            call("paul", "POST", f"{PATH}/{exp_id}/ramp", {"traffic_percent": 20}).status_code
            == 409
        )

        got = call("paul", "GET", f"{PATH}/{exp_id}").json()
        assert got["traffic_percent"] == 20 and got["overrides"] == {"support_threshold": 0.4}
        listed = call("paul", "GET", PATH, params={"page_size": 200}).json()["experiments"]
        assert exp_id in {e["exp_id"] for e in listed}
    finally:
        r = call("paul", "POST", f"{PATH}/{exp_id}/stop", {"reason": "e2e 结束"})
    assert r.status_code == 200 and r.json()["status"] == "stopped", r.text
    assert r.json()["stopped_by"] == "paul" and r.json()["stop_reason"] == "e2e 结束"
    assert running("risk") == []
    # 已停止的再停原样返回
    again = call("paul", "POST", f"{PATH}/{exp_id}/stop", {"reason": "再停"})
    assert again.status_code == 200 and again.json()["stop_reason"] == "e2e 结束"


def test_invalid_experiments_are_rejected() -> None:
    patches: list[dict[str, Any]] = [
        {"primary_metric": "citation_click_rate"},
        {"overrides": {}},
        {"overrides": {"support_threshold": 0.95}},
        {"baseline_rate": 1.2},
    ]
    for patch in patches:
        r = call("paul", "POST", PATH, {**BODY, **patch})
        assert r.status_code == 400, (patch, r.text)
    assert call("paul", "GET", f"{PATH}/exp_000000000000000000000000").status_code == 404


def records(tenant: str, exp_id: str) -> list[records_pb2.AnswerRecord]:
    stub = records_pb2_grpc.AnswerRecordServiceStub(
        grpc.insecure_channel(os.environ["BF_ORCHESTRATOR_ADDR"])
    )
    now = time.time()
    req = records_pb2.ListAnswerRecordsRequest(tenant=tenant, experiment_id=exp_id)
    req.since.FromSeconds(int(now) - 3600)
    req.until.FromSeconds(int(now) + 60)
    resp: records_pb2.ListAnswerRecordsResponse = stub.ListAnswerRecords(req, timeout=10)
    return list(resp.records)


def test_orchestrator_assigns_users_and_exports_records() -> None:
    """P3-E2（orchestrator 详设 12.3）：运行中的实验 1 分钟内被 orchestrator 拉到；
    同一用户始终在同一组，分组写进 why 与回答记录，经导出接口（只给 evaluation）按实验读到，
    提问人只给假名。

    实验组的阈值取与基线相同的值，不改变信贷租户的回答，不影响同时运行的其他用例。"""
    stop_running("credit")
    body = {
        **BODY,
        "tenant": "credit",
        "overrides": {"support_threshold": 0.5, "min_coverage": 0.8},
    }
    r = call("paul", "POST", PATH, body)
    assert r.status_code == 201, r.text
    exp_id = r.json()["exp_id"]
    try:
        assert (
            call("paul", "POST", f"{PATH}/{exp_id}/ramp", {"traffic_percent": 50}).status_code
            == 200
        )
        first: dict[str, Any] = {}
        for _ in range(15):  # orchestrator 每 30 秒拉一次快照
            first = ask("alice", "credit", "个人消费贷款单户额度上限是多少？")
            if first["why"].get("experiment_id") == exp_id:
                break
            time.sleep(5)
        assert first["why"].get("experiment_id") == exp_id, first["why"]
        arm = first["why"]["experiment_arm"]
        assert arm in ("control", "treatment")
        second = ask("alice", "credit", "个人消费贷款的期限最长多久？")
        assert second["why"]["experiment_arm"] == arm
        fb = httpx.post(
            f"{GATEWAY}/v1/answers/{first['answer_id']}/feedback",
            json={"rating": "useful"},
            headers={
                "Authorization": "Bearer " + token("alice"),
                "Bambuflow-Tenant": "credit",
                "Idempotency-Key": uuid.uuid4().hex,
            },
            timeout=10,
        )
        assert fb.status_code in (200, 201), fb.text

        got = {x.answer_id: x for x in records("credit", exp_id)}
        a, b = got[first["answer_id"]], got[second["answer_id"]]
        assert a.user_key == b.user_key and "alice" not in a.user_key
        assert answer_pb2.ExperimentArm.Name(a.experiment_arm) == "EXPERIMENT_ARM_" + arm.upper()
        assert list(a.feedback) == ["useful"] and list(b.feedback) == []
        assert a.latency_ms > 0 and a.usage.prompt_tokens > 0
        # 默认不带正文
        assert not a.HasField("answer")

        # P3-E3：evaluation 的 A/B 分析（evaluation 详设 7.2）。
        # alice 只在一组；样本远不够，只有进度、没有结论
        r = call("paul", "GET", f"{PATH}/{exp_id}/analysis")
        assert r.status_code == 200, r.text
        an = r.json()
        mine = an[arm]
        assert mine["users"] >= 1 and mine["answers"] >= 2 and mine["metric_users"] >= 1
        # 进度按两组中较少的一组算
        assert 0 <= an["progress"] < 0.01 and "conclusion" not in an
        assert {g["name"] for g in an["guardrails"]} == {
            "refused_rate",
            "p95_latency_ms",
            "mean_tokens",
        }
        assert not any(g["breached"] for g in an["guardrails"])
        assert call("audra", "GET", f"{PATH}/{exp_id}/analysis").status_code == 403
    finally:
        call("paul", "POST", f"{PATH}/{exp_id}/stop", {"reason": "e2e 结束"})
