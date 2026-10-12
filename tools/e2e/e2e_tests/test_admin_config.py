"""租户配置的版本、变更单与发布（Z13、Z14，governance 详设 3、4、7.2）：
部署目录里的 tenant.yaml 已作为初始版本导入；变更单的作者不能审批，
另一位平台管理员审批后立即生效并推给 gateway；
回退切回上一版本；生效版本已经变了的变更单作废。
"""

import uuid
from typing import Any

from .admin import call

TENANT = "ops"


def versions() -> list[dict[str, Any]]:
    r = call("paul", "GET", f"/v1/admin/tenants/{TENANT}/config-versions")
    assert r.status_code == 200, r.text
    vs: list[dict[str, Any]] = r.json()["versions"]
    return vs


def active() -> dict[str, Any]:
    (v,) = [v for v in versions() if v["active"]]
    return v


def test_tenants_are_bootstrapped_from_deployment_files() -> None:
    r = call("paul", "GET", "/v1/admin/tenants")
    assert r.status_code == 200, r.text
    tenants = {t["tenant"]: t for t in r.json()["tenants"]}
    assert {"credit", "ops", "risk"} <= set(tenants)
    assert tenants["credit"]["rollout"] == "general" and tenants["credit"]["active"]["content"]
    # 只有平台管理员能看
    assert call("olga", "GET", "/v1/admin/tenants").status_code == 403


def test_change_request_approval_and_rollback() -> None:
    before = active()
    # 只加一行注释：内容变了、配置的含义不变，不影响其他用例
    content = before["content"].rstrip("\n") + f"\n# e2e {uuid.uuid4().hex[:8]}\n"
    body = {
        "object": "tenant",
        "scope": TENANT,
        "base_version_id": before["version_id"],
        "content": content,
        "reason": "e2e 变更单",
    }
    created = call("paul", "POST", "/v1/admin/change-requests", body)
    assert created.status_code == 201, created.text
    chg = created.json()
    assert chg["status"] == "open" and chg["version"]["number"] == before["number"] + 1

    approve = f"/v1/admin/change-requests/{chg['chg_id']}/approve"
    selfie = call("paul", "POST", approve)
    assert selfie.status_code == 403 and selfie.json()["type"] == "self_approval_forbidden"
    done = call("pete", "POST", approve)
    assert done.status_code == 200, done.text
    assert done.json()["status"] == "approved" and done.json()["release_id"].startswith("rel_")
    assert active()["version_id"] == chg["version"]["version_id"]

    # 基于旧版本的变更单：生效版本已经变了
    stale = call(
        "paul", "POST", "/v1/admin/change-requests", body | {"content": content + "# again\n"}
    )
    assert stale.status_code == 409 and stale.json()["type"] == "stale_base_version"

    rel = done.json()["release_id"]
    got = call("paul", "GET", f"/v1/admin/releases/{rel}")
    assert (
        got.status_code == 200
        and got.json()["status"] == "active"
        and got.json()["source"] == "change_request"
    )
    back = call("paul", "POST", f"/v1/admin/releases/{rel}/rollback", {"reason": "e2e 回退"})
    assert back.status_code == 200, back.text
    assert back.json()["source"] == "rollback" and active()["version_id"] == before["version_id"]
    assert call("paul", "GET", f"/v1/admin/releases/{rel}").json()["status"] == "rolled_back"


def test_new_tenant_directly_general_needs_gate() -> None:
    name = "e2e-" + uuid.uuid4().hex[:6]
    r = call(
        "paul",
        "POST",
        "/v1/admin/tenants",
        {"tenant": name, "content": f"tenant: {name}\nrollout: general\n", "reason": "e2e 新租户"},
    )
    assert r.status_code == 409 and r.json()["type"] == "gate_failed", r.text
    bad = call(
        "paul",
        "POST",
        "/v1/admin/tenants",
        {"tenant": name, "content": "tenant: other\nrollout: pilot\n", "reason": "e2e"},
    )
    assert bad.status_code == 400, bad.text


def test_switching_to_general_needs_golden_set_ready() -> None:
    """第二个业务系统（risk，试点阶段）切到全量：门禁通过还不够，
    门禁里 risk 的评测集要满足全量要求（≥ 300 道已审核题、双人标注 ≥ 150 等，evaluation 详设 3.1）。
    样例评测集的题都没审核，所以被拒绝并说明差什么。"""
    gates = call("paul", "GET", "/v1/admin/eval-gates", params={"tenant": "risk", "page_size": 50})
    assert gates.status_code == 200, gates.text
    passed = [
        g for g in gates.json()["gates"] if g["conclusion"] == "passed" and "risk" in g["tenants"]
    ]
    assert passed, "make ingest 的发布门禁应已覆盖 risk"
    ready = {r["tenant"]: r for r in passed[0]["readiness"]}
    assert ready["risk"]["stage"] == "none" and ready["risk"]["unreviewed"] > 0, ready
    assert any("至少 100" in g for g in ready["risk"]["gaps"])

    r = call("paul", "GET", "/v1/admin/tenants/risk/config-versions")
    (cur,) = [v for v in r.json()["versions"] if v["active"]]
    content = cur["content"].replace("rollout: pilot", "rollout: general")
    body = {
        "object": "tenant",
        "scope": "risk",
        "base_version_id": cur["version_id"],
        "content": content,
        "reason": "e2e 转全量",
        "gate_id": passed[0]["gate_id"],
    }
    created = call("paul", "POST", "/v1/admin/change-requests", body)
    assert created.status_code == 409 and created.json()["type"] == "gate_failed", created.text


def test_tenant_owner_reads_only_own_tenant() -> None:
    # owen 是 credit 租户的负责人（租户配置的 owner_groups），不是成员，也没有平台角色
    r = call("owen", "GET", "/v1/admin/tenants")
    assert r.status_code == 200, r.text
    assert [t["tenant"] for t in r.json()["tenants"]] == ["credit"]
    assert call("owen", "GET", "/v1/admin/tenants/credit/config-versions").status_code == 200
    assert call("owen", "GET", f"/v1/admin/tenants/{TENANT}/config-versions").status_code == 403
    assert call("owen", "GET", "/v1/admin/eval-gates", params={"tenant": TENANT}).status_code == 403
    assert call("owen", "GET", "/v1/admin/eval-gates").status_code == 200
    body = {"object": "tenant", "scope": "credit", "content": "x", "reason": "越权"}
    assert call("owen", "POST", "/v1/admin/change-requests", body).status_code == 403
