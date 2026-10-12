"""配置快照的分发情况（governance 详设 8.2）：
平台管理员与平台运维能看各服务当前版本与各实例加载到哪一版。
"""

from .admin import call

PATH = "/v1/admin/snapshot-sync"


def test_snapshot_sync_lists_services_and_instances() -> None:
    for user in ("paul", "olga"):
        r = call(user, "GET", PATH)
        assert r.status_code == 200, r.text
        services = {s["service"]: s for s in r.json()["services"]}
        # gateway 与 retrieval 每次刷新都上报，环境起来后应已有实例
        assert services["gateway"]["instances"] and services["retrieval"]["instances"], services
    assert call("audra", "GET", PATH).status_code == 403
