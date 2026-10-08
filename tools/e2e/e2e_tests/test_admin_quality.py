"""在线质量趋势（P3-E4，evaluation 详设 7.1、8.2）：经 gateway 转给 evaluation，只有平台管理员能看。

抽样评审本身是每天的 CronJob（开发环境按需 docker compose run evaluation-api online-sample），
这里不跑它：新环境里还没有结果，只核对接口与权限；抽样、判定与趋势告警由单元测试覆盖。
"""

from .admin import call

PATH = "/v1/admin/quality-trends"


def test_quality_trends_for_platform_admin() -> None:
    r = call("paul", "GET", PATH, params={"tenant": "credit", "days": 7})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["tenant"] == "credit" and isinstance(body["days"], list)
    assert body["declining"] is False
    assert call("paul", "GET", PATH, params={"tenant": "Credit"}).status_code == 400
    assert call("olga", "GET", PATH, params={"tenant": "credit"}).status_code == 403
