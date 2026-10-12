"""按改动路径算受影响的租户（evaluation 详设 5.1：代码改动按路径映射）。

规则偏保守：拿不准的路径一律算影响全部租户，宁可多跑也不漏跑。
- 只改文档、Helm chart 与它的冒烟测试：不影响回答，不跑业务评测（权限评测集照常全量跑）；
- 租户自己的配置目录：只影响该租户；
- 其余（services/、libs/、contracts/、config/policies/、评测集与基线、CI 本身等）：影响全部租户。
"""

from __future__ import annotations

from collections.abc import Iterable

NO_EFFECT = ("docs/", "deploy/helm/", "tools/helm-smoke/")
# 租户配置目录：开发与 CI 用 tools/test-upstream/tenants，governance 分发后为 config/tenants
TENANT_DIRS = ("tools/test-upstream/tenants/", "config/tenants/")


def affected(paths: Iterable[str], tenants: set[str]) -> set[str]:
    out: set[str] = set()
    for p in paths:
        if p.endswith(".md") or p.startswith(NO_EFFECT):
            continue
        tenant_dir = next((d for d in TENANT_DIRS if p.startswith(d)), None)
        if tenant_dir is None:
            return set(tenants)
        t = p[len(tenant_dir) :].split("/", 1)[0]
        if t in tenants:
            out.add(t)
    return out
