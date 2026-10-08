"""orchestrator schema 的表结构变更（python -m bambuflow_orchestrator migrate）。

技术设计文档 12.8：升级前以 Job 执行。

语义与 libs/platform-go/pgmigrate 相同，两边共用进度表与锁，不能各自为政：
- 迁移文件 sql/NNNN_说明.sql 随包发布，按序号执行，序号重复报错；
- 进度记在 public.schema_migrations（service = orchestrator）；
- 所有服务共用一把会话级咨询锁 hashtext('pgmigrate')，串行执行；
- 每个文件一个事务，连同进度记录一起提交。
Go 的实现不能直接用：orchestrator 的镜像里只有 Python。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from importlib import resources

import psycopg

SERVICE = "orchestrator"
_LOCK = "pgmigrate"
_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Step:
    version: int
    name: str
    sql: str


def steps(files: dict[str, str]) -> list[Step]:
    """按序号排序；文件名不以序号开头或序号重复时报错。"""
    out = []
    for name, sql in files.items():
        head = name.split("_", 1)[0]
        if not head.isdigit():
            raise ValueError(f"迁移文件名应以序号开头: {name}")
        out.append(Step(int(head), name, sql))
    out.sort(key=lambda s: s.version)
    for a, b in zip(out, out[1:], strict=False):
        if a.version == b.version:
            raise ValueError(f"迁移序号重复: {a.name} 与 {b.name}")
    return out


def bundled() -> dict[str, str]:
    root = resources.files("bambuflow_orchestrator") / "sql"
    return {
        f.name: f.read_text(encoding="utf-8")
        for f in root.iterdir()
        if f.is_file() and f.name.endswith(".sql")
    }


async def up(dsn: str) -> list[str]:
    """执行尚未执行过的迁移，返回本次执行的文件名。"""
    all_steps = steps(bundled())
    applied: list[str] = []
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (_LOCK,))
        try:
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS public.schema_migrations (
                     service text NOT NULL, version int NOT NULL, name text NOT NULL,
                     applied_at timestamptz NOT NULL DEFAULT now(),
                     PRIMARY KEY (service, version))"""
            )
            for s in all_steps:
                cur = await conn.execute(
                    "SELECT 1 FROM public.schema_migrations WHERE service = %s AND version = %s",
                    (SERVICE, s.version),
                )
                if await cur.fetchone() is not None:
                    continue
                async with conn.transaction():
                    # 迁移文件是随包发布的常量，可能含多条语句，只能以不带参数的方式执行
                    await conn.execute(s.sql.encode())
                    await conn.execute(
                        "INSERT INTO public.schema_migrations (service, version, name) "
                        "VALUES (%s, %s, %s)",
                        (SERVICE, s.version, s.name),
                    )
                applied.append(s.name)
                _log.info("已执行迁移", extra={"migration": s.name})
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (_LOCK,))
    return applied
