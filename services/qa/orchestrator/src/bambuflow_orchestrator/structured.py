"""结构化数据实时查询 sql_query（总体设计 5.2、6.2、6.4，orchestrator 详设 6.3）。

问业务数据（不是文档数量）的问题由 agent 调用只读 sql_query：
数据平台的只读查询服务经端口（QueryService）接入，外部团队给出服务后只换实现（总体设计 ADR-13）。
默认实现 PGQueryService 直连只读账号：

- 只允许白名单视图（目录 Catalog，部署配置）；参数按 schema 给出（视图、度量、条件、分组、上限），
  不接受原始 SQL，列名逐个对照目录，值一律走参数绑定。
- 以调用者身份执行：按调用者的群组映射到数据库角色，在只读事务里 SET LOCAL ROLE 后查询，
  能看到哪些视图、哪些行由数据库的授权与视图定义决定；登录账号本身不继承这些角色的权限。
  调用者没有能读该视图的角色时拒绝（fail-closed）。
- 单次 1 秒超时（statement_timeout），结果最多 50 行。

结果转成带引用的证据（source_type = db）：引用信息是视图名与查询时间，句子仍要过逐句校验。
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import psycopg
import yaml
from psycopg import sql

from bambuflow.common.v1 import types_pb2
from bambuflow.identity.v1 import principal_pb2
from bambuflow.retrieval.v1 import retrieval_pb2

TOOL = "sql_query"
MAX_ROWS = 50
TIMEOUT_MS = 1000
AGGREGATES = ("count", "sum", "avg", "min", "max")
OPERATORS = ("=", "!=", ">", ">=", "<", "<=", "in", "between")
TYPES = ("text", "number", "date")
# 问业务数据的数量、统计、明细（总体设计 6.2）；与视图的业务名称同时出现才算结构化问题
_STATISTIC = re.compile(r"多少|几笔|几条|合计|总共|总额|平均|最高|最低|统计|明细|笔数")
_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}\.[a-z_][a-z0-9_]{0,62}$")
_COLUMN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
CLASSIFICATIONS = {
    "public": types_pb2.CLASSIFICATION_PUBLIC,
    "internal": types_pb2.CLASSIFICATION_INTERNAL,
    "confidential": types_pb2.CLASSIFICATION_CONFIDENTIAL,
    "restricted": types_pb2.CLASSIFICATION_RESTRICTED,
}


class QueryError(ValueError):
    """参数不合法：视图不在白名单、列不存在、操作符或值不对。"""


class QueryDenied(Exception):
    """调用者没有能读该视图的数据库角色。"""


class QueryUnavailable(Exception):
    """查询服务不可用或超时。"""


@dataclass(frozen=True)
class Column:
    name: str
    label: str
    type: str


@dataclass(frozen=True)
class View:
    name: str
    title: str
    description: str
    classification: types_pb2.Classification
    columns: Mapping[str, Column]
    # 问题里出现这些说法时，认为在问这个视图的业务数据
    terms: tuple[str, ...]


@dataclass(frozen=True)
class Catalog:
    views: Mapping[str, View]
    # 数据库角色 → 权限 token（group:…、user:…）；调用者带其中任一 token 即可使用该角色
    roles: Mapping[str, tuple[str, ...]]
    # 开放 sql_query 的租户（工具列表按租户裁剪，orchestrator 详设 6.3）
    tenants: frozenset[str]

    @staticmethod
    def load(path: str) -> Catalog:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        views: dict[str, View] = {}
        for v in raw.get("views") or []:
            name = str(v["name"])
            if not _NAME.match(name):
                raise ValueError(f"视图名不合法: {name}")
            cols: dict[str, Column] = {}
            for c in v.get("columns") or []:
                cname, ctype = str(c["name"]), str(c.get("type", "text"))
                if not _COLUMN.match(cname) or ctype not in TYPES:
                    raise ValueError(f"{name} 的列 {cname} 不合法")
                cols[cname] = Column(cname, str(c.get("label") or cname), ctype)
            if not cols:
                raise ValueError(f"{name} 没有列")
            cls = CLASSIFICATIONS.get(str(v.get("classification", "internal")))
            if cls is None:
                raise ValueError(f"{name} 的密级不合法")
            views[name] = View(
                name,
                str(v.get("title") or name),
                str(v.get("description") or ""),
                cls,
                cols,
                tuple(str(t) for t in v.get("terms") or []),
            )
        roles = {str(r): tuple(str(t) for t in ts) for r, ts in (raw.get("roles") or {}).items()}
        for r in roles:
            if not _COLUMN.match(r):
                raise ValueError(f"角色名不合法: {r}")
        return Catalog(views, roles, frozenset(str(t) for t in raw.get("tenants") or []))

    def enabled(self, tenant: str) -> bool:
        return tenant in self.tenants and bool(self.views)

    def matches(self, question: str) -> bool:
        """问题同时提到某个视图的业务说法与统计类词语：按结构化问题处理。"""
        if not _STATISTIC.search(question):
            return False
        return any(t in question for v in self.views.values() for t in (v.title, *v.terms))

    def describe(self) -> str:
        """给规划器看的目录：视图名、业务名称、说明与列，不含任何数据。"""
        lines = []
        for v in self.views.values():
            cols = "、".join(f"{c.name}（{c.label}，{c.type}）" for c in v.columns.values())
            lines.append(f"{v.name}：{v.title}。{v.description} 列：{cols}")
        return "\n".join(lines)

    def roles_for(self, tokens: Sequence[str]) -> list[str]:
        have = set(tokens)
        return sorted(r for r, ts in self.roles.items() if have & set(ts))


@dataclass(frozen=True)
class Measure:
    agg: str
    column: str  # count 时可为空

    @property
    def alias(self) -> str:
        return self.agg if not self.column else f"{self.agg}_{self.column}"


@dataclass(frozen=True)
class Filter:
    column: str
    op: str
    value: Any


@dataclass(frozen=True)
class Query:
    view: View
    measures: tuple[Measure, ...]
    columns: tuple[str, ...]
    filters: tuple[Filter, ...]
    group_by: tuple[str, ...]
    limit: int

    def key(self) -> str:
        """同一查询的稳定摘要，用作证据块 ID 的一部分。"""
        raw = repr((self.view.name, self.measures, self.columns, self.filters, self.group_by))
        return hashlib.sha256(f"{raw}|{self.limit}".encode()).hexdigest()[:16]


def _value(col: Column, v: Any) -> Any:
    if col.type == "number":
        if isinstance(v, bool) or not isinstance(v, int | float):
            raise QueryError(f"{col.name} 需要数字")
        return v
    if not isinstance(v, str) or not v or len(v) > 200:
        raise QueryError(f"{col.name} 的值不合法")
    if col.type == "date":
        try:
            return dt.date.fromisoformat(v)
        except ValueError as e:
            raise QueryError(f"{col.name} 需要 YYYY-MM-DD") from e
    return v


def parse(args: Mapping[str, Any], catalog: Catalog) -> Query:
    """按 schema 校验规划器给出的参数；任何不认识的视图、列、操作符都拒绝。"""
    view = catalog.views.get(str(args.get("view", "")))
    if view is None:
        raise QueryError("视图不在白名单里")

    def column(name: Any) -> Column:
        c = view.columns.get(str(name))
        if c is None:
            raise QueryError(f"{view.name} 没有列 {name}")
        return c

    measures: list[Measure] = []
    for m in args.get("measures") or []:
        if not isinstance(m, Mapping) or m.get("agg") not in AGGREGATES:
            raise QueryError("度量不合法")
        col = str(m.get("column") or "")
        if m["agg"] != "count" or col:
            c = column(col)
            if m["agg"] in ("sum", "avg") and c.type != "number":
                raise QueryError(f"{c.name} 不是数字列")
        measures.append(Measure(str(m["agg"]), col))
    columns = tuple(column(c).name for c in args.get("columns") or [])
    group_by = tuple(column(c).name for c in args.get("group_by") or [])
    if not measures and not columns:
        raise QueryError("至少要有一个度量或列")
    if columns and (measures or group_by):
        raise QueryError("明细查询不能同时带度量或分组")
    filters: list[Filter] = []
    for f in args.get("filters") or []:
        if not isinstance(f, Mapping) or f.get("op") not in OPERATORS:
            raise QueryError("条件不合法")
        c = column(f.get("column"))
        raw = f.get("value")
        if f["op"] in ("in", "between"):
            if not isinstance(raw, list) or not raw or len(raw) > 20:
                raise QueryError("in 与 between 需要列表")
            if f["op"] == "between" and len(raw) != 2:
                raise QueryError("between 需要两个值")
            value: Any = [_value(c, x) for x in raw]
        else:
            value = _value(c, raw)
        filters.append(Filter(c.name, str(f["op"]), value))
    limit = args.get("limit", MAX_ROWS)
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_ROWS:
        raise QueryError(f"limit 应为 1–{MAX_ROWS}")
    return Query(view, tuple(measures), columns, tuple(filters), group_by, limit)


def build(q: Query) -> tuple[sql.Composed, list[Any]]:
    """拼 SQL：标识符一律经 sql.Identifier，值一律参数绑定。"""
    schema, name = q.view.name.split(".")
    select: list[sql.Composable] = [sql.Identifier(c) for c in (*q.group_by, *q.columns)]
    for m in q.measures:
        expr = (
            sql.SQL("count(*)")
            if m.agg == "count" and not m.column
            else sql.SQL("{}({})").format(sql.SQL(m.agg), sql.Identifier(m.column))
        )
        select.append(sql.SQL("{} AS {}").format(expr, sql.Identifier(m.alias)))
    params: list[Any] = []
    where: list[sql.Composable] = []
    for f in q.filters:
        col = sql.Identifier(f.column)
        if f.op == "in":
            where.append(sql.SQL("{} = ANY(%s)").format(col))
            params.append(f.value)
        elif f.op == "between":
            where.append(sql.SQL("{} BETWEEN %s AND %s").format(col))
            params += f.value
        else:
            where.append(sql.SQL("{} {} %s").format(col, sql.SQL(f.op)))
            params.append(f.value)
    stmt = sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(", ").join(select), sql.Identifier(schema, name)
    )
    if where:
        stmt += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where)
    if q.group_by:
        stmt += sql.SQL(" GROUP BY ") + sql.SQL(", ").join(sql.Identifier(c) for c in q.group_by)
    if q.measures and q.group_by:
        stmt += sql.SQL(" ORDER BY {} DESC").format(sql.Identifier(q.measures[0].alias))
    stmt += sql.SQL(" LIMIT %s")
    params.append(q.limit)
    return stmt, params


@dataclass(frozen=True)
class Result:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]
    at: dt.datetime


class QueryService(Protocol):
    """数据平台的只读查询服务（端口）。tokens 是调用者的权限 token，决定以哪个数据库角色执行。"""

    async def run(self, q: Query, tokens: Sequence[str], timeout: float) -> Result: ...


def tokens_of(p: principal_pb2.Principal) -> list[str]:
    """调用者的权限 token：本人与所在群组（与入库时的 ACL token 写法一致）。"""
    return [f"user:{p.sub}", *(f"group:{g}" for g in p.groups)]


class PGQueryService:
    """默认实现：只读账号直连，按调用者映射的数据库角色执行。

    每次查询单独连接（查询量小，免去连接池状态）。"""

    def __init__(self, dsn: str, catalog: Catalog) -> None:
        self._dsn, self._catalog = dsn, catalog

    async def run(self, q: Query, tokens: Sequence[str], timeout: float) -> Result:
        candidates = self._catalog.roles_for(tokens)
        if not candidates:
            raise QueryDenied
        stmt, params = build(q)
        try:
            async with await psycopg.AsyncConnection.connect(
                self._dsn, connect_timeout=max(1, int(timeout))
            ) as conn:
                async with conn.transaction():
                    await conn.execute("SET TRANSACTION READ ONLY")
                    await conn.execute(
                        sql.SQL("SET LOCAL statement_timeout = {}").format(
                            sql.Literal(min(TIMEOUT_MS, int(timeout * 1000)))
                        )
                    )
                    # 先在系统目录里按名字找到视图的 OID 再判断权限：
                    # 直接把 'schema.view' 交给 has_table_privilege 时要解析名字，
                    # 需要登录账号对 schema 有 USAGE，而它本身没有任何权限
                    schema, name = q.view.name.split(".")
                    cur = await conn.execute(
                        "SELECT r FROM unnest(%s::text[]) AS r, pg_class c "
                        "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE n.nspname = %s AND c.relname = %s "
                        "AND has_schema_privilege(r, n.oid, 'USAGE') "
                        "AND has_table_privilege(r, c.oid, 'SELECT') ORDER BY r LIMIT 1",
                        (candidates, schema, name),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise QueryDenied
                    await conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(row[0])))
                    cur = await conn.execute(stmt, params)
                    rows = await cur.fetchall()
                    cols = tuple(d.name for d in cur.description or ())
        except psycopg.Error as e:
            raise QueryUnavailable(type(e).__name__) from e
        return Result(cols, tuple(tuple(r) for r in rows), dt.datetime.now(dt.UTC))


def _fmt(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:.2f}".rstrip("0").rstrip(".")
    return str(v)


def _label(q: Query, col: str) -> str:
    for m in q.measures:
        if m.alias == col:
            base = q.view.columns[m.column].label if m.column else "记录数"
            names = {
                "count": base if not m.column else f"{base}计数",
                "sum": f"{base}合计",
                "avg": f"{base}平均值",
                "min": f"{base}最小值",
                "max": f"{base}最大值",
            }
            return names[m.agg]
    return q.view.columns[col].label


def _conditions(q: Query) -> str:
    out = []
    for f in q.filters:
        label = q.view.columns[f.column].label
        if f.op == "between":
            out.append(f"{label}在 {_fmt(f.value[0])} 至 {_fmt(f.value[1])} 之间")
        elif f.op == "in":
            out.append(f"{label}为 {'、'.join(_fmt(x) for x in f.value)}")
        else:
            out.append(f"{label} {f.op} {_fmt(f.value)}")
    return "，".join(out)


def evidence(q: Query, r: Result) -> retrieval_pb2.Hit:
    """查询结果转成一条证据。第一句是结果本身（便于引用），第二句写明来源与查询时间。"""
    cond = _conditions(q)
    prefix = f"按{cond}" if cond else "不加条件"
    if not r.rows:
        body = f"{prefix}查询{q.view.title}，没有符合条件的记录。"
    elif len(r.rows) == 1 and not q.group_by and not q.columns:
        parts = [f"{_label(q, c)}为 {_fmt(v)}" for c, v in zip(r.columns, r.rows[0], strict=True)]
        body = f"{prefix}查询{q.view.title}，{'，'.join(parts)}。"
    else:
        items = [
            "，".join(f"{_label(q, c)} {_fmt(v)}" for c, v in zip(r.columns, row, strict=True))
            for row in r.rows
        ]
        body = f"{prefix}查询{q.view.title}，共 {len(r.rows)} 条：{'；'.join(items)}。"
    at = r.at.strftime("%Y-%m-%d %H:%M:%S UTC")
    text = f"{body}\n数据来源：{q.view.title}（{q.view.name}），查询时间 {at}。"
    return retrieval_pb2.Hit(
        chunk_id=f"db:{q.view.name}#{q.key()}",
        doc_id=f"db:{q.view.name}",
        text=text,
        title=q.view.title,
        source_uri=f"db://{q.view.name}",
        source_type=types_pb2.SOURCE_TYPE_DB,
        owner="数据平台",
        classification=q.view.classification,
        # 查询结果就是答案的依据，不参与精排；给满分以免被精排分下限过滤
        score=1.0,
    )
