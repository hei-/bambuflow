"""单跳问题的查询理解（orchestrator 详设 5.1、5.2）。

- 指代消解与改写：会话里有前几轮问题时，用一次模型调用把追问（“二套住房呢？”）
  改写成能单独检索的问题；输入还有会话摘要，问题里指代上一轮的文档（“上面那个制度”）时
  再给上一轮所引文档的标题（按调用者当前的权限取，详设 10.1）。
  结果按“租户 + 问题 + 前几轮问题 + 摘要 + 文档标题 + 模型 + 提示词版本”缓存 24 小时。
  超时、失败或输出不像问题时用原问题。
  首轮问题不改写：单跳的时间预算紧，口语化的首轮问题交给混合检索与精排。
- 同义扩展：问题里的同义词、旧称换成受控词表的规范名称，作为第二个子查询
  （retrieval 按子查询分别召回后合并）。
- 过滤推断：只取“2025 年”“去年”这类明确的时间说法与“现行”“已废止”这类效力说法，
  转成生效日期与效力状态条件；
  这两类规则的准确度高，相当于设计要求的置信度 ≥ 0.8。部门、文档类型、主题容易误判，单跳不推断
  （清单类问题另有完整的条件抽取，listing.conditions）。推断的条件写进 why.inferred_filters，
  检索没有结果时去掉推断条件再检索一次（详设 5.2）。

意图分类仍是规则（intent.py）：设计待决问题“小模型还是规则优先”还没定。
"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from bambuflow_vocabulary import Vocabularies

from bambuflow.common.v1 import types_pb2
from bambuflow_orchestrator import listing
from bambuflow_orchestrator.llm import LLMUnavailableError
from bambuflow_orchestrator.session import StoreUnavailableError

_log = logging.getLogger(__name__)

PROMPT_VERSION = "rewrite@2"
CACHE_TTL = 24 * 3600
# 改写的时间上限（秒）：单跳总预算 4 秒，改写超时就用原问题
REWRITE_SECONDS = 1.2
# 改写只看最近 3 轮问题（详设 10.1）
RECENT = 3
# 指代上一轮所引文档的说法：“上面那个制度”“刚才的文件”“该规定”
REFERS_BACK = re.compile(
    r"(上面|上述|上文|刚才|前面|之前|那个|这个|该|此)(的)?(那份|那个|这份|这个)?"
    r"(制度|文件|文档|规定|办法|通知|条款|材料)"
)

SYSTEM = """你把用户在多轮对话里的追问改写成一个能单独理解的完整问题，供检索使用。
规则：
1. 补全追问里省略或指代的对象（“它”“这个制度”“那二套呢”），对象取自前几轮问题、对话摘要
   或上一轮引用的文档标题；
2. 不回答问题，不添加前几轮问题与对话摘要里没有的条件，不改变问题要问的事；
3. 追问本身已经完整时原样输出；
4. 只输出改写后的问题一行，不要解释。"""


class Cache(Protocol):
    """会话所在的 Redis（session.Store）；不可用时抛 StoreUnavailableError，
    改写照做、只是不缓存。"""

    async def get(self, key: str) -> bytes | None: ...

    async def set(self, key: str, value: bytes, ttl: int) -> None: ...


class Rewriter(Protocol):
    """一次短的模型调用：返回模型输出的全部文字。
    不可用时抛 LLMUnavailableError，由调用方退回原问题。"""

    model: str

    async def complete(self, system: str, user: str, timeout: float) -> str: ...


@dataclass(frozen=True)
class Understood:
    # 检索用的子查询：第一个是（改写后的）问题，其后是同义扩展
    queries: tuple[str, ...]
    # 推断出的条件；没有时为 None
    inferred: types_pb2.Filters | None
    rewritten: bool = False


class Understander:
    def __init__(
        self,
        vocab: Vocabularies,
        today: dt.date,
        recent: Sequence[str] = (),
        rewriter: Rewriter | None = None,
        cache: Cache | None = None,
        tenant: str = "",
        remaining: Callable[[], float] = lambda: float("inf"),
        summary: str = "",
        references: Callable[[], Awaitable[Sequence[str]]] | None = None,
    ) -> None:
        self._vocab, self._today = vocab, today
        # 会话摘要；references 取上一轮所引文档的标题，只在问题指代上一轮的文档时调用
        self._summary, self._references = summary, references
        # 这次回答还剩多少时间（秒）：不够改写时直接用原问题
        self._remaining = remaining
        self._recent = list(recent)[-RECENT:]
        self._rewriter, self._cache, self._tenant = rewriter, cache, tenant

    async def run(self, question: str) -> Understood:
        query, rewritten = question, False
        # 改写之后还要检索、生成与校验：剩余时间不到改写上限的 2 倍就不改写
        rw = self._rewriter
        if self._recent and rw is not None and self._remaining() > 2 * REWRITE_SECONDS:
            titles: Sequence[str] = ()
            if self._references is not None and REFERS_BACK.search(question):
                titles = await self._references()
            query = await self._rewrite(question, rw, list(titles))
            rewritten = query != question
        queries = [query]
        expanded = self._canonical(query)
        if expanded != query:
            queries.append(expanded)
        return Understood(tuple(queries), infer(query, self._vocab, self._today), rewritten)

    def _canonical(self, text: str) -> str:
        v = self._vocab
        return v.topic.canonical(v.dept.canonical(v.doc_type.canonical(text)))

    async def _rewrite(self, question: str, rw: Rewriter, titles: list[str]) -> str:
        key = self._key(question, rw.model, titles)
        if self._cache is not None:
            try:
                hit = await self._cache.get(key)
            except StoreUnavailableError:
                hit = None
            if hit:
                return hit.decode()
        parts = ["前几轮问题：\n" + "\n".join(f"- {q}" for q in self._recent)]
        if self._summary:
            parts.append(f"对话摘要：{self._summary}")
        if titles:
            parts.append("上一轮引用的文档：" + "、".join(f"《{t}》" for t in titles))
        user = "\n\n".join([*parts, f"追问：{question}"])
        try:
            out = await rw.complete(SYSTEM, user, REWRITE_SECONDS)
        except (LLMUnavailableError, TimeoutError, OSError) as e:
            # 改写失败不影响问答：退回原问题（详设 5.1）
            _log.warning("查询改写失败，用原问题", extra={"err": type(e).__name__})
            return question
        got = plausible(question, out)
        if self._cache is not None and got != question:
            try:
                await self._cache.set(key, got.encode(), CACHE_TTL)
            except StoreUnavailableError:
                pass
        return got

    def _key(self, question: str, model: str, titles: list[str]) -> str:
        raw = "\x00".join(
            [
                self._tenant,
                question,
                *self._recent,
                self._summary,
                *titles,
                model,
                PROMPT_VERSION,
            ]
        )
        return "orch:rewrite:" + hashlib.sha256(raw.encode()).hexdigest()


def plausible(question: str, output: str) -> str:
    """模型输出不可信：取第一行非空文字，太长、太短或像在回答问题时用原问题。"""
    line = next((x.strip() for x in output.splitlines() if x.strip()), "")
    line = line.strip("“”\"' ").removeprefix("改写后的问题：").removeprefix("问题：").strip()
    if not line or len(line) > max(80, 3 * len(question)) or len(line) < 2:
        return question
    return line


def infer(question: str, vocab: Vocabularies, today: dt.date) -> types_pb2.Filters | None:
    """只取时间与效力两类条件（模块说明）。"""
    full, _ = listing.conditions(question, vocab, today)
    f = types_pb2.Filters(
        effective_from=full.effective_from,
        effective_to=full.effective_to,
        validity_statuses=full.validity_statuses,
    )
    return f if (f.effective_from or f.effective_to or f.validity_statuses) else None


def merge(caller: types_pb2.Filters | None, inferred: types_pb2.Filters) -> types_pb2.Filters:
    """调用方给的条件优先，推断的补在调用方没给的字段上；两者都只收窄范围。"""
    return listing.merge(caller, inferred)
