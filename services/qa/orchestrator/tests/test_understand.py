import asyncio
import datetime as dt
from collections.abc import Sequence

from bambuflow_vocabulary import Term, Vocabularies, Vocabulary

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_grounding import Grounder
from bambuflow_orchestrator import flow, understand
from bambuflow_orchestrator.llm import LLMUnavailableError
from bambuflow_orchestrator.session import StoreUnavailableError
from bambuflow_orchestrator.understand import Understander, infer, plausible

VOCAB = Vocabularies(
    "vocab-test",
    Vocabulary([Term("DT-POLICY", ("管理办法", "办法", "制度"))]),
    Vocabulary([Term("DEPT-CREDIT", ("信贷管理部", "信贷部"))]),
    Vocabulary([Term("T-MORTGAGE", ("个人住房贷款", "房贷"))]),
)
TODAY = dt.date(2026, 10, 10)


class FakeRewriter:
    model = "chat@t"

    def __init__(
        self, output: str = "个人住房贷款二套住房的首付比例是多少？", fail: bool = False
    ) -> None:
        self.output, self.fail = output, fail
        self.calls: list[str] = []

    async def complete(self, system: str, user: str, timeout: float) -> str:
        self.calls.append(user)
        if self.fail:
            raise LLMUnavailableError("down")
        return self.output


class MemCache:
    def __init__(self, down: bool = False) -> None:
        self.data: dict[str, bytes] = {}
        self.down = down

    async def get(self, key: str) -> bytes | None:
        if self.down:
            raise StoreUnavailableError("down")
        return self.data.get(key)

    async def set(self, key: str, value: bytes, ttl: int) -> None:
        if self.down:
            raise StoreUnavailableError("down")
        self.data[key] = value


def run(u: Understander, q: str) -> understand.Understood:
    return asyncio.run(u.run(q))


def test_first_turn_is_not_rewritten_but_synonyms_and_conditions_apply() -> None:
    rw = FakeRewriter()
    got = run(Understander(VOCAB, TODAY, (), rw), "去年房贷的现行办法里首付比例是多少？")
    assert rw.calls == [] and not got.rewritten
    # 同义扩展：房贷 → 个人住房贷款，办法 → 管理办法，作为第二个子查询
    assert got.queries == (
        "去年房贷的现行办法里首付比例是多少？",
        "去年个人住房贷款的现行管理办法里首付比例是多少？",
    )
    f = got.inferred
    assert f is not None and (f.effective_from, f.effective_to) == ("2025-01-01", "2025-12-31")
    assert list(f.validity_statuses) == [types_pb2.VALIDITY_STATUS_EFFECTIVE]
    # 部门、文档类型、主题单跳不推断
    assert not f.depts and not f.doc_types and not f.topic_codes


def test_follow_up_is_rewritten_once_and_cached() -> None:
    rw, cache = FakeRewriter(), MemCache()
    u = Understander(VOCAB, TODAY, ["个人住房贷款的首付比例是多少？"], rw, cache, "credit")
    got = run(u, "二套住房呢？")
    assert got.rewritten and got.queries[0] == "个人住房贷款二套住房的首付比例是多少？"
    assert "个人住房贷款的首付比例是多少？" in rw.calls[0]
    again = run(
        Understander(VOCAB, TODAY, ["个人住房贷款的首付比例是多少？"], rw, cache, "credit"),
        "二套住房呢？",
    )
    assert again.queries[0] == got.queries[0] and len(rw.calls) == 1


def test_summary_and_previous_titles_feed_the_rewrite() -> None:
    asked: list[int] = []

    async def titles() -> list[str]:
        asked.append(1)
        return ["个人住房贷款管理办法"]

    rw = FakeRewriter()
    recent = ["个人住房贷款的首付比例是多少？"]
    u = Understander(
        VOCAB, TODAY, recent, rw, None, "credit", summary="用户在问住房贷款", references=titles
    )
    run(u, "二套住房呢？")
    # 没有指代上一轮的文档：不取标题
    assert asked == [] and "对话摘要：用户在问住房贷款" in rw.calls[0]
    run(u, "上面那个制度里二套怎么规定？")
    assert asked == [1] and "《个人住房贷款管理办法》" in rw.calls[1]


def test_rewrite_falls_back_to_original() -> None:
    recent = ["个人住房贷款的首付比例是多少？"]
    for u in (
        Understander(VOCAB, TODAY, recent, FakeRewriter(fail=True)),
        # 输出太长（像在回答）：不采用
        Understander(VOCAB, TODAY, recent, FakeRewriter(output="答：" + "首付" * 100)),
        # 剩余时间不够改写
        Understander(VOCAB, TODAY, recent, FakeRewriter(), remaining=lambda: 1.0),
    ):
        assert run(u, "二套住房呢？").queries[0] == "二套住房呢？"
    # 缓存不可用时照常改写，只是不缓存
    got = run(
        Understander(VOCAB, TODAY, recent, FakeRewriter(), MemCache(down=True)), "二套住房呢？"
    )
    assert got.rewritten


def test_plausible_and_infer() -> None:
    assert (
        plausible("二套呢？", "改写后的问题：二套住房首付比例是多少？\n多余")
        == "二套住房首付比例是多少？"
    )
    assert plausible("二套呢？", "") == "二套呢？"
    assert infer("首付比例是多少？", VOCAB, TODAY) is None


class Recording:
    def __init__(self, empty_with_filters: bool) -> None:
        self.empty_with_filters = empty_with_filters
        self.calls: list[tuple[str, bool, tuple[str, ...]]] = []

    async def search(
        self, question: str, filters: types_pb2.Filters | None, variants: Sequence[str] = ()
    ) -> retrieval_pb2.SearchResponse:
        filtered = filters is not None and bool(filters.validity_statuses)
        self.calls.append((question, filtered, tuple(variants)))
        if filtered and self.empty_with_filters:
            return retrieval_pb2.SearchResponse()
        return retrieval_pb2.SearchResponse(hits=[retrieval_pb2.Hit(chunk_id="c1")])


class NoLLM:
    """取证据这一步不调用模型。"""

    def generate(self, system: str, user: str, level: int = 2, model: str = "") -> flow.Generation:
        raise AssertionError("取证据时不应调用模型")


class NoNLI:
    async def support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        raise AssertionError("取证据时不应校验")

    async def contradiction(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        raise AssertionError("取证据时不应校验")


class Access:
    async def allowed(self, chunk_ids: Sequence[str]) -> dict[str, bool]:
        return dict.fromkeys(chunk_ids, True)


def gather(r: Recording) -> tuple[retrieval_pb2.SearchResponse, answer_pb2.Why]:
    a = flow.Answer(
        flow.Request("现行办法里房贷首付比例是多少？", None, 8),
        r,
        NoLLM(),
        Grounder(NoNLI(), Access()),
        Understander(VOCAB, TODAY),
    )
    found = asyncio.run(a._gather())
    return found, a.why


def test_inferred_filters_are_recorded_or_dropped_when_nothing_found() -> None:
    r = Recording(empty_with_filters=False)
    found, why = gather(r)
    assert len(found.hits) == 1 and len(r.calls) == 1 and r.calls[0][1]
    assert list(why.inferred_filters.validity_statuses) == [types_pb2.VALIDITY_STATUS_EFFECTIVE]
    r = Recording(empty_with_filters=True)
    found, why = gather(r)
    # 加了推断条件没有结果：去掉再检索一次，并标注
    assert [c[1] for c in r.calls] == [True, False] and len(found.hits) == 1
    assert [(d.stage, d.reason) for d in why.degraded] == [
        ("inferred_filters", "dropped_no_result")
    ]
    assert r.calls[0][2] == ("现行管理办法里个人住房贷款首付比例是多少？",)
