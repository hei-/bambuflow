"""agent 状态机（orchestrator 详设第 6 节）：轮次、子问题、预算与降级。

下游全部用假实现，时钟可控。"""

import asyncio
import json
from collections.abc import AsyncGenerator, Sequence
from typing import Any

import grpc
import pytest

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_grounding import Grounder, GroundingUnavailableError
from bambuflow_orchestrator import agent, flow, llm_rerank, prompt
from bambuflow_orchestrator.budget import Deadline
from bambuflow_orchestrator.llm import LLMUnavailableError
from bambuflow_orchestrator.session import StoreUnavailableError

OLD = "旧版信贷政策规定抵押率不超过 70%。"
NEW = "新版信贷政策规定抵押率不超过 60%。"
QUESTION = "新旧两版信贷政策在抵押率上有什么区别？"


def hit(chunk: str, text: str, score: float) -> retrieval_pb2.Hit:
    return retrieval_pb2.Hit(
        chunk_id=chunk,
        doc_id=chunk.split("#")[0],
        doc_version=1,
        text=text,
        title=chunk,
        score=score,
    )


def unit(text: str, ref: int, quote: str, block: str = "b1") -> str:
    return json.dumps(
        {
            "block_id": block,
            "block_type": "paragraph",
            "text": f"{text}[S{ref}]",
            "refs": [ref],
            "quote": [quote],
            "claim_type": "quote",
        },
        ensure_ascii=False,
    )


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


class FakeSearch:
    """按检索表述返回预设结果；errors 指定（表述，轮次）上的错误；每次调用让时钟走 cost 秒。"""

    def __init__(
        self,
        results: dict[str, list[retrieval_pb2.Hit]],
        clock: Clock,
        cost: float = 0.3,
        rerank_model: str = "rerank@t",
        errors: dict[tuple[str, int], grpc.StatusCode] | None = None,
    ) -> None:
        self.results, self.clock, self.cost = results, clock, cost
        self.rerank_model = rerank_model
        self.errors = errors or {}
        self.calls: list[tuple[str, int, float]] = []

    async def search(
        self, query: str, filters: types_pb2.Filters | None, round_no: int, timeout: float
    ) -> retrieval_pb2.SearchResponse:
        self.calls.append((query, round_no, timeout))
        self.clock.t += self.cost
        if (code := self.errors.get((query, round_no))) is not None:
            raise grpc.aio.AioRpcError(code, grpc.aio.Metadata(), grpc.aio.Metadata())
        return retrieval_pb2.SearchResponse(
            hits=self.results.get(query, []),
            index_version="ix-1",
            policy_version="sha256:p",
            embedding_model="bge-m3@t",
            rerank_model=self.rerank_model,
        )


class FakeGeneration:
    def __init__(self, output: str | None) -> None:
        self.model = "chat@t"
        self.output = output

    async def __aiter__(self) -> AsyncGenerator[str, None]:
        if self.output is None:
            raise LLMUnavailableError("down")
        for raw in self.output.split("\n"):
            yield raw


class FakeLLM:
    """按系统提示词区分规划、改写与回答；值为 None 时该类调用不可用。"""

    def __init__(
        self,
        plan: str | None = "",
        replan: str | None = "",
        answer: str | None = "",
        clock: Clock | None = None,
        cost: float = 0.0,
        rerank: str | None = "",
    ) -> None:
        self.outputs = {
            prompt.PLAN_SYSTEM: plan,
            prompt.REPLAN_SYSTEM: replan,
            prompt.AGENT_SYSTEM: answer,
            RERANK: rerank,
        }
        self.calls: list[tuple[str, str, float | None]] = []
        self.models: list[str] = []
        self.clock, self.cost = clock, cost

    def generate(
        self,
        system: str,
        user: str,
        timeout: float | None = None,
        level: int = 2,
        model: str = "",
    ) -> FakeGeneration:
        self.models.append(model)
        self.calls.append((system, user, timeout))
        if self.clock is not None:
            self.clock.t += self.cost
        return FakeGeneration(self.outputs[system])

    def systems(self) -> list[str]:
        return [s for s, _, _ in self.calls]


def plan_lines(*texts: str) -> str:
    return "\n".join(
        json.dumps({"id": f"q{i}", "text": t}, ensure_ascii=False)
        for i, t in enumerate(texts, start=1)
    )


class OverlapNLI:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail

    async def support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        if self.fail:
            raise GroundingUnavailableError("down")
        return [sum(ch in p for ch in h) / len(h) for p, h in pairs], "nli@t"

    async def contradiction(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        if self.fail:
            raise GroundingUnavailableError("down")
        return [0.0] * len(pairs), "nli@t"


class Access:
    async def allowed(self, chunk_ids: Sequence[str]) -> dict[str, bool]:
        return dict.fromkeys(chunk_ids, True)


class Sink:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.saved: list[dict[str, Any]] = []
        self.dropped = False

    async def save(self, state: dict[str, Any]) -> None:
        if self.fail:
            raise StoreUnavailableError("down")
        self.saved.append(state)

    async def drop(self) -> None:
        if self.fail:
            raise StoreUnavailableError("down")
        self.dropped = True


RERANK = prompt.RERANK_SYSTEM.format(keep=llm_rerank.KEEP)
TWO = plan_lines("旧版信贷政策的抵押率", "新版信贷政策的抵押率")
ANSWER = "\n".join([unit(OLD, 1, OLD), unit(NEW, 2, NEW, "b2")])


def results(old: float = 0.9, new: float = 0.8) -> dict[str, list[retrieval_pb2.Hit]]:
    return {
        "旧版信贷政策的抵押率": [hit("old#1", OLD, old)],
        "新版信贷政策的抵押率": [hit("new#1", NEW, new)],
    }


def run(
    llm: FakeLLM,
    search: FakeSearch,
    clock: Clock,
    seconds: float = 10.0,
    sink: Sink | None = None,
    history: Sequence[str] = (),
    nli: OverlapNLI | None = None,
    max_rounds: int | None = None,
    max_sub_questions: int | None = None,
) -> answer_pb2.AnswerResponse:
    deadline = Deadline(seconds, clock)
    a = agent.Agent(
        QUESTION,
        None,
        history,
        search,
        llm,
        deadline,
        sink or Sink(),
        clock,
        max_rounds=max_rounds,
        max_sub_questions=max_sub_questions,
    )
    req = flow.Request(QUESTION, None, 8)
    answer = agent.AgentAnswer(req, a, llm, Grounder(nli or OverlapNLI(), Access()))

    async def go() -> answer_pb2.AnswerResponse:
        async for _ in answer.events():
            pass
        return answer.response()

    return asyncio.run(go())


def test_one_round_when_every_sub_question_is_covered() -> None:
    clock = Clock()
    llm, search = FakeLLM(plan=TWO, answer=ANSWER), FakeSearch(results(), clock)
    r = run(llm, search, clock)
    assert r.status == answer_pb2.STATUS_ANSWERED
    assert [s.text for s in r.sentences] == [OLD + "[S1]", NEW + "[S2]"]
    assert [(q, n) for q, n, _ in search.calls] == [
        ("旧版信贷政策的抵押率", 1),
        ("新版信贷政策的抵押率", 1),
    ]
    assert llm.systems() == [prompt.PLAN_SYSTEM, prompt.AGENT_SYSTEM]
    (rnd,) = r.why.agent_rounds
    assert rnd.round == 1 and [c.sub_question_id for c in rnd.calls] == ["q1", "q2"]
    assert all(c.covered and c.tool == "doc_search" for c in rnd.calls)
    assert [list(c.chunk_ids) for c in rnd.calls] == [["old#1"], ["new#1"]]
    assert r.why.prompt_version == prompt.AGENT.version
    assert (r.why.index_version, r.why.rerank_model) == ("ix-1", "rerank@t")
    # 回答的提示词带子问题，没有未覆盖的子问题
    user = llm.calls[-1][1]
    assert "<subquestions>" in user and "<unresolved>" not in user


def test_uncovered_sub_question_is_replanned_in_next_round() -> None:
    clock = Clock()
    res = results(new=0.3)  # 0.2–0.5：有弱证据，继续规划
    res["新版信贷政策的抵押率 正式表述"] = [hit("new#2", NEW, 0.9)]
    replan = json.dumps({"id": "q2", "text": "新版信贷政策的抵押率 正式表述"}, ensure_ascii=False)
    llm = FakeLLM(plan=TWO, replan=replan, answer=ANSWER)
    search = FakeSearch(res, clock)
    r = run(llm, search, clock)
    assert [(q, n) for q, n, _ in search.calls][2:] == [("新版信贷政策的抵押率 正式表述", 2)]
    assert [x.round for x in r.why.agent_rounds] == [1, 2]
    assert [c.covered for c in r.why.agent_rounds[0].calls] == [True, False]
    assert r.why.agent_rounds[1].calls[0].covered
    # 改写只为未覆盖的子问题，规划器看到的已检索证据只有标题与开头
    replan_user = llm.calls[1][1]
    assert (
        "<pending>\nq2：新版信贷政策的抵押率\n</pending>" in replan_user
        and "q1：" not in replan_user
    )
    assert r.status == answer_pb2.STATUS_ANSWERED


def test_flagged_evidence_never_reaches_the_planner() -> None:
    clock = Clock()
    res = results(new=0.3)
    flagged = hit("old#1", OLD, 0.9)
    flagged.injection_flag = True
    res["旧版信贷政策的抵押率"] = [flagged]
    res["新版信贷政策的抵押率 正式表述"] = [hit("new#2", NEW, 0.9)]
    replan = json.dumps({"id": "q2", "text": "新版信贷政策的抵押率 正式表述"}, ensure_ascii=False)
    llm = FakeLLM(plan=TWO, replan=replan, answer=ANSWER)
    run(llm, FakeSearch(res, clock), clock)
    replan_user = llm.calls[1][1]
    assert "<pending>" in replan_user and OLD not in replan_user


def test_at_most_three_rounds() -> None:
    clock = Clock()
    res = {"a": [hit("x#1", OLD, 0.3)], "b": [hit("x#2", OLD, 0.3)], "c": [hit("x#3", OLD, 0.3)]}
    replans = iter(["b", "c", "d"])

    class Replanner(FakeLLM):
        def generate(
            self,
            system: str,
            user: str,
            timeout: float | None = None,
            level: int = 2,
            model: str = "",
        ) -> FakeGeneration:
            if system == prompt.REPLAN_SYSTEM:
                self.outputs[system] = json.dumps({"id": "q1", "text": next(replans)})
            return super().generate(system, user, timeout)

    llm = Replanner(plan=plan_lines("a"), answer=unit(OLD, 1, OLD))
    search = FakeSearch(res, clock, cost=0.1)
    r = run(llm, search, clock)
    assert [n for _, n, _ in search.calls] == [1, 2, 3]
    assert len(r.why.agent_rounds) == agent.MAX_ROUNDS
    # 三轮都只有弱证据：仍然作答（不 GIVE_UP），也不算“未找到依据”
    assert r.status == answer_pb2.STATUS_ANSWERED
    assert all(b.block_id != "unresolved" for b in r.blocks)


def test_pipeline_budget_lowers_rounds_and_sub_questions() -> None:
    clock = Clock()
    res = results(new=0.3)  # 第 2 个子问题只有弱证据：默认预算下会开第 2 轮
    replan = json.dumps({"id": "q2", "text": "新版 正式表述"}, ensure_ascii=False)
    llm = FakeLLM(
        plan=plan_lines("旧版信贷政策的抵押率", "新版信贷政策的抵押率", "c"), replan=replan
    )
    search = FakeSearch(res, clock)
    r = run(llm, search, clock, max_rounds=1, max_sub_questions=2)
    assert len(r.why.agent_rounds) == 1
    assert [q for q, _, _ in search.calls] == ["旧版信贷政策的抵押率", "新版信贷政策的抵押率"]


def test_no_new_round_when_remaining_time_is_within_generation_reserve() -> None:
    clock = Clock()
    llm = FakeLLM(plan=TWO, replan=plan_lines("x", "y"), answer=ANSWER)
    # 第 1 轮之后剩余不到 3 秒（生成预留）：不再开第 2 轮
    search = FakeSearch(results(new=0.3), clock, cost=3.5)
    r = run(llm, search, clock, seconds=10.0)
    assert len(r.why.agent_rounds) == 1
    assert prompt.REPLAN_SYSTEM not in llm.systems()


def test_search_timeout_leaves_generation_reserve() -> None:
    clock = Clock()
    search = FakeSearch(results(), clock)
    run(FakeLLM(plan=TWO, answer=ANSWER), search, clock, seconds=4.0)
    # 剩余 4 秒、预留 3 秒：每次检索最多 1 秒
    assert all(t == pytest.approx(1.0) for _, _, t in search.calls)


def test_too_little_time_for_any_round_returns_insufficient_without_generation() -> None:
    clock = Clock()
    llm = FakeLLM(plan=TWO, answer=ANSWER)
    search = FakeSearch(results(), clock)
    r = run(llm, search, clock, seconds=3.0)
    assert search.calls == [] and llm.calls == []
    assert r.status == answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
    assert ("plan", "no_time") in [(d.stage, d.reason) for d in r.why.degraded]


def test_give_up_when_no_sub_question_has_evidence() -> None:
    clock = Clock()
    res = {"旧版信贷政策的抵押率": [hit("old#1", OLD, 0.1)]}
    llm = FakeLLM(plan=TWO, replan="", answer=ANSWER)
    r = run(llm, FakeSearch(res, clock), clock)
    assert r.status == answer_pb2.STATUS_INSUFFICIENT_EVIDENCE and not r.sentences
    assert prompt.AGENT_SYSTEM not in llm.systems(), "GIVE_UP 不调用生成"
    assert r.why.agent_rounds


def test_unresolved_sub_question_gets_fixed_note_not_model_text() -> None:
    clock = Clock()
    res = {"旧版信贷政策的抵押率": [hit("old#1", OLD, 0.9)]}
    llm = FakeLLM(plan=TWO, replan="", answer=unit(OLD, 1, OLD))
    r = run(llm, FakeSearch(res, clock), clock)
    assert r.status == answer_pb2.STATUS_ANSWERED
    assert "<unresolved>\n新版信贷政策的抵押率\n</unresolved>" in llm.calls[-1][1]
    note = r.sentences[-1]
    assert note.text == agent.UNRESOLVED_NOTE and note.claim_type == answer_pb2.CLAIM_TYPE_NONE
    assert not note.ref_ids and r.blocks[-1].block_type == answer_pb2.BLOCK_TYPE_NOTE


def test_final_answer_is_still_verified_sentence_by_sentence() -> None:
    clock = Clock()
    bad = unit("所有客户均可享受三年免息优惠。", 1, OLD, "b3")
    llm = FakeLLM(plan=TWO, answer="\n".join([ANSWER, bad]))
    r = run(llm, FakeSearch(results(), clock), clock)
    assert all("免息" not in s.text for s in r.sentences)
    clock = Clock()
    r = run(
        FakeLLM(plan=TWO, answer=ANSWER),
        FakeSearch(results(), clock),
        clock,
        nli=OverlapNLI(fail=True),
    )
    assert r.status == answer_pb2.STATUS_EVIDENCE_ONLY and not r.sentences


def test_planner_unavailable_falls_back_to_the_question() -> None:
    clock = Clock()
    search = FakeSearch({QUESTION: [hit("old#1", OLD, 0.9)]}, clock)
    r = run(FakeLLM(plan=None, answer=unit(OLD, 1, OLD)), search, clock)
    assert [q for q, _, _ in search.calls] == [QUESTION]
    assert ("plan", "unavailable") in [(d.stage, d.reason) for d in r.why.degraded]
    assert r.status == answer_pb2.STATUS_ANSWERED


def test_unparseable_plan_falls_back_to_the_question() -> None:
    clock = Clock()
    search = FakeSearch({QUESTION: [hit("old#1", OLD, 0.9)]}, clock)
    r = run(FakeLLM(plan="不是 JSON", answer=unit(OLD, 1, OLD)), search, clock)
    assert [q for q, _, _ in search.calls] == [QUESTION]
    assert ("plan", "unparseable") in [(d.stage, d.reason) for d in r.why.degraded]


def test_planner_sees_history_and_at_most_four_sub_questions() -> None:
    clock = Clock()
    llm = FakeLLM(plan=plan_lines("a", "b", "c", "d", "e"), answer="")
    search = FakeSearch({}, clock)
    run(llm, search, clock, history=["上一轮的问题"])
    assert "<history>\n上一轮的问题\n</history>" in llm.calls[0][1]
    assert sorted(q for q, n, _ in search.calls if n == 1) == ["a", "b", "c", "d"]


def test_later_round_retrieval_failure_answers_with_what_was_found() -> None:
    clock = Clock()
    replan = json.dumps({"id": "q2", "text": "新版 正式表述"}, ensure_ascii=False)
    search = FakeSearch(
        results(new=0.3), clock, errors={("新版 正式表述", 2): grpc.StatusCode.UNAVAILABLE}
    )
    r = run(FakeLLM(plan=TWO, replan=replan, answer=ANSWER), search, clock)
    assert ("agent_round", "unavailable") in [(d.stage, d.reason) for d in r.why.degraded]
    assert r.status == answer_pb2.STATUS_ANSWERED


def test_first_round_failures_are_raised() -> None:
    for code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.PERMISSION_DENIED):
        clock = Clock()
        errors = {(q, 1): code for q in results()}
        with pytest.raises(grpc.aio.AioRpcError) as e:
            run(
                FakeLLM(plan=TWO, answer=ANSWER), FakeSearch(results(), clock, errors=errors), clock
            )
        assert e.value.code() == code


def test_permission_errors_are_never_swallowed() -> None:
    clock = Clock()
    errors = {("新版信贷政策的抵押率", 1): grpc.StatusCode.PERMISSION_DENIED}
    with pytest.raises(grpc.aio.AioRpcError):
        run(FakeLLM(plan=TWO, answer=ANSWER), FakeSearch(results(), clock, errors=errors), clock)


def test_evidence_alternates_between_sub_questions() -> None:
    clock = Clock()
    res = {
        "旧版信贷政策的抵押率": [hit(f"old#{i}", OLD, 0.9 - i / 100) for i in range(5)],
        "新版信贷政策的抵押率": [hit("new#1", NEW, 0.6), hit("old#0", OLD, 0.95)],
    }
    llm = FakeLLM(plan=TWO, answer=ANSWER)
    r = run(llm, FakeSearch(res, clock), clock)
    assert list(r.audit.candidate_chunk_ids)[:4] == ["old#0", "new#1", "old#1", "old#2"]


def test_state_is_saved_each_round_and_dropped() -> None:
    clock = Clock()
    sink = Sink()
    run(FakeLLM(plan=TWO, answer=ANSWER), FakeSearch(results(), clock), clock, sink=sink)
    assert [s["round"] for s in sink.saved] == [1] and sink.dropped
    assert sink.saved[0]["evidence"] == [
        {"chunk_id": "old#1", "score": pytest.approx(0.9)},
        {"chunk_id": "new#1", "score": pytest.approx(0.8)},
    ]
    assert "text" not in json.dumps(sink.saved[0]["evidence"]), "中间状态不存正文"


def test_state_store_failure_only_degrades() -> None:
    clock = Clock()
    r = run(
        FakeLLM(plan=TWO, answer=ANSWER),
        FakeSearch(results(), clock),
        clock,
        sink=Sink(fail=True),
    )
    assert r.status == answer_pb2.STATUS_ANSWERED
    assert ("session", "unavailable") in [(d.stage, d.reason) for d in r.why.degraded]


def test_unreranked_hits_count_as_covered() -> None:
    clock = Clock()
    res = results(old=0.01, new=0.02)  # 融合分，量纲不同
    search = FakeSearch(res, clock, rerank_model="")
    r = run(FakeLLM(plan=TWO, answer=ANSWER), search, clock)
    assert len(r.why.agent_rounds) == 1 and r.status == answer_pb2.STATUS_ANSWERED


def many() -> dict[str, list[retrieval_pb2.Hit]]:
    """两个子问题各 13 条候选，合并后 26 条（超过 LLM 重排的门槛 20）。"""
    return {
        "旧版信贷政策的抵押率": [hit(f"old#{i}", OLD, 0.9 - i / 100) for i in range(13)],
        "新版信贷政策的抵押率": [hit(f"new#{i}", NEW, 0.855 - i / 100) for i in range(13)],
    }


def by_score() -> list[str]:
    hits = [h for hs in many().values() for h in hs]
    return [h.chunk_id for h in sorted(hits, key=lambda h: h.score, reverse=True)]


def test_llm_rerank_picks_context_from_top_twenty() -> None:
    clock = Clock()
    llm = FakeLLM(plan=TWO, answer=ANSWER, rerank='{"order": [3, 1, 3, 99, true]}')
    r = run(llm, FakeSearch(many(), clock), clock)
    top = by_score()[:20]
    assert r.why.llm_rerank == "chat@t+" + prompt.RERANK_PROMPT_VERSION
    # 只认范围内的编号，重复的只取一次；选中的排在最前，其余按精排分
    assert list(r.audit.context_chunk_ids)[:3] == [top[2], top[0], top[1]]
    (call,) = [c for c in llm.calls if c[0] == RERANK]
    assert call[2] == llm_rerank.BUDGET and call[1].count("[") >= 20


def test_llm_rerank_failure_falls_back_to_rerank_scores() -> None:
    for out, reason in ((None, "unavailable"), ("不是 JSON", "unparseable")):
        clock = Clock()
        llm = FakeLLM(plan=TWO, answer=ANSWER, rerank=out)
        r = run(llm, FakeSearch(many(), clock), clock)
        assert r.why.llm_rerank == ""
        assert ("llm_rerank", reason) in [(d.stage, d.reason) for d in r.why.degraded]
        assert list(r.audit.context_chunk_ids)[:8] == by_score()[:8]


def test_llm_rerank_needs_enough_candidates_and_time() -> None:
    clock = Clock()
    llm = FakeLLM(plan=TWO, answer=ANSWER)
    run(llm, FakeSearch(results(), clock), clock)
    assert RERANK not in llm.systems()
    clock = Clock()
    llm = FakeLLM(plan=TWO, answer=ANSWER)
    r = run(llm, FakeSearch(many(), clock), clock, seconds=4.0)
    assert RERANK not in llm.systems()
    assert ("llm_rerank", "skipped_deadline") in [(d.stage, d.reason) for d in r.why.degraded]
