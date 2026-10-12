import asyncio
import json
from collections.abc import Sequence
from typing import Any

import httpx
import pytest

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow_evaluation import llm_judge
from bambuflow_evaluation.golden import Asker, Question
from bambuflow_evaluation.llm_judge import JudgeError, Judgment, OpenAIJudge, parse
from bambuflow_evaluation.metrics import Outcome, compute


def test_parse_takes_first_json_object_and_validates_fields() -> None:
    got = parse(
        '评审结果：{"faithful": false, "issues": ["x"], '
        '"key_points": [{"index": 2, "covered": true}]}',
        3,
    )
    assert got == Judgment(False, (False, True, False))
    # 序号越界、字段类型不对的要点忽略（按没覆盖）
    assert parse(
        '{"faithful": true, "key_points": [{"index": 9, "covered": true}, {"index": "1"}]}', 1
    ) == Judgment(True, (False,))
    for bad in ("没有 JSON", "{坏的 JSON}", '{"issues": []}', '{"faithful": "yes"}'):
        with pytest.raises(JudgeError):
            parse(bad, 0)


def test_openai_judge_is_deterministic_and_cached() -> None:
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(body)
        content = '{"faithful": true, "key_points": [{"index": 1, "covered": true}]}'
        return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})

    async def main() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            j = OpenAIJudge("http://judge/v1", "judge-model", "k", http)
            a = await j.judge(
                "额度多少？", "不超过 30 万元。[S1]", ["额度不超过 30 万元"], ["30 万元"]
            )
            b = await j.judge(
                "额度多少？", "不超过 30 万元。[S1]", ["额度不超过 30 万元"], ["30 万元"]
            )
            assert a == b == Judgment(True, (True,))
            assert j.version == "judge-model+" + llm_judge.PROMPT_VERSION

    asyncio.run(main())
    # 同一输入只评一次；温度 0
    assert len(calls) == 1 and calls[0]["temperature"] == 0 and calls[0]["model"] == "judge-model"


def outcome(
    qid: str, expected: str, status: str, faithful: bool | None, kps: tuple[bool, ...]
) -> Outcome:
    return Outcome(
        qid,
        "credit",
        "b",
        expected,
        status,
        (),
        (),
        (),
        (True,),
        (True,),
        False,
        (),
        1.0,
        faithful,
        kps,
    )


def test_faithfulness_and_correctness_only_count_judged_answers() -> None:
    m = compute(
        [
            outcome("a", "answered", "answered", True, (True, False)),
            outcome("b", "answered", "answered", False, (True, True)),
            # 应回答却拒答：要点都算没覆盖
            outcome("c", "answered", "insufficient_evidence", None, (False,)),
            # 没评审（受限级或评审失败）：不计入
            outcome("d", "answered", "answered", None, ()),
        ]
    )
    assert m["faithfulness"] == 0.5
    assert m["answer_correctness"] == pytest.approx((0.5 + 1.0 + 0.0) / 3)
    none = compute([outcome("e", "answered", "answered", None, ())])
    assert none["faithfulness"] is None and none["answer_correctness"] is None


class FakeJudge:
    version = "fake+judge@1"

    def __init__(self) -> None:
        self.seen: list[Sequence[str]] = []

    async def judge(
        self, question: str, answer: str, quotes: Sequence[str], key_points: Sequence[str]
    ) -> Judgment:
        self.seen.append(quotes)
        return Judgment(True, tuple(True for _ in key_points))


def test_runner_skips_restricted_and_unanswered() -> None:
    from bambuflow_evaluation.runner import Runner

    asker = Asker("u", "u", "credit", "部门", 2, ())
    q = Question("q1", "single_hop", asker, "问", "answered", ("要点",), (), ())
    judge = FakeJudge()
    r = Runner.__new__(Runner)
    r._llm = judge

    def resp(status: answer_pb2.Status, restricted: bool) -> answer_pb2.AnswerResponse:
        a = answer_pb2.AnswerResponse(status=status, answer="答。[S1]")
        a.sentences.add(text="答。[S1]").quotes.add(ref_id=1, quote="引文")
        a.citations.add(
            classification=types_pb2.CLASSIFICATION_RESTRICTED
            if restricted
            else types_pb2.CLASSIFICATION_INTERNAL
        )
        return a

    async def main() -> None:
        assert await r._judge(q, resp(answer_pb2.STATUS_ANSWERED, False)) == Judgment(True, (True,))
        assert await r._judge(q, resp(answer_pb2.STATUS_ANSWERED, True)) == llm_judge.NOT_JUDGED
        assert await r._judge(q, resp(answer_pb2.STATUS_INSUFFICIENT_EVIDENCE, False)) == Judgment(
            None, (False,)
        )

    asyncio.run(main())
    assert judge.seen == [["引文"]]
