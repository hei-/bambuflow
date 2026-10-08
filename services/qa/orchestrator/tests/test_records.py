"""回答记录保存什么、反馈请求怎么校验，以及迁移文件的排序规则。"""

import pytest

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow_orchestrator import migrate, records


def _resp(classification: types_pb2.Classification) -> answer_pb2.AnswerResponse:
    a = answer_pb2.AnswerResponse(
        answer_id="ans_1",
        trace_id="t1",
        status=answer_pb2.STATUS_ANSWERED,
        answer="额度上限为 30 万元[S1]。",
    )
    a.sentences.append(answer_pb2.Sentence(text=a.answer, start=0, end=len(a.answer), ref_ids=[1]))
    a.citations.append(
        answer_pb2.Citation(
            ref_id=1,
            chunk_id="c1",
            doc_id="d1",
            title="贷款办法",
            quote="30 万元",
            classification=classification,
        )
    )
    a.audit.shown_chunk_ids.append("c1")
    a.why.index_version = "v1"
    rnd = a.why.agent_rounds.add(round=1)
    rnd.calls.add(sub_question="客户 13812345678 的额度")
    a.why.graph_path.add(ref_id=1, chunk_id="c1")
    return a


def _build(resp: answer_pb2.AnswerResponse, listed: bool = False) -> records.Record:
    return records.build(
        answer_id="ans_1",
        tenant="credit",
        asker="alice",
        actor="",
        purpose="answer",
        question="身份证 110101199003077777 的客户能贷多少？",
        resp=resp,
        listed_restricted=listed,
    )


def test_record_masks_question_and_drops_audit() -> None:
    rec = _build(_resp(types_pb2.CLASSIFICATION_INTERNAL))
    assert "110101199003077777" not in rec.question and rec.question.startswith("身份证 110101")
    assert not rec.answer.HasField("audit")
    assert rec.answer.why.agent_rounds[0].calls[0].sub_question == "客户 138****5678 的额度"
    assert rec.answer.citations[0].quote == "30 万元" and not rec.body_withheld
    assert rec.sentence_count == 1


@pytest.mark.parametrize("listed", [False, True])
def test_restricted_body_withheld(listed: bool) -> None:
    c = types_pb2.CLASSIFICATION_INTERNAL if listed else types_pb2.CLASSIFICATION_RESTRICTED
    rec = _build(_resp(c), listed)
    assert rec.body_withheld and rec.question == ""
    a = rec.answer
    assert a.answer == "" and not a.sentences and not a.blocks and not a.why.graph_path
    assert a.why.index_version == "v1" and a.why.agent_rounds[0].calls[0].sub_question == ""
    assert (
        a.citations[0].chunk_id == "c1"
        and a.citations[0].title == ""
        and a.citations[0].quote == ""
    )
    assert a.status == answer_pb2.STATUS_ANSWERED and a.answer_id == "ans_1"


def _fb(
    rating: answer_pb2.FeedbackRating = answer_pb2.FEEDBACK_RATING_WRONG,
    key: str = "k1",
    comment: str = "",
    sentence_index: int | None = None,
) -> answer_pb2.CreateFeedbackRequest:
    req = answer_pb2.CreateFeedbackRequest(
        answer_id="ans_1", rating=rating, idempotency_key=key, comment=comment
    )
    if sentence_index is not None:
        req.sentence_index = sentence_index
    return req


def test_feedback_input() -> None:
    fb = records.feedback_input(_fb(comment=" 手机 13812345678 写错了 ", sentence_index=0))
    assert fb is not None
    assert fb.rating == "wrong" and fb.sentence_index == 0
    assert fb.comment == "手机 138****5678 写错了"
    same = records.feedback_input(_fb(comment="手机 13812345678 写错了", sentence_index=0))
    other = records.feedback_input(_fb(comment="另一个说明", sentence_index=0))
    assert same is not None and other is not None
    assert same.digest == fb.digest != other.digest
    assert records.feedback_input(_fb()) is not None


@pytest.mark.parametrize(
    "req",
    [
        _fb(rating=answer_pb2.FEEDBACK_RATING_UNSPECIFIED),
        _fb(key=""),
        _fb(key="k" * 129),
        _fb(comment="长" * 501),
        _fb(sentence_index=-1),
    ],
)
def test_feedback_input_rejects(req: answer_pb2.CreateFeedbackRequest) -> None:
    assert records.feedback_input(req) is None


def test_migration_steps() -> None:
    got = migrate.steps({"0002_b.sql": "2", "0001_a.sql": "1", "0010_c.sql": "10"})
    assert [s.name for s in got] == ["0001_a.sql", "0002_b.sql", "0010_c.sql"]
    with pytest.raises(ValueError):
        migrate.steps({"0001_a.sql": "", "0001_b.sql": ""})
    with pytest.raises(ValueError):
        migrate.steps({"init.sql": ""})
    assert "0001_answer_records.sql" in migrate.bundled()
