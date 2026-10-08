"""回放时按当前权限调整回答：去掉不可见的引用、依赖它们的句子与列表项。"""

import pytest

from bambuflow.answer.v1 import answer_pb2
from bambuflow_orchestrator import listing, replay


def _fact() -> answer_pb2.AnswerResponse:
    s1, s2, s3 = "额度上限为 30 万元[S1]。", "期限最长 5 年[S1][S2]。", "利率按 LPR 执行[S3]。"
    text = f"{s1}{s2}\n{s3}"
    a = answer_pb2.AnswerResponse(status=answer_pb2.STATUS_ANSWERED, answer=text)
    pos = 0
    for i, (t, refs) in enumerate([(s1, [1]), (s2, [1, 2]), (s3, [3])]):
        if i == 2:
            pos += 1
        a.sentences.append(
            answer_pb2.Sentence(text=t, start=pos, end=pos + len(t), ref_ids=refs, block_id="p")
        )
        pos += len(t)
    a.blocks.append(answer_pb2.Block(block_id="p", sentence_indexes=[0, 1, 2]))
    for ref in (1, 2, 3):
        a.citations.append(answer_pb2.Citation(ref_id=ref, chunk_id=f"c{ref}"))
    a.why.graph_path.append(answer_pb2.GraphPath(ref_id=2, chunk_id="c2"))
    return a


def test_all_visible_unchanged() -> None:
    a = _fact()
    out = replay.prune(a, {"c1": True, "c2": True, "c3": True}, {})
    assert out == a


def test_sentence_citing_any_hidden_ref_is_dropped() -> None:
    # 第二句同时引用了 S1 与不可见的 S2：只要依据里有不可见的引用就整句去掉
    out = replay.prune(_fact(), {"c1": True, "c2": False, "c3": True}, {})
    assert out.answer == "额度上限为 30 万元[S1]。\n利率按 LPR 执行[S3]。"
    assert [s.text for s in out.sentences] == ["额度上限为 30 万元[S1]。", "利率按 LPR 执行[S3]。"]
    for s in out.sentences:
        assert out.answer[s.start : s.end] == s.text
    assert list(out.blocks[0].sentence_indexes) == [0, 1]
    assert [c.ref_id for c in out.citations] == [1, 3]
    assert not out.why.graph_path
    assert out.status == answer_pb2.STATUS_ANSWERED


def test_missing_result_counts_as_hidden_and_status_drops() -> None:
    out = replay.prune(_fact(), {}, {})
    assert out.answer == ""
    assert not out.sentences and not out.citations and not out.blocks
    assert out.status == answer_pb2.STATUS_INSUFFICIENT_EVIDENCE


def test_calc_inputs_count_as_dependency() -> None:
    a = _fact()
    a.sentences[2].calc.inputs.append(answer_pb2.CalcInput(var="x", ref_id=1))
    out = replay.prune(a, {"c1": False, "c2": True, "c3": True}, {})
    assert out.sentences == []


def test_evidence_only_without_citations_becomes_insufficient() -> None:
    a = answer_pb2.AnswerResponse(status=answer_pb2.STATUS_EVIDENCE_ONLY)
    a.citations.append(answer_pb2.Citation(ref_id=1, chunk_id="c1"))
    out = replay.prune(a, {"c1": False}, {})
    assert out.status == answer_pb2.STATUS_INSUFFICIENT_EVIDENCE and not out.citations


def _listing() -> answer_pb2.AnswerResponse:
    scope = answer_pb2.Scope(total_count=2, total_is_exact=True, shown=2, basis=listing.BASIS)
    note = listing.scope_note(scope, answer_pb2.ANSWER_KIND_ENUMERATION)
    items = ["《甲》", "《乙》"]
    text = "\n".join([note, *items])
    a = answer_pb2.AnswerResponse(
        status=answer_pb2.STATUS_ANSWERED,
        answer=text,
        answer_kind=answer_pb2.ANSWER_KIND_ENUMERATION,
    )
    a.sentences.append(answer_pb2.Sentence(text=note, start=0, end=len(note), block_id="scope"))
    pos = len(note) + 1
    for i, t in enumerate(items):
        a.sentences.append(
            answer_pb2.Sentence(
                text=t,
                start=pos,
                end=pos + len(t),
                claim_type=answer_pb2.CLAIM_TYPE_LIST_ITEM,
                block_id="list",
                document=answer_pb2.ListedDocument(doc_id=f"d{i}", title=t),
            )
        )
        pos += len(t) + 1
    a.blocks.append(
        answer_pb2.Block(
            block_id="list", block_type=answer_pb2.BLOCK_TYPE_LIST_ITEM, sentence_indexes=[1, 2]
        )
    )
    a.blocks.append(
        answer_pb2.Block(
            block_id="scope",
            block_type=answer_pb2.BLOCK_TYPE_SCOPE,
            order=1000,
            sentence_indexes=[0],
            scope=scope,
        )
    )
    return a


def test_listed_document_hidden_adjusts_scope() -> None:
    out = replay.prune(_listing(), {}, {"d0": True, "d1": False})
    assert [s.text for s in out.sentences][1:] == ["《甲》"]
    scope = out.blocks[1].scope
    assert scope.total_count == 1 and scope.shown == 1
    # 口径句按新数量重写，不暴露被去掉的那份
    assert (
        out.sentences[0].text
        == "符合条件的文档共 1 份，以下列出全部 1 份（仅统计你有权查看的文档）。"
    )
    assert out.answer == out.sentences[0].text + "\n《甲》"
    assert out.status == answer_pb2.STATUS_ANSWERED


def test_all_listed_hidden() -> None:
    out = replay.prune(_listing(), {}, {})
    assert out.sentences[0].text == "没有找到符合条件的文档；可以放宽条件后再试。"
    assert len(out.sentences) == 1 and [b.block_id for b in out.blocks] == ["scope"]
    assert out.status == answer_pb2.STATUS_INSUFFICIENT_EVIDENCE


def test_malformed_spans_rejected() -> None:
    a = _fact()
    a.sentences[1].start = 0
    with pytest.raises(replay.MalformedError):
        replay.prune(a, {"c1": True, "c2": False, "c3": True}, {})
