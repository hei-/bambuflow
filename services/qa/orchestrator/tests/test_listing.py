import asyncio
import datetime as dt

from bambuflow_vocabulary import Term, Vocabularies, Vocabulary

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_orchestrator import listing

VOCAB = Vocabularies(
    "vocab-test",
    Vocabulary(
        [
            Term("DT-POLICY", ("制度", "管理办法", "办法")),
            Term("DT-PROCEDURE", ("流程", "规程"), parent="DT-POLICY"),
            Term("DT-NOTICE", ("通知",)),
        ]
    ),
    Vocabulary([Term("DEPT-CREDIT", ("信贷管理部", "信贷部"))]),
    Vocabulary(
        [
            Term("T-CREDIT", ("信贷", "贷款")),
            Term("T-RETAIL", ("零售信贷", "个人贷款"), parent="T-CREDIT"),
        ]
    ),
)
TODAY = dt.date(2026, 10, 6)


def test_understand_extracts_conditions_and_expands_children() -> None:
    c = listing.understand("信贷部 2025 年发布了哪些现行的制度？", VOCAB, TODAY)
    assert c is not None and c.kind == answer_pb2.ANSWER_KIND_ENUMERATION
    f = c.filters
    # 按“制度”查包含下级“流程”
    assert list(f.doc_types) == ["DT-POLICY", "DT-PROCEDURE"] and list(f.depts) == ["DEPT-CREDIT"]
    assert (f.effective_from, f.effective_to) == ("2025-01-01", "2025-12-31")
    assert list(f.validity_statuses) == [types_pb2.VALIDITY_STATUS_EFFECTIVE]
    assert c.text == "文档类型为制度、发文部门为信贷管理部、2025 年生效、效力状态为现行"


def test_understand_count_and_topic() -> None:
    c = listing.understand("去年发布的贷款类通知有多少份？", VOCAB, TODAY)
    assert c is not None and c.kind == answer_pb2.ANSWER_KIND_COUNT
    assert (
        list(c.filters.topic_codes) == ["T-CREDIT", "T-RETAIL"]
        and c.filters.effective_from == "2025-01-01"
    )


def test_not_a_set_question() -> None:
    for q in (
        "哪些情况下可以提前还款？",
        "额度是多少？",
        "所有员工都要参加培训吗？",
        # 比较类问题走多跳，即使问法里有“哪些”且出现了文档类型
        "新旧两版个人住房贷款管理办法在首付比例上有什么区别？",
        "两个管理办法有哪些不同？",
        # 问办事要求，不是列举库里的文档
        "授信审批需要哪些材料？",
        "开户应提交哪些文件？",
    ):
        assert listing.understand(q, VOCAB, TODAY) is None, q


def test_caller_filters_take_precedence() -> None:
    inferred = types_pb2.Filters(doc_types=["DT-POLICY"], depts=["DEPT-CREDIT"])
    merged = listing.merge(
        types_pb2.Filters(source_ids=["src_wiki"], doc_types=["DT-NOTICE"]), inferred
    )
    assert list(merged.source_ids) == ["src_wiki"] and list(merged.doc_types) == ["DT-NOTICE"]
    assert list(merged.depts) == ["DEPT-CREDIT"]


def doc(i: int) -> retrieval_pb2.DocumentSummary:
    return retrieval_pb2.DocumentSummary(
        doc_id=f"d{i}",
        doc_version=1,
        title=f"制度 {i}",
        source_uri=f"wiki://d{i}",
        metadata=types_pb2.DocMetadata(
            effective_date="2025-04-01", validity_status=types_pb2.VALIDITY_STATUS_EFFECTIVE
        ),
    )


class FakeCatalog:
    def __init__(self, total: int, exact: bool = True) -> None:
        self.total, self.exact = total, exact
        self.calls: list[str] = []

    async def count(
        self, filters: types_pb2.Filters, group_by: retrieval_pb2.CountGroupBy
    ) -> retrieval_pb2.CountDocumentsResponse:
        self.calls.append(f"count:{group_by}")
        return retrieval_pb2.CountDocumentsResponse(
            total_count=self.total,
            total_is_exact=self.exact,
            index_version="ix-1",
            policy_version="sha256:p",
            groups=[retrieval_pb2.CountGroup(value="2025", count=self.total)] if group_by else [],
            metadata_gaps=[retrieval_pb2.MetadataGap(field="doc_type", count=3)],
        )

    async def list(
        self, filters: types_pb2.Filters, page_size: int, page_token: str = ""
    ) -> retrieval_pb2.ListDocumentsResponse:
        self.calls.append(f"list:{page_size}")
        n = min(self.total, page_size)
        return retrieval_pb2.ListDocumentsResponse(
            documents=[doc(i) for i in range(n)], total_count=self.total
        )


def run(question: str, catalog: FakeCatalog) -> answer_pb2.AnswerResponse:
    c = listing.understand(question, VOCAB, TODAY)
    assert c is not None
    return asyncio.run(listing.answer(c, c.filters, catalog))


def test_enumeration_lists_all_when_few() -> None:
    r = run("有哪些制度？", FakeCatalog(2))
    assert (
        r.answer_kind == answer_pb2.ANSWER_KIND_ENUMERATION
        and r.status == answer_pb2.STATUS_ANSWERED
    )
    items = [s for s in r.sentences if s.claim_type == answer_pb2.CLAIM_TYPE_LIST_ITEM]
    assert [s.document.doc_id for s in items] == ["d0", "d1"]
    assert items[0].text == "《制度 0》（2025-04-01 起生效，现行）" and not items[0].ref_ids
    scope = next(b for b in r.blocks if b.block_type == answer_pb2.BLOCK_TYPE_SCOPE)
    assert (scope.scope.total_count, scope.scope.shown, scope.scope.total_is_exact) == (2, 2, True)
    assert scope.scope.metadata_gaps == "另有 3 份文档未标注文档类型，可能也符合条件"
    assert scope.scope.basis == "仅统计你有权查看的文档"
    # 口径块版式上排在最后
    assert r.blocks[-1].block_type == answer_pb2.BLOCK_TYPE_SCOPE
    assert list(r.audit.listed_doc_ids) == ["d0", "d1"] and r.audit.total_count == 2
    assert list(r.why.inferred_filters.doc_types) == ["DT-POLICY", "DT-PROCEDURE"]
    for s in r.sentences:
        assert r.answer[s.start : s.end] == s.text


def test_enumeration_truncates_and_states_total() -> None:
    r = run("列出所有制度", FakeCatalog(47))
    items = [s for s in r.sentences if s.claim_type == answer_pb2.CLAIM_TYPE_LIST_ITEM]
    assert len(items) == listing.MAX_LISTED
    note = next(s.text for s in r.sentences if s.block_id == "scope")
    assert note.startswith("符合条件的文档共 47 份，以下列出最新的 30 份")


def test_count_gives_groups_and_no_list() -> None:
    cat = FakeCatalog(12)
    r = run("制度有多少份？", cat)
    assert r.answer_kind == answer_pb2.ANSWER_KIND_COUNT and not any(
        c.startswith("list") for c in cat.calls
    )
    assert [s.text for s in r.sentences if s.block_id == "groups"] == ["2025：12 份"]


def test_nothing_found_does_not_hint_at_hidden_docs() -> None:
    r = run("有哪些制度？", FakeCatalog(0))
    assert r.status == answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
    assert (
        "无权" not in r.answer and "隐藏" not in r.answer and "没有找到符合条件的文档" in r.answer
    )


def test_inexact_total_is_not_stated_as_exact() -> None:
    r = run("列出所有制度", FakeCatalog(10000, exact=False))
    assert "超过 10000 份" in r.answer
