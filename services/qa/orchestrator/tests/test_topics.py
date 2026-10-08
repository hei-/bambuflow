import asyncio
import datetime as dt
import json
import re
from collections.abc import AsyncGenerator, Sequence

from bambuflow_vocabulary import Term, Vocabularies, Vocabulary

from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2
from bambuflow.retrieval.v1 import retrieval_pb2
from bambuflow_grounding import Grounder
from bambuflow_orchestrator import prompt, topics
from bambuflow_orchestrator.llm import LLMUnavailableError


class OverlapNLI:
    async def support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        return [sum(ch in p for ch in h) / len(h) for p, h in pairs], "nli@t"


class Access:
    def __init__(self, denied: frozenset[str] = frozenset()) -> None:
        self.denied = denied

    async def allowed(self, chunk_ids: Sequence[str]) -> dict[str, bool]:
        return {c: c not in self.denied for c in chunk_ids}


VOCAB = Vocabularies(
    "vocab-test",
    Vocabulary([Term("DT-POLICY", ("制度", "管理办法", "办法")), Term("DT-NOTICE", ("通知",))]),
    Vocabulary([Term("DEPT-CREDIT", ("信贷管理部", "信贷部"))]),
    Vocabulary([Term("T-CREDIT", ("信贷", "贷款"))]),
)
TODAY = dt.date(2026, 10, 6)


def test_understand_summary_questions() -> None:
    c = topics.understand("总结一下信贷部今年发布的制度的主要内容", VOCAB, TODAY)
    assert c is not None and c.kind == answer_pb2.ANSWER_KIND_SUMMARY
    assert list(c.filters.doc_types) == ["DT-POLICY"] and list(c.filters.depts) == ["DEPT-CREDIT"]
    assert c.filters.effective_from == "2026-01-01"
    for q in (
        # 只问一份文档：普通问答
        "《个人贷款管理办法》的主要内容是什么？",
        # 没有集合说法
        "个人贷款管理办法的主要内容是什么？",
        # 不是汇总
        "有哪些制度？",
        "授信额度是多少？",
    ):
        assert topics.understand(q, VOCAB, TODAY) is None, q


def summary(i: int) -> str:
    return f"第{i}号制度规定了授信业务第{i}项要求的办理流程。"


class Catalog:
    def __init__(self, total: int, with_summary: int | None = None) -> None:
        self.total = total
        self.with_summary = total if with_summary is None else with_summary
        self.pages: list[str] = []

    async def count(
        self, filters: types_pb2.Filters, group_by: retrieval_pb2.CountGroupBy
    ) -> retrieval_pb2.CountDocumentsResponse:
        return retrieval_pb2.CountDocumentsResponse(
            total_count=self.total, total_is_exact=True, index_version="ix-1", policy_version="p"
        )

    async def list(
        self, filters: types_pb2.Filters, page_size: int, page_token: str = ""
    ) -> retrieval_pb2.ListDocumentsResponse:
        self.pages.append(page_token)
        start = int(page_token or "0")
        end = min(self.total, start + page_size)
        docs = [
            retrieval_pb2.DocumentSummary(
                doc_id=f"d{i}",
                doc_version=1,
                title=f"制度 {i}",
                summary_chunk_id=f"s:d{i}#sum" if i < self.with_summary else "",
            )
            for i in range(start, end)
        ]
        return retrieval_pb2.ListDocumentsResponse(
            documents=docs,
            total_count=self.total,
            next_page_token=str(end) if end < self.total else "",
        )


class Excerpts:
    def __init__(self, hidden: frozenset[str] = frozenset()) -> None:
        self.hidden = hidden
        self.calls = 0

    async def excerpt(self, doc_id: str, chunk_id: str) -> retrieval_pb2.GetExcerptResponse | None:
        self.calls += 1
        if doc_id in self.hidden:
            return None
        i = int(doc_id[1:])
        return retrieval_pb2.GetExcerptResponse(
            chunk_id=chunk_id, doc_id=doc_id, doc_version=1, text=summary(i), title=f"制度 {i}"
        )


_EVIDENCE = re.compile(r'<evidence ref="S(\d+)"[^>]*>\n(.*?)\n</evidence>', re.S)
_GROUP = re.compile(r'<group id="g\d+" title="([^"]*)">\n(.*?)\n</group>', re.S)


class Gen:
    def __init__(self, lines: list[str], fail: bool) -> None:
        self.model = "chat@t"
        self.lines, self.fail = lines, fail

    async def __aiter__(self) -> AsyncGenerator[str, None]:
        if self.fail:
            raise LLMUnavailableError("down")
        for line in self.lines:
            yield line


class LLM:
    """批次：每条证据一个小标题加一条要点（照抄摘要）；合并：前一半与后一半各一组。"""

    def __init__(self, fail_batches: int = 0, fail_merge: bool = False, bad: bool = False) -> None:
        self.fail_batches, self.fail_merge, self.bad = fail_batches, fail_merge, bad
        self.systems: list[str] = []

    def generate(self, system: str, user: str) -> Gen:
        self.systems.append(system)
        if "<groups>" in user:
            pts = [
                p for _, body in _GROUP.findall(user) for p in re.findall(r"^(p\d+)：", body, re.M)
            ]
            half = len(pts) // 2
            out = [
                json.dumps({"title": "合并A", "points": pts[:half]}, ensure_ascii=False),
                # 漏掉最后一个要点，检查程序把它放进“其他”
                json.dumps({"title": "合并B", "points": pts[half:-1]}, ensure_ascii=False),
            ]
            return Gen(out, self.fail_merge)
        fail = self.fail_batches > 0
        self.fail_batches -= 1
        lines = []
        for ref, text in _EVIDENCE.findall(user):
            heading = {
                "block_id": f"h{ref}",
                "block_type": "heading",
                "text": f"主题{ref}",
                "refs": [],
            }
            lines.append(json.dumps(heading, ensure_ascii=False))
            point = text if not self.bad else "所有客户均可享受三年免息优惠。"
            item = {
                "block_id": f"b{ref}",
                "block_type": "list_item",
                "text": f"{point}[S{ref}]",
                "refs": [int(ref)],
                "quote": [text],
                "claim_type": "quote",
            }
            lines.append(json.dumps(item, ensure_ascii=False))
        return Gen(lines, fail)


def run(
    catalog: Catalog, llm: LLM, excerpts: Excerpts | None = None, access: Access | None = None
) -> tuple[answer_pb2.AnswerResponse, list[answer_pb2.StreamAnswerResponse]]:
    c = topics.understand("总结一下所有制度的主要内容", VOCAB, TODAY)
    assert c is not None
    ts = topics.TopicSummary(
        "总结一下所有制度的主要内容",
        c,
        c.filters,
        catalog,
        excerpts or Excerpts(),
        llm,
        Grounder(OverlapNLI(), access or Access()),
    )

    async def go() -> list[answer_pb2.StreamAnswerResponse]:
        return [e async for e in ts.events()]

    events = asyncio.run(go())
    return ts.response(), events


def items(r: answer_pb2.AnswerResponse) -> list[answer_pb2.Sentence]:
    return [s for s in r.sentences if s.block_id.endswith("-items")]


def test_one_batch_groups_points_under_titles_with_citations() -> None:
    r, events = run(Catalog(3), LLM())
    assert (
        r.answer_kind == answer_pb2.ANSWER_KIND_SUMMARY and r.status == answer_pb2.STATUS_ANSWERED
    )
    assert r.why.based_on_summaries and r.why.prompt_version == prompt.TOPIC_PROMPT_VERSION
    headings = [s.text for s in r.sentences if s.claim_type == answer_pb2.CLAIM_TYPE_NONE]
    assert headings[:3] == ["主题1", "主题2", "主题3"]
    pts = items(r)
    assert [p.ref_ids[0] for p in pts] == [1, 2, 3]
    assert {c.chunk_id for c in r.citations} == {"s:d0#sum", "s:d1#sum", "s:d2#sum"}
    scope = next(b.scope for b in r.blocks if b.block_type == answer_pb2.BLOCK_TYPE_SCOPE)
    assert (scope.total_count, scope.shown) == (3, 3)
    assert "基于其中 3 份的摘要归纳" in r.sentences[-1].text
    assert r.blocks[-1].block_type == answer_pb2.BLOCK_TYPE_SCOPE
    assert list(r.audit.shown_chunk_ids) == ["s:d0#sum", "s:d1#sum", "s:d2#sum"]
    for s in r.sentences:
        assert r.answer[s.start : s.end] == s.text
    # citation 先于引用它的句子；final 最后
    assert events[-1].HasField("final") and events[0].HasField("sentence")
    assert events[1].HasField("citation")


def test_many_batches_are_merged_and_unassigned_points_kept() -> None:
    cat = Catalog(45)
    llm = LLM()
    r, _ = run(cat, llm)
    # 45 份分 3 批；45 个要点合并成 2 组，模型漏掉的一个进“其他”
    assert llm.systems.count(prompt.TOPIC_SYSTEM) == 3 and llm.systems[-1] == prompt.MERGE_SYSTEM
    headings = [s.text for s in r.sentences if s.claim_type == answer_pb2.CLAIM_TYPE_NONE][:-1]
    assert headings == ["合并A", "合并B", "其他"]
    assert len(items(r)) == 45
    assert cat.pages == [""]  # 45 份一页取完


def test_collection_is_capped_at_200() -> None:
    cat = Catalog(260)
    r, _ = run(cat, LLM())
    assert cat.pages == ["", "50", "100", "150"]
    note = r.sentences[-1].text
    assert "共 260 份，基于其中 200 份的摘要归纳" in note and "缩小条件" in note


def test_missing_and_invisible_summaries_are_disclosed() -> None:
    r, _ = run(Catalog(4, with_summary=2), LLM(), Excerpts(hidden=frozenset({"d1"})))
    assert len(items(r)) == 1
    assert "另有 3 份文档尚未生成摘要，未纳入归纳。" in r.sentences[-1].text


def test_unsupported_points_are_dropped() -> None:
    r, _ = run(Catalog(2), LLM(bad=True))
    assert r.status == answer_pb2.STATUS_INSUFFICIENT_EVIDENCE and not items(r)


def test_access_is_rechecked_for_cited_summaries() -> None:
    r, _ = run(Catalog(2), LLM(), access=Access(frozenset({"s:d0#sum"})))
    assert [c.chunk_id for c in r.citations] == ["s:d1#sum"]


def test_partial_batches_are_disclosed() -> None:
    r, _ = run(Catalog(30), LLM(fail_batches=1))
    assert r.status == answer_pb2.STATUS_ANSWERED and len(items(r)) == 10
    assert ("summary_batch", "partial") in [(d.stage, d.reason) for d in r.why.degraded]


def test_merge_failure_falls_back_to_document_list() -> None:
    r, _ = run(Catalog(30), LLM(fail_merge=True))
    assert r.answer_kind == answer_pb2.ANSWER_KIND_ENUMERATION and not r.why.based_on_summaries
    listed = [s for s in r.sentences if s.claim_type == answer_pb2.CLAIM_TYPE_LIST_ITEM]
    assert len(listed) == 30 and listed[0].document.doc_id == "d0"
    assert r.sentences[0].text == "暂时无法归纳，以下为相关文档。"
    assert ("summary_merge", "unavailable") in [(d.stage, d.reason) for d in r.why.degraded]


def test_nothing_found() -> None:
    r, _ = run(Catalog(0), LLM())
    assert r.status == answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
    assert r.sentences[-1].text == "没有找到符合条件的文档；可以放宽条件后再试。"
