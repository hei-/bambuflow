from collections.abc import Sequence

from bambuflow_processing.chunker import SUMMARY_HEADING, Chunk, chunk
from bambuflow_processing.enrich import (
    EXTRACTIVE_VERSION,
    MAX_SUMMARY_CHARS,
    Enricher,
    SectionSummary,
    Summary,
    sentences_of,
)
from bambuflow_vocabulary import Term, Vocabularies, Vocabulary

VOCAB = Vocabularies(
    "vocab-test",
    Vocabulary([Term("DT-POLICY", ("制度", "管理办法"), "")]),
    Vocabulary([Term("DEPT-CREDIT", ("信贷管理部", "信贷部"), "")]),
    Vocabulary([Term("TOPIC-RETAIL-CREDIT", ("零售信贷", "个人贷款"), "")]),
)

BODY = """# 个人贷款管理办法

## 总则

本办法适用于个人贷款的受理、审批与发放。个人贷款由信贷部归口管理。

## 额度

- 单户个人贷款额度不超过人民币 100 万元。
- 超过额度的须报总行审批。

## 附则

本办法自发布之日起施行。
"""


def chunks() -> list[Chunk]:
    return chunk("src_wiki", "loan", BODY)


def test_extractive_summary_takes_the_lead_sentence_of_each_section() -> None:
    e = Enricher(VOCAB).enrich("src_wiki", "loan", "个人贷款管理办法", chunks(), 2)
    assert e is not None and e.version == EXTRACTIVE_VERSION
    s = e.summary_chunk
    assert s.text == (
        "本办法适用于个人贷款的受理、审批与发放。"
        "单户个人贷款额度不超过人民币 100 万元。"
        "本办法自发布之日起施行。"
    )
    assert s.heading_path == SUMMARY_HEADING
    assert s.chunk_id.startswith("src_wiki:loan#") and len(s.chunk_id.split("#")[1]) == 16
    # 位置覆盖全部正文块，长度满足 indexing 的校验（区间不短于正文的一半）
    cs = chunks()
    assert (s.start, s.end) == (min(c.start for c in cs), max(c.end for c in cs))
    assert s.end - s.start >= len(s.text) / 2
    # 关键词：词表里的规范名称在前，再是一、二级标题
    assert e.keywords[:2] == ("零售信贷", "信贷管理部")
    assert {"个人贷款管理办法", "额度"} <= set(e.keywords)
    assert [sec.heading_path[-1] for sec in e.sections] == ["总则", "额度", "附则"]
    assert e.sections[1].summary.startswith("单户个人贷款额度不超过人民币 100 万元。")


def test_summary_is_deterministic() -> None:
    a = Enricher(VOCAB).enrich("src_wiki", "loan", "t", chunks(), 2)
    b = Enricher(VOCAB).enrich("src_wiki", "loan", "t", chunks(), 2)
    assert a == b


def test_flagged_chunks_are_left_out_of_the_summary() -> None:
    cs = chunks()
    flagged = frozenset({cs[0].chunk_id})
    e = Enricher(VOCAB).enrich("src_wiki", "loan", "t", cs, 2, flagged)
    assert e is not None and "适用于" not in e.summary_chunk.text
    assert all(sec.parent_chunk_id != cs[0].parent_chunk_id for sec in e.sections)
    # 全部可疑时不生成摘要块
    assert (
        Enricher(VOCAB).enrich("src_wiki", "loan", "t", cs, 2, frozenset(c.chunk_id for c in cs))
        is None
    )


def test_no_usable_sentences_means_no_summary() -> None:
    assert (
        Enricher(VOCAB).enrich(
            "src_wiki", "x", "t", chunk("src_wiki", "x", "# 标题\n\n短句。\n"), 2
        )
        is None
    )


def test_summary_length_is_capped() -> None:
    body = "\n\n".join(
        f"## 第{i}节\n\n这是第{i}节的第一句话，内容比较长一些以便测试上限。" for i in range(40)
    )
    e = Enricher(VOCAB).enrich("src_wiki", "long", "t", chunk("src_wiki", "long", body), 2)
    assert e is not None and len(e.summary_chunk.text) <= MAX_SUMMARY_CHARS


def test_sentences_strip_layout_marks() -> None:
    assert sentences_of("- 第一条 贷款额度上限为一百万元。\n| 表头 | 值 |") == [
        "第一条 贷款额度上限为一百万元。"
    ]


class FailingModel:
    version = "model-x"

    def summarize(self, title: str, chunks: Sequence[Chunk], classification: int) -> Summary:
        raise RuntimeError("网关不可用")


class FixedModel:
    version = "model-y"

    def summarize(self, title: str, chunks: Sequence[Chunk], classification: int) -> Summary:
        return Summary(
            "模型生成的摘要。",
            ("贷款", "贷款", " "),
            (
                SectionSummary(chunks[0].parent_chunk_id, (), "第一节。", ()),
                SectionSummary("别的文档#p-1", (), "不属于本文档。", ()),
            ),
            self.version,
        )


def test_model_failure_falls_back_to_extractive() -> None:
    e = Enricher(VOCAB, FailingModel()).enrich("src_wiki", "loan", "t", chunks(), 2)
    assert e is not None and e.version == EXTRACTIVE_VERSION


def test_model_output_is_checked() -> None:
    e = Enricher(VOCAB, FixedModel()).enrich("src_wiki", "loan", "t", chunks(), 2)
    assert e is not None and e.version == "model-y"
    assert e.summary_chunk.text == "模型生成的摘要。"
    assert e.keywords == ("贷款",)
    assert [s.summary for s in e.sections] == ["第一节。"]
