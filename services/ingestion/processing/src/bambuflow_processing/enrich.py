"""摘要与关键词（入库管道详设第 7 节）：文档级与章节级各一份。

文档级摘要单独存为摘要块（documents.summary_chunk_id），供主题汇总类问题归纳和引用
（K2 详设第 6 节），权限与密级随所属文档；章节级摘要与关键词随父块（章节）保存。

默认实现是抽取式的，不依赖模型（ADR-13：外部系统经端口接入并自带默认实现）：
- 文档摘要：按章节顺序取每节第一句完整的句子，到 MAX_SUMMARY_CHARS 为止；
- 章节摘要：该节开头的句子，到 MAX_SECTION_CHARS 为止；
- 关键词：正文里出现的受控词表说法（主题、部门、文档类型，取规范名称）加上一、二级标题。
同一份内容的结果总是相同的：内容没变时摘要块的 ID 不变，重复入库不会产生新的内容版本。

模型摘要经 SummaryModel 端口接入，默认不接：设计要求它经 LLM 网关按文档密级路由、用量计入租户配额，
由网关侧的实现负责。模型调用失败时退回抽取式结果（增强失败不阻塞入库）。
可疑指令标记为真的块（I2）不参与摘要：摘要会进入主题汇总的上下文，不能把可疑内容带进去。
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from bambuflow_vocabulary import Vocabularies

from bambuflow_processing.chunker import Chunk, summary_chunk

EXTRACTIVE_VERSION = "extractive-1"
MAX_SUMMARY_CHARS = 300
MAX_SECTION_CHARS = 120
MAX_KEYWORDS = 10
# 短于它的句子多半是条目编号或残句，不作摘要句
MIN_SENTENCE_CHARS = 8

_log = logging.getLogger(__name__)

_SENTENCE = re.compile(r"[^。！？；!?;\n]+[。！？；!?;]?")
# 行首的版式记号：列表、编号、引用、表格竖线
_LEADING_MARKS = re.compile(r"^(?:[-*+]\s+|\d+[.、]\s*|>\s*|\|\s*)+")


@dataclass(frozen=True)
class SectionSummary:
    parent_chunk_id: str
    heading_path: tuple[str, ...]
    summary: str
    keywords: tuple[str, ...]


@dataclass(frozen=True)
class Summary:
    text: str
    keywords: tuple[str, ...]
    sections: tuple[SectionSummary, ...]
    version: str


@dataclass(frozen=True)
class Enrichment:
    summary_chunk: Chunk
    keywords: tuple[str, ...]
    sections: tuple[SectionSummary, ...]
    version: str


class SummaryModel(Protocol):
    """模型摘要端口：实现方经 LLM 网关调用模型，按 classification 路由（受限级用独立实例）。"""

    version: str

    def summarize(self, title: str, chunks: Sequence[Chunk], classification: int) -> Summary: ...


@dataclass
class Enricher:
    vocab: Vocabularies
    # 不配置时只有抽取式摘要
    model: SummaryModel | None = None

    def enrich(
        self,
        source_id: str,
        doc_id: str,
        title: str,
        chunks: Sequence[Chunk],
        classification: int,
        flagged: frozenset[str] = frozenset(),
    ) -> Enrichment | None:
        """没有可用的正文（全部为空或都带可疑指令标记）时返回 None：不生成摘要块。"""
        usable = [c for c in chunks if c.chunk_id not in flagged]
        if not usable:
            return None
        s = self._summarize(title, usable, classification)
        if not s.text:
            return None
        # 摘要块的位置覆盖它概括的原文范围；摘要比原文还长时截断，保证引用定位不越界
        start = min(c.start for c in chunks)
        end = max(c.end for c in chunks)
        lines = (min(c.line_start for c in chunks), max(c.line_end for c in chunks))
        text = s.text[: end - start]
        return Enrichment(
            summary_chunk(source_id, doc_id, text, start, end, lines),
            s.keywords,
            s.sections,
            s.version,
        )

    def _summarize(self, title: str, chunks: Sequence[Chunk], classification: int) -> Summary:
        if self.model is not None:
            try:
                got = self.model.summarize(title, chunks, classification)
            except (OSError, ValueError, RuntimeError) as e:
                # 增强失败不阻塞入库（入库管道详设第 7 节）：用抽取式结果，下次重处理时再试
                _log.warning("模型摘要失败，改用抽取式摘要", extra={"reason": str(e)})
            else:
                parents = {c.parent_chunk_id for c in chunks}
                # 模型的输出不可信：章节必须属于本文档，摘要不能为空
                if got.text.strip():
                    return Summary(
                        got.text.strip(),
                        tuple(dict.fromkeys(k for k in got.keywords if k.strip()))[:MAX_KEYWORDS],
                        tuple(
                            sec
                            for sec in got.sections
                            if sec.parent_chunk_id in parents and sec.summary.strip()
                        ),
                        got.version,
                    )
        return extractive(self.vocab, chunks)


def extractive(vocab: Vocabularies, chunks: Sequence[Chunk]) -> Summary:
    sections: dict[str, list[Chunk]] = {}
    for c in chunks:
        sections.setdefault(c.parent_chunk_id, []).append(c)
    doc_sentences: list[str] = []
    used = 0
    out_sections: list[SectionSummary] = []
    for parent, group in sections.items():
        sentences = [s for c in group for s in sentences_of(c.text)]
        if not sentences:
            continue
        lead = sentences[0]
        if lead not in doc_sentences and used + len(lead) <= MAX_SUMMARY_CHARS:
            doc_sentences.append(lead)
            used += len(lead)
        section_text = _take(sentences, MAX_SECTION_CHARS)
        path = group[0].heading_path
        heading = (path[-1],) if path else ()
        out_sections.append(
            SectionSummary(
                parent,
                path,
                section_text,
                _keywords(vocab, " ".join(c.text for c in group), heading),
            )
        )
    headings = [h for c in chunks for h in c.heading_path[:2]]
    return Summary(
        "".join(doc_sentences),
        _keywords(vocab, " ".join(c.text for c in chunks), headings),
        tuple(out_sections),
        EXTRACTIVE_VERSION,
    )


def sentences_of(text: str) -> list[str]:
    """正文里够长的句子，去掉行首的版式记号；句子都是原文的连续片段（去掉记号之后）。"""
    out: list[str] = []
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("#"):
            continue  # 标题不是正文句子
        line = _LEADING_MARKS.sub("", line)
        for m in _SENTENCE.finditer(line):
            s = m.group(0).strip()
            if len(s) >= MIN_SENTENCE_CHARS and "|" not in s:
                out.append(s)
    return out


def _take(sentences: Sequence[str], limit: int) -> str:
    out = ""
    for s in sentences:
        if out and len(out) + len(s) > limit:
            break
        out += s
    return out[:limit]


def _keywords(vocab: Vocabularies, text: str, headings: Sequence[str]) -> tuple[str, ...]:
    found: list[str] = []
    for v in (vocab.topic, vocab.dept, vocab.doc_type):
        found.extend(v.name(code) for code in v.contained(text))
    found.extend(h.strip() for h in headings if h.strip())
    return tuple(dict.fromkeys(found))[:MAX_KEYWORDS]
