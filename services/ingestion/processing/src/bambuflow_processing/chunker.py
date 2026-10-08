"""分块（入库管道详设第 6 节）：Markdown 按标题分块；其他格式按解析器输出的块序列分块。

每块都带原文位置：字符区间按 Unicode 字符计（左闭右开），另记行号。Markdown 以去掉
front matter 后的正文为准，其他格式以解析后的全文为准（blocks.assemble），分页文档另记页码与区域。
块不跨标题边界；超过 MAX_CHARS 的节按段落再切。chunk_id 由“标题路径 + 归一化文本”的哈希生成
（入库管道详设第 3 节），文档中间插入或删除一节，只有真正变化的块的 ID 会变。
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass

from bambuflow_processing.blocks import Box, Parsed, Placed, union

# 一个索引版本只允许一个分块器版本（indexing 的待发布版本），所以它同时覆盖 Markdown 分块、
# 各格式的解析器与版面分块；任何一处的输出变化都要提升它
CHUNKER_VERSION = "md-heading-1+layout-1"
MAX_CHARS = 800
# 表格不超过这么多字符时整块保留，否则按行组切分并重复表头（详设第 6 节建议 1000 token）
TABLE_MAX_CHARS = 1500

_HEADING = re.compile(r"^(#{1,6})[ \t]+(.+?)[ \t#]*$")
_FENCE = re.compile(r"^(```|~~~)")


@dataclass(frozen=True)
class Piece:
    heading_path: tuple[str, ...]
    start: int
    end: int
    line_start: int
    line_end: int


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    parent_chunk_id: str
    heading_path: tuple[str, ...]
    text: str
    text_hash: str
    start: int
    end: int
    line_start: int
    line_end: int
    page: int | None = None
    bbox: Box | None = None

    def embedding_input(self) -> str:
        """向量化的输入：前缀标题路径，让脱离上下文的块也能被召回。"""
        prefix = " > ".join(self.heading_path)
        return f"{prefix}\n{self.text}" if prefix else self.text


def _lines(body: str) -> list[tuple[int, str]]:
    """返回每行的起始字符位置与内容（不含换行符）。"""
    out, pos = [], 0
    for line in body.split("\n"):
        out.append((pos, line))
        pos += len(line) + 1
    return out


def _sections(body: str) -> list[Piece]:
    lines = _lines(body)
    path: list[tuple[int, str]] = []
    sections: list[Piece] = []
    start_idx, in_fence = 0, False
    current: tuple[str, ...] = ()

    def close(end_idx: int) -> None:
        # 只保留含正文的节：标题后紧跟下一级标题的“空节”不成块
        content = [
            ln for _, ln in lines[start_idx:end_idx] if ln.strip() and not _HEADING.match(ln)
        ]
        if not content:
            return
        last = end_idx - 1
        while last > start_idx and not lines[last][1].strip():
            last -= 1
        start = lines[start_idx][0]
        end = lines[last][0] + len(lines[last][1])
        sections.append(Piece(current, start, end, start_idx + 1, last + 1))

    for i, (_, line) in enumerate(lines):
        if _FENCE.match(line):
            in_fence = not in_fence
        m = None if in_fence else _HEADING.match(line)
        if not m:
            continue
        close(i)
        level, title = len(m.group(1)), m.group(2).strip()
        path = [(lv, t) for lv, t in path if lv < level] + [(level, title)]
        current = tuple(t for _, t in path)
        start_idx = i
    close(len(lines))
    return sections


def _split_long(body: str, s: Piece) -> list[Piece]:
    """超长的节按空行分段，把相邻段落聚合到不超过 MAX_CHARS；单段超长时整段保留，不在句中切断。"""
    if s.end - s.start <= MAX_CHARS:
        return [s]
    seg = body[s.start : s.end]
    bounds = [0] + [m.end() for m in re.finditer(r"\n[ \t]*\n", seg)] + [len(seg)]
    pieces: list[Piece] = []
    cur = 0
    for i in range(1, len(bounds)):
        if bounds[i] - cur > MAX_CHARS and bounds[i - 1] > cur:
            pieces.append(_piece(body, s.heading_path, s.start + cur, s.start + bounds[i - 1]))
            cur = bounds[i - 1]
    pieces.append(_piece(body, s.heading_path, s.start + cur, s.end))
    return [p for p in pieces if body[p.start : p.end].strip()]


def _piece(body: str, path: tuple[str, ...], start: int, end: int) -> Piece:
    while end > start and body[end - 1] in " \t\n":
        end -= 1
    return Piece(path, start, end, body.count("\n", 0, start) + 1, body.count("\n", 0, end) + 1)


def _normalize(text: str) -> str:
    # 只去掉空白差异与全半角差异，不改变内容（入库管道详设第 3 节）
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", text)).strip()


def _h(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


class _Ids:
    """同一文档内的 chunk_id 分配。"""

    def __init__(self, source_id: str, doc_id: str) -> None:
        self._prefix = f"{source_id}:{doc_id}#"
        self._seen: dict[str, int] = {}

    def make(
        self,
        path: tuple[str, ...],
        text: str,
        start: int,
        end: int,
        lines: tuple[int, int],
        page: int | None = None,
        bbox: Box | None = None,
    ) -> Chunk:
        path_key = "\n".join(path)
        anchor = _h(path_key + "\n" + _normalize(text))[:16]
        self._seen[anchor] = self._seen.get(anchor, 0) + 1
        if self._seen[anchor] > 1:
            # 同一文档里正文完全相同的块（如重复的免责声明）加序号区分
            anchor = f"{anchor}~{self._seen[anchor]}"
        return Chunk(
            chunk_id=self._prefix + anchor,
            parent_chunk_id=self._prefix + "p-" + _h(path_key)[:16],
            heading_path=path,
            text=text,
            text_hash=_h(text),
            start=start,
            end=end,
            line_start=lines[0],
            line_end=lines[1],
            page=page,
            bbox=bbox,
        )


def chunk(source_id: str, doc_id: str, body: str) -> list[Chunk]:
    ids = _Ids(source_id, doc_id)
    out: list[Chunk] = []
    for section in _sections(body):
        for p in _split_long(body, section):
            text = body[p.start : p.end]
            out.append(ids.make(p.heading_path, text, p.start, p.end, (p.line_start, p.line_end)))
    return out


# 摘要块的标题路径：引用时显示为“文档摘要”，与正文的标题区分开
SUMMARY_HEADING = ("文档摘要",)


def summary_chunk(
    source_id: str, doc_id: str, text: str, start: int, end: int, lines: tuple[int, int]
) -> Chunk:
    """文档级摘要块（入库管道详设第 7 节）：ID 与正文块同一规则（标题路径 + 归一化文本的哈希），
    摘要内容不变时 ID 不变；位置覆盖它概括的原文范围，不带页码与区域（概括的是整份文档）。"""
    return _Ids(source_id, doc_id).make(SUMMARY_HEADING, text, start, end, lines)


class MissingSpanError(ValueError):
    """分页文档里有块缺页码或区域：引用无法定位，整份拒收（与 indexing 的内核兜底规则一致）。"""


def chunk_parsed(source_id: str, doc_id: str, parsed: Parsed) -> list[Chunk]:
    """按解析器输出的块分块：同一标题路径、同一页的相邻段落合并到不超过 MAX_CHARS；
    表格与图片单独成块；只有标题、没有正文的组不成块（与 Markdown 的“空节”规则一致）。"""
    for p in parsed.blocks:
        b = p.block
        if (b.page is not None and b.bbox is None) or (b.page is None and b.row_boxes):
            raise MissingSpanError(f"{b.kind} 块缺少页码或区域（第 {b.page} 页）")
    ids = _Ids(source_id, doc_id)
    out: list[Chunk] = []
    group: list[Placed] = []

    def flush() -> None:
        if group and any(p.block.kind != "heading" for p in group):
            out.append(_from_range(ids, parsed.text, group))
        group.clear()

    for p in parsed.blocks:
        b = p.block
        if b.kind in ("table", "image"):
            flush()
            if b.kind == "table" and len(b.text) > TABLE_MAX_CHARS:
                out.extend(_split_table(ids, parsed.text, p))
            else:
                out.append(_from_range(ids, parsed.text, [p]))
            continue
        if group:
            first = group[0].block
            too_long = p.end - group[0].start > MAX_CHARS
            if b.path != first.path or b.page != first.page or too_long:
                flush()
        group.append(p)
    flush()
    return out


def _from_range(ids: _Ids, text: str, group: list[Placed]) -> Chunk:
    start, end = group[0].start, group[-1].end
    first = group[0].block
    boxes = [p.block.bbox for p in group if p.block.bbox is not None]
    return ids.make(
        first.path,
        text[start:end],
        start,
        end,
        _line_range(text, start, end),
        first.page,
        union(boxes) if boxes else None,
    )


def _split_table(ids: _Ids, text: str, p: Placed) -> list[Chunk]:
    """大表按行组切分，每块重复表头；span 指向本组的行（表头在原文中只出现一次）。
    行组的正文至少与表头一样长，否则并入上一组，避免出现“几乎只有表头”的块。"""
    b = p.block
    header, rows = b.rows[0], b.rows[1:]
    offsets = [p.start + len(header) + 1]
    for r in rows[:-1]:
        offsets.append(offsets[-1] + len(r) + 1)
    groups: list[list[int]] = [[]]
    size = len(header)
    for i, r in enumerate(rows):
        if groups[-1] and size + 1 + len(r) > MAX_CHARS:
            groups.append([])
            size = len(header)
        groups[-1].append(i)
        size += 1 + len(r)
    tail = groups[-1]
    if len(groups) > 1 and sum(len(rows[i]) + 1 for i in tail) < len(header):
        groups[-2] += groups.pop()
    out = []
    for g in groups:
        start, end = offsets[g[0]], offsets[g[-1]] + len(rows[g[-1]])
        body = text[start:end]
        bbox = union(b.row_boxes[i + 1] for i in g) if b.row_boxes else None
        out.append(
            ids.make(
                b.path,
                header + "\n" + body,
                start,
                end,
                _line_range(text, start, end),
                b.page,
                bbox,
            )
        )
    return out


def _line_range(text: str, start: int, end: int) -> tuple[int, int]:
    return text.count("\n", 0, start) + 1, text.count("\n", 0, end) + 1
