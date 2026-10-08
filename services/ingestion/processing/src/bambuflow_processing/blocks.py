"""解析器的共同输出：带版面结构与原文位置的块序列（入库管道详设第 6 节）。

所有解析器（PDF、Office、邮件、图片）都输出 Block 序列，再由 assemble 拼成“解析后的全文”：
块之间以空行分隔，每块记下自己在全文中的字符区间（左闭右开）。块的 span 由此而来——字符区间
以解析后的全文为准（契约 common.v1.Span），分页文档另记页码与页面上的区域，用于引用高亮。
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

# (x0, y0, x1, y1)：PDF 以点为单位、原点在页面左上角；图片以像素为单位
Box = tuple[float, float, float, float]
Kind = Literal["heading", "paragraph", "table", "image"]

SEPARATOR = "\n\n"


class ParseError(ValueError):
    """文件无法解析：格式不支持、文件损坏、加密等。不可重试（技术设计文档 5.6）。"""


class UnsupportedFormatError(ParseError):
    """没有对应解析器的内容类型。"""


@dataclass(frozen=True)
class Block:
    kind: Kind
    text: str  # 表格为各行按换行连接，第一行是表头
    path: tuple[str, ...]  # 所属标题路径；标题块的路径包含它自己
    page: int | None = None  # 从 1 开始，分页文档才有
    bbox: Box | None = None
    rows: tuple[str, ...] = ()  # 只有表格：各行文本，rows[0] 为表头
    row_boxes: tuple[Box, ...] = ()  # 只有分页文档里的表格：各行的区域，与 rows 一一对应


@dataclass(frozen=True)
class Placed:
    block: Block
    start: int
    end: int


@dataclass(frozen=True)
class Parsed:
    text: str  # 解析后的全文，字符区间都以它为准
    blocks: tuple[Placed, ...]


def assemble(blocks: Iterable[Block]) -> Parsed:
    parts: list[str] = []
    placed: list[Placed] = []
    pos = 0
    for b in blocks:
        if not b.text.strip():
            continue
        if parts:
            parts.append(SEPARATOR)
            pos += len(SEPARATOR)
        parts.append(b.text)
        placed.append(Placed(b, pos, pos + len(b.text)))
        pos += len(b.text)
    return Parsed("".join(parts), tuple(placed))


def union(boxes: Iterable[Box]) -> Box:
    bs = list(boxes)
    if not bs:
        raise ValueError("没有可合并的区域")
    return (
        min(b[0] for b in bs),
        min(b[1] for b in bs),
        max(b[2] for b in bs),
        max(b[3] for b in bs),
    )


def table_text(rows: Iterable[Iterable[str | None]]) -> tuple[str, ...]:
    """表格每行渲染成“| a | b |”，单元格内的换行改成空格；空行去掉。"""
    out = []
    for row in rows:
        cells = [" ".join((c or "").split()) for c in row]
        if any(cells):
            out.append("| " + " | ".join(cells) + " |")
    return tuple(out)


def join_lines(lines: Iterable[str]) -> str:
    """把一段里换行切开的各行接起来：中文直接相连，两侧都是英文字母或数字时补一个空格。"""
    out = ""
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if out and out[-1].isascii() and out[-1].isalnum() and line[0].isascii():
            out += " "
        out += line
    return out


class Outline:
    """按标题级别维护当前的标题路径（与 Markdown 分块器的规则相同：遇到同级或更高级标题时回退）。"""

    def __init__(self, root: tuple[str, ...] = ()) -> None:
        self._root = root
        self._stack: list[tuple[int, str]] = []

    def heading(self, level: int, title: str) -> tuple[str, ...]:
        self._stack = [(lv, t) for lv, t in self._stack if lv < level] + [(level, title)]
        return self.path

    @property
    def path(self) -> tuple[str, ...]:
        return self._root + tuple(t for _, t in self._stack)
