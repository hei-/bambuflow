"""PDF 解析：文字层直接抽取，没有文字层的扫描页走 OCR（技术设计文档 5.2）。

pypdfium2 打开文档、判断扫描页并渲染给 OCR；文字层的行与表格用 pdfplumber 抽取（都不是 AGPL 许可，
替换了原先的 PyMuPDF）。每块记录页码与页面上的区域（点，原点在左上角），用于引用高亮。按版面处理：
- 字号比正文大的行识别为标题，按字号分级，维护标题路径；
- 有边框的表格用 pdfplumber 的 find_tables 识别，按行保留，第一行为表头；
  表格区域内的文字不再重复成段落；
- 页眉页脚去重：页面上下边距内、在多数页面重复出现的行（数字归一后比较）和页码行不入库。
"""

from __future__ import annotations

import io
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import pdfplumber
import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
from pdfplumber.page import Page as PlumberPage
from pdfplumber.utils.exceptions import PdfminerException

from bambuflow_processing.blocks import Block, Box, Outline, ParseError, table_text, union
from bambuflow_processing.images import ImageDescriber, image_text
from bambuflow_processing.ocr import (
    OcrEngine,
    TextLine,
    Typography,
    ocr_lines,
    paragraphs,
    reading_order,
    typography,
)

# 扫描页的渲染倍数：2 倍即 144 dpi，PaddleOCR 对 10 磅以上的中文足够
OCR_ZOOM = 2.0
# 文字层少于这么多字符、且有图片的页面视为扫描页
MIN_TEXT_CHARS = 10
# 小于 64 × 64 像素的内嵌图片多是图标与装饰，跳过（技术设计文档 5.5）
MIN_IMAGE_SIDE = 64
# 页眉页脚所在的上下边距（页面高度的比例）
MARGIN = 0.08
# 同一行里两个词的间距超过这么多倍字号就断开：多栏版面的左右两栏不拼成一行
COLUMN_GAP = 2.0
# 词间距超过这么多倍字号时补一个空格（PDF 里的空格不一定是字符，常常只是间距）
SPACE_GAP = 0.15
_PAGE_NUMBER = re.compile(r"^(第\s*\d+\s*页.*|-?\s*\d+\s*-?|\d+\s*/\s*\d+|page \d+.*)$", re.I)


class OcrRequiredError(ParseError):
    """文档含扫描页但没有配置 OCR：不能跳过这些页面悄悄入库半份文档。"""


@dataclass(frozen=True)
class _Table:
    rows: tuple[str, ...]
    row_boxes: tuple[Box, ...]
    box: Box


@dataclass(frozen=True)
class _Image:
    """有文字层的页里的内嵌图片（入库管道详设第 6 节）：
    OCR 文字加描述，区域是图片在页面上的位置。"""

    text: str
    box: Box


@dataclass
class _Page:
    height: float
    ocr: bool = False
    # 按纵坐标排好序的行与表格
    items: list[TextLine | _Table | _Image] = field(default_factory=list)


def parse_pdf(
    data: bytes, title: str, ocr: OcrEngine | None, describe: ImageDescriber | None = None
) -> list[Block]:
    try:
        doc = pdfium.PdfDocument(data)
    except pdfium.PdfiumError as e:
        if e.err_code == pdfium_c.FPDF_ERR_PASSWORD:
            raise ParseError("加密的 PDF 不入库") from e
        raise ParseError(f"PDF 无法打开: {e}") from e
    try:
        with pdfplumber.open(io.BytesIO(data)) as plumb:
            pages = [
                _read_page(no, doc[no], plumb.pages[no], ocr, describe) for no in range(len(doc))
            ]
    except PdfminerException as e:
        raise ParseError(f"PDF 无法解析: {e}") from e
    finally:
        doc.close()
    repeated = _repeated_margin_lines(pages)
    for p in pages:
        p.items = [
            it
            for it in p.items
            if not (isinstance(it, TextLine) and _is_margin_noise(it, p.height, repeated))
        ]
    # 文字层与 OCR 的“字号”含义不同（字号与行高），各自统计
    typo = {
        kind: typography([it for p in pages if p.ocr == kind for it in _lines(p.items)])
        for kind in (False, True)
    }
    outline = Outline()
    blocks: list[Block] = []
    for no, p in enumerate(pages, start=1):
        blocks += _page_blocks(no, p.items, typo[p.ocr], outline, title)
    return blocks


def _lines(items: Iterable[TextLine | _Table | _Image]) -> list[TextLine]:
    return [it for it in items if isinstance(it, TextLine)]


def _read_page(
    index: int,
    page: pdfium.PdfPage,
    plumb: PlumberPage,
    ocr: OcrEngine | None,
    describe: ImageDescriber | None = None,
) -> _Page:
    out = _Page(page.get_height())
    textpage = page.get_textpage()
    try:
        scanned = len(textpage.get_text_range().strip()) < MIN_TEXT_CHARS
    finally:
        textpage.close()
    if scanned:
        if not any(page.get_objects(filter=(pdfium_c.FPDF_PAGEOBJ_IMAGE,))):
            return out
        if ocr is None:
            raise OcrRequiredError(f"第 {index + 1} 页是扫描页，需要 OCR 但没有配置")
        png = io.BytesIO()
        page.render(scale=OCR_ZOOM).to_pil().save(png, format="PNG")
        out.ocr = True
        out.items = list(ocr_lines(ocr.recognize(png.getvalue()), OCR_ZOOM))
        return out
    tables: list[_Table] = []
    for tab in plumb.find_tables():
        rows = tab.extract()
        if len(rows) < 2:
            continue
        rendered = table_text(rows)
        boxes = tuple(_box(r.bbox) for r in tab.rows)
        if len(rendered) != len(boxes):
            # 有空行被去掉时行与区域对不上，按行切分的块会定位错，整份拒收
            raise ParseError(f"第 {index + 1} 页的表格有空行，无法逐行定位")
        tables.append(_Table(rendered, boxes, _box(tab.bbox)))
    lines = [ln for ln in _text_lines(plumb) if not any(_inside(ln.box, t.box) for t in tables)]
    images = _embedded_images(page, out.height, ocr, describe)
    items: list[TextLine | _Table | _Image] = [*reading_order(lines), *tables, *images]
    out.items = sorted(items, key=lambda it: it.box[1])
    return out


def _embedded_images(
    page: pdfium.PdfPage, height: float, ocr: OcrEngine | None, describe: ImageDescriber | None
) -> list[_Image]:
    """有文字层的页里的内嵌图片：OCR 加描述生成图片块，挂在所属章节下；小于 64 × 64 像素的图标跳过。
    OCR 与描述都没配置时不处理：正文已经有文字层，图片只是补充，不因此让整份文档等待。"""
    if ocr is None and describe is None:
        return []
    out: list[_Image] = []
    for obj in page.get_objects(filter=(pdfium_c.FPDF_PAGEOBJ_IMAGE,)):
        if not isinstance(obj, pdfium.PdfImage):
            continue
        w, h = obj.get_px_size()
        if w < MIN_IMAGE_SIDE or h < MIN_IMAGE_SIDE:
            continue
        png = io.BytesIO()
        obj.get_bitmap(render=True).to_pil().save(png, format="PNG")
        text, _ = image_text(png.getvalue(), ocr, describe)
        if not text:
            continue
        # pdfium 的坐标原点在左下角，块的区域与 pdfplumber 一样以左上角为原点
        left, bottom, right, top = obj.get_bounds()
        out.append(_Image(text, _box((left, height - top, right, height - bottom))))
    return out


def _text_lines(page: PlumberPage) -> list[TextLine]:
    """把词拼成行：纵向重叠过半个字号、横向间距不到 COLUMN_GAP 倍字号的词属于同一行。"""
    words: list[dict[str, Any]] = page.extract_words(extra_attrs=["size"])
    out: list[TextLine] = []
    cur: list[dict[str, Any]] = []

    def flush() -> None:
        if cur:
            text = cur[0]["text"]
            for prev, w in zip(cur, cur[1:], strict=False):
                gap = w["x0"] - prev["x1"]
                text += (" " if gap > SPACE_GAP * w["size"] else "") + w["text"]
            box = union((w["x0"], w["top"], w["x1"], w["bottom"]) for w in cur)
            out.append(TextLine(text.strip(), _box(box), round(max(w["size"] for w in cur), 1)))
            cur.clear()

    for w in sorted(words, key=lambda w: (round(w["top"]), w["x0"])):
        if cur:
            last = cur[-1]
            size = min(last["size"], w["size"])
            overlap = min(last["bottom"], w["bottom"]) - max(last["top"], w["top"])
            if overlap > 0.5 * size and 0 <= w["x0"] - last["x1"] < COLUMN_GAP * size:
                cur.append(w)
                continue
            flush()
        cur.append(w)
    flush()
    return [ln for ln in out if ln.text]


def _page_blocks(
    no: int,
    items: list[TextLine | _Table | _Image],
    typo: Typography,
    outline: Outline,
    title: str,
) -> list[Block]:
    """把一页的元素转成块：连续的行聚成段落（识别标题），表格单独成块。"""
    out: list[Block] = []
    run: list[TextLine] = []

    def flush() -> None:
        for p in paragraphs(run, typo):
            if p.heading_level:
                path = outline.heading(p.heading_level, p.text)
                out.append(Block("heading", p.text, path, no, p.box))
            else:
                out.append(Block("paragraph", p.text, outline.path or (title,), no, p.box))
        run.clear()

    for it in items:
        if isinstance(it, TextLine):
            run.append(it)
            continue
        flush()
        path = outline.path or (title,)
        if isinstance(it, _Image):
            out.append(Block("image", it.text, path, no, it.box))
            continue
        out.append(Block("table", "\n".join(it.rows), path, no, it.box, it.rows, it.row_boxes))
    flush()
    return out


def _repeated_margin_lines(pages: list[_Page]) -> set[str]:
    if len(pages) < 2:
        return set()
    counts: Counter[str] = Counter()
    for p in pages:
        counts.update(
            {_margin_key(ln.text) for ln in _lines(p.items) if _in_margin(ln.box, p.height)}
        )
    return {k for k, n in counts.items() if n >= 2 and n * 2 >= len(pages)}


def _is_margin_noise(line: TextLine, height: float, repeated: set[str]) -> bool:
    if not _in_margin(line.box, height):
        return False
    return _margin_key(line.text) in repeated or bool(_PAGE_NUMBER.match(line.text.strip()))


def _in_margin(box: Box, height: float) -> bool:
    return box[3] <= MARGIN * height or box[1] >= (1 - MARGIN) * height


def _margin_key(text: str) -> str:
    return re.sub(r"\d+", "#", "".join(text.split()))


def _inside(inner: Box, outer: Box) -> bool:
    cx, cy = (inner[0] + inner[2]) / 2, (inner[1] + inner[3]) / 2
    return outer[0] <= cx <= outer[2] and outer[1] <= cy <= outer[3]


def _box(b: Iterable[float]) -> Box:
    x0, y0, x1, y1 = (round(float(v), 2) for v in b)
    return (x0, y0, x1, y1)
