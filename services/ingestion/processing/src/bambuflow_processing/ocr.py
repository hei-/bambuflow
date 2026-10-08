"""OCR 端口：扫描件与图片的文字识别（技术设计文档 5.2、12.2）。

OCR 模型（PaddleOCR）只装在单独的 OCR 镜像里（ocr/Dockerfile），跑在独立的 CPU 节点池；
processing 经 OcrEngine 接口调用它，单元测试用假的实现。P1 没有 Temporal，OCR 镜像以 HTTP 服务
的形式提供（ocr/server.py），引入 Temporal 后改为 ingest-ocr 任务队列上的 worker，接口不变。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from bambuflow_processing.blocks import Box, join_lines, union

# 识别置信度低于它的行丢弃：宁可少一行，也不把识别错的文字当作原文引用
MIN_SCORE = 0.5
TIMEOUT_SECONDS = 120.0  # 单页（入库管道详设第 11 节：OCR 单文档 30 分钟）


class OcrError(RuntimeError):
    """OCR 服务不可用或返回了不合法的结果；文档留在队列里等待重试。"""


@dataclass(frozen=True)
class OcrLine:
    text: str
    box: Box  # 像素坐标，原点在左上角
    score: float


class OcrEngine(Protocol):
    def recognize(self, image: bytes) -> list[OcrLine]: ...


class HttpOcrEngine:
    """调用 OCR 镜像的 POST /v1/ocr：请求体是图片（PNG 或 JPEG），响应是识别出的文本行。"""

    def __init__(self, base_url: str, client: httpx.Client | None = None) -> None:
        self._url = base_url.rstrip("/") + "/v1/ocr"
        self._client = client or httpx.Client(timeout=TIMEOUT_SECONDS)

    def recognize(self, image: bytes) -> list[OcrLine]:
        try:
            resp = self._client.post(
                self._url, content=image, headers={"Content-Type": "application/octet-stream"}
            )
            resp.raise_for_status()
            body: Any = resp.json()
        except (httpx.HTTPError, ValueError) as e:
            raise OcrError(f"OCR 服务调用失败: {e}") from e
        return parse_response(body)


def parse_response(body: Any) -> list[OcrLine]:
    """在边界上校验 OCR 服务的响应，任何一行不合法就整页失败，不带着错误的坐标入库。"""
    if not isinstance(body, dict) or not isinstance(body.get("lines"), list):
        raise OcrError("OCR 响应缺少 lines")
    out = []
    for i, item in enumerate(body["lines"]):
        try:
            text, box, score = item["text"], item["box"], item["score"]
            coords = tuple(float(v) for v in box)
            score = float(score)
        except (KeyError, TypeError, ValueError) as e:
            raise OcrError(f"OCR 响应第 {i} 行格式不对") from e
        ok = (
            isinstance(text, str)
            and len(coords) == 4
            and all(math.isfinite(v) and v >= 0 for v in coords)
            and coords[2] > coords[0]
            and coords[3] > coords[1]
            and 0.0 <= score <= 1.0
        )
        if not ok:
            raise OcrError(f"OCR 响应第 {i} 行的文本、坐标或置信度不合法")
        out.append(OcrLine(text, (coords[0], coords[1], coords[2], coords[3]), score))
    return out


@dataclass(frozen=True)
class TextLine:
    """版面上的一行字：来自 PDF 文字层或 OCR。size 用来区分标题与正文（字号或行高）。"""

    text: str
    box: Box
    size: float


def ocr_lines(lines: list[OcrLine], scale: float = 1.0) -> list[TextLine]:
    """OCR 结果转成版面行：去掉低置信度与空行，坐标除以渲染倍数换回页面坐标，按阅读顺序排列。"""
    out = []
    for ln in lines:
        if ln.score < MIN_SCORE or not ln.text.strip():
            continue
        x0, y0, x1, y1 = (v / scale for v in ln.box)
        out.append(TextLine(ln.text.strip(), (x0, y0, x1, y1), y1 - y0))
    return reading_order(out)


def reading_order(lines: list[TextLine]) -> list[TextLine]:
    """从上到下、同一行从左到右。纵向重叠超过半个行高的视为同一行。"""
    rows: list[list[TextLine]] = []
    for ln in sorted(lines, key=lambda t: (t.box[1], t.box[0])):
        if rows:
            last = rows[-1][-1]
            overlap = min(last.box[3], ln.box[3]) - max(last.box[1], ln.box[1])
            if overlap > 0.5 * min(last.size, ln.size):
                rows[-1].append(ln)
                continue
        rows.append([ln])
    return [ln for row in rows for ln in sorted(row, key=lambda t: t.box[0])]


@dataclass(frozen=True)
class Paragraph:
    text: str
    box: Box
    heading_level: int  # 0 为正文，1 起为标题级别


# 字号（或行高）比正文大 20% 以上的行当作标题；标题之间小于上一级 85% 的算下一级
HEADING_RATIO = 1.2
LEVEL_RATIO = 0.85
# 与上一行的竖直间距小于行高的 80% 时接成同一段
PARAGRAPH_GAP = 0.8


@dataclass(frozen=True)
class Typography:
    """一份文档（同一种来源）的正文字号与各标题字号的级别。要按整份文档统计：
    只看一页或一段时，标题行占比高，会把标题误当正文。"""

    body: float
    levels: dict[float, int]


def typography(lines: list[TextLine]) -> Typography:
    if not lines:
        return Typography(0.0, {})
    # 按字数加权的中位数：正文字数最多，标题行再多也不会把它拉偏
    ordered = sorted(lines, key=lambda ln: ln.size)
    half, acc, body = sum(len(ln.text) for ln in ordered) / 2, 0, ordered[-1].size
    for ln in ordered:
        acc += len(ln.text)
        if acc >= half:
            body = ln.size
            break
    levels: dict[float, int] = {}
    level, top = 0, math.inf
    for s in sorted({ln.size for ln in lines if ln.size >= HEADING_RATIO * body}, reverse=True):
        if s < LEVEL_RATIO * top:
            level, top = level + 1, s
        levels[s] = level
    return Typography(body, levels)


def paragraphs(lines: list[TextLine], typo: Typography) -> list[Paragraph]:
    """把版面行聚成段落并识别标题。lines 须已按阅读顺序排列；标题行各自成段。"""
    out: list[Paragraph] = []
    cur: list[TextLine] = []

    def flush() -> None:
        if cur:
            text = join_lines(ln.text for ln in cur)
            box = union(ln.box for ln in cur)
            out.append(Paragraph(text, box, typo.levels.get(cur[0].size, 0)))
            cur.clear()

    for ln in lines:
        heading = ln.size in typo.levels
        if cur and (heading or ln.box[1] - cur[-1].box[3] >= PARAGRAPH_GAP * cur[-1].size):
            flush()
        cur.append(ln)
        if heading:
            flush()
    flush()
    return out
