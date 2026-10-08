"""按内容类型选择解析器（技术设计文档 5.2 多模态处理），输出 Block 序列。

文本类（Markdown）仍走 chunker.chunk；这里处理二进制原文：PDF、Word、Excel、CSV、邮件、图片。
"""

from __future__ import annotations

import io
from functools import partial

from PIL import Image, UnidentifiedImageError

from bambuflow_processing.blocks import Block, ParseError, UnsupportedFormatError, union
from bambuflow_processing.mail import parse_eml
from bambuflow_processing.ocr import OcrEngine, ocr_lines, paragraphs, typography
from bambuflow_processing.office import parse_csv, parse_docx, parse_xlsx
from bambuflow_processing.pdf import OcrRequiredError, parse_pdf

DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
# 小于 64 × 64 像素的图片多是图标，跳过（技术设计文档 5.5）
MIN_IMAGE_SIDE = 64


def parse(content_type: str, data: bytes, title: str, ocr: OcrEngine | None) -> list[Block]:
    if content_type == "application/pdf":
        return parse_pdf(data, title, ocr)
    if content_type == DOCX:
        return parse_docx(data, title)
    if content_type == XLSX:
        return parse_xlsx(data, title)
    if content_type == "text/csv":
        return parse_csv(data, title)
    if content_type == "message/rfc822":
        return parse_eml(data, title, partial(_attachment, ocr=ocr))
    if content_type in ("image/png", "image/jpeg"):
        return parse_image(data, title, ocr)
    raise UnsupportedFormatError(f"不支持的内容类型 {content_type}")


def _attachment(content_type: str, data: bytes, title: str, ocr: OcrEngine | None) -> list[Block]:
    return parse(content_type, data, title, ocr)


def parse_image(data: bytes, title: str, ocr: OcrEngine | None) -> list[Block]:
    """一图一块（技术设计文档 5.2）：OCR 出的各段按换行连接，区域取全部文字的外接矩形（像素）。"""
    try:
        with Image.open(io.BytesIO(data)) as img:
            width, height = img.size
    except (UnidentifiedImageError, OSError) as e:
        raise ParseError(f"图片无法解码: {e}") from e
    if width < MIN_IMAGE_SIDE or height < MIN_IMAGE_SIDE:
        return []
    if ocr is None:
        raise OcrRequiredError("图片需要 OCR 但没有配置")
    lines = ocr_lines(ocr.recognize(data))
    paras = paragraphs(lines, typography(lines))
    if not paras:
        return []
    text = "\n".join(p.text for p in paras)
    return [Block("image", text, (title,), bbox=union(p.box for p in paras))]
