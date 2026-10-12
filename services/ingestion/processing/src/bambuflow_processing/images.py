"""图片的文字：OCR 加多模态描述（入库管道详设第 6 节“文档内嵌图片”、第 7 节）。

图片块的正文是“图片说明：<描述>”加 OCR 出的文字；向量化与检索都用这段文字
（只维护一种向量空间，入库管道详设第 8 节）。描述模型经 ImageDescriber 端口接入，
与 OCR 一样是平台内部部署的服务（POST /v1/describe），不配置时只有 OCR 文字。
描述只是辅助召回的说明，之后照常经过 PII 掩码；它不是原文，引用时以图片所在的页与区域定位。
"""

from __future__ import annotations

from typing import Any, Protocol

import httpx

from bambuflow_processing.blocks import Box, union
from bambuflow_processing.ocr import OcrEngine, ocr_lines, paragraphs, typography

TIMEOUT_SECONDS = 120.0
# 描述最长保留的字数：说明性文字，过长的多半是模型复述了图里的全部文字（那部分由 OCR 给出）
MAX_DESCRIPTION = 300


class ImageDescribeError(RuntimeError):
    """描述服务不可用或返回了不合法的结果；文档留在队列里等待重试。"""


class ImageDescriber(Protocol):
    def describe(self, image: bytes) -> str: ...


class HttpImageDescriber:
    """调用描述服务的 POST /v1/describe：请求体是图片，响应是 {"description": "..."}。"""

    def __init__(self, base_url: str, client: httpx.Client | None = None) -> None:
        self._url = base_url.rstrip("/") + "/v1/describe"
        self._client = client or httpx.Client(timeout=TIMEOUT_SECONDS)

    def describe(self, image: bytes) -> str:
        try:
            resp = self._client.post(
                self._url, content=image, headers={"Content-Type": "application/octet-stream"}
            )
            resp.raise_for_status()
            body: Any = resp.json()
        except (httpx.HTTPError, ValueError) as e:
            raise ImageDescribeError(f"图片描述服务调用失败: {e}") from e
        if not isinstance(body, dict) or not isinstance(body.get("description"), str):
            raise ImageDescribeError("图片描述响应缺少 description")
        return str(body["description"]).strip()[:MAX_DESCRIPTION]


def image_text(
    data: bytes, ocr: OcrEngine | None, describe: ImageDescriber | None
) -> tuple[str, Box | None]:
    """图片块的正文与 OCR 文字的外接矩形（像素，没有 OCR 文字时为 None）。两者都没有时正文为空。"""
    parts: list[str] = []
    box: Box | None = None
    if describe is not None and (d := describe.describe(data)):
        parts.append("图片说明：" + d)
    if ocr is not None:
        lines = ocr_lines(ocr.recognize(data))
        paras = paragraphs(lines, typography(lines))
        if paras:
            parts.append("\n".join(p.text for p in paras))
            box = union(p.box for p in paras)
    return "\n".join(parts), box
