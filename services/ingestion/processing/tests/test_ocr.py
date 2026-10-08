"""OCR 端口：HTTP 适配器的请求与响应校验（真实 PaddleOCR 由 OCR 镜像的冒烟测试覆盖）。"""

from typing import Any

import httpx
import pytest
from bambuflow_processing.ocr import HttpOcrEngine, OcrError, OcrLine, parse_response


def engine(handler: Any) -> HttpOcrEngine:
    return HttpOcrEngine("http://ocr:8000/", httpx.Client(transport=httpx.MockTransport(handler)))


def test_http_engine_posts_image_and_parses_lines() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(
            200,
            json={
                "model": "PP-OCRv5_mobile_det+PP-OCRv5_server_rec",
                "lines": [{"text": "访客登记须知", "box": [10, 20, 300, 60], "score": 0.99}],
            },
        )

    lines = engine(handler).recognize(b"\x89PNG...")
    assert lines == [OcrLine("访客登记须知", (10.0, 20.0, 300.0, 60.0), 0.99)]
    assert seen[0].url == "http://ocr:8000/v1/ocr" and seen[0].content == b"\x89PNG..."


def test_http_engine_failures_raise_ocr_error() -> None:
    with pytest.raises(OcrError):
        engine(lambda req: httpx.Response(503)).recognize(b"img")
    with pytest.raises(OcrError):
        engine(lambda req: httpx.Response(200, content=b"not json")).recognize(b"img")

    def refuse(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("连接被拒绝")

    with pytest.raises(OcrError):
        engine(refuse).recognize(b"img")


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"lines": "x"},
        {"lines": [{"text": "a", "box": [0, 0, 10], "score": 0.9}]},  # 坐标不足 4 个
        {"lines": [{"text": "a", "box": [10, 0, 5, 10], "score": 0.9}]},  # 左右颠倒
        {"lines": [{"text": "a", "box": [-1, 0, 5, 10], "score": 0.9}]},  # 负坐标
        {"lines": [{"text": "a", "box": [0, 0, 5, 10], "score": 1.5}]},  # 置信度越界
        {"lines": [{"text": 3, "box": [0, 0, 5, 10], "score": 0.9}]},  # 文本不是字符串
        {"lines": [{"box": [0, 0, 5, 10], "score": 0.9}]},  # 缺文本
    ],
)
def test_invalid_responses_rejected(body: Any) -> None:
    with pytest.raises(OcrError):
        parse_response(body)
