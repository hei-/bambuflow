import time

import pytest
from bambuflow_processing.blocks import Block, UnsupportedFormatError
from bambuflow_processing.images import ImageDescriber
from bambuflow_processing.ocr import OcrEngine
from bambuflow_processing.sandbox import ParseCrashedError, ParseTimeoutError, SandboxParser

# 子进程按 spawn 启动，解析函数须是模块级函数（按名字在子进程里导入）


def hang(
    ct: str, data: bytes, title: str, ocr: OcrEngine | None, describe: ImageDescriber | None
) -> list[Block]:
    time.sleep(60)
    return []


def crash(
    ct: str, data: bytes, title: str, ocr: OcrEngine | None, describe: ImageDescriber | None
) -> list[Block]:
    import os

    os._exit(3)


def echo(
    ct: str, data: bytes, title: str, ocr: OcrEngine | None, describe: ImageDescriber | None
) -> list[Block]:
    if ct == "bad":
        raise UnsupportedFormatError("不支持")
    return [Block(kind="paragraph", text=data.decode(), path=(title,))]


def test_returns_blocks_and_parse_errors_from_child() -> None:
    got = SandboxParser(fn=echo)("text/plain", "正文".encode(), "标题")
    assert [b.text for b in got] == ["正文"]
    with pytest.raises(UnsupportedFormatError):
        SandboxParser(fn=echo)("bad", b"", "t")


def test_timeout_kills_the_child() -> None:
    start = time.monotonic()
    with pytest.raises(ParseTimeoutError):
        SandboxParser(fn=hang, parse_timeout=1)("application/pdf", b"", "t")
    assert time.monotonic() - start < 15


def test_ocr_documents_get_the_longer_timeout() -> None:
    # 配了 OCR 的 PDF 用 OCR 的超时（这里给 1 秒）；没配 OCR 时用解析超时（给 30 秒，不应触发）
    with pytest.raises(ParseTimeoutError, match="OCR"):
        SandboxParser("http://ocr", fn=hang, parse_timeout=30, ocr_timeout=1)(
            "application/pdf", b"", "t"
        )


def test_crash_is_reported_as_retryable_failure() -> None:
    with pytest.raises(ParseCrashedError):
        SandboxParser(fn=crash)("application/pdf", b"", "t")
