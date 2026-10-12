"""解析与 OCR 放在子进程里跑，按阶段超时（入库管道详设第 11 节）。

解析在进程内执行时，一份异常文件（解析器死循环、OCR 服务一直不返回）会让 worker 一直卡住，
排在它后面的消息全都等着。放进子进程后超时就杀掉子进程，这份文档按可重试的失败处理
（满 5 次进死信）；子进程崩溃（如解析库的原生代码出错）同样只影响这一份文档。

每份二进制文档起一个子进程（spawn，不继承父进程的 gRPC 连接），启动开销约一秒，
与解析本身相比可以接受；Markdown 正文不经过这里。
"""

from __future__ import annotations

import multiprocessing
import pickle
from collections.abc import Callable
from multiprocessing.connection import Connection

from bambuflow_processing.blocks import Block
from bambuflow_processing.images import HttpImageDescriber, ImageDescriber
from bambuflow_processing.ocr import HttpOcrEngine, OcrEngine
from bambuflow_processing.parsers import parse

# 单文档超时（建议值）：解析 10 分钟；可能要 OCR 的（PDF 的扫描页、图片、邮件附件）30 分钟
PARSE_TIMEOUT = 10 * 60.0
OCR_TIMEOUT = 30 * 60.0
_MAY_NEED_OCR = frozenset({"application/pdf", "image/png", "image/jpeg", "message/rfc822"})

ParseFn = Callable[[str, bytes, str, OcrEngine | None, ImageDescriber | None], list[Block]]


class ParseTimeoutError(RuntimeError):
    """解析或 OCR 超时，子进程已被终止。"""


class ParseCrashedError(RuntimeError):
    """解析子进程异常退出，没有返回结果。"""


def _run(
    conn: Connection,
    fn: ParseFn,
    args: tuple[str, bytes, str],
    ocr_url: str,
    describe_url: str = "",
) -> None:
    """解析可能抛出的失败交回父进程按失败分类处理（与进程内解析时 worker 捕获的一致）；
    其他异常让子进程异常退出，父进程按 ParseCrashedError（可重试）处理。"""
    try:
        ocr = HttpOcrEngine(ocr_url) if ocr_url else None
        describe = HttpImageDescriber(describe_url) if describe_url else None
        conn.send(("ok", fn(*args, ocr, describe)))
    except (ValueError, KeyError, OSError, RuntimeError) as e:
        try:
            conn.send(("err", e))
        except (pickle.PicklingError, TypeError, AttributeError):
            # 异常本身无法序列化：只带类型与消息
            conn.send(("err", RuntimeError(f"{type(e).__name__}: {e}")))
    finally:
        conn.close()


class SandboxParser:
    def __init__(
        self,
        ocr_url: str = "",
        fn: ParseFn = parse,
        describe_url: str = "",
        parse_timeout: float = PARSE_TIMEOUT,
        ocr_timeout: float = OCR_TIMEOUT,
    ) -> None:
        self._ocr_url = ocr_url
        # 图片描述服务（images 模块）；不配置时图片只有 OCR 文字
        self._describe_url = describe_url
        self._fn = fn
        self._parse_timeout = parse_timeout
        self._ocr_timeout = ocr_timeout
        self._ctx = multiprocessing.get_context("spawn")

    def __call__(self, content_type: str, data: bytes, title: str) -> list[Block]:
        ocr = bool(self._ocr_url) and content_type in _MAY_NEED_OCR
        timeout = self._ocr_timeout if ocr else self._parse_timeout
        recv, send = self._ctx.Pipe(duplex=False)
        proc = self._ctx.Process(
            target=_run,
            args=(send, self._fn, (content_type, data, title), self._ocr_url, self._describe_url),
        )
        proc.start()
        send.close()
        try:
            if not recv.poll(timeout):
                raise ParseTimeoutError(f"{'OCR' if ocr else '解析'}超过 {timeout:.0f} 秒")
            kind, value = recv.recv()
        except EOFError:
            raise ParseCrashedError(f"解析子进程异常退出（退出码 {proc.exitcode}）") from None
        finally:
            if proc.is_alive():
                proc.kill()
            proc.join(5)
            recv.close()
        if kind == "err":
            raise value
        blocks: list[Block] = value
        return blocks
