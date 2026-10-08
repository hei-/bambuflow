"""processing 的 OCR 镜像（技术设计文档 12.2：OCR 单独镜像，跑在独立 CPU 节点池）。

只做一件事：收到一张图片（PNG 或 JPEG），用 PaddleOCR（PP-OCRv5 检测与识别模型）
识别出文本行，返回每行的文本、像素坐标 [x0, y0, x1, y1] 与置信度。版面分析、段落聚合、
坐标换算都在 processing 里做（bambuflow_processing.ocr），这里不处理文档语义，
换 OCR 引擎只需实现同样的接口。

    python server.py serve
        在 8000 端口提供 POST /v1/ocr 与 GET /healthz
    python server.py selftest <图片，- 表示标准输入> <期望文字>
        识别一张图片，包含期望文字则退出码为 0（镜像冒烟测试）

PaddleOCR、numpy、OpenCV 只装在这个镜像里（requirements.txt 按 sha256 固定），
工作区的开发环境没有它们，所以按名字动态导入；
模型在构建镜像时按 sha256 下载到 MODEL_DIR，运行时不联网。
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

MODEL_DIR = Path(os.environ.get("BF_OCR_MODEL_DIR", "/opt/ocr/models"))
DET_MODEL = "PP-OCRv5_mobile_det"
# 识别用 server 版：mobile 版在 150 dpi 扫描件的长行上会漏字（“工作日”识别成“作”，实测），
# server 版逐字正确；检测仍用 mobile 版，框选文本行两者差别不大
REC_MODEL = "PP-OCRv5_server_rec"
MAX_BYTES = 20 << 20  # 单张图片上限：A4 页面按 144 dpi 渲染的 PNG 远小于它

_log = logging.getLogger("ocr")


class Recognizer:
    def __init__(self, model_dir: Path) -> None:
        paddleocr: Any = importlib.import_module("paddleocr")
        self._np: Any = importlib.import_module("numpy")
        self._cv2: Any = importlib.import_module("cv2")
        # 文档方向分类、弯曲矫正、文本行方向这三个可选模型都关掉：样例与公司扫描件是正向平整的页面，
        # 开启它们要多下载模型、推理也慢几倍
        self._ocr: Any = paddleocr.PaddleOCR(
            text_detection_model_name=DET_MODEL,
            text_detection_model_dir=str(model_dir / f"{DET_MODEL}_infer"),
            text_recognition_model_name=REC_MODEL,
            text_recognition_model_dir=str(model_dir / f"{REC_MODEL}_infer"),
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
            # PaddlePaddle 3.3 的 oneDNN 执行器在检测模型上报 NotImplementedError
            # （ConvertPirAttribute2RuntimeAttribute，实测），关掉 oneDNN 走普通 CPU 内核
            enable_mkldnn=False,
        )
        # PaddleOCR 的预测器不保证线程安全，识别串行执行；服务是多线程的，识别时健康检查照常响应
        self._lock = threading.Lock()

    def recognize(self, data: bytes) -> list[dict[str, Any]]:
        img = self._cv2.imdecode(self._np.frombuffer(data, dtype=self._np.uint8), 1)
        if img is None:
            raise ValueError("图片无法解码")
        with self._lock:
            results = self._ocr.predict(img)
        lines: list[dict[str, Any]] = []
        for res in results:
            for text, score, box in zip(
                res["rec_texts"], res["rec_scores"], res["rec_boxes"], strict=True
            ):
                lines.append(
                    {"text": str(text), "score": float(score), "box": [float(v) for v in box]}
                )
        return lines


def serve(recognizer: Recognizer, port: int) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/healthz":
                self._reply(200, {"status": "ready"})
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self) -> None:
            if self.path != "/v1/ocr":
                self._reply(404, {"error": "not found"})
                return
            size = int(self.headers.get("Content-Length") or 0)
            if size <= 0 or size > MAX_BYTES:
                self._reply(413 if size > MAX_BYTES else 400, {"error": "图片大小不合法"})
                return
            data = self.rfile.read(size)
            try:
                lines = recognizer.recognize(data)
            except ValueError as e:
                self._reply(400, {"error": str(e)})
                return
            self._reply(200, {"model": f"{DET_MODEL}+{REC_MODEL}", "lines": lines})

        def _reply(self, status: int, body: dict[str, Any]) -> None:
            raw = json.dumps(body, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, format: str, *args: Any) -> None:
            # 默认的访问日志写 stderr 且不带结构；这里只记方法、路径与状态，不记图片内容
            _log.info("%s", format % args)

    # 主机名留空即监听容器内的全部地址；对外暴露哪些端口由编排（compose、Helm）控制
    httpd = ThreadingHTTPServer(("", port), Handler)
    _log.info("OCR 服务已启动，端口 %d", port)
    httpd.serve_forever()


def selftest(recognizer: Recognizer, image: str, expected: str) -> int:
    data = sys.stdin.buffer.read() if image == "-" else Path(image).read_bytes()
    text = "".join("".join(line["text"].split()) for line in recognizer.recognize(data))
    print(text)
    return 0 if "".join(expected.split()) in text else 1


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    args = sys.argv[1:]
    if args == ["serve"]:
        serve(Recognizer(MODEL_DIR), int(os.environ.get("BF_OCR_PORT", "8000")))
        return 0
    if len(args) == 3 and args[0] == "selftest":
        return selftest(Recognizer(MODEL_DIR), args[1], args[2])
    print("用法: server.py serve | selftest <图片|-> <期望文字>", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
