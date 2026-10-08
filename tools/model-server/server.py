"""开发用模型服务：在没有 GPU 与 Triton 的环境里，用 CPU 跑真实的嵌入、精排与 NLI 模型。

实现 Triton 的 KServe v2 HTTP 接口子集，与 tools/devtools 的模拟服务同一套接口：
processing、retrieval、grounding 换成生产 Triton 时代码不变，只换地址（ADR-9、ADR-13）。
只用于开发与评测，生产用 Triton。

启动时按 models.json 下载权重（ModelScope），逐个校验 sha256，
全部加载后 /v2/health/ready 才返回 200。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import torch
from transformers import AutoModel, AutoModelForSequenceClassification, AutoTokenizer

_log = logging.getLogger("model-server")
DOWNLOAD = "https://www.modelscope.cn/api/v1/models/{repo}/repo?Revision={rev}&FilePath={path}"
# 每批条数：CPU 上 16 较合适，GPU 上可调大（BF_BATCH）
BATCH = int(os.environ.get("BF_BATCH", "16"))
# 输入截断长度。bge-m3 支持更长，但 CPU 上耗时随长度增长很快；512 是 MTEB 评测的常用设置，
# Wiki 的块（最多约 800 字）基本不受影响
MAX_TOKENS = int(os.environ.get("BF_MAX_TOKENS", "512"))
NLI_MAX_TOKENS = 512
MAX_BODY = 8 << 20


def _ensure_file(root: Path, repo: str, f: dict[str, Any], client: httpx.Client) -> None:
    """文件不存在或未校验过时下载，sha256 不符就删除并报错（不加载来路不明的权重）。"""
    target = root / f["path"]
    marker = target.with_name(target.name + ".sha256")
    if target.exists() and marker.exists() and marker.read_text() == f["sha256"]:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".part")
    digest = hashlib.sha256()
    url = DOWNLOAD.format(repo=repo, rev=f["revision"], path=f["path"])
    _log.info("下载 %s/%s（%d MiB）", repo, f["path"], f["size"] >> 20)
    with client.stream("GET", url) as resp, tmp.open("wb") as out:
        resp.raise_for_status()
        for chunk in resp.iter_bytes(1 << 20):
            digest.update(chunk)
            out.write(chunk)
    if digest.hexdigest() != f["sha256"]:
        tmp.unlink()
        raise RuntimeError(f"{repo}/{f['path']} 的 sha256 与 models.json 不符")
    tmp.rename(target)
    marker.write_text(f["sha256"])


@dataclass
class Served:
    name: str
    version: str
    # 输入名 → 字符串列表；返回输出张量名与一维或二维浮点结果
    infer: Callable[[dict[str, list[str]]], tuple[str, list[list[float]] | list[float]]]
    lock: threading.Lock


def _batches(n: int) -> list[slice]:
    return [slice(i, min(i + BATCH, n)) for i in range(0, n, BATCH)]


# 有 GPU 时用 CUDA 半精度推理（BF_MODELS_GPU=1 的 compose 叠加文件申请显卡），否则用 CPU 单精度
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
DTYPE = torch.float16 if DEVICE.type == "cuda" else torch.float32


def _to_device(model: torch.nn.Module) -> torch.nn.Module:
    return model.to(device=DEVICE, dtype=DTYPE).eval()


def _embedding(root: Path) -> Callable[[dict[str, list[str]]], tuple[str, list[list[float]]]]:
    tok = AutoTokenizer.from_pretrained(root)
    model = _to_device(AutoModel.from_pretrained(root))

    def infer(inputs: dict[str, list[str]]) -> tuple[str, list[list[float]]]:
        texts = inputs["text"]
        out: list[list[float]] = []
        with torch.inference_mode():
            for s in _batches(len(texts)):
                enc = tok(
                    texts[s],
                    padding=True,
                    truncation=True,
                    max_length=MAX_TOKENS,
                    return_tensors="pt",
                ).to(DEVICE)
                # bge-m3 的稠密向量取 [CLS] 再归一化（与 FlagEmbedding 的做法一致）
                cls = model(**enc).last_hidden_state[:, 0]
                out.extend(torch.nn.functional.normalize(cls.float(), dim=-1).tolist())
        return "embedding", out

    return infer


def _pair_classifier(
    root: Path,
    first: str,
    second: str,
    output: str,
    score: Callable[[torch.Tensor], torch.Tensor],
    max_tokens: int,
) -> Callable[[dict[str, list[str]]], tuple[str, list[float]]]:
    tok = AutoTokenizer.from_pretrained(root)
    model = _to_device(AutoModelForSequenceClassification.from_pretrained(root))

    def infer(inputs: dict[str, list[str]]) -> tuple[str, list[float]]:
        a, b = inputs[first], inputs[second]
        if len(a) != len(b):
            raise ValueError(f"{first} 与 {second} 条数不同")
        out: list[float] = []
        with torch.inference_mode():
            for s in _batches(len(a)):
                enc = tok(
                    a[s],
                    b[s],
                    padding=True,
                    truncation="only_first",
                    max_length=max_tokens,
                    return_tensors="pt",
                ).to(DEVICE)
                out.extend(score(model(**enc).logits.float()).tolist())
        return output, out

    return infer


def load(manifest: dict[str, Any], names: list[str], cache: Path) -> dict[str, Served]:
    served: dict[str, Served] = {}
    with httpx.Client(timeout=httpx.Timeout(60, read=300), follow_redirects=True) as client:
        for name in names:
            spec = manifest[name]
            root = cache / name / spec["version"]
            for f in spec["files"]:
                _ensure_file(root, spec["repo"], f, client)
            _log.info("加载 %s@%s", name, spec["version"])
            if spec["kind"] == "embedding":
                infer: Callable[..., Any] = _embedding(root)
            elif spec["kind"] == "reranker":
                infer = _pair_classifier(
                    root, "query", "passage", "score", lambda x: torch.sigmoid(x[:, 0]), MAX_TOKENS
                )
            else:
                # 支持度 = “蕴含”类的概率；标签编号取自模型自带的 label_mapping.json
                entail = json.loads((root / "label_mapping.json").read_text(encoding="utf-8"))[
                    "蕴含"
                ]
                infer = _pair_classifier(
                    root,
                    "premise",
                    "hypothesis",
                    "support",
                    lambda x, i=entail: torch.softmax(x, dim=-1)[:, i],
                    NLI_MAX_TOKENS,
                )
            served[name] = Served(name, spec["version"], infer, threading.Lock())
    return served


def handler(
    served: dict[str, Served], aliases: dict[str, str], ready: threading.Event
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            _log.debug(fmt, *args)

        def _json(self, code: int, body: dict[str, Any]) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _model(self) -> Served | None:
            parts = self.path.split("/")  # /v2/models/<name>[/infer]
            if len(parts) < 4 or parts[1:3] != ["v2", "models"]:
                return None
            return served.get(aliases.get(parts[3], parts[3]))

        def do_GET(self) -> None:
            if self.path == "/v2/health/ready":
                self._json(200 if ready.is_set() else 503, {})
                return
            m = self._model()
            if m is None or self.path.endswith("/infer"):
                self._json(404, {"error": "model not found"})
                return
            self._json(200, {"name": m.name, "versions": [m.version], "platform": "pytorch_cpu"})

        def do_POST(self) -> None:
            m = self._model()
            if m is None or not self.path.endswith("/infer"):
                self._json(404, {"error": "model not found"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_BODY:
                self._json(400, {"error": "body size"})
                return
            try:
                req = json.loads(self.rfile.read(length))
                inputs = {
                    t["name"]: [str(x) for x in t["data"]]
                    for t in req["inputs"]
                    if t["datatype"] == "BYTES"
                }
                with m.lock:
                    name, data = m.infer(inputs)
            except (KeyError, TypeError, ValueError) as e:
                self._json(400, {"error": str(e)})
                return
            flat = [x for row in data for x in row] if data and isinstance(data[0], list) else data
            shape = [len(data), len(data[0])] if data and isinstance(data[0], list) else [len(data)]
            self._json(
                200,
                {
                    "model_name": m.name,
                    "model_version": m.version,
                    "outputs": [{"name": name, "datatype": "FP32", "shape": shape, "data": flat}],
                },
            )

    return Handler


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    torch.set_num_threads(int(os.environ.get("BF_TORCH_THREADS", "4")))
    manifest = json.loads(Path(__file__).with_name("models.json").read_text(encoding="utf-8"))
    names = [n for n in os.environ.get("BF_SERVE_MODELS", ",".join(manifest)).split(",") if n]
    # NLI 模型按设计“在评测集上比选”（技术设计文档的模型选型表）；
    # 选定的候选以别名 nli 对外，回答的 why 里仍是真实名称
    aliases = {"nli": os.environ.get("BF_NLI_DEFAULT", "nli-erlangshen-330m")}
    served: dict[str, Served] = {}
    ready = threading.Event()
    # 空地址即监听所有网卡：容器里要让 compose 网络内的服务访问，端口不映射到宿主机
    port = int(os.environ.get("BF_PORT", "8000"))
    srv = ThreadingHTTPServer(("", port), handler(served, aliases, ready))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    served.update(load(manifest, names, Path(os.environ.get("BF_MODEL_DIR", "/models"))))
    ready.set()
    _log.info("模型已就绪: %s", ", ".join(f"{s.name}@{s.version}" for s in served.values()))
    threading.Event().wait()


if __name__ == "__main__":
    main()
