"""生成 models.json：把每个模型要用的文件钉到 ModelScope 上的提交号与 sha256。

只在升级模型时手动运行：
  make run CMD='uv run --locked python tools/model-server/pin.py' \
    OUT='tools/model-server/models.json'
模型服务启动时按 models.json 下载并逐个校验 sha256，内容不符就拒绝加载。
"""

import json
from pathlib import Path

import httpx

API = "https://www.modelscope.cn/api/v1/models/{repo}/repo/files?Recursive=true"

# 名称 → (ModelScope 仓库, 类型, 需要的文件, 主权重文件)。版本号取主权重文件的提交号前 12 位。
SPEC = {
    "bge-m3": (
        "BAAI/bge-m3",
        "embedding",
        [
            "config.json",
            "pytorch_model.bin",
            "sparse_linear.pt",
            "tokenizer.json",
            "tokenizer_config.json",
            "special_tokens_map.json",
            "sentencepiece.bpe.model",
        ],
        "pytorch_model.bin",
    ),
    "bge-reranker-v2-m3": (
        "BAAI/bge-reranker-v2-m3",
        "reranker",
        [
            "config.json",
            "model.safetensors",
            "tokenizer.json",
            "tokenizer_config.json",
            "special_tokens_map.json",
            "sentencepiece.bpe.model",
        ],
        "model.safetensors",
    ),
    "nli-erlangshen-110m": (
        "Fengshenbang/Erlangshen-Roberta-110M-NLI",
        "nli",
        ["config.json", "pytorch_model.bin", "vocab.txt", "label_mapping.json"],
        "pytorch_model.bin",
    ),
    "nli-erlangshen-330m": (
        "Fengshenbang/Erlangshen-Roberta-330M-NLI",
        "nli",
        ["config.json", "pytorch_model.bin", "vocab.txt", "label_mapping.json"],
        "pytorch_model.bin",
    ),
}


def main() -> None:
    out = {}
    for name, (repo, kind, wanted, weights) in SPEC.items():
        resp = httpx.get(API.format(repo=repo), timeout=30)
        resp.raise_for_status()
        files = {f["Path"]: f for f in resp.json()["Data"]["Files"] if f["Type"] == "blob"}
        out[name] = {
            "repo": repo,
            "kind": kind,
            "version": "ms-" + files[weights]["Revision"][:12],
            "files": [
                {
                    "path": p,
                    "revision": files[p]["Revision"],
                    "sha256": files[p]["Sha256"],
                    "size": files[p]["Size"],
                }
                for p in wanted
            ],
        }
    path = Path(__file__).with_name("models.json")
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
