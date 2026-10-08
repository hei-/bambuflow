"""从公开的 T2Retrieval（C-MTEB，Apache 2.0）抽取检索评测子集，写到 golden/t2retrieval/。

只在需要重建子集时手动运行（依赖 pyarrow 与 httpx，不进工作区）：
  make run CMD='uv run --no-project --with pyarrow --with httpx \\
    python tools/test-upstream/make_t2_subset.py' OUT='golden/t2retrieval'

抽样按“种子 + ID”的哈希排序取前 N 个，结果可复现。
子集 = QUERIES 条查询 + 它们全部的相关段落 + 干扰段落，凑足 CORPUS 段。
仓库里只存查询、标注与段落 ID（corpus_ids.txt），段落正文在入库时从 ModelScope 下载原文件，
按 meta.json 记录的 sha256 校验后筛出（数据集没有可钉住的提交号，内容一致由 sha256 保证）。
"""

import hashlib
import json
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path

import httpx
import pyarrow.parquet as pq

REPO = "MTEB/T2Retrieval"
URL = "https://www.modelscope.cn/api/v1/datasets/{repo}/repo?Revision={rev}&FilePath={path}"
REVISION = "master"
FILES = {
    "qrels": "data/dev-00000-of-00001.parquet",
    "queries": "queries/dev-00000-of-00001.parquet",
    "corpus": "corpus/dev-00000-of-00001.parquet",
}
QUERIES = 500
# 语料取全量（约 11.8 万段）：GPU 主机上嵌入约十几分钟。设为正数时抽样到该规模（CPU 环境用 5000）
CORPUS = 0
SEED = 20261005
OUT = Path(__file__).resolve().parents[2] / "golden" / "t2retrieval"


def fetch(path: str) -> tuple[Path, str]:
    local = Path(tempfile.gettempdir()) / path.replace("/", "_")
    if not local.exists():
        url = URL.format(repo=REPO, rev=REVISION, path=path)
        with (
            httpx.stream("GET", url, timeout=600, follow_redirects=True) as r,
            local.open("wb") as f,
        ):
            r.raise_for_status()
            for chunk in r.iter_bytes(1 << 20):
                f.write(chunk)
    return local, hashlib.sha256(local.read_bytes()).hexdigest()


def _key(x: str) -> str:
    return hashlib.sha256(f"{SEED}:{x}".encode()).hexdigest()


def _sample(items: Iterable[str], n: int) -> list[str]:
    """确定的抽样：按“种子 + ID”的 sha256 排序取前 n 个（不需要随机数生成器）。"""
    return sorted(items, key=_key)[:n]


def main() -> None:
    paths = {k: fetch(v) for k, v in FILES.items()}
    qrels = [r for r in pq.read_table(paths["qrels"][0]).to_pylist() if r["score"] > 0]
    rel: dict[str, set[str]] = {}
    for r in qrels:
        rel.setdefault(str(r["query-id"]), set()).add(str(r["corpus-id"]))
    qids = sorted(_sample(rel, QUERIES))
    queries = {str(r["_id"]): r["text"] for r in pq.read_table(paths["queries"][0]).to_pylist()}
    corpus = {str(r["_id"]): r["text"] for r in pq.read_table(paths["corpus"][0]).to_pylist()}
    needed = sorted({c for q in qids for c in rel[q]})
    OUT.mkdir(parents=True, exist_ok=True)
    ids_file = OUT / "corpus_ids.txt"
    if CORPUS > 0:
        others = sorted(set(corpus) - set(needed))
        picked = needed + _sample(others, max(0, CORPUS - len(needed)))
        ids_file.write_text("\n".join(sorted(picked)) + "\n", encoding="utf-8")
    else:
        # 全量语料：不列 ID，入库时取原文件的全部段落
        picked = sorted(corpus)
        ids_file.unlink(missing_ok=True)
    with (OUT / "queries.jsonl").open("w", encoding="utf-8") as f:
        for q in qids:
            f.write(
                json.dumps(
                    {"id": q, "text": queries[q], "relevant": sorted(rel[q])}, ensure_ascii=False
                )
                + "\n"
            )
    meta = {
        "source": REPO,
        "revision": REVISION,
        "license": "Apache-2.0",
        "files": {k: {"path": FILES[k], "sha256": v[1]} for k, v in paths.items()},
        "queries": len(qids),
        "corpus": len(picked),
        "corpus_all": CORPUS == 0,
        "relevant_passages": len(needed),
        "seed": SEED,
    }
    (OUT / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    lens = sorted(len(corpus[c]) for c in picked)
    median, p90 = lens[len(lens) // 2], lens[int(len(lens) * 0.9)]
    print(
        json.dumps(meta, ensure_ascii=False), "段落长度中位数", median, "p90", p90, file=sys.stderr
    )


if __name__ == "__main__":
    main()
