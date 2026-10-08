"""把 golden/t2retrieval 的段落写入待发布版本（make bench-seed，随后 make ingest 发布）。

段落正文从 ModelScope 下载原文件，按 meta.json 的 sha256 校验后按 corpus_ids.txt 筛出。
每段作为一份文档、一个块，写入独立数据源 src_t2retrieval，只授权给评测租户 bench
（tools/test-upstream/sources.yaml），不出现在业务租户的检索结果里。
内容不变时重复运行返回 unchanged。
"""

import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

import grpc
import httpx
import psycopg
import pyarrow.parquet as pq
from google.protobuf.timestamp_pb2 import Timestamp

from bambuflow.common.v1 import types_pb2
from bambuflow.ingestion.v1 import index_writer_pb2, index_writer_pb2_grpc

SET = Path(__file__).resolve().parents[3] / "golden" / "t2retrieval"
URL = "https://www.modelscope.cn/api/v1/datasets/{repo}/repo?Revision={rev}&FilePath={path}"
SOURCE = "src_t2retrieval"
ACL = ["group:bench-readers"]
BATCH = 32


def corpus(meta: dict[str, Any]) -> dict[str, str]:
    f = meta["files"]["corpus"]
    url = URL.format(repo=meta["source"], rev=meta["revision"], path=f["path"])
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        digest = hashlib.sha256()
        with httpx.stream("GET", url, timeout=600, follow_redirects=True) as r:
            r.raise_for_status()
            for chunk in r.iter_bytes(1 << 20):
                digest.update(chunk)
                tmp.write(chunk)
        tmp.flush()
        if digest.hexdigest() != f["sha256"]:
            raise SystemExit("语料文件的 sha256 与 meta.json 不符，拒绝入库")
        rows = pq.read_table(tmp.name, columns=["_id", "text"]).to_pylist()
    if meta.get("corpus_all"):
        return {str(r["_id"]): r["text"] for r in rows}
    wanted = set((SET / "corpus_ids.txt").read_text(encoding="utf-8").split())
    return {str(r["_id"]): r["text"] for r in rows if str(r["_id"]) in wanted}


def live_models(dsn: str) -> tuple[str, str]:
    """写入的待发布版本必须与线上版本同一嵌入模型和分块器，否则随后的 Wiki 入库会被拒。"""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            """SELECT v.embedding_model, v.chunker_version FROM idx.aliases a
               JOIN idx.index_versions v USING (version) WHERE a.alias = 'prod'"""
        ).fetchone()
    if row is None:
        raise SystemExit("还没有线上索引版本，先运行 make ingest")
    return str(row[0]), str(row[1])


def embed(client: httpx.Client, texts: list[str]) -> tuple[list[list[float]], str]:
    resp = client.post(
        os.environ["BF_EMBEDDER_URL"] + "/v2/models/bge-m3/infer",
        json={
            "inputs": [{"name": "text", "shape": [len(texts)], "datatype": "BYTES", "data": texts}]
        },
    )
    resp.raise_for_status()
    body = resp.json()
    out = body["outputs"][0]
    n, dim = out["shape"]
    data = out["data"]
    return [
        data[i * dim : (i + 1) * dim] for i in range(n)
    ], f"{body['model_name']}@{body['model_version']}"


def request(
    cid: str, text: str, vec: list[float], model: str, chunker: str
) -> index_writer_pb2.WriteDocumentRequest:
    ts = Timestamp()
    ts.FromJsonString("2026-10-01T00:00:00Z")
    h = hashlib.sha256(text.encode()).hexdigest()
    doc = f"t2-{cid}"
    return index_writer_pb2.WriteDocumentRequest(
        idempotency_key=f"{doc}:{h}:bench",
        source_id=SOURCE,
        doc_id=doc,
        content_hash=h,
        occurred_at=ts,
        title=f"T2Retrieval {cid}",
        source_uri=f"modelscope://MTEB/T2Retrieval/{cid}",
        source_type=types_pb2.SOURCE_TYPE_WIKI,
        classification=types_pb2.CLASSIFICATION_PUBLIC,
        acl_tokens=ACL,
        chunks=[
            index_writer_pb2.Chunk(
                chunk_id=f"{SOURCE}:{doc}#{h[:16]}",
                parent_chunk_id="",
                text=text,
                text_hash=h,
                span=types_pb2.Span(char_start=0, char_end=len(text), line_start=1, line_end=1),
                embedding=vec,
            )
        ],
        embedding_model_version=model,
        chunker_version=chunker,
        pipeline_version="bench",
    )


def main() -> None:
    meta: dict[str, Any] = json.loads((SET / "meta.json").read_text(encoding="utf-8"))
    model, chunker = live_models(os.environ["BF_PG_DSN"])
    passages = corpus(meta)
    # 按长度排序再分批：同一批长度相近，补齐浪费的计算最少（CPU 上差别很大）
    ids = sorted(passages, key=lambda c: (len(passages[c]), c))
    written = 0
    with (
        httpx.Client(timeout=300) as client,
        grpc.insecure_channel(os.environ["BF_INDEXING_ADDR"]) as ch,
    ):
        stub = index_writer_pb2_grpc.IndexWriterServiceStub(ch)
        for start in range(0, len(ids), BATCH):
            batch = ids[start : start + BATCH]
            vecs, embed_model = embed(client, [passages[c] for c in batch])
            if embed_model != model:
                raise SystemExit(f"嵌入模型 {embed_model} 与线上索引的 {model} 不一致")
            for cid, vec in zip(batch, vecs, strict=True):
                resp = stub.WriteDocument(
                    request(cid, passages[cid], vec, model, chunker), timeout=30
                )
                if resp.result not in (
                    index_writer_pb2.WRITE_RESULT_WRITTEN,
                    index_writer_pb2.WRITE_RESULT_UNCHANGED,
                ):
                    raise SystemExit(
                        f"{cid}: 写入结果 {index_writer_pb2.WriteResult.Name(resp.result)}"
                    )
                written += resp.result == index_writer_pb2.WRITE_RESULT_WRITTEN
            print(f"{start + len(batch)}/{len(ids)}", file=sys.stderr, flush=True)
    print(f"检索评测集 {len(ids)} 段已写入待发布版本（新写入 {written} 段）")


if __name__ == "__main__":
    main()
