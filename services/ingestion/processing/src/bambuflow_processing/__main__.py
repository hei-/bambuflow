"""python -m bambuflow_processing run-once：处理事件流里的全部 DocumentChanged 后退出。"""

from __future__ import annotations

import logging
import os
import sys
from urllib.parse import urlparse

import grpc
import redis
from bambuflow_vocabulary import Vocabularies
from minio import Minio

from bambuflow.config.v1 import config_pb2, config_pb2_grpc
from bambuflow.ingestion.v1 import index_writer_pb2_grpc
from bambuflow_platform import logs
from bambuflow_processing.embedder import Embedder
from bambuflow_processing.ocr import HttpOcrEngine
from bambuflow_processing.worker import RedisStream, Worker

_log = logging.getLogger("bambuflow_processing")


class S3RawStore:
    def __init__(self, endpoint: str, access_key: str, secret_key: str) -> None:
        u = urlparse(endpoint)
        self._c = Minio(
            u.netloc, access_key=access_key, secret_key=secret_key, secure=u.scheme == "https"
        )

    def get(self, ref: str) -> bytes:
        u = urlparse(ref)
        if u.scheme != "s3":
            raise ValueError(f"不支持的 raw_ref: {ref}")
        resp = self._c.get_object(u.netloc, u.path.lstrip("/"))
        try:
            data: bytes = resp.read()
            return data
        finally:
            resp.close()
            resp.release_conn()


def main() -> int:
    logs.setup("processing")
    if sys.argv[1:] != ["run-once"]:
        print("用法: python -m bambuflow_processing run-once", file=sys.stderr)
        return 2
    env = os.environ
    # 词表快照取不到就不处理：没有词表时元数据全部缺失，宁可这批稍后再入库（governance 详设 8.1）
    with grpc.insecure_channel(env["BF_CONFIG_ADDR"]) as cfg:
        snap = config_pb2_grpc.ConfigServiceStub(cfg).GetSnapshot(
            config_pb2.GetSnapshotRequest(service="processing"), timeout=10
        )
    vocab = Vocabularies.from_snapshot(snap)
    _log.info("已加载词表快照", extra={"snapshot_version": vocab.version})
    with grpc.insecure_channel(env["BF_INDEXING_ADDR"]) as channel:
        worker = Worker(
            stream=RedisStream(redis.Redis.from_url(env["BF_REDIS_URL"])),
            raw=S3RawStore(env["BF_S3_ENDPOINT"], env["BF_S3_ACCESS_KEY"], env["BF_S3_SECRET_KEY"]),
            embedder=Embedder(env["BF_EMBEDDER_URL"], env.get("BF_EMBEDDING_MODEL", "bge-m3")),
            writer=index_writer_pb2_grpc.IndexWriterServiceStub(channel),
            # OCR 镜像的地址；不配置时含扫描页或图片的文档处理失败，留在队列里等配置好再处理
            ocr=HttpOcrEngine(env["BF_OCR_URL"]) if env.get("BF_OCR_URL") else None,
            vocab=vocab,
        )
        stats = worker.run_once()
    _log.info(
        "处理完成",
        extra={
            "written": stats.written,
            "unchanged": stats.unchanged,
            "stale": stats.stale,
            "deleted": stats.deleted,
            "acl_updated": stats.acl_updated,
            "failed": len(stats.failed),
        },
    )
    return 1 if stats.failed else 0


if __name__ == "__main__":
    sys.exit(main())
