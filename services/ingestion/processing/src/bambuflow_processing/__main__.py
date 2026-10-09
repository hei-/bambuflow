"""processing 的入口。

python -m bambuflow_processing run-once      处理事件流里的全部 DocumentChanged 后退出
python -m bambuflow_processing serve-admin   常驻的入库运维接口：入库状态、死信、重放
                                             （入库管道详设 14.4）
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import sys
from urllib.parse import urlparse

import grpc
import httpx
import redis
from bambuflow_vocabulary import Vocabularies
from minio import Minio

from bambuflow.admin.v1 import ingest_admin_pb2_grpc
from bambuflow.config.v1 import config_pb2, config_pb2_grpc
from bambuflow.ingestion.v1 import index_writer_pb2_grpc
from bambuflow_platform import envelope, logs
from bambuflow_platform.grpc_server import AuthInterceptor
from bambuflow_platform.principal import JWKS, Verifier
from bambuflow_platform.snapshot import open_snapshot, parse_keys
from bambuflow_processing.admin import IngestAdmin
from bambuflow_processing.embedder import Embedder
from bambuflow_processing.ocr import HttpOcrEngine
from bambuflow_processing.state import IngestState
from bambuflow_processing.worker import PIPELINE_VERSION, RedisStream, Worker

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


# 下线前等进行中的管理请求结束
GRACE_SECONDS = 10


async def serve_admin() -> None:
    env = os.environ
    server = grpc.aio.server(
        # 只经 gateway 的管理接口调用，只接受管理 Principal
        interceptors=[AuthInterceptor(Verifier(JWKS(env["BF_PRINCIPAL_JWKS_URL"]), operator=True))]
    )
    rdb = redis.Redis.from_url(env["BF_REDIS_URL"], socket_timeout=5, socket_connect_timeout=5)
    state = IngestState(rdb, PIPELINE_VERSION)
    ingest_admin_pb2_grpc.add_IngestAdminServiceServicer_to_server(IngestAdmin(state), server)
    server.add_insecure_port(env.get("BF_LISTEN", "0.0.0.0:9090"))
    await server.start()
    _log.info("processing 管理服务已启动")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    await server.stop(GRACE_SECONDS)


def main() -> int:
    logs.setup("processing")
    if sys.argv[1:] == ["serve-admin"]:
        asyncio.run(serve_admin())
        return 0
    if sys.argv[1:] != ["run-once"]:
        print("用法: python -m bambuflow_processing run-once | serve-admin", file=sys.stderr)
        return 2
    env = os.environ
    # 词表快照取不到就不处理：没有词表时元数据全部缺失，宁可这批稍后再入库（governance 详设 8.1）
    # 快照验签后才使用（governance 详设 9.2）；验签不过同样不处理
    with grpc.insecure_channel(env["BF_CONFIG_ADDR"]) as cfg:
        stub = config_pb2_grpc.ConfigServiceStub(cfg)
        signed = stub.GetSnapshot(config_pb2.GetSnapshotRequest(service="processing"), timeout=10)
        keys = parse_keys(stub.GetSigningKeys(config_pb2.GetSigningKeysRequest(), timeout=10))
        snap = open_snapshot(signed, keys)
        stub.Ack(
            config_pb2.AckRequest(
                service="processing",
                instance_id=socket.gethostname(),
                snapshot_version=snap.snapshot_version,
                loaded=True,
            ),
            timeout=10,
        )
    vocab = Vocabularies.from_snapshot(snap)
    _log.info("已加载词表快照", extra={"snapshot_version": vocab.version})
    # PII 原值的信封加密（技术设计文档 8.6）：Vault transit 的主密钥；没有配置时只掩码、不保存原值
    pii_keys = (
        envelope.VaultTransit(
            env["VAULT_ADDR"],
            env["VAULT_TOKEN"],
            env.get("BF_PII_KEY", "bambuflow-pii"),
            httpx.Client(timeout=10),
        )
        if env.get("VAULT_ADDR") and env.get("VAULT_TOKEN")
        else None
    )
    rdb = redis.Redis.from_url(env["BF_REDIS_URL"])
    with grpc.insecure_channel(env["BF_INDEXING_ADDR"]) as channel:
        worker = Worker(
            stream=RedisStream(rdb),
            raw=S3RawStore(env["BF_S3_ENDPOINT"], env["BF_S3_ACCESS_KEY"], env["BF_S3_SECRET_KEY"]),
            embedder=Embedder(env["BF_EMBEDDER_URL"], env.get("BF_EMBEDDING_MODEL", "bge-m3")),
            writer=index_writer_pb2_grpc.IndexWriterServiceStub(channel),
            # OCR 镜像的地址；不配置时含扫描页或图片的文档处理失败，留在队列里等配置好再处理
            ocr=HttpOcrEngine(env["BF_OCR_URL"]) if env.get("BF_OCR_URL") else None,
            vocab=vocab,
            pii_keys=pii_keys,
            state=IngestState(rdb, PIPELINE_VERSION),
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
            "dead_lettered": stats.dead_lettered,
        },
    )
    return 1 if stats.failed else 0


if __name__ == "__main__":
    sys.exit(main())
