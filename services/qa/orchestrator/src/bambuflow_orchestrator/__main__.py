"""python -m bambuflow_orchestrator serve | migrate | healthcheck"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import sys

import grpc
import httpx
import redis.asyncio
from bambuflow_vocabulary import Vocabularies
from psycopg_pool import AsyncConnectionPool

from bambuflow.answer.v1 import answer_pb2_grpc, records_pb2_grpc
from bambuflow.config.v1 import config_pb2, config_pb2_grpc
from bambuflow.retrieval.v1 import retrieval_pb2_grpc
from bambuflow_grounding import KServeNLI
from bambuflow_orchestrator import export, migrate, structured
from bambuflow_orchestrator.admission import DEFAULT_LIMIT, Admission
from bambuflow_orchestrator.experiments import Experiments, refresh
from bambuflow_orchestrator.llm import ChatClient
from bambuflow_orchestrator.records import PGRecords
from bambuflow_orchestrator.server import AnswerService
from bambuflow_orchestrator.session import RedisStore
from bambuflow_platform import logs, telemetry
from bambuflow_platform.grpc_server import AuthInterceptor
from bambuflow_platform.principal import JWKS, Verifier

_log = logging.getLogger("bambuflow_orchestrator")
LISTEN = os.environ.get("BF_LISTEN", "0.0.0.0:9090")
# NLI 调用的兜底超时；实际受请求 deadline 约束
NLI_TIMEOUT = 2.0
# 会话读写的超时：超时按 Redis 不可用处理，退化为单轮（orchestrator 详设 11.2）
REDIS_TIMEOUT = 0.2
GRACE_SECONDS = 15
# 回答记录的连接池：取连接的等待上限很短，元数据库不可用时回答照常返回（只是不留记录）
PG_POOL_MAX = 10
PG_POOL_TIMEOUT = 0.5


def _llm_api_key() -> str:
    """外部 LLM 的密钥从 secret 文件读取。

    不经环境变量，避免出现在 docker inspect 与进程环境里。
    """
    path = os.environ.get("BF_LLM_API_KEY_FILE", "")
    if not path or not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8") as f:
        return f.read().strip()


async def _snapshot(addr: str) -> config_pb2.GetSnapshotResponse:
    """配置快照：词表（枚举与计数类问题按它把问题里的说法映射成筛选条件）与运行中的实验。"""
    async with grpc.aio.insecure_channel(addr) as ch:
        snap: config_pb2.GetSnapshotResponse = await config_pb2_grpc.ConfigServiceStub(
            ch
        ).GetSnapshot(config_pb2.GetSnapshotRequest(service="orchestrator"), timeout=10)
    return snap


def _sql(env: os._Environ[str]) -> tuple[structured.Catalog, structured.QueryService] | None:
    """数据平台只读查询服务（sql_query）：目录由部署配置给出，只读账号的连接串经 Vault 注入。
    没有配置目录时不开放结构化查询。"""
    path = env.get("BF_SQL_CATALOG", "")
    if not path:
        return None
    catalog = structured.Catalog.load(path)
    return catalog, structured.PGQueryService(env["BF_SQL_DSN"], catalog)


async def serve() -> None:
    env = os.environ
    provider = telemetry.setup("orchestrator") if env.get("OTEL_EXPORTER_OTLP_ENDPOINT") else None
    verifier = Verifier(JWKS(env["BF_PRINCIPAL_JWKS_URL"]))
    # 取不到快照就不启动（governance 详设 8.1）
    snap = await _snapshot(env["BF_CONFIG_ADDR"])
    vocab = Vocabularies.from_snapshot(snap)
    experiments = Experiments(list(snap.experiments))
    _log.info("已加载配置快照", extra={"snapshot_version": snap.snapshot_version})
    host, _, port = env["BF_REDIS_ADDR"].rpartition(":")
    # 口令经 Vault 注入（部署配置清单）；开发环境的 Redis 没有口令
    rdb = redis.asyncio.Redis(
        host=host,
        port=int(port),
        password=env.get("BF_REDIS_PASSWORD") or None,
        socket_timeout=REDIS_TIMEOUT,
        socket_connect_timeout=REDIS_TIMEOUT,
    )
    # 连接串经 Vault 注入（bambuflow/orchestrator）。启动时不等数据库就绪：它只影响回放与反馈
    pool = AsyncConnectionPool(
        env["BF_PG_DSN"], min_size=1, max_size=PG_POOL_MAX, timeout=PG_POOL_TIMEOUT, open=False
    )
    await pool.open(wait=False)
    async with (
        grpc.aio.insecure_channel(
            env["BF_RETRIEVAL_ADDR"], interceptors=telemetry.aio_client_interceptors()
        ) as channel,
        httpx.AsyncClient(timeout=NLI_TIMEOUT) as http,
    ):
        server = grpc.aio.server(
            interceptors=[
                telemetry.aio_server_interceptor(),
                AuthInterceptor(verifier, internal=frozenset({export.SERVICE})),
            ]
        )
        records_pb2_grpc.add_AnswerRecordServiceServicer_to_server(
            export.RecordService(pool), server
        )
        answer_pb2_grpc.add_AnswerServiceServicer_to_server(
            AnswerService(
                retrieval_pb2_grpc.RetrievalServiceStub(channel),
                ChatClient(
                    env["BF_LLM_URL"],
                    env["BF_LLM_MODEL"],
                    http,
                    _llm_api_key(),
                    json.loads(env.get("BF_LLM_EXTRA_BODY") or "{}"),
                ),
                KServeNLI(env["BF_NLI_URL"], http),
                vocab,
                RedisStore(rdb),
                PGRecords(pool),
                # 每副本并发上限（详设 11.4），随副本规格由部署配置调整
                Admission(int(env.get("BF_MAX_CONCURRENCY") or DEFAULT_LIMIT)),
                experiments,
                _sql(env),
            ),
            server,
        )
        server.add_insecure_port(LISTEN)
        await server.start()
        # 实验的流量调整与停止 1 分钟内生效（governance 详设 4.2）
        refreshing = asyncio.create_task(
            refresh(experiments, lambda: _snapshot(env["BF_CONFIG_ADDR"]))
        )
        _log.info("orchestrator 已启动")
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await stop.wait()
        refreshing.cancel()
        # 下线前等进行中的请求结束
        await server.stop(GRACE_SECONDS)
    await rdb.aclose()
    await pool.close()
    if provider is not None:
        provider.shutdown()


def healthcheck() -> int:
    port = int(LISTEN.rsplit(":", 1)[1])
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3):
            return 0
    except OSError:
        return 1


def main() -> int:
    logs.setup("orchestrator")
    match sys.argv[1:]:
        case ["serve"]:
            asyncio.run(serve())
            return 0
        case ["migrate"]:
            applied = asyncio.run(migrate.up(os.environ["BF_PG_DSN"]))
            _log.info("迁移完成", extra={"applied": applied})
            return 0
        case ["healthcheck"]:
            return healthcheck()
    print("用法: python -m bambuflow_orchestrator serve | migrate | healthcheck", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
