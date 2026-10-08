"""python -m bambuflow_evaluation run --golden <目录> [--tenants a,b] [--repeat 3] [--out <目录>]
                                     [--write-baseline]
python -m bambuflow_evaluation affected --golden <目录> < 改动文件列表   输出受影响租户（逗号分隔）
python -m bambuflow_evaluation serve    常驻服务：A/B 分析接口（evaluation 详设 2、8.2）
python -m bambuflow_evaluation guard    检查运行中实验的护栏，触发即停止（每天由调度方运行）
python -m bambuflow_evaluation online-sample [--day YYYY-MM-DD]
    在线抽样评审（默认前一天，每天由调度方运行）
python -m bambuflow_evaluation migrate  建 evaluation 自有的表（升级前执行）

退出码：0 通过；1 不通过；2 无法判定。报告写到 --out：report.md、report.json；--write-baseline 时
改写 <基线名>.md、<基线名>.json（--baseline，默认 baseline；基线随评测集提交，之后的运行与它比较）。
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import os
import signal
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import grpc
import httpx

from bambuflow.admin.v1 import experiment_analysis_pb2_grpc, quality_trend_pb2_grpc
from bambuflow.answer.v1 import answer_pb2_grpc, records_pb2_grpc
from bambuflow.config.v1 import experiment_control_pb2_grpc
from bambuflow.retrieval.v1 import retrieval_pb2_grpc
from bambuflow_evaluation import affected, golden, online, report, retrieval, service
from bambuflow_evaluation.metrics import average, judge
from bambuflow_evaluation.runner import Runner
from bambuflow_evaluation.signer import Signer
from bambuflow_grounding import KServeNLI
from bambuflow_platform.grpc_server import AuthInterceptor
from bambuflow_platform.principal import JWKS, Verifier

_log = logging.getLogger("bambuflow_evaluation")
EXIT = {"passed": 0, "failed": 1, "indeterminate": 2}


async def evaluate(args: argparse.Namespace) -> int:
    env = os.environ
    gs = golden.load(Path(args.golden))
    if args.tenants:
        # CI 只跑受影响租户的题（评测详设 5.1）；权限评测集另由 make eval-perm 全量跑
        wanted = set(args.tenants.split(","))
        gs = replace(gs, questions=[q for q in gs.questions if q.asker.tenant in wanted])
        if not gs.questions:
            raise SystemExit(f"评测集里没有租户 {args.tenants} 的题")
    baseline_path = Path(args.golden) / "reports" / f"{args.baseline}.json"
    baseline: dict[str, Any] | None = None
    tenant_baseline: dict[str, Any] | None = None
    if baseline_path.exists() and not args.write_baseline:
        saved = json.loads(baseline_path.read_text(encoding="utf-8"))
        baseline, tenant_baseline = saved["metrics"], saved.get("tenant_metrics")
        if args.tenants and not tenant_baseline:
            raise SystemExit("基线没有按租户的指标，不能只跑部分租户；先重写基线")
    signer = Signer(env["VAULT_ADDR"], env["VAULT_TOKEN"])
    async with (
        grpc.aio.insecure_channel(env["BF_RETRIEVAL_ADDR"]) as rch,
        grpc.aio.insecure_channel(env["BF_ORCHESTRATOR_ADDR"]) as och,
        httpx.AsyncClient(timeout=10) as http,
    ):
        runner = Runner(
            retrieval_pb2_grpc.RetrievalServiceStub(rch),
            answer_pb2_grpc.AnswerServiceStub(och),
            signer,
            # 判定用的 NLI 应与线上校验器不同（evaluation 详设 6.1），模型名由部署配置给出
            KServeNLI(env["BF_NLI_URL"], http, env.get("BF_EVAL_NLI_MODEL") or "nli"),
        )
        runs = []
        for i in range(args.repeat):
            _log.info("开始第 %d 次运行", i + 1)
            runs.append(await runner.run(gs.questions))
    metrics = average(runs)
    verdict = judge(runs, baseline, tenant_baseline)
    config = {k: sorted(v) for k, v in runner.config.versions.items()}

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    name = args.baseline if args.write_baseline else "report"
    md = report.markdown(
        gs, config, runner.judge_model, runs, metrics, baseline, verdict, tenant_baseline
    )
    (out / f"{name}.md").write_text(md, encoding="utf-8")
    data = report.as_json(gs, config, runner.judge_model, runs, metrics, verdict)
    (out / f"{name}.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(md)
    return EXIT[verdict.conclusion]


async def evaluate_retrieval(args: argparse.Namespace) -> int:
    env = os.environ
    path = Path(args.set)
    meta, queries = retrieval.load(path)
    baseline_path = path / "reports" / f"{args.baseline}.json"
    baseline = None
    if baseline_path.exists() and not args.write_baseline:
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))["metrics"]
    signer = Signer(env["VAULT_ADDR"], env["VAULT_TOKEN"])
    async with grpc.aio.insecure_channel(env["BF_RETRIEVAL_ADDR"]) as ch:
        metrics, index_version = await retrieval.evaluate(
            retrieval_pb2_grpc.RetrievalServiceStub(ch), signer, queries, args.rerank
        )
    conclusion, reasons = retrieval.judge(metrics, baseline)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    name = args.baseline if args.write_baseline else "report"
    md = retrieval.markdown(
        meta, len(queries), index_version, metrics, baseline, conclusion, reasons
    )
    (out / f"{name}.md").write_text(md, encoding="utf-8")
    data = {
        "conclusion": conclusion,
        "reasons": reasons,
        "index_version": index_version,
        "metrics": metrics,
    }
    (out / f"{name}.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(md)
    return EXIT[conclusion]


# 下线前等进行中的分析请求结束
GRACE_SECONDS = 10


async def serve() -> None:
    env = os.environ
    async with (
        grpc.aio.insecure_channel(env["BF_CONFIG_ADDR"]) as gch,
        grpc.aio.insecure_channel(env["BF_ORCHESTRATOR_ADDR"]) as och,
    ):
        server = grpc.aio.server(
            # 分析与趋势接口只经 gateway 的管理接口调用，只接受管理 Principal
            interceptors=[
                AuthInterceptor(Verifier(JWKS(env["BF_PRINCIPAL_JWKS_URL"]), operator=True))
            ]
        )
        experiment_analysis_pb2_grpc.add_ExperimentAnalysisServiceServicer_to_server(
            service.AnalysisService(
                experiment_control_pb2_grpc.ExperimentControlServiceStub(gch),
                records_pb2_grpc.AnswerRecordServiceStub(och),
            ),
            server,
        )
        quality_trend_pb2_grpc.add_QualityTrendServiceServicer_to_server(
            online.TrendService(online.PGStore(env["BF_PG_DSN"])), server
        )
        server.add_insecure_port(env.get("BF_LISTEN", "0.0.0.0:9090"))
        await server.start()
        _log.info("evaluation 已启动")
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await stop.wait()
        await server.stop(GRACE_SECONDS)


async def guard() -> int:
    env = os.environ
    async with (
        grpc.aio.insecure_channel(env["BF_CONFIG_ADDR"]) as gch,
        grpc.aio.insecure_channel(env["BF_ORCHESTRATOR_ADDR"]) as och,
    ):
        stopped = await service.guard(
            experiment_control_pb2_grpc.ExperimentControlServiceStub(gch),
            records_pb2_grpc.AnswerRecordServiceStub(och),
            dt.datetime.now(dt.UTC),
        )
    _log.info("护栏检查完成，停止 %d 个实验", len(stopped))
    return 0


async def online_sample(day: dt.date) -> int:
    env = os.environ
    # 抽样的租户由部署配置给出（逗号分隔）；新租户上线时加进来
    tenants = [t for t in env.get("BF_ONLINE_TENANTS", "").split(",") if t]
    async with (
        grpc.aio.insecure_channel(env["BF_ORCHESTRATOR_ADDR"]) as och,
        httpx.AsyncClient(timeout=30) as http,
    ):
        results = await online.run_day(
            records_pb2_grpc.AnswerRecordServiceStub(och),
            # 判定用的 NLI 应与线上校验器不同（evaluation 详设 6.1）
            KServeNLI(env["BF_NLI_URL"], http, env.get("BF_EVAL_NLI_MODEL") or "nli"),
            online.PGStore(env["BF_PG_DSN"]),
            tenants,
            day,
        )
    for d in results:
        _log.info(
            "在线抽样评审 %s %s：抽样 %d，Groundedness %.3f，引用精确率 %.3f",
            d.tenant,
            d.day,
            d.sampled,
            d.groundedness,
            d.citation_precision,
        )
    # 有租户失败时以非零退出，调度方据此重试与告警
    return 0 if len(results) == len(tenants) else 1


def main() -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(message)s")
    p = argparse.ArgumentParser(prog="python -m bambuflow_evaluation")
    sub = p.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run")
    run.add_argument("--golden", required=True)
    run.add_argument("--tenants", default="")
    run.add_argument("--repeat", type=int, default=3)
    run.add_argument("--out", default="/out")
    run.add_argument("--write-baseline", action="store_true")
    # 基线随评测环境区分：模拟服务（CI 回归用）与真实模型的指标不可比，各有一份
    run.add_argument("--baseline", default="baseline")
    ret = sub.add_parser("retrieval")
    ret.add_argument("--set", required=True)
    ret.add_argument("--out", default="/out")
    ret.add_argument("--write-baseline", action="store_true")
    ret.add_argument("--baseline", default="baseline")
    ret.add_argument("--rerank", action="store_true")
    rev = sub.add_parser("review")
    rev.add_argument("--golden", required=True)
    aff = sub.add_parser("affected")
    aff.add_argument("--golden", required=True)
    sub.add_parser("serve")
    sub.add_parser("guard")
    sub.add_parser("migrate")
    on = sub.add_parser("online-sample")
    on.add_argument("--day", default="")
    args = p.parse_args()
    if args.cmd == "migrate":
        asyncio.run(online.PGStore(os.environ["BF_PG_DSN"]).migrate())
        return 0
    if args.cmd == "online-sample":
        yesterday = dt.datetime.now(dt.UTC).date() - dt.timedelta(days=1)
        return asyncio.run(
            online_sample(dt.date.fromisoformat(args.day) if args.day else yesterday)
        )
    if args.cmd == "serve":
        asyncio.run(serve())
        return 0
    if args.cmd == "guard":
        return asyncio.run(guard())
    if args.cmd == "affected":
        tenants = {q.asker.tenant for q in golden.load(Path(args.golden)).questions}
        paths = [ln.strip() for ln in sys.stdin if ln.strip()]
        print(",".join(sorted(affected.affected(paths, tenants))))
        return 0
    if args.cmd == "review":
        print(golden.review_sheet(golden.load(Path(args.golden))), end="")
        return 0
    if args.cmd == "retrieval":
        return asyncio.run(evaluate_retrieval(args))
    return asyncio.run(evaluate(args))


if __name__ == "__main__":
    sys.exit(main())
