"""日志数据源（P3-I3，技术设计文档 5.2、5.5）。

logs-seed 向日志主题（开发默认实现是 Redis Streams）写确定的测试日志，connectors-logs 订阅、脱敏、
只取配置的服务与级别，按“一个服务一天”成文档、每 5 分钟窗口一块。make ingest 连跑两次：
第二次不应产生新的内容版本（偏移量去重、正文不变不重发）。需要先 make ingest。
"""

import json
import os
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import psycopg
from minio import Minio

from .test_answers import search_hits

SOURCE = "src_applogs"
PHONE = re.compile(r"1[3-9]\d{9}")


def log_docs() -> dict[str, dict[str, Any]]:
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        rows = conn.execute(
            """SELECT d.doc_id, d.doc_version, d.source_type, a.acl_tokens, a.state
               FROM idx.documents d JOIN idx.doc_acl a USING (doc_id)
               WHERE d.source_id = %s""",
            (SOURCE,),
        ).fetchall()
    return {r[0]: {"version": r[1], "source_type": r[2], "acl": r[3], "state": r[4]} for r in rows}


def chunks(doc_id: str) -> list[tuple[list[str], str]]:
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        rows = conn.execute(
            """SELECT c.heading_path, c.text FROM idx.aliases al
               JOIN idx.version_docs vd ON vd.version = al.version
               JOIN idx.chunks c ON c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version
               WHERE al.alias = 'prod' AND vd.doc_id = %s AND c.kind = 'content'
               ORDER BY c.heading_path""",
            (doc_id,),
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def days() -> list[str]:
    today = datetime.now(UTC).date()
    return [(today - timedelta(days=d)).strftime("%Y%m%d") for d in (2, 1)]


def test_only_configured_services_within_retention_are_ingested() -> None:
    docs = log_docs()
    want = {f"{s}-{d}" for s in ("payments-svc", "core-batch") for d in days()}
    assert want <= set(docs), f"缺少 {sorted(want - set(docs))}"
    # 未配置的服务、过了保留期（40 天前）的日志都不入库
    assert not any(d.startswith("marketing-web-") for d in docs)
    old = (datetime.now(UTC).date() - timedelta(days=40)).strftime("%Y%m%d")
    assert not any(d.endswith(old) for d in docs)
    for d in want:
        assert docs[d]["source_type"] == 6 and docs[d]["state"] == "active"
        # make ingest 跑了两次：重复消费不产生新的内容版本
        assert docs[d]["version"] == 1, f"{d} 的版本 {docs[d]['version']}"


def test_acl_follows_service_ownership() -> None:
    docs = log_docs()
    for d, info in docs.items():
        if d.startswith("payments-svc-"):
            assert info["acl"] == ["group:sre-oncall"]
        elif d.startswith("core-batch-"):
            assert info["acl"] == ["group:sre-core"]


def test_windows_templates_and_redaction() -> None:
    doc = f"payments-svc-{days()[1]}"
    cs = chunks(doc)
    assert [h[-1] for h, _ in cs] == ["08:00–08:05", "08:05–08:10", "08:10–08:15"]
    text = "\n".join(t for _, t in cs)
    assert "[ERROR] payment timeout order=<*> user=<*> after <*>" in text
    assert "[WARN] channel unionpay retry <*> latency <*>" in text
    # 只入库配置的级别；手机号在进入管道前已脱敏
    assert "request served" not in text and "debug dump" not in text
    assert not PHONE.search(text)


def test_state_kept_by_connectors_is_redacted() -> None:
    u = urlparse(os.environ["BF_S3_ENDPOINT"])
    s3 = Minio(
        u.netloc, os.environ["BF_S3_ACCESS_KEY"], os.environ["BF_S3_SECRET_KEY"], secure=False
    )
    obj = s3.get_object("bambuflow-raw", f"state/{SOURCE}/payments-svc-{days()[1]}.json")
    try:
        state = json.loads(obj.read())
    finally:
        obj.close()
        obj.release_conn()
    texts = [e["text"] for e in state["entries"]]
    assert len(texts) == 12 and all("[已脱敏:phone]" in t for t in texts if "timeout" in t)
    assert not any(PHONE.search(t) for t in texts)


def test_log_documents_have_no_graph_relations() -> None:
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        n = conn.execute(
            "SELECT count(*) FROM idx.relations r JOIN idx.documents d USING (doc_id) "
            "WHERE d.source_id = %s",
            (SOURCE,),
        ).fetchone()
    assert n is not None and n[0] == 0


def test_search_respects_service_ownership() -> None:
    hits = search_hits("sam", "ops", "payment timeout batch job failed")
    docs = {h["doc_id"] for h in hits.values() if h["source_type"] == "log"}
    assert any(d.startswith("payments-svc-") for d in docs)
    assert not any(d.startswith("core-batch-") for d in docs)
    # 其他租户与运维组以外的人看不到
    assert not any(
        h["source_type"] == "log" for h in search_hits("eve", "ops", "payment timeout").values()
    )
