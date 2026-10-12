"""审计全覆盖（P2-T3，技术设计文档 8.3）：一次问答的候选、上下文、展示三类块进入审计，
审计段送达开启对象锁的桶后不能删除。三类块只进审计，不出现在对外响应里。
"""

import json
import os
import time
from typing import Any
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error

from .test_answers import QUESTION, ask

BUCKET = "bambuflow-audit"


def s3() -> Minio:
    u = urlparse(os.environ["BF_S3_ENDPOINT"])
    return Minio(
        u.netloc, os.environ["BF_S3_ACCESS_KEY"], os.environ["BF_S3_SECRET_KEY"], secure=False
    )


def find_record(c: Minio, answer_id: str) -> tuple[str, dict[str, Any]] | None:
    for obj in c.list_objects(BUCKET, prefix="gateway/", recursive=True):
        body = c.get_object(BUCKET, obj.object_name).read().decode()
        for line in body.splitlines():
            rec: dict[str, Any] = json.loads(line)
            if rec.get("answer_id") == answer_id:
                return str(obj.object_name), rec
    return None


def test_answer_blocks_reach_append_only_audit_bucket() -> None:
    a = ask("alice", "credit", QUESTION)
    assert "audit" not in a and "candidate_chunk_ids" not in json.dumps(a)
    c = s3()
    # gateway 每 5 秒送一次（compose 的 BF_AUDIT_SHIP_INTERVAL）
    found = None
    for _ in range(30):
        found = find_record(c, a["answer_id"])
        if found:
            break
        time.sleep(1)
    assert found, "审计桶里找不到这次问答"
    key, rec = found
    shown, context, cands = (
        set(rec["shown_chunk_ids"]),
        set(rec["context_chunk_ids"]),
        set(rec["candidate_chunk_ids"]),
    )
    assert shown == {x["chunk_id"] for x in a["citations"]}
    assert shown and shown <= context <= cands
    assert "question" not in rec and rec["question_sha256"]

    # 对象锁：保留期内删不掉
    try:
        c.remove_object(BUCKET, key, version_id=c.stat_object(BUCKET, key).version_id)
    except S3Error as e:
        assert e.code == "AccessDenied", e.code
    else:
        raise AssertionError("审计对象在保留期内被删除了")


def test_answer_audit_carries_versions_and_answer_digest() -> None:
    """回答记录按保留期销毁原文后，审计里仍能查到当时的版本组合（技术设计文档 8.3）。"""
    a = ask("alice", "credit", QUESTION)
    c = s3()
    found = None
    for _ in range(30):
        found = find_record(c, a["answer_id"])
        if found:
            break
        time.sleep(1)
    assert found, "审计桶里找不到这次问答"
    _, rec = found
    v = rec["versions"]
    assert v["index_version"] == a["why"]["index_version"] and v["generation_model"], rec
    assert rec["answer_status"] == a["status"] and len(rec["answer_sha256"]) == 64


def changes(c: Minio, service: str, action: str) -> list[dict[str, Any]]:
    out = []
    for obj in c.list_objects(BUCKET, prefix=f"{service}/changes/", recursive=True):
        rec: dict[str, Any] = json.loads(c.get_object(BUCKET, obj.object_name).read())
        if rec["action"] == action:
            out.append(rec)
    return out


def test_background_operations_write_change_audit() -> None:
    """不经 gateway 的后台操作同样审计：定时发布（make ingest 时发布过）与它之前的评测门禁。"""
    c = s3()
    published = changes(c, "indexing", "index.publish")
    assert published and published[-1]["actor"] == "service:indexing", published
    assert published[-1]["detail"]["docs"] > 0
    gates = changes(c, "evaluation", "gate.conclude")
    assert gates and gates[-1]["detail"]["conclusion"] in ("passed", "failed", "indeterminate"), (
        gates
    )
