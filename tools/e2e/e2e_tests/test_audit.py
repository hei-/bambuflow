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
