"""撤销误写的删除标记（Z6，入库管道详设第 5 节、14.4）：平台运维申请、另一位复核后执行；
执行后删除时间被清掉（文档仍不可见，直到重新入库），比原删除更早产生的内容也能重新写入，
并经 indexing 的发件表发出 RefetchRequested。
"""

import os
import time
import uuid

import psycopg

from bambuflow.ingestion.v1 import index_writer_pb2

from .admin import call
from .test_index_admin import SOURCE, live_models, tombstone, write

PATH = "/v1/admin/tombstone-revocations"


def acl_state(doc: str) -> tuple[str, object]:
    with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
        row = conn.execute(
            "SELECT state, tombstoned_at FROM idx.doc_acl WHERE doc_id = %s", (doc,)
        ).fetchone()
    assert row is not None
    return str(row[0]), row[1]


def test_revocation_requires_second_operator_and_allows_rewrite() -> None:
    _, model, chunker = live_models()
    doc = "e2e-rev-" + uuid.uuid4().hex[:8]
    assert write(doc, f"撤销删除标记测试 {doc}", "2025-03-01T00:00:00Z", model, chunker) == (
        index_writer_pb2.WRITE_RESULT_WRITTEN
    )
    assert tombstone(doc, "2025-03-02T00:00:00Z") == index_writer_pb2.TOMBSTONE_RESULT_TOMBSTONED
    body = {
        "source_id": SOURCE,
        "tombstoned_since": "2025-03-02T00:00:00Z",
        "tombstoned_until": "2025-03-03T00:00:00Z",
        "doc_ids": [doc],
        "reason": "e2e 清单故障误删",
    }
    # 只有平台运维能申请；没有符合条件的删除标记时 404
    assert call("dana", "POST", PATH, body).status_code == 403
    empty = call("olga", "POST", PATH, body | {"doc_ids": ["e2e-rev-none"]})
    assert empty.status_code == 404, empty.text

    r = call("olga", "POST", PATH, body)
    assert r.status_code in (200, 202), r.text
    op = r.json()
    assert op["status"] == "pending" and op["matched_docs"] == 1 and op["requested_by"] == "olga"
    # 申请本身不撤销
    assert acl_state(doc)[1] is not None

    approve = f"{PATH}/operations/{op['operation_id']}/approve"
    selfie = call("olga", "POST", approve)
    assert selfie.status_code == 403 and selfie.json()["type"] == "self_approval_forbidden"
    done = call("oscar", "POST", approve)
    assert done.status_code == 200, done.text
    d = done.json()
    assert (
        d["status"] == "executed"
        and d["revoked_docs"] == 1
        and d["refetch_event_id"].startswith("evt_")
    )
    # 仍不可见，只是删除时间清掉了
    assert acl_state(doc) == ("tombstoned", None)
    assert call("oscar", "POST", approve).json()["type"] == "conflict"

    # 删除优先不再挡住：比原删除更早产生的内容（重新拉取到的源端内容）也能写入并恢复可见
    assert write(doc, f"撤销删除标记测试 {doc} 恢复", "2025-03-01T12:00:00Z", model, chunker) == (
        index_writer_pb2.WRITE_RESULT_WRITTEN
    )
    assert acl_state(doc)[0] == "active"

    # RefetchRequested 经发件表转发到事件流
    deadline = time.monotonic() + 15
    sent = None
    while time.monotonic() < deadline and sent is None:
        with psycopg.connect(os.environ["BF_PG_DSN"]) as conn:
            row = conn.execute(
                "SELECT sent_at FROM idx.refetch_outbox WHERE event_id = %s",
                (d["refetch_event_id"],),
            ).fetchone()
        sent = row[0] if row else None
        time.sleep(1)
    assert sent is not None
