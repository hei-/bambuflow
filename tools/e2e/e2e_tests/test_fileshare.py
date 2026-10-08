"""共享盘样例源的端到端测试（P1-T2）：PDF、扫描件、Word、Excel、邮件、图片经 connectors →
processing（含 OCR 镜像）→ indexing 入库并发布后：

- 每份文件都在线上版本里，每块的 span 能回指原文：文本 PDF 按页码与区域从原文件取出的文字
  包含块的正文，扫描件与图片的区域落在页面或图片之内，邮件不含引用的历史正文与邮件地址；
- 经 gateway 检索时，有权用户能检索到、无权用户检索不到
  （按文件的 ACL 与密级计算期望的可见范围）。

需要先 make up PROFILE=core,ingest 与 make ingest。
"""

import os
from collections.abc import Iterator
from typing import Any

import httpx
import psycopg
import pymupdf
import pytest

from .auth import TEST_PASSWORD, platform_login
from .upstream import FILESHARE_DIR, fileshare_docs, visible_fileshare

SOURCE = "src_fileshare"
GATEWAY = os.environ.get("GATEWAY_URL", "http://gateway:8080")
DOCS = {d.id: d for d in fileshare_docs()}


@pytest.fixture(scope="module")
def db() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(os.environ["BF_PG_DSN"], autocommit=True) as conn:
        yield conn


def live_chunks(db: psycopg.Connection[Any]) -> list[tuple[str, str, str, dict[str, Any]]]:
    """线上版本里共享盘样例的块：(doc_id, chunk_id, 正文, span)。"""
    with db.cursor() as cur:
        cur.execute(
            """SELECT c.doc_id, c.chunk_id, c.text, c.span FROM idx.aliases a
               JOIN idx.version_docs vd ON vd.version = a.version
               JOIN idx.chunks c ON c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version
               JOIN idx.documents d ON d.doc_id = c.doc_id
               WHERE a.alias = 'prod' AND d.source_id = %s AND c.kind = 'content'
               ORDER BY c.doc_id, c.chunk_id""",
            (SOURCE,),
        )
        return [(r[0], r[1], r[2], r[3]) for r in cur.fetchall()]


def squash(s: str) -> str:
    return "".join(s.replace("|", " ").split())


def test_every_file_is_ingested_with_chunks(db: psycopg.Connection[Any]) -> None:
    chunks = live_chunks(db)
    assert {doc for doc, *_ in chunks} == set(DOCS), "每份样例文件都应入库且至少有一块"
    for _, chunk_id, text, span in chunks:
        assert chunk_id.startswith(SOURCE + ":") and text
        assert 0 <= span["char_start"] < span["char_end"], chunk_id


def test_text_pdf_spans_point_back_to_the_page(db: psycopg.Connection[Any]) -> None:
    chunks = [c for c in live_chunks(db) if c[0] == "fs-credit-rate-notice"]
    assert {c[3]["page"] for c in chunks} == {1, 2}
    with pymupdf.open(FILESHARE_DIR / DOCS["fs-credit-rate-notice"].file) as doc:
        for _, chunk_id, text, span in chunks:
            clip = pymupdf.Rect(span["bbox"]) + (-1, -1, 1, 1)
            page_text = doc[span["page"] - 1].get_text(clip=clip)
            assert squash(text) in squash(page_text), chunk_id
    table = [t for _, _, t, _ in chunks if t.startswith("| 产品 | 期限 | 利率下限 | 定价审批 |")]
    assert len(table) == 1, "表格保留表头"


def test_scanned_pdf_is_ocred_with_page_regions(db: psycopg.Connection[Any]) -> None:
    chunks = [c for c in live_chunks(db) if c[0] == "fs-fire-inspection"]
    for _, chunk_id, _, span in chunks:
        x0, y0, x1, y1 = span["bbox"]
        assert span["page"] in (1, 2) and 0 <= x0 < x1 <= 595 and 0 <= y0 < y1 <= 842, chunk_id
    text = squash("".join(t for _, _, t, _ in chunks))
    for phrase in ("监控录像是否保存不少于90天", "须在5个工作日内整改完毕"):
        assert phrase in text, f"OCR 结果应包含“{phrase}”"
    assert "第1页" not in text, "页码行不入库"


def test_image_is_one_chunk_with_region(db: psycopg.Connection[Any]) -> None:
    chunks = [c for c in live_chunks(db) if c[0] == "fs-visitor-notice"]
    assert len(chunks) == 1
    _, _, text, span = chunks[0]
    assert span["page"] is None and span["bbox"] is not None
    assert "访客不得进入机房和金库区域" in squash(text)


def test_office_and_mail_content(db: psycopg.Connection[Any]) -> None:
    by_doc: dict[str, str] = {}
    for doc, _, text, span in live_chunks(db):
        if doc in ("fs-it-asset", "fs-risk-limit-monitor", "fs-aml-training-mail"):
            assert span["page"] is None and span["bbox"] is None
            by_doc[doc] = by_doc.get(doc, "") + text + "\n"
    assert "| 设备类别 | 标准配置 | 使用年限 | 报废审批 |" in by_doc["fs-it-asset"]
    assert (
        "| 前十大客户授信集中度 | 50% | 46.5% | 93.0% | 预警 |" in by_doc["fs-risk-limit-monitor"]
    )
    mail = by_doc["fs-aml-training-mail"]
    assert "考试合格线为 80 分" in mail and "| 场次 | 日期 | 时间 | 形式 |" in mail
    assert "70 分" not in mail and "@" not in mail and "分机" not in mail


def search_docs(user: str, tenant: str) -> set[str]:
    tok = platform_login(user, TEST_PASSWORD)["access_token"]
    resp = httpx.post(
        GATEWAY + "/v1/search",
        json={"question": "制度通知的要求", "top_k": 50, "filters": {"source_ids": [SOURCE]}},
        headers={"Authorization": "Bearer " + tok, "Bambuflow-Tenant": tenant},
        timeout=10,
    )
    assert resp.status_code == 200, resp.text
    hits = resp.json()["hits"]
    for h in hits:
        assert h["chunk_id"].startswith(SOURCE + ":") and h["span"]["char_end"] > 0
    return {h["doc_id"] for h in hits}


@pytest.mark.parametrize(
    ("user", "tenant", "tokens", "clearance"),
    [
        ("alice", "credit", {"group:credit-l2", "group:all-staff", "user:alice"}, 2),
        ("bob", "risk", {"group:risk-dept", "group:all-staff", "user:bob"}, 3),
        ("eve", "ops", {"group:all-staff", "user:eve"}, 1),
    ],
)
def test_search_sees_exactly_the_visible_files(
    user: str, tenant: str, tokens: set[str], clearance: int
) -> None:
    # 样例的块数远少于 top_k，按数据源过滤后能取回全部可见的块，所以期望与实际应完全相等
    expected = visible_fileshare(tokens, clearance)
    assert expected and expected != set(DOCS)
    assert search_docs(user, tenant) == expected
