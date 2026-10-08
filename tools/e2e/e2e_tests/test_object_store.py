"""对象存储兼容性测试（ADR-9、12.7）：P0 参考后端 RustFS 是否满足平台对 S3 的要求。

读写、版本化、对象锁（审计 WORM 依赖它，P2 用到）。
对象锁的测试会留下一个保留 1 天的桶，到期后可清理。
"""

import io
import os
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import pytest
from minio import Minio
from minio.commonconfig import COMPLIANCE, ENABLED
from minio.error import S3Error
from minio.retention import Retention
from minio.versioningconfig import VersioningConfig


@pytest.fixture(scope="module")
def s3() -> Minio:
    u = urlparse(os.environ["BF_S3_ENDPOINT"])
    return Minio(
        u.netloc, os.environ["BF_S3_ACCESS_KEY"], os.environ["BF_S3_SECRET_KEY"], secure=False
    )


def bucket(s3: Minio, object_lock: bool = False) -> str:
    name = f"e2e-{uuid.uuid4().hex[:12]}"
    s3.make_bucket(name, object_lock=object_lock)
    return name


def put(s3: Minio, b: str, key: str, data: bytes, retention: Retention | None = None) -> str | None:
    res = s3.put_object(b, key, io.BytesIO(data), len(data), retention=retention)
    return res.version_id


def test_put_get_roundtrip(s3: Minio) -> None:
    b = bucket(s3)
    put(s3, b, "a.txt", "你好".encode())
    resp = s3.get_object(b, "a.txt")
    try:
        assert resp.read() == "你好".encode()
    finally:
        resp.close()
        resp.release_conn()


def test_versioning_keeps_old_versions(s3: Minio) -> None:
    b = bucket(s3)
    s3.set_bucket_versioning(b, VersioningConfig(ENABLED))
    v1 = put(s3, b, "k", b"one")
    v2 = put(s3, b, "k", b"two")
    assert v1 and v2 and v1 != v2
    versions = [o.version_id for o in s3.list_objects(b, include_version=True)]
    assert {v1, v2} <= set(versions)


def test_object_lock_blocks_deletion(s3: Minio) -> None:
    b = bucket(s3, object_lock=True)
    until = datetime.now(UTC) + timedelta(days=1)
    v = put(s3, b, "audit.jsonl", b"{}", retention=Retention(COMPLIANCE, until))
    assert v
    got = s3.get_object_retention(b, "audit.jsonl", version_id=v)
    assert got is not None and got.mode == COMPLIANCE
    with pytest.raises(S3Error):
        s3.remove_object(b, "audit.jsonl", version_id=v)
