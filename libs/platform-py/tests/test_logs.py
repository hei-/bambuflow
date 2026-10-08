import json
import logging
import sys

from bambuflow_platform.logs import JSONFormatter, redact

JWT = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"
PHONE = "13812345678"
ID_CARD = "11010119900307777X"
BANK_CARD = "6222021234567890123"
EMAIL = "alice@corp.example"


def _format(msg: str, *args: object, exc: bool = False, **extra: object) -> str:
    logger = logging.getLogger("t")
    record = logger.makeRecord("t", logging.INFO, __file__, 1, msg, args, None, extra=extra)
    if exc:
        try:
            raise RuntimeError("上游拒绝令牌 " + JWT)
        except RuntimeError:
            record.exc_info = sys.exc_info()
    return JSONFormatter("test").format(record)


def test_sensitive_values_never_reach_output() -> None:
    sample = "s3cr3t-value"  # 按字段名 client_secret 整体替换
    out = _format(
        "用户 %s 提交了身份证 %s，联系 %s",
        PHONE,
        ID_CARD,
        EMAIL,
        exc=True,
        authorization="Bearer " + JWT,
        client_secret=sample,
        note="收款卡号 " + BANK_CARD,
        req={"password": "p@ss", "header": "bearer abc.def-123"},
        question="请查询客户的贷款余额",
    )
    leaked = [JWT, PHONE, ID_CARD, BANK_CARD, EMAIL, sample, "p@ss", "abc.def-123", "贷款余额"]
    for secret in leaked:
        assert secret not in out, f"日志中出现了敏感内容 {secret!r}: {out}"
    rec = json.loads(out)
    assert rec["service"] == "test"


def test_ordinary_numbers_kept() -> None:
    s = "索引版本 v20261004-3，耗时 1234 ms，返回 20 条"
    assert redact(s) == s


def test_id_card_not_split_as_phone() -> None:
    got = redact("证件 " + ID_CARD)
    assert "[已脱敏:id_card]" in got and "phone" not in got
