"""统一的结构化 JSON 日志，在日志库层脱敏（技术设计文档 12.5）。

规则与 libs/platform-go/logging 一致。

按字段名整体替换，再按内容模式替换消息和字符串字段里的令牌、手机号、身份证号、银行卡号、邮箱。
这是最后一道兜底，调用方仍不应主动记录这些内容。
"""

from __future__ import annotations

import json
import logging
import re
import sys
from typing import Any

REDACTED = "[已脱敏]"

_SENSITIVE_KEYS = {
    "authorization", "proxyauthorization", "cookie", "setcookie",
    "token", "accesstoken", "refreshtoken", "idtoken",
    "password", "passwd", "secret", "clientsecret", "apikey", "privatekey",
    "question", "query", "answer",
}  # fmt: skip

# 先替换较长、较具体的模式，避免身份证号被当成手机号截掉一段（与 Go 版顺序一致）
_PATTERNS = [
    (re.compile(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*"), "jwt"),
    (re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+"), "bearer"),
    (re.compile(r"(?<!\d)\d{17}[\dXx](?![\dA-Za-z])"), "id_card"),
    (re.compile(r"(?<!\d)\d{16,19}(?!\d)"), "bank_card"),
    (re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)"), "phone"),
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), "email"),
]

# LogRecord 自带的属性，不当作调用方传入的 extra 字段输出
_RECORD_ATTRS = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "taskName"}


def redact(s: str) -> str:
    for pattern, kind in _PATTERNS:
        s = pattern.sub(f"[已脱敏:{kind}]", s)
    return s


def _normalize(key: str) -> str:
    return key.lower().replace("-", "").replace("_", "")


def _clean(key: str, value: Any) -> Any:
    if _normalize(key) in _SENSITIVE_KEYS:
        return REDACTED
    if isinstance(value, dict):
        return {k: _clean(str(k), v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(key, v) for v in value]
    if isinstance(value, (bool, int, float)) or value is None:
        return value
    return redact(str(value))


class JSONFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "msg": redact(record.getMessage()),
            "service": self._service,
            "logger": record.name,
        }
        for key, value in vars(record).items():
            if key not in _RECORD_ATTRS:
                out[key] = _clean(key, value)
        if record.exc_info:
            out["exc"] = redact(self.formatException(record.exc_info))
        return json.dumps(out, ensure_ascii=False)


def setup(service: str, level: int = logging.INFO) -> None:
    """把根日志器配置为输出到 stdout 的 JSON 日志。"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter(service))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
