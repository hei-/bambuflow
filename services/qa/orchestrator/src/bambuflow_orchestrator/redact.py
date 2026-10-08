"""PII 掩码（技术设计文档 8.2）：送入 LLM 前按用户密级再做一次，是防止索引漏掉 PII 的第二道防线。

规则与 retrieval 的 Go 实现相同，两边以 tools/test-upstream/pii_cases.json 为共同样例。
掩码是幂等的：已掩码的文本不会再被识别，引文核对以掩码后的正文为准。
"""

from __future__ import annotations

import re

_ID = re.compile(
    r"(?<![0-9])([1-9][0-9]{5}(?:19|20)[0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])"
    r"[0-9]{3}[0-9Xx])(?![0-9Xx])"
)
_MOBILE = re.compile(r"(?<![0-9])(1[3-9][0-9]{9})(?![0-9])")
_CARD = re.compile(r"(?<![0-9])([0-9]{16,19})(?![0-9])")
_EMAIL = re.compile(r"([A-Za-z0-9._%+-])[A-Za-z0-9._%+-]*@([A-Za-z0-9.-]+\.[A-Za-z]{2,})")


def _luhn(s: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(s)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def _card(m: re.Match[str]) -> str:
    s = m.group(1)
    return s[:4] + "*" * (len(s) - 8) + s[-4:] if _luhn(s) else s


def mask(text: str) -> str:
    text = _ID.sub(lambda m: m.group(1)[:6] + "*" * 8 + m.group(1)[14:], text)
    text = _CARD.sub(_card, text)
    text = _MOBILE.sub(lambda m: m.group(1)[:3] + "****" + m.group(1)[7:], text)
    return _EMAIL.sub(r"\1***@\2", text)
