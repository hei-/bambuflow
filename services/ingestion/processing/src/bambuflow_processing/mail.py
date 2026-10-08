"""邮件解析（标准库 email，MIME）：技术设计文档 5.2。

- 正文取纯文本部分，没有时取 HTML 转成的文本；
- 去掉引用的历史正文（回复里附带的原邮件）、签名和免责声明，只保留这封邮件新写的内容；
- 发件人、收件人只保留显示名，不入库邮件地址；
- 附件递归交给对应的解析器，块的标题路径为“主题 > 附件：文件名”。不支持的附件跳过并记日志。

ACL 由连接器给出（收发件人、共享邮箱成员展开），这里不处理。
"""

from __future__ import annotations

import email
import email.policy
import logging
import re
from collections.abc import Callable
from email.message import EmailMessage
from email.utils import getaddresses, parsedate_to_datetime
from html.parser import HTMLParser

from bambuflow_processing.blocks import Block, ParseError, UnsupportedFormatError

_log = logging.getLogger(__name__)

# 回复、转发时附带的原邮件从这些行开始，之后的内容都不属于这封邮件
_QUOTE_START = re.compile(
    r"^\s*(-{2,}\s*(原始邮件|转发邮件|Original Message|Forwarded message)\s*-{2,}"
    r"|在.+写道[:：]|On .+ wrote:)\s*$"
)
_SIGNATURE = re.compile(r"^--\s?$")  # 标准的签名分隔行“-- ”
_DISCLAIMER = re.compile(r"^\s*(免责声明|保密声明|Disclaimer|Confidentiality)\s*[:：]", re.I)

# 附件的解析器：(内容类型, 数据, 标题) -> 块。由 parsers.parse 传入，避免循环导入
AttachmentParser = Callable[[str, bytes, str], list[Block]]


def parse_eml(data: bytes, title: str, attachment: AttachmentParser) -> list[Block]:
    msg = email.message_from_bytes(data, policy=email.policy.default)
    if not isinstance(msg, EmailMessage):
        raise ParseError("不是合法的邮件")
    subject = " ".join(str(msg.get("Subject", "")).split()) or title
    path = (subject,)
    out = [Block("heading", subject, path)]
    header = _header_line(msg)
    if header:
        out.append(Block("paragraph", header, path))
    out += [Block("paragraph", p, path) for p in _paragraphs(_body_text(msg))]
    for part in msg.iter_attachments():
        if not isinstance(part, EmailMessage):
            continue
        name = part.get_filename() or "未命名附件"
        payload = part.get_payload(decode=True)
        if not isinstance(payload, bytes) or not payload:
            continue
        ctype = part.get_content_type()
        try:
            inner = attachment(ctype, payload, f"附件：{name}")
        except UnsupportedFormatError:
            _log.warning("跳过不支持的附件", extra={"content_type": ctype})
            continue
        out += [
            Block(b.kind, b.text, (subject, *b.path), b.page, b.bbox, b.rows, b.row_boxes)
            for b in inner
        ]
    return out


def _header_line(msg: EmailMessage) -> str:
    parts = []
    sender = _names(msg.get_all("From", []))
    if sender:
        parts.append("发件人：" + "、".join(sender))
    to = _names(msg.get_all("To", []) + msg.get_all("Cc", []))
    if to:
        parts.append("收件人：" + "、".join(to))
    date = _date(str(msg.get("Date", "")))
    if date:
        parts.append("日期：" + date)
    return "；".join(parts)


def _date(value: str) -> str:
    """日期头不合法时不写日期，而不是让整封邮件解析失败。"""
    if not value:
        return ""
    try:
        return parsedate_to_datetime(value).date().isoformat()
    except (TypeError, ValueError):
        return ""


def _names(values: list[str]) -> list[str]:
    # 只留显示名；没有显示名时用地址的本地部分（多为部门或群组邮箱），不入库完整地址
    return [name or addr.split("@")[0] for name, addr in getaddresses([str(v) for v in values])]


def _body_text(msg: EmailMessage) -> str:
    part = msg.get_body(preferencelist=("plain", "html"))
    if not isinstance(part, EmailMessage):
        return ""
    content = part.get_content()
    if not isinstance(content, str):
        return ""
    if part.get_content_subtype() == "html":
        return _html_text(content)
    return content


def _paragraphs(body: str) -> list[str]:
    kept: list[str] = []
    for line in body.replace("\r\n", "\n").split("\n"):
        if _QUOTE_START.match(line) or _SIGNATURE.match(line):
            break
        if line.lstrip().startswith(">"):
            continue
        kept.append(line.rstrip())
    paras = [p.strip() for p in "\n".join(kept).split("\n\n")]
    return [p for p in paras if p and not _DISCLAIMER.match(p)]


class _TextExtractor(HTMLParser):
    _BREAKS = frozenset({"p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "table"})

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style"):
            self._skip += 1
        elif tag in self._BREAKS:
            self.parts.append("\n\n" if tag in ("p", "div", "table") else "\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style") and self._skip:
            self._skip -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self.parts.append(data)


def _html_text(html: str) -> str:
    p = _TextExtractor()
    p.feed(html)
    p.close()
    return re.sub(r"\n{3,}", "\n\n", "".join(p.parts))
