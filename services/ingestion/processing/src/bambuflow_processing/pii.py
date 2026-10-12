"""PII 掩码与原值的信封加密（入库管道详设第 6 节，技术设计文档 8.2、8.6）。

入库时识别身份证号、银行卡号（Luhn 校验）、手机号、邮箱，索引只存**等长**掩码后的文本：长度不变，
块的原文位置（span）与引用定位照常成立。原值按块收集成 [[start, end, 原值], …]，用文档级数据密钥
AES-256-GCM 加密（附加数据为 chunk_id，密文换到别的块上无法解开），数据密钥由密钥服务的主密钥包裹。
只有策略允许看 PII 原文的调用者，retrieval 才解开并还原（retrieval/internal/pii）。

密钥服务是端口（KeyService，libs/platform-py 的 envelope），默认实现是 Vault transit：
主密钥不离开 Vault，
每份文档申请一个数据密钥。
没有配置密钥服务时只掩码、不保存原值：原文绝不以明文进索引，能看 PII 的人也只能看到掩码。

规则之外还可以接 NER（NerModel 端口，BF_NER_URL）识别人名、地址等；NER 只在入库时用。
识别规则与 retrieval（Go）、orchestrator 的掩码规则相同，
以 tools/test-upstream/pii_cases.json 为共同样例；
区别只在掩码形式：这里等长（邮箱的用户名除首字符外逐字替换为 *）。
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from bambuflow_platform import envelope
from bambuflow_platform.envelope import KeyService

_ID = re.compile(
    r"(?<![0-9])([1-9][0-9]{5}(?:19|20)[0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])"
    r"[0-9]{3}[0-9Xx])(?![0-9Xx])"
)
_CARD = re.compile(r"(?<![0-9])([0-9]{16,19})(?![0-9])")
_MOBILE = re.compile(r"(?<![0-9])(1[3-9][0-9]{9})(?![0-9])")
_EMAIL = re.compile(r"(?<![A-Za-z0-9._%+-])([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+\.[A-Za-z]{2,})")


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


@dataclass(frozen=True)
class Found:
    start: int
    end: int
    original: str
    masked: str


# NER 识别的实体类型（入库管道详设第 6 节：规则 + NER）：
# 规则认不出的人名、地址、单位内部的账户名称等
NER_LABELS = frozenset({"person", "address", "account_name"})
NER_TIMEOUT_SECONDS = 30.0


class NerError(RuntimeError):
    """NER 服务不可用或返回了不合法的结果。

    脱敏失败即停在本阶段（入库管道详设第 11 节），文档留在队列里重试。"""


@dataclass(frozen=True)
class Entity:
    start: int
    end: int
    label: str


class NerModel(Protocol):
    """NER 端口：与 OCR 一样是平台内部部署的模型服务，不经外部模型。"""

    def entities(self, text: str) -> list[Entity]: ...


class HttpNerModel:
    """调用 NER 服务的 POST /v1/ner：请求 {"text": ...}，
    响应 {"entities": [{"start", "end", "label"}]}
    （start、end 是 Unicode 字符位置，左闭右开）。"""

    def __init__(self, base_url: str, client: httpx.Client | None = None) -> None:
        self._url = base_url.rstrip("/") + "/v1/ner"
        self._client = client or httpx.Client(timeout=NER_TIMEOUT_SECONDS)

    def entities(self, text: str) -> list[Entity]:
        try:
            resp = self._client.post(self._url, json={"text": text})
            resp.raise_for_status()
            body: Any = resp.json()
        except (httpx.HTTPError, ValueError) as e:
            raise NerError(f"NER 服务调用失败: {e}") from e
        if not isinstance(body, dict) or not isinstance(body.get("entities"), list):
            raise NerError("NER 响应缺少 entities")
        out = []
        for item in body["entities"]:
            if not isinstance(item, dict):
                raise NerError("NER 响应的实体格式不对")
            start, end, label = item.get("start"), item.get("end"), item.get("label")
            if not (
                isinstance(start, int) and isinstance(end, int) and 0 <= start < end <= len(text)
            ):
                # 坐标越界就整段失败：错位的掩码会盖掉不该盖的字、漏掉该盖的
                raise NerError("NER 响应的实体位置越界")
            if label in NER_LABELS:
                out.append(Entity(start, end, str(label)))
        return out


def find(text: str, ner: NerModel | None = None) -> list[Found]:
    """文本里的 PII 片段（按位置排序、互不重叠），每个都给出等长的掩码。
    先按规则（证件号、卡号、手机号、邮箱），再加 NER 识别的人名、地址等；
    与规则结果重叠的 NER 实体不再重复掩码。"""
    out: list[Found] = []
    taken: list[tuple[int, int]] = []

    def add(start: int, end: int, original: str, masked: str) -> None:
        if any(start < e and s < end for s, e in taken):
            return
        taken.append((start, end))
        out.append(Found(start, end, original, masked))

    for m in _ID.finditer(text):
        s = m.group(1)
        add(m.start(1), m.end(1), s, s[:6] + "*" * 8 + s[14:])
    for m in _CARD.finditer(text):
        s = m.group(1)
        if _luhn(s):
            add(m.start(1), m.end(1), s, s[:4] + "*" * (len(s) - 8) + s[-4:])
    for m in _MOBILE.finditer(text):
        s = m.group(1)
        add(m.start(1), m.end(1), s, s[:3] + "****" + s[7:])
    for m in _EMAIL.finditer(text):
        user = m.group(1)
        add(m.start(1), m.end(1), user, user[0] + "*" * (len(user) - 1))
    if ner is not None:
        for e in ner.entities(text):
            s = text[e.start : e.end]
            # 保留首字（“张**”“上海市********”），便于读者知道这里原本是什么
            add(e.start, e.end, s, s[0] + "*" * (len(s) - 1))
    return sorted(out, key=lambda f: f.start)


def mask(text: str, ner: NerModel | None = None) -> tuple[str, list[Found]]:
    """等长掩码后的文本与被替换的片段。"""
    found = find(text, ner)
    parts: list[str] = []
    pos = 0
    for f in found:
        parts.append(text[pos : f.start])
        parts.append(f.masked)
        pos = f.end
    parts.append(text[pos:])
    return "".join(parts), found


@dataclass(frozen=True)
class Sealed:
    ciphertext: bytes
    wrapped_key: str
    key_id: str


class DocumentKey:
    """一份文档的数据密钥：第一次需要时向密钥服务申请，文档内的块共用。"""

    def __init__(self, keys: KeyService) -> None:
        self._keys = keys
        self._key: tuple[bytes, str] | None = None

    def seal(self, chunk_id: str, found: Sequence[Found]) -> Sealed:
        if self._key is None:
            self._key = self._keys.data_key()
        dek, wrapped = self._key
        plain = json.dumps([[f.start, f.end, f.original] for f in found], ensure_ascii=False)
        return Sealed(envelope.seal(dek, plain.encode(), chunk_id), wrapped, self._keys.key_id)


def unseal(dek: bytes, chunk_id: str, ciphertext: bytes) -> list[tuple[int, int, str]]:
    """解开一个块的原值（测试与运维核对用；在线还原在 retrieval）。"""
    plain = envelope.unseal(dek, ciphertext, chunk_id)
    return [(int(s), int(e), str(o)) for s, e, o in json.loads(plain)]
