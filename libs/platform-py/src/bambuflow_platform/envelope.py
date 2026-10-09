"""应用层信封加密（技术设计文档 8.4）：每份数据一个数据密钥，AES-256-GCM 加密，
数据密钥由密钥服务的主密钥包裹后与密文一起存；主密钥不离开密钥服务。
默认密钥服务是 Vault transit（datakey 生成、decrypt 解包）。

密文格式：12 字节随机 nonce + 密文（含认证标签）；附加数据（aad）绑定数据所属的对象，
密文换到别的对象上无法解开。
使用方：processing（PII 原值，按块）、orchestrator（问题原文与受限级回答快照，按回答）。
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from typing import Protocol

import httpx
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


@dataclass(frozen=True)
class DataKey:
    plaintext: bytes
    # 被主密钥包裹后的数据密钥（Vault transit 的 vault:v1:… 形式）
    wrapped: str
    key_id: str


def seal(dek: bytes, plaintext: bytes, aad: str) -> bytes:
    nonce = os.urandom(12)
    return nonce + AESGCM(dek).encrypt(nonce, plaintext, aad.encode())


class DecryptError(ValueError):
    """密文、数据密钥或附加数据对不上（被篡改、换了对象或换了密钥）。"""


def unseal(dek: bytes, blob: bytes, aad: str) -> bytes:
    try:
        return AESGCM(dek).decrypt(blob[:12], blob[12:], aad.encode())
    except InvalidTag as e:
        raise DecryptError("解密失败") from e


class KeyService(Protocol):
    """主密钥服务（端口，同步）：生成数据密钥。"""

    key_id: str

    def data_key(self) -> tuple[bytes, str]: ...


class VaultTransit:
    """KeyService 的默认实现（同步）：Vault transit 的 datakey 接口。"""

    def __init__(self, addr: str, token: str, key_id: str, http: httpx.Client) -> None:
        self._url = f"{addr.rstrip('/')}/v1/transit/datakey/plaintext/{key_id}"
        self._token, self.key_id, self._http = token, key_id, http

    def data_key(self) -> tuple[bytes, str]:
        resp = self._http.post(self._url, headers={"X-Vault-Token": self._token}, json={})
        resp.raise_for_status()
        data = resp.json()["data"]
        return base64.b64decode(data["plaintext"]), str(data["ciphertext"])


class AsyncKeyService(Protocol):
    """主密钥服务（端口，异步）：生成数据密钥、解包数据密钥。"""

    key_id: str

    async def data_key(self) -> DataKey: ...

    async def unwrap(self, wrapped: str) -> bytes: ...


class AsyncVaultTransit:
    """AsyncKeyService 的默认实现：Vault transit 的 datakey 与 decrypt 接口。"""

    def __init__(
        self, addr: str, token: str, key_id: str, http: httpx.AsyncClient, token_file: str = ""
    ) -> None:
        self._base = f"{addr.rstrip('/')}/v1/transit"
        self._token, self.key_id, self._http = token, key_id, http
        # Vault Agent 注入的令牌文件会续期改写，
        # 每次调用重新读（与 retrieval 的 pii.VaultTransit 相同）
        self._token_file = token_file

    def _headers(self) -> dict[str, str]:
        token = self._token
        if self._token_file:
            with open(self._token_file, encoding="utf-8") as f:
                token = f.read().strip()
        return {"X-Vault-Token": token}

    async def data_key(self) -> DataKey:
        resp = await self._http.post(
            f"{self._base}/datakey/plaintext/{self.key_id}",
            headers=self._headers(),
            json={},
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        return DataKey(base64.b64decode(data["plaintext"]), str(data["ciphertext"]), self.key_id)

    async def unwrap(self, wrapped: str) -> bytes:
        resp = await self._http.post(
            f"{self._base}/decrypt/{self.key_id}",
            headers=self._headers(),
            json={"ciphertext": wrapped},
        )
        resp.raise_for_status()
        return base64.b64decode(resp.json()["data"]["plaintext"])
