"""会话与 agent 中间状态（orchestrator 详设第 10 节），存在 Redis。

会话只存理解下一轮所需的最少信息：最近 3 轮的问题（已做 PII 掩码）和上一轮引用的 chunk_id，
不存文档正文，也不存回答里的事实：下一轮的事实必须重新检索（详设 10.2）。
会话摘要要等选定生成摘要的模型（详设第 16 节待决问题），在那之前只存问题，规划器据此理解指代。

会话只属于发起它的用户：owner 是“租户 + 用户”的指纹，每轮校验；不匹配或已过期时当作新会话、
换一个新 ID，不报错，也不沿用对方给的 ID，避免暴露或覆盖他人的会话（详设 10.4）。
Redis 不可用时退化为单轮（degraded 标注 session），这一轮也不写会话：
没能校验归属，写入可能覆盖他人的会话。
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import redis.asyncio
from redis.exceptions import RedisError

from bambuflow_orchestrator import redact

_log = logging.getLogger(__name__)

SESSION_TTL = 24 * 3600  # 每轮续期
MAX_TURNS = 50
RECENT_QUESTIONS = 3
AGENT_STATE_TTL = 600  # agent 中间状态，回答完成后立即删除（详设 10.3）


class StoreUnavailableError(Exception):
    pass


class Store(Protocol):
    """带过期时间的键值存储；不可用或超时抛 StoreUnavailableError。"""

    async def get(self, key: str) -> bytes | None: ...

    async def set(self, key: str, value: bytes, ttl: int) -> None: ...

    async def delete(self, key: str) -> None: ...


def owner_of(tenant: str, sub: str) -> str:
    """用户指纹：键和值里都不出现用户名。"""
    return hashlib.sha256(f"{tenant}\x00{sub}".encode()).hexdigest()


def new_conversation_id() -> str:
    return "conv_" + secrets.token_hex(12)


@dataclass
class Session:
    conversation_id: str
    owner: str
    recent_questions: list[str] = field(default_factory=list)
    last_citations: list[str] = field(default_factory=list)
    turn_count: int = 0
    # False：没能读到并校验会话（Redis 不可用），本轮结束后不写
    writable: bool = True
    degraded: bool = False


class Sessions:
    def __init__(self, store: Store, clock: Callable[[], float] = time.time) -> None:
        self._store, self._clock = store, clock

    @staticmethod
    def _key(tenant: str, conversation_id: str) -> str:
        return f"bf:conv:{tenant}:{conversation_id}"

    async def open(self, tenant: str, owner: str, conversation_id: str) -> Session:
        """取会话；没有、不属于当前用户、过期或已满 50 轮时开一个新会话。"""
        if not conversation_id:
            return Session(new_conversation_id(), owner)
        try:
            raw = await self._store.get(self._key(tenant, conversation_id))
        except StoreUnavailableError as e:
            _log.warning("会话存储不可用，按单轮处理", extra={"err": str(e)})
            # 返回调用方自己给的 ID：本轮不读也不写，下一轮存储恢复后还能接上
            return Session(conversation_id, owner, writable=False, degraded=True)
        data = _load(raw)
        if (
            data is None
            or data.get("owner") != owner
            or not isinstance(data.get("turn_count"), int)
            or data["turn_count"] >= MAX_TURNS
        ):
            return Session(new_conversation_id(), owner)
        return Session(
            conversation_id,
            owner,
            recent_questions=[q for q in data.get("recent_questions", []) if isinstance(q, str)],
            last_citations=[c for c in data.get("last_citations", []) if isinstance(c, str)],
            turn_count=data["turn_count"],
        )

    async def record(self, tenant: str, session: Session, question: str, cited: list[str]) -> None:
        """本轮结束后更新会话；失败只记日志，不影响已经返回的回答（详设 10.2）。"""
        if not session.writable:
            return
        recent = [*session.recent_questions, redact.mask(question)][-RECENT_QUESTIONS:]
        value = {
            "owner": session.owner,
            "recent_questions": recent,
            "last_citations": cited,
            "turn_count": session.turn_count + 1,
            "updated_at": int(self._clock()),
        }
        try:
            await self._store.set(
                self._key(tenant, session.conversation_id),
                json.dumps(value, ensure_ascii=False).encode(),
                SESSION_TTL,
            )
        except StoreUnavailableError as e:
            _log.warning("会话写入失败", extra={"err": str(e)})


class AgentStates:
    """agent 路径的中间状态：子问题、轮次、候选证据的 chunk_id 与分数（不含正文），键含 answer_id。

    写入失败不影响回答，由调用方在 degraded 里标注 session。"""

    def __init__(self, store: Store) -> None:
        self._store = store

    @staticmethod
    def _key(tenant: str, answer_id: str) -> str:
        return f"bf:agent:{tenant}:{answer_id}"

    async def save(self, tenant: str, answer_id: str, state: dict[str, Any]) -> None:
        await self._store.set(
            self._key(tenant, answer_id),
            json.dumps(state, ensure_ascii=False).encode(),
            AGENT_STATE_TTL,
        )

    async def drop(self, tenant: str, answer_id: str) -> None:
        await self._store.delete(self._key(tenant, answer_id))


def _load(raw: bytes | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


class RedisStore:
    """Store 的 Redis 实现。每次操作的超时由客户端的 socket_timeout 决定（见 __main__）。"""

    def __init__(self, client: redis.asyncio.Redis) -> None:
        self._r = client

    async def get(self, key: str) -> bytes | None:
        try:
            value = await self._r.get(key)
        except (RedisError, OSError) as e:
            raise StoreUnavailableError(type(e).__name__) from e
        return value if isinstance(value, bytes) else None

    async def set(self, key: str, value: bytes, ttl: int) -> None:
        try:
            await self._r.set(key, value, ex=ttl)
        except (RedisError, OSError) as e:
            raise StoreUnavailableError(type(e).__name__) from e

    async def delete(self, key: str) -> None:
        try:
            await self._r.delete(key)
        except (RedisError, OSError) as e:
            raise StoreUnavailableError(type(e).__name__) from e
