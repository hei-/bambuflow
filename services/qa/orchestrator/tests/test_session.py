import asyncio

from bambuflow.common.v1 import types_pb2
from bambuflow_orchestrator import session
from bambuflow_orchestrator.session import Cited, Sessions, StoreUnavailableError

OWNER = session.owner_of("credit", "alice")


class MemStore:
    def __init__(self, fail: bool = False) -> None:
        self.data: dict[str, tuple[bytes, int]] = {}
        self.fail = fail

    async def get(self, key: str) -> bytes | None:
        if self.fail:
            raise StoreUnavailableError("down")
        v = self.data.get(key)
        return v[0] if v else None

    async def set(self, key: str, value: bytes, ttl: int) -> None:
        if self.fail:
            raise StoreUnavailableError("down")
        self.data[key] = (value, ttl)

    async def delete(self, key: str) -> None:
        if self.fail:
            raise StoreUnavailableError("down")
        self.data.pop(key, None)


def test_new_session_then_next_turn_sees_previous_question() -> None:
    store = MemStore()
    s = Sessions(store, clock=lambda: 100.0)

    async def go() -> session.Session:
        first = await s.open("credit", OWNER, "")
        assert first.conversation_id.startswith("conv_") and first.writable
        await s.record("credit", first, "我的手机号 13812345678 能办吗？", [Cited("wiki:a#1", "a")])
        return await s.open("credit", OWNER, first.conversation_id)

    nxt = asyncio.run(go())
    assert nxt.recent_questions == ["我的手机号 138****5678 能办吗？"], "问题存储前做 PII 掩码"
    assert nxt.last_citations == [Cited("wiki:a#1", "a")] and nxt.turn_count == 1
    ((key, (_, ttl)),) = store.data.items()
    assert key == f"bf:conv:credit:{nxt.conversation_id}" and ttl == session.SESSION_TTL
    assert "alice" not in key, "键里不出现用户名"


def test_only_recent_three_questions_are_kept() -> None:
    s = Sessions(MemStore())

    async def go() -> session.Session:
        cur = await s.open("credit", OWNER, "")
        for i in range(5):
            await s.record("credit", cur, f"问题{i}", [])
            cur = await s.open("credit", OWNER, cur.conversation_id)
        return cur

    assert asyncio.run(go()).recent_questions == ["问题2", "问题3", "问题4"]


def test_other_users_session_is_treated_as_new() -> None:
    store = MemStore()
    s = Sessions(store)

    async def go() -> tuple[session.Session, session.Session]:
        mine = await s.open("credit", OWNER, "")
        await s.record("credit", mine, "我的问题", [])
        theirs = await s.open("credit", session.owner_of("credit", "mallory"), mine.conversation_id)
        return mine, theirs

    mine, theirs = asyncio.run(go())
    assert theirs.conversation_id != mine.conversation_id, "不沿用他人的会话 ID"
    assert theirs.recent_questions == [] and not theirs.degraded


def test_expired_and_full_sessions_start_over() -> None:
    store = MemStore()
    s = Sessions(store)

    async def go() -> tuple[session.Session, session.Session]:
        expired = await s.open("credit", OWNER, "conv_gone")
        cur = await s.open("credit", OWNER, "")
        cur.turn_count = session.MAX_TURNS - 1
        await s.record("credit", cur, "q", [])
        full = await s.open("credit", OWNER, cur.conversation_id)
        return expired, full

    expired, full = asyncio.run(go())
    assert expired.conversation_id != "conv_gone" and full.turn_count == 0


def test_store_unavailable_degrades_to_single_turn_and_does_not_write() -> None:
    store = MemStore(fail=True)
    s = Sessions(store)

    async def go() -> session.Session:
        cur = await s.open("credit", OWNER, "conv_abc")
        store.fail = False
        await s.record("credit", cur, "q", [])
        return cur

    cur = asyncio.run(go())
    assert cur.degraded and not cur.writable and cur.conversation_id == "conv_abc"
    assert store.data == {}, "没校验归属的会话不能写，可能覆盖他人的会话"


def test_agent_state_has_ttl_and_is_dropped() -> None:
    store = MemStore()
    states = session.AgentStates(store)

    async def go() -> None:
        await states.save("credit", "ans_1", {"round": 1})
        assert store.data["bf:agent:credit:ans_1"][1] == session.AGENT_STATE_TTL
        await states.drop("credit", "ans_1")

    asyncio.run(go())
    assert store.data == {}


def test_summary_is_written_for_the_latest_turn_only() -> None:
    store = MemStore()
    s = Sessions(store)

    async def go() -> tuple[session.Session, session.Session]:
        cur = await s.open("credit", OWNER, "")
        turn = await s.record("credit", cur, "个人住房贷款首付比例", [])
        assert turn == 1
        await s.update_summary("credit", cur.conversation_id, OWNER, 1, "用户在问个人住房贷款")
        first = await s.open("credit", OWNER, cur.conversation_id)
        await s.record("credit", first, "二套呢", [])
        # 第 1 轮的摘要晚到：会话已经到第 2 轮，不覆盖
        await s.update_summary("credit", cur.conversation_id, OWNER, 1, "过时的摘要")
        # 别人的会话不写
        await s.update_summary(
            "credit", cur.conversation_id, session.owner_of("credit", "mallory"), 2, "x"
        )
        return first, await s.open("credit", OWNER, cur.conversation_id)

    first, second = asyncio.run(go())
    assert first.summary == "用户在问个人住房贷款"
    assert second.summary == "用户在问个人住房贷款" and second.turn_count == 2


def test_restricted_session_keeps_no_summary() -> None:
    s = Sessions(MemStore())

    async def go() -> session.Session:
        cur = await s.open("credit", OWNER, "")
        await s.record("credit", cur, "q1", [])
        await s.update_summary("credit", cur.conversation_id, OWNER, 1, "摘要")
        cur = await s.open("credit", OWNER, cur.conversation_id)
        turn = await s.record("credit", cur, "q2", [], types_pb2.CLASSIFICATION_RESTRICTED)
        assert turn is not None
        await s.update_summary("credit", cur.conversation_id, OWNER, turn, "受限级摘要")
        cur = await s.open("credit", OWNER, cur.conversation_id)
        # 之后的公开级轮次也不再存摘要
        await s.record("credit", cur, "q3", [], types_pb2.CLASSIFICATION_PUBLIC)
        await s.update_summary("credit", cur.conversation_id, OWNER, 3, "又一份摘要")
        return await s.open("credit", OWNER, cur.conversation_id)

    cur = asyncio.run(go())
    assert cur.restricted and cur.summary == "" and cur.recent_questions == ["q1", "q2", "q3"]


def test_delete_removes_only_own_session() -> None:
    store = MemStore()
    s = Sessions(store)

    async def go() -> None:
        mine = await s.open("credit", OWNER, "")
        await s.record("credit", mine, "q", [])
        await s.delete("credit", session.owner_of("credit", "mallory"), mine.conversation_id)
        assert store.data, "别人删不掉"
        await s.delete("credit", OWNER, "conv_missing")
        await s.delete("credit", OWNER, mine.conversation_id)
        assert store.data == {}
        store.fail = True
        try:
            await s.delete("credit", OWNER, mine.conversation_id)
        except StoreUnavailableError:
            return
        raise AssertionError("存储不可用时删除要报错，不能静默失败")

    asyncio.run(go())
