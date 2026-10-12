"""LLM 评审（evaluation 详设 6.1、6.2）：判 Faithfulness 与答案正确率。

- Faithfulness：回答与它引用的全部引文对照，有没有与证据矛盾、或证据里没有的说法；
- 答案正确率：标准答案的每个要点（key_points）回答里是否覆盖。

评审模型应与生成模型不同（至少不同版本），温度 0，输出结构化 JSON；
评审提示词版本与模型一起算“判定器版本”，
写进报告。同一份输入的结果按（输入摘要，判定器版本）在进程内缓存，三次重复运行只评一次。
受限级回答只能用允许处理受限数据的模型评审（与线上密级路由一致）：评审模型目前只配一个、
不区分密级，所以带受限级引用的回答不评，计入“未评审”。
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

_log = logging.getLogger(__name__)

PROMPT_VERSION = "judge@1"

SYSTEM = """你是问答系统的评审员，只依据给出的引文判断回答，不用你自己的知识。
输出一个 JSON 对象，不要输出其他内容：
{"faithful": true 或 false, "issues": ["与引文矛盾或引文里没有的说法，原样摘出"],
 "key_points": [{"index": 要点序号, "covered": true 或 false}]}
判断规则：
1. 回答里任何一个事实说法与引文矛盾，或引文里找不到依据，faithful 为 false，
   并在 issues 里列出该说法。
2. 措辞不同但意思与引文一致，算有依据；回答说“未找到依据”“证据不足”不算违背。
3. key_points 逐条判断回答是否表达了该要点的意思，不要求原词；没有给出要点时返回空数组。"""


@dataclass(frozen=True)
class Judgment:
    # None 表示没有评审（没配评审模型、受限级、评审失败）
    faithful: bool | None
    key_points: tuple[bool, ...] = ()


NOT_JUDGED = Judgment(None)


class JudgeError(Exception):
    """评审模型不可用或输出无法解析。"""


class Judge(Protocol):
    @property
    def version(self) -> str: ...

    async def judge(
        self, question: str, answer: str, quotes: Sequence[str], key_points: Sequence[str]
    ) -> Judgment: ...


class OpenAIJudge:
    """OpenAI 兼容的 chat completions 端点（公司 LLM 网关）。"""

    def __init__(self, url: str, model: str, api_key: str, client: httpx.AsyncClient) -> None:
        self._url = url.rstrip("/") + "/chat/completions"
        self._model = model
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._client = client
        self._cache: dict[str, Judgment] = {}

    @property
    def version(self) -> str:
        return f"{self._model}+{PROMPT_VERSION}"

    async def judge(
        self, question: str, answer: str, quotes: Sequence[str], key_points: Sequence[str]
    ) -> Judgment:
        user = render(question, answer, quotes, key_points)
        key = hashlib.sha256((self.version + "\n" + user).encode()).hexdigest()
        if key in self._cache:
            return self._cache[key]
        body = {
            "model": self._model,
            "temperature": 0,
            "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}],
        }
        try:
            resp = await self._client.post(self._url, json=body, headers=self._headers)
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
        except (httpx.HTTPError, KeyError, IndexError, ValueError) as e:
            raise JudgeError(f"评审模型调用失败：{type(e).__name__}") from e
        got = parse(str(content), len(key_points))
        self._cache[key] = got
        return got


def render(question: str, answer: str, quotes: Sequence[str], key_points: Sequence[str]) -> str:
    lines = [f"问题：{question}", "", "回答：", answer, "", "引文："]
    lines += [f"[{i}] {q}" for i, q in enumerate(quotes, 1)] or ["（无）"]
    lines += ["", "要点："]
    lines += [f"{i}. {k}" for i, k in enumerate(key_points, 1)] or ["（无）"]
    return "\n".join(lines)


def parse(content: str, n_points: int) -> Judgment:
    """模型输出不可信：取第一个 JSON 对象，字段缺失或类型不对按评审失败处理。"""
    start, end = content.find("{"), content.rfind("}")
    if start < 0 or end <= start:
        raise JudgeError("评审输出里没有 JSON")
    try:
        data: Any = json.loads(content[start : end + 1])
    except ValueError as e:
        raise JudgeError("评审输出不是合法的 JSON") from e
    faithful = data.get("faithful") if isinstance(data, dict) else None
    if not isinstance(faithful, bool):
        raise JudgeError("评审输出缺少 faithful")
    covered = [False] * n_points
    for kp in data.get("key_points") or []:
        if (
            isinstance(kp, dict)
            and isinstance(kp.get("index"), int)
            and isinstance(kp.get("covered"), bool)
        ):
            if 1 <= kp["index"] <= n_points:
                covered[kp["index"] - 1] = kp["covered"]
    return Judgment(faithful, tuple(covered))
