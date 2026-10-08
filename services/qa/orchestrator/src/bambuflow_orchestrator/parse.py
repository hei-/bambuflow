"""解析模型输出（orchestrator 详设 8.3、8.4）：每行一个 JSON 单元，读到一行就是一个待校验的陈述。"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from bambuflow_grounding import Calc, Claim, Input, Part, split_claim

MARKER = re.compile(r"\[S(\d+)\]")
# 丢弃行超过这个比例，或第一行就无法解析，整次输出视为不可用
MAX_BAD_RATIO = 0.3


@dataclass
class Parser:
    """逐行解析，流式读取时每读到一行就调用一次 feed。"""

    lines: int = 0
    bad: int = 0
    first_bad: bool = False

    @property
    def unusable(self) -> bool:
        return self.lines == 0 or self.first_bad or self.bad / self.lines > MAX_BAD_RATIO

    def feed(self, raw: str) -> list[Claim]:
        """返回这一行切出的陈述；空行、代码块标记和无法解析的行返回空列表（后者计入 bad）。"""
        line = raw.strip()
        if not line or line.startswith("```"):
            return []
        self.lines += 1
        obj = load_json_line(line)
        c = _claim(obj) if obj is not None else None
        if c is None:
            self.bad += 1
            self.first_bad = self.first_bad or self.lines == 1
            return []
        return split_claim(c)


def load_json_line(line: str) -> dict[str, Any] | None:
    """一行里的 JSON 对象；模型常在前后带说明文字或代码块标记，取第一个 { 到最后一个 } 再试一次。"""
    for candidate in (line, line[line.find("{") : line.rfind("}") + 1]):
        try:
            # 小数按十进制读入：计算句的取值要与引文里的数逐位相等，不能经过二进制浮点
            obj = json.loads(candidate, parse_float=Decimal)
        except ValueError:
            continue
        if isinstance(obj, dict):
            return obj
    return None


REF = re.compile(r"^\[?S?(\d+)\]?$")


def _ref(r: Any) -> int | None:
    """引用编号：要求是整数，模型常写成 "1"、"S1" 或 "[S1]"，按同一编号接受；其他写法视为不合法。"""
    if isinstance(r, int) and not isinstance(r, bool):
        return r
    if isinstance(r, str) and (m := REF.match(r.strip())):
        return int(m.group(1))
    return None


def _claim(obj: dict[str, Any]) -> Claim | None:
    text, raw_refs, quote = obj.get("text"), obj.get("refs", []), obj.get("quote", [])
    if isinstance(quote, str):
        quote = [quote]
    refs = [_ref(r) for r in raw_refs] if isinstance(raw_refs, list) else [None]
    if (
        not isinstance(text, str)
        or None in refs
        or not isinstance(quote, list)
        or not all(isinstance(q, str) for q in quote)
    ):
        return None
    core = MARKER.sub("", text).strip()
    if not core:
        return None
    return Claim(
        text=core,
        refs=tuple(r for r in refs if r is not None),
        quotes=tuple(quote),
        claim_type=str(obj.get("claim_type", "")),
        block_id=str(obj.get("block_id", "b1")),
        block_type=str(obj.get("block_type", "paragraph")),
        parts=_parts(obj.get("parts")),
        calc=_calc(obj.get("calc")),
    )


def _parts(raw: Any) -> tuple[Part, ...]:
    """综合句的原子事实；格式不对时当作没有 parts，这句按引述验（K8 详设 4.4）。"""
    if not isinstance(raw, list):
        return ()
    out = []
    for p in raw:
        ref = _ref(p.get("ref")) if isinstance(p, dict) else None
        if ref is None or not isinstance(p.get("text"), str) or not isinstance(p.get("quote"), str):
            return ()
        out.append(Part(MARKER.sub("", p["text"]).strip(), ref, p["quote"]))
    return tuple(out)


def _calc(raw: Any) -> Calc | None:
    """计算句的取数与算式；格式不对时为 None，这句校验不通过（不会被当作引述放行）。"""
    if not isinstance(raw, dict) or not isinstance(raw.get("inputs"), list):
        return None
    exprs = raw.get("exprs")
    if not isinstance(exprs, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in exprs.items()
    ):
        return None
    inputs = []
    for i in raw["inputs"]:
        if not isinstance(i, dict):
            return None
        ref, value = _ref(i.get("ref")), _decimal(i.get("value"))
        var, quote, unit = i.get("var"), i.get("quote"), i.get("unit", "")
        if (
            ref is None
            or value is None
            or not isinstance(var, str)
            or not isinstance(quote, str)
            or not isinstance(unit, str)
        ):
            return None
        inputs.append(Input(var, ref, quote, value, unit))
    return Calc(tuple(inputs), exprs)


def _decimal(v: Any) -> Decimal | None:
    if isinstance(v, bool) or not isinstance(v, int | Decimal | str):
        return None
    try:
        d = Decimal(v)
    except InvalidOperation:
        return None
    return d if d.is_finite() else None
