"""计算类陈述的取数核对与程序重算（K7 详设第 3 节）。

模型只给出“从哪里取数、怎么算”，结果由这里用十进制定点数算出、填进句子：
句子里的结果不可能与重算不一致，校验的重点变成输入是否取对。算式用自己的解析器执行，不用 eval。
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, DivisionByZero, InvalidOperation

CALCULATOR_VERSION = "calc@1"  # 写入 why，回放时用同一版本重算

_MULTIPLIERS = {
    "万亿": Decimal(10) ** 12,
    "亿": Decimal(10) ** 8,
    "万": Decimal(10) ** 4,
    "千": Decimal(1000),
}
_NUMBER = re.compile(r"(\d+(?:,\d{3})*(?:\.\d+)?)\s*(万亿|亿|万|千)?\s*(%)?")
_PLACEHOLDER = re.compile(r"\{([A-Za-z_]\w*)\}")
# 比较的结果按固定词表渲染，模型不能自己写“超过”“未超过”
_COMPARE_WORDS = {
    ">": ("超过", "未超过"),
    ">=": ("达到", "未达到"),
    "<": ("低于", "不低于"),
    "<=": ("不超过", "超过"),
    "=": ("等于", "不等于"),
}
_MAX_ROUND = 6


class CalcError(ValueError):
    """取数、算式或单位不合法：这句不通过。"""


@dataclass(frozen=True)
class Input:
    var: str
    ref: int
    quote: str
    value: Decimal
    # 原文中紧跟数字（及万、亿等量级字）的单位字样，如“美元”“元”“天”“%”；无单位为空
    unit: str


@dataclass(frozen=True)
class Calc:
    inputs: tuple[Input, ...]
    # 结果名 → 算式，结果名对应句子里的 {占位符}
    exprs: Mapping[str, str]


@dataclass(frozen=True)
class Number:
    value: Decimal
    after: str  # 数字（含量级字）之后的原文，用来核对单位


def numbers(text: str) -> list[Number]:
    """抽取文本里的数字：归一全角、千分位、万 / 亿 / 千与百分号。"""
    s = unicodedata.normalize("NFKC", text)
    out = []
    for m in _NUMBER.finditer(s):
        v = Decimal(m.group(1).replace(",", ""))
        if m.group(2):
            v *= _MULTIPLIERS[m.group(2)]
        after = "%" + s[m.end() :] if m.group(3) else s[m.end() :]
        out.append(Number(v, after.lstrip()))
    return out


def check_input(inp: Input) -> None:
    """取数核对：value 必须是引文里的某个数，unit 必须紧跟在那个数后面。"""
    for n in numbers(inp.quote):
        if n.value == inp.value and n.after.startswith(unicodedata.normalize("NFKC", inp.unit)):
            return
    raise CalcError(f"{inp.var} 的取值 {inp.value}{inp.unit} 不在引文中")


@dataclass(frozen=True)
class _Val:
    value: Decimal
    unit: str


class _Parser:
    """算式：+ − × ÷、括号、比较、sum / min / max / round，数字字面量无单位。"""

    _TOKEN = re.compile(r"\s*(>=|<=|==|\d+(?:\.\d+)?|[A-Za-z_]\w*|[-+*/×÷()<>=≥≤,])")

    def __init__(self, expr: str, env: Mapping[str, _Val]) -> None:
        self.tokens = self._tokenize(unicodedata.normalize("NFKC", expr))
        self.pos = 0
        self.env = env

    def _tokenize(self, s: str) -> list[str]:
        out, i = [], 0
        while i < len(s):
            if s[i].isspace():
                i += 1
                continue
            m = self._TOKEN.match(s, i)
            if not m:
                raise CalcError(f"算式里有不支持的字符：{s[i]}")
            tok = m.group(1)
            out.append({"×": "*", "÷": "/", "≥": ">=", "≤": "<=", "==": "="}.get(tok, tok))
            i = m.end()
        return out

    def _peek(self) -> str | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _take(self, want: str | None = None) -> str:
        tok = self._peek()
        if tok is None or (want is not None and tok != want):
            raise CalcError(f"算式在第 {self.pos + 1} 个记号处不完整")
        self.pos += 1
        return tok

    def parse(self) -> _Val | tuple[str, bool]:
        left = self._add()
        op = self._peek()
        if op in _COMPARE_WORDS:
            self._take()
            right = self._add()
            _same_unit(left, right, op)
            result: _Val | tuple[str, bool] = (op, _compare(op, left.value, right.value))
        else:
            result = left
        if self._peek() is not None:
            raise CalcError("算式末尾有多余的内容")
        return result

    def _add(self) -> _Val:
        v = self._mul()
        while (op := self._peek()) in ("+", "-"):
            self._take()
            r = self._mul()
            _same_unit(v, r, op)
            v = _Val(v.value + r.value if op == "+" else v.value - r.value, v.unit)
        return v

    def _mul(self) -> _Val:
        v = self._unary()
        while (op := self._peek()) in ("*", "/"):
            self._take()
            r = self._unary()
            if v.unit and r.unit and not (op == "/" and v.unit == r.unit):
                raise CalcError(f"不支持 {v.unit} {op} {r.unit}")
            try:
                value = v.value * r.value if op == "*" else v.value / r.value
            except (DivisionByZero, InvalidOperation) as e:
                raise CalcError("除数为 0") from e
            # 同单位相除得到比值（无单位），否则保留有单位的一侧
            unit = "" if op == "/" and v.unit == r.unit else (v.unit or r.unit)
            v = _Val(value, unit)
        return v

    def _unary(self) -> _Val:
        if self._peek() == "-":
            self._take()
            v = self._unary()
            return _Val(-v.value, v.unit)
        return self._atom()

    def _atom(self) -> _Val:
        tok = self._take()
        if tok == "(":
            v = self._add()
            self._take(")")
            return v
        if re.fullmatch(r"\d+(?:\.\d+)?", tok):
            return _Val(Decimal(tok), "")
        if tok in ("sum", "min", "max", "round"):
            self._take("(")
            args = [self._add()]
            while self._peek() == ",":
                self._take()
                args.append(self._add())
            self._take(")")
            return _call(tok, args)
        if tok in self.env:
            return self.env[tok]
        raise CalcError(f"算式引用了未定义的 {tok}")


def _same_unit(a: _Val, b: _Val, op: str) -> None:
    if a.unit != b.unit:
        raise CalcError(f"单位不同不能做 {op}：{a.unit or '无单位'}、{b.unit or '无单位'}")


def _compare(op: str, a: Decimal, b: Decimal) -> bool:
    return {">": a > b, ">=": a >= b, "<": a < b, "<=": a <= b, "=": a == b}[op]


def _call(name: str, args: Sequence[_Val]) -> _Val:
    if name == "round":
        if len(args) != 2 or args[1].unit or args[1].value != args[1].value.to_integral():
            raise CalcError("round 需要 (值, 小数位数)")
        places = int(args[1].value)
        if not 0 <= places <= _MAX_ROUND:
            raise CalcError(f"round 的小数位数需要 0–{_MAX_ROUND}")
        return _Val(args[0].value.quantize(Decimal(1).scaleb(-places)), args[0].unit)
    for a in args[1:]:
        _same_unit(args[0], a, name)
    values = [a.value for a in args]
    value = (
        sum(values, Decimal(0))
        if name == "sum"
        else (min(values) if name == "min" else max(values))
    )
    return _Val(value, args[0].unit)


def _places(inputs: Sequence[Input], expr: str) -> int:
    """按输入的最大精度格式化；有除法时至少保留 2 位，避免比值被截成 0。"""
    p = max((_decimals(i.value) for i in inputs), default=0)
    return max(p, 2) if "/" in expr or "÷" in expr else p


def _decimals(v: Decimal) -> int:
    exp = v.normalize().as_tuple().exponent
    return max(0, -exp) if isinstance(exp, int) else 0


def render(template: str, calc: Calc) -> tuple[str, dict[str, str]]:
    """校验取数与算式，算出各结果并填入句子。

    返回（填好的句子，结果名 → 结果）；任何一步不合法抛 CalcError。"""
    if not calc.inputs or not calc.exprs:
        raise CalcError("计算句缺少输入或算式")
    env: dict[str, _Val] = {}
    for inp in calc.inputs:
        if inp.var in env:
            raise CalcError(f"输入 {inp.var} 重复")
        check_input(inp)
        env[inp.var] = _Val(inp.value, unicodedata.normalize("NFKC", inp.unit))
    names = _PLACEHOLDER.findall(template)
    if not names or set(names) - calc.exprs.keys():
        raise CalcError("句子里的占位符与算式对不上")
    _no_mental_math(_PLACEHOLDER.sub("", template), calc.inputs)
    filled: dict[str, str] = {}
    for name in set(names):
        expr = calc.exprs[name]
        result = _Parser(expr, env).parse()
        if isinstance(result, tuple):
            op, ok = result
            filled[name] = _COMPARE_WORDS[op][0 if ok else 1]
        else:
            places = _places(calc.inputs, expr)
            v = result.value.quantize(Decimal(1).scaleb(-places))
            filled[name] = f"{v:f}" + ("%" if result.unit == "%" else "")
    return _PLACEHOLDER.sub(lambda m: filled[m.group(1)], template), filled


def _no_mental_math(text: str, inputs: Sequence[Input]) -> None:
    """占位符以外的数字只能是引文里出现过的数：模型不能把心算结果直接写进句子。"""
    quoted = {n.value for i in inputs for n in numbers(i.quote)}
    for n in numbers(text):
        if n.value not in quoted:
            raise CalcError(f"句子里的 {n.value} 不是引文中的数，算出来的数必须用占位符")
