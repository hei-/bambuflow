"""受控词表（governance 详设 6.1）：按名称或同义词把说法映射成编码，停用的编码换成替代编码，
按上级编码展开下级。词表快照来自 governance 的 ConfigService。"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from bambuflow.config.v1 import config_pb2


@dataclass(frozen=True)
class Term:
    code: str
    names: tuple[str, ...]  # 名称与同义词
    replaced_by: str = ""
    parent: str = ""


class Vocabulary:
    """一套词表。"""

    def __init__(self, terms: Sequence[Term]) -> None:
        self._terms = {t.code: t for t in terms}

    def resolve(self, code: str) -> str:
        """停用的编码沿替代链换成在用的编码。"""
        seen: set[str] = set()
        while (t := self._terms.get(code)) and t.replaced_by and code not in seen:
            seen.add(code)
            code = t.replaced_by
        return code

    def name(self, code: str) -> str:
        t = self._terms.get(code)
        return t.names[0] if t else code

    def exact(self, text: str) -> str:
        """说法与某个名称或同义词完全相同。"""
        key = _norm(text)
        for t in self._terms.values():
            if key in (_norm(n) for n in t.names):
                return self.resolve(t.code)
        return ""

    def contained(self, text: str) -> list[str]:
        """文本里出现的词条编码，按命中的名称从长到短；被更长的命中覆盖的短名称不算。"""
        key = _norm(text)
        hits = sorted(
            (
                (len(n), t.code, n)
                for t in self._terms.values()
                for n in map(_norm, t.names)
                if n and n in key
            ),
            reverse=True,
        )
        out: list[str] = []
        covered: list[str] = []
        for _, code, n in hits:
            if any(n in c for c in covered):
                continue
            covered.append(n)
            resolved = self.resolve(code)
            if resolved not in out:
                out.append(resolved)
        return out

    def suffix(self, text: str) -> str:
        """文本以某个名称或同义词结尾时取最长的那个。"""
        key = _norm(text)
        best = max(
            (
                (len(n), t.code)
                for t in self._terms.values()
                for n in map(_norm, t.names)
                if n and key.endswith(n)
            ),
            default=None,
        )
        return self.resolve(best[1]) if best else ""

    def without_names(self, text: str) -> str:
        """去掉文本里出现的本词表说法（先去长的）。用于避免一个说法同时被另一套词表命中，
        如“信贷部”里的“信贷”不能再算作主题。"""
        out = "".join(text.split())
        names = sorted(
            {n for t in self._terms.values() for n in map(_norm, t.names) if n},
            key=len,
            reverse=True,
        )
        for n in names:
            out = re.sub(re.escape(n), "，", out, flags=re.IGNORECASE)
        return out

    def canonical(self, text: str) -> str:
        """把文本里的同义词换成词条的规范名称（停用的编码换成替代编码的名称）。
        从左到右每处取最长的说法，所以“住房贷款”整体算一个词条，不会把其中的“贷款”换掉。
        用于查询的同义扩展（orchestrator 详设 5.1）：没有可换的说法时原样返回。"""
        names = {
            n: self.name(self.resolve(t.code)) for t in self._terms.values() for n in t.names if n
        }
        longest = max((len(n) for n in names), default=0)
        out, i = [], 0
        while i < len(text):
            hit = ""
            for k in range(min(longest, len(text) - i), 0, -1):
                if text[i : i + k] in names:
                    hit = text[i : i + k]
                    break
            if hit:
                out.append(names[hit])
                i += len(hit)
            else:
                out.append(text[i])
                i += 1
        return "".join(out)

    def with_descendants(self, code: str) -> list[str]:
        """编码本身加上全部下级（按上级查询时包含下级）。"""
        out = [code]
        for c in out:
            out.extend(t.code for t in self._terms.values() if t.parent == c and t.code not in out)
        return out


@dataclass(frozen=True)
class Vocabularies:
    version: str
    doc_type: Vocabulary
    dept: Vocabulary
    topic: Vocabulary

    @staticmethod
    def from_snapshot(snap: config_pb2.GetSnapshotResponse) -> Vocabularies:
        by_name: dict[str, list[Term]] = {}
        for v in snap.vocabularies:
            by_name[v.name] = [
                Term(t.code, (t.name, *t.synonyms), t.replaced_by, t.parent) for t in v.terms
            ]
        missing = {"doc_type", "dept", "topic"} - by_name.keys()
        if missing:
            raise ValueError(f"词表快照缺少 {sorted(missing)}")
        return Vocabularies(
            snap.snapshot_version,
            Vocabulary(by_name["doc_type"]),
            Vocabulary(by_name["dept"]),
            Vocabulary(by_name["topic"]),
        )


def _norm(s: str) -> str:
    return "".join(s.split()).lower()
