"""三道检查（orchestrator 详设 9.2）：引文核对 → 蕴含判定 → 权限复核。

任何一道不过，这句就不出现在回答里。另有一条规则：句子通过检查的依据不能只有带可疑指令标记
（injection_flag）的证据，否则丢句（详设 9.2）。

矛盾检查（K8 详设 4.2 第 4 步、技术设计文档 7.8）：综合句的任一引文与整句矛盾时丢句；
回答之外的证据与已通过的陈述矛盾时，作为未解决的证据冲突交给调用方（降低置信度并提示核实原文）。

NLI 或权限复核不可用时，它们的实现抛 GroundingUnavailableError，原样传给调用方
按 fail-closed 处理（返回 evidence_only），绝不把未经校验的句子当作通过。
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from bambuflow_grounding.calc import Calc, CalcError, render

SUPPORT_THRESHOLD = 0.5
# 综合句的“合并推出”与计算句的措辞校验比单条严一档（K7/K8 详设第 9 节待校准的建议值）
COMBINED_THRESHOLD = 0.6
# 矛盾概率超过它算冲突（K8 详设 4.2 第 4 步）
CONTRADICTION_THRESHOLD = 0.5
# 冲突检查一次最多判多少对（已通过的陈述 × 回答没有引用的证据），超出的不查：控制在一次 NLI 批次内
MAX_CONFLICT_PAIRS = 64
MAX_PARTS = 3  # 综合句最多由几条原子事实组成
CHECK_ACCESS_BATCH = 50  # CheckAccess 单次上限（retrieval 契约）


@dataclass(frozen=True)
class Evidence:
    """上下文里编号为 [S<ref_id>] 的一条证据。"""

    ref_id: int
    chunk_id: str
    text: str
    # 入库时检测到可疑指令：可以与其他证据一起支撑句子，不能是唯一依据
    suspicious: bool = False
    # 所属文档：冲突检查只看回答没有引用的文档
    doc_id: str = ""


@dataclass(frozen=True)
class Part:
    """综合句的一条原子事实：主语写全、可以单独成立，只有一个来源（K8 详设 4.1）。"""

    text: str
    ref: int
    quote: str


@dataclass(frozen=True)
class Claim:
    """一句待校验的陈述。text 不含 [S#] 标记；quotes 与 refs 一一对应，是模型给出的原文片段。
    综合句另带 parts；计算句另带 calc，text 里的 {占位符} 由程序算出后填入。"""

    text: str
    refs: tuple[int, ...]
    quotes: tuple[str, ...]
    claim_type: str
    block_id: str
    block_type: str
    parts: tuple[Part, ...] = ()
    calc: Calc | None = None


@dataclass(frozen=True)
class Verdict:
    claim: Claim
    passed: bool
    # 通过全部检查的引用及其引文；未通过时为空
    refs: tuple[int, ...]
    quotes: Mapping[int, str]
    support: float
    # passed / no_refs / unsupported_claim_type / quote_mismatch / low_support / access_denied /
    # calc_invalid / too_many_parts / suspicious_only / contradiction
    reason: str
    # 计算句填好结果的句子（不含 [S#]）与各结果；其他类型为空
    rendered: str = ""
    results: Mapping[str, str] = field(default_factory=dict)
    # 因矛盾丢句时，与整句矛盾的证据编号（调用方据此提示“来源之间有冲突”）
    conflicts: tuple[int, ...] = ()

    @property
    def text(self) -> str:
        """带 [S#] 标记的句子，只标通过检查的引用。"""
        return (self.rendered or self.claim.text) + "".join(f"[S{r}]" for r in self.refs)


@dataclass(frozen=True)
class GroundingResult:
    verdicts: list[Verdict]
    nli_model: str
    support_threshold: float = field(default=SUPPORT_THRESHOLD)


class GroundingUnavailableError(Exception):
    pass


class NLI(Protocol):
    async def support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        """对每对（前提=引文，假设=句子）给出支持度 0–1，并返回模型版本。

        不可用时抛 GroundingUnavailableError。
        """
        ...

    async def contradiction(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        """对每对（前提，假设）给出矛盾概率 0–1，并返回模型版本。

        不可用时抛 GroundingUnavailableError。
        """
        ...


class AccessChecker(Protocol):
    async def allowed(self, chunk_ids: Sequence[str]) -> dict[str, bool]:
        """复核调用者此刻是否仍可见这些块；不在结果里的按不可见处理。

        不可用时抛 GroundingUnavailableError。
        """
        ...


def normalize(s: str) -> str:
    """引文核对忽略空白与全半角差异。"""
    return "".join(unicodedata.normalize("NFKC", s).split())


def _reject(
    claim: Claim, reason: str, support: float = 0.0, conflicts: tuple[int, ...] = ()
) -> Verdict:
    return Verdict(claim, False, (), {}, support, reason, conflicts=conflicts)


class Grounder:
    """一个实例只用于一次请求：权限复核的结论在实例内复用，不能跨请求、跨用户共享。"""

    def __init__(
        self, nli: NLI, access: AccessChecker, threshold: float = SUPPORT_THRESHOLD
    ) -> None:
        self._nli = nli
        self._access = access
        self._threshold = threshold
        self._allowed: dict[str, bool] = {}
        self._model = ""

    def use_threshold(self, threshold: float) -> None:
        """换用另一个支持度门槛（A/B 实验组，orchestrator 详设 12.3）。

        只接受 0.3–0.9 的合理范围（12.2）。"""
        if not 0.3 <= threshold <= 0.9:
            raise ValueError(f"支持度门槛越界: {threshold}")
        self._threshold = threshold

    async def verify(
        self, claims: Sequence[Claim], evidence: Mapping[int, Evidence]
    ) -> GroundingResult:
        """按陈述类型选校验方法（K7/K8 详设第 2 节）；引文核对与权限复核对所有类型都一样。

        返回的结论按陈述顺序排列。综合句不通过时，已逐条验过的原子事实作为独立的引述句返回，
        所以结论可能比陈述多（K8 详设 4.3）。"""
        self._model = ""
        out: list[list[Verdict]] = [[] for _ in claims]
        quotes: dict[int, list[tuple[int, str]]] = {}
        calcs: dict[int, tuple[tuple[str, dict[str, str]], str, str]] = {}
        synths: dict[int, list[Part]] = {}
        # 第 1 道：引文必须是所引证据原文的子串；计算句同时做取数核对与重算
        for i, c in enumerate(claims):
            if not c.refs:
                out[i] = [_reject(c, "no_refs")]
            elif c.claim_type == "synthesis" and len(set(c.refs)) >= 2 and c.parts:
                if len(c.parts) > MAX_PARTS:
                    out[i] = [_reject(c, "too_many_parts")]
                    continue
                valid = [
                    p for p in c.parts if p.ref in c.refs and _quoted(evidence, p.ref, p.quote)
                ]
                if valid:
                    synths[i] = valid
                else:
                    out[i] = [_reject(c, "quote_mismatch")]
            elif c.claim_type == "calculation":
                calc = c.calc
                if calc is None or not all(
                    i.ref in c.refs and _quoted(evidence, i.ref, i.quote) for i in calc.inputs
                ):
                    out[i] = [_reject(c, "calc_invalid" if calc is None else "quote_mismatch")]
                    continue
                try:
                    calc_out = render(c.text, calc)
                except CalcError:
                    out[i] = [_reject(c, "calc_invalid")]
                    continue
                # 措辞校验：数字换成“若干”后，拼接的引文能否支撑句子里的描述（K7 详设 3.2 第 6 步）
                premise = "；".join(inp.quote for inp in calc.inputs)
                calcs[i] = (calc_out, premise, _PLACEHOLDER.sub("若干", c.text))
            elif c.claim_type in ("quote", "list_item", "synthesis"):
                # 只有一个引用或没有 parts 的综合句按引述验（K8 详设 4.4）
                valid_q = [
                    (ref, quote)
                    for ref, quote in zip(c.refs, c.quotes, strict=False)
                    if _quoted(evidence, ref, quote)
                ]
                if len(c.quotes) != len(c.refs) or not valid_q:
                    out[i] = [_reject(c, "quote_mismatch")]
                else:
                    quotes[i] = valid_q
            else:
                out[i] = [_reject(c, "unsupported_claim_type")]

        # 第 2 道：引文是否支撑句子。同一批送 NLI
        batch = _Batch()
        for i, valid_q in quotes.items():
            for ref, quote in valid_q:
                batch.add(("q", i, ref, quote), quote, claims[i].text)
        for i, (_, premise, hypothesis) in calcs.items():
            batch.add(("c", i), premise, hypothesis)
        for i, parts in synths.items():
            for p in parts:
                batch.add(("p", i, p), p.quote, p.text)
        scores = await self._score(batch)

        kept: list[_Kept] = []
        for i, valid_q in quotes.items():
            scored = [(ref, q, scores[("q", i, ref, q)]) for ref, q in valid_q]
            best = max(sc for _, _, sc in scored)
            good = [(ref, q) for ref, q, sc in scored if sc >= self._threshold]
            if good:
                kept.append((i, claims[i], good, best, None))
            else:
                out[i] = [_reject(claims[i], "low_support", best)]
        for i, (calc_out, _, _) in calcs.items():
            sc = scores[("c", i)]
            calc = claims[i].calc
            if sc >= COMBINED_THRESHOLD and calc is not None:
                refs = list(dict.fromkeys((inp.ref, inp.quote) for inp in calc.inputs))
                kept.append((i, claims[i], _first_quote_per_ref(refs), sc, calc_out))
            else:
                out[i] = [_reject(claims[i], "low_support", sc)]
        kept += await self._synthesis(claims, synths, scores, out)

        # 第 3 道：调用者此刻是否仍可见所引的块。每个块在本次请求内只复核一次（详设 9.2），
        # 流式时逐行调用 verify，后面的句子复用前面的结论
        await self._check_access(
            {evidence[ref].chunk_id for _, _, good, _, _ in kept for ref, _ in good}
        )
        for i, claim, good, best, computed in kept:
            visible = [(ref, q) for ref, q in good if self._allowed[evidence[ref].chunk_id]]
            # 计算句的输入少了任何一条，结果就没有依据：要求全部引用都仍可见
            if visible and all(evidence[ref].suspicious for ref, _ in visible):
                v = _reject(claim, "suspicious_only", best)
            elif visible and (computed is None or len(visible) == len(good)):
                rendered, results = computed or ("", {})
                v = Verdict(
                    claim,
                    True,
                    tuple(r for r, _ in visible),
                    dict(visible),
                    best,
                    "passed",
                    rendered,
                    results,
                )
            else:
                v = _reject(claim, "access_denied", best)
            out[i].append(v)

        if any(not vs for vs in out):
            raise RuntimeError("有陈述没有得到校验结论")
        return GroundingResult([v for vs in out for v in vs], self._model, self._threshold)

    async def _synthesis(
        self,
        claims: Sequence[Claim],
        synths: Mapping[int, list[Part]],
        scores: Mapping[object, float],
        out: list[list[Verdict]],
    ) -> list[_Kept]:
        """综合句（K8 详设 4.2、4.3）：原子事实逐条验，
        全部通过才验“合起来能否推出整句”与各条是否必要；
        不通过时把已通过的原子事实作为独立的引述句返回，而不是整句丢掉。"""
        kept: list[_Kept] = []
        passed: dict[int, list[Part]] = {}
        merge = _Batch()
        for i, parts in synths.items():
            ok = [p for p in parts if scores[("p", i, p)] >= self._threshold]
            passed[i] = ok
            if len(ok) == len(claims[i].parts) and len(ok) >= 2:
                merge.add(("m", i), _join(ok), claims[i].text)
                for j, p in enumerate(ok):
                    merge.add(("n", i, p), _join(ok[:j] + ok[j + 1 :]), claims[i].text)
        merged = await self._score(merge)
        # 第 4 步：任一引文与整句矛盾则丢句。只查合并推出已通过的句子
        contra = _Batch()
        for i, ok in passed.items():
            if ("m", i) in merged and merged[("m", i)] >= COMBINED_THRESHOLD:
                for p in ok:
                    contra.add(("x", i, p), p.quote, claims[i].text)
        against = await self._contradict(contra)
        for i, ok in passed.items():
            c = claims[i]
            if ("m", i) in merged and merged[("m", i)] >= COMBINED_THRESHOLD:
                conflicts = sorted(
                    {p.ref for p in ok if against[("x", i, p)] > CONTRADICTION_THRESHOLD}
                )
                if conflicts:
                    out[i] = [_reject(c, "contradiction", merged[("m", i)], tuple(conflicts))]
                    continue
                # 去掉某条后仍能推出整句：这条多余，只删它的引用，不丢句（K8 详设 4.2 第 3 步）
                needed = [p for p in ok if merged[("n", i, p)] < COMBINED_THRESHOLD] or ok
                refs = _first_quote_per_ref([(p.ref, p.quote) for p in needed])
                kept.append((i, c, refs, merged[("m", i)], None))
                continue
            if not ok:
                best = max(scores[("p", i, p)] for p in synths[i])
                out[i] = [_reject(c, "low_support", best)]
                continue
            for p in ok:
                part = Claim(p.text, (p.ref,), (p.quote,), "quote", c.block_id, c.block_type)
                kept.append((i, part, [(p.ref, p.quote)], scores[("p", i, p)], None))
        return kept

    async def conflicts(
        self,
        passed: Sequence[Verdict],
        evidence: Mapping[int, Evidence],
        noted: Sequence[int] = (),
    ) -> tuple[int, ...]:
        """未解决的证据冲突（技术设计文档 7.8 的 k）：
        回答没有引用的文档里，有证据与已通过的陈述矛盾。
        回答引用了的文档不算——按提示词规则，引用两者时已说明新旧差异。
        带可疑指令标记的证据不参与，免得注入的内容借“冲突提示”被展示出来。

        noted 是校验时已发现的冲突（综合句因矛盾丢句）。
        返回两者中调用者此刻仍可见的证据编号，升序。"""
        cited = {evidence[r].doc_id for v in passed for r in v.refs}
        others = [e for e in evidence.values() if e.doc_id not in cited and not e.suspicious]
        batch = _Batch()
        for v in passed:
            for e in others:
                if len(batch.pairs) < MAX_CONFLICT_PAIRS:
                    batch.add(e.ref_id, e.text, v.rendered or v.claim.text)
        against = await self._contradict(batch)
        # 键是证据编号：同一条证据与多句陈述各判一次，_contradict 已取最大值
        found = {
            ref
            for ref, sc in against.items()
            if isinstance(ref, int) and sc > CONTRADICTION_THRESHOLD
        }
        found |= {r for r in noted if r in evidence}
        await self._check_access({evidence[r].chunk_id for r in found})
        return tuple(sorted(r for r in found if self._allowed[evidence[r].chunk_id]))

    async def _score(self, batch: _Batch) -> dict[object, float]:
        if not batch.pairs:
            return {}
        scores, self._model = await self._nli.support(batch.pairs)
        if len(scores) != len(batch.pairs):
            raise GroundingUnavailableError("NLI 返回的条数与请求不符")
        return dict(zip(batch.keys, scores, strict=True))

    async def _contradict(self, batch: _Batch) -> dict[object, float]:
        """矛盾概率。同一对证据与陈述可能出现多次（键相同），取最大值。"""
        if not batch.pairs:
            return {}
        scores, _ = await self._nli.contradiction(batch.pairs)
        if len(scores) != len(batch.pairs):
            raise GroundingUnavailableError("NLI 返回的条数与请求不符")
        out: dict[object, float] = {}
        for key, sc in zip(batch.keys, scores, strict=True):
            out[key] = max(out.get(key, 0.0), sc)
        return out

    async def _check_access(self, chunk_ids: set[str]) -> None:
        todo = sorted(chunk_ids - self._allowed.keys())
        for start in range(0, len(todo), CHECK_ACCESS_BATCH):
            batch = todo[start : start + CHECK_ACCESS_BATCH]
            got = await self._access.allowed(batch)
            self._allowed |= {c: got.get(c, False) for c in batch}


_PLACEHOLDER = re.compile(r"\{[A-Za-z_]\w*\}")
# 通过前两道、待复核权限的结论：(陈述下标, 陈述, 引用与引文, 支持度, 计算句填好的句子与结果)
_Kept = tuple[int, Claim, list[tuple[int, str]], float, tuple[str, dict[str, str]] | None]


@dataclass
class _Batch:
    """一批 NLI 判定：pairs 是（前提，假设），keys 是调用方用来取回分数的键。"""

    pairs: list[tuple[str, str]] = field(default_factory=list)
    keys: list[object] = field(default_factory=list)

    def add(self, key: object, premise: str, hypothesis: str) -> None:
        self.keys.append(key)
        self.pairs.append((premise, hypothesis))


def _quoted(evidence: Mapping[int, Evidence], ref: int, quote: str) -> bool:
    return (
        ref in evidence
        and bool(normalize(quote))
        and normalize(quote) in normalize(evidence[ref].text)
    )


def _join(parts: Sequence[Part]) -> str:
    return "。".join(p.text.rstrip("。") for p in parts) + "。"


def _first_quote_per_ref(pairs: Sequence[tuple[int, str]]) -> list[tuple[int, str]]:
    seen: dict[int, str] = {}
    for ref, quote in pairs:
        seen.setdefault(ref, quote)
    return list(seen.items())
