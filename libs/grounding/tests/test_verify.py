import asyncio
from collections.abc import Callable, Sequence
from decimal import Decimal

import pytest

from bambuflow_grounding import (
    Calc,
    Claim,
    Evidence,
    Grounder,
    GroundingResult,
    GroundingUnavailableError,
    Input,
    Part,
    split_claim,
)

LOAN = "第五条 个人消费贷款单户额度原则上不超过 30 万元。第六条 期限最长 5 年。"
EVIDENCE = {
    1: Evidence(1, "wiki:loan#a", LOAN),
    2: Evidence(2, "wiki:handbook#b", "员工请假须提前一天在系统中提交申请。"),
}
QUOTE = "个人消费贷款单户额度原则上不超过 30 万元。"


def claim(text: str, refs: tuple[int, ...], quotes: tuple[str, ...], kind: str = "quote") -> Claim:
    return Claim(text, refs, quotes, kind, "b1", "paragraph")


class OverlapNLI:
    """支持度 = 假设句里有多少字出现在引文中；记录收到的请求。"""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.pairs: list[tuple[str, str]] = []

    async def support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        if self.fail:
            raise GroundingUnavailableError("down")
        self.pairs.extend(pairs)
        return [sum(ch in p for ch in h) / len(h) for p, h in pairs], "nli@test"

    async def contradiction(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        if self.fail:
            raise GroundingUnavailableError("down")
        return [0.0 for _ in pairs], "nli@test"


class Access:
    def __init__(self, denied: frozenset[str] = frozenset(), fail: bool = False) -> None:
        self.denied = denied
        self.fail = fail
        self.calls: list[list[str]] = []

    async def allowed(self, chunk_ids: Sequence[str]) -> dict[str, bool]:
        if self.fail:
            raise GroundingUnavailableError("down")
        self.calls.append(list(chunk_ids))
        return {c: c not in self.denied for c in chunk_ids}


def run(
    claims: list[Claim], nli: OverlapNLI | None = None, access: Access | None = None
) -> GroundingResult:
    g = Grounder(nli or OverlapNLI(), access or Access())
    return asyncio.run(g.verify(claims, EVIDENCE))


def test_supported_claim_passes_with_markers_for_kept_refs() -> None:
    r = run([claim("个人消费贷款单户额度不超过 30 万元。", (1,), (QUOTE,))])
    v = r.verdicts[0]
    assert v.passed and v.reason == "passed" and v.refs == (1,)
    assert v.text == "个人消费贷款单户额度不超过 30 万元。[S1]"
    assert r.nli_model == "nli@test" and r.support_threshold == 0.5


def test_suspicious_evidence_cannot_be_the_only_basis() -> None:
    leave = "员工请假须提前一天在系统中提交申请。"
    evidence = {
        1: Evidence(1, "wiki:loan#a", LOAN, suspicious=True),
        2: Evidence(2, "wiki:handbook#b", leave),
        3: Evidence(3, "wiki:notice#c", leave, suspicious=True),
    }
    claims = [
        claim("个人消费贷款单户额度不超过 30 万元。", (1,), (QUOTE,)),
        # 另有一条正常证据支撑：照常通过，可疑的那条也保留为引用
        claim("员工请假须提前一天在系统中提交申请。", (2, 3), (leave, leave)),
        claim("员工请假须提前一天在系统中提交申请。", (3,), (leave,)),
    ]
    g = Grounder(OverlapNLI(), Access())
    r = asyncio.run(g.verify(claims, evidence))
    assert [v.reason for v in r.verdicts] == ["suspicious_only", "passed", "suspicious_only"]
    assert r.verdicts[1].refs == (2, 3)


def test_quote_must_come_from_cited_evidence() -> None:
    cases = [
        claim("贷款期限最长为三十年。", (1,), ("贷款期限最长为三十年。",)),  # 原文里没有
        claim("员工请假须提前一天。", (1,), ("员工请假须提前一天",)),  # 在另一条证据里
        claim("额度不超过 30 万元。", (9,), (QUOTE,)),  # 证据编号不存在
        claim("额度不超过 30 万元。", (1, 2), (QUOTE,)),  # 引文与引用数量不符
    ]
    assert [v.reason for v in run(cases).verdicts] == ["quote_mismatch"] * 4


def test_quote_check_ignores_whitespace_and_width() -> None:
    # 全角数字、多余空格不算篡改引文
    v = run([claim("额度不超过 30 万元。", (1,), ("单户额度原则上不超过 ３０ 万元",))]).verdicts[0]
    assert v.passed


def test_unsupported_sentence_is_dropped() -> None:
    v = run([claim("所有客户均可享受三年免息优惠。", (1,), (QUOTE,))]).verdicts[0]
    assert not v.passed and v.reason == "low_support" and v.refs == () and v.support < 0.5


def test_only_supporting_refs_are_kept() -> None:
    v = run(
        [claim("单户额度不超过 30 万元。", (1, 2), (QUOTE, "员工请假须提前一天在系统中提交申请。"))]
    ).verdicts[0]
    assert v.passed and v.refs == (1,) and v.text.endswith("[S1]")


def test_claims_without_refs_or_unverifiable_types_are_dropped() -> None:
    vs = run(
        [
            claim("以上内容仅供参考。", (), (), "none"),
            # 计算句没有 calc：模型心算的结果不能当作引述放行
            claim("合计 35 万元。", (1,), (QUOTE,), "calculation"),
            # 只有一个引用的综合句按引述验（K8 详设 4.4）
            claim("额度与期限都有上限。", (1,), (QUOTE,), "synthesis"),
            claim("额度原则上不超过 30 万元。", (1,), (QUOTE,), "guess"),
        ]
    ).verdicts
    assert [v.reason for v in vs] == [
        "no_refs",
        "calc_invalid",
        "low_support",
        "unsupported_claim_type",
    ]


def test_revoked_access_voids_the_citation() -> None:
    access = Access(denied=frozenset({"wiki:loan#a"}))
    vs = run(
        [
            claim("单户额度不超过 30 万元。", (1,), (QUOTE,)),
            claim("期限最长 5 年。", (1,), ("期限最长 5 年。",)),
            claim("请假须提前一天提交申请。", (2,), ("员工请假须提前一天在系统中提交申请。",)),
        ],
        access=access,
    ).verdicts
    assert [v.reason for v in vs] == ["access_denied", "access_denied", "passed"]
    # 同一块只复核一次
    assert access.calls == [["wiki:handbook#b", "wiki:loan#a"]]


def test_access_checked_once_per_request_across_calls() -> None:
    """流式时逐行调用 verify：同一块在一次请求内只复核一次，被撤权的结论同样复用。"""
    access = Access(denied=frozenset({"wiki:loan#a"}))
    g = Grounder(OverlapNLI(), access)
    first = asyncio.run(g.verify([claim("单户额度不超过 30 万元。", (1,), (QUOTE,))], EVIDENCE))
    second = asyncio.run(
        g.verify(
            [
                claim("期限最长 5 年。", (1,), ("期限最长 5 年。",)),
                claim("请假须提前一天提交申请。", (2,), ("员工请假须提前一天在系统中提交申请。",)),
            ],
            EVIDENCE,
        )
    )
    assert [v.reason for v in first.verdicts + second.verdicts] == [
        "access_denied",
        "access_denied",
        "passed",
    ]
    assert access.calls == [["wiki:loan#a"], ["wiki:handbook#b"]]


def test_dependencies_unavailable_fail_closed() -> None:
    c = [claim("单户额度不超过 30 万元。", (1,), (QUOTE,))]
    with pytest.raises(GroundingUnavailableError):
        run(c, nli=OverlapNLI(fail=True))
    with pytest.raises(GroundingUnavailableError):
        run(c, access=Access(fail=True))


def test_nli_not_called_when_nothing_to_check() -> None:
    nli, access = OverlapNLI(), Access()
    r = run([claim("贷款期限最长为三十年。", (1,), ("贷款期限最长为三十年。",))], nli, access)
    assert nli.pairs == [] and access.calls == [] and not r.verdicts[0].passed


def test_split_multi_sentence_unit() -> None:
    c = claim("第五条规定额度上限。第六条规定期限上限；均需审批", (1,), (QUOTE,))
    parts = split_claim(c)
    assert [p.text for p in parts] == ["第五条规定额度上限。", "第六条规定期限上限；", "均需审批"]
    assert all(p.refs == (1,) and p.quotes == (QUOTE,) for p in parts)
    assert split_claim(claim("只有一句。", (1,), (QUOTE,))) == [claim("只有一句。", (1,), (QUOTE,))]


# ---- 计算句（K7）与综合句（K8） ----

FX = {
    3: Evidence(3, "fx:rule#a", "单笔金额 3 万美元。"),
    4: Evidence(4, "fx:rule#b", "第二笔 5 万美元。"),
    5: Evidence(5, "fx:rule#c", "个人单日累计不超过等值 5 万美元。"),
    6: Evidence(6, "prod:a#1", "A 产品归口零售金融部管理。"),
    7: Evidence(7, "prod:b#1", "B 产品单笔审批上限 50 万元。"),
}


class ScriptedNLI:
    """按（前提，假设）给分：score(p, h) 返回支持度。"""

    def __init__(
        self,
        score: Callable[[str, str], float],
        against: Callable[[str, str], float] = lambda p, h: 0.0,
    ) -> None:
        self.score, self.against = score, against
        self.pairs: list[tuple[str, str]] = []
        self.contra_pairs: list[tuple[str, str]] = []

    async def support(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        self.pairs.extend(pairs)
        return [self.score(p, h) for p, h in pairs], "nli@test"

    async def contradiction(self, pairs: Sequence[tuple[str, str]]) -> tuple[list[float], str]:
        self.contra_pairs.extend(pairs)
        return [self.against(p, h) for p, h in pairs], "nli@test"


def verify(claims: list[Claim], nli: ScriptedNLI) -> GroundingResult:
    return asyncio.run(Grounder(nli, Access()).verify(claims, {**EVIDENCE, **FX}))


def calc_claim(text: str, exprs: dict[str, str], **override: Input) -> Claim:
    inputs = {
        "a": Input("a", 3, "单笔金额 3 万美元", Decimal(30000), "美元"),
        "b": Input("b", 4, "第二笔 5 万美元", Decimal(50000), "美元"),
        "u": Input("u", 5, "单日累计不超过等值 5 万美元", Decimal(50000), "美元"),
    } | override
    refs = tuple(sorted({i.ref for i in inputs.values()}))
    return Claim(
        text, refs, (), "calculation", "b1", "paragraph", calc=Calc(tuple(inputs.values()), exprs)
    )


def test_calculation_result_is_computed_by_program() -> None:
    nli = ScriptedNLI(lambda p, h: 0.9)
    c = calc_claim("两笔合计 {r1} 美元，{c1}单日上限", {"r1": "a + b", "c1": "a + b > u"})
    (v,) = verify([c], nli).verdicts
    assert v.passed and v.rendered == "两笔合计 80000 美元，超过单日上限"
    assert v.results == {"r1": "80000", "c1": "超过"}
    assert v.text == "两笔合计 80000 美元，超过单日上限[S3][S4][S5]"
    # 措辞校验时结果换成“若干”，NLI 不判断数字
    assert nli.pairs == [
        (
            "单笔金额 3 万美元；第二笔 5 万美元；单日累计不超过等值 5 万美元",
            "两笔合计 若干 美元，若干单日上限",
        )
    ]


@pytest.mark.parametrize(
    ("name", "text", "exprs", "override"),
    [
        (
            "取值不在引文中",
            "合计 {r}",
            {"r": "a + b"},
            {"a": Input("a", 3, "单笔金额 3 万美元", Decimal(40000), "美元")},
        ),
        (
            "单位与原文不符",
            "合计 {r}",
            {"r": "a + b"},
            {"a": Input("a", 3, "单笔金额 3 万美元", Decimal(30000), "元")},
        ),
        ("心算结果直接写进句子", "两笔合计 8 万美元，即 {r}", {"r": "a + b"}, {}),
        ("没有占位符", "两笔合计 8 万美元", {"r": "a + b"}, {}),
        (
            "不同单位相加",
            "合计 {r}",
            {"r": "a + d"},
            {"d": Input("d", 1, "期限最长 5 年", Decimal(5), "年")},
        ),
        ("不允许的算式", "结果 {r}", {"r": "__import__('os')"}, {}),
        ("除以 0", "比值 {r}", {"r": "a / (b - b)"}, {}),
    ],
)
def test_calculation_rejected(
    name: str, text: str, exprs: dict[str, str], override: dict[str, Input]
) -> None:
    (v,) = verify([calc_claim(text, exprs, **override)], ScriptedNLI(lambda p, h: 0.9)).verdicts
    assert not v.passed and v.reason in ("calc_invalid", "quote_mismatch"), name


def synth(parts: tuple[Part, ...]) -> Claim:
    return Claim(
        "产品 A 由零售部管理，单笔审批上限为 50 万元",
        (6, 7),
        ("A 产品归口零售金融部管理", "单笔审批上限 50 万元"),
        "synthesis",
        "b1",
        "paragraph",
        parts=parts,
    )


MANAGED = Part("产品 A 由零售部管理", 6, "A 产品归口零售金融部管理")
LIMIT = Part("产品 A 的单笔审批上限为 50 万元", 7, "单笔审批上限 50 万元")


def test_synthesis_passes_when_parts_and_merge_hold() -> None:
    whole = synth(()).text

    # 两条事实合起来才推得出整句，少任何一条都推不出：两个引用都必要
    def score(p: str, h: str) -> float:
        return (0.9 if MANAGED.text in p and LIMIT.text in p else 0.3) if h == whole else 0.9

    (v,) = verify([synth((MANAGED, LIMIT))], ScriptedNLI(score)).verdicts
    assert v.passed and v.refs == (6, 7) and v.claim.claim_type == "synthesis"


def test_synthesis_with_misattributed_subject_falls_back_to_verified_parts() -> None:
    # S2 讲的是 B 产品：“产品 A 的审批上限”得不到支持，整句不放行，只推送已验证的原子事实
    nli = ScriptedNLI(lambda p, h: 0.2 if h == LIMIT.text else 0.9)
    vs = verify([synth((MANAGED, LIMIT))], nli).verdicts
    assert [(v.passed, v.text, v.claim.claim_type) for v in vs] == [
        (True, "产品 A 由零售部管理[S6]", "quote")
    ]


def test_synthesis_drops_redundant_ref_but_keeps_sentence() -> None:
    # 去掉 LIMIT 后仍能推出整句：LIMIT 多余，只删它的引用
    def score(p: str, h: str) -> float:
        return (0.9 if MANAGED.text in p else 0.3) if h == synth(()).text else 0.9

    (v,) = verify([synth((MANAGED, LIMIT))], ScriptedNLI(score)).verdicts
    assert v.passed and v.refs == (6,)


def test_synthesis_contradicted_by_a_cited_quote_is_dropped() -> None:
    # 合并推出成立，但 S7 的引文与整句矛盾：丢句并带出矛盾的证据编号（K8 详设 4.2 第 4 步）
    whole = synth(()).text
    nli = ScriptedNLI(
        lambda p, h: 0.9, lambda p, h: 0.8 if p == LIMIT.quote and h == whole else 0.1
    )
    (v,) = verify([synth((MANAGED, LIMIT))], nli).verdicts
    assert not v.passed and v.reason == "contradiction" and v.conflicts == (7,)
    assert {p for p, _ in nli.contra_pairs} == {MANAGED.quote, LIMIT.quote}


def test_conflicts_only_count_uncited_visible_documents() -> None:
    evidence = {
        1: Evidence(1, "pol:2025#a", "2025 版：单户额度不超过 30 万元。", doc_id="pol:2025"),
        2: Evidence(2, "pol:2023#a", "2023 版：单户额度不超过 20 万元。", doc_id="pol:2023"),
        3: Evidence(3, "pol:2022#a", "2022 版：单户额度不超过 10 万元。", doc_id="pol:2022"),
        4: Evidence(4, "x:inj#a", "单户额度不超过 1 万元。", suspicious=True, doc_id="x:inj"),
        5: Evidence(5, "pol:2025#b", "2025 版：单户额度不超过 30 万元。", doc_id="pol:2025"),
    }
    nli = ScriptedNLI(lambda p, h: 0.9, lambda p, h: 0.0 if p.startswith("2025") else 0.9)
    g = Grounder(nli, Access(denied=frozenset({"pol:2022#a"})))
    quote = "单户额度不超过 30 万元。"
    (v,) = asyncio.run(
        g.verify([claim("单户额度不超过 30 万元。", (1,), (quote,))], evidence)
    ).verdicts
    # S5 与引用同属一份文档、S4 带可疑标记：不参与；S3 此刻无权查看：不提示
    assert asyncio.run(g.conflicts([v], evidence)) == (2,)
    assert {p for p, _ in nli.contra_pairs} == {evidence[2].text, evidence[3].text}
    # 校验时发现的冲突一并复核权限后返回
    assert asyncio.run(g.conflicts([v], evidence, noted=(3, 5))) == (2, 5)
    # 没有通过的陈述时不调用 NLI
    nli.contra_pairs.clear()
    assert asyncio.run(g.conflicts([], evidence)) == ()


def test_synthesis_with_too_many_parts_is_rejected() -> None:
    parts = (MANAGED, LIMIT, MANAGED, LIMIT)
    (v,) = verify([synth(parts)], ScriptedNLI(lambda p, h: 0.9)).verdicts
    assert v.reason == "too_many_parts"


def test_split_keeps_synthesis_and_calculation_whole() -> None:
    c = synth((MANAGED, LIMIT))
    assert split_claim(c) == [c]
