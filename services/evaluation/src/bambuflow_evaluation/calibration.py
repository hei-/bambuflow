"""置信度校准（evaluation 详设 7.3，技术设计文档 7.8）。

用评测集上每道已回答题的置信度特征（引用覆盖率、平均支持度、最高精排分、来源多样性、是否有未解决的冲突）
与“回答正确且有据”的标签，拟合逻辑回归权重，用期望校准误差（ECE）检验：
- 每个租户已回答的题不少于 300 道时单独拟合，否则用全局权重；
- 拟合后的 ECE ≤ 0.05 才算达标，产出权重版本。

只算已回答的题：置信度是给已回答的回答看的，证据不足、拒答的回答不展示置信度。
标签：状态与期望一致（都是 answered）、每句都有支撑，且 LLM 评审没有判为不忠实、
标准答案的要点全部覆盖（没有评审时只看前两条）。

权重随报告产出，由人提交 governance 发布（权重作为受治理的配置对象下发 orchestrator 在 H24 做）。
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass

from bambuflow_evaluation.metrics import Outcome

FEATURES = ("coverage", "support", "rerank", "diversity", "conflict")
# 租户单独拟合的最少样本数（技术设计文档 7.8）
MIN_TENANT = 300
# 少于这个数不拟合：几十道题拟合出的权重没有意义
MIN_FIT = 50
ECE_TARGET = 0.05
BINS = 10
# L2 正则：防止某个特征恒定（如从没出现冲突）时矩阵奇异、权重发散
L2 = 0.01
ITERATIONS = 50


@dataclass(frozen=True)
class Sample:
    tenant: str
    features: tuple[float, ...]
    # orchestrator 用当前权重算出的置信度
    score: float
    label: bool


@dataclass(frozen=True)
class Result:
    # global 或租户名
    scope: str
    samples: int
    positive_rate: float | None
    # 当前权重（回答里的置信度）的 ECE
    ece_current: float | None
    # 拟合后权重的 ECE（在同一批样本上算）；没有拟合时为 None
    ece_fitted: float | None
    # 特征名 → 权重，另有 bias；没有拟合时为空
    weights: dict[str, float]
    version: str
    passed: bool
    note: str


def label(o: Outcome) -> bool:
    if o.status != "answered" or o.expected_status != "answered":
        return False
    if not o.sentence_grounded or not all(o.sentence_grounded):
        return False
    if o.faithful is False:
        return False
    return all(o.key_points_covered)


def samples(outcomes: Sequence[Outcome]) -> list[Sample]:
    return [
        Sample(o.tenant, o.confidence_features, o.confidence_score, label(o))
        for o in outcomes
        if o.status == "answered" and o.confidence_features and not o.pending
    ]


def calibrate(outcomes: Sequence[Outcome]) -> list[Result]:
    """全局一行，另每个租户一行：样本够的单独拟合，不够的注明使用全局权重。"""
    all_samples = samples(outcomes)
    out = [_result("global", all_samples)]
    for tenant in sorted({s.tenant for s in all_samples}):
        sub = [s for s in all_samples if s.tenant == tenant]
        if len(sub) >= MIN_TENANT:
            out.append(_result(tenant, sub))
        else:
            out.append(
                Result(
                    tenant,
                    len(sub),
                    _rate(sub),
                    ece([s.score for s in sub], [s.label for s in sub]) if sub else None,
                    None,
                    {},
                    "",
                    False,
                    f"已回答 {len(sub)} 题，少于 {MIN_TENANT}，使用全局权重",
                )
            )
    return out


def _rate(xs: Sequence[Sample]) -> float | None:
    return sum(s.label for s in xs) / len(xs) if xs else None


def _result(scope: str, xs: Sequence[Sample]) -> Result:
    labels = [s.label for s in xs]
    current = ece([s.score for s in xs], labels) if xs else None
    if len(xs) < MIN_FIT:
        return Result(
            scope,
            len(xs),
            _rate(xs),
            current,
            None,
            {},
            "",
            False,
            f"已回答 {len(xs)} 题，少于 {MIN_FIT}，不拟合",
        )
    if len(set(labels)) < 2:
        return Result(
            scope, len(xs), _rate(xs), current, None, {}, "", False, "标签全部相同，无法拟合"
        )
    w = fit([s.features for s in xs], labels)
    fitted = ece([predict(w, s.features) for s in xs], labels)
    weights = dict(zip((*FEATURES, "bias"), (round(v, 4) for v in w), strict=True))
    # 按 orchestrator 置信度公式的写法导出：冲突项在公式里是减号（−w5·k）
    weights["conflict"] = -weights["conflict"]
    version = (
        "confidence-"
        + hashlib.sha256(json.dumps(weights, sort_keys=True).encode()).hexdigest()[:12]
    )
    passed = fitted <= ECE_TARGET
    note = "达标，可提交发布" if passed else f"ECE 高于 {ECE_TARGET}，保留现有权重"
    return Result(scope, len(xs), _rate(xs), current, fitted, weights, version, passed, note)


def predict(w: Sequence[float], x: Sequence[float]) -> float:
    """w 的最后一个是 bias。拟合时各项都按加号，冲突项的符号在导出时换成公式的写法。"""
    z = sum(wi * xi for wi, xi in zip(w, x, strict=False)) + w[-1]
    return 1 / (1 + math.exp(-max(-30.0, min(30.0, z))))


def fit(xs: Sequence[Sequence[float]], ys: Sequence[bool]) -> list[float]:
    """带 L2 正则的逻辑回归，牛顿法求解。返回各特征权重，最后一个是 bias（不加正则）。"""
    n = len(FEATURES) + 1
    w = [0.0] * n
    rows = [[*x, 1.0] for x in xs]
    for _ in range(ITERATIONS):
        grad = [L2 * w[j] if j < n - 1 else 0.0 for j in range(n)]
        hess = [[(L2 if (i == j and i < n - 1) else 0.0) for j in range(n)] for i in range(n)]
        for row, y in zip(rows, ys, strict=True):
            p = predict(w, row[:-1])
            for j in range(n):
                grad[j] += (p - float(y)) * row[j]
                for k in range(n):
                    hess[j][k] += p * (1 - p) * row[j] * row[k]
        # 极小的对角项：bias 不加正则，样本全落在一侧时也能求解
        for j in range(n):
            hess[j][j] += 1e-9
        step = _solve(hess, grad)
        w = [wi - si for wi, si in zip(w, step, strict=True)]
        if max(abs(si) for si in step) < 1e-8:
            break
    return w


def _solve(a: list[list[float]], b: list[float]) -> list[float]:
    """高斯消元（列主元）。矩阵是正定的 6×6，不需要数值库。"""
    n = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(m[r][c]))
        m[c], m[p] = m[p], m[c]
        for r in range(c + 1, n):
            f = m[r][c] / m[c][c]
            for k in range(c, n + 1):
                m[r][k] -= f * m[c][k]
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        x[r] = (m[r][n] - sum(m[r][k] * x[k] for k in range(r + 1, n))) / m[r][r]
    return x


def ece(probs: Sequence[float], labels: Sequence[bool]) -> float:
    """期望校准误差：按预测概率等宽分 10 档，各档 |平均预测 − 实际正确率| 按样本数加权。"""
    bins: list[list[tuple[float, bool]]] = [[] for _ in range(BINS)]
    for p, y in zip(probs, labels, strict=True):
        bins[min(BINS - 1, int(p * BINS))].append((p, y))
    total = len(probs)
    return sum(
        len(b) / total * abs(sum(p for p, _ in b) / len(b) - sum(y for _, y in b) / len(b))
        for b in bins
        if b
    )
