"""中文切句（orchestrator 详设 9.1）。

模型每行输出一个陈述单元；单元含多个句子时按句末标点拆开，子句共用该单元的引用，分别做蕴含判定。
设计里“超过 120 字也拆”的规则同样只能在句末标点处拆，所以只看句末标点即可。
"""

from __future__ import annotations

import dataclasses
import re

from bambuflow_grounding.verify import Claim

# 句末标点；拆开后标点留在前一句末尾
_END = re.compile(r"(?<=[。？！；?!;])")


def split_claim(claim: Claim) -> list[Claim]:
    if claim.parts or claim.calc is not None:
        # 综合句与计算句按整体校验：拆开后原子事实、占位符就和句子对不上了
        return [claim]
    parts = [p for p in (s.strip() for s in _END.split(claim.text)) if p]
    if len(parts) <= 1:
        return [claim]
    return [dataclasses.replace(claim, text=p) for p in parts]
