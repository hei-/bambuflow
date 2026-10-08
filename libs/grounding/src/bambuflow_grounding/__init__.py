"""逐句溯源校验：切句，以及引文核对、蕴含判定、权限复核三道检查（orchestrator 详设第 9 节）。
综合句与计算句的校验见 K7/K8 详设。"""

from bambuflow_grounding.calc import CALCULATOR_VERSION, Calc, CalcError, Input
from bambuflow_grounding.nli import KServeNLI
from bambuflow_grounding.split import split_claim
from bambuflow_grounding.verify import (
    COMBINED_THRESHOLD,
    SUPPORT_THRESHOLD,
    AccessChecker,
    Claim,
    Evidence,
    Grounder,
    GroundingResult,
    GroundingUnavailableError,
    Part,
    Verdict,
)

__all__ = [
    "CALCULATOR_VERSION",
    "COMBINED_THRESHOLD",
    "SUPPORT_THRESHOLD",
    "AccessChecker",
    "Calc",
    "CalcError",
    "Claim",
    "Evidence",
    "Grounder",
    "GroundingResult",
    "GroundingUnavailableError",
    "Input",
    "KServeNLI",
    "Part",
    "Verdict",
    "split_claim",
]
