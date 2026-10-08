import json
from pathlib import Path

from bambuflow_orchestrator.redact import mask

CASES = Path(__file__).resolve().parents[4] / "tools" / "test-upstream" / "pii_cases.json"


def test_shared_cases_match_go_implementation() -> None:
    for c in json.loads(CASES.read_text(encoding="utf-8"))["cases"]:
        assert mask(c["in"]) == c["out"], c["in"]
        assert mask(c["out"]) == c["out"], "掩码应幂等"
