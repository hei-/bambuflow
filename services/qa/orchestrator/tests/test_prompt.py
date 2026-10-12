from bambuflow_orchestrator import prompt


def test_builtin_templates_pass_five_zone_check() -> None:
    """内置模板与 governance 发布前的检查用同一份规则：system 只有租户规则，user 按顺序是各区。"""
    for scope, t in (("answer", prompt.ANSWER), ("agent-answer", prompt.AGENT)):
        assert prompt.placeholders(t.system) == ["tenant_rules"]
        assert tuple(prompt.placeholders(t.user)) == prompt.ZONES[scope]


def test_fill_drops_empty_zones_and_appends_tenant_rules() -> None:
    zones = {
        "summary": "",
        "evidence": "<evidence>E</evidence>",
        "question": "<question>Q</question>",
    }
    system, user = prompt.fill(prompt.ANSWER, "", zones)
    assert system == prompt.SYSTEM
    assert user == "<evidence>E</evidence>\n<question>Q</question>"
    system, user = prompt.fill(
        prompt.ANSWER, "金额写成万元。", {**zones, "summary": prompt.render_summary("在问贷款")}
    )
    assert system.startswith(prompt.SYSTEM) and system.endswith("金额写成万元。")
    assert user.startswith("<summary>\n在问贷款\n</summary>\n<evidence>")
