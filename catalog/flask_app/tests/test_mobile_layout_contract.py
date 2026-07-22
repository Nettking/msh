from pathlib import Path


def test_mobile_layout_stacks_navigation_and_overview_decision() -> None:
    css = Path("catalog/flask_app/static/css/global-refresh.css").read_text(encoding="utf-8")
    mobile_css = css.split("@media (max-width: 820px)", maxsplit=1)[1].split(
        "@media (max-width: 480px)", maxsplit=1
    )[0]

    assert ".site-header--grouped" in mobile_css
    assert "grid-template-columns: repeat(3, minmax(0, 1fr))" in mobile_css
    assert ".overview-decision" in mobile_css
    assert "grid-template-columns: minmax(0, 1fr)" in mobile_css
    assert "overflow-wrap: anywhere" in mobile_css


def test_setup_focus_uses_compact_mobile_shell_and_stepper() -> None:
    global_css = Path("catalog/flask_app/static/css/global-refresh.css").read_text(encoding="utf-8")
    wizard_css = Path("catalog/flask_app/static/css/wizard.css").read_text(encoding="utf-8")

    assert ".setup-shell-header" in global_css
    assert "main.setup-focus-main" in global_css
    assert "display: flex" in wizard_css.split(".setup-wizard-nav", maxsplit=1)[1]
    assert ".setup-wizard-nav button[hidden]" in wizard_css
    assert "grid-template-columns: 1fr 1fr" not in wizard_css
