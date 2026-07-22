from pathlib import Path


def test_mobile_layout_uses_compact_overlay_navigation() -> None:
    css = Path("catalog/flask_app/static/css/global-refresh.css").read_text(encoding="utf-8")
    mobile_css = css.split("@media (max-width: 820px)", maxsplit=1)[1].split(
        "@media (max-width: 480px)", maxsplit=1
    )[0]

    assert ".site-header--grouped" in mobile_css
    assert "grid-template-columns: minmax(0, 1fr) auto" in mobile_css
    assert ".mobile-navigation__panel" in mobile_css
    assert "position: absolute" in mobile_css
    assert "max-height: calc(100dvh - 76px)" in mobile_css
    assert ".site-header--grouped > .catalog-freshness" in mobile_css
    assert "display: none" in mobile_css
    assert ".overview-decision" in mobile_css
    assert "grid-template-columns: minmax(0, 1fr)" in mobile_css
    assert "overflow-wrap: anywhere" in mobile_css


def test_mobile_navigation_closes_outside_and_with_escape() -> None:
    script = Path("catalog/flask_app/static/js/mobile-navigation.js").read_text(encoding="utf-8")

    assert 'querySelector("[data-mobile-navigation]")' in script
    assert '!menu.contains(event.target)' in script
    assert 'event.key === "Escape"' in script
    assert 'window.matchMedia("(min-width: 821px)")' in script


def test_setup_focus_uses_compact_mobile_shell_and_stepper() -> None:
    global_css = Path("catalog/flask_app/static/css/global-refresh.css").read_text(encoding="utf-8")
    wizard_css = Path("catalog/flask_app/static/css/wizard.css").read_text(encoding="utf-8")

    assert ".setup-shell-header" in global_css
    assert "main.setup-focus-main" in global_css
    assert "display: flex" in wizard_css.split(".setup-wizard-nav", maxsplit=1)[1]
    assert ".setup-wizard-nav button[hidden]" in wizard_css
    assert "grid-template-columns: 1fr 1fr" not in wizard_css
