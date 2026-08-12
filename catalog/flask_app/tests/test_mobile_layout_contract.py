from pathlib import Path


def test_small_screens_slide_the_rail_over_the_content() -> None:
    """The rail is the navigation at every width.

    On small screens it leaves the grid and slides in over the content behind a
    scrim, so there is no second mobile-only menu to keep in step with it.
    """

    css = Path("catalog/flask_app/static/css/admin-shell.css").read_text(encoding="utf-8")
    narrow = css.split("@media (max-width: 900px)", maxsplit=1)[1]

    assert "grid-template-columns: minmax(0, 1fr)" in narrow
    assert "position: fixed" in narrow
    assert "transform: translateX(-100%)" in narrow
    assert '.admin-shell[data-rail-open="true"] .admin-rail' in narrow
    assert ".admin-rail__scrim" in narrow
    assert ".admin-rail-toggle" in narrow
    # Collapsing is a desktop affordance; the overlay always shows full labels.
    assert "display: revert" in narrow


def test_content_stays_readable_on_narrow_screens() -> None:
    css = Path("catalog/flask_app/static/css/global-refresh.css").read_text(encoding="utf-8")
    mobile_css = css.split("@media (max-width: 820px)", maxsplit=1)[1].split(
        "@media (max-width: 480px)", maxsplit=1
    )[0]

    assert ".overview-decision" in mobile_css
    assert "grid-template-columns: minmax(0, 1fr)" in mobile_css
    assert "overflow-wrap: anywhere" in mobile_css


def test_rail_overlay_closes_on_scrim_escape_and_navigation() -> None:
    script = Path("catalog/flask_app/static/js/admin-rail.js").read_text(encoding="utf-8")

    assert 'querySelector("[data-admin-shell]")' in script
    assert "[data-rail-close]" in script
    assert 'event.key === "Escape"' in script
    assert 'window.matchMedia("(min-width: 901px)")' in script
    # The collapsed preference persists; the overlay never starts open.
    assert "fcp-rail-collapsed" in script


def test_setup_focus_uses_compact_mobile_shell_and_stepper() -> None:
    global_css = Path("catalog/flask_app/static/css/global-refresh.css").read_text(encoding="utf-8")
    wizard_css = Path("catalog/flask_app/static/css/wizard.css").read_text(encoding="utf-8")

    assert ".setup-shell-header" in global_css
    assert "main.setup-focus-main" in global_css
    assert "display: flex" in wizard_css.split(".setup-wizard-nav", maxsplit=1)[1]
    assert ".setup-wizard-nav button[hidden]" in wizard_css
    assert "grid-template-columns: 1fr 1fr" not in wizard_css
