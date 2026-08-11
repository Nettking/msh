from pathlib import Path


APP_ROOT = Path("catalog/flask_app")
THEME_CSS = APP_ROOT / "static" / "css" / "theme.css"
CHART_THEME_JS = APP_ROOT / "static" / "js" / "chart-theme.js"


def _theme_css() -> str:
    return THEME_CSS.read_text(encoding="utf-8")


def _chart_theme_js() -> str:
    return CHART_THEME_JS.read_text(encoding="utf-8")


def test_chart_tooltip_uses_explicit_contrasting_theme_tokens() -> None:
    script = _chart_theme_js()

    assert 'backgroundColor = token("--tooltip-bg"' in script
    assert 'titleColor = token("--on-solid"' in script
    assert 'bodyColor = token("--on-solid"' in script
    assert 'footerColor = token("--on-solid"' in script
    assert "options.plugins.tooltip.backgroundColor" in script
    assert "options.plugins.tooltip.titleColor" in script
    assert "options.plugins.tooltip.bodyColor" in script
    assert "options.plugins.tooltip.footerColor" in script


def test_chart_palette_is_defined_for_light_and_both_dark_paths() -> None:
    css = _theme_css()

    assert css.count("--tooltip-bg:") == 3
    for index in range(1, 9):
        assert css.count(f"--chart-{index}:") == 3


def test_legacy_playback_series_are_rethemed_on_load_and_theme_change() -> None:
    script = _chart_theme_js()

    for color in (
        "#1d4ed8",
        "#15803d",
        "#a16207",
        "#7c3aed",
        "#be123c",
        "#0891b2",
        "#4338ca",
        "#0f766e",
    ):
        assert f'"{color}"' in script

    assert "_fcpThemeSeriesIndex" in script
    assert 'document.addEventListener("DOMContentLoaded", refreshExistingCharts)' in script
    assert 'document.addEventListener("fcp:themechange"' in script


def test_live_board_reflows_below_380px_without_fixed_track_overflow() -> None:
    css = _theme_css()
    narrow = css.split("@media (max-width: 380px)", maxsplit=1)[1]

    assert "body .live-grid" in narrow
    assert "body .live-card .kv-grid" in narrow
    assert narrow.count("grid-template-columns: minmax(0, 1fr);") >= 2
