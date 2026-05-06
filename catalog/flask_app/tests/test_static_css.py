from __future__ import annotations

from pathlib import Path
import re


CSS_PATH = Path("catalog/flask_app/static/css/styles.css")


def _styles() -> str:
    return CSS_PATH.read_text(encoding="utf-8")


def _rule_body(css: str, selector: str, *, last: bool = False) -> str:
    pattern = re.compile(
        rf"^{re.escape(selector)}\s*\{{(?P<body>.*?)\}}",
        re.DOTALL | re.MULTILINE,
    )
    matches = list(pattern.finditer(css))
    assert matches, f"Missing CSS rule for {selector}"
    match = matches[-1] if last else matches[0]
    return match.group("body")


def test_primary_buttons_override_section_button_surface() -> None:
    css = _styles()

    assert css.index("button.button-primary") > css.index(".section-block button")
    primary_body = _rule_body(css, "button.button-primary")
    assert "background: var(--button-primary-bg);" in primary_body
    assert "color: var(--button-primary-text);" in primary_body


def test_secondary_and_hover_buttons_keep_explicit_readable_text_color() -> None:
    css = _styles()

    button_body = _rule_body(css, "button")
    section_button_body = _rule_body(css, ".section-block button", last=True)
    hover_body = _rule_body(css, "button:hover")
    primary_hover_body = _rule_body(css, "button.button-primary:hover")

    assert "color: var(--button-secondary-text);" in button_body
    assert "color: var(--button-secondary-text);" in section_button_body
    assert "color: var(--button-secondary-text);" in hover_body
    assert "color: var(--button-primary-text);" in primary_hover_body
