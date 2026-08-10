from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"


def _render_badge(state: str, label: str | None = None) -> str:
    environment = Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=True,
    )
    template = environment.from_string(
        "{% from 'federation/_status_badge.html' import status_badge %}"
        "{{ status_badge(state, label) }}"
    )
    return template.render(state=state, label=label)


def test_runtime_verified_is_presented_as_updated_success() -> None:
    rendered = _render_badge("runtime_verified", "Runtime Verified")

    assert "federation-status--complete" in rendered
    assert "Updated" in rendered
    assert "Runtime Verified" not in rendered


def test_other_status_labels_are_unchanged() -> None:
    rendered = _render_badge("activation_queued", "Activation Queued")

    assert "federation-status--activation-queued" in rendered
    assert "Activation Queued" in rendered
