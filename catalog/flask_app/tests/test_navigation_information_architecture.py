from __future__ import annotations

from pathlib import Path


TEMPLATES = Path("catalog/flask_app/templates")


def _base() -> str:
    return (TEMPLATES / "base.html").read_text(encoding="utf-8")


def test_workbench_primary_navigation_is_monitor_knowledge_federation_only() -> None:
    template = _base()
    primary = template.split("{% macro primary_links", maxsplit=1)[1].split(
        "{%- endmacro %}", maxsplit=1
    )[0]

    assert "'Monitor'" in primary
    assert "'Knowledge'" in primary
    assert "'Federation'" in primary
    assert "'System'" not in primary
    assert "'Device setup'" not in primary
    assert "'Device'" not in primary


def test_monitor_navigation_exposes_daily_operator_pages_without_data_upload() -> None:
    template = _base()
    monitor = template.split("{% else %}\n        <a class=\"admin-subnav__link", maxsplit=1)[1]

    for label in ("Overview", "Live", "Playback", "Explore", "Assist"):
        assert f">{label}</a>" in monitor
    assert ">Data upload</a>" not in monitor


def test_knowledge_owns_guide_and_ai_explainer() -> None:
    template = _base()
    knowledge = template.split("{% elif group == 'knowledge' %}", maxsplit=1)[1].split(
        "{% else %}", maxsplit=1
    )[0]

    assert ">AI Explainer</a>" in knowledge
    assert ">Guide</a>" in knowledge


def test_federation_owns_sources_recorders_and_this_device_diagnostics() -> None:
    template = _base()
    federation = template.split("{% macro federation_links", maxsplit=1)[1].split(
        "{%- endmacro %}", maxsplit=1
    )[0]

    assert ">This device</a>" in federation
    assert ">↳ Diagnostics</a>" in federation
    assert ">Sources</a>" in federation
    assert ">Recorders</a>" in federation
    assert "Device onboarding" not in federation
    assert "Device setup" not in federation


def test_sources_diagnostics_and_ai_are_classified_under_their_new_sections() -> None:
    template = _base()

    federation_classifier = "endpoint.startswith('source_web.') or endpoint == 'web.status'"
    assert federation_classifier in template
    assert "endpoint == 'web.guide' or endpoint.startswith('ai_web.')" in template
    assert "active_group = 'system'" not in template


def test_this_device_page_links_to_diagnostics() -> None:
    template = (TEMPLATES / "federation" / "detail_page.html").read_text(
        encoding="utf-8"
    )

    section = template.split("{% if vm.active_section == 'this-device' %}", maxsplit=1)[1].split(
        "{% endif %}", maxsplit=1
    )[0]
    assert "Open diagnostics" in section
    assert "url_for('web.status')" in section
