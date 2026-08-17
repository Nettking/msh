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

    recorder = template.split("{% macro recorder_link", maxsplit=1)[1].split(
        "{%- endmacro %}", maxsplit=1
    )[0]

    assert ">This device</a>" in federation
    assert ">↳ Diagnostics</a>" in federation
    assert ">Sources</a>" in federation
    # Recorders is rendered through a shared macro so both the data-driven and
    # the fallback navigation place it in the same position, after Services.
    assert ">Recorders</a>" in recorder
    assert "{{ recorder_link(current_endpoint) }}" in federation
    assert "Device onboarding" not in federation
    assert "Device setup" not in federation


def test_federation_navigation_puts_devices_before_contribution_surfaces() -> None:
    """A Federation is its devices first, then what those devices contribute."""

    template = _base()
    federation = template.split("{% macro federation_links", maxsplit=1)[1].split(
        "{%- endmacro %}", maxsplit=1
    )[0]
    fallback = federation.split("{% else %}", maxsplit=1)[1]

    order = [
        label
        for label in (
            ">Overview</a>",
            ">Devices</a>",
            ">This device</a>",
            ">Services</a>",
            ">Benchmarks</a>",
        )
        if label in fallback
    ]
    positions = [fallback.index(label) for label in order]
    assert positions == sorted(positions)
    assert fallback.index(">Devices</a>") < fallback.index(">Services</a>")
    assert fallback.index(">Devices</a>") < fallback.index(">Benchmarks</a>")


def test_approvals_is_a_demoted_exception_surface() -> None:
    """Trusted, green contributions enroll automatically, so this is not a queue."""

    template = _base()
    federation = template.split("{% macro federation_links", maxsplit=1)[1].split(
        "{%- endmacro %}", maxsplit=1
    )[0]

    assert "admin-subnav__link--exception" in federation
    assert federation.index(">Approvals</a>") > federation.index(">Settings</a>")


def test_federation_section_model_leads_with_devices() -> None:
    from catalog.federation.projections.service_core import _SECTION_LINKS

    keys = [link.key for link in _SECTION_LINKS]
    assert keys[0] == "overview"
    assert keys[1] == "devices"
    assert keys.index("devices") < keys.index("services")
    assert keys.index("devices") < keys.index("this-device")


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


def test_overview_shows_member_devices_before_contribution_panels() -> None:
    """Members come immediately after the Federation header, not below services."""

    template = (TEMPLATES / "federation_overview.html").read_text(encoding="utf-8")

    devices = template.index('federation-panel--devices')
    this_device = template.index('federation-panel--device"')
    benchmarks = template.index('federation-panel--benchmarks')
    contributions = template.index('<span class="federation-eyebrow">Contributions</span>')

    assert devices < this_device
    assert devices < benchmarks
    assert devices < contributions
    # Each member row carries its name, its role, and its live health.
    assert "device.role_label" in template
    assert "status_badge(device.state, device.state_label)" in template
