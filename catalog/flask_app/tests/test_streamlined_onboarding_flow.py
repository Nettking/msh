from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from catalog.flask_app.services.capability_startup_transition_service import (
    CapabilityStartupTransitionService,
)

TEMPLATE_ROOT = Path(__file__).resolve().parents[1] / "templates"


def test_completed_setup_offers_explicit_continue_instead_of_overview() -> None:
    service = object.__new__(CapabilityStartupTransitionService)
    service.load = lambda: SimpleNamespace(
        completed=True,
        source_kind="capability-first",
    )
    service.legacy_preview = lambda: None
    view_model = {
        "overall_state": "pending",
        "overall_state_label": "Pending",
        "finish": {},
        "steps": [{"key": "finish", "available": True, "summary": "Finish"}],
        "completed_steps": [],
        "migration": {},
        "actions": {},
        "storage_key": "msh.onboarding.device.cfi5",
    }

    service.apply_to_onboarding_view_model(
        view_model,
        requested_step="finish",
    )

    assert view_model["current_step"] == "finish"
    assert view_model["finish"]["next_action"] == {
        "title": "Setup complete",
        "message": "Continue to MSH when you are ready.",
        "label": "Continue to MSH",
        "url": "/",
    }
    assert view_model["completed_steps"] == ["finish"]


def test_finish_panel_has_no_federation_escape_link() -> None:
    template = (
        TEMPLATE_ROOT / "federation" / "onboarding" / "_finish.html"
    ).read_text(encoding="utf-8")

    assert "Open Federation overview" not in template
    assert "vm.actions.overview_url" not in template
    assert "data-prev-step" in template


def test_capability_onboarding_uses_focused_setup_shell() -> None:
    base = (TEMPLATE_ROOT / "base.html").read_text(encoding="utf-8")

    assert "endpoint.startswith('capability_')" in base
    assert "{% if setup_focus %}" in base
