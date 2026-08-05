from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.federation.onboarding_models import (
    ContributionDesiredState,
    FederationConnectionState,
)
from catalog.flask_app.services.capability_startup_transition_service import (
    CapabilityStartupTransitionService,
)
from catalog.flask_app.services.server_setup_service import default_settings

NOW = datetime(2026, 8, 5, 15, 30, tzinfo=timezone.utc)


class OnboardingStub:
    def __init__(self) -> None:
        identity = SimpleNamespace(node_id="device-fast-one")
        credentials = SimpleNamespace(identity=identity)
        binding = SimpleNamespace(
            federation_id="federation-fast-one",
            internal_session_id="session-fast-one",
            state=FederationConnectionState.CONNECTED,
        )
        self.context = SimpleNamespace(credentials=credentials, binding=binding)

    def identity_or_none(self):
        return self.context.credentials

    def authorized_context(self):
        return self.context

    def binding_or_none(self):
        return self.context.binding


class InspectionStub:
    def __init__(self, *, current: bool = True) -> None:
        self.snapshot = SimpleNamespace(device_id="device-fast-one", revision=7)
        self.current = current

    def load(self):
        return self.snapshot

    def state(self, snapshot):
        assert snapshot is self.snapshot
        return "current" if self.current else "expired"


class ContributionsMustNotRun:
    def __getattr__(self, name: str):
        raise AssertionError(f"Fast setup must not call contribution service: {name}")


def _service(tmp_path, *, inspection_current: bool = True):
    saved_settings = []
    service = CapabilityStartupTransitionService(
        onboarding_service=OnboardingStub(),
        inspection_service=InspectionStub(current=inspection_current),
        contribution_service=ContributionsMustNotRun(),
        state_database=tmp_path / "onboarding.sqlite3",
        setup_loader=lambda: default_settings(configured=False),
        setup_saver=saved_settings.append,
        clock=lambda: NOW,
    )
    return service, saved_settings


def test_fast_setup_completes_after_inspection_without_benchmarks(tmp_path) -> None:
    service, saved_settings = _service(tmp_path)

    state = service.complete_current()

    assert state.completed is True
    assert state.inspection_revision == 7
    assert state.contribution_intents["workbench"] == (
        ContributionDesiredState.ENABLED.value
    )
    assert state.contribution_intents["runtime"] == (
        ContributionDesiredState.ENABLED.value
    )
    for capability in ("recorder", "language-model", "compute", "storage"):
        assert state.contribution_intents[capability] == (
            ContributionDesiredState.ASK_LATER.value
        )
    assert len(saved_settings) == 1
    assert saved_settings[0].configured is True
    assert saved_settings[0].user_setup_complete is True
    assert saved_settings[0].deployment_mode == "web-workbench"
    assert saved_settings[0].ai_enabled is False


def test_fast_setup_still_requires_current_inspection(tmp_path) -> None:
    service, saved_settings = _service(tmp_path, inspection_current=False)

    with pytest.raises(FederationOperationError) as error:
        service.complete_current()

    assert error.value.code == "startup-transition-inspection-required"
    assert saved_settings == []


def test_fast_setup_templates_expose_finish_and_optional_follow_up() -> None:
    inspect_template = Path(
        "catalog/flask_app/templates/federation/onboarding/_inspect.html"
    ).read_text(encoding="utf-8")
    overview_template = Path(
        "catalog/flask_app/templates/federation_overview.html"
    ).read_text(encoding="utf-8")

    assert "Step 3 of 3" in inspect_template
    assert "Finish setup and open Federation" in inspect_template
    assert "Recommended bounded checks" in inspect_template
    assert "These recommendations do not block setup" in inspect_template
    assert "Review optional capabilities" in overview_template
