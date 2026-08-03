from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationOperationError, FederationValidationError
from catalog.federation.onboarding_models import (
    ContributionDesiredState,
    FederationConnectionState,
)
from catalog.flask_app import app as app_module
from catalog.flask_app import capability_startup_transition_routes as transition_routes
from catalog.flask_app import routes as routes_module
from catalog.flask_app import server_setup_routes
from catalog.flask_app.app import create_app
from catalog.flask_app.capability_onboarding_routes import (
    _COMMAND_SESSION_KEY,
    _CSRF_SESSION_KEY,
)
from catalog.flask_app.services.capability_startup_transition_service import (
    CapabilityStartupState,
    CapabilityStartupStateStore,
    CapabilityStartupTransitionService,
)
from catalog.flask_app.services.server_setup_service import default_settings

NOW = datetime(2026, 8, 3, 11, 30, tzinfo=timezone.utc)
DEVICE_ID = "node-transition-device"
FEDERATION_ID = "federation-transition"
SESSION_ID = "session-transition"


class FakeRuntimeManager:
    def mark_app_started(self) -> None:
        pass

    def requires_startup_choice(self) -> bool:
        return False

    def startup_decision_snapshot(self) -> dict[str, bool]:
        return {"requires_choice": False}


class FakeOnboarding:
    def __init__(
        self,
        *,
        connected: bool = True,
        saved_binding: bool = False,
        connect_error: Exception | None = None,
    ) -> None:
        identity = SimpleNamespace(node_id=DEVICE_ID, display_name="Transition device")
        self.credentials = SimpleNamespace(identity=identity)
        self.binding = SimpleNamespace(
            federation_id=FEDERATION_ID,
            internal_session_id=SESSION_ID,
            state=FederationConnectionState.CONNECTED,
        )
        self.context = (
            SimpleNamespace(credentials=self.credentials, binding=self.binding)
            if connected
            else None
        )
        self.saved_binding = self.binding if saved_binding else None
        self.connect_error = connect_error
        self.connect_calls: list[str] = []

    def identity_or_none(self):
        return self.credentials

    def create_identity(self):
        return self.credentials

    def authorized_context(self):
        return self.context

    def binding_or_none(self):
        return self.saved_binding

    def connect(self, *, request_id: str):
        self.connect_calls.append(request_id)
        if self.connect_error is not None:
            raise self.connect_error
        self.context = SimpleNamespace(
            credentials=self.credentials,
            binding=self.binding,
        )
        return self.binding


class FakeInspection:
    def __init__(self, revision: int = 0) -> None:
        self.snapshot = (
            None
            if revision == 0
            else SimpleNamespace(device_id=DEVICE_ID, revision=revision)
        )

    def load(self):
        return self.snapshot


class FakeContribution:
    def __init__(self, choices=None, *, complete: bool = True) -> None:
        self.choices = choices or {}
        self.complete = complete
        self.snapshot = SimpleNamespace(device_id=DEVICE_ID, revision=4)

    def _authorized_snapshot(self, *, require_benchmark_review: bool):
        assert require_benchmark_review is True
        return self.snapshot, True

    def recommend(self, *, require_benchmark_review: bool = True):
        assert require_benchmark_review is True
        return tuple(
            SimpleNamespace(candidate_id=candidate_id, capability_type=kind)
            for candidate_id, (kind, _desired) in self.choices.items()
        )

    def view_model(self, _snapshot, *, connected: bool, benchmark_complete: bool):
        assert connected and benchmark_complete
        state = "complete" if self.complete else "pending"
        return {"state": state}, [], self.complete

    def intents(self):
        return tuple(
            SimpleNamespace(candidate_id=candidate_id, desired_state=desired)
            for candidate_id, (_kind, desired) in self.choices.items()
        )


class FakeTransitionRouteService:
    def __init__(self, flags: dict[str, object]) -> None:
        self.flags = flags
        self.migration_calls: list[str] = []
        self.finish_calls = 0

    def capability_flags(self):
        return dict(self.flags)

    def migrate_legacy(self, *, request_id: str):
        self.migration_calls.append(request_id)

    def complete_current(self):
        self.finish_calls += 1

    def apply_to_onboarding_view_model(self, view_model, *, requested_step):
        view_model["current_step"] = requested_step or "finish"
        return view_model


def _state(**changes) -> CapabilityStartupState:
    values = {
        "device_id": DEVICE_ID,
        "federation_id": FEDERATION_ID,
        "internal_session_id": SESSION_ID,
        "federation_state": FederationConnectionState.CONNECTED,
        "inspection_revision": 0,
        "contribution_intents": {
            "workbench": "enabled",
            "runtime": "enabled",
            "recorder": "disabled",
            "language-model": "enabled",
            "compute": "disabled",
            "storage": "disabled",
        },
        "completed": True,
        "source_kind": "capability-first",
        "source_schema": None,
        "source_mode": None,
        "source_revision": 1,
        "updated_at": NOW,
    }
    values.update(changes)
    return CapabilityStartupState(**values)


def _settings(mode: str, *, ai_enabled: bool = True):
    return replace(
        default_settings(configured=True),
        deployment_mode=mode,
        ai_enabled=ai_enabled,
        ai_model="fixture-model:1",
        ollama_base_url="http://10.20.30.40:11434",
        recorder_sources="Machine=http://10.20.30.50:5000",
    )


def _service(
    tmp_path: Path,
    *,
    onboarding=None,
    inspection=None,
    contribution=None,
    settings=None,
    saved_settings=None,
):
    saved_settings = [] if saved_settings is None else saved_settings
    return CapabilityStartupTransitionService(
        onboarding_service=onboarding or FakeOnboarding(),
        inspection_service=inspection or FakeInspection(),
        contribution_service=contribution or FakeContribution(),
        state_database=tmp_path / "onboarding.sqlite3",
        setup_loader=lambda: settings or default_settings(configured=False),
        setup_saver=saved_settings.append,
        clock=lambda: NOW,
    )


@pytest.mark.parametrize(
    ("mode", "enabled"),
    [
        ("full-server", {"workbench", "runtime", "recorder", "language-model"}),
        ("web-workbench", {"workbench", "runtime", "language-model"}),
        ("web-ui-only", {"workbench", "language-model"}),
        ("recorder-only", {"recorder"}),
        ("language-model-provider", {"language-model"}),
    ],
)
def test_all_legacy_modes_migrate_without_compute_storage_or_private_values(
    tmp_path: Path,
    mode: str,
    enabled: set[str],
) -> None:
    service = _service(tmp_path, settings=_settings(mode))

    first = service.migrate_legacy(request_id="migration-command")
    second = service.migrate_legacy(request_id="ignored-command")

    actual = {
        key for key, value in first.contribution_intents.items() if value == "enabled"
    }
    assert actual == enabled
    assert first.contribution_intents["compute"] == "disabled"
    assert first.contribution_intents["storage"] == "disabled"
    assert first.source_mode == mode
    assert second == first
    persisted = (tmp_path / "onboarding.sqlite3").read_bytes()
    for private in (b"10.20.30.40", b"10.20.30.50", b"fixture-model"):
        assert private not in persisted


def test_migration_fences_ambiguous_discovery_and_broken_saved_binding(
    tmp_path: Path,
) -> None:
    ambiguous = FakeOnboarding(
        connected=False,
        connect_error=FederationOperationError(
            "candidate-selection-required",
            "choose one",
            "discovery_id",
        ),
    )
    service = _service(
        tmp_path / "ambiguous",
        onboarding=ambiguous,
        settings=_settings("web-workbench"),
    )
    with pytest.raises(FederationOperationError) as selection:
        service.migrate_legacy(request_id="ambiguous")
    assert selection.value.code == "candidate-selection-required"
    assert service.load() is None

    broken = FakeOnboarding(connected=False, saved_binding=True)
    service = _service(
        tmp_path / "broken",
        onboarding=broken,
        settings=_settings("web-workbench"),
    )
    with pytest.raises(FederationOperationError) as repair:
        service.migrate_legacy(request_id="repair")
    assert repair.value.code == "startup-transition-reconnect-required"
    assert broken.connect_calls == []


def test_migration_creates_local_federation_only_when_none_exists(tmp_path: Path) -> None:
    onboarding = FakeOnboarding(connected=False)
    service = _service(
        tmp_path,
        onboarding=onboarding,
        settings=_settings("web-ui-only"),
    )

    state = service.migrate_legacy(request_id="local-create")

    assert state.completed
    assert onboarding.connect_calls == ["local-create"]


def test_store_rejects_binding_replacement_and_corrupt_state(tmp_path: Path) -> None:
    store = CapabilityStartupStateStore(tmp_path / "state.sqlite3")
    saved = store.save(_state())
    assert store.load() == saved

    with pytest.raises(FederationValidationError) as replacement:
        store.save(replace(saved, federation_id="different-federation"))
    assert replacement.value.code == "startup-transition-replacement-forbidden"

    with sqlite3.connect(tmp_path / "state.sqlite3") as connection:
        connection.execute(
            "UPDATE capability_startup_state SET state_json=? WHERE singleton=1",
            ("{not-json",),
        )
        connection.commit()
    with pytest.raises(FederationValidationError) as corrupt:
        store.load()
    assert corrupt.value.code == "malformed-startup-transition"


def test_capability_finish_persists_intent_and_compatibility_settings(
    tmp_path: Path,
) -> None:
    saved_settings: list[object] = []
    contribution = FakeContribution(
        {
            "ai": ("language-model", ContributionDesiredState.ENABLED),
            "compute": ("compute", ContributionDesiredState.DISABLED),
        }
    )
    service = _service(
        tmp_path,
        inspection=FakeInspection(4),
        contribution=contribution,
        saved_settings=saved_settings,
    )

    state = service.complete_current()

    assert state.inspection_revision == 4
    assert state.contribution_intents == {
        "workbench": "enabled",
        "runtime": "enabled",
        "recorder": "disabled",
        "language-model": "enabled",
        "compute": "disabled",
        "storage": "disabled",
    }
    assert saved_settings[0].deployment_mode == "web-workbench"
    assert saved_settings[0].ai_enabled is True


def test_runtime_flags_and_finish_action_come_from_capability_state(tmp_path: Path) -> None:
    service = _service(tmp_path, settings=_settings("full-server"))
    service.store.save(
        _state(
            contribution_intents={
                "workbench": "enabled",
                "runtime": "disabled",
                "recorder": "enabled",
                "language-model": "disabled",
                "compute": "disabled",
                "storage": "disabled",
            }
        )
    )
    flags = service.capability_flags()
    assert flags["runtime"] is False
    assert flags["recorder"] is True
    assert service.runtime_should_start() is False

    fresh = _service(tmp_path / "fresh")
    view_model = {
        "steps": [{"key": "finish", "available": True}],
        "finish": {"state": "blocked"},
        "migration": {},
        "actions": {},
        "storage_key": "msh.onboarding.device.cfi5",
    }
    fresh.apply_to_onboarding_view_model(view_model, requested_step="finish")
    assert view_model["finish"]["transition_action"]["kind"] == "finish"
    assert view_model["storage_key"].endswith(".cfi6")


def _flags(*, completed: bool = False, needs_migration: bool = False):
    return {
        "workbench": completed,
        "runtime": completed,
        "recorder": False,
        "language_model": False,
        "compute": False,
        "storage": False,
        "completed": completed,
        "needs_migration": needs_migration,
        "source": "capability-first" if completed else "unconfigured",
        "state": object() if completed else None,
    }


def _route_app(monkeypatch, tmp_path: Path, transition):
    tmp_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)
    manager = FakeRuntimeManager()
    settings = replace(
        default_settings(configured=True),
        deployment_mode="web-workbench",
        ai_enabled=False,
    )
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(routes_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(server_setup_routes, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(app_module, "load_settings", lambda: settings)
    monkeypatch.setattr(routes_module, "load_settings", lambda: settings)
    monkeypatch.setattr(server_setup_routes, "load_settings", lambda: settings)
    monkeypatch.setattr(
        transition_routes,
        "get_capability_startup_transition_service",
        lambda: transition,
    )
    app = create_app()
    app.config.update(TESTING=True)
    return app


def test_cfi6_registration_and_startup_routing(monkeypatch, tmp_path) -> None:
    fresh = FakeTransitionRouteService(_flags())
    app = _route_app(monkeypatch, tmp_path / "fresh", fresh)
    blueprints = list(app.blueprints)
    assert blueprints.index("capability_startup_transition_web") < blueprints.index(
        "capability_contribution_web"
    )
    assert app.test_client().get("/").location == "/onboarding"

    legacy = FakeTransitionRouteService(_flags(needs_migration=True))
    app = _route_app(monkeypatch, tmp_path / "legacy", legacy)
    response = app.test_client().get("/startup")
    assert response.location == "/onboarding?step=finish"


def test_returning_device_and_explicit_legacy_fallback(monkeypatch, tmp_path) -> None:
    completed = FakeTransitionRouteService(_flags(completed=True))
    app = _route_app(monkeypatch, tmp_path / "completed", completed)
    client = app.test_client()
    assert client.get("/").location == "/federation"
    assert client.get("/").status_code == 200

    legacy = FakeTransitionRouteService(_flags(needs_migration=True))
    app = _route_app(monkeypatch, tmp_path / "fallback", legacy)
    client = app.test_client()
    assert client.get("/startup").location == "/onboarding?step=finish"
    assert client.get("/startup?legacy=1&edit=1").status_code == 200


def test_transition_posts_require_csrf_command_and_server_owned_context(
    monkeypatch,
    tmp_path,
) -> None:
    transition = FakeTransitionRouteService(_flags(needs_migration=True))
    app = _route_app(monkeypatch, tmp_path, transition)
    client = app.test_client()
    csrf = "csrf-" + "x" * 32
    command = "command-" + "y" * 24
    with client.session_transaction() as browser:
        browser[_CSRF_SESSION_KEY] = csrf
        browser[_COMMAND_SESSION_KEY] = command

    assert client.post("/onboarding/migrate", data={}).status_code == 403
    assert client.post(
        "/onboarding/migrate",
        data={
            "_csrf_token": csrf,
            "command_id": command,
            "federation_id": "attacker",
        },
    ).status_code == 403
    accepted = client.post(
        "/onboarding/migrate",
        data={"_csrf_token": csrf, "command_id": command},
    )
    assert accepted.status_code == 303
    assert accepted.location == "/federation"
    assert transition.migration_calls == [f"flask-startup-migration-{command}"]
