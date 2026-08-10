from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.flask_app import federation_routes as routes
from catalog.flask_app.app import create_app

TARGET = "a" * 40
CHECKED_AT = "2026-08-10T17:48:33.008475Z"
HERE = Path(__file__).resolve().parent
STATIC = HERE.parent / "static"


class _UpdateService:
    def __init__(self, snapshot: dict[str, object]) -> None:
        self._snapshot = snapshot

    def snapshot(self) -> dict[str, object]:
        return dict(self._snapshot)


class _Store:
    def __init__(self, creator: str) -> None:
        self.creator = creator

    def get_session(self, _session_id: str) -> object:
        return SimpleNamespace(created_by_node_id=self.creator)


def _onboarding(*, actor: str, creator: str) -> object:
    context = SimpleNamespace(
        coordinator=SimpleNamespace(store=_Store(creator)),
        binding=SimpleNamespace(internal_session_id="session-one"),
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=actor)),
    )
    return SimpleNamespace(authorized_context=lambda: context)


def _app() -> object:
    app = create_app()
    app.config.update(TESTING=True)
    return app


def test_update_authority_view_distinguishes_member_and_coordinator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        routes,
        "get_capability_onboarding_service",
        lambda: _onboarding(actor="node-remote", creator="node-owner"),
    )
    assert routes._update_authority_view() == (True, False)

    monkeypatch.setattr(
        routes,
        "get_capability_onboarding_service",
        lambda: _onboarding(actor="node-owner", creator="node-owner"),
    )
    assert routes._update_authority_view() == (True, True)


def test_remote_member_sees_coordinator_managed_status_without_controls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        routes,
        "get_federation_update_service",
        lambda: _UpdateService(
            {
                "status": "not_checked",
                "branch": "main",
                "devices": [],
                "eligible_count": 0,
            }
        ),
    )
    monkeypatch.setattr(
        routes,
        "get_capability_onboarding_service",
        lambda: _onboarding(actor="node-remote", creator="node-owner"),
    )

    page = _app().test_client().get("/federation").get_data(as_text=True)

    assert "Managed by Federation coordinator" in page
    assert "Update authority" in page
    assert "Federation coordinator" in page
    assert "Not checked yet" not in page
    assert "Check for updates" not in page
    assert "Update all devices" not in page


def test_coordinator_renders_machine_timestamp_for_browser_localization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        routes,
        "get_federation_update_service",
        lambda: _UpdateService(
            {
                "status": "up_to_date",
                "checked_at": CHECKED_AT,
                "branch": "main",
                "target_commit": TARGET,
                "devices": [],
                "eligible_count": 0,
            }
        ),
    )
    monkeypatch.setattr(
        routes,
        "get_capability_onboarding_service",
        lambda: _onboarding(actor="node-owner", creator="node-owner"),
    )

    page = _app().test_client().get("/federation").get_data(as_text=True)

    assert "Check for updates" in page
    assert f'datetime="{CHECKED_AT}"' in page
    assert "data-local-datetime" in page
    assert "federation-local-time.js" in page


def test_local_datetime_script_uses_browser_locale_and_preserves_utc_source() -> None:
    javascript = (STATIC / "js" / "federation-local-time.js").read_text(
        encoding="utf-8"
    )

    assert "Intl.DateTimeFormat(undefined" in javascript
    assert 'time[data-local-datetime]' in javascript
    assert "new Date(raw)" in javascript
    assert "UTC source time" in javascript
