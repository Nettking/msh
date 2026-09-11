"""Public routing survives relay loss without creating or refreshing authority."""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from dataclasses import replace

import pytest

from catalog.federation.onboarding_models import FederationConnectionState
from catalog.flask_app import federation_pairing_routes as routes
from catalog.flask_app.services.federation_pairing_install import _build_service
from catalog.flask_app.services.federation_pairing_service import RemotePairingState
from catalog.flask_app.tests.test_c03_pairing_onboarding import _app_config


@pytest.fixture
def configured(tmp_path):
    app = _app_config(
        tmp_path, tmp_path / "identity", tmp_path / "control.db", "ws://127.0.0.1:1"
    )
    app.config["FCP_REPLICATED_CONTROL_PLANE_CONFIG"] = ""
    bootstrap = _build_service(app)
    bootstrap.create_identity()
    bootstrap.connect(request_id="local-discovery-fixture")
    app.config["FCP_REPLICATED_CONTROL_PLANE_CONFIG"] = "configured"
    service = _build_service(app)
    app.config["CAPABILITY_ONBOARDING_SERVICE"] = service
    return app, service


def _get(app):
    with app.test_request_context("/onboarding/federation/discovery.json"):
        return routes._discovery_response()


def test_discovery_uses_only_existing_local_metadata(configured, monkeypatch):
    app, service = configured
    with closing(sqlite3.connect(service._coordinator_database)) as db:
        before = tuple(db.iterdump())

    def forbidden():
        pytest.fail("public discovery must not request fresh authority")

    monkeypatch.setattr(service, "authorized_context", forbidden)
    response = _get(app)
    assert response.status_code == 200
    assert response.get_json()["pairing_required"] is True
    assert set(response.get_json()) == {
        "schema",
        "federation_label",
        "federation_fingerprint",
        "device_name",
        "relay_port",
        "pairing_required",
        "auto_join_port",
    }
    assert service.relay_runtime._loop is None
    assert service._coordinator is None
    with closing(sqlite3.connect(service._coordinator_database)) as db:
        assert tuple(db.iterdump()) == before


@pytest.mark.parametrize(
    "problem", ["untrusted", "revoked", "wrong-device", "wrong-federation"]
)
def test_invalid_retained_binding_does_not_advertise(configured, problem):
    app, service = configured
    binding = service.binding_store.load()
    edits = {
        "untrusted": {"trusted": False, "state": FederationConnectionState.UNAVAILABLE},
        "revoked": {"state": FederationConnectionState.REVOKED},
        "wrong-device": {"device_id": "another-device"},
        "wrong-federation": {"federation_id": "another-federation"},
    }
    # Inject invalid retained input only into this fixture's database. The
    # supported save API deliberately refuses replacing a device/Federation.
    with closing(sqlite3.connect(service.binding_store.database)) as db:
        db.execute(
            "UPDATE federation_binding SET binding_json=?",
            (json.dumps(replace(binding, **edits[problem]).to_dict()),),
        )
        db.commit()
    assert _get(app).status_code == 404
    assert service.relay_runtime._loop is None


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE session_memberships SET removed_at='2026-09-11T00:00:00Z'",
        "UPDATE nodes SET revoked_at='2026-09-11T00:00:00Z'",
        "UPDATE nodes SET public_key='different-key'",
        "DELETE FROM sessions",
    ],
)
def test_missing_or_revoked_local_membership_does_not_advertise(configured, statement):
    app, service = configured
    with closing(sqlite3.connect(service._coordinator_database)) as db:
        db.execute(statement)
        db.commit()
    assert _get(app).status_code == 404


def test_remote_member_still_does_not_advertise(configured):
    app, service = configured
    service.remote_store.save(
        RemotePairingState("ws://127.0.0.1:1", service.binding_store.load())
    )
    assert _get(app).status_code == 404
    assert service.relay_runtime._loop is None


def test_missing_projection_is_not_created(configured, tmp_path):
    app, service = configured
    absent = tmp_path / "never-created.sqlite3"
    service._coordinator_database = absent
    assert _get(app).status_code == 404
    assert not absent.exists()
    assert service._coordinator is None


def test_binding_lock_wait_is_bounded(configured):
    app, service = configured
    with closing(sqlite3.connect(service.binding_store.database)) as blocker:
        blocker.execute("PRAGMA journal_mode=DELETE")
        blocker.execute("BEGIN EXCLUSIVE")
        begin = time.monotonic()
        response = _get(app)
        elapsed = time.monotonic() - begin
    assert response.status_code == 404
    assert elapsed < 0.7
