from __future__ import annotations

from types import SimpleNamespace

import pytest
from flask import Flask

from catalog.federation.errors import FederationOperationError
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.flask_app.services.recorder_federation_publication_install import (
    install_recorder_federation_publication,
)


class _Onboarding:
    def __init__(self, context: object | None) -> None:
        self.context = context

    def authorized_context(self):
        return self.context


class _Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def ingest_batch(self, **kwargs):
        self.calls.append(dict(kwargs))
        return PhaseDIngestOutcome(committed=True)


def _context():
    return SimpleNamespace(
        binding=SimpleNamespace(internal_session_id="session-1"),
        credentials=SimpleNamespace(
            identity=SimpleNamespace(node_id="recorder-node-1")
        ),
    )


def _configured_app(tmp_path, *, context=None):
    app = Flask(__name__)
    onboarding = _Onboarding(_context() if context is None else context)
    monitor = install_recorder_federation_publication(
        app,
        onboarding_service=onboarding,
    )
    app.config.update(
        RECORDER_FEDERATION_PUBLICATION_ENABLED=True,
        RECORDER_FEDERATION_STORAGE_GROUP_ID="telemetry-storage",
        RECORDER_FEDERATION_DATA_DIRECTORY=str(tmp_path / "data"),
        RECORDER_FEDERATION_CHECKPOINT_FILE=str(
            tmp_path / "data" / "source_state" / "mtconnect_recorder_state.json"
        ),
        RECORDER_FEDERATION_OUTBOX_DATABASE=str(tmp_path / "publication.sqlite3"),
        RECORDER_FEDERATION_POLL_INTERVAL_SECONDS=0.2,
        RECORDER_FEDERATION_DELIVERY_LIMIT=10,
    )
    return app, monitor


def test_publication_is_disabled_without_explicit_consent(tmp_path):
    app = Flask(__name__)
    monitor = install_recorder_federation_publication(
        app,
        onboarding_service=_Onboarding(_context()),
    )

    monitor.start()

    snapshot = monitor.snapshot()
    assert snapshot.status == "disabled"
    assert snapshot.enabled is False
    assert app.extensions["recorder_federation_publication"] is monitor


def test_enabled_publication_requires_explicit_logical_storage_target(tmp_path):
    app, monitor = _configured_app(tmp_path)
    app.config["RECORDER_FEDERATION_STORAGE_GROUP_ID"] = ""
    app.config["RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY"] = lambda _context: _Client()

    monitor.start()

    snapshot = monitor.snapshot()
    assert snapshot.status == "not-configured"
    assert snapshot.enabled is True
    assert snapshot.last_error_code == "recorder-publication-target-required"


def test_worker_composition_uses_authorized_session_node_and_client_factory(tmp_path):
    context = _context()
    app, monitor = _configured_app(tmp_path, context=context)
    client = _Client()
    seen_contexts: list[object] = []

    def factory(value):
        seen_contexts.append(value)
        return client

    app.config["RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY"] = factory

    with app.app_context():
        worker = monitor.build_worker()

    assert seen_contexts == [context]
    assert worker.reconciler.target.session_id == "session-1"
    assert worker.reconciler.target.recorder_node_id == "recorder-node-1"
    assert worker.reconciler.target.group_id == "telemetry-storage"
    assert worker.reconciler.target.dataset_id("Mazak") == (
        "mtconnect:recorder-node-1:Mazak"
    )

    result = monitor.run_once()
    assert result.checkpoint_changed is True
    assert result.reconcile is not None
    assert result.reconcile.enqueued == 0
    assert result.delivery.attempted == 0


def test_worker_does_not_compose_without_trusted_federation_context(tmp_path):
    app, monitor = _configured_app(tmp_path, context=False)
    monitor.onboarding_service = _Onboarding(None)
    app.config["RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY"] = lambda _context: _Client()

    with app.app_context(), pytest.raises(FederationOperationError) as excinfo:
        monitor.build_worker()

    assert excinfo.value.code == "recorder-publication-federation-required"
