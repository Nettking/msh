from __future__ import annotations

import threading
from types import SimpleNamespace

from flask import Flask

from catalog.flask_app.services import (
    capability_contribution_service,
    federation_pairing_install,
)
from catalog.flask_app.services.federation_pairing_install import (
    SavedFederationReconnectMonitor,
)


def _local_service_context():
    binding = SimpleNamespace(
        internal_session_id="session-a",
        device_id="node-owner",
    )
    context = SimpleNamespace(
        binding=binding,
        credentials=SimpleNamespace(
            identity=SimpleNamespace(node_id="node-owner")
        ),
    )

    class RemoteStore:
        @staticmethod
        def load():
            return None

    class Runtime:
        def __init__(self) -> None:
            self.states = []

        def ensure_connected(self, state) -> None:
            self.states.append(state)

    class Service:
        remote_store = RemoteStore()
        relay_runtime = Runtime()

        @staticmethod
        def authorized_context():
            return context

    return binding, context, Service()


def test_local_creator_uses_real_relay_runtime_instead_of_local_ui_override() -> None:
    binding, context, service = _local_service_context()

    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_PAIRING_RELAY_URL"] = "ws://127.0.0.1:8765"
    monitor = SavedFederationReconnectMonitor(app, service)  # type: ignore[arg-type]

    resolved = monitor._connected_state_and_context()

    assert resolved is not None
    state, returned_context = resolved
    assert returned_context is context
    assert state.binding is binding
    assert state.relay_url == "ws://127.0.0.1:8765"
    assert service.relay_runtime.states == [state]


def test_local_relay_override_takes_priority_over_pairing_relay() -> None:
    _binding, _context, service = _local_service_context()
    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_PAIRING_RELAY_URL"] = "ws://127.0.0.1:8765"
    app.config["CAPABILITY_ONBOARDING_LOCAL_RELAY_URL"] = "wss://relay.example.test"
    monitor = SavedFederationReconnectMonitor(app, service)  # type: ignore[arg-type]

    assert monitor._local_relay_url() == "wss://relay.example.test"


def test_publication_waits_for_completed_startup_reconciliation(monkeypatch) -> None:
    _binding, context, service = _local_service_context()
    app = Flask(__name__)
    monitor = SavedFederationReconnectMonitor(app, service)  # type: ignore[arg-type]
    published: list[dict[str, object]] = []
    synchronized: list[tuple[object, object]] = []
    runtime_state = SimpleNamespace()

    monkeypatch.setattr(
        capability_contribution_service,
        "get_capability_contribution_service",
        lambda: object(),
    )
    monkeypatch.setattr(
        federation_pairing_install,
        "publish_local_contributions",
        lambda **kwargs: published.append(kwargs),
    )
    monkeypatch.setattr(
        monitor.ai_bridge,
        "sync",
        lambda state, trusted_context: synchronized.append(
            (state, trusted_context)
        ),
    )

    app.extensions["capability_contribution_startup_reconciled"] = "in-progress"
    monitor._publish_contributions(runtime_state, context)
    monitor._sync_remote_ai(runtime_state, context)

    assert published == []
    assert synchronized == []

    app.extensions["capability_contribution_startup_reconciled"] = True
    monitor._publish_contributions(runtime_state, context)
    monitor._sync_remote_ai(runtime_state, context)

    assert len(published) == 1
    assert synchronized == [(runtime_state, context)]


def test_contribution_refresh_interrupts_periodic_wait_immediately(monkeypatch) -> None:
    class WakeProbe:
        def __init__(self) -> None:
            self.entered = threading.Event()
            self._event = threading.Event()

        def set(self) -> None:
            self._event.set()

        def clear(self) -> None:
            self._event.clear()

        def wait(self, timeout: float | None = None) -> bool:
            self.entered.set()
            return self._event.wait(timeout=timeout)

    _binding, _context, service = _local_service_context()
    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_LOCAL_RELAY_URL"] = "ws://relay:8765"
    app.extensions["capability_contribution_startup_reconciled"] = True
    monitor = SavedFederationReconnectMonitor(app, service)  # type: ignore[arg-type]
    wake = WakeProbe()
    monitor._wake = wake  # type: ignore[assignment]

    published = threading.Event()
    publish_count = [0]

    def publish(_runtime_state, _context) -> None:
        publish_count[0] += 1
        published.set()

    monkeypatch.setattr(monitor, "_publish_contributions", publish)
    monitor.start()
    try:
        assert published.wait(timeout=1.0)
        assert wake.entered.wait(timeout=1.0)
        published.clear()

        monitor.request_contribution_refresh()

        assert published.wait(timeout=1.0)
        assert publish_count[0] >= 2
    finally:
        monitor.stop()


def test_testing_app_does_not_leave_background_federation_monitors(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.chdir(tmp_path)
    app = Flask(__name__)
    federation_pairing_install.install_federation_pairing(app)
    app.config["TESTING"] = True

    reconnect = app.extensions["federation_saved_membership_reconnect"]
    update = app.extensions["federation_update_event_monitor"]
    starts: list[str] = []
    monkeypatch.setattr(reconnect, "start", lambda: starts.append("reconnect"))
    monkeypatch.setattr(update, "start", lambda: starts.append("update"))

    before_request = next(
        handler
        for handler in app.before_request_funcs[None]
        if handler.__name__ == "_start_saved_membership_reconnect"
    )
    before_request()

    assert starts == []
    assert app.extensions["capability_onboarding_startup_checked"] is True
