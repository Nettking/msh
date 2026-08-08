from __future__ import annotations

from types import SimpleNamespace

from flask import Flask

from catalog.flask_app.services.federation_pairing_install import (
    SavedFederationReconnectMonitor,
)


def test_local_creator_uses_real_relay_runtime_instead_of_local_ui_override() -> None:
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

    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_LOCAL_RELAY_URL"] = "ws://relay:8765"
    service = Service()
    monitor = SavedFederationReconnectMonitor(app, service)  # type: ignore[arg-type]

    resolved = monitor._connected_state_and_context()

    assert resolved is not None
    state, returned_context = resolved
    assert returned_context is context
    assert state.binding is binding
    assert state.relay_url == "ws://relay:8765"
    assert service.relay_runtime.states == [state]
