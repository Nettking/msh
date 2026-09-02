from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from flask import Flask

from catalog.federation.errors import AuthorizationError
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)
from catalog.flask_app import federation_routes
from catalog.flask_app.services import federation_projection_service as composition
from catalog.flask_app.services.capability_onboarding_service import FederationBindingStore

NOW = datetime(2026, 9, 2, 9, 30, tzinfo=timezone.utc)


class _PersistedOnboarding:
    """Real persisted binding with deliberately unavailable live authority."""

    def __init__(self, store: FederationBindingStore, *, node_id: str) -> None:
        self.store = store
        self.node_id = node_id
        self.authorized_calls = 0

    def authorized_context(self) -> object:
        self.authorized_calls += 1
        raise ConnectionError("coordinator unavailable")

    def identity_or_none(self) -> object:
        return SimpleNamespace(identity=SimpleNamespace(node_id=self.node_id))

    def binding_or_none(self) -> FederationSessionBinding | None:
        return self.store.load()


class _RejectedOnboarding(_PersistedOnboarding):
    def authorized_context(self) -> object:
        self.authorized_calls += 1
        raise AuthorizationError(
            "not-session-member",
            "the saved node is no longer a Federation member",
            "node_id",
        )


def _binding_store(tmp_path) -> FederationBindingStore:
    store = FederationBindingStore(tmp_path / "onboarding.sqlite3")
    store.save(
        FederationSessionBinding(
            federation_id="federation-saved",
            internal_session_id="session-private-saved",
            device_id="node-local",
            state=FederationConnectionState.CONNECTED,
            revision=7,
            trusted=True,
            created_at=NOW,
            last_verified_at=NOW,
        )
    )
    return store


def _configure_read_only_projection(app: Flask) -> None:
    app.config["FEDERATION_DEVICE_INSPECTION"] = None
    app.config["FEDERATION_CONTRIBUTION_CANDIDATES"] = ()
    app.config["FEDERATION_CONTRIBUTION_INTENTS"] = ()
    app.config["FEDERATION_AUTHORIZED_BENCHMARK_STORE"] = None


def test_saved_trusted_member_reaches_real_control_plane_outage_surface(
    monkeypatch,
    tmp_path,
) -> None:
    """Coordinator loss must not turn an established member into fresh setup."""

    store = _binding_store(tmp_path)
    onboarding = _PersistedOnboarding(store, node_id="node-local")
    monkeypatch.setattr(
        composition,
        "get_capability_onboarding_service",
        lambda: onboarding,
    )
    monkeypatch.setattr(
        federation_routes,
        "get_capability_onboarding_service",
        lambda: onboarding,
    )

    app = Flask(__name__)
    _configure_read_only_projection(app)
    with app.app_context():
        service = composition.get_federation_projection_service()
        overview = service.overview().to_dict()
        leader_view = federation_routes._leader_authority_view()

    assert onboarding.authorized_calls >= 2
    assert overview["state"] == "degraded"
    assert overview["state_label"] == "Control plane unavailable / reconnecting"
    assert overview["notice"]["title"] == "Federation control plane unavailable"
    assert "saved trusted membership is retained" in overview["notice"]["message"]
    assert "no new setup or member failure" in overview["notice"]["message"]
    assert overview["recommended_action"] is None
    assert overview["control_plane"] == {
        "state": "unavailable",
        "state_label": "Unavailable / reconnecting",
        "membership_retained": True,
        "reason_code": "federation-authority-unavailable",
    }
    assert "session-private-saved" not in str(overview)
    assert "Federation setup is not complete" not in str(overview)
    assert leader_view == (False, False, None)


def test_saved_binding_is_not_reused_for_a_different_local_identity(
    monkeypatch,
    tmp_path,
) -> None:
    """Display continuity is allowed only for the identity that owns the binding."""

    store = _binding_store(tmp_path)
    onboarding = _PersistedOnboarding(store, node_id="node-substituted")
    monkeypatch.setattr(
        composition,
        "get_capability_onboarding_service",
        lambda: onboarding,
    )

    app = Flask(__name__)
    _configure_read_only_projection(app)
    with app.app_context():
        overview = composition.get_federation_projection_service().overview().to_dict()

    assert overview["control_plane"]["membership_retained"] is False
    assert overview["notice"]["title"] == "Federation setup is not complete"
    assert "federation-saved" not in str(overview)
    assert "session-private-saved" not in str(overview)


def test_definitive_membership_rejection_never_uses_saved_outage_fallback(
    monkeypatch,
    tmp_path,
) -> None:
    """A revoked/removed member is not allowed to masquerade as reconnecting."""

    store = _binding_store(tmp_path)
    onboarding = _RejectedOnboarding(store, node_id="node-local")
    monkeypatch.setattr(
        composition,
        "get_capability_onboarding_service",
        lambda: onboarding,
    )

    app = Flask(__name__)
    _configure_read_only_projection(app)
    with app.app_context():
        overview = composition.get_federation_projection_service().overview().to_dict()

    assert onboarding.authorized_calls == 1
    assert overview["control_plane"]["membership_retained"] is False
    assert overview["notice"]["title"] == "Federation setup is not complete"
    assert "federation-saved" not in str(overview)
