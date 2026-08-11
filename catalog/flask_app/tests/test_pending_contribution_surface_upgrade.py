from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from flask import Flask

from catalog.capabilities.operator_surface import ProviderOperatorSurface
from catalog.federation.coordinator import SessionCoordinator
from catalog.flask_app.services.pending_contribution_approval import (
    CapabilityFirstProviderOperatorSurface,
    get_local_provider_operator_surface,
)
from catalog.node.identity import IdentityStore
from catalog.relay.provider_service import build_provider_authorities

NOW = datetime(2026, 8, 11, 9, tzinfo=timezone.utc)
SESSION = "session-surface-upgrade"


def test_existing_generic_owner_surface_is_upgraded_without_changing_authority(
    tmp_path,
) -> None:
    coordinator = SessionCoordinator(tmp_path / "control.sqlite3", clock=lambda: NOW)
    owner = IdentityStore(
        tmp_path / "identity",
        display_name="Owner",
    ).create(now=NOW)
    token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    coordinator.enroll_node(owner.identity, token=token)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Surface upgrade",
        request_id="create-surface-upgrade",
        session_id=SESSION,
    )

    enrollment, health = build_provider_authorities(coordinator)
    generic = ProviderOperatorSurface(
        enrollment,
        health,
        session_id=SESSION,
        actor_node_id=owner.identity.node_id,
        clock=lambda: NOW,
    )

    class Onboarding:
        _clock = staticmethod(lambda: NOW)

        def authorized_context(self):
            return SimpleNamespace(
                credentials=owner,
                binding=SimpleNamespace(internal_session_id=SESSION),
                coordinator=coordinator,
            )

    app = Flask(__name__)
    app.config.update(
        CAPABILITY_ONBOARDING_SERVICE=Onboarding(),
        CAPABILITY_ONBOARDING_STATE_DATABASE=(tmp_path / "onboarding.sqlite3"),
        PROVIDER_OPERATOR_SURFACE=generic,
    )

    with app.app_context():
        upgraded = get_local_provider_operator_surface()
        configured = app.config["PROVIDER_OPERATOR_SURFACE"]

    assert isinstance(upgraded, CapabilityFirstProviderOperatorSurface)
    assert configured is upgraded
    assert upgraded is not generic
    assert upgraded.enrollment.coordinator is coordinator
    assert upgraded.enrollment.store.database == generic.enrollment.store.database
    assert upgraded.health.store.database == generic.health.store.database
    assert upgraded.session_id == generic.session_id
    assert upgraded.actor_node_id == generic.actor_node_id
