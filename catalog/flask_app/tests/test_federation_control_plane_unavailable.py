from __future__ import annotations

import sqlite3
from pathlib import Path

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import AuthorizationError
from catalog.flask_app.app import create_app
from catalog.flask_app.services.capability_onboarding_service import (
    CapabilityOnboardingService,
)


def _service(tmp_path: Path) -> CapabilityOnboardingService:
    coordinator = SessionCoordinator(tmp_path / "coordinator.sqlite3")
    return CapabilityOnboardingService(
        identity_directory=tmp_path / "device",
        state_database=tmp_path / "onboarding.sqlite3",
        coordinator_database=coordinator,
        device_name="Saved member",
    )


def test_real_saved_binding_stays_visible_when_coordinator_is_unavailable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _service(tmp_path)
    credentials = service.create_identity()
    binding = service.authority.create_local(
        credentials.identity,
        display_name="Saved federation",
        request_id="create-saved-federation",
    )
    service.binding_store.save(binding)

    # Load the actual durable binding before taking the authoritative store out
    # of service. This is the production composition's saved-trust path, not a
    # synthetic OnboardingSnapshot fixture.
    saved = service.binding_or_none()
    assert saved is not None
    assert saved.trusted is True
    assert saved.internal_session_id == binding.internal_session_id

    def unavailable(*_args, **_kwargs):
        raise sqlite3.OperationalError("coordinator temporarily unavailable")

    # Keep the membership check from manufacturing a revocation while making
    # the authoritative session/projection read unavailable.
    monkeypatch.setattr(
        service.coordinator.store,
        "require_membership",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(service.coordinator.store, "get_session", unavailable)

    app = create_app()
    app.config.update(
        TESTING=True,
        CAPABILITY_ONBOARDING_SERVICE=service,
        CAPABILITY_ONBOARDING_STATE_DATABASE=service.binding_store.database,
        CAPABILITY_ONBOARDING_TRANSITION_DATABASE=service.binding_store.database,
    )

    response = app.test_client().get("/federation")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Federation control plane unavailable" in html
    assert "Control plane unavailable / reconnecting" in html
    assert "saved trusted membership is retained" in html
    assert "no new setup or member failure was inferred" in html
    assert "Federation setup is not complete" not in html
    assert service.binding_or_none() == saved


def test_definitive_membership_rejection_does_not_retain_outage_projection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _service(tmp_path)
    credentials = service.create_identity()
    binding = service.authority.create_local(
        credentials.identity,
        display_name="Saved federation",
        request_id="create-rejected-federation",
    )
    service.binding_store.save(binding)

    def rejected(*_args, **_kwargs):
        raise AuthorizationError(
            "membership-required",
            "the saved device is no longer a Federation member",
            "node_id",
        )

    monkeypatch.setattr(service.coordinator.store, "require_membership", rejected)

    app = create_app()
    app.config.update(
        TESTING=True,
        CAPABILITY_ONBOARDING_SERVICE=service,
        CAPABILITY_ONBOARDING_STATE_DATABASE=service.binding_store.database,
        CAPABILITY_ONBOARDING_TRANSITION_DATABASE=service.binding_store.database,
    )

    response = app.test_client().get("/federation")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Federation setup is not complete" in html
    assert "Federation control plane unavailable" not in html
