from __future__ import annotations

import sqlite3
from pathlib import Path

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import AuthorizationError
from catalog.flask_app.app import create_app
from catalog.flask_app.services import federation_projection_service as composition
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


def _configured_app(service: CapabilityOnboardingService):
    app = create_app()
    app.config.update(
        TESTING=True,
        CAPABILITY_ONBOARDING_SERVICE=service,
        CAPABILITY_ONBOARDING_STATE_DATABASE=service.binding_store.database,
        CAPABILITY_ONBOARDING_TRANSITION_DATABASE=service.binding_store.database,
    )
    return app


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

    # The retained binding is display continuity only. Prove the production
    # composition discards the coordinator before storage/job authority is
    # composed, rather than merely relying on downstream reads to fail later.
    observed_storage_coordinators: list[object | None] = []
    real_storage_adapter = composition._storage_adapter

    def capture_storage_adapter(session_id: str, coordinator: object | None):
        observed_storage_coordinators.append(coordinator)
        return real_storage_adapter(session_id, coordinator)

    monkeypatch.setattr(composition, "_storage_adapter", capture_storage_adapter)

    def forbidden_job_adapter(_session_id: str):
        raise AssertionError("retained membership must not compose job authority")

    monkeypatch.setattr(composition, "_job_adapter", forbidden_job_adapter)

    app = _configured_app(service)
    response = app.test_client().get("/federation")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Federation control plane unavailable" in html
    assert "Control plane unavailable / reconnecting" in html
    assert "saved trusted membership is retained" in html
    assert "no new setup or member failure was inferred" in html
    assert "Federation setup is not complete" not in html
    assert service.binding_or_none() == saved
    assert observed_storage_coordinators
    assert all(coordinator is None for coordinator in observed_storage_coordinators)


def test_definitive_membership_rejection_does_not_retain_outage_projection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _service(tmp_path)
    credentials = service.create_identity()
    binding = service.authority.create_local(
        credentials.identity,
        display_name="Revoked federation",
        request_id="create-revoked-federation",
    )
    service.binding_store.save(binding)

    def rejected(*_args, **_kwargs):
        raise AuthorizationError(
            "membership-required",
            "the saved device is no longer a Federation member",
            "node_id",
        )

    monkeypatch.setattr(service.authority, "reconnect", rejected)

    app = _configured_app(service)
    response = app.test_client().get("/federation")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Federation control plane unavailable" not in html
    assert "saved trusted membership is retained" not in html
    assert "Federation setup is not complete" in html
