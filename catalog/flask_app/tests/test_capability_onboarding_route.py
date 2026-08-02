from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from flask import redirect

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_discovery import (
    ConfiguredFederationDiscoveryAdapter,
    FederationDiscoveryCandidate,
)
from catalog.flask_app.app import create_app
from catalog.flask_app.capability_onboarding_routes import (
    _COMMAND_SESSION_KEY,
    _CSRF_SESSION_KEY,
)
from catalog.flask_app.services.capability_onboarding_service import (
    CapabilityOnboardingService,
)
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 8, 3, 0, 30, tzinfo=timezone.utc)


def _service(
    tmp_path: Path,
    *,
    coordinator: SessionCoordinator | None = None,
    sources=(),
    setup_payload: dict[str, object] | None = None,
) -> CapabilityOnboardingService:
    coordinator = coordinator or SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: NOW,
    )
    return CapabilityOnboardingService(
        identity_directory=tmp_path / "device",
        state_database=tmp_path / "onboarding.sqlite3",
        coordinator_database=coordinator,
        device_name="Workshop laptop",
        discovery_sources=sources,
        setup_loader=lambda: setup_payload or {
            "configured": False,
            "user_setup_complete": False,
        },
        clock=lambda: NOW,
    )


def _app(service: CapabilityOnboardingService):
    app = create_app()
    app.config.update(
        TESTING=True,
        CAPABILITY_ONBOARDING_SERVICE=service,
    )
    return app


def _form_context(client) -> tuple[str, str]:
    client.get("/onboarding")
    with client.session_transaction() as browser:
        return (
            str(browser[_CSRF_SESSION_KEY]),
            str(browser[_COMMAND_SESSION_KEY]),
        )


def _create_identity(client) -> tuple[str, str]:
    csrf, command_id = _form_context(client)
    response = client.post(
        "/onboarding/identity",
        data={"_csrf_token": csrf},
    )
    assert response.status_code == 303
    return csrf, command_id


def _credentials(tmp_path: Path, name: str):
    return IdentityStore(
        tmp_path / f"identity-{name.casefold().replace(' ', '-')}",
        display_name=name,
    ).create(now=NOW)


def _enroll(coordinator: SessionCoordinator, credentials) -> None:
    token = coordinator.create_enrollment_token(
        ttl_seconds=300,
        max_uses=1,
    )["token"]
    coordinator.enroll_node(credentials.identity, token=token)


def _remote_candidate(
    tmp_path: Path,
    *,
    session_id: str = "session-alpha",
    code: str = "ALPHA-42",
):
    database = tmp_path / "coordinator.sqlite3"
    coordinator = SessionCoordinator(database, clock=lambda: NOW)
    owner = _credentials(tmp_path, "Owner")
    _enroll(coordinator, owner)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Workshop federation",
        request_id=f"create-{session_id}",
        session_id=session_id,
    )
    enrollment_token = coordinator.create_enrollment_token(
        ttl_seconds=300,
        max_uses=1,
    )["token"]
    invitation_token = coordinator.create_invitation(
        session_id=session_id,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=300,
        max_uses=1,
        request_id=f"invite-{session_id}",
    )["token"]
    candidate = FederationDiscoveryCandidate(
        federation_label="Workshop federation",
        internal_session_id=session_id,
        federation_fingerprint="fingerprint-workshop",
        transport_kind="configured-relay",
        verification_code=code,
        discovered_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
        enrollment_token=enrollment_token,
        invitation_token=invitation_token,
    )
    source = ConfiguredFederationDiscoveryAdapter((candidate,))
    return (
        database,
        coordinator,
        owner,
        candidate,
        enrollment_token,
        invitation_token,
        source,
    )


def test_app_factory_registers_cfi2_without_replacing_legacy_setup() -> None:
    app = create_app()

    assert "capability_onboarding_web.onboarding" in app.view_functions
    assert "capability_onboarding_web.create_identity" in app.view_functions
    assert "capability_onboarding_web.federation_action" in app.view_functions
    assert "web.startup" in app.view_functions
    assert "server_setup_web" in app.blueprints

    onboarding_rule = next(
        rule
        for rule in app.url_map.iter_rules()
        if rule.endpoint == "capability_onboarding_web.onboarding"
    )
    assert str(onboarding_rule) == "/onboarding"
    assert onboarding_rule.methods == {"GET", "HEAD", "OPTIONS"}


def test_identity_route_bypasses_legacy_gate_and_reopens_stably(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    app = _app(service)

    @app.before_request
    def simulated_legacy_gate():
        return redirect("/startup")

    client = app.test_client()
    first = client.get("/onboarding")
    csrf, _command_id = _form_context(client)
    created = client.post(
        "/onboarding/identity",
        data={"_csrf_token": csrf},
    )
    page = client.get("/onboarding").get_data(as_text=True)
    identity = service.identity_or_none()

    assert first.status_code == 200
    assert created.status_code == 303
    assert identity is not None
    assert identity.identity.node_id in page
    assert "Create stable identity" not in page
    assert first.headers["Cache-Control"] == "no-store"
    assert first.headers["X-Content-Type-Options"] == "nosniff"

    reopened = _service(
        tmp_path,
        coordinator=service.coordinator,
    ).identity_or_none()
    assert reopened is not None
    assert reopened.identity == identity.identity


def test_legacy_migration_preview_never_renders_private_values(
    tmp_path: Path,
) -> None:
    private_url = "http://10.0.0.9:11434"
    private_source = "Mazak=http://192.168.1.40:5000"
    service = _service(
        tmp_path,
        setup_payload={
            "configured": True,
            "user_setup_complete": True,
            "deployment_mode": "full-server",
            "ai_enabled": True,
            "ai_provider_mode": "connected",
            "ai_provider_name": "Private GPU laptop",
            "ai_profile": "workstation-strong",
            "ai_model": "qwen2.5:7b",
            "ollama_base_url": private_url,
            "recorder_sources": private_source,
            "recorder_poll_interval": "0.2",
            "recorder_include_condition": False,
        },
    )
    client = _app(service).test_client()
    _create_identity(client)

    page = client.get("/onboarding").get_data(as_text=True)

    assert "Existing installation found" in page
    assert "Full server" in page
    assert "Workbench access" in page
    assert "Recorder behavior" in page
    assert "Private connection settings remain local" in page
    assert private_url not in page
    assert private_source not in page
    assert "10.0.0.9" not in page
    assert "192.168.1.40" not in page


def test_verified_join_wrong_code_is_side_effect_free_then_replays_idempotently(
    tmp_path: Path,
) -> None:
    (
        database,
        coordinator,
        _owner,
        candidate,
        enrollment_token,
        invitation_token,
        source,
    ) = _remote_candidate(tmp_path)
    service = _service(tmp_path, coordinator=coordinator, sources=(source,))
    client = _app(service).test_client()
    csrf, command_id = _create_identity(client)
    joining = service.identity_or_none()
    assert joining is not None

    discovered_page = client.get(
        "/onboarding?step=federation"
    ).get_data(as_text=True)
    assert candidate.discovery_id in discovered_page
    assert "ALPHA-42" in discovered_page
    assert enrollment_token not in discovered_page
    assert invitation_token not in discovered_page

    wrong = client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "join",
            "discovery_id": candidate.discovery_id,
            "verification_code": "WRONG",
        },
    )
    assert wrong.status_code == 303
    assert coordinator.store.get_node(joining.identity.node_id) is None
    assert coordinator.session_ids_for_node(joining.identity.node_id) == ()
    assert service.binding_or_none() is None

    first = client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "join",
            "discovery_id": candidate.discovery_id,
            "verification_code": "ALPHA-42",
        },
    )
    revision = coordinator.store.get_session("session-alpha").revision
    second = client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "join",
            "discovery_id": candidate.discovery_id,
            "verification_code": "ALPHA-42",
        },
    )

    assert first.status_code == 303
    assert second.status_code == 303
    coordinator.store.require_membership(
        session_id="session-alpha",
        node_id=joining.identity.node_id,
    )
    assert coordinator.store.get_session("session-alpha").revision == revision
    binding = service.binding_or_none()
    assert binding is not None
    assert binding.internal_session_id == "session-alpha"

    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT use_count FROM session_invitations"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM session_join_requests"
        ).fetchone()[0] == 1

    page = client.get("/onboarding").get_data(as_text=True)
    assert "Federation connected" in page
    assert "session-alpha" not in page
    assert enrollment_token not in page
    assert invitation_token not in page


def test_multiple_candidates_require_explicit_selection_through_flask(
    tmp_path: Path,
) -> None:
    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: NOW,
    )
    owner = _credentials(tmp_path, "Owner")
    _enroll(coordinator, owner)
    candidates = []
    for suffix in ("a", "b"):
        session_id = f"session-{suffix}"
        coordinator.create_session(
            actor_node_id=owner.identity.node_id,
            display_name=f"Federation {suffix.upper()}",
            request_id=f"create-{suffix}",
            session_id=session_id,
        )
        candidates.append(
            FederationDiscoveryCandidate(
                federation_label=f"Federation {suffix.upper()}",
                internal_session_id=session_id,
                federation_fingerprint=f"fingerprint-{suffix}",
                transport_kind="configured-relay",
                verification_code=f"CODE-{suffix.upper()}",
                discovered_at=NOW,
                expires_at=NOW + timedelta(minutes=5),
                enrollment_token=coordinator.create_enrollment_token(
                    ttl_seconds=300,
                    max_uses=1,
                )["token"],
                invitation_token=coordinator.create_invitation(
                    session_id=session_id,
                    actor_node_id=owner.identity.node_id,
                    ttl_seconds=300,
                    max_uses=1,
                    request_id=f"invite-{suffix}",
                )["token"],
            )
        )
    service = _service(
        tmp_path,
        coordinator=coordinator,
        sources=(ConfiguredFederationDiscoveryAdapter(candidates),),
    )
    client = _app(service).test_client()
    csrf, command_id = _create_identity(client)
    joining = service.identity_or_none()
    assert joining is not None
    client.get("/onboarding?step=federation")

    response = client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "join",
            "verification_code": "CODE-B",
        },
    )

    assert response.status_code == 303
    assert coordinator.session_ids_for_node(joining.identity.node_id) == ()
    assert service.binding_or_none() is None


def test_no_candidate_local_creation_uses_existing_coordinator_authority(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    client = _app(service).test_client()
    csrf, command_id = _create_identity(client)
    client.get("/onboarding?step=federation")

    response = client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
        },
    )

    assert response.status_code == 303
    binding = service.binding_or_none()
    identity = service.identity_or_none()
    assert binding is not None
    assert identity is not None
    service.coordinator.store.require_membership(
        session_id=binding.internal_session_id,
        node_id=identity.identity.node_id,
    )
    assert binding.trusted is True
    assert binding.state.value == "connected"


def test_returning_startup_reconnect_mints_no_new_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_service = _service(tmp_path)
    first_client = _app(first_service).test_client()
    csrf, command_id = _create_identity(first_client)
    first_client.get("/onboarding?step=federation")
    first_client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
        },
    )
    binding = first_service.binding_or_none()
    assert binding is not None
    revision = first_service.coordinator.store.get_session(
        binding.internal_session_id
    ).revision

    def forbidden(*_args, **_kwargs):
        raise AssertionError("startup reconnect attempted to mint authority")

    reopened = _service(
        tmp_path,
        coordinator=first_service.coordinator,
    )
    monkeypatch.setattr(
        reopened.coordinator,
        "create_enrollment_token",
        forbidden,
    )
    monkeypatch.setattr(reopened.coordinator, "create_invitation", forbidden)
    monkeypatch.setattr(reopened.coordinator, "join_session", forbidden)

    page = _app(reopened).test_client().get(
        "/onboarding"
    ).get_data(as_text=True)

    assert "Federation connected" in page
    assert reopened.coordinator.store.get_session(
        binding.internal_session_id
    ).revision == revision


def test_membership_removal_keeps_binding_but_fails_reconnect_closed(
    tmp_path: Path,
) -> None:
    (
        _database,
        coordinator,
        owner,
        candidate,
        _enrollment_token,
        _invitation_token,
        source,
    ) = _remote_candidate(tmp_path)
    service = _service(tmp_path, coordinator=coordinator, sources=(source,))
    client = _app(service).test_client()
    csrf, command_id = _create_identity(client)
    client.get("/onboarding?step=federation")
    client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "join",
            "discovery_id": candidate.discovery_id,
            "verification_code": "ALPHA-42",
        },
    )
    identity = service.identity_or_none()
    saved = service.binding_or_none()
    assert identity is not None
    assert saved is not None

    coordinator.remove_member(
        session_id=saved.internal_session_id,
        actor_node_id=owner.identity.node_id,
        target_node_id=identity.identity.node_id,
        request_id="remove-device",
        reason="test-removal",
    )

    page = client.get("/onboarding").get_data(as_text=True)

    assert "Reconnection needed" in page
    assert "No new authority was created" in page
    assert service.binding_or_none() == saved
    assert coordinator.session_ids_for_node(identity.identity.node_id) == ()


def test_csrf_and_context_override_are_rejected_before_authority(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    client = _app(service).test_client()
    csrf, command_id = _form_context(client)

    no_csrf = client.post("/onboarding/identity", data={})
    override = client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
            "actor_node_id": "node-attacker",
            "session_id": "session-attacker",
        },
    )

    assert no_csrf.status_code == 403
    assert override.status_code == 403
    assert service.identity_or_none() is None
    assert service.binding_or_none() is None


@pytest.mark.parametrize(
    "path",
    [
        "/onboarding/inspect",
        "/onboarding/benchmarks/run",
        "/onboarding/benchmarks/skip",
        "/onboarding/contributions",
    ],
)
def test_later_product_mutations_remain_blocked(
    tmp_path: Path,
    path: str,
) -> None:
    service = _service(tmp_path)
    client = _app(service).test_client()
    csrf, _command_id = _create_identity(client)

    response = client.post(path, data={"_csrf_token": csrf})

    assert response.status_code == 409
    assert b"remains blocked" in response.data
    assert service.binding_or_none() is None


def test_federation_overview_uses_revalidated_cfi2_context_without_leakage(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    app = _app(service)
    client = app.test_client()
    csrf, command_id = _create_identity(client)
    client.get("/onboarding?step=federation")
    client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
        },
    )
    binding = service.binding_or_none()
    assert binding is not None

    response = client.get(
        "/federation?actor_node_id=node-attacker&session_id=session-attacker",
        follow_redirects=True,
    )
    page = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Workshop laptop federation" in page
    assert "Connected" in page or "Limited status" in page
    assert binding.internal_session_id not in page
    assert "session-attacker" not in page
    assert "node-attacker" not in page
    assert "internal_session_id" not in page


def test_corrupt_onboarding_database_is_not_overwritten_by_safe_render(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    client = _app(service).test_client()
    csrf, command_id = _create_identity(client)
    client.get("/onboarding?step=federation")
    client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
        },
    )
    database = Path(service.binding_store.database)
    database.write_bytes(b"not-a-sqlite-database")
    before = database.read_bytes()

    response = client.get("/onboarding")
    after = database.read_bytes()

    assert response.status_code == 200
    assert before == after
    assert b"onboarding state unavailable" not in response.data.lower()
    assert b"Workshop laptop" in response.data
