from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from flask import redirect

from catalog.capabilities.operator_surface import (
    ProviderActivationState,
    ProviderOperatorSnapshot,
    ProviderOperatorSurface,
    ProviderOperatorView,
)
from catalog.federation.errors import AuthorizationError
from catalog.flask_app.app import create_app

NOW = datetime(2026, 8, 3, tzinfo=timezone.utc)
PRIVATE_SESSION_ID = "session-private-cfi1"


class _CoordinatorStore:
    def __init__(self) -> None:
        self.session_ids: list[str] = []

    def get_session(self, session_id: str) -> object:
        self.session_ids.append(session_id)
        return SimpleNamespace(
            display_name="Workshop federation",
            state="active",
            revision=2,
        )


class _AuthorizedCoordinator:
    def __init__(self) -> None:
        self.store = _CoordinatorStore()
        self.status_actors: list[str] = []
        self.replay_contexts: list[tuple[str, str]] = []

    def status(self, *, actor_node_id: str, cursor: str | None = None) -> object:
        self.status_actors.append(actor_node_id)
        assert cursor is None
        return {
            "nodes": [
                {
                    "node_id": "node-owner",
                    "display_name": "Owner laptop",
                    "connection_state": "connected",
                    "last_heartbeat": NOW,
                    "endpoint": "http://127.0.0.1:11434",
                },
                {
                    "node_id": "node-ai",
                    "display_name": "AI device",
                    "connection_state": "connected",
                    "credential": "bearer private",
                },
            ],
            "capabilities": [
                {
                    "session_id": PRIVATE_SESSION_ID,
                    "node_id": "node-owner",
                },
                {
                    "session_id": PRIVATE_SESSION_ID,
                    "node_id": "node-ai",
                },
            ],
            "pagination": {"has_more": False, "next_cursor": None},
        }

    def replay_page(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        last_applied_revision: int,
        limit: int,
    ) -> tuple[tuple[object, ...], int]:
        self.replay_contexts.append((session_id, actor_node_id))
        assert last_applied_revision == 0
        assert limit == 1_000
        return (
            (
                SimpleNamespace(
                    revision=1,
                    event_type="node.joined",
                    occurred_at=NOW - timedelta(minutes=2),
                    payload={
                        "node_id": "node-owner",
                        "invitation": "msh_join_private",
                    },
                ),
                SimpleNamespace(
                    revision=2,
                    event_type="node.joined",
                    occurred_at=NOW - timedelta(minutes=1),
                    payload={
                        "node_id": "node-ai",
                        "fence_epoch": "lease-secret",
                    },
                ),
            ),
            2,
        )


class _AuthorizedSurface(ProviderOperatorSurface):
    def __init__(self, coordinator: _AuthorizedCoordinator) -> None:
        self.enrollment = SimpleNamespace(coordinator=coordinator)
        self.view_calls = 0
        self._provider = ProviderOperatorSnapshot(
            session_id=PRIVATE_SESSION_ID,
            capability_id="provider-ai",
            node_id="node-ai",
            capability_type="language-model",
            protocol="msh-ai",
            protocol_version="1.0",
            discovered=True,
            announcement_status="ready",
            announced_at=NOW,
            enrollment_state="approved",
            enrollment_revision=4,
            enrollment_reason_code="approved",
            health_state="current",
            health_reason_code="current",
            provider_status="ready",
            provider_generation=3,
            report_revision=8,
            reported_at=NOW,
            expires_at=NOW + timedelta(minutes=5),
            max_concurrent_jobs=1,
            active_jobs=0,
            queue_depth=0,
            utilization_millis=125,
            activation_state=ProviderActivationState.ELIGIBLE,
            activation_reason_code="remote-ai-eligible",
            inventory_compatible=None,
            can_manage=True,
            allowed_actions=(),
        )

    def view(self) -> ProviderOperatorView:
        self.view_calls += 1
        return ProviderOperatorView(
            session_id=PRIVATE_SESSION_ID,
            actor_node_id="node-owner",
            is_owner=True,
            generated_at=NOW,
            providers=(self._provider,),
        )


class _DeniedSurface(ProviderOperatorSurface):
    def __init__(self) -> None:
        self.enrollment = SimpleNamespace(
            coordinator=SimpleNamespace(
                status=lambda **_kwargs: (_ for _ in ()).throw(
                    AssertionError("authority must not be consulted")
                )
            )
        )

    def view(self) -> ProviderOperatorView:
        raise AuthorizationError(
            "federation-overview-not-authorized",
            "the server-bound actor cannot read this federation",
            "actor_node_id",
        )


class _BenchmarkStore:
    def list_results(self) -> tuple[object, ...]:
        return (
            SimpleNamespace(
                run_id="run-ai-current",
                device_id="node-owner",
                benchmark_id="ai-response",
                target_service_id="provider-ai",
                state="passed",
                recommendation="recommended",
                finished_at=NOW - timedelta(minutes=3),
                expires_at=NOW + timedelta(hours=1),
                metrics={
                    "latency_band": "interactive",
                    "endpoint": "http://127.0.0.1:11434",
                },
                diagnostics=("Completed safely", "C:\\private\\model.bin"),
            ),
        )


class _StorageStore:
    def __init__(self) -> None:
        self.session_ids: list[str] = []

    def snapshot(self, session_id: str) -> object:
        self.session_ids.append(session_id)
        return SimpleNamespace(
            revision=7,
            providers=(
                SimpleNamespace(
                    provider_id="storage-one",
                    node_id="node-owner",
                    protocol="msh-storage",
                    protocol_version="1.0",
                    status="ready",
                    authorized=True,
                    assignable=True,
                    credential="bearer storage-private",
                ),
            ),
            groups=(
                SimpleNamespace(
                    group_id="reports",
                    primary_provider_id="storage-one",
                    replica_provider_ids=(),
                    grant_secret="grant-secret",
                ),
            ),
            leader_grants={
                "reports": SimpleNamespace(
                    lease_secret="lease-secret",
                    fence_epoch=42,
                )
            },
        )


def _job_snapshots() -> tuple[object, ...]:
    return (
        SimpleNamespace(
            job=SimpleNamespace(
                job_id="job-one",
                capability_type="language-model",
                status="running",
                attempts=(),
                endpoint="https://private.example.invalid",
            ),
            ownership=SimpleNamespace(owner_provider_id="provider-ai"),
            updated_at=NOW,
            handler_binding="private-handler-binding",
        ),
    )


def _configured_app(surface: ProviderOperatorSurface | None = None):
    app = create_app()
    app.config.update(TESTING=True)
    if surface is not None:
        app.config["PROVIDER_OPERATOR_SURFACE"] = surface
    return app


def test_app_factory_registers_only_read_only_federation_overview() -> None:
    app = _configured_app()

    assert "federation_web.overview" in app.view_functions
    rule = next(
        rule
        for rule in app.url_map.iter_rules()
        if rule.endpoint == "federation_web.overview"
    )
    assert str(rule) == "/federation"
    assert rule.methods == {"GET", "HEAD", "OPTIONS"}


def test_no_context_renders_safe_empty_state_with_no_store_headers() -> None:
    client = _configured_app().test_client()

    response = client.get("/federation")
    head = client.head("/federation")

    assert response.status_code == 200
    assert head.status_code == 200
    assert head.data == b""
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Pragma"] == "no-cache"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Referrer-Policy"] == "same-origin"
    assert b"Complete federation setup" in response.data
    assert b"federation.css" in response.data


def test_read_only_route_dispatches_before_legacy_startup_gate() -> None:
    app = _configured_app()

    @app.before_request
    def simulated_legacy_gate():
        return redirect("/startup")

    response = app.test_client().get("/federation")

    assert response.status_code == 200
    assert response.headers.get("Location") is None


def test_authorized_composition_uses_only_server_bound_context() -> None:
    coordinator = _AuthorizedCoordinator()
    surface = _AuthorizedSurface(coordinator)
    storage = _StorageStore()
    app = _configured_app(surface)
    app.config.update(
        FEDERATION_DEVICE_INSPECTION=SimpleNamespace(
            device_id="node-owner",
            revision=2,
            expires_at=NOW + timedelta(hours=1),
            os_family="linux",
            architecture="x86-64",
            resource_observations={
                "cpu": {"logical_cores_band": "8-15"},
                "local_path": "/var/lib/msh/private",
            },
            detected_services=("ollama", "http://127.0.0.1:11434"),
            registered_handlers=("image-analysis",),
            detected_data_sources=("mtconnect",),
            warnings=("Inspection completed", "bearer private"),
        ),
        FEDERATION_AUTHORIZED_BENCHMARK_STORE=_BenchmarkStore(),
        FEDERATION_STORAGE_AUTHORITY_STORE=storage,
        FEDERATION_AUTHORIZED_JOB_SNAPSHOT_SUPPLIER=_job_snapshots,
    )

    response = app.test_client().get(
        "/federation?session_id=session-attacker&actor_node_id=node-attacker"
        "&include_technical=1",
        follow_redirects=True,
    )
    page = response.get_data(as_text=True)

    assert response.status_code == 200
    assert surface.view_calls == 1
    assert coordinator.store.session_ids == [PRIVATE_SESSION_ID]
    assert coordinator.status_actors == ["node-owner"]
    assert coordinator.replay_contexts == [(PRIVATE_SESSION_ID, "node-owner")]
    assert storage.session_ids == [PRIVATE_SESSION_ID]
    assert "Workshop federation" in page
    assert "Owner laptop" in page
    assert "AI device" in page
    assert "Local language model" not in page
    assert "provider-ai" not in page
    assert "session-attacker" not in page
    assert "node-attacker" not in page
    assert "Federation ID" not in page
    assert "Federation revision" not in page


def test_authorization_failure_fails_closed_before_authority_reads() -> None:
    app = _configured_app(_DeniedSurface())

    response = app.test_client().get("/federation")

    assert response.status_code == 200
    assert b"federation-overview-not-authorized" not in response.data
    assert b"actor_node_id" not in response.data


def test_rendered_overview_contains_no_private_or_fencing_material() -> None:
    coordinator = _AuthorizedCoordinator()
    app = _configured_app(_AuthorizedSurface(coordinator))
    app.config.update(
        FEDERATION_AUTHORIZED_BENCHMARK_STORE=_BenchmarkStore(),
        FEDERATION_STORAGE_AUTHORITY_STORE=_StorageStore(),
        FEDERATION_AUTHORIZED_JOB_SNAPSHOT_SUPPLIER=_job_snapshots,
    )

    page = app.test_client().get("/federation").get_data(as_text=True).casefold()

    forbidden = (
        PRIVATE_SESSION_ID,
        "session_id",
        "actor_node_id",
        "http://",
        "https://private",
        "127.0.0.1",
        "bearer ",
        "msh_join_",
        "c:\\private",
        "/var/lib/",
        "lease-secret",
        "grant-secret",
        "fence_epoch",
        "fencing",
        "descriptor_fingerprint",
        "handler_binding",
        "private-handler-binding",
    )
    assert not any(value.casefold() in page for value in forbidden)
