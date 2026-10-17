from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from flask import Flask

from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.live_failover import StorageFailoverCoordinator
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.flask_app.services import trusted_storage_authority_runtime as runtime


def test_local_disabled_intent_fences_reassigned_builtin_provider(
    tmp_path: Path, monkeypatch
) -> None:
    from catalog.capabilities.contributions import StorageCandidateSource
    from catalog.capabilities.contributions.candidates import (
        ContributionCandidateGenerator,
    )
    from catalog.capabilities.contributions.store import SQLiteContributionIntentStore
    from catalog.federation.errors import FederationValidationError
    from catalog.federation.onboarding_models import (
        ContributionActivationState,
        ContributionDesiredState,
        ContributionPolicyState,
        DeviceInspectionSnapshot,
    )
    from catalog.flask_app.services import (
        federation_storage_authority_install as install,
    )
    from catalog.flask_app.services.local_capability_candidates import (
        CreatorBootstrapStorageContributionAdapter,
        get_local_candidate_bundle,
    )
    from catalog.flask_app.tests.test_storage_authority_control_plane_identity import (
        SESSION,
        _federation,
    )

    async def scenario() -> None:
        coordinator, creator = _federation(tmp_path)
        node_id = creator.identity.node_id
        now = datetime.now(timezone.utc)
        database = tmp_path / "configured-contributions.sqlite3"
        app = Flask(__name__)
        app.config.update(
            CAPABILITY_ONBOARDING_STATE_DATABASE=str(tmp_path / "onboarding.sqlite3"),
            CAPABILITY_ONBOARDING_CONTRIBUTION_DATABASE=str(database),
            CAPABILITY_ONBOARDING_STORAGE_PROBE_DIRECTORY=str(
                tmp_path / "storage-probe"
            ),
        )
        context = SimpleNamespace(
            credentials=creator,
            coordinator=coordinator,
            binding=SimpleNamespace(internal_session_id=SESSION),
        )
        onboarding = SimpleNamespace(authorized_context=lambda: context)
        with app.app_context():
            bundle = get_local_candidate_bundle()
        generator = ContributionCandidateGenerator(
            sources=(
                StorageCandidateSource(
                    {bundle.storage_spec.provider_id: bundle.storage_spec}
                ),
            ),
            now=lambda: now,
        )
        inspection = DeviceInspectionSnapshot(
            device_id=node_id,
            revision=1,
            os_family="test",
            architecture="test",
            resource_observations={},
            detected_services=(bundle.storage_spec.provider_id,),
            registered_handlers=(),
            detected_data_sources=(),
            recommended_benchmark_ids=(),
            warnings=(),
            created_at=now,
            expires_at=now + timedelta(minutes=10),
        )
        candidate = generator.generate(inspection, ())[0].candidate
        adapter = CreatorBootstrapStorageContributionAdapter(
            onboarding, bundle.storage_probe
        )
        store = SQLiteContributionIntentStore(database)

        def persist(desired, outcome):
            return store.transition(
                candidate=candidate,
                desired_state=desired,
                policy_state=ContributionPolicyState.ALLOWED,
                activation_state=outcome.activation_state,
                reason=outcome.reason,
                decided_at=now,
            )

        persist(ContributionDesiredState.ENABLED, adapter.enable(candidate))
        settings = replace(
            _settings(tmp_path),
            session_id=SESSION,
            relay_control_database=coordinator.store.database,
            storage_control_database=coordinator.store.database,
            storage_floor_bytes=0,
        )
        captured = {}

        async def capture_run(_settings, **kwargs):
            captured.update(kwargs)

        monkeypatch.setattr(install, "run_trusted_storage_authority", capture_run)
        monitor = install.FederationStorageAuthorityMonitor(app, onboarding)
        await monitor._run_authority(settings)
        admission = {}
        if "authorize_local_storage_write" in captured:
            admission["authorize_local_storage_write"] = captured[
                "authorize_local_storage_write"
            ]
        control = PhaseDControlPlane(coordinator.store.database)
        client = _LoopbackRelayClient(node_id)
        provider_id = runtime._builtin_local_provider_id(node_id)
        control.grant_leader(
            SESSION,
            node_id,
            "fcp-local-storage",
            provider_id,
            "before-disable",
            1,
            1,
            lease_expires_at=now + timedelta(minutes=10),
            occurred_at=now,
        )
        endpoint = RelayStorageEndpoint(client)
        try:
            service = runtime._ensure_builtin_local_storage_service(
                endpoint=endpoint,
                control=control,
                client=client,
                settings=settings,
                **admission,
            )
            assert service is not None
            await endpoint.start()
            logical = PhaseDLogicalStorageClient(
                session_id=SESSION,
                actor_node_id=node_id,
                control_plane=control,
                transport=endpoint,
                acknowledgements=DurableAcknowledgementStore(
                    settings.acknowledgements_database
                ),
            )

            async def ingest(batch_id):
                return await logical.ingest_batch(
                    group_id="fcp-local-storage",
                    dataset_id="local-contribution",
                    batch_id=batch_id,
                    idempotency_key=batch_id,
                    content={"batch": batch_id},
                    created_at=now,
                )

            assert (await ingest("already-committed")).committed
            disabled = persist(
                ContributionDesiredState.DISABLED, adapter.disable(candidate)
            )
            assert disabled.activation_state is ContributionActivationState.INACTIVE
            assert (
                control.snapshot(SESSION)
                .groups["fcp-local-storage"]
                .primary_provider_id
                is None
            )
            # A new inspection keeps the same normal producer key and cannot forget the disabled choice.
            assert (
                generator.generate(replace(inspection, revision=2), ())[
                    0
                ].candidate.candidate_id
                == candidate.candidate_id
            )
            control.change_assignment(
                SESSION, node_id, "fcp-local-storage", provider_id
            )
            control.grant_leader(
                SESSION,
                node_id,
                "fcp-local-storage",
                provider_id,
                "after-disable",
                2,
                2,
                lease_expires_at=now + timedelta(minutes=10),
                occurred_at=now,
            )
            assert store.get(candidate.candidate_id) == disabled
            assert await logical.read(
                group_id="fcp-local-storage", batch_id="already-committed"
            ) == {"batch": "already-committed"}
            rejected = None
            try:
                accepted = await ingest("must-stay-fenced")
            except FederationValidationError as exc:
                rejected = exc.code
            else:
                assert not accepted.committed, (
                    "Durable local disable must reject a later reassigned new write"
                )
            assert rejected == "unauthorized"
            assert not service.provider.exists(
                session_id=SESSION,
                group_id="fcp-local-storage",
                batch_id="must-stay-fenced",
            )
            assert (
                service.acknowledgements.status(
                    SESSION, "fcp-local-storage", "must-stay-fenced"
                )
                is None
            )
            persist(ContributionDesiredState.ENABLED, adapter.enable(candidate))
            assert (await ingest("must-stay-fenced")).committed
        finally:
            await endpoint.close()
            store.close()

    asyncio.run(scenario())


def test_builtin_disabled_write_fence_covers_catchup_and_preserves_other_provider(
    tmp_path,
):
    from catalog.federation.errors import FederationValidationError
    from catalog.federation.live_catchup import LIVE_CATCHUP_KIND
    from catalog.federation.local_storage import FilesystemBatchStorageProvider
    from catalog.federation.outbox import SQLiteOutbox
    from catalog.federation.phase_d_service import PhaseDStorageService
    from catalog.federation.storage_protocol import (
        BatchIngestRequest,
        StorageOperation,
        StorageRequestEnvelope,
        WriteAuthority,
    )

    async def scenario():
        now = datetime.now(timezone.utc)
        settings = replace(_settings(tmp_path), storage_floor_bytes=0)
        control = PhaseDControlPlane(settings.storage_control_database)
        _register_primary(
            control,
            session_id=settings.session_id,
            provider_id="external-primary",
            node_id="external-node",
            now=now,
        )
        client = _LoopbackRelayClient("local-node")
        provider_id = runtime._builtin_local_provider_id(client.node_id)
        control.register_provider(
            settings.session_id,
            "external-node",
            StorageProviderRegistration(
                session_id=settings.session_id,
                provider_id=provider_id,
                node_id=client.node_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                authorized=True,
                status="ready",
            ),
        )
        enabled = [True]

        def authorize(node_id):
            assert node_id == client.node_id
            if not enabled[0]:
                raise FederationValidationError(
                    "unauthorized", "contribution", "disabled"
                )

        endpoint = RelayStorageEndpoint(client)
        target = runtime._ensure_builtin_local_storage_service(
            endpoint=endpoint,
            control=control,
            client=client,
            settings=settings,
            authorize_local_storage_write=authorize,
        )
        assert target is not None
        target.recovery_authority_node_id = "control-authority"
        external = PhaseDStorageService(
            provider_id="external-primary",
            provider=FilesystemBatchStorageProvider(tmp_path / "external"),
            control_plane=control,
            outbox=SQLiteOutbox(tmp_path / "external-outbox.sqlite3"),
            acknowledgements=DurableAcknowledgementStore(
                tmp_path / "external-acks.sqlite3"
            ),
        )

        async def external_write(batch_id):
            content = {"batch": batch_id}
            request = BatchIngestRequest(
                authority=WriteAuthority(
                    settings.session_id,
                    "fcp-local-storage",
                    "external-node",
                    "grant-1",
                    1,
                    1,
                    now + timedelta(minutes=10),
                ),
                dataset_id="catchup-dataset",
                batch_id=batch_id,
                idempotency_key=batch_id,
                content_hash=BatchIngestRequest.calculate_content_hash(content),
                content=content,
                created_at=now,
            )
            response = await external.dispatch(
                StorageRequestEnvelope(
                    request_id="external-" + batch_id,
                    protocol=STORAGE_PROTOCOL,
                    protocol_version=STORAGE_PROTOCOL_VERSION,
                    operation=StorageOperation.BATCH_INGEST,
                    session_id=settings.session_id,
                    actor_node_id="external-node",
                    authorization_context={
                        "kind": "storage-primary-route",
                        "provider_id": "external-primary",
                        "group_id": "fcp-local-storage",
                    },
                    payload=request.to_dict(),
                )
            )
            assert response.ok
            return request, response.result

        def catchup(request, committed):
            return StorageRequestEnvelope(
                request_id="catchup-" + request.batch_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                operation=StorageOperation.BATCH_INGEST,
                session_id=settings.session_id,
                actor_node_id="control-authority",
                authorization_context={
                    "kind": LIVE_CATCHUP_KIND,
                    "action": "ingest",
                    "provider_id": provider_id,
                    "group_id": "fcp-local-storage",
                    "source_provider_id": "external-primary",
                    "source_node_id": "external-node",
                    "recovery_id": "recovery-one",
                    "manifest_revision": committed["manifest_revision"],
                    "manifest_hash": committed["manifest_hash"],
                    "item_id": request.batch_id,
                    "dataset_id": request.dataset_id,
                    "schema_name": request.dataset_schema_name,
                    "schema_version": request.dataset_schema_version,
                    "idempotency_key": request.idempotency_key,
                    "content_hash": request.content_hash,
                },
                payload=request.to_dict(),
            )

        first, committed = await external_write("already-caught-up")
        assert (await target.dispatch(catchup(first, committed))).ok
        enabled[0] = False
        second, committed = await external_write("new-while-local-disabled")
        response = await target.dispatch(catchup(second, committed))
        assert not response.ok and response.error.code.value == "unauthorized"
        assert not target.provider.exists(
            session_id=settings.session_id,
            group_id="fcp-local-storage",
            batch_id=second.batch_id,
        )
        assert (
            target.provider.read(
                session_id=settings.session_id,
                group_id="fcp-local-storage",
                batch_id=first.batch_id,
            )
            == first.content
        )
        enabled[0] = True
        assert (await target.dispatch(catchup(second, committed))).ok

    asyncio.run(scenario())


def test_local_storage_write_intent_read_is_legacy_compatible_and_fail_closed(tmp_path):
    import sqlite3

    from catalog.federation.errors import FederationValidationError
    from catalog.flask_app.services.federation_storage_authority_install import (
        FederationStorageAuthorityMonitor,
    )

    database = tmp_path / "configured.sqlite3"
    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_CONTRIBUTION_DATABASE"] = str(database)
    monitor = FederationStorageAuthorityMonitor(app, object())
    monitor._authorize_local_storage_write("node-one")  # No durable legacy intent.
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE old_onboarding_state(value TEXT)")
    connection.close()
    monitor._authorize_local_storage_write("node-one")  # Existing older onboarding DB.
    database.write_bytes(b"corrupt-not-sqlite")
    with pytest.raises(FederationValidationError, match="unauthorized"):
        monitor._authorize_local_storage_write("node-one")
    app.config["CAPABILITY_ONBOARDING_CONTRIBUTION_DATABASE"] = str(tmp_path)
    with pytest.raises(FederationValidationError, match="unauthorized"):
        monitor._authorize_local_storage_write("node-one")
class _LoopbackRelayClient:
    """One authenticated node talking to its own provider over relay.message."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self._messages: asyncio.Queue[object] = asyncio.Queue()

    async def send_message(
        self,
        *,
        session_id: str,
        target_node_id: str,
        payload: dict[str, object],
        request_id: str | None = None,
    ) -> dict[str, object]:
        del request_id
        assert target_node_id == self.node_id
        await self._messages.put(
            SimpleNamespace(
                actor_node_id=self.node_id,
                session_id=session_id,
                payload=payload,
            )
        )
        return {"delivered": True}

    async def receive_message(self, *, timeout: float | None = None):
        if timeout is None:
            return await self._messages.get()
        return await asyncio.wait_for(self._messages.get(), timeout=timeout)


def _settings(tmp_path: Path) -> runtime.StorageAuthoritySettings:
    storage_root = tmp_path / "authority-storage"
    return runtime.StorageAuthoritySettings(
        relay_control_database=str(tmp_path / "control.sqlite3"),
        storage_control_database=str(tmp_path / "control.sqlite3"),
        publication_database=str(storage_root / "publication.sqlite3"),
        failover_database=str(storage_root / "failover.sqlite3"),
        acknowledgements_database=str(
            storage_root / "recorder_ingest_acknowledgements.sqlite3"
        ),
        state_dir=str(tmp_path / "state"),
        relay="ws://127.0.0.1:8765",
        display_name="Creator",
        session_id="session-a",
    )


def _register_primary(
    control: PhaseDControlPlane,
    *,
    session_id: str,
    provider_id: str,
    node_id: str,
    now: datetime,
) -> None:
    control.create_group(session_id, node_id, "fcp-local-storage")
    control.register_provider(
        session_id,
        node_id,
        StorageProviderRegistration(
            session_id=session_id,
            provider_id=provider_id,
            node_id=node_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            authorized=True,
            status="ready",
        ),
    )
    control.change_assignment(
        session_id,
        node_id,
        "fcp-local-storage",
        provider_id,
    )
    control.grant_leader(
        session_id,
        node_id,
        "fcp-local-storage",
        provider_id,
        "grant-1",
        1,
        1,
        lease_expires_at=now + timedelta(minutes=10),
        occurred_at=now,
    )


def test_creator_builtin_storage_provider_is_hosted_and_accepts_logical_ingest(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        now = datetime.now(timezone.utc)
        settings = replace(
            _settings(tmp_path), request_timeout=0.02
        )
        client = _LoopbackRelayClient("node-creator")
        control = PhaseDControlPlane(settings.storage_control_database)
        provider_id = runtime._builtin_local_provider_id(client.node_id)
        _register_primary(
            control,
            session_id=settings.session_id,
            provider_id=provider_id,
            node_id=client.node_id,
            now=now,
        )

        endpoint = RelayStorageEndpoint(
            client, request_timeout=settings.storage_request_timeout
        )
        try:
            service = runtime._ensure_builtin_local_storage_service(
                endpoint=endpoint,
                control=control,
                client=client,  # type: ignore[arg-type]
                settings=settings,
            )
            assert service is not None
            assert endpoint.services[provider_id] is service
            original_dispatch = service.dispatch

            async def delayed_dispatch(request):
                await asyncio.sleep(0.05)
                return await original_dispatch(request)

            service.dispatch = delayed_dispatch
            assert settings.request_timeout < 0.05 < settings.storage_request_timeout

            await endpoint.start()
            logical = PhaseDLogicalStorageClient(
                session_id=settings.session_id,
                actor_node_id=client.node_id,
                control_plane=control,
                transport=endpoint,
                acknowledgements=DurableAcknowledgementStore(
                    Path(settings.acknowledgements_database)
                ),
            )
            content = {"schema": "test.batch.v1", "value": 42}
            outcome = await logical.ingest_batch(
                group_id="fcp-local-storage",
                dataset_id="test-dataset",
                batch_id="batch-1",
                idempotency_key="test-dataset:batch-1",
                content=content,
                created_at=now,
            )

            assert outcome.committed is True
            assert service.provider.read(
                session_id=settings.session_id,
                group_id="fcp-local-storage",
                batch_id="batch-1",
            ) == content
        finally:
            await endpoint.close()

    asyncio.run(scenario())


def test_authority_does_not_auto_host_an_arbitrary_registered_provider(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    client = _LoopbackRelayClient("node-creator")
    control = PhaseDControlPlane(settings.storage_control_database)
    now = datetime.now(timezone.utc)
    _register_primary(
        control,
        session_id=settings.session_id,
        provider_id="explicit-external-provider",
        node_id=client.node_id,
        now=now,
    )
    endpoint = RelayStorageEndpoint(client, request_timeout=2.0)

    service = runtime._ensure_builtin_local_storage_service(
        endpoint=endpoint,
        control=control,
        client=client,  # type: ignore[arg-type]
        settings=settings,
    )

    assert service is None
    assert endpoint.services == {}


@pytest.mark.parametrize(
    "condition",
    ["ready", "no-service", "wrong-service", "wrong-node", "unauthorized", "unavailable", "protocol", "unhealthy"],
)
def test_builtin_provider_liveness_requires_the_hosted_authorized_provider(
    tmp_path: Path, monkeypatch, condition: str
) -> None:
    settings = _settings(tmp_path)
    client = _LoopbackRelayClient("node-creator")
    control = PhaseDControlPlane(settings.storage_control_database)
    provider_id = runtime._builtin_local_provider_id(client.node_id)
    _register_primary(
        control, session_id=settings.session_id, provider_id=provider_id,
        node_id=client.node_id, now=datetime.now(timezone.utc),
    )
    endpoint = RelayStorageEndpoint(client)
    service = runtime._ensure_builtin_local_storage_service(
        endpoint=endpoint, control=control, client=client, settings=settings,
    )
    assert service is not None
    registration = control.snapshot(settings.session_id).providers[provider_id]
    changes = {
        "wrong-node": {"node_id": "other-node"},
        "unauthorized": {"authorized": False},
        "unavailable": {"status": "unavailable"},
        "protocol": {"protocol": "other-storage-protocol"},
    }
    if condition in changes:
        control.register_provider(
            settings.session_id, client.node_id, replace(registration, **changes[condition])
        )
    if condition == "no-service":
        service = None
    elif condition == "wrong-service":
        service.provider_id = "some-other-provider"
    elif condition == "unhealthy":
        monkeypatch.setattr(service.provider, "health", lambda: {"status": "unavailable"})
    announcement = runtime._builtin_local_storage_capability(
        service=service, control=control, client=client, settings=settings,
    )
    if condition in {"no-service", "wrong-service"}:
        assert announcement is None
        return
    assert announcement is not None
    assert announcement.type == "storage-provider"
    assert announcement.properties["provider_id"] == provider_id
    assert announcement.status == ("ready" if condition == "ready" else "unavailable")
    status = {
        "nodes": [{"node_id": client.node_id, "connection_state": "connected"}],
        "capabilities": [announcement.to_dict()],
    }
    assert StorageFailoverCoordinator._provider_online(provider_id, client.node_id, status) == (condition == "ready")
    status["capabilities"][0]["type"] = "storage"
    assert not StorageFailoverCoordinator._provider_online(provider_id, client.node_id, status)
    status["capabilities"][0]["type"] = "storage-provider"
    # A disconnected authenticated owner is never made live by its old advert.
    status["nodes"][0]["connection_state"] = "disconnected"
    assert not StorageFailoverCoordinator._provider_online(provider_id, client.node_id, status)
