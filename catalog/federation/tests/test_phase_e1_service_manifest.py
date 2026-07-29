from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.acknowledgement import (
    AcknowledgementMode,
    AcknowledgementPolicy,
)
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.errors import FederationValidationError
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.phase_d_service import PhaseDStorageService
from catalog.federation.relay_storage import RELAY_STORAGE_KIND, RelayStorageEndpoint
from catalog.federation.replication import REPLICATION_SCHEMA
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    BatchIngestState,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
    WriteAuthority,
)

NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)
SESSION_ID = "session-e1"
GROUP_ID = "storage-main"
PRIMARY_ID = "provider-primary"
PRIMARY_NODE_ID = "node-primary"
REPLICA_ID = "provider-replica"
REPLICA_NODE_ID = "node-replica"


@dataclass(frozen=True)
class Runtime:
    control_database: Path
    provider_root: Path
    outbox_database: Path
    acknowledgement_database: Path
    control: PhaseDControlPlane
    provider: FilesystemBatchStorageProvider
    outbox: SQLiteOutbox
    acknowledgements: DurableAcknowledgementStore
    service: PhaseDStorageService


class UnavailableTransport:
    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        del target_node_id, envelope
        raise TimeoutError("replica unavailable")


class IncorrectIdentityTransport:
    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        assert target_node_id == REPLICA_NODE_ID
        request = BatchIngestRequest.from_dict(envelope.payload)
        result = BatchIngestResult(
            batch_id=f"{request.batch_id}-spoofed",
            idempotency_key=request.idempotency_key,
            content_hash=request.content_hash,
            state=BatchIngestState.STORED,
        )
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            ok=True,
            result=result.to_dict(),
        )


class CorrectIdentityTransport:
    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        assert target_node_id == REPLICA_NODE_ID
        request = BatchIngestRequest.from_dict(envelope.payload)
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            ok=True,
            result=BatchIngestResult(
                batch_id=request.batch_id,
                idempotency_key=request.idempotency_key,
                content_hash=request.content_hash,
                state=BatchIngestState.STORED,
            ).to_dict(),
        )


class CurrentTermTransport:
    def __init__(self, current_term: int) -> None:
        self.current_term = current_term
        self.attempted_terms: list[int] = []

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        assert target_node_id == REPLICA_NODE_ID
        request = BatchIngestRequest.from_dict(envelope.payload)
        self.attempted_terms.append(request.authority.term)
        if request.authority.term != self.current_term:
            raise FederationValidationError(
                "stale-term",
                "authority.term",
                "replica rejected an obsolete primary term",
            )
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            ok=True,
            result=BatchIngestResult(
                batch_id=request.batch_id,
                idempotency_key=request.idempotency_key,
                content_hash=request.content_hash,
                state=BatchIngestState.STORED,
            ).to_dict(),
        )


class UnmanifestedSuccessTransport:
    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        assert target_node_id == PRIMARY_NODE_ID
        request = BatchIngestRequest.from_dict(envelope.payload)
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            ok=True,
            result=BatchIngestResult(
                batch_id=request.batch_id,
                idempotency_key=request.idempotency_key,
                content_hash=request.content_hash,
                state=BatchIngestState.STORED,
            ).to_dict(),
        )


class QueueRelayClient:
    node_id = "recorder-node"

    def __init__(self) -> None:
        self.messages: asyncio.Queue[object] = asyncio.Queue()

    async def send_message(self, **_kwargs: object) -> dict[str, bool]:
        return {"delivered": True}

    async def receive_message(self, *, timeout: float | None = None) -> object:
        del timeout
        return await self.messages.get()


def _registration(
    provider_id: str,
    node_id: str,
) -> StorageProviderRegistration:
    return StorageProviderRegistration(
        session_id=SESSION_ID,
        provider_id=provider_id,
        node_id=node_id,
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        authorized=True,
        status="ready",
    )


def _runtime(
    tmp_path: Path,
    *,
    acknowledgement_mode: AcknowledgementMode,
    with_replica: bool = False,
    transport: object | None = None,
) -> Runtime:
    control_database = tmp_path / "control.sqlite3"
    provider_root = tmp_path / "primary-storage"
    outbox_database = tmp_path / "primary-outbox.sqlite3"
    acknowledgement_database = tmp_path / "primary-acks.sqlite3"

    control = PhaseDControlPlane(control_database)
    control.create_group(SESSION_ID, "coordinator", GROUP_ID)
    control.register_provider(
        SESSION_ID,
        "coordinator",
        _registration(PRIMARY_ID, PRIMARY_NODE_ID),
    )
    replicas: tuple[str, ...] = ()
    if with_replica:
        control.register_provider(
            SESSION_ID,
            "coordinator",
            _registration(REPLICA_ID, REPLICA_NODE_ID),
        )
        replicas = (REPLICA_ID,)
    control.change_assignment(
        SESSION_ID,
        "coordinator",
        GROUP_ID,
        PRIMARY_ID,
        replicas,
    )
    control.set_acknowledgement_policy(
        SESSION_ID,
        GROUP_ID,
        acknowledgement_mode,
    )
    control.grant_leader(
        SESSION_ID,
        "coordinator",
        GROUP_ID,
        PRIMARY_ID,
        "grant-1",
        1,
        10,
        lease_expires_at=NOW + timedelta(minutes=10),
        occurred_at=NOW,
    )

    provider = FilesystemBatchStorageProvider(provider_root)
    outbox = SQLiteOutbox(outbox_database)
    acknowledgements = DurableAcknowledgementStore(acknowledgement_database)
    service = PhaseDStorageService(
        provider_id=PRIMARY_ID,
        provider=provider,
        control_plane=control,
        outbox=outbox,
        acknowledgements=acknowledgements,
        replication_transport=transport,
        clock=lambda: NOW,
    )
    return Runtime(
        control_database,
        provider_root,
        outbox_database,
        acknowledgement_database,
        control,
        provider,
        outbox,
        acknowledgements,
        service,
    )


def _request() -> BatchIngestRequest:
    content = {
        "source": "recorder-a",
        "observations": [{"sequence": 1, "value": 42.0}],
    }
    return BatchIngestRequest(
        authority=WriteAuthority(
            session_id=SESSION_ID,
            group_id=GROUP_ID,
            actor_node_id=PRIMARY_NODE_ID,
            grant_id="grant-1",
            term=1,
            fencing_token=10,
            lease_expires_at=NOW + timedelta(minutes=10),
        ),
        dataset_id="telemetry",
        batch_id="batch-1",
        idempotency_key="recorder-a:1",
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        content=content,
        created_at=NOW,
        dataset_schema_name="msh.telemetry.observations",
        dataset_schema_version=3,
    )


def _envelope(
    request: BatchIngestRequest,
    *,
    request_id: str,
) -> StorageRequestEnvelope:
    return StorageRequestEnvelope(
        request_id=request_id,
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        operation=StorageOperation.BATCH_INGEST,
        session_id=SESSION_ID,
        actor_node_id="recorder-node",
        authorization_context={
            "kind": "storage-primary-route",
            "group_id": GROUP_ID,
            "provider_id": PRIMARY_ID,
        },
        payload=request.to_dict(),
    )


def _dispatch(
    service: PhaseDStorageService,
    request: BatchIngestRequest,
    *,
    request_id: str,
) -> StorageResponseEnvelope:
    return asyncio.run(
        service.dispatch(_envelope(request, request_id=request_id))
    )


def test_primary_policy_publishes_manifest_and_duplicate_keeps_revision(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.PRIMARY,
    )
    request = _request()

    first = _dispatch(runtime.service, request, request_id="request-1")

    assert first.ok
    assert first.result is not None
    manifest = runtime.control.manifest(SESSION_ID, GROUP_ID)
    assert first.result["commit_state"] == "committed"
    assert first.result["manifest_revision"] == manifest.revision == 1
    assert first.result["manifest_hash"] == manifest.manifest_hash
    assert tuple(item.item_id for item in manifest.items) == ("batch-1",)
    assert manifest.datasets[0].schema_name == "msh.telemetry.observations"
    assert manifest.datasets[0].schema_version == 3
    assert manifest.items[0].acknowledged_provider_ids == (PRIMARY_ID,)
    assert runtime.outbox.pending() == ()

    duplicate = _dispatch(runtime.service, request, request_id="request-2")

    assert duplicate.ok
    assert duplicate.result is not None
    assert duplicate.result["state"] == BatchIngestState.ALREADY_STORED.value
    assert duplicate.result["manifest_revision"] == 1
    assert runtime.control.manifest(SESSION_ID, GROUP_ID) == manifest
    assert len(runtime.control.manifest_history(SESSION_ID, GROUP_ID)) == 2


def test_one_replica_unavailable_keeps_genesis_and_manifest_intent_pending(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.ONE_REPLICA,
        with_replica=True,
        transport=UnavailableTransport(),
    )
    request = _request()

    response = _dispatch(runtime.service, request, request_id="request-1")

    assert not response.ok
    assert response.error is not None and response.error.retryable
    assert runtime.provider.exists(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=request.batch_id,
    )
    assert runtime.control.manifest(SESSION_ID, GROUP_ID).revision == 0
    pending = runtime.outbox.pending()
    manifest_intents = runtime.control.pending_batch_manifest_intents(
        primary_provider_id=PRIMARY_ID,
    )
    assert len(manifest_intents) == 1
    assert manifest_intents[0].item_id == request.batch_id
    replication = [
        entry for entry in pending if entry.schema_id == REPLICATION_SCHEMA
    ]
    assert len(replication) == 1
    assert replication[0].attempt_count == 1
    status = runtime.acknowledgements.status(
        SESSION_ID,
        GROUP_ID,
        request.batch_id,
    )
    assert status is not None
    assert status.primary_committed
    assert not status.committed
    assert status.acknowledged_replica_ids == ()


def test_restart_reconciles_provider_durable_primary_only_manifest(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.PRIMARY,
    )
    request = _request()

    def crash_before_manifest(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("simulated crash before manifest publication")

    runtime.control.commit_batch_manifest = crash_before_manifest  # type: ignore[method-assign]
    failed = _dispatch(runtime.service, request, request_id="request-1")

    assert not failed.ok
    assert runtime.provider.exists(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=request.batch_id,
    )
    assert PhaseDControlPlane(runtime.control_database).manifest(
        SESSION_ID,
        GROUP_ID,
    ).revision == 0
    assert runtime.outbox.pending() == ()
    assert len(
        runtime.control.pending_batch_manifest_intents(
            primary_provider_id=PRIMARY_ID,
        )
    ) == 1

    restarted_control = PhaseDControlPlane(runtime.control_database)
    restarted_service = PhaseDStorageService(
        provider_id=PRIMARY_ID,
        provider=FilesystemBatchStorageProvider(runtime.provider_root),
        control_plane=restarted_control,
        outbox=SQLiteOutbox(runtime.outbox_database),
        acknowledgements=DurableAcknowledgementStore(
            runtime.acknowledgement_database
        ),
        clock=lambda: NOW,
    )

    assert restarted_service.reconcile_prepared() == 1
    manifest = restarted_control.manifest(SESSION_ID, GROUP_ID)
    assert manifest.revision == 1
    assert tuple(item.item_id for item in manifest.items) == (request.batch_id,)
    assert SQLiteOutbox(runtime.outbox_database).pending() == ()
    assert (
        restarted_control.pending_batch_manifest_intents(
            primary_provider_id=PRIMARY_ID,
        )
        == ()
    )
    assert restarted_service.reconcile_prepared() == 0
    assert restarted_control.manifest(SESSION_ID, GROUP_ID).revision == 1


def test_incorrect_replica_result_is_never_acknowledged_or_manifested(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.ONE_REPLICA,
        with_replica=True,
        transport=IncorrectIdentityTransport(),
    )
    request = _request()

    response = _dispatch(runtime.service, request, request_id="request-1")

    assert not response.ok
    assert runtime.control.manifest(SESSION_ID, GROUP_ID).revision == 0
    status = runtime.acknowledgements.status(
        SESSION_ID,
        GROUP_ID,
        request.batch_id,
    )
    assert status is not None
    assert not status.committed
    assert status.acknowledged_replica_ids == ()
    replication = [
        entry
        for entry in runtime.outbox.pending()
        if entry.schema_id == REPLICATION_SCHEMA
    ]
    assert len(replication) == 1
    assert replication[0].attempt_count == 1
    assert replication[0].last_error is not None
    assert "replica response does not match" in replication[0].last_error
    assert len(
        runtime.control.pending_batch_manifest_intents(
            primary_provider_id=PRIMARY_ID,
        )
    ) == 1


def test_recovery_never_materializes_a_prepared_but_absent_batch(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.PRIMARY,
    )
    request = _request()

    def crash_before_provider(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("simulated crash before provider mutation")

    runtime.provider.ingest = crash_before_provider  # type: ignore[method-assign]
    failed = _dispatch(runtime.service, request, request_id="request-1")

    assert not failed.ok
    assert not FilesystemBatchStorageProvider(runtime.provider_root).exists(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=request.batch_id,
    )
    runtime.control.revoke_leader(
        SESSION_ID,
        "coordinator",
        GROUP_ID,
        "grant-1",
    )
    restarted = PhaseDStorageService(
        provider_id=PRIMARY_ID,
        provider=FilesystemBatchStorageProvider(runtime.provider_root),
        control_plane=PhaseDControlPlane(runtime.control_database),
        outbox=SQLiteOutbox(runtime.outbox_database),
        acknowledgements=DurableAcknowledgementStore(
            runtime.acknowledgement_database
        ),
        clock=lambda: NOW,
    )

    assert restarted.reconcile_prepared() == 0
    assert not restarted.provider.exists(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=request.batch_id,
    )
    assert restarted.control_plane.manifest(SESSION_ID, GROUP_ID).revision == 0


def test_primary_only_manifest_intent_does_not_cap_batch_at_outbox_limit(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.PRIMARY,
    )
    original = _request()
    content = {"payload": "x" * 1_100_000}
    request = BatchIngestRequest(
        authority=original.authority,
        dataset_id=original.dataset_id,
        batch_id="batch-large",
        idempotency_key="recorder-a:large",
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        content=content,
        created_at=original.created_at,
        dataset_schema_name=original.dataset_schema_name,
        dataset_schema_version=original.dataset_schema_version,
    )

    response = _dispatch(runtime.service, request, request_id="request-large")

    assert response.ok
    assert runtime.control.manifest(SESSION_ID, GROUP_ID).revision == 1
    assert runtime.outbox.pending() == ()


def test_relay_response_must_match_authenticated_target_session_and_provider(
) -> None:
    async def scenario() -> None:
        relay = QueueRelayClient()
        endpoint = RelayStorageEndpoint(relay, request_timeout=1)
        envelope = StorageRequestEnvelope(
            request_id="bound-response",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_EXISTS,
            session_id=SESSION_ID,
            actor_node_id="recorder-node",
            authorization_context={
                "provider_id": REPLICA_ID,
                "group_id": GROUP_ID,
            },
            payload={"group_id": GROUP_ID, "batch_id": "batch-1"},
        )
        response = StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            ok=True,
            result={"exists": True},
        )
        payload = {
            "kind": RELAY_STORAGE_KIND,
            "message": "response",
            "provider_id": REPLICA_ID,
            "frame": json.dumps(response.to_dict()),
        }
        request_task = asyncio.create_task(
            endpoint.request(
                target_node_id=REPLICA_NODE_ID,
                envelope=envelope,
            )
        )
        await asyncio.sleep(0)
        await relay.messages.put(
            SimpleNamespace(
                payload={
                    **payload,
                    "frame": json.dumps(
                        {
                            "request_id": envelope.request_id,
                            "protocol": STORAGE_PROTOCOL,
                        }
                    ),
                },
                actor_node_id="wrong-node",
                session_id=SESSION_ID,
            )
        )
        await asyncio.sleep(0.01)
        assert not request_task.done()
        await relay.messages.put(
            SimpleNamespace(
                payload=payload,
                actor_node_id="wrong-node",
                session_id=SESSION_ID,
            )
        )
        await asyncio.sleep(0.01)
        assert not request_task.done()
        await relay.messages.put(
            SimpleNamespace(
                payload=payload,
                actor_node_id=REPLICA_NODE_ID,
                session_id=SESSION_ID,
            )
        )

        assert await request_task == response
        await endpoint.close()

    asyncio.run(scenario())


def test_retry_after_grant_renewal_finalizes_original_intent_idempotently(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.PRIMARY,
    )
    original = _request()

    def crash_before_manifest(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("simulated crash before manifest publication")

    runtime.control.commit_batch_manifest = crash_before_manifest  # type: ignore[method-assign]
    assert not _dispatch(
        runtime.service,
        original,
        request_id="request-old-grant",
    ).ok

    control = PhaseDControlPlane(runtime.control_database)
    control.grant_leader(
        SESSION_ID,
        "coordinator",
        GROUP_ID,
        PRIMARY_ID,
        "grant-2",
        2,
        11,
        lease_expires_at=NOW + timedelta(minutes=20),
        occurred_at=NOW + timedelta(minutes=1),
    )
    renewed = BatchIngestRequest(
        authority=WriteAuthority(
            session_id=SESSION_ID,
            group_id=GROUP_ID,
            actor_node_id=PRIMARY_NODE_ID,
            grant_id="grant-2",
            term=2,
            fencing_token=11,
            lease_expires_at=NOW + timedelta(minutes=20),
        ),
        dataset_id=original.dataset_id,
        batch_id=original.batch_id,
        idempotency_key=original.idempotency_key,
        content_hash=original.content_hash,
        content=original.content,
        created_at=original.created_at,
        dataset_schema_name=original.dataset_schema_name,
        dataset_schema_version=original.dataset_schema_version,
    )
    service = PhaseDStorageService(
        provider_id=PRIMARY_ID,
        provider=FilesystemBatchStorageProvider(runtime.provider_root),
        control_plane=control,
        outbox=SQLiteOutbox(runtime.outbox_database),
        acknowledgements=DurableAcknowledgementStore(
            runtime.acknowledgement_database
        ),
        clock=lambda: NOW + timedelta(minutes=2),
    )

    response = _dispatch(service, renewed, request_id="request-new-grant")

    assert response.ok
    manifest = control.manifest(SESSION_ID, GROUP_ID)
    assert manifest.revision == 1
    assert manifest.term == 2
    assert control.pending_batch_manifest_intents(
        primary_provider_id=PRIMARY_ID,
    ) == ()


def test_replica_backed_retry_after_grant_renewal_uses_current_term(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.ONE_REPLICA,
        with_replica=True,
        transport=UnavailableTransport(),
    )
    original = _request()

    first = _dispatch(runtime.service, original, request_id="request-term-1")
    assert not first.ok
    assert len(runtime.outbox.pending()) == 1

    control = PhaseDControlPlane(runtime.control_database)
    control.grant_leader(
        SESSION_ID,
        "coordinator",
        GROUP_ID,
        PRIMARY_ID,
        "grant-2",
        2,
        11,
        lease_expires_at=NOW + timedelta(minutes=20),
        occurred_at=NOW + timedelta(minutes=1),
    )
    renewed = BatchIngestRequest(
        authority=WriteAuthority(
            session_id=SESSION_ID,
            group_id=GROUP_ID,
            actor_node_id=PRIMARY_NODE_ID,
            grant_id="grant-2",
            term=2,
            fencing_token=11,
            lease_expires_at=NOW + timedelta(minutes=20),
        ),
        dataset_id=original.dataset_id,
        batch_id=original.batch_id,
        idempotency_key=original.idempotency_key,
        content_hash=original.content_hash,
        content=original.content,
        created_at=original.created_at,
        dataset_schema_name=original.dataset_schema_name,
        dataset_schema_version=original.dataset_schema_version,
    )
    transport = CurrentTermTransport(2)
    service = PhaseDStorageService(
        provider_id=PRIMARY_ID,
        provider=FilesystemBatchStorageProvider(runtime.provider_root),
        control_plane=control,
        outbox=SQLiteOutbox(runtime.outbox_database),
        acknowledgements=DurableAcknowledgementStore(
            runtime.acknowledgement_database
        ),
        replication_transport=transport,
        clock=lambda: NOW + timedelta(minutes=2),
    )

    response = _dispatch(service, renewed, request_id="request-term-2")

    assert response.ok
    assert transport.attempted_terms == [1, 2]
    assert service.outbox is not None
    assert service.outbox.pending() == ()
    manifest = control.manifest(SESSION_ID, GROUP_ID)
    assert manifest.revision == 1
    assert manifest.term == 2


def test_policy_committed_intent_finalizes_after_primary_handover(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.ONE_REPLICA,
        with_replica=True,
        transport=CorrectIdentityTransport(),
    )
    request = _request()

    def crash_before_manifest(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("simulated crash before manifest publication")

    runtime.control.commit_batch_manifest = crash_before_manifest  # type: ignore[method-assign]
    failed = _dispatch(runtime.service, request, request_id="request-1")
    status = runtime.acknowledgements.status(
        SESSION_ID,
        GROUP_ID,
        request.batch_id,
    )
    assert not failed.ok
    assert status is not None and status.committed

    control = PhaseDControlPlane(runtime.control_database)
    control.complete_handover(
        session_id=SESSION_ID,
        actor_node_id="coordinator",
        group_id=GROUP_ID,
        target_provider_id=REPLICA_ID,
        grant_id="grant-2",
        term=2,
        fencing_token=11,
        lease_expires_at=NOW + timedelta(minutes=20),
        occurred_at=NOW + timedelta(minutes=1),
    )
    old_primary = PhaseDStorageService(
        provider_id=PRIMARY_ID,
        provider=FilesystemBatchStorageProvider(runtime.provider_root),
        control_plane=control,
        outbox=SQLiteOutbox(runtime.outbox_database),
        acknowledgements=DurableAcknowledgementStore(
            runtime.acknowledgement_database
        ),
        clock=lambda: NOW + timedelta(minutes=2),
    )

    assert old_primary.reconcile_prepared() == 1
    manifest = control.manifest(SESSION_ID, GROUP_ID)
    assert manifest.revision == 1
    assert manifest.term == 2
    assert tuple(item.item_id for item in manifest.items) == (request.batch_id,)


def test_logical_client_retains_obligation_without_manifest_evidence(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.PRIMARY,
    )
    client = PhaseDLogicalStorageClient(
        session_id=SESSION_ID,
        actor_node_id="recorder-node",
        control_plane=runtime.control,
        transport=UnmanifestedSuccessTransport(),
    )

    outcome = asyncio.run(client.ingest(_request()))

    assert not outcome.committed
    assert outcome.retryable
    assert outcome.error_code == "invalid-storage-response"
    assert runtime.control.manifest(SESSION_ID, GROUP_ID).revision == 0


def test_logical_client_requires_authoritative_manifest_reader(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.PRIMARY,
    )

    class SnapshotOnlyControl:
        def snapshot(self, session_id: str):
            return runtime.control.snapshot(session_id)

    class SelfAssertedCommitTransport:
        async def request(
            self,
            *,
            target_node_id: str,
            envelope: StorageRequestEnvelope,
        ) -> StorageResponseEnvelope:
            del target_node_id
            request = BatchIngestRequest.from_dict(envelope.payload)
            result = BatchIngestResult(
                batch_id=request.batch_id,
                idempotency_key=request.idempotency_key,
                content_hash=request.content_hash,
                state=BatchIngestState.STORED,
            ).to_dict()
            result.update(
                {
                    "commit_state": "committed",
                    "manifest_revision": 1,
                    "manifest_hash": "sha256:" + ("0" * 64),
                }
            )
            return StorageResponseEnvelope(
                request_id=envelope.request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                ok=True,
                result=result,
            )

    client = PhaseDLogicalStorageClient(
        session_id=SESSION_ID,
        actor_node_id="recorder-node",
        control_plane=SnapshotOnlyControl(),
        transport=SelfAssertedCommitTransport(),
    )

    outcome = asyncio.run(client.ingest(_request()))

    assert not outcome.committed
    assert outcome.retryable
    assert outcome.error_code == "invalid-storage-response"
    assert outcome.message == "authoritative manifest verification is unavailable"


def test_acknowledgement_store_migrates_legacy_rows_fail_closed(
    tmp_path: Path,
) -> None:
    database = tmp_path / "legacy-acknowledgements.sqlite3"
    request = _request()
    stamp = NOW.isoformat()
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE storage_commits (
                session_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                required_replica_acks INTEGER NOT NULL,
                primary_committed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(session_id, group_id, batch_id),
                UNIQUE(session_id, group_id, idempotency_key)
            );
            CREATE TABLE storage_commit_replicas (
                session_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                acknowledged INTEGER NOT NULL DEFAULT 0,
                acknowledged_at TEXT,
                PRIMARY KEY(session_id, group_id, batch_id, provider_id),
                FOREIGN KEY(session_id, group_id, batch_id)
                    REFERENCES storage_commits(session_id, group_id, batch_id)
                    ON DELETE CASCADE
            );
            """
        )
        connection.execute(
            """INSERT INTO storage_commits
               (session_id, group_id, batch_id, idempotency_key, content_hash,
                required_replica_acks, primary_committed, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 0, 1, ?, ?)""",
            (
                SESSION_ID,
                GROUP_ID,
                request.batch_id,
                request.idempotency_key,
                request.content_hash,
                stamp,
                stamp,
            ),
        )

    store = DurableAcknowledgementStore(database)
    status = store.status(SESSION_ID, GROUP_ID, request.batch_id)

    assert status is not None
    assert status.dataset_id == "legacy-unknown-dataset"
    assert status.dataset_schema_name == "msh.storage.dataset.opaque"
    assert status.dataset_schema_version == 1
    with pytest.raises(FederationValidationError) as error:
        store.prepare(
            request,
            policy=AcknowledgementPolicy(),
            replica_provider_ids=(),
            now=NOW,
        )
    assert error.value.code == "idempotency-conflict"
