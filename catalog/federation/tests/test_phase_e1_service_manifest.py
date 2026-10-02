from __future__ import annotations

import asyncio
import hashlib
import json
import logging
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
    StorageError,
    StorageErrorCode,
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
        dataset_schema_name="fcp.telemetry.observations",
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
    assert manifest.datasets[0].schema_name == "fcp.telemetry.observations"
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


def test_provider_clock_before_grant_issuance_fails_closed(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.PRIMARY,
    )
    runtime.service.clock = lambda: NOW - timedelta(seconds=1)

    response = _dispatch(runtime.service, _request(), request_id="request-before-issue")

    assert not response.ok
    assert response.error is not None
    assert response.error.code.value == "grant-not-yet-valid"
    assert not runtime.provider.exists(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id="batch-1",
    )


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
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        relay = QueueRelayClient()
        endpoint = RelayStorageEndpoint(relay, request_timeout=1)
        caplog.set_level(logging.INFO, logger="catalog.federation.relay_storage")
        envelope = StorageRequestEnvelope(
            request_id="bound-response",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_INGEST,
            session_id=SESSION_ID,
            actor_node_id="recorder-node",
            authorization_context={
                "provider_id": REPLICA_ID,
                "group_id": GROUP_ID,
            },
            payload={
                "group_id": GROUP_ID,
                "dataset_id": "dataset-1",
                "batch_id": "batch-1",
                "content_hash": "sha256:" + "a" * 64,
                "idempotency_key": "idem-1",
            },
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
                session_id="wrong-session",
            )
        )
        await asyncio.sleep(0.01)
        assert not request_task.done()
        await relay.messages.put(
            SimpleNamespace(
                payload={**payload, "provider_id": "wrong-provider"},
                actor_node_id=REPLICA_NODE_ID,
                session_id=SESSION_ID,
            )
        )
        await asyncio.sleep(0.01)
        assert not request_task.done()
        await relay.messages.put(
            SimpleNamespace(
                payload={key: value for key, value in payload.items() if key != "provider_id"},
                actor_node_id=REPLICA_NODE_ID,
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
        rejection_reasons = [
            record.storage_rejection_reason
            for record in caplog.records
            if record.name == "catalog.federation.relay_storage"
            and hasattr(record, "storage_rejection_reason")
        ]
        assert rejection_reasons == [
            "route_mismatch",
            "route_mismatch",
            "route_mismatch",
            "provider_mismatch",
            "provider_mismatch",
        ]
        rejected = [
            record for record in caplog.records
            if record.name == "catalog.federation.relay_storage"
            and getattr(record, "storage_rejection_reason", None) is not None
        ]
        assert len(rejected) == 5
        for record in rejected:
            assert record.storage_batch_id == "batch-1"
            assert record.storage_dataset_id == "dataset-1"
            assert record.storage_content_hash == "sha256:" + "a" * 64
            assert record.storage_idempotency_key_sha256 == hashlib.sha256(
                b"idem-1"
            ).hexdigest()
        correlated = [
            record for record in caplog.records
            if record.name == "catalog.federation.relay_storage"
            and getattr(record, "storage_stage", None) in {"request_delivery", "response_accepted"}
        ]
        assert {record.storage_stage for record in correlated} == {
            "request_delivery",
            "response_accepted",
        }
        for record in correlated:
            assert record.storage_operation == "batch.ingest"
            assert record.storage_group_id == GROUP_ID
            assert record.storage_dataset_id == "dataset-1"
            assert record.storage_batch_id == "batch-1"
            assert record.storage_content_hash == "sha256:" + "a" * 64
            assert record.storage_idempotency_key_sha256 == hashlib.sha256(
                b"idem-1"
            ).hexdigest()
        await endpoint.close()

    asyncio.run(scenario())


def test_late_response_from_timed_out_attempt_cannot_acknowledge_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        relay = QueueRelayClient()
        endpoint = RelayStorageEndpoint(relay, request_timeout=0.25)
        caplog.set_level(logging.INFO, logger="catalog.federation.relay_storage")

        def request(request_id: str) -> StorageRequestEnvelope:
            return StorageRequestEnvelope(
                request_id=request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                operation=StorageOperation.BATCH_INGEST,
                session_id=SESSION_ID,
                actor_node_id="recorder-node",
                authorization_context={
                    "provider_id": REPLICA_ID,
                    "group_id": GROUP_ID,
                },
                payload={
                    "group_id": GROUP_ID,
                    "dataset_id": "dataset-retry",
                    "batch_id": "same-batch-replay",
                    "content_hash": "sha256:" + "b" * 64,
                    "idempotency_key": "same-stable-idempotency-key",
                },
            )

        def response(request_id: str) -> SimpleNamespace:
            envelope = StorageResponseEnvelope(
                request_id=request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                ok=True,
                result={"state": "committed"},
            )
            return SimpleNamespace(
                payload={
                    "kind": RELAY_STORAGE_KIND,
                    "message": "response",
                    "provider_id": REPLICA_ID,
                    "frame": json.dumps(envelope.to_dict()),
                },
                actor_node_id=REPLICA_NODE_ID,
                session_id=SESSION_ID,
            )

        try:
            with pytest.raises(TimeoutError):
                await endpoint.request(
                    target_node_id=REPLICA_NODE_ID,
                    envelope=request("same-batch-attempt-1"),
                )
            assert "same-batch-attempt-1" not in endpoint._pending

            retry = asyncio.create_task(
                endpoint.request(
                    target_node_id=REPLICA_NODE_ID,
                    envelope=request("same-batch-attempt-2"),
                )
            )
            await asyncio.sleep(0)
            await relay.messages.put(response("same-batch-attempt-1"))
            await asyncio.sleep(0.01)
            assert not retry.done()
            assert set(endpoint._pending) == {"same-batch-attempt-2"}

            retry_response = response("same-batch-attempt-2")
            committed = StorageResponseEnvelope.from_dict(
                json.loads(retry_response.payload["frame"])
            )
            await relay.messages.put(retry_response)
            assert await retry == committed

            stale = [
                record
                for record in caplog.records
                if record.name == "catalog.federation.relay_storage"
                and getattr(record, "storage_rejection_reason", None)
                == "request_not_pending"
            ]
            assert len(stale) == 1
            assert stale[0].storage_request_id == "same-batch-attempt-1"
        finally:
            await endpoint.close()

    asyncio.run(scenario())


def test_untrusted_response_identifiers_are_bounded_in_diagnostics(
    caplog: pytest.LogCaptureFixture,
) -> None:
    relay = QueueRelayClient()
    endpoint = RelayStorageEndpoint(relay)
    caplog.set_level(logging.WARNING, logger="catalog.federation.relay_storage")
    endpoint._accept_response(
        SimpleNamespace(
            request_id="relay-response\ncredential",
            actor_node_id="node\ncredential",
            session_id="s" * 2049,
        ),
        {
            "provider_id": {"credential": "must-not-be-logged"},
            "frame": json.dumps({"request_id": "response\ncredential"}),
        },
    )

    records = [
        record
        for record in caplog.records
        if record.name == "catalog.federation.relay_storage"
        and getattr(record, "storage_rejection_reason", None) == "request_not_pending"
    ]
    assert len(records) == 1
    record = records[0]
    assert record.storage_request_id == "invalid"
    assert record.storage_relay_request_id == "invalid"
    assert record.storage_actor_node_id == "invalid"
    assert record.storage_session_id == "invalid"
    assert record.storage_provider_id == "invalid"
    assert "credential" not in caplog.text
    assert "must-not-be-logged" not in caplog.text


def test_provider_rejection_keeps_malformed_request_id_out_of_delivery_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        class RecordingRelayClient(QueueRelayClient):
            def __init__(self) -> None:
                super().__init__()
                self.sent: list[dict[str, object]] = []

            async def send_message(self, **kwargs: object) -> dict[str, bool]:
                self.sent.append(kwargs)
                return {"delivered": True}

        relay = RecordingRelayClient()
        endpoint = RelayStorageEndpoint(relay)
        caplog.set_level(logging.INFO, logger="catalog.federation.relay_storage")
        request_value = _envelope(
            _request(), request_id="placeholder-request"
        ).to_dict()
        request_value["request_id"] = {"credential": "request-id-marker"}
        payload = {
            "provider_id": "provider\nsecret-marker",
            "frame": json.dumps(request_value),
        }

        await endpoint._handle_request(
            SimpleNamespace(actor_node_id="recorder-node", session_id=SESSION_ID),
            payload,
        )

        records = [
            record
            for record in caplog.records
            if record.name == "catalog.federation.relay_storage"
            and getattr(record, "storage_stage", None) == "response_delivery"
        ]
        assert len(records) == 1
        assert records[0].storage_request_id == "invalid-storage-request"
        assert records[0].storage_provider_id == "invalid"
        assert "request-id-marker" not in caplog.text
        assert "secret-marker" not in caplog.text
        assert len(relay.sent) == 1
        sent_payload = relay.sent[0]["payload"]
        assert isinstance(sent_payload, dict)
        response = json.loads(sent_payload["frame"])
        assert response["request_id"] == "invalid-storage-request"

    asyncio.run(scenario())


def test_provider_rejection_preserves_valid_request_correlation_id() -> None:
    async def scenario() -> None:
        class RecordingRelayClient(QueueRelayClient):
            def __init__(self) -> None:
                super().__init__()
                self.sent: list[dict[str, object]] = []

            async def send_message(self, **kwargs: object) -> dict[str, bool]:
                self.sent.append(kwargs)
                return {"delivered": True}

        relay = RecordingRelayClient()
        endpoint = RelayStorageEndpoint(relay)
        request_value = _envelope(
            _request(), request_id="corr-valid-rejected"
        ).to_dict()
        del request_value["protocol"]

        await endpoint._handle_request(
            SimpleNamespace(actor_node_id="recorder-node", session_id=SESSION_ID),
            {"provider_id": REPLICA_ID, "frame": json.dumps(request_value)},
        )

        assert len(relay.sent) == 1
        sent_payload = relay.sent[0]["payload"]
        assert isinstance(sent_payload, dict)
        response = json.loads(sent_payload["frame"])
        assert response["request_id"] == "corr-valid-rejected"
        assert response["ok"] is False

    asyncio.run(scenario())


def test_provider_dispatch_timeout_is_logged_with_batch_provenance(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _TimeoutService:
        async def dispatch(self, _envelope):
            raise TimeoutError("simulated provider dispatch timeout")

    async def scenario() -> None:
        relay = QueueRelayClient()
        endpoint = RelayStorageEndpoint(relay, services={REPLICA_ID: _TimeoutService()})
        caplog.set_level(logging.ERROR, logger="catalog.federation.relay_storage")
        envelope = StorageRequestEnvelope(
            request_id="dispatch-timeout",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_INGEST,
            session_id=SESSION_ID,
            actor_node_id="recorder-node",
            authorization_context={"provider_id": REPLICA_ID, "group_id": GROUP_ID},
            payload={
                "group_id": GROUP_ID,
                "dataset_id": "dataset-timeout",
                "batch_id": "batch-timeout",
                "content_hash": "sha256:" + "b" * 64,
                "idempotency_key": "idempotency-timeout",
            },
        )
        with pytest.raises(TimeoutError):
            await endpoint._handle_request(
                SimpleNamespace(actor_node_id="recorder-node", session_id=SESSION_ID),
                {
                    "provider_id": REPLICA_ID,
                    "frame": json.dumps(envelope.to_dict()),
                },
            )

        records = [
            record for record in caplog.records
            if record.name == "catalog.federation.relay_storage"
            and getattr(record, "storage_stage", None) == "provider_dispatch_timeout"
        ]
        assert len(records) == 1
        record = records[0]
        assert record.storage_request_id == "dispatch-timeout"
        assert record.storage_session_id == SESSION_ID
        assert record.storage_provider_id == REPLICA_ID
        assert record.storage_group_id == GROUP_ID
        assert record.storage_dataset_id == "dataset-timeout"
        assert record.storage_batch_id == "batch-timeout"
        assert record.storage_content_hash == "sha256:" + "b" * 64
        assert record.storage_idempotency_key_sha256 == hashlib.sha256(
            b"idempotency-timeout"
        ).hexdigest()

    asyncio.run(scenario())


def test_provider_response_delivery_exception_is_logged_by_reader_task(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        class FailingRelayClient(QueueRelayClient):
            def __init__(self) -> None:
                super().__init__()
                self.delivery_attempted = asyncio.Event()

            async def send_message(self, **_kwargs: object) -> dict[str, bool]:
                self.delivery_attempted.set()
                raise TimeoutError("private transport detail must not be logged")

        class ObservedEndpoint(RelayStorageEndpoint):
            def __init__(self, relay_client: FailingRelayClient) -> None:
                super().__init__(relay_client)
                self.handler_finished = asyncio.Event()
                self.handler_cancelled = False
                self.handler_exception: BaseException | None = None

            def _finish_handler(self, task: asyncio.Task[None]) -> None:
                self.handler_cancelled = task.cancelled()
                self.handler_exception = (
                    None if task.cancelled() else task.exception()
                )
                super()._finish_handler(task)
                self.handler_finished.set()

        relay = FailingRelayClient()
        endpoint = ObservedEndpoint(relay)
        caplog.set_level(logging.INFO, logger="catalog.federation.relay_storage")
        envelope = StorageRequestEnvelope(
            request_id="response-delivery-timeout",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_INGEST,
            session_id=SESSION_ID,
            actor_node_id="recorder-node",
            authorization_context={"provider_id": REPLICA_ID, "group_id": GROUP_ID},
            payload={
                "group_id": GROUP_ID,
                "dataset_id": "dataset-delivery-timeout",
                "batch_id": "batch-delivery-timeout",
                "content_hash": "sha256:" + "c" * 64,
                "idempotency_key": "idempotency-delivery-timeout",
            },
        )

        await endpoint.start()
        await relay.messages.put(
            SimpleNamespace(
                actor_node_id="recorder-node",
                session_id=SESSION_ID,
                payload={
                    "kind": RELAY_STORAGE_KIND,
                    "message": "request",
                    "provider_id": REPLICA_ID,
                    "frame": json.dumps(envelope.to_dict()),
                },
            )
        )
        await asyncio.wait_for(relay.delivery_attempted.wait(), timeout=1)
        await asyncio.wait_for(endpoint.handler_finished.wait(), timeout=1)
        assert not endpoint.handler_cancelled
        assert isinstance(endpoint.handler_exception, TimeoutError)
        assert not endpoint._handler_tasks
        await endpoint.close()

        records = [
            record
            for record in caplog.records
            if record.name == "catalog.federation.relay_storage"
            and getattr(record, "storage_stage", None) == "response_delivery_failed"
        ]
        assert len(records) == 1
        record = records[0]
        assert record.storage_request_id == "response-delivery-timeout"
        assert record.storage_session_id == SESSION_ID
        assert record.storage_target_node_id == "recorder-node"
        assert record.storage_provider_id == REPLICA_ID
        assert record.storage_dataset_id == "dataset-delivery-timeout"
        assert record.storage_batch_id == "batch-delivery-timeout"
        assert record.storage_content_hash == "sha256:" + "c" * 64
        assert record.storage_exception_type == "TimeoutError"
        assert record.storage_elapsed_seconds >= 0
        assert record.exc_info is None
        assert record.exc_text is None
        assert "private transport detail" not in caplog.text
        assert "private transport detail" not in repr(vars(record))
        assert not any(
            getattr(entry, "storage_stage", None) == "response_delivery"
            for entry in caplog.records
        )

    asyncio.run(scenario())


def test_provider_response_delivery_cancellation_is_logged_by_reader_task(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        class BlockingRelayClient(QueueRelayClient):
            def __init__(self) -> None:
                super().__init__()
                self.delivery_started = asyncio.Event()

            async def send_message(self, **_kwargs: object) -> dict[str, bool]:
                self.delivery_started.set()
                await asyncio.Future()

        class ObservedEndpoint(RelayStorageEndpoint):
            def __init__(self, relay_client: BlockingRelayClient) -> None:
                super().__init__(relay_client)
                self.handler_finished = asyncio.Event()
                self.handler_cancelled = False

            def _finish_handler(self, task: asyncio.Task[None]) -> None:
                self.handler_cancelled = task.cancelled()
                super()._finish_handler(task)
                self.handler_finished.set()

        relay = BlockingRelayClient()
        endpoint = ObservedEndpoint(relay)
        caplog.set_level(logging.INFO, logger="catalog.federation.relay_storage")
        envelope = StorageRequestEnvelope(
            request_id="response-delivery-cancelled",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_INGEST,
            session_id=SESSION_ID,
            actor_node_id="recorder-node",
            authorization_context={"provider_id": REPLICA_ID, "group_id": GROUP_ID},
            payload={
                "group_id": GROUP_ID,
                "dataset_id": "dataset-delivery-cancelled",
                "batch_id": "batch-delivery-cancelled",
                "content_hash": "sha256:" + "d" * 64,
                "idempotency_key": "idempotency-delivery-cancelled",
            },
        )

        await endpoint.start()
        await relay.messages.put(
            SimpleNamespace(
                actor_node_id="recorder-node",
                session_id=SESSION_ID,
                payload={
                    "kind": RELAY_STORAGE_KIND,
                    "message": "request",
                    "provider_id": REPLICA_ID,
                    "frame": json.dumps(envelope.to_dict()),
                },
            )
        )
        await asyncio.wait_for(relay.delivery_started.wait(), timeout=1)
        handler = next(iter(endpoint._handler_tasks))
        handler.cancel("private cancellation detail must not be logged")
        await asyncio.wait_for(endpoint.handler_finished.wait(), timeout=1)
        assert endpoint.handler_cancelled
        assert not endpoint._handler_tasks
        await endpoint.close()

        cancellation_records = [
            record
            for record in caplog.records
            if record.name == "catalog.federation.relay_storage"
            and getattr(record, "storage_stage", None)
            == "response_delivery_cancelled"
        ]
        assert len(cancellation_records) == 1
        record = cancellation_records[0]
        assert record.storage_request_id == "response-delivery-cancelled"
        assert record.storage_session_id == SESSION_ID
        assert record.storage_target_node_id == "recorder-node"
        assert record.storage_provider_id == REPLICA_ID
        assert record.storage_dataset_id == "dataset-delivery-cancelled"
        assert record.storage_batch_id == "batch-delivery-cancelled"
        assert record.storage_content_hash == "sha256:" + "d" * 64
        assert record.storage_elapsed_seconds >= 0
        assert record.exc_info is None
        assert record.exc_text is None
        assert "private cancellation detail" not in caplog.text
        assert "private cancellation detail" not in repr(vars(record))
        assert not any(
            getattr(entry, "storage_stage", None)
            in {"response_delivery", "response_delivery_failed"}
            for entry in caplog.records
        )

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


def test_logical_client_refreshes_grant_after_provider_rejects_stale_route(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.PRIMARY,
    )
    runtime.service.clock = lambda: NOW + timedelta(seconds=2)

    class GrantRenewalRaceTransport:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        async def request(self, *, target_node_id, envelope):
            routed = BatchIngestRequest.from_dict(envelope.payload)
            self.calls.append(
                (
                    routed.authority.grant_id,
                    routed.batch_id,
                    routed.content_hash,
                )
            )
            if len(self.calls) == 1:
                # The coordinator renews the same provider's grant after the
                # logical client prepares its intent, but before the provider
                # validates the routed write.
                runtime.control.grant_leader(
                    SESSION_ID,
                    "coordinator",
                    GROUP_ID,
                    PRIMARY_ID,
                    "grant-2",
                    2,
                    11,
                    lease_expires_at=NOW + timedelta(minutes=20),
                    occurred_at=NOW + timedelta(seconds=1),
                )
            assert target_node_id == PRIMARY_NODE_ID
            return await runtime.service.dispatch(envelope)

    transport = GrantRenewalRaceTransport()
    client = PhaseDLogicalStorageClient(
        session_id=SESSION_ID,
        actor_node_id="recorder-node",
        control_plane=runtime.control,
        transport=transport,
        acknowledgements=runtime.acknowledgements,
        clock=lambda: NOW + timedelta(seconds=2),
    )

    outcome = asyncio.run(client.ingest(_request()))

    assert outcome.committed
    assert [grant_id for grant_id, _, _ in transport.calls] == [
        "grant-1",
        "grant-2",
    ]
    assert len({batch_id for _, batch_id, _ in transport.calls}) == 1
    assert len({content_hash for _, _, content_hash in transport.calls}) == 1
    manifest = runtime.control.manifest(SESSION_ID, GROUP_ID)
    assert manifest.revision == 1
    assert manifest.items[0].item_id == _request().batch_id
    retry_records = [
        record
        for record in caplog.records
        if record.message == "phase_d_ingest_retry_after_grant_rotation"
    ]
    assert len(retry_records) == 1
    assert retry_records[0].stale_term == 1
    assert retry_records[0].current_term == 2


def test_logical_client_does_not_retry_across_primary_change(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        acknowledgement_mode=AcknowledgementMode.PRIMARY,
        with_replica=True,
    )
    runtime.service.clock = lambda: NOW + timedelta(seconds=2)

    class ProviderChangeTransport:
        def __init__(self) -> None:
            self.targets: list[str] = []

        async def request(self, *, target_node_id, envelope):
            self.targets.append(target_node_id)
            runtime.control.change_assignment(
                SESSION_ID,
                "coordinator",
                GROUP_ID,
                REPLICA_ID,
            )
            runtime.control.grant_leader(
                SESSION_ID,
                "coordinator",
                GROUP_ID,
                REPLICA_ID,
                "grant-2",
                2,
                11,
                lease_expires_at=NOW + timedelta(minutes=20),
                occurred_at=NOW + timedelta(seconds=1),
            )
            return StorageResponseEnvelope(
                request_id=envelope.request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                ok=False,
                error=StorageError(
                    code=StorageErrorCode.UNKNOWN_GRANT,
                    message="test provider rejected the stale routed grant",
                    field="authority.grant_id",
                    retryable=False,
                ),
            )

    transport = ProviderChangeTransport()
    client = PhaseDLogicalStorageClient(
        session_id=SESSION_ID,
        actor_node_id="recorder-node",
        control_plane=runtime.control,
        transport=transport,
        acknowledgements=runtime.acknowledgements,
        clock=lambda: NOW + timedelta(seconds=2),
    )

    with pytest.raises(FederationValidationError) as error:
        asyncio.run(client.ingest(_request()))

    assert error.value.code == StorageErrorCode.UNKNOWN_GRANT.value
    assert transport.targets == [PRIMARY_NODE_ID]


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
    assert status.dataset_schema_name == "fcp.storage.dataset.opaque"
    assert status.dataset_schema_version == 1
    with pytest.raises(FederationValidationError) as error:
        store.prepare(
            request,
            policy=AcknowledgementPolicy(),
            replica_provider_ids=(),
            now=NOW,
        )
    assert error.value.code == "idempotency-conflict"
