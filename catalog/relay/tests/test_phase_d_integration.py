from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationValidationError
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.phase_d_handover import PhaseDHandoverCoordinator
from catalog.federation.phase_d_service import PhaseDStorageService
from catalog.federation.recorder_delivery import DurableRecorderDeliveryQueue
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    StorageOperation,
    StorageRequestEnvelope,
    WriteAuthority,
)
from catalog.node.client import RelayNodeClient
from catalog.relay.service import RelayServer

NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)
TIMEOUT = 5.0


async def _start_relay(database: Path) -> RelayServer:
    relay = RelayServer(
        SessionCoordinator(database, clock=lambda: NOW),
        host="127.0.0.1",
        port=0,
        auth_timeout_seconds=TIMEOUT,
        send_timeout_seconds=TIMEOUT,
        heartbeat_timeout_seconds=300,
        sweep_interval_seconds=300,
    )
    await relay.start()
    return relay


def _client(path: Path, relay: RelayServer, name: str) -> RelayNodeClient:
    return RelayNodeClient(
        state_directory=path,
        relay_url=relay.url,
        display_name=name,
        allow_insecure_local=True,
        heartbeat_interval=300,
        request_timeout=TIMEOUT,
        clock=lambda: NOW,
    )


async def _enroll(relay: RelayServer, client: RelayNodeClient) -> None:
    token = relay.coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    await client.connect(enrollment_token=token)


async def _join(owner: RelayNodeClient, member: RelayNodeClient, session_id: str) -> None:
    invitation = await owner.create_invitation(session_id, ttl_seconds=60, max_uses=1)
    joined = await member.join_session(str(invitation["token"]))
    assert joined["session_id"] == session_id
    await owner.request_replay(session_id)


def _registration(session_id: str, provider_id: str, node_id: str) -> StorageProviderRegistration:
    return StorageProviderRegistration(
        session_id=session_id,
        provider_id=provider_id,
        node_id=node_id,
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        authorized=True,
        status="ready",
    )


def test_phase_d_three_nodes_replicate_and_handover(tmp_path: Path) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        recorder: RelayNodeClient | None = None
        primary_node: RelayNodeClient | None = None
        replica_node: RelayNodeClient | None = None
        endpoints: list[RelayStorageEndpoint] = []
        try:
            relay = await _start_relay(tmp_path / "relay" / "control.sqlite3")
            recorder = _client(tmp_path / "recorder-node", relay, "Recorder")
            primary_node = _client(tmp_path / "primary-node", relay, "Primary")
            replica_node = _client(tmp_path / "replica-node", relay, "Replica")
            await _enroll(relay, recorder)
            await _enroll(relay, primary_node)
            await _enroll(relay, replica_node)
            session = await recorder.create_session("Phase D integration")
            session_id = str(session["session_id"])
            await _join(recorder, primary_node, session_id)
            await _join(recorder, replica_node, session_id)

            control = PhaseDControlPlane(tmp_path / "storage-control.sqlite3")
            control.create_group(session_id, "coordinator", "storage-main")
            control.register_provider(
                session_id,
                "coordinator",
                _registration(session_id, "provider-primary", primary_node.node_id),
            )
            control.register_provider(
                session_id,
                "coordinator",
                _registration(session_id, "provider-replica", replica_node.node_id),
            )
            control.change_assignment(
                session_id,
                "coordinator",
                "storage-main",
                "provider-primary",
                ("provider-replica",),
            )
            control.set_acknowledgement_policy(
                session_id, "storage-main", AcknowledgementMode.ONE_REPLICA
            )
            control.grant_leader(
                session_id,
                "coordinator",
                "storage-main",
                "provider-primary",
                "grant-1",
                1,
                10,
                lease_expires_at=NOW + timedelta(minutes=10),
                occurred_at=NOW,
            )

            primary_provider = FilesystemBatchStorageProvider(tmp_path / "primary-storage")
            replica_provider = FilesystemBatchStorageProvider(tmp_path / "replica-storage")
            primary_outbox = SQLiteOutbox(tmp_path / "primary-outbox.sqlite3")
            replica_outbox = SQLiteOutbox(tmp_path / "replica-outbox.sqlite3")
            primary_acks = DurableAcknowledgementStore(tmp_path / "primary-acks.sqlite3")
            replica_acks = DurableAcknowledgementStore(tmp_path / "replica-acks.sqlite3")

            primary_endpoint = RelayStorageEndpoint(primary_node, request_timeout=TIMEOUT)
            replica_endpoint = RelayStorageEndpoint(replica_node, request_timeout=TIMEOUT)
            recorder_endpoint = RelayStorageEndpoint(recorder, request_timeout=TIMEOUT)
            endpoints.extend((primary_endpoint, replica_endpoint, recorder_endpoint))

            primary_service = PhaseDStorageService(
                provider_id="provider-primary",
                provider=primary_provider,
                control_plane=control,
                outbox=primary_outbox,
                acknowledgements=primary_acks,
                replication_transport=primary_endpoint,
                clock=lambda: NOW,
            )
            replica_service = PhaseDStorageService(
                provider_id="provider-replica",
                provider=replica_provider,
                control_plane=control,
                outbox=replica_outbox,
                acknowledgements=replica_acks,
                replication_transport=replica_endpoint,
                clock=lambda: NOW,
            )
            primary_endpoint.register_service("provider-primary", primary_service)
            replica_endpoint.register_service("provider-replica", replica_service)
            await asyncio.gather(*(endpoint.start() for endpoint in endpoints))

            client = PhaseDLogicalStorageClient(
                session_id=session_id,
                actor_node_id=recorder.node_id,
                control_plane=control,
                transport=recorder_endpoint,
            )
            recorder_outbox = SQLiteOutbox(tmp_path / "recorder-delivery.sqlite3")
            recorder_delivery = DurableRecorderDeliveryQueue(
                outbox=recorder_outbox,
                client=client,
                clock=lambda: NOW,
            )
            content = {
                "source": "recorder-a",
                "observations": [{"sequence": 1, "value": 42.0}],
            }
            recorder_delivery.enqueue(
                session_id=session_id,
                group_id="storage-main",
                dataset_id="telemetry",
                batch_id="batch-1",
                idempotency_key="recorder-a:1",
                content=content,
                created_at=NOW,
            )
            delivery_result = await recorder_delivery.run_once()
            assert delivery_result.committed == 1
            assert recorder_outbox.pending() == ()
            assert primary_provider.read(
                session_id=session_id, group_id="storage-main", batch_id="batch-1"
            ) == content
            assert replica_provider.read(
                session_id=session_id, group_id="storage-main", batch_id="batch-1"
            ) == content
            assert primary_outbox.pending() == ()
            commit_status = primary_acks.status(session_id, "storage-main", "batch-1")
            assert commit_status is not None and commit_status.committed

            duplicate = await client.ingest_batch(
                group_id="storage-main",
                dataset_id="telemetry",
                batch_id="batch-1",
                idempotency_key="recorder-a:1",
                content=content,
                created_at=NOW,
            )
            assert duplicate.committed
            assert duplicate.result is not None
            assert duplicate.result.state.value == "already-stored"

            handovers = PhaseDHandoverCoordinator(
                control_plane=control,
                outbox=primary_outbox,
                acknowledgements=primary_acks,
            )
            handover = handovers.plan(
                session_id=session_id,
                group_id="storage-main",
                target_provider_id="provider-replica",
                target_term=2,
                target_fencing_token=11,
            )
            committed = handovers.commit(
                handover,
                actor_node_id="coordinator",
                grant_id="grant-2",
                lease_expires_at=NOW + timedelta(minutes=20),
                occurred_at=NOW + timedelta(minutes=1),
            )
            assert committed.target_provider_id == "provider-replica"
            snapshot = control.snapshot(session_id)
            assert snapshot.groups["storage-main"].primary_provider_id == "provider-replica"
            assert snapshot.groups["storage-main"].replica_provider_ids == ("provider-primary",)

            second_content = {
                "source": "recorder-a",
                "observations": [{"sequence": 2, "value": 43.0}],
            }
            second = await client.ingest_batch(
                group_id="storage-main",
                dataset_id="telemetry",
                batch_id="batch-2",
                idempotency_key="recorder-a:2",
                content=second_content,
                created_at=NOW + timedelta(minutes=2),
            )
            assert second.committed
            assert primary_provider.read(
                session_id=session_id, group_id="storage-main", batch_id="batch-2"
            ) == second_content
            assert replica_provider.read(
                session_id=session_id, group_id="storage-main", batch_id="batch-2"
            ) == second_content

            stale_content = {"value": "stale"}
            stale_request = BatchIngestRequest(
                authority=WriteAuthority(
                    session_id=session_id,
                    group_id="storage-main",
                    actor_node_id=primary_node.node_id,
                    grant_id="grant-1",
                    term=1,
                    fencing_token=10,
                    lease_expires_at=NOW + timedelta(minutes=10),
                ),
                dataset_id="telemetry",
                batch_id="batch-stale",
                idempotency_key="stale:1",
                content_hash=BatchIngestRequest.calculate_content_hash(stale_content),
                content=stale_content,
                created_at=NOW + timedelta(minutes=2),
            )
            stale_response = await recorder_endpoint.request(
                target_node_id=primary_node.node_id,
                envelope=StorageRequestEnvelope(
                    request_id="stale-request",
                    protocol=STORAGE_PROTOCOL,
                    protocol_version=STORAGE_PROTOCOL_VERSION,
                    operation=StorageOperation.BATCH_INGEST,
                    session_id=session_id,
                    actor_node_id=recorder.node_id,
                    authorization_context={
                        "kind": "storage-primary-route",
                        "group_id": "storage-main",
                        "provider_id": "provider-primary",
                    },
                    payload=stale_request.to_dict(),
                ),
            )
            assert not stale_response.ok
            assert stale_response.error is not None
            assert stale_response.error.code.value == "not-primary"
        finally:
            await asyncio.gather(
                *(endpoint.close() for endpoint in endpoints), return_exceptions=True
            )
            clients = tuple(
                client
                for client in (recorder, primary_node, replica_node)
                if client is not None
            )
            if clients:
                await asyncio.gather(
                    *(client.disconnect() for client in clients), return_exceptions=True
                )
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())


def test_fencing_counter_survives_revocation(tmp_path: Path) -> None:
    control = PhaseDControlPlane(tmp_path / "control.sqlite3")
    control.create_group("session-1", "coordinator", "storage-main")
    control.register_provider(
        "session-1",
        "coordinator",
        _registration("session-1", "provider-a", "node-a"),
    )
    control.change_assignment("session-1", "coordinator", "storage-main", "provider-a")
    control.grant_leader(
        "session-1",
        "coordinator",
        "storage-main",
        "provider-a",
        "grant-1",
        4,
        104,
        lease_expires_at=NOW + timedelta(minutes=5),
        occurred_at=NOW,
    )
    control.revoke_leader("session-1", "coordinator", "storage-main", "grant-1")

    reconstructed = PhaseDControlPlane(tmp_path / "control.sqlite3")
    with pytest.raises(FederationValidationError) as stale_term:
        reconstructed.grant_leader(
            "session-1",
            "coordinator",
            "storage-main",
            "provider-a",
            "grant-2",
            4,
            105,
            lease_expires_at=NOW + timedelta(minutes=10),
            occurred_at=NOW + timedelta(minutes=1),
        )
    assert stale_term.value.code == "stale-term"


def test_recorder_queue_retains_batch_until_remote_commit(tmp_path: Path) -> None:
    from catalog.federation.phase_d_client import PhaseDIngestOutcome

    class PendingClient:
        async def ingest_batch(self, **_kwargs):
            return PhaseDIngestOutcome(
                committed=False,
                retryable=True,
                error_code="internal-error",
                message="replica offline",
            )

    outbox = SQLiteOutbox(tmp_path / "recorder.sqlite3")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=PendingClient(),
        clock=lambda: NOW,
    )
    queue.enqueue(
        session_id="session-1",
        group_id="storage-main",
        dataset_id="telemetry",
        batch_id="batch-1",
        idempotency_key="recorder:1",
        content={"value": 42},
        created_at=NOW,
    )

    result = asyncio.run(queue.run_once())

    assert result.pending == 1
    pending = outbox.pending()
    assert len(pending) == 1
    assert pending[0].attempt_count == 1
    assert pending[0].last_error == "replica offline"
