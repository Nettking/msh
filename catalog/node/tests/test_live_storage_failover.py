from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.control_sync import StorageControlPublicationStore
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.live_failover import (
    LiveFailoverStore,
    StorageControlRelayChannel,
    StorageFailoverCoordinator,
)
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    WriteAuthority,
)
from catalog.node.client import RelayNodeClient
from catalog.node.live_storage_agent import LiveStorageNodeAgent
from catalog.node.storage_agent import STORAGE_NODE_CONFIG_SCHEMA, StorageNodeConfig
from catalog.relay.service import RelayServer

NOW = datetime(2026, 7, 31, 8, 0, tzinfo=timezone.utc)
TIMEOUT = 5.0


def _write_config(
    root: Path,
    *,
    relay_url: str,
    session_id: str,
    display_name: str,
    provider_id: str,
) -> StorageNodeConfig:
    root.mkdir(parents=True, exist_ok=True)
    path = root / "storage-node.json"
    path.write_text(
        json.dumps(
            {
                "schema": STORAGE_NODE_CONFIG_SCHEMA,
                "state_directory": "state",
                "relay_url": relay_url,
                "display_name": display_name,
                "provider_id": provider_id,
                "session_id": session_id,
                "control_database": "state/control.sqlite3",
                "storage_directory": "state/storage",
                "outbox_database": "state/outbox.sqlite3",
                "acknowledgements_database": "state/acks.sqlite3",
                "allow_insecure_local": True,
                "heartbeat_interval": 300,
                "request_timeout": TIMEOUT,
            }
        ),
        encoding="utf-8",
    )
    return StorageNodeConfig.load(path)


def _registration(
    session_id: str,
    provider_id: str,
    node_id: str,
) -> StorageProviderRegistration:
    return StorageProviderRegistration(
        session_id=session_id,
        provider_id=provider_id,
        node_id=node_id,
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        authorized=True,
        status="ready",
    )


async def _enroll(relay: RelayServer, client: RelayNodeClient) -> None:
    token = relay.coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    await client.connect(enrollment_token=str(token))


async def _wait_for_control_waiting(
    agent: LiveStorageNodeAgent,
    bootstrap: asyncio.Task[None],
) -> None:
    waiter = asyncio.create_task(agent.control_waiting_event.wait())
    try:
        done, _pending = await asyncio.wait(
            {waiter, bootstrap},
            timeout=TIMEOUT,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if bootstrap in done:
            await bootstrap
            if not agent.control_waiting_event.is_set():
                raise AssertionError(
                    "storage bootstrap completed without trusted control"
                )
        if waiter not in done and not agent.control_waiting_event.is_set():
            raise TimeoutError("storage node did not enter control waiting state")
    finally:
        if not waiter.done():
            waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def _wait_for_promotion(
    failover: StorageFailoverCoordinator,
) -> object:
    for _ in range(40):
        results = await failover.scan_once()
        promoted = next(
            (result for result in results if result.status == "promoted"),
            None,
        )
        if promoted is not None:
            return promoted
        await asyncio.sleep(0.05)
    raise TimeoutError("automatic storage promotion did not complete")


def test_primary_loss_promotes_complete_replica_and_old_grant_stays_fenced(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        authority: RelayNodeClient | None = None
        authority_endpoint: RelayStorageEndpoint | None = None
        failover: StorageFailoverCoordinator | None = None
        primary: LiveStorageNodeAgent | None = None
        replica: LiveStorageNodeAgent | None = None
        bootstrap_tasks: list[asyncio.Task[None]] = []
        try:
            coordinator = SessionCoordinator(
                tmp_path / "relay" / "control.sqlite3",
                clock=lambda: NOW,
            )
            relay = RelayServer(
                coordinator,
                host="127.0.0.1",
                port=0,
                auth_timeout_seconds=TIMEOUT,
                send_timeout_seconds=TIMEOUT,
                heartbeat_timeout_seconds=300,
                sweep_interval_seconds=300,
            )
            await relay.start()
            authority = RelayNodeClient(
                state_directory=tmp_path / "authority-node",
                relay_url=relay.url,
                display_name="Storage authority",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await _enroll(relay, authority)
            session = await authority.create_session("Physical federation F3")
            session_id = str(session["session_id"])

            primary_config = _write_config(
                tmp_path / "primary",
                relay_url=relay.url,
                session_id=session_id,
                display_name="Primary storage",
                provider_id="provider-primary",
            )
            replica_config = _write_config(
                tmp_path / "replica",
                relay_url=relay.url,
                session_id=session_id,
                display_name="Replica storage",
                provider_id="provider-replica",
            )
            primary = LiveStorageNodeAgent(
                primary_config,
                control_authority_node_id=authority.node_id,
                control_sync_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            replica = LiveStorageNodeAgent(
                replica_config,
                control_authority_node_id=authority.node_id,
                control_sync_timeout=TIMEOUT,
                clock=lambda: NOW,
            )

            control = PhaseDControlPlane(
                tmp_path / "authority-storage-control.sqlite3"
            )
            control.create_group(
                session_id,
                authority.node_id,
                "storage-main",
            )
            control.register_provider(
                session_id,
                authority.node_id,
                _registration(
                    session_id,
                    "provider-primary",
                    primary.node_id,
                ),
            )
            control.register_provider(
                session_id,
                authority.node_id,
                _registration(
                    session_id,
                    "provider-replica",
                    replica.node_id,
                ),
            )
            control.change_assignment(
                session_id,
                authority.node_id,
                "storage-main",
                "provider-primary",
                ("provider-replica",),
            )
            control.set_acknowledgement_policy(
                session_id,
                "storage-main",
                AcknowledgementMode.ONE_REPLICA,
            )
            old_lease = NOW + timedelta(minutes=15)
            control.grant_leader(
                session_id,
                authority.node_id,
                "storage-main",
                "provider-primary",
                "grant-1",
                1,
                1,
                lease_expires_at=old_lease,
                occurred_at=NOW,
            )
            pre_failover_revision = control.snapshot(session_id).revision

            authority_endpoint = RelayStorageEndpoint(
                authority,
                request_timeout=TIMEOUT,
            )
            await authority_endpoint.start()
            failover_store_path = tmp_path / "authority-failover.sqlite3"
            failover = StorageFailoverCoordinator(
                session_coordinator=coordinator,
                control_plane=control,
                publication_store=StorageControlPublicationStore(
                    tmp_path / "authority-publications.sqlite3"
                ),
                credentials=authority.credentials,
                channel=StorageControlRelayChannel(
                    authority,
                    authority_endpoint,
                    timeout=TIMEOUT,
                ),
                failover_store=LiveFailoverStore(failover_store_path),
                session_id=session_id,
                lease_seconds=300,
                clock=lambda: NOW,
            )
            await failover.start()

            primary_enrollment = coordinator.create_enrollment_token(
                ttl_seconds=60,
                max_uses=1,
            )["token"]
            replica_enrollment = coordinator.create_enrollment_token(
                ttl_seconds=60,
                max_uses=1,
            )["token"]
            primary_invitation = await authority.create_invitation(
                session_id,
                ttl_seconds=60,
                max_uses=1,
            )
            replica_invitation = await authority.create_invitation(
                session_id,
                ttl_seconds=60,
                max_uses=1,
            )
            primary_bootstrap = asyncio.create_task(
                primary.bootstrap(
                    enrollment_token=str(primary_enrollment),
                    session_invitation=str(primary_invitation["token"]),
                )
            )
            replica_bootstrap = asyncio.create_task(
                replica.bootstrap(
                    enrollment_token=str(replica_enrollment),
                    session_invitation=str(replica_invitation["token"]),
                )
            )
            bootstrap_tasks.extend((primary_bootstrap, replica_bootstrap))
            await asyncio.gather(
                _wait_for_control_waiting(primary, primary_bootstrap),
                _wait_for_control_waiting(replica, replica_bootstrap),
            )
            initial_plan = await failover.publish_current(
                (primary.node_id, replica.node_id)
            )
            assert initial_plan.publication_revision == 1
            await asyncio.wait_for(primary_bootstrap, TIMEOUT)
            await asyncio.wait_for(replica_bootstrap, TIMEOUT)

            acknowledgements = DurableAcknowledgementStore(
                tmp_path / "authority-acks.sqlite3"
            )
            logical = PhaseDLogicalStorageClient(
                session_id=session_id,
                actor_node_id=authority.node_id,
                control_plane=control,
                transport=authority_endpoint,
                acknowledgements=acknowledgements,
                clock=lambda: NOW,
            )
            first_content = {
                "source": "f3-before-failover",
                "observations": [{"sequence": 1, "value": 42.0}],
            }
            first = await logical.ingest_batch(
                group_id="storage-main",
                dataset_id="telemetry",
                batch_id="batch-1",
                idempotency_key="f3:batch-1",
                content=first_content,
                created_at=NOW,
            )
            assert first.committed
            manifest = control.manifest(session_id, "storage-main")
            assert manifest.revision == 1
            assert manifest.items[0].acknowledged_provider_ids == (
                "provider-primary",
                "provider-replica",
            )

            old_primary_node_id = primary.node_id
            await primary.close()
            promotion = await _wait_for_promotion(failover)
            assert promotion.promoted_provider_id == "provider-replica"
            assert promotion.failed_provider_id == "provider-primary"
            assert promotion.term == 2
            assert promotion.fencing_token == 2

            assessment = control.latest_storage_replica_assessment(
                session_id,
                "storage-main",
                "provider-replica",
            )
            assert assessment is not None
            assert assessment.accepted
            assert assessment.eligibility
            assert assessment.report is not None
            assert assessment.report.integrity_verified
            assert assessment.report.manifest_revision == manifest.revision
            assert assessment.report.manifest_hash == manifest.manifest_hash

            promoted_snapshot = control.snapshot(session_id)
            assert promoted_snapshot.revision > pre_failover_revision
            assignment = promoted_snapshot.groups["storage-main"]
            grant = promoted_snapshot.leader_grants["storage-main"]
            assert assignment.primary_provider_id == "provider-replica"
            assert assignment.replica_provider_ids == ()
            assert grant["provider_id"] == "provider-replica"
            assert grant["term"] == 2
            assert grant["fencing_token"] == 2
            assert control.acknowledgement_policy(
                session_id,
                "storage-main",
            ).mode is AcknowledgementMode.PRIMARY
            degraded = control.storage_degraded_state(
                session_id,
                "storage-main",
            )
            assert degraded is not None
            assert degraded["reason_code"] == (
                "automatic-failover-redundancy-lost"
            )
            assert replica.status()["provider"]["groups"] == [
                {"group_id": "storage-main", "role": "primary"}
            ]

            second_content = {
                "source": "f3-after-failover",
                "observations": [{"sequence": 2, "value": 43.0}],
            }
            second = await logical.ingest_batch(
                group_id="storage-main",
                dataset_id="telemetry",
                batch_id="batch-2",
                idempotency_key="f3:batch-2",
                content=second_content,
                created_at=NOW,
            )
            assert second.committed
            assert replica.provider.read(
                session_id=session_id,
                group_id="storage-main",
                batch_id="batch-2",
            ) == second_content

            observation_id = StorageFailoverCoordinator._observation_id(
                session_id,
                "storage-main",
                "provider-primary",
                "grant-1",
            )
            failover_id = StorageFailoverCoordinator._failover_id(
                observation_id,
                "provider-replica",
                assessment.report_hash,
            )
            durable = LiveFailoverStore(failover_store_path).get(
                session_id,
                "storage-main",
                failover_id,
            )
            assert durable is not None
            assert durable.state == "published"
            assert durable.publication is not None
            assert durable.selected_report_revision == assessment.report_revision
            assert durable.selected_report_hash == assessment.report_hash

            primary = LiveStorageNodeAgent(
                primary_config,
                control_authority_node_id=authority.node_id,
                control_sync_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await asyncio.wait_for(primary.bootstrap(), TIMEOUT)
            assert primary.node_id == old_primary_node_id
            assert primary.status()["provider"]["groups"] == [
                {"group_id": "storage-main", "role": "unassigned"}
            ]
            assert primary.status()["control_sync"]["ready"]

            stale_request = BatchIngestRequest(
                authority=WriteAuthority(
                    session_id=session_id,
                    group_id="storage-main",
                    actor_node_id=old_primary_node_id,
                    grant_id="grant-1",
                    term=1,
                    fencing_token=1,
                    lease_expires_at=old_lease,
                ),
                dataset_id="telemetry",
                batch_id="stale-batch",
                idempotency_key="f3:stale-batch",
                content_hash=BatchIngestRequest.calculate_content_hash(
                    {"stale": True}
                ),
                content={"stale": True},
                created_at=NOW,
            )
            stale_response = await authority_endpoint.request(
                target_node_id=old_primary_node_id,
                envelope=StorageRequestEnvelope(
                    request_id="stale-primary-authority",
                    protocol=STORAGE_PROTOCOL,
                    protocol_version=STORAGE_PROTOCOL_VERSION,
                    operation=StorageOperation.BATCH_INGEST,
                    session_id=session_id,
                    actor_node_id=authority.node_id,
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
            assert stale_response.error.code is StorageErrorCode.NOT_PRIMARY
            assert primary.storage.inbound_listener_ports == ()
            assert replica.storage.inbound_listener_ports == ()
        finally:
            for task in bootstrap_tasks:
                if not task.done():
                    task.cancel()
            if bootstrap_tasks:
                await asyncio.gather(*bootstrap_tasks, return_exceptions=True)
            if primary is not None:
                await primary.close()
            if replica is not None:
                await replica.close()
            if failover is not None:
                await failover.close()
            if authority_endpoint is not None:
                await authority_endpoint.close()
            if authority is not None and authority.connected_event.is_set():
                await authority.disconnect()
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())
