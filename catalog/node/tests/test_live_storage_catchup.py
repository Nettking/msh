from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.control_sync import StorageControlPublicationStore
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.live_catchup import (
    LiveCatchupStore,
    LiveFormerPrimaryCatchupCoordinator,
)
from catalog.federation.live_failover import (
    LiveFailoverStore,
    StorageControlRelayChannel,
    StorageFailoverCoordinator,
)
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.node.client import RelayNodeClient
from catalog.node.live_storage_agent import LiveStorageNodeAgent
from catalog.relay.service import RelayServer

from .test_live_storage_failover import (
    NOW,
    TIMEOUT,
    _enroll,
    _registration,
    _wait_for_control_waiting,
    _wait_for_promotion,
    _write_config,
)


def test_live_catchup_repairs_only_missing_batches_and_keeps_node_unassigned(
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
            session = await authority.create_session("Physical federation F4.1")
            session_id = str(session["session_id"])

            primary_config = _write_config(
                tmp_path / "primary",
                relay_url=relay.url,
                session_id=session_id,
                display_name="Former primary storage",
                provider_id="provider-primary",
            )
            replica_config = _write_config(
                tmp_path / "replica",
                relay_url=relay.url,
                session_id=session_id,
                display_name="Promoted storage",
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
            control.create_group(session_id, authority.node_id, "storage-main")
            control.register_provider(
                session_id,
                authority.node_id,
                _registration(session_id, "provider-primary", primary.node_id),
            )
            control.register_provider(
                session_id,
                authority.node_id,
                _registration(session_id, "provider-replica", replica.node_id),
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
            control.grant_leader(
                session_id,
                authority.node_id,
                "storage-main",
                "provider-primary",
                "grant-1",
                1,
                1,
                lease_expires_at=NOW + timedelta(minutes=15),
                occurred_at=NOW,
            )

            authority_endpoint = RelayStorageEndpoint(
                authority,
                request_timeout=TIMEOUT,
            )
            await authority_endpoint.start()
            failover_store_path = tmp_path / "authority-failover.sqlite3"
            failover_store = LiveFailoverStore(failover_store_path)
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
                failover_store=failover_store,
                session_id=session_id,
                lease_seconds=300,
                clock=lambda: NOW,
            )
            await failover.start()

            primary_bootstrap = asyncio.create_task(
                primary.bootstrap(
                    enrollment_token=str(
                        coordinator.create_enrollment_token(
                            ttl_seconds=60, max_uses=1
                        )["token"]
                    ),
                    session_invitation=str(
                        (
                            await authority.create_invitation(
                                session_id, ttl_seconds=60, max_uses=1
                            )
                        )["token"]
                    ),
                )
            )
            replica_bootstrap = asyncio.create_task(
                replica.bootstrap(
                    enrollment_token=str(
                        coordinator.create_enrollment_token(
                            ttl_seconds=60, max_uses=1
                        )["token"]
                    ),
                    session_invitation=str(
                        (
                            await authority.create_invitation(
                                session_id, ttl_seconds=60, max_uses=1
                            )
                        )["token"]
                    ),
                )
            )
            bootstrap_tasks.extend((primary_bootstrap, replica_bootstrap))
            await asyncio.gather(
                _wait_for_control_waiting(primary, primary_bootstrap),
                _wait_for_control_waiting(replica, replica_bootstrap),
            )
            await failover.publish_current((primary.node_id, replica.node_id))
            await asyncio.wait_for(primary_bootstrap, TIMEOUT)
            await asyncio.wait_for(replica_bootstrap, TIMEOUT)

            logical = PhaseDLogicalStorageClient(
                session_id=session_id,
                actor_node_id=authority.node_id,
                control_plane=control,
                transport=authority_endpoint,
                acknowledgements=DurableAcknowledgementStore(
                    tmp_path / "authority-acks.sqlite3"
                ),
                clock=lambda: NOW,
            )
            contents = {
                "batch-1": {"sequence": 1, "phase": "before-failover"},
                "batch-2": {"sequence": 2, "phase": "after-failover"},
                "batch-3": {"sequence": 3, "phase": "after-failover"},
            }
            first = await logical.ingest_batch(
                group_id="storage-main",
                dataset_id="telemetry",
                batch_id="batch-1",
                idempotency_key="f41:batch-1",
                content=contents["batch-1"],
                created_at=NOW,
            )
            assert first.committed
            old_node_id = primary.node_id
            await primary.close()
            promotion = await _wait_for_promotion(failover)
            assert promotion.promoted_provider_id == "provider-replica"

            for batch_id in ("batch-2", "batch-3"):
                committed = await logical.ingest_batch(
                    group_id="storage-main",
                    dataset_id="telemetry",
                    batch_id=batch_id,
                    idempotency_key=f"f41:{batch_id}",
                    content=contents[batch_id],
                    created_at=NOW,
                )
                assert committed.committed

            primary = LiveStorageNodeAgent(
                primary_config,
                control_authority_node_id=authority.node_id,
                control_sync_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await asyncio.wait_for(primary.bootstrap(), TIMEOUT)
            assert primary.node_id == old_node_id
            assert primary.status()["provider"]["groups"] == [
                {"group_id": "storage-main", "role": "unassigned"}
            ]
            assert primary.provider.read(
                session_id=session_id,
                group_id="storage-main",
                batch_id="batch-1",
            ) == contents["batch-1"]
            assert primary.provider.read(
                session_id=session_id,
                group_id="storage-main",
                batch_id="batch-2",
            ) is None

            catchup_path = tmp_path / "authority-catchup.sqlite3"
            catchup = LiveFormerPrimaryCatchupCoordinator(
                session_coordinator=coordinator,
                control_plane=control,
                failover_store=failover_store,
                transport=authority_endpoint,
                credentials=authority.credentials,
                catchup_store=LiveCatchupStore(catchup_path),
                session_id=session_id,
                clock=lambda: NOW,
            )
            partial = await catchup.run_once(
                group_id="storage-main",
                returning_provider_id="provider-primary",
                limit=1,
            )
            assert partial.status == "retryable"
            assert partial.attempted == 1
            assert partial.delivered == 1
            assert partial.record is not None
            assert sum(
                item.status == "missing" for item in partial.record.items
            ) == 1

            restarted = LiveFormerPrimaryCatchupCoordinator(
                session_coordinator=coordinator,
                control_plane=PhaseDControlPlane(
                    tmp_path / "authority-storage-control.sqlite3"
                ),
                failover_store=LiveFailoverStore(failover_store_path),
                transport=authority_endpoint,
                credentials=authority.credentials,
                catchup_store=LiveCatchupStore(catchup_path),
                session_id=session_id,
                clock=lambda: NOW,
            )
            completed = await restarted.run_once(
                group_id="storage-main",
                returning_provider_id="provider-primary",
            )
            assert completed.status == "caught-up"
            assert completed.attempted == 1
            assert completed.delivered == 1
            assert completed.record is not None
            assert completed.record.state == "caught-up"
            assert completed.record.final_report_revision is not None
            assert all(
                item.status == "verified" for item in completed.record.items
            )
            assert sorted(item.attempt_count for item in completed.record.items) == [
                0,
                1,
                1,
            ]

            for batch_id, content in contents.items():
                assert primary.provider.read(
                    session_id=session_id,
                    group_id="storage-main",
                    batch_id=batch_id,
                ) == content

            snapshot = control.snapshot(session_id)
            assignment = snapshot.groups["storage-main"]
            assert assignment.primary_provider_id == "provider-replica"
            assert assignment.replica_provider_ids == ()
            assert control.acknowledgement_policy(
                session_id, "storage-main"
            ).mode is AcknowledgementMode.PRIMARY
            assert control.storage_degraded_state(
                session_id, "storage-main"
            ) is not None
            assert primary.status()["provider"]["groups"] == [
                {"group_id": "storage-main", "role": "unassigned"}
            ]
            assessment = control.latest_storage_replica_assessment(
                session_id,
                "storage-main",
                "provider-primary",
            )
            assert assessment is not None
            assert assessment.accepted
            assert assessment.report is not None
            assert assessment.report.integrity_verified
            assert assessment.report.synchronization_state == "synchronized"
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
            elif authority_endpoint is not None:
                await authority_endpoint.close()
            if authority is not None and authority.connected_event.is_set():
                await authority.disconnect()
            if relay is not None:
                await relay.close()

    asyncio.run(scenario())
