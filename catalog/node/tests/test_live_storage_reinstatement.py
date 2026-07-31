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
from catalog.federation.live_reinstatement import (
    LiveFormerPrimaryReinstatementCoordinator,
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


def test_live_reinstatement_restores_replica_and_acknowledgement_policy(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        authority: RelayNodeClient | None = None
        authority_endpoint: RelayStorageEndpoint | None = None
        failover: StorageFailoverCoordinator | None = None
        former_primary: LiveStorageNodeAgent | None = None
        promoted: LiveStorageNodeAgent | None = None
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
            session = await authority.create_session("Physical federation F4.2")
            session_id = str(session["session_id"])

            former_config = _write_config(
                tmp_path / "former-primary",
                relay_url=relay.url,
                session_id=session_id,
                display_name="Former primary storage",
                provider_id="provider-primary",
            )
            promoted_config = _write_config(
                tmp_path / "promoted",
                relay_url=relay.url,
                session_id=session_id,
                display_name="Promoted storage",
                provider_id="provider-replica",
            )
            former_primary = LiveStorageNodeAgent(
                former_config,
                control_authority_node_id=authority.node_id,
                control_sync_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            promoted = LiveStorageNodeAgent(
                promoted_config,
                control_authority_node_id=authority.node_id,
                control_sync_timeout=TIMEOUT,
                clock=lambda: NOW,
            )

            control_path = tmp_path / "authority-storage-control.sqlite3"
            control = PhaseDControlPlane(control_path)
            control.create_group(session_id, authority.node_id, "storage-main")
            control.register_provider(
                session_id,
                authority.node_id,
                _registration(
                    session_id,
                    "provider-primary",
                    former_primary.node_id,
                ),
            )
            control.register_provider(
                session_id,
                authority.node_id,
                _registration(
                    session_id,
                    "provider-replica",
                    promoted.node_id,
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
            publication_store = StorageControlPublicationStore(
                tmp_path / "authority-publications.sqlite3"
            )
            failover_store_path = tmp_path / "authority-failover.sqlite3"
            failover_store = LiveFailoverStore(failover_store_path)
            channel = StorageControlRelayChannel(
                authority,
                authority_endpoint,
                timeout=TIMEOUT,
            )
            failover = StorageFailoverCoordinator(
                session_coordinator=coordinator,
                control_plane=control,
                publication_store=publication_store,
                credentials=authority.credentials,
                channel=channel,
                failover_store=failover_store,
                session_id=session_id,
                lease_seconds=300,
                clock=lambda: NOW,
            )
            await failover.start()

            former_bootstrap = asyncio.create_task(
                former_primary.bootstrap(
                    enrollment_token=str(
                        coordinator.create_enrollment_token(
                            ttl_seconds=60,
                            max_uses=1,
                        )["token"]
                    ),
                    session_invitation=str(
                        (
                            await authority.create_invitation(
                                session_id,
                                ttl_seconds=60,
                                max_uses=1,
                            )
                        )["token"]
                    ),
                )
            )
            promoted_bootstrap = asyncio.create_task(
                promoted.bootstrap(
                    enrollment_token=str(
                        coordinator.create_enrollment_token(
                            ttl_seconds=60,
                            max_uses=1,
                        )["token"]
                    ),
                    session_invitation=str(
                        (
                            await authority.create_invitation(
                                session_id,
                                ttl_seconds=60,
                                max_uses=1,
                            )
                        )["token"]
                    ),
                )
            )
            bootstrap_tasks.extend((former_bootstrap, promoted_bootstrap))
            await asyncio.gather(
                _wait_for_control_waiting(former_primary, former_bootstrap),
                _wait_for_control_waiting(promoted, promoted_bootstrap),
            )
            await failover.publish_current(
                (former_primary.node_id, promoted.node_id)
            )
            await asyncio.wait_for(former_bootstrap, TIMEOUT)
            await asyncio.wait_for(promoted_bootstrap, TIMEOUT)

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
            contents = {
                "batch-1": {"sequence": 1, "phase": "before-failover"},
                "batch-2": {"sequence": 2, "phase": "after-failover"},
                "batch-3": {"sequence": 3, "phase": "after-failover"},
                "batch-4": {"sequence": 4, "phase": "admission-race"},
                "batch-5": {"sequence": 5, "phase": "restored-redundancy"},
            }
            first = await logical.ingest_batch(
                group_id="storage-main",
                dataset_id="telemetry",
                batch_id="batch-1",
                idempotency_key="f42:batch-1",
                content=contents["batch-1"],
                created_at=NOW,
            )
            assert first.committed
            old_node_id = former_primary.node_id
            await former_primary.close()
            promotion = await _wait_for_promotion(failover)
            assert promotion.promoted_provider_id == "provider-replica"

            for batch_id in ("batch-2", "batch-3"):
                committed = await logical.ingest_batch(
                    group_id="storage-main",
                    dataset_id="telemetry",
                    batch_id=batch_id,
                    idempotency_key=f"f42:{batch_id}",
                    content=contents[batch_id],
                    created_at=NOW,
                )
                assert committed.committed

            former_primary = LiveStorageNodeAgent(
                former_config,
                control_authority_node_id=authority.node_id,
                control_sync_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await asyncio.wait_for(former_primary.bootstrap(), TIMEOUT)
            assert former_primary.node_id == old_node_id
            assert former_primary.status()["provider"]["groups"] == [
                {"group_id": "storage-main", "role": "unassigned"}
            ]

            catchup_store = LiveCatchupStore(
                tmp_path / "authority-catchup.sqlite3"
            )
            catchup = LiveFormerPrimaryCatchupCoordinator(
                session_coordinator=coordinator,
                control_plane=control,
                failover_store=failover_store,
                transport=authority_endpoint,
                credentials=authority.credentials,
                catchup_store=catchup_store,
                session_id=session_id,
                clock=lambda: NOW,
            )
            caught_up = await catchup.run_once(
                group_id="storage-main",
                returning_provider_id="provider-primary",
            )
            assert caught_up.status == "caught-up", caught_up.to_dict()

            reinstatement = LiveFormerPrimaryReinstatementCoordinator(
                control_plane=control,
                publication_store=publication_store,
                failover_store=failover_store,
                catchup_store=catchup_store,
                catchup=catchup,
                channel=channel,
                credentials=authority.credentials,
                session_id=session_id,
                clock=lambda: NOW,
            )

            original_publish = channel.publish
            admission_write_injected = False

            async def publish_with_admission_write(plan, target_node_ids):
                nonlocal admission_write_injected
                group = next(
                    item
                    for item in plan.groups
                    if item["group_id"] == "storage-main"
                )
                if (
                    not admission_write_injected
                    and "provider-primary" in group["replica_provider_ids"]
                    and group["acknowledgement_mode"] == "primary"
                ):
                    # The coordinator assignment is committed, but the returning
                    # node has not applied it yet. A primary-only write may commit
                    # while that node still rejects normal replica traffic.
                    admission_write_injected = True
                    committed = await logical.ingest_batch(
                        group_id="storage-main",
                        dataset_id="telemetry",
                        batch_id="batch-4",
                        idempotency_key="f42:batch-4",
                        content=contents["batch-4"],
                        created_at=NOW,
                    )
                    assert committed.committed
                    admission_status = acknowledgements.status(
                        session_id, "storage-main", "batch-4"
                    )
                    assert admission_status is not None
                    assert admission_status.required_replica_acks == 0
                    assert admission_status.acknowledged_replica_ids == ()
                return await original_publish(plan, target_node_ids)

            channel.publish = publish_with_admission_write
            first_reinstatement = await reinstatement.run_once(
                group_id="storage-main",
                returning_provider_id="provider-primary",
            )
            channel.publish = original_publish
            assert admission_write_injected
            assert first_reinstatement.status == "retryable", (
                first_reinstatement.to_dict()
            )
            assert (
                first_reinstatement.code
                == "reinstatement-tail-catchup-required"
            )
            after_rollback = control.snapshot(session_id).groups["storage-main"]
            assert after_rollback.primary_provider_id == "provider-replica"
            assert after_rollback.replica_provider_ids == ()
            assert control.acknowledgement_policy(
                session_id,
                "storage-main",
            ).mode is AcknowledgementMode.PRIMARY
            assert control.storage_degraded_state(
                session_id,
                "storage-main",
            ) is not None
            assert former_primary.status()["provider"]["groups"] == [
                {"group_id": "storage-main", "role": "unassigned"}
            ]

            restarted = LiveFormerPrimaryReinstatementCoordinator(
                control_plane=PhaseDControlPlane(control_path),
                publication_store=StorageControlPublicationStore(
                    tmp_path / "authority-publications.sqlite3"
                ),
                failover_store=LiveFailoverStore(failover_store_path),
                catchup_store=LiveCatchupStore(
                    tmp_path / "authority-catchup.sqlite3"
                ),
                catchup=LiveFormerPrimaryCatchupCoordinator(
                    session_coordinator=coordinator,
                    control_plane=PhaseDControlPlane(control_path),
                    failover_store=LiveFailoverStore(failover_store_path),
                    transport=authority_endpoint,
                    credentials=authority.credentials,
                    catchup_store=LiveCatchupStore(
                        tmp_path / "authority-catchup.sqlite3"
                    ),
                    session_id=session_id,
                    clock=lambda: NOW,
                ),
                channel=channel,
                credentials=authority.credentials,
                session_id=session_id,
                clock=lambda: NOW,
            )
            completed = await restarted.run_once(
                group_id="storage-main",
                returning_provider_id="provider-primary",
            )
            assert completed.status == "completed", completed.to_dict()
            assert completed.record is not None
            assert completed.record.state == "completed"
            assert completed.record.final_report_revision is not None
            assert completed.record.final_report_hash is not None
            assert completed.record.final_publication is not None

            snapshot = control.snapshot(session_id)
            assignment = snapshot.groups["storage-main"]
            assert assignment.primary_provider_id == "provider-replica"
            assert assignment.replica_provider_ids == ("provider-primary",)
            grant = snapshot.leader_grants["storage-main"]
            assert grant["provider_id"] == "provider-replica"
            assert grant["term"] == promotion.term
            assert grant["fencing_token"] == promotion.fencing_token
            assert control.acknowledgement_policy(
                session_id,
                "storage-main",
            ).mode is AcknowledgementMode.ONE_REPLICA
            assert control.storage_degraded_state(
                session_id,
                "storage-main",
            ) is None
            assert former_primary.status()["provider"]["groups"] == [
                {"group_id": "storage-main", "role": "replica"}
            ]
            assessment = control.latest_storage_replica_assessment(
                session_id,
                "storage-main",
                "provider-primary",
            )
            assert assessment is not None
            assert assessment.accepted
            assert assessment.eligibility

            final_write = await logical.ingest_batch(
                group_id="storage-main",
                dataset_id="telemetry",
                batch_id="batch-5",
                idempotency_key="f42:batch-5",
                content=contents["batch-5"],
                created_at=NOW,
            )
            assert final_write.committed
            final_status = acknowledgements.status(
                session_id, "storage-main", "batch-5"
            )
            assert final_status is not None
            assert final_status.required_replica_acks == 1
            assert final_status.acknowledged_replica_ids == (
                "provider-primary",
            )
            for agent in (former_primary, promoted):
                assert agent.provider.read(
                    session_id=session_id,
                    group_id="storage-main",
                    batch_id="batch-5",
                ) == contents["batch-5"]
                assert agent.storage.inbound_listener_ports == ()

            replay = await restarted.run_once(
                group_id="storage-main",
                returning_provider_id="provider-primary",
            )
            assert replay.status == "completed"
            assert replay.reinstatement_id == completed.reinstatement_id
        finally:
            for task in bootstrap_tasks:
                if not task.done():
                    task.cancel()
            if bootstrap_tasks:
                await asyncio.gather(*bootstrap_tasks, return_exceptions=True)
            if former_primary is not None:
                await former_primary.close()
            if promoted is not None:
                await promoted.close()
            if failover is not None:
                await failover.close()
            elif authority_endpoint is not None:
                await authority_endpoint.close()
            if authority is not None and authority.connected_event.is_set():
                await authority.disconnect()
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())
