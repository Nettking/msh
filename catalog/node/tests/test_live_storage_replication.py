from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.control_relay import FramedStorageControlRelayPublisher
from catalog.federation.control_sync import (
    StorageControlPlan,
    StorageControlPublicationStore,
    StorageControlReplicaStore,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.node.client import RelayNodeClient
from catalog.node.identity import IdentityStore
from catalog.node.live_storage_agent import LiveStorageNodeAgent
from catalog.node.storage_agent import STORAGE_NODE_CONFIG_SCHEMA, StorageNodeConfig
from catalog.relay.service import RelayServer

NOW = datetime(2026, 7, 31, 1, 0, tzinfo=timezone.utc)
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
                    "storage bootstrap completed without persisted control or waiting state"
                )
        if waiter not in done and not agent.control_waiting_event.is_set():
            raise TimeoutError("storage node did not reach control waiting state")
    finally:
        if not waiter.done():
            waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


def test_signed_control_plan_is_idempotent_and_tampering_fails_closed(
    tmp_path: Path,
) -> None:
    authority = IdentityStore(
        tmp_path / "authority",
        display_name="Authority",
    ).create(now=NOW)
    control = PhaseDControlPlane(tmp_path / "authority-control.sqlite3")
    control.create_group("session-1", authority.identity.node_id, "storage-main")
    control.register_provider(
        "session-1",
        authority.identity.node_id,
        _registration("session-1", "provider-a", "node-a"),
    )
    control.change_assignment(
        "session-1",
        authority.identity.node_id,
        "storage-main",
        "provider-a",
    )
    control.grant_leader(
        "session-1",
        authority.identity.node_id,
        "storage-main",
        "provider-a",
        "grant-1",
        1,
        1,
        lease_expires_at=NOW + timedelta(minutes=15),
        occurred_at=NOW,
    )
    publications = StorageControlPublicationStore(
        tmp_path / "publications.sqlite3"
    )
    plan = publications.issue(control, authority, "session-1", now=NOW)

    replica_control = PhaseDControlPlane(tmp_path / "replica-control.sqlite3")
    replica = StorageControlReplicaStore(tmp_path / "replica-control.sqlite3")
    first = replica.apply(
        plan,
        replica_control,
        expected_authority_node_id=authority.identity.node_id,
        now=NOW,
    )
    duplicate = replica.apply(
        plan,
        replica_control,
        expected_authority_node_id=authority.identity.node_id,
        now=NOW,
    )

    assert first.status == "applied"
    assert duplicate.status == "duplicate"
    snapshot = replica_control.snapshot("session-1")
    assert snapshot.groups["storage-main"].primary_provider_id == "provider-a"
    assert snapshot.leader_grants["storage-main"]["grant_id"] == "grant-1"

    tampered = plan.to_dict()
    tampered["groups"][0]["primary_provider_id"] = None
    with pytest.raises(FederationValidationError) as rejected:
        StorageControlPlan.from_dict(tampered).verify(
            expected_authority_node_id=authority.identity.node_id
        )
    assert rejected.value.code == "storage-control-hash-mismatch"


def test_two_live_storage_nodes_replicate_and_replica_restarts(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        authority: RelayNodeClient | None = None
        primary: LiveStorageNodeAgent | None = None
        replica: LiveStorageNodeAgent | None = None
        authority_endpoint: RelayStorageEndpoint | None = None
        bootstrap_tasks: list[asyncio.Task[None]] = []
        try:
            relay = RelayServer(
                SessionCoordinator(
                    tmp_path / "relay" / "control.sqlite3",
                    clock=lambda: NOW,
                ),
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
                display_name="Storage coordinator",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await _enroll(relay, authority)
            session = await authority.create_session("Physical federation F2")
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

            authority_control = PhaseDControlPlane(
                tmp_path / "authority-storage-control.sqlite3"
            )
            authority_control.create_group(
                session_id,
                authority.node_id,
                "storage-main",
            )
            authority_control.register_provider(
                session_id,
                authority.node_id,
                _registration(
                    session_id,
                    "provider-primary",
                    primary.node_id,
                ),
            )
            authority_control.register_provider(
                session_id,
                authority.node_id,
                _registration(
                    session_id,
                    "provider-replica",
                    replica.node_id,
                ),
            )
            authority_control.change_assignment(
                session_id,
                authority.node_id,
                "storage-main",
                "provider-primary",
                ("provider-replica",),
            )
            authority_control.set_acknowledgement_policy(
                session_id,
                "storage-main",
                AcknowledgementMode.ONE_REPLICA,
            )
            authority_control.grant_leader(
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
            plan = StorageControlPublicationStore(
                tmp_path / "authority-publications.sqlite3"
            ).issue(
                authority_control,
                authority.credentials,
                session_id,
                now=NOW,
            )

            primary_enrollment = relay.coordinator.create_enrollment_token(
                ttl_seconds=60,
                max_uses=1,
            )["token"]
            replica_enrollment = relay.coordinator.create_enrollment_token(
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

            published = await FramedStorageControlRelayPublisher(
                authority,
                timeout=TIMEOUT,
            ).publish(
                plan,
                (primary.node_id, replica.node_id),
            )
            assert published.acknowledged_node_ids == tuple(
                sorted((primary.node_id, replica.node_id))
            )
            await asyncio.wait_for(primary_bootstrap, TIMEOUT)
            await asyncio.wait_for(replica_bootstrap, TIMEOUT)

            assert primary.status()["provider"]["groups"] == [
                {"group_id": "storage-main", "role": "primary"}
            ]
            assert replica.status()["provider"]["groups"] == [
                {"group_id": "storage-main", "role": "replica"}
            ]

            authority_endpoint = RelayStorageEndpoint(
                authority,
                request_timeout=TIMEOUT,
            )
            await authority_endpoint.start()
            logical = PhaseDLogicalStorageClient(
                session_id=session_id,
                actor_node_id=authority.node_id,
                control_plane=authority_control,
                transport=authority_endpoint,
                acknowledgements=DurableAcknowledgementStore(
                    tmp_path / "authority-acks.sqlite3"
                ),
                clock=lambda: NOW,
            )
            content = {
                "source": "f2-live-replication",
                "observations": [{"sequence": 1, "value": 42.0}],
            }
            outcome = await logical.ingest_batch(
                group_id="storage-main",
                dataset_id="telemetry",
                batch_id="batch-1",
                idempotency_key="f2:batch-1",
                content=content,
                created_at=NOW,
            )
            assert outcome.committed
            assert primary.provider.read(
                session_id=session_id,
                group_id="storage-main",
                batch_id="batch-1",
            ) == content
            assert replica.provider.read(
                session_id=session_id,
                group_id="storage-main",
                batch_id="batch-1",
            ) == content
            authority_manifest = authority_control.manifest(
                session_id,
                "storage-main",
            )
            assert authority_manifest.revision == 1
            assert authority_manifest.items[0].item_id == "batch-1"

            stable_replica_id = replica.node_id
            await replica.close()
            replica = LiveStorageNodeAgent(
                replica_config,
                control_authority_node_id=authority.node_id,
                control_sync_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await replica.bootstrap()
            assert replica.node_id == stable_replica_id
            assert replica.provider.read(
                session_id=session_id,
                group_id="storage-main",
                batch_id="batch-1",
            ) == content
            assert replica.status()["control_sync"]["publication_revision"] == 1
        finally:
            for task in bootstrap_tasks:
                if not task.done():
                    task.cancel()
            if bootstrap_tasks:
                await asyncio.gather(*bootstrap_tasks, return_exceptions=True)
            if authority_endpoint is not None:
                await authority_endpoint.close()
            if primary is not None:
                await primary.close()
            if replica is not None:
                await replica.close()
            if authority is not None:
                await authority.disconnect()
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())
