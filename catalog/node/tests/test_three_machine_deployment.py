from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.control_relay import FramedStorageControlRelayPublisher
from catalog.federation.control_sync import StorageControlPublicationStore
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.node.client import RelayNodeClient
from catalog.node.live_storage_agent import LiveStorageNodeAgent
from catalog.node.storage_deployment import (
    EVIDENCE_SCHEMA,
    DeploymentNode,
    ThreeMachineDeployment,
    ensure_initial_control,
    render_storage_config,
    verify_evidence,
)
from catalog.relay.service import RelayServer

NOW = datetime(2026, 7, 31, 13, 0, tzinfo=timezone.utc)
TIMEOUT = 5.0


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
        elif waiter not in done and not agent.control_waiting_event.is_set():
            raise TimeoutError("storage node did not reach control waiting state")
    finally:
        if not waiter.done():
            waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


def _storage_evidence(
    deployment: ThreeMachineDeployment,
    agent: LiveStorageNodeAgent,
    *,
    role: str,
    stage: str,
    batch_id: str,
) -> dict[str, object]:
    identity = agent.provider.committed_identity(
        session_id=deployment.session_id,
        group_id=deployment.group_id,
        batch_id=batch_id,
    )
    return {
        "schema": EVIDENCE_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "role": role,
        "stage": stage,
        "captured_at": NOW.isoformat().replace("+00:00", "Z"),
        "node_id": agent.node_id,
        "provider_id": agent.storage.config.provider_id,
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "status": agent.status(),
        "batch": {
            "batch_id": batch_id,
            "present": identity is not None,
            "content_hash": None if identity is None else identity.content_hash,
            "idempotency_key": None if identity is None else identity.idempotency_key,
        },
    }


def _authority_evidence(
    deployment: ThreeMachineDeployment,
    control: PhaseDControlPlane,
) -> dict[str, object]:
    snapshot = control.snapshot(deployment.session_id)
    assignment = snapshot.groups[deployment.group_id]
    manifest = control.manifest(deployment.session_id, deployment.group_id)
    return {
        "schema": EVIDENCE_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "role": "authority",
        "stage": "final",
        "captured_at": NOW.isoformat().replace("+00:00", "Z"),
        "node_id": deployment.authority_node_id,
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "control": {
            "revision": snapshot.revision,
            "primary_provider_id": assignment.primary_provider_id,
            "replica_provider_ids": list(assignment.replica_provider_ids),
            "acknowledgement_mode": control.acknowledgement_policy(
                deployment.session_id, deployment.group_id
            ).mode.value,
            "grant": snapshot.leader_grants[deployment.group_id],
            "degraded_state": control.storage_degraded_state(
                deployment.session_id, deployment.group_id
            ),
            "manifest": {
                "revision": manifest.revision,
                "manifest_hash": manifest.manifest_hash,
                "items": [
                    {
                        "item_id": item.item_id,
                        "content_hash": item.content_hash,
                        "idempotency_key": item.idempotency_key,
                    }
                    for item in manifest.items
                ],
            },
        },
        "relay": {"nodes": [], "capabilities": []},
    }


def test_rendered_storage_config_is_secret_free_and_strict(tmp_path: Path) -> None:
    path = tmp_path / "storage-node.json"
    config = render_storage_config(
        output=path,
        relay_url="ws://127.0.0.1:8765",
        session_id="session-1",
        display_name="Storage B",
        provider_id="provider-b",
        allow_insecure_local=True,
    )
    value = json.loads(path.read_text(encoding="utf-8"))

    assert config.provider_id == "provider-b"
    assert config.session_id == "session-1"
    assert value["schema"] == "msh.storage_node_config.v1"
    assert not any(
        marker in key.lower()
        for key in value
        for marker in ("token", "password", "secret", "private_key")
    )


def test_three_machine_deployment_survives_normal_restart(tmp_path: Path) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        authority: RelayNodeClient | None = None
        authority_endpoint: RelayStorageEndpoint | None = None
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
            authority_state = tmp_path / "authority-state"
            authority = RelayNodeClient(
                state_directory=authority_state,
                relay_url=relay.url,
                display_name="F5.1 authority",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            authority_token = coordinator.create_enrollment_token(
                ttl_seconds=60, max_uses=1
            )["token"]
            await authority.connect(enrollment_token=str(authority_token))
            session = await authority.create_session("F5.1 physical deployment")
            session_id = str(session["session_id"])
            primary_invitation = str(
                (
                    await authority.create_invitation(
                        session_id, ttl_seconds=60, max_uses=1
                    )
                )["token"]
            )
            replica_invitation = str(
                (
                    await authority.create_invitation(
                        session_id, ttl_seconds=60, max_uses=1
                    )
                )["token"]
            )

            primary_config = render_storage_config(
                output=tmp_path / "primary" / "storage-node.json",
                relay_url=relay.url,
                session_id=session_id,
                display_name="Storage machine B",
                provider_id="provider-primary",
                allow_insecure_local=True,
            )
            replica_config = render_storage_config(
                output=tmp_path / "replica" / "storage-node.json",
                relay_url=relay.url,
                session_id=session_id,
                display_name="Storage machine C",
                provider_id="provider-replica",
                allow_insecure_local=True,
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
            deployment = ThreeMachineDeployment(
                deployment_id="f51-test-deployment",
                relay_url=relay.url,
                session_id=session_id,
                group_id="storage-main",
                authority_node_id=authority.node_id,
                authority_display_name="F5.1 authority",
                primary=DeploymentNode(
                    primary.node_id,
                    "provider-primary",
                    "Storage machine B",
                ),
                replica=DeploymentNode(
                    replica.node_id,
                    "provider-replica",
                    "Storage machine C",
                ),
            )
            deployment.validate()
            control_path = tmp_path / "authority-control.sqlite3"
            control = ensure_initial_control(
                deployment,
                authority_state_directory=authority_state,
                control_database=control_path,
                lease_seconds=3600,
                now=NOW,
            )
            assert control.acknowledgement_policy(
                session_id, "storage-main"
            ).mode is AcknowledgementMode.ONE_REPLICA

            primary_task = asyncio.create_task(
                primary.bootstrap(
                    enrollment_token=str(
                        coordinator.create_enrollment_token(
                            ttl_seconds=60, max_uses=1
                        )["token"]
                    ),
                    session_invitation=primary_invitation,
                )
            )
            replica_task = asyncio.create_task(
                replica.bootstrap(
                    enrollment_token=str(
                        coordinator.create_enrollment_token(
                            ttl_seconds=60, max_uses=1
                        )["token"]
                    ),
                    session_invitation=replica_invitation,
                )
            )
            bootstrap_tasks.extend((primary_task, replica_task))
            await asyncio.gather(
                _wait_for_control_waiting(primary, primary_task),
                _wait_for_control_waiting(replica, replica_task),
            )
            plan = StorageControlPublicationStore(
                tmp_path / "publications.sqlite3"
            ).issue(control, authority.credentials, session_id, now=NOW)
            published = await FramedStorageControlRelayPublisher(
                authority, timeout=TIMEOUT
            ).publish(plan, (primary.node_id, replica.node_id))
            assert set(published.acknowledged_node_ids) == {
                primary.node_id,
                replica.node_id,
            }
            await asyncio.gather(primary_task, replica_task)

            authority_endpoint = RelayStorageEndpoint(
                authority, request_timeout=TIMEOUT
            )
            await authority_endpoint.start()
            acknowledgements = DurableAcknowledgementStore(
                tmp_path / "authority-acks.sqlite3"
            )
            outcome = await PhaseDLogicalStorageClient(
                session_id=session_id,
                actor_node_id=authority.node_id,
                control_plane=control,
                transport=authority_endpoint,
                acknowledgements=acknowledgements,
                clock=lambda: NOW,
            ).ingest_batch(
                group_id="storage-main",
                dataset_id="f51-deployment",
                batch_id="f51-probe-1",
                idempotency_key="f51:probe:1",
                content={"deployment": "normal", "sequence": 1},
                created_at=NOW,
            )
            assert outcome.committed
            commit = acknowledgements.status(
                session_id, "storage-main", "f51-probe-1"
            )
            assert commit is not None
            assert commit.required_replica_acks == 1
            assert commit.acknowledged_replica_ids == ("provider-replica",)

            primary_before = _storage_evidence(
                deployment,
                primary,
                role="primary",
                stage="before-restart",
                batch_id="f51-probe-1",
            )
            replica_before = _storage_evidence(
                deployment,
                replica,
                role="replica",
                stage="before-restart",
                batch_id="f51-probe-1",
            )
            await primary.close()
            await replica.close()

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
            primary_restart = asyncio.create_task(primary.bootstrap())
            replica_restart = asyncio.create_task(replica.bootstrap())
            bootstrap_tasks.extend((primary_restart, replica_restart))
            await asyncio.sleep(0.1)
            restart_plan = StorageControlPublicationStore(
                tmp_path / "publications.sqlite3"
            ).issue(control, authority.credentials, session_id, now=NOW + timedelta(seconds=1))
            await FramedStorageControlRelayPublisher(
                authority, timeout=TIMEOUT
            ).publish(restart_plan, (primary.node_id, replica.node_id))
            await asyncio.gather(primary_restart, replica_restart)

            primary_after = _storage_evidence(
                deployment,
                primary,
                role="primary",
                stage="after-restart",
                batch_id="f51-probe-1",
            )
            replica_after = _storage_evidence(
                deployment,
                replica,
                role="replica",
                stage="after-restart",
                batch_id="f51-probe-1",
            )
            report = verify_evidence(
                deployment,
                authority=_authority_evidence(deployment, control),
                primary_before=primary_before,
                replica_before=replica_before,
                primary_after=primary_after,
                replica_after=replica_after,
            )
            assert report["passed"], report
            assert len(report["checks"]) >= 18
            assert primary.node_id == deployment.primary.node_id
            assert replica.node_id == deployment.replica.node_id
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
            if authority_endpoint is not None:
                await authority_endpoint.close()
            if authority is not None and authority.connected_event.is_set():
                await authority.disconnect()
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())
