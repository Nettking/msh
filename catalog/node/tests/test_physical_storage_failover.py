from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

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
from catalog.node.client import RelayNodeClient
from catalog.node.live_storage_agent import LiveStorageNodeAgent
from catalog.node.storage_deployment import (
    EVIDENCE_SCHEMA as F51_EVIDENCE_SCHEMA,
    DeploymentNode,
    ThreeMachineDeployment,
    ensure_initial_control,
    render_storage_config,
    verify_evidence as verify_f51_evidence,
)
from catalog.node.storage_failover_drill import (
    build_authority_evidence,
    post_failover_probe_with_transport,
    stale_authority_probe_with_transport,
    storage_evidence_from_agent,
    verify_evidence,
)
from catalog.relay.service import RelayServer

NOW = datetime(2026, 7, 31, 14, 0, tzinfo=timezone.utc)
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


def _f51_storage_evidence(
    deployment: ThreeMachineDeployment,
    agent: LiveStorageNodeAgent,
    *,
    role: str,
    batch_id: str,
) -> dict[str, Any]:
    identity = agent.provider.committed_identity(
        session_id=deployment.session_id,
        group_id=deployment.group_id,
        batch_id=batch_id,
    )
    return {
        "schema": F51_EVIDENCE_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "role": role,
        "stage": "after-restart",
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


def _f51_authority_evidence(
    deployment: ThreeMachineDeployment,
    control: PhaseDControlPlane,
    relay_status: dict[str, Any],
) -> dict[str, Any]:
    snapshot = control.snapshot(deployment.session_id)
    assignment = snapshot.groups[deployment.group_id]
    manifest = control.manifest(deployment.session_id, deployment.group_id)
    return {
        "schema": F51_EVIDENCE_SCHEMA,
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
        "relay": {
            "nodes": relay_status.get("nodes", []),
            "capabilities": relay_status.get("capabilities", []),
        },
    }


async def _scenario(root: Path) -> dict[str, Any]:
    relay: RelayServer | None = None
    authority: RelayNodeClient | None = None
    authority_endpoint: RelayStorageEndpoint | None = None
    failover: StorageFailoverCoordinator | None = None
    primary: LiveStorageNodeAgent | None = None
    replica: LiveStorageNodeAgent | None = None
    bootstrap_tasks: list[asyncio.Task[None]] = []
    try:
        coordinator = SessionCoordinator(
            root / "relay" / "control.sqlite3",
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
        authority_state = root / "authority-state"
        authority = RelayNodeClient(
            state_directory=authority_state,
            relay_url=relay.url,
            display_name="F5.2 authority",
            allow_insecure_local=True,
            heartbeat_interval=300,
            request_timeout=TIMEOUT,
            clock=lambda: NOW,
        )
        authority_token = coordinator.create_enrollment_token(
            ttl_seconds=60, max_uses=1
        )["token"]
        await authority.connect(enrollment_token=str(authority_token))
        session = await authority.create_session("F5.2 physical failover")
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
            output=root / "primary" / "storage-node.json",
            relay_url=relay.url,
            session_id=session_id,
            display_name="Storage machine B",
            provider_id="provider-primary",
            allow_insecure_local=True,
        )
        replica_config = render_storage_config(
            output=root / "replica" / "storage-node.json",
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
            deployment_id="f52-test-deployment",
            relay_url=relay.url,
            session_id=session_id,
            group_id="storage-main",
            authority_node_id=authority.node_id,
            authority_display_name="F5.2 authority",
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
        control_path = root / "authority-control.sqlite3"
        control = ensure_initial_control(
            deployment,
            authority_state_directory=authority_state,
            control_database=control_path,
            lease_seconds=3600,
            now=NOW,
        )
        publication_path = root / "authority-publications.sqlite3"
        failover_path = root / "authority-failover.sqlite3"
        authority_endpoint = RelayStorageEndpoint(
            authority, request_timeout=TIMEOUT
        )
        await authority_endpoint.start()
        failover = StorageFailoverCoordinator(
            session_coordinator=coordinator,
            control_plane=control,
            publication_store=StorageControlPublicationStore(publication_path),
            credentials=authority.credentials,
            channel=StorageControlRelayChannel(
                authority,
                authority_endpoint,
                timeout=TIMEOUT,
            ),
            failover_store=LiveFailoverStore(failover_path),
            session_id=session_id,
            lease_seconds=300,
            clock=lambda: NOW,
        )
        await failover.start()

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
        await failover.publish_current((primary.node_id, replica.node_id))
        await asyncio.gather(primary_task, replica_task)

        acknowledgements = DurableAcknowledgementStore(
            root / "authority-acks.sqlite3"
        )
        logical = PhaseDLogicalStorageClient(
            session_id=session_id,
            actor_node_id=authority.node_id,
            control_plane=control,
            transport=authority_endpoint,
            acknowledgements=acknowledgements,
            clock=lambda: NOW,
        )
        pre_failure = await logical.ingest_batch(
            group_id="storage-main",
            dataset_id="f52-failover",
            batch_id="f52-before-1",
            idempotency_key="f52:before:1",
            content={"stage": "before-failover", "sequence": 1},
            created_at=NOW,
        )
        assert pre_failure.committed

        primary_f51 = _f51_storage_evidence(
            deployment, primary, role="primary", batch_id="f52-before-1"
        )
        replica_f51 = _f51_storage_evidence(
            deployment, replica, role="replica", batch_id="f52-before-1"
        )
        baseline_authority = _f51_authority_evidence(
            deployment,
            control,
            await authority.coordinator_status(),
        )
        baseline_report = verify_f51_evidence(
            deployment,
            authority=baseline_authority,
            primary_before=primary_f51,
            replica_before=replica_f51,
            primary_after=primary_f51,
            replica_after=replica_f51,
        )
        assert baseline_report["passed"], baseline_report
        old_primary_node_id = primary.node_id

        await primary.close()
        promotion = await _wait_for_promotion(failover)
        assert promotion.promoted_provider_id == deployment.replica.provider_id

        post_probe = await post_failover_probe_with_transport(
            deployment,
            control=control,
            transport=authority_endpoint,
            acknowledgements=acknowledgements,
            dataset_id="f52-failover",
            batch_id="f52-after-1",
            idempotency_key="f52:after:1",
            content={"stage": "after-failover", "sequence": 2},
            created_at=NOW,
        )

        primary = LiveStorageNodeAgent(
            primary_config,
            control_authority_node_id=authority.node_id,
            control_sync_timeout=TIMEOUT,
            clock=lambda: NOW,
        )
        primary_restart = asyncio.create_task(primary.bootstrap())
        bootstrap_tasks.append(primary_restart)
        await asyncio.wait_for(primary_restart, TIMEOUT)
        assert primary.node_id == old_primary_node_id

        stale_probe = await stale_authority_probe_with_transport(
            deployment,
            baseline_authority=baseline_authority,
            transport=authority_endpoint,
            batch_id="f52-stale-1",
            idempotency_key="f52:stale:1",
            content={"stale": True},
            created_at=NOW,
        )
        authority_f52 = build_authority_evidence(
            deployment,
            baseline_authority=baseline_authority,
            control=control,
            failover_store=LiveFailoverStore(failover_path),
            relay_status=await authority.coordinator_status(),
            post_failover_batch_id="f52-after-1",
            captured_at=NOW,
        )
        promoted_f52 = storage_evidence_from_agent(
            deployment,
            replica,
            role="promoted-primary",
            pre_failure_batch_id="f52-before-1",
            post_failover_batch_id="f52-after-1",
            captured_at=NOW,
        )
        former_f52 = storage_evidence_from_agent(
            deployment,
            primary,
            role="former-primary",
            pre_failure_batch_id="f52-before-1",
            post_failover_batch_id="f52-after-1",
            captured_at=NOW,
        )
        return {
            "deployment": deployment,
            "baseline_report": baseline_report,
            "baseline_authority": baseline_authority,
            "authority": authority_f52,
            "promoted_primary": promoted_f52,
            "former_primary": former_f52,
            "post_failover_probe": post_probe,
            "stale_authority_probe": stale_probe,
        }
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


@pytest.fixture(scope="module")
def f52_bundle(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return asyncio.run(_scenario(tmp_path_factory.mktemp("physical-f52")))


def _verify(bundle: dict[str, Any]) -> dict[str, Any]:
    return verify_evidence(
        bundle["deployment"],
        baseline_report=bundle["baseline_report"],
        baseline_authority=bundle["baseline_authority"],
        authority=bundle["authority"],
        promoted_primary=bundle["promoted_primary"],
        former_primary=bundle["former_primary"],
        post_failover_probe=bundle["post_failover_probe"],
        stale_authority_probe=bundle["stale_authority_probe"],
    )


def test_physical_primary_loss_produces_passed_f52_report(
    f52_bundle: dict[str, Any],
) -> None:
    report = _verify(f52_bundle)

    assert report["passed"], report
    assert report["assignment"] == {
        "primary_provider_id": "provider-replica",
        "replica_provider_ids": [],
    }
    assert report["acknowledgement_mode"] == "primary"
    assert report["degraded_reason"] == "automatic-failover-redundancy-lost"
    assert report["former_primary"]["role"] == "unassigned"
    assert report["stale_authority_rejection"]["code"] == "not-primary"


def _mismatch_deployment(bundle: dict[str, Any]) -> None:
    bundle["authority"]["deployment_id"] = "foreign-deployment"


def _unchanged_authority(bundle: dict[str, Any]) -> None:
    source = bundle["baseline_authority"]["control"]["grant"]
    promoted = bundle["authority"]["control"]["grant"]
    promoted["term"] = source["term"]
    promoted["fencing_token"] = source["fencing_token"]


def _remove_fresh_report(bundle: dict[str, Any]) -> None:
    bundle["authority"]["selected_replica_assessment"]["report"] = None


def _remove_degraded_state(bundle: dict[str, Any]) -> None:
    bundle["authority"]["control"]["degraded_state"] = None


def _restore_former_authority(bundle: dict[str, Any]) -> None:
    bundle["former_primary"]["status"]["provider"]["groups"] = [
        {"group_id": "storage-main", "role": "primary"}
    ]


@pytest.mark.parametrize(
    ("mutation", "failed_check"),
    [
        (_mismatch_deployment, "deployment-binding"),
        (_unchanged_authority, "advanced-authority"),
        (_remove_fresh_report, "fresh-selected-report"),
        (_remove_degraded_state, "degraded-policy"),
        (_restore_former_authority, "former-primary-state"),
    ],
)
def test_f52_verifier_rejects_unsafe_or_contradictory_evidence(
    f52_bundle: dict[str, Any],
    mutation: Callable[[dict[str, Any]], None],
    failed_check: str,
) -> None:
    mutated = copy.deepcopy(f52_bundle)
    mutation(mutated)

    report = _verify(mutated)
    checks = {item["name"]: item["passed"] for item in report["checks"]}

    assert not report["passed"]
    assert checks[failed_check] is False
