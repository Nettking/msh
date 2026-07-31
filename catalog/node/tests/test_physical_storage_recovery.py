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
from catalog.node.storage_deployment import (
    DeploymentNode,
    ThreeMachineDeployment,
    ensure_initial_control,
    render_storage_config,
    verify_evidence as verify_f51_evidence,
)
from catalog.node.storage_failover_drill import (
    build_authority_evidence as build_f52_authority_evidence,
    post_failover_probe_with_transport,
    stale_authority_probe_with_transport,
    storage_evidence_from_agent as f52_storage_evidence,
    verify_evidence as verify_f52_evidence,
)
from catalog.node.storage_recovery_drill import (
    OPERATION_SCHEMA,
    build_authority_evidence,
    post_recovery_probe_with_transport,
    storage_evidence_from_agent,
    verify_evidence,
)
from catalog.relay.service import RelayServer

from .test_physical_storage_failover import (
    _f51_authority_evidence,
    _f51_storage_evidence,
    _wait_for_control_waiting,
    _wait_for_promotion,
)

NOW = datetime(2026, 7, 31, 15, 0, tzinfo=timezone.utc)
TIMEOUT = 5.0


async def _scenario(root: Path) -> dict[str, Any]:
    relay: RelayServer | None = None
    authority: RelayNodeClient | None = None
    authority_endpoint: RelayStorageEndpoint | None = None
    failover: StorageFailoverCoordinator | None = None
    machine_b: LiveStorageNodeAgent | None = None
    machine_c: LiveStorageNodeAgent | None = None
    bootstrap_tasks: list[asyncio.Task[None]] = []
    try:
        relay_control_path = root / "relay" / "control.sqlite3"
        coordinator = SessionCoordinator(relay_control_path, clock=lambda: NOW)
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
            display_name="F5.3 authority",
            allow_insecure_local=True,
            heartbeat_interval=300,
            request_timeout=TIMEOUT,
            clock=lambda: NOW,
        )
        await authority.connect(
            enrollment_token=str(
                coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)[
                    "token"
                ]
            )
        )
        session = await authority.create_session("F5.3 physical recovery")
        session_id = str(session["session_id"])
        invitation_b = str(
            (
                await authority.create_invitation(
                    session_id, ttl_seconds=60, max_uses=1
                )
            )["token"]
        )
        invitation_c = str(
            (
                await authority.create_invitation(
                    session_id, ttl_seconds=60, max_uses=1
                )
            )["token"]
        )

        config_b = render_storage_config(
            output=root / "machine-b" / "storage-node.json",
            relay_url=relay.url,
            session_id=session_id,
            display_name="Storage machine B",
            provider_id="provider-primary",
            allow_insecure_local=True,
        )
        config_c = render_storage_config(
            output=root / "machine-c" / "storage-node.json",
            relay_url=relay.url,
            session_id=session_id,
            display_name="Storage machine C",
            provider_id="provider-replica",
            allow_insecure_local=True,
        )
        machine_b = LiveStorageNodeAgent(
            config_b,
            control_authority_node_id=authority.node_id,
            control_sync_timeout=TIMEOUT,
            clock=lambda: NOW,
        )
        machine_c = LiveStorageNodeAgent(
            config_c,
            control_authority_node_id=authority.node_id,
            control_sync_timeout=TIMEOUT,
            clock=lambda: NOW,
        )
        deployment = ThreeMachineDeployment(
            deployment_id="f53-test-deployment",
            relay_url=relay.url,
            session_id=session_id,
            group_id="storage-main",
            authority_node_id=authority.node_id,
            authority_display_name="F5.3 authority",
            primary=DeploymentNode(
                machine_b.node_id,
                "provider-primary",
                "Storage machine B",
            ),
            replica=DeploymentNode(
                machine_c.node_id,
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
        catchup_path = root / "authority-catchup.sqlite3"
        acknowledgements = DurableAcknowledgementStore(
            root / "authority-acks.sqlite3"
        )
        authority_endpoint = RelayStorageEndpoint(
            authority, request_timeout=TIMEOUT
        )
        await authority_endpoint.start()
        channel = StorageControlRelayChannel(
            authority,
            authority_endpoint,
            timeout=TIMEOUT,
        )
        failover_store = LiveFailoverStore(failover_path)
        publication_store = StorageControlPublicationStore(publication_path)
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

        bootstrap_b = asyncio.create_task(
            machine_b.bootstrap(
                enrollment_token=str(
                    coordinator.create_enrollment_token(
                        ttl_seconds=60, max_uses=1
                    )["token"]
                ),
                session_invitation=invitation_b,
            )
        )
        bootstrap_c = asyncio.create_task(
            machine_c.bootstrap(
                enrollment_token=str(
                    coordinator.create_enrollment_token(
                        ttl_seconds=60, max_uses=1
                    )["token"]
                ),
                session_invitation=invitation_c,
            )
        )
        bootstrap_tasks.extend((bootstrap_b, bootstrap_c))
        await asyncio.gather(
            _wait_for_control_waiting(machine_b, bootstrap_b),
            _wait_for_control_waiting(machine_c, bootstrap_c),
        )
        await failover.publish_current((machine_b.node_id, machine_c.node_id))
        await asyncio.gather(bootstrap_b, bootstrap_c)

        logical = PhaseDLogicalStorageClient(
            session_id=session_id,
            actor_node_id=authority.node_id,
            control_plane=control,
            transport=authority_endpoint,
            acknowledgements=acknowledgements,
            clock=lambda: NOW,
        )
        first = await logical.ingest_batch(
            group_id="storage-main",
            dataset_id="f53-recovery",
            batch_id="f53-before-1",
            idempotency_key="f53:before:1",
            content={"stage": "before-failover", "sequence": 1},
            created_at=NOW,
        )
        assert first.committed

        f51_b = _f51_storage_evidence(
            deployment, machine_b, role="primary", batch_id="f53-before-1"
        )
        f51_c = _f51_storage_evidence(
            deployment, machine_c, role="replica", batch_id="f53-before-1"
        )
        f51_authority = _f51_authority_evidence(
            deployment,
            control,
            await authority.coordinator_status(),
        )
        f51_report = verify_f51_evidence(
            deployment,
            authority=f51_authority,
            primary_before=f51_b,
            replica_before=f51_c,
            primary_after=f51_b,
            replica_after=f51_c,
        )
        assert f51_report["passed"], f51_report

        stable_b_node_id = machine_b.node_id
        stable_c_node_id = machine_c.node_id
        await machine_b.close()
        promotion = await _wait_for_promotion(failover)
        assert promotion.promoted_provider_id == deployment.replica.provider_id

        f52_probe = await post_failover_probe_with_transport(
            deployment,
            control=control,
            transport=authority_endpoint,
            acknowledgements=acknowledgements,
            dataset_id="f53-recovery",
            batch_id="f53-after-1",
            idempotency_key="f53:after:1",
            content={"stage": "after-failover", "sequence": 2},
            created_at=NOW,
        )
        machine_b = LiveStorageNodeAgent(
            config_b,
            control_authority_node_id=authority.node_id,
            control_sync_timeout=TIMEOUT,
            clock=lambda: NOW,
        )
        await asyncio.wait_for(machine_b.bootstrap(), TIMEOUT)
        assert machine_b.node_id == stable_b_node_id
        stale_probe = await stale_authority_probe_with_transport(
            deployment,
            baseline_authority=f51_authority,
            transport=authority_endpoint,
            batch_id="f53-stale-1",
            idempotency_key="f53:stale:1",
            content={"stale": True},
            created_at=NOW,
        )
        f52_authority = build_f52_authority_evidence(
            deployment,
            baseline_authority=f51_authority,
            control=control,
            failover_store=failover_store,
            relay_status=await authority.coordinator_status(),
            post_failover_batch_id="f53-after-1",
            captured_at=NOW,
        )
        f52_promoted = f52_storage_evidence(
            deployment,
            machine_c,
            role="promoted-primary",
            pre_failure_batch_id="f53-before-1",
            post_failover_batch_id="f53-after-1",
            captured_at=NOW,
        )
        f52_former = f52_storage_evidence(
            deployment,
            machine_b,
            role="former-primary",
            pre_failure_batch_id="f53-before-1",
            post_failover_batch_id="f53-after-1",
            captured_at=NOW,
        )
        f52_report = verify_f52_evidence(
            deployment,
            baseline_report=f51_report,
            baseline_authority=f51_authority,
            authority=f52_authority,
            promoted_primary=f52_promoted,
            former_primary=f52_former,
            post_failover_probe=f52_probe,
            stale_authority_probe=stale_probe,
        )
        assert f52_report["passed"], f52_report

        catchup_store = LiveCatchupStore(catchup_path)
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
        recovered = await reinstatement.run_once(
            group_id="storage-main",
            returning_provider_id=deployment.primary.provider_id,
        )
        assert recovered.status == "completed", recovered.to_dict()
        assert recovered.record is not None
        recovery_operation = {
            "schema": OPERATION_SCHEMA,
            "deployment_id": deployment.deployment_id,
            "session_id": deployment.session_id,
            "group_id": deployment.group_id,
            "status": recovered.status,
            "code": recovered.code,
            "reason": recovered.reason,
            "reinstatement_id": recovered.reinstatement_id,
            "record": recovered.record.to_dict(),
        }

        # Restart both storage processes without first-start credentials. Machine C
        # must refresh its cached primary control; Machine B must retain replica role.
        await machine_b.close()
        await machine_c.close()
        machine_b = LiveStorageNodeAgent(
            config_b,
            control_authority_node_id=authority.node_id,
            control_sync_timeout=TIMEOUT,
            clock=lambda: NOW,
        )
        machine_c = LiveStorageNodeAgent(
            config_c,
            control_authority_node_id=authority.node_id,
            control_sync_timeout=TIMEOUT,
            clock=lambda: NOW,
        )
        restart_b = asyncio.create_task(machine_b.bootstrap())
        restart_c = asyncio.create_task(machine_c.bootstrap())
        bootstrap_tasks.extend((restart_b, restart_c))
        await asyncio.wait_for(asyncio.gather(restart_b, restart_c), TIMEOUT)
        assert machine_b.node_id == stable_b_node_id
        assert machine_c.node_id == stable_c_node_id

        final_probe = await post_recovery_probe_with_transport(
            deployment,
            f52_report=f52_report,
            control=control,
            transport=authority_endpoint,
            acknowledgements=acknowledgements,
            dataset_id="f53-recovery",
            batch_id="f53-restored-1",
            idempotency_key="f53:restored:1",
            content={"stage": "restored-redundancy", "sequence": 3},
            created_at=NOW,
        )
        authority_f53 = build_authority_evidence(
            deployment,
            f52_report=f52_report,
            control=control,
            failover_store=failover_store,
            catchup_store=catchup_store,
            relay_status=await authority.coordinator_status(),
            post_recovery_batch_id="f53-restored-1",
            captured_at=NOW,
        )
        current_primary = storage_evidence_from_agent(
            deployment,
            machine_c,
            role="current-primary",
            pre_failure_batch_id="f53-before-1",
            post_failover_batch_id="f53-after-1",
            post_recovery_batch_id="f53-restored-1",
            captured_at=NOW,
        )
        reinstated_replica = storage_evidence_from_agent(
            deployment,
            machine_b,
            role="reinstated-replica",
            pre_failure_batch_id="f53-before-1",
            post_failover_batch_id="f53-after-1",
            post_recovery_batch_id="f53-restored-1",
            captured_at=NOW,
        )
        replay = await reinstatement.run_once(
            group_id="storage-main",
            returning_provider_id=deployment.primary.provider_id,
        )
        assert replay.status == "completed"
        assert replay.reinstatement_id == recovered.reinstatement_id
        return {
            "deployment": deployment,
            "f52_report": f52_report,
            "f52_authority": f52_authority,
            "f52_promoted_primary": f52_promoted,
            "f52_former_primary": f52_former,
            "recovery_operation": recovery_operation,
            "authority": authority_f53,
            "current_primary": current_primary,
            "reinstated_replica": reinstated_replica,
            "post_recovery_probe": final_probe,
        }
    finally:
        for task in bootstrap_tasks:
            if not task.done():
                task.cancel()
        if bootstrap_tasks:
            await asyncio.gather(*bootstrap_tasks, return_exceptions=True)
        if machine_b is not None:
            await machine_b.close()
        if machine_c is not None:
            await machine_c.close()
        if failover is not None:
            await failover.close()
        if authority_endpoint is not None:
            await authority_endpoint.close()
        if authority is not None and authority.connected_event.is_set():
            await authority.disconnect()
        if relay is not None:
            await relay.stop()


@pytest.fixture(scope="module")
def f53_bundle(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return asyncio.run(_scenario(tmp_path_factory.mktemp("physical-f53")))


def _verify(bundle: dict[str, Any]) -> dict[str, Any]:
    return verify_evidence(
        bundle["deployment"],
        f52_report=bundle["f52_report"],
        f52_authority=bundle["f52_authority"],
        f52_promoted_primary=bundle["f52_promoted_primary"],
        f52_former_primary=bundle["f52_former_primary"],
        recovery_operation=bundle["recovery_operation"],
        authority=bundle["authority"],
        current_primary=bundle["current_primary"],
        reinstated_replica=bundle["reinstated_replica"],
        post_recovery_probe=bundle["post_recovery_probe"],
    )


def test_physical_recovery_produces_passed_f53_report(
    f53_bundle: dict[str, Any],
) -> None:
    report = _verify(f53_bundle)

    assert report["passed"], report
    assert report["assignment"] == {
        "primary_provider_id": "provider-replica",
        "replica_provider_ids": ["provider-primary"],
    }
    assert report["acknowledgement_mode"] == "one-replica"
    assert report["degraded_state"] is None
    assert report["catchup"]["state"] == "caught-up"
    assert report["reinstatement"]["state"] == "completed"


def _foreign_f52_baseline(bundle: dict[str, Any]) -> None:
    bundle["f52_report"]["deployment_id"] = "foreign-deployment"


def _remove_catchup_report(bundle: dict[str, Any]) -> None:
    bundle["authority"]["catchup"]["final_report"] = None


def _change_preserved_term(bundle: dict[str, Any]) -> None:
    bundle["authority"]["control"]["grant"]["term"] += 1


def _restore_degraded_state(bundle: dict[str, Any]) -> None:
    bundle["authority"]["control"]["degraded_state"] = {
        "reason_code": "automatic-failover-redundancy-lost"
    }


def _remove_replica_role(bundle: dict[str, Any]) -> None:
    bundle["reinstated_replica"]["status"]["provider"]["groups"] = [
        {"group_id": "storage-main", "role": "unassigned"}
    ]


def _remove_final_replica_ack(bundle: dict[str, Any]) -> None:
    bundle["post_recovery_probe"]["acknowledged_replica_ids"] = []


def _foreign_catchup_manifest(bundle: dict[str, Any]) -> None:
    bundle["authority"]["catchup"]["manifest_hash"] = "sha256:" + "0" * 64


@pytest.mark.parametrize(
    ("mutation", "failed_check"),
    [
        (_foreign_f52_baseline, "f52-baseline"),
        (_remove_catchup_report, "catchup-complete"),
        (_change_preserved_term, "leadership-preserved"),
        (_restore_degraded_state, "normal-control-restored"),
        (_remove_replica_role, "reinstated-replica-state"),
        (_remove_final_replica_ack, "post-recovery-write"),
        (_foreign_catchup_manifest, "catchup-manifest-binding"),
    ],
)
def test_f53_verifier_rejects_unsafe_or_contradictory_evidence(
    f53_bundle: dict[str, Any],
    mutation: Callable[[dict[str, Any]], None],
    failed_check: str,
) -> None:
    mutated = copy.deepcopy(f53_bundle)
    mutation(mutated)

    report = _verify(mutated)
    checks = {item["name"]: item["passed"] for item in report["checks"]}

    assert not report["passed"]
    assert checks[failed_check] is False
