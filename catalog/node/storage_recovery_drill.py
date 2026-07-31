"""Phase F5.3 physical recovery and replica reinstatement drill.

F5.3 starts from a passed F5.2 physical failover report. It reuses the existing
F4.1 live catch-up and F4.2 live reinstatement coordinators to repair Machine B,
restore it as a replica, restore the original acknowledgement policy, and clear
only the matching degraded state. Leadership remains on Machine C.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.control_sync import StorageControlPublicationStore
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationValidationError
from catalog.federation.live_catchup import (
    LiveCatchupStore,
    LiveFormerPrimaryCatchupCoordinator,
)
from catalog.federation.live_failover import (
    LiveFailoverStore,
    StorageControlRelayChannel,
)
from catalog.federation.live_reinstatement import (
    LiveFormerPrimaryReinstatementCoordinator,
    LiveReinstatementStore,
)
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.relay_storage import RelayStorageEndpoint

from .client import RelayNodeClient
from .live_storage_agent import LiveStorageNodeAgent
from .storage_agent import StorageNodeConfig
from .storage_deployment import StorageDeploymentError, ThreeMachineDeployment
from .storage_failover_drill import (
    EVIDENCE_SCHEMA as F52_EVIDENCE_SCHEMA,
    REPORT_SCHEMA as F52_REPORT_SCHEMA,
)

EVIDENCE_SCHEMA = "msh.storage_physical_recovery_evidence.v1"
REPORT_SCHEMA = "msh.storage_physical_recovery_report.v1"
OPERATION_SCHEMA = "msh.storage_physical_recovery_operation.v1"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise StorageDeploymentError(
            "invalid-f53-timestamp", "captured_at", "must be timezone-aware"
        )
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise StorageDeploymentError(
            "invalid-f53-field", field, "must be non-empty text"
        )
    return value


def _object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _read_json(path: Path | str, *, field: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise StorageDeploymentError(
            "f53-file-unavailable", field, "could not read JSON file"
        ) from exc
    except json.JSONDecodeError as exc:
        raise StorageDeploymentError(
            "invalid-f53-json", field, "must contain valid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise StorageDeploymentError(
            "invalid-f53-json", field, "must contain a JSON object"
        )
    return value


def _write_json(path: Path | str, value: dict[str, Any], *, force: bool) -> None:
    output = Path(path)
    if output.exists() and not force:
        raise StorageDeploymentError(
            "f53-output-exists",
            "output",
            "refusing to replace an existing file without --force",
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _allow_insecure_loopback(relay_url: str) -> bool:
    parsed = urlparse(relay_url)
    return parsed.scheme == "ws" and parsed.hostname in {
        "127.0.0.1",
        "::1",
        "localhost",
    }


def _provider_status(evidence: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    status = _object(evidence.get("status"))
    return _object(status.get("provider")), _object(status.get("control_sync"))


def validate_f52_baseline(
    deployment: ThreeMachineDeployment,
    *,
    f52_report: dict[str, Any],
    f52_authority: dict[str, Any],
    f52_promoted_primary: dict[str, Any],
    f52_former_primary: dict[str, Any],
) -> dict[str, Any]:
    """Require one passed, internally consistent F5.2 degraded deployment."""

    if (
        f52_report.get("schema") != F52_REPORT_SCHEMA
        or f52_report.get("deployment_id") != deployment.deployment_id
        or f52_report.get("session_id") != deployment.session_id
        or f52_report.get("group_id") != deployment.group_id
        or f52_report.get("passed") is not True
    ):
        raise StorageDeploymentError(
            "f53-baseline-not-passed",
            "f52_report",
            "F5.3 requires a passed F5.2 report for the same deployment",
        )
    evidence = (f52_authority, f52_promoted_primary, f52_former_primary)
    if not all(
        item.get("schema") == F52_EVIDENCE_SCHEMA
        and item.get("deployment_id") == deployment.deployment_id
        and item.get("session_id") == deployment.session_id
        and item.get("group_id") == deployment.group_id
        for item in evidence
    ):
        raise StorageDeploymentError(
            "f53-baseline-evidence-mismatch",
            "f52_evidence",
            "F5.2 evidence is missing or belongs to another deployment",
        )
    assignment = _object(f52_report.get("assignment"))
    promoted_grant = _object(f52_report.get("promoted_grant"))
    manifest = _object(f52_report.get("manifest"))
    post_batch = _object(f52_report.get("post_failover_batch"))
    original = _object(f52_report.get("original_primary"))
    promoted = _object(f52_report.get("promoted_primary"))
    former = _object(f52_report.get("former_primary"))
    if (
        original.get("node_id") != deployment.primary.node_id
        or original.get("provider_id") != deployment.primary.provider_id
        or promoted.get("node_id") != deployment.replica.node_id
        or promoted.get("provider_id") != deployment.replica.provider_id
        or assignment.get("primary_provider_id") != deployment.replica.provider_id
        or assignment.get("replica_provider_ids") != []
        or f52_report.get("acknowledgement_mode")
        != AcknowledgementMode.PRIMARY.value
        or f52_report.get("degraded_reason")
        != "automatic-failover-redundancy-lost"
        or former.get("node_id") != deployment.primary.node_id
        or former.get("provider_id") != deployment.primary.provider_id
        or former.get("role") != "unassigned"
        or former.get("control_ready") is not True
    ):
        raise StorageDeploymentError(
            "f53-baseline-not-degraded",
            "f52_report",
            "F5.3 must start from the exact F5.2 degraded assignment",
        )
    if (
        not isinstance(promoted_grant.get("grant_id"), str)
        or not isinstance(promoted_grant.get("term"), int)
        or not isinstance(promoted_grant.get("fencing_token"), int)
        or not isinstance(manifest.get("revision"), int)
        or not isinstance(manifest.get("manifest_hash"), str)
        or not isinstance(post_batch.get("batch_id"), str)
        or not isinstance(post_batch.get("content_hash"), str)
    ):
        raise StorageDeploymentError(
            "f53-baseline-incomplete",
            "f52_report",
            "F5.2 report lacks promoted authority or manifest evidence",
        )
    control = _object(f52_authority.get("control"))
    authority_grant = _object(control.get("grant"))
    degraded = _object(control.get("degraded_state"))
    if (
        f52_authority.get("role") != "authority"
        or control.get("primary_provider_id") != deployment.replica.provider_id
        or control.get("replica_provider_ids") != []
        or control.get("acknowledgement_mode")
        != AcknowledgementMode.PRIMARY.value
        or degraded.get("reason_code")
        != "automatic-failover-redundancy-lost"
        or authority_grant.get("grant_id") != promoted_grant.get("grant_id")
        or authority_grant.get("term") != promoted_grant.get("term")
        or authority_grant.get("fencing_token")
        != promoted_grant.get("fencing_token")
    ):
        raise StorageDeploymentError(
            "f53-baseline-authority-mismatch",
            "f52_authority",
            "F5.2 authority evidence contradicts its public report",
        )
    promoted_provider, promoted_sync = _provider_status(f52_promoted_primary)
    former_provider, former_sync = _provider_status(f52_former_primary)
    if (
        f52_promoted_primary.get("role") != "promoted-primary"
        or f52_promoted_primary.get("node_id") != deployment.replica.node_id
        or promoted_provider.get("groups")
        != [{"group_id": deployment.group_id, "role": "primary"}]
        or promoted_sync.get("ready") is not True
        or f52_former_primary.get("role") != "former-primary"
        or f52_former_primary.get("node_id") != deployment.primary.node_id
        or former_provider.get("groups")
        != [{"group_id": deployment.group_id, "role": "unassigned"}]
        or former_sync.get("ready") is not True
    ):
        raise StorageDeploymentError(
            "f53-baseline-storage-mismatch",
            "f52_storage_evidence",
            "F5.2 storage roles or verified control are inconsistent",
        )
    return {
        "schema": "msh.storage_physical_recovery_baseline.v1",
        "deployment_id": deployment.deployment_id,
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "current_primary": deployment.replica.to_dict(),
        "returning_provider": deployment.primary.to_dict(),
        "promoted_grant": {
            "grant_id": promoted_grant["grant_id"],
            "term": promoted_grant["term"],
            "fencing_token": promoted_grant["fencing_token"],
        },
        "manifest": {
            "revision": manifest["revision"],
            "manifest_hash": manifest["manifest_hash"],
        },
        "post_failover_batch": {
            "batch_id": post_batch["batch_id"],
            "content_hash": post_batch["content_hash"],
            "idempotency_key": post_batch.get("idempotency_key"),
        },
        "failover": _object(f52_report.get("failover")),
    }


async def run_recovery_operation(
    deployment: ThreeMachineDeployment,
    *,
    relay_control_database: Path | str,
    storage_control_database: Path | str,
    failover_database: Path | str,
    catchup_database: Path | str,
    publication_database: Path | str,
    authority_state_directory: Path | str,
    catchup_limit: int,
    timeout: float,
) -> dict[str, Any]:
    """Run the production F4.1/F4.2 path once and return public evidence."""

    if isinstance(catchup_limit, bool) or catchup_limit < 1:
        raise StorageDeploymentError(
            "invalid-f53-limit", "catchup_limit", "must be a positive integer"
        )
    client = RelayNodeClient(
        state_directory=authority_state_directory,
        relay_url=deployment.relay_url,
        display_name=deployment.authority_display_name,
        allow_insecure_local=_allow_insecure_loopback(deployment.relay_url),
        request_timeout=timeout,
    )
    await client.connect()
    if client.node_id != deployment.authority_node_id:
        await client.disconnect(error_code="f53-authority-mismatch")
        raise StorageDeploymentError(
            "f53-authority-mismatch",
            "authority_node_id",
            "local authority identity differs from the deployment",
        )
    endpoint = RelayStorageEndpoint(client, request_timeout=timeout)
    await endpoint.start()
    channel = StorageControlRelayChannel(client, endpoint, timeout=timeout)
    await channel.start()
    try:
        control = PhaseDControlPlane(storage_control_database)
        failover_store = LiveFailoverStore(failover_database)
        catchup_store = LiveCatchupStore(catchup_database)
        catchup = LiveFormerPrimaryCatchupCoordinator(
            session_coordinator=SessionCoordinator(relay_control_database),
            control_plane=control,
            failover_store=failover_store,
            transport=endpoint,
            credentials=client.credentials,
            catchup_store=catchup_store,
            session_id=deployment.session_id,
        )
        result = await LiveFormerPrimaryReinstatementCoordinator(
            control_plane=control,
            publication_store=StorageControlPublicationStore(publication_database),
            failover_store=failover_store,
            catchup_store=catchup_store,
            catchup=catchup,
            channel=channel,
            credentials=client.credentials,
            session_id=deployment.session_id,
        ).run_once(
            group_id=deployment.group_id,
            returning_provider_id=deployment.primary.provider_id,
            catchup_limit=catchup_limit,
        )
        return {
            "schema": OPERATION_SCHEMA,
            "deployment_id": deployment.deployment_id,
            "session_id": deployment.session_id,
            "group_id": deployment.group_id,
            "status": result.status,
            "code": result.code,
            "reason": result.reason,
            "reinstatement_id": result.reinstatement_id,
            "record": None if result.record is None else result.record.to_dict(),
        }
    finally:
        await channel.close()
        await endpoint.close()
        await client.disconnect()


async def post_recovery_probe_with_transport(
    deployment: ThreeMachineDeployment,
    *,
    f52_report: dict[str, Any],
    control: PhaseDControlPlane,
    transport: RelayStorageEndpoint,
    acknowledgements: DurableAcknowledgementStore,
    dataset_id: str,
    batch_id: str,
    idempotency_key: str,
    content: Any,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Commit one write that must receive an acknowledgement from Machine B."""

    timestamp = created_at or _now()
    snapshot = control.snapshot(deployment.session_id)
    assignment = snapshot.groups.get(deployment.group_id)
    grant = snapshot.leader_grants.get(deployment.group_id)
    promoted_grant = _object(f52_report.get("promoted_grant"))
    if (
        assignment is None
        or assignment.primary_provider_id != deployment.replica.provider_id
        or tuple(assignment.replica_provider_ids)
        != (deployment.primary.provider_id,)
        or grant is None
        or grant.get("provider_id") != deployment.replica.provider_id
        or grant.get("grant_id") != promoted_grant.get("grant_id")
        or grant.get("term") != promoted_grant.get("term")
        or grant.get("fencing_token") != promoted_grant.get("fencing_token")
        or control.acknowledgement_policy(
            deployment.session_id, deployment.group_id
        ).mode
        is not AcknowledgementMode.ONE_REPLICA
        or control.storage_degraded_state(
            deployment.session_id, deployment.group_id
        )
        is not None
    ):
        raise StorageDeploymentError(
            "f53-recovery-not-established",
            "control",
            "final writes require restored replica assignment and normal policy",
        )
    outcome = await PhaseDLogicalStorageClient(
        session_id=deployment.session_id,
        actor_node_id=deployment.authority_node_id,
        control_plane=control,
        transport=transport,
        acknowledgements=acknowledgements,
        clock=lambda: timestamp,
    ).ingest_batch(
        group_id=deployment.group_id,
        dataset_id=_text(dataset_id, "dataset_id"),
        batch_id=_text(batch_id, "batch_id"),
        idempotency_key=_text(idempotency_key, "idempotency_key"),
        content=content,
        created_at=timestamp,
    )
    if not outcome.committed or outcome.result is None:
        raise StorageDeploymentError(
            outcome.error_code or "f53-probe-not-committed",
            "batch_id",
            outcome.message or "post-recovery probe was not committed",
        )
    acknowledgement = acknowledgements.status(
        deployment.session_id, deployment.group_id, batch_id
    )
    if (
        acknowledgement is None
        or not acknowledgement.committed
        or acknowledgement.required_replica_acks != 1
        or acknowledgement.acknowledged_replica_ids
        != (deployment.primary.provider_id,)
    ):
        raise StorageDeploymentError(
            "f53-probe-acknowledgement-mismatch",
            "batch_id",
            "normal write must be acknowledged by the reinstated replica",
        )
    manifest = control.manifest(deployment.session_id, deployment.group_id)
    item = next((value for value in manifest.items if value.item_id == batch_id), None)
    if item is None or item.content_hash != outcome.result.content_hash:
        raise StorageDeploymentError(
            "f53-probe-manifest-missing",
            "batch_id",
            "authoritative manifest does not contain the post-recovery batch",
        )
    return {
        "schema": EVIDENCE_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "role": "post-recovery-probe",
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "batch_id": batch_id,
        "content_hash": outcome.result.content_hash,
        "primary_provider_id": deployment.replica.provider_id,
        "required_replica_acks": 1,
        "acknowledged_replica_ids": [deployment.primary.provider_id],
        "manifest_revision": manifest.revision,
        "manifest_hash": manifest.manifest_hash,
    }


async def post_recovery_probe(
    deployment: ThreeMachineDeployment,
    *,
    f52_report: dict[str, Any],
    authority_state_directory: Path | str,
    control_database: Path | str,
    acknowledgements_database: Path | str,
    dataset_id: str,
    batch_id: str,
    idempotency_key: str,
    content: Any,
    timeout: float,
) -> dict[str, Any]:
    client = RelayNodeClient(
        state_directory=authority_state_directory,
        relay_url=deployment.relay_url,
        display_name=deployment.authority_display_name,
        allow_insecure_local=_allow_insecure_loopback(deployment.relay_url),
        request_timeout=timeout,
    )
    await client.connect()
    endpoint = RelayStorageEndpoint(client, request_timeout=timeout)
    await endpoint.start()
    try:
        return await post_recovery_probe_with_transport(
            deployment,
            f52_report=f52_report,
            control=PhaseDControlPlane(control_database),
            transport=endpoint,
            acknowledgements=DurableAcknowledgementStore(
                acknowledgements_database
            ),
            dataset_id=dataset_id,
            batch_id=batch_id,
            idempotency_key=idempotency_key,
            content=content,
        )
    finally:
        await endpoint.close()
        await client.disconnect()


def storage_evidence_from_agent(
    deployment: ThreeMachineDeployment,
    agent: LiveStorageNodeAgent,
    *,
    role: str,
    pre_failure_batch_id: str,
    post_failover_batch_id: str,
    post_recovery_batch_id: str,
    captured_at: datetime | None = None,
) -> dict[str, Any]:
    if role not in {"current-primary", "reinstated-replica"}:
        raise StorageDeploymentError(
            "invalid-f53-storage-role",
            "role",
            "must be current-primary or reinstated-replica",
        )
    expected = deployment.replica if role == "current-primary" else deployment.primary
    if (
        agent.node_id != expected.node_id
        or agent.storage.config.provider_id != expected.provider_id
        or agent.storage.config.session_id != deployment.session_id
        or agent.storage.config.relay_url != deployment.relay_url
    ):
        raise StorageDeploymentError(
            "f53-storage-identity-mismatch",
            "storage",
            "local storage identity or configuration differs from the deployment",
        )
    latest = agent.replica_store.latest(deployment.session_id)
    if latest is None:
        raise StorageDeploymentError(
            "f53-control-missing",
            "control_sync",
            "storage node has no durable signed control publication",
        )
    latest.verify(expected_authority_node_id=deployment.authority_node_id)
    agent.storage.validate_control_state()
    status = agent.status()
    status["control_sync"]["ready"] = True
    status["control_sync"]["ready_source"] = "durable-verified"

    def batch(batch_id: str) -> dict[str, Any]:
        identity = agent.provider.committed_identity(
            session_id=deployment.session_id,
            group_id=deployment.group_id,
            batch_id=batch_id,
        )
        return {
            "batch_id": batch_id,
            "present": identity is not None,
            "content_hash": None if identity is None else identity.content_hash,
            "idempotency_key": None if identity is None else identity.idempotency_key,
        }

    return {
        "schema": EVIDENCE_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "role": role,
        "captured_at": _stamp(captured_at or _now()),
        "node_id": agent.node_id,
        "provider_id": expected.provider_id,
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "status": status,
        "batches": {
            "pre_failure": batch(_text(pre_failure_batch_id, "pre_failure_batch_id")),
            "post_failover": batch(
                _text(post_failover_batch_id, "post_failover_batch_id")
            ),
            "post_recovery": batch(
                _text(post_recovery_batch_id, "post_recovery_batch_id")
            ),
        },
    }


def storage_evidence(
    deployment: ThreeMachineDeployment,
    *,
    role: str,
    config_path: Path | str,
    pre_failure_batch_id: str,
    post_failover_batch_id: str,
    post_recovery_batch_id: str,
) -> dict[str, Any]:
    config = StorageNodeConfig.load(config_path)
    agent = LiveStorageNodeAgent(
        config, control_authority_node_id=deployment.authority_node_id
    )
    return storage_evidence_from_agent(
        deployment,
        agent,
        role=role,
        pre_failure_batch_id=pre_failure_batch_id,
        post_failover_batch_id=post_failover_batch_id,
        post_recovery_batch_id=post_recovery_batch_id,
    )


def build_authority_evidence(
    deployment: ThreeMachineDeployment,
    *,
    f52_report: dict[str, Any],
    control: PhaseDControlPlane,
    failover_store: LiveFailoverStore,
    catchup_store: LiveCatchupStore,
    relay_status: dict[str, Any],
    post_recovery_batch_id: str,
    captured_at: datetime | None = None,
) -> dict[str, Any]:
    reinstatement = LiveReinstatementStore(control.database).latest(
        deployment.session_id,
        deployment.group_id,
        deployment.primary.provider_id,
    )
    if reinstatement is None:
        raise StorageDeploymentError(
            "f53-reinstatement-record-missing",
            "reinstatement",
            "durable reinstatement record is unavailable",
        )
    catchup = catchup_store.get(reinstatement.catchup_recovery_id)
    if catchup is None:
        raise StorageDeploymentError(
            "f53-catchup-record-missing",
            "catchup",
            "durable catch-up record is unavailable",
        )
    failover = failover_store.get(
        deployment.session_id,
        deployment.group_id,
        reinstatement.failover_id,
    )
    if failover is None:
        raise StorageDeploymentError(
            "f53-failover-record-missing",
            "failover",
            "durable F5.2 failover record is unavailable",
        )
    snapshot = control.snapshot(deployment.session_id)
    assignment = snapshot.groups.get(deployment.group_id)
    if assignment is None:
        raise StorageDeploymentError(
            "f53-group-missing", "group_id", "storage group is not provisioned"
        )
    assessment = control.latest_storage_replica_assessment(
        deployment.session_id,
        deployment.group_id,
        deployment.primary.provider_id,
    )
    if assessment is None or assessment.report is None:
        raise StorageDeploymentError(
            "f53-final-report-missing",
            "replica_report",
            "reinstatement requires a durable final replica assessment",
        )
    manifest = control.manifest(deployment.session_id, deployment.group_id)
    post_item = next(
        (item for item in manifest.items if item.item_id == post_recovery_batch_id),
        None,
    )
    if post_item is None:
        raise StorageDeploymentError(
            "f53-post-batch-missing",
            "post_recovery_batch_id",
            "authoritative manifest does not contain the post-recovery batch",
        )
    relevant_nodes = {
        deployment.authority_node_id,
        deployment.primary.node_id,
        deployment.replica.node_id,
    }
    nodes = [
        item
        for item in _list(relay_status.get("nodes"))
        if isinstance(item, dict) and item.get("node_id") in relevant_nodes
    ]
    capabilities = [
        item
        for item in _list(relay_status.get("capabilities"))
        if isinstance(item, dict) and item.get("node_id") in relevant_nodes
    ]
    return {
        "schema": EVIDENCE_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "role": "authority",
        "captured_at": _stamp(captured_at or _now()),
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "control": {
            "revision": snapshot.revision,
            "primary_provider_id": assignment.primary_provider_id,
            "replica_provider_ids": list(assignment.replica_provider_ids),
            "acknowledgement_mode": control.acknowledgement_policy(
                deployment.session_id, deployment.group_id
            ).mode.value,
            "grant": snapshot.leader_grants.get(deployment.group_id),
            "degraded_state": control.storage_degraded_state(
                deployment.session_id, deployment.group_id
            ),
            "manifest": {
                "revision": manifest.revision,
                "manifest_hash": manifest.manifest_hash,
                "post_recovery_batch": {
                    "batch_id": post_item.item_id,
                    "content_hash": post_item.content_hash,
                    "idempotency_key": post_item.idempotency_key,
                },
            },
        },
        "f52_promoted_grant": _object(f52_report.get("promoted_grant")),
        "failover": failover.to_dict(),
        "catchup": catchup.to_dict(),
        "reinstatement": reinstatement.to_dict(),
        "final_replica_assessment": assessment.to_dict(),
        "relay": {"nodes": nodes, "capabilities": capabilities},
    }


async def authority_evidence(
    deployment: ThreeMachineDeployment,
    *,
    f52_report: dict[str, Any],
    authority_state_directory: Path | str,
    control_database: Path | str,
    failover_database: Path | str,
    catchup_database: Path | str,
    post_recovery_batch_id: str,
    timeout: float,
) -> dict[str, Any]:
    client = RelayNodeClient(
        state_directory=authority_state_directory,
        relay_url=deployment.relay_url,
        display_name=deployment.authority_display_name,
        allow_insecure_local=_allow_insecure_loopback(deployment.relay_url),
        request_timeout=timeout,
    )
    await client.connect()
    try:
        relay_status = await client.coordinator_status()
    finally:
        await client.disconnect()
    return build_authority_evidence(
        deployment,
        f52_report=f52_report,
        control=PhaseDControlPlane(control_database),
        failover_store=LiveFailoverStore(failover_database),
        catchup_store=LiveCatchupStore(catchup_database),
        relay_status=relay_status,
        post_recovery_batch_id=post_recovery_batch_id,
    )


def verify_evidence(
    deployment: ThreeMachineDeployment,
    *,
    f52_report: dict[str, Any],
    f52_authority: dict[str, Any],
    f52_promoted_primary: dict[str, Any],
    f52_former_primary: dict[str, Any],
    recovery_operation: dict[str, Any],
    authority: dict[str, Any],
    current_primary: dict[str, Any],
    reinstated_replica: dict[str, Any],
    post_recovery_probe: dict[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    evidence = (authority, current_primary, reinstated_replica, post_recovery_probe)
    check(
        "schemas",
        recovery_operation.get("schema") == OPERATION_SCHEMA
        and all(item.get("schema") == EVIDENCE_SCHEMA for item in evidence),
        "all F5.3 inputs use the public recovery schemas",
    )
    check(
        "deployment-binding",
        recovery_operation.get("deployment_id") == deployment.deployment_id
        and recovery_operation.get("session_id") == deployment.session_id
        and recovery_operation.get("group_id") == deployment.group_id
        and all(
            item.get("deployment_id") == deployment.deployment_id
            and item.get("session_id") == deployment.session_id
            and item.get("group_id") == deployment.group_id
            for item in evidence
        ),
        "all evidence belongs to the same deployment, session, and group",
    )
    baseline_ok = True
    try:
        baseline = validate_f52_baseline(
            deployment,
            f52_report=f52_report,
            f52_authority=f52_authority,
            f52_promoted_primary=f52_promoted_primary,
            f52_former_primary=f52_former_primary,
        )
    except StorageDeploymentError:
        baseline_ok = False
        baseline = {}
    check(
        "f52-baseline",
        baseline_ok,
        "F5.3 starts from a passed degraded F5.2 deployment",
    )
    promoted_grant = _object(baseline.get("promoted_grant"))
    baseline_manifest = _object(baseline.get("manifest"))
    baseline_failover = _object(baseline.get("failover"))
    control = _object(authority.get("control"))
    grant = _object(control.get("grant"))
    manifest = _object(control.get("manifest"))
    post_item = _object(manifest.get("post_recovery_batch"))
    catchup = _object(authority.get("catchup"))
    catchup_report = _object(catchup.get("final_report"))
    reinstatement = _object(authority.get("reinstatement"))
    assessment = _object(authority.get("final_replica_assessment"))
    final_report = _object(assessment.get("report"))
    operation_record = _object(recovery_operation.get("record"))

    check(
        "recovery-operation",
        recovery_operation.get("status") == "completed"
        and recovery_operation.get("reinstatement_id")
        == reinstatement.get("reinstatement_id")
        and operation_record.get("state") == "completed"
        and operation_record.get("reinstatement_id")
        == reinstatement.get("reinstatement_id"),
        "operator recovery completed through the production F4.1/F4.2 path",
    )
    check(
        "catchup-complete",
        catchup.get("state") == "caught-up"
        and catchup.get("source_provider_id") == deployment.replica.provider_id
        and catchup.get("source_node_id") == deployment.replica.node_id
        and catchup.get("returning_provider_id") == deployment.primary.provider_id
        and catchup.get("returning_node_id") == deployment.primary.node_id
        and bool(_list(catchup.get("items")))
        and all(
            isinstance(item, dict) and item.get("status") == "verified"
            for item in _list(catchup.get("items"))
        )
        and catchup_report.get("integrity_verified") is True
        and catchup_report.get("synchronization_state") == "synchronized"
        and catchup_report.get("eligible") is True,
        "Machine B is completely repaired before assignment",
    )
    check(
        "catchup-manifest-binding",
        catchup.get("manifest_revision") == baseline_manifest.get("revision")
        and catchup.get("manifest_hash") == baseline_manifest.get("manifest_hash")
        and catchup_report.get("manifest_revision")
        == baseline_manifest.get("revision")
        and catchup_report.get("manifest_hash")
        == baseline_manifest.get("manifest_hash"),
        "catch-up is bound to the retained F5.2 authoritative manifest",
    )
    check(
        "reinstatement-complete",
        reinstatement.get("state") == "completed"
        and reinstatement.get("failover_id") == baseline_failover.get("failover_id")
        and reinstatement.get("catchup_recovery_id") == catchup.get("recovery_id")
        and reinstatement.get("source_provider_id")
        == deployment.replica.provider_id
        and reinstatement.get("returning_provider_id")
        == deployment.primary.provider_id
        and reinstatement.get("previous_replica_provider_ids") == []
        and reinstatement.get("restored_replica_provider_ids")
        == [deployment.primary.provider_id]
        and reinstatement.get("previous_acknowledgement_mode")
        == AcknowledgementMode.ONE_REPLICA.value
        and reinstatement.get("effective_acknowledgement_mode")
        == AcknowledgementMode.PRIMARY.value
        and isinstance(reinstatement.get("final_publication"), dict),
        "durable F4.2 evidence restores exactly Machine B as replica",
    )
    check(
        "leadership-preserved",
        control.get("primary_provider_id") == deployment.replica.provider_id
        and control.get("replica_provider_ids") == [deployment.primary.provider_id]
        and grant.get("provider_id") == deployment.replica.provider_id
        and grant.get("grant_id") == promoted_grant.get("grant_id")
        and grant.get("term") == promoted_grant.get("term")
        and grant.get("fencing_token") == promoted_grant.get("fencing_token")
        and reinstatement.get("grant_id") == promoted_grant.get("grant_id")
        and reinstatement.get("term") == promoted_grant.get("term")
        and reinstatement.get("fencing_token")
        == promoted_grant.get("fencing_token"),
        "Machine C remains primary without a new grant, term, or fencing token",
    )
    check(
        "normal-control-restored",
        control.get("acknowledgement_mode")
        == AcknowledgementMode.ONE_REPLICA.value
        and control.get("degraded_state") is None,
        "normal acknowledgement policy is restored and degraded state is cleared",
    )
    check(
        "fresh-final-report",
        assessment.get("provider_id") == deployment.primary.provider_id
        and assessment.get("accepted") is True
        and assessment.get("eligibility") is True
        and assessment.get("report_revision")
        == reinstatement.get("final_report_revision")
        and assessment.get("report_hash")
        == reinstatement.get("final_report_hash")
        and final_report.get("integrity_verified") is True
        and final_report.get("synchronization_state") == "synchronized"
        and final_report.get("eligible") is True
        and final_report.get("manifest_revision")
        == reinstatement.get("manifest_revision")
        and final_report.get("manifest_hash") == reinstatement.get("manifest_hash"),
        "reinstatement is supported by fresh assigned-replica integrity proof",
    )
    check(
        "manifest-progressed",
        isinstance(manifest.get("revision"), int)
        and isinstance(baseline_manifest.get("revision"), int)
        and manifest.get("revision") > baseline_manifest.get("revision"),
        "authoritative manifest advances only after restored redundancy",
    )
    check(
        "post-recovery-write",
        post_recovery_probe.get("role") == "post-recovery-probe"
        and post_recovery_probe.get("primary_provider_id")
        == deployment.replica.provider_id
        and post_recovery_probe.get("required_replica_acks") == 1
        and post_recovery_probe.get("acknowledged_replica_ids")
        == [deployment.primary.provider_id]
        and post_recovery_probe.get("batch_id") == post_item.get("batch_id")
        and post_recovery_probe.get("content_hash") == post_item.get("content_hash")
        and post_recovery_probe.get("manifest_revision")
        == manifest.get("revision")
        and post_recovery_probe.get("manifest_hash")
        == manifest.get("manifest_hash"),
        "final write is committed by Machine C and acknowledged by Machine B",
    )

    primary_provider, primary_sync = _provider_status(current_primary)
    replica_provider, replica_sync = _provider_status(reinstated_replica)
    check(
        "current-primary-state",
        current_primary.get("role") == "current-primary"
        and current_primary.get("node_id") == deployment.replica.node_id
        and current_primary.get("provider_id") == deployment.replica.provider_id
        and primary_provider.get("groups")
        == [{"group_id": deployment.group_id, "role": "primary"}]
        and primary_sync.get("ready") is True,
        "Machine C retains verified signed primary control after restart",
    )
    check(
        "reinstated-replica-state",
        reinstated_replica.get("role") == "reinstated-replica"
        and reinstated_replica.get("node_id") == deployment.primary.node_id
        and reinstated_replica.get("provider_id") == deployment.primary.provider_id
        and replica_provider.get("groups")
        == [{"group_id": deployment.group_id, "role": "replica"}]
        and replica_sync.get("ready") is True,
        "Machine B has stable identity and verified signed replica control",
    )
    primary_batches = _object(current_primary.get("batches"))
    replica_batches = _object(reinstated_replica.get("batches"))
    data_ok = True
    for name in ("pre_failure", "post_failover", "post_recovery"):
        primary_batch = _object(primary_batches.get(name))
        replica_batch = _object(replica_batches.get(name))
        data_ok = data_ok and (
            primary_batch.get("present") is True
            and replica_batch.get("present") is True
            and primary_batch.get("batch_id") == replica_batch.get("batch_id")
            and primary_batch.get("content_hash")
            == replica_batch.get("content_hash")
        )
    check(
        "replicated-data",
        data_ok,
        (
            "both storage machines contain matching pre-failure, failover, "
            "and recovery data"
        ),
    )
    check(
        "no-inbound-listeners",
        primary_provider.get("inbound_listener_ports") == []
        and replica_provider.get("inbound_listener_ports") == [],
        "neither storage machine exposes an inbound application listener",
    )
    relay = _object(authority.get("relay"))
    relay_nodes = {
        item.get("node_id"): item
        for item in _list(relay.get("nodes"))
        if isinstance(item, dict)
    }
    capabilities = [
        item for item in _list(relay.get("capabilities")) if isinstance(item, dict)
    ]
    check(
        "relay-connections",
        all(
            isinstance(relay_nodes.get(node.node_id), dict)
            and relay_nodes[node.node_id].get("connection_state") == "connected"
            and relay_nodes[node.node_id].get("revoked") is not True
            for node in (deployment.primary, deployment.replica)
        ),
        "both storage nodes are connected and non-revoked",
    )
    check(
        "relay-storage-capabilities",
        all(
            any(
                capability.get("node_id") == node.node_id
                and capability.get("type") == "storage-provider"
                and capability.get("status") == "ready"
                and isinstance(capability.get("properties"), dict)
                and capability["properties"].get("provider_id")
                == node.provider_id
                for capability in capabilities
            )
            for node in (deployment.primary, deployment.replica)
        ),
        "both restored storage capabilities are ready and identity-bound",
    )
    report = {
        "schema": REPORT_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "current_primary": deployment.replica.to_dict(),
        "reinstated_replica": deployment.primary.to_dict(),
        "preserved_grant": {
            "grant_id": grant.get("grant_id"),
            "term": grant.get("term"),
            "fencing_token": grant.get("fencing_token"),
        },
        "catchup": {
            "recovery_id": catchup.get("recovery_id"),
            "state": catchup.get("state"),
            "manifest_revision": catchup.get("manifest_revision"),
            "manifest_hash": catchup.get("manifest_hash"),
            "final_report_revision": catchup.get("final_report_revision"),
            "final_report_hash": catchup.get("final_report_hash"),
        },
        "reinstatement": {
            "reinstatement_id": reinstatement.get("reinstatement_id"),
            "state": reinstatement.get("state"),
            "final_report_revision": reinstatement.get("final_report_revision"),
            "final_report_hash": reinstatement.get("final_report_hash"),
        },
        "assignment": {
            "primary_provider_id": control.get("primary_provider_id"),
            "replica_provider_ids": control.get("replica_provider_ids"),
        },
        "acknowledgement_mode": control.get("acknowledgement_mode"),
        "degraded_state": control.get("degraded_state"),
        "manifest": {
            "revision": manifest.get("revision"),
            "manifest_hash": manifest.get("manifest_hash"),
        },
        "post_recovery_batch": post_item,
        "passed": all(item["passed"] for item in checks),
        "checks": checks,
    }
    return report


def _load_content(arguments: argparse.Namespace) -> Any:
    if arguments.content_file:
        return _read_json(arguments.content_file, field="content_file")
    try:
        return json.loads(arguments.content_json)
    except json.JSONDecodeError as exc:
        raise StorageDeploymentError(
            "invalid-f53-content", "content_json", "must contain valid JSON"
        ) from exc


def _add_baseline_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--deployment", required=True)
    command.add_argument("--f52-report", required=True)
    command.add_argument("--f52-authority", required=True)
    command.add_argument("--f52-promoted-primary", required=True)
    command.add_argument("--f52-former-primary", required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    baseline = commands.add_parser("validate-baseline")
    _add_baseline_arguments(baseline)

    recover = commands.add_parser("recover")
    _add_baseline_arguments(recover)
    recover.add_argument("--relay-control-database", required=True)
    recover.add_argument("--storage-control-database", required=True)
    recover.add_argument("--failover-database", required=True)
    recover.add_argument("--catchup-database", required=True)
    recover.add_argument("--publication-database", required=True)
    recover.add_argument("--authority-state-dir", required=True)
    recover.add_argument("--catchup-limit", type=int, default=100)
    recover.add_argument("--timeout", type=float, default=15.0)
    recover.add_argument("--output", required=True)
    recover.add_argument("--force", action="store_true")

    probe = commands.add_parser("post-recovery-probe")
    _add_baseline_arguments(probe)
    probe.add_argument("--authority-state-dir", required=True)
    probe.add_argument("--control-database", required=True)
    probe.add_argument("--acknowledgements-database", required=True)
    probe.add_argument("--dataset-id", default="f53-recovery")
    probe.add_argument("--batch-id", required=True)
    probe.add_argument("--idempotency-key", required=True)
    source = probe.add_mutually_exclusive_group(required=True)
    source.add_argument("--content-json")
    source.add_argument("--content-file")
    probe.add_argument("--timeout", type=float, default=15.0)
    probe.add_argument("--output", required=True)
    probe.add_argument("--force", action="store_true")

    storage = commands.add_parser("capture-storage")
    _add_baseline_arguments(storage)
    storage.add_argument(
        "--role", choices=("current-primary", "reinstated-replica"), required=True
    )
    storage.add_argument("--config", required=True)
    storage.add_argument("--pre-failure-batch-id", required=True)
    storage.add_argument("--post-failover-batch-id", required=True)
    storage.add_argument("--post-recovery-batch-id", required=True)
    storage.add_argument("--output", required=True)
    storage.add_argument("--force", action="store_true")

    authority = commands.add_parser("capture-authority")
    _add_baseline_arguments(authority)
    authority.add_argument("--authority-state-dir", required=True)
    authority.add_argument("--control-database", required=True)
    authority.add_argument("--failover-database", required=True)
    authority.add_argument("--catchup-database", required=True)
    authority.add_argument("--post-recovery-batch-id", required=True)
    authority.add_argument("--timeout", type=float, default=15.0)
    authority.add_argument("--output", required=True)
    authority.add_argument("--force", action="store_true")

    verify = commands.add_parser("verify")
    _add_baseline_arguments(verify)
    verify.add_argument("--recovery-operation", required=True)
    verify.add_argument("--authority", required=True)
    verify.add_argument("--current-primary", required=True)
    verify.add_argument("--reinstated-replica", required=True)
    verify.add_argument("--post-recovery-probe", required=True)
    verify.add_argument("--output", required=True)
    verify.add_argument("--force", action="store_true")
    return parser


def _load_baseline(
    arguments: argparse.Namespace,
) -> tuple[
    ThreeMachineDeployment,
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    deployment = ThreeMachineDeployment.load(arguments.deployment)
    report = _read_json(arguments.f52_report, field="f52_report")
    authority = _read_json(arguments.f52_authority, field="f52_authority")
    promoted = _read_json(
        arguments.f52_promoted_primary, field="f52_promoted_primary"
    )
    former = _read_json(arguments.f52_former_primary, field="f52_former_primary")
    validate_f52_baseline(
        deployment,
        f52_report=report,
        f52_authority=authority,
        f52_promoted_primary=promoted,
        f52_former_primary=former,
    )
    return deployment, report, authority, promoted, former


async def _run(arguments: argparse.Namespace) -> int:
    deployment, report, f52_authority, promoted, former = _load_baseline(arguments)
    if arguments.command == "validate-baseline":
        result = validate_f52_baseline(
            deployment,
            f52_report=report,
            f52_authority=f52_authority,
            f52_promoted_primary=promoted,
            f52_former_primary=former,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    if arguments.command == "recover":
        result = await run_recovery_operation(
            deployment,
            relay_control_database=arguments.relay_control_database,
            storage_control_database=arguments.storage_control_database,
            failover_database=arguments.failover_database,
            catchup_database=arguments.catchup_database,
            publication_database=arguments.publication_database,
            authority_state_directory=arguments.authority_state_dir,
            catchup_limit=arguments.catchup_limit,
            timeout=arguments.timeout,
        )
    elif arguments.command == "post-recovery-probe":
        result = await post_recovery_probe(
            deployment,
            f52_report=report,
            authority_state_directory=arguments.authority_state_dir,
            control_database=arguments.control_database,
            acknowledgements_database=arguments.acknowledgements_database,
            dataset_id=arguments.dataset_id,
            batch_id=arguments.batch_id,
            idempotency_key=arguments.idempotency_key,
            content=_load_content(arguments),
            timeout=arguments.timeout,
        )
    elif arguments.command == "capture-storage":
        result = storage_evidence(
            deployment,
            role=arguments.role,
            config_path=arguments.config,
            pre_failure_batch_id=arguments.pre_failure_batch_id,
            post_failover_batch_id=arguments.post_failover_batch_id,
            post_recovery_batch_id=arguments.post_recovery_batch_id,
        )
    elif arguments.command == "capture-authority":
        result = await authority_evidence(
            deployment,
            f52_report=report,
            authority_state_directory=arguments.authority_state_dir,
            control_database=arguments.control_database,
            failover_database=arguments.failover_database,
            catchup_database=arguments.catchup_database,
            post_recovery_batch_id=arguments.post_recovery_batch_id,
            timeout=arguments.timeout,
        )
    else:
        result = verify_evidence(
            deployment,
            f52_report=report,
            f52_authority=f52_authority,
            f52_promoted_primary=promoted,
            f52_former_primary=former,
            recovery_operation=_read_json(
                arguments.recovery_operation, field="recovery_operation"
            ),
            authority=_read_json(arguments.authority, field="authority"),
            current_primary=_read_json(
                arguments.current_primary, field="current_primary"
            ),
            reinstated_replica=_read_json(
                arguments.reinstated_replica, field="reinstated_replica"
            ),
            post_recovery_probe=_read_json(
                arguments.post_recovery_probe, field="post_recovery_probe"
            ),
        )
    _write_json(arguments.output, result, force=arguments.force)
    print(json.dumps(result, sort_keys=True))
    if arguments.command == "recover":
        return 0 if result.get("status") == "completed" else 2
    if arguments.command == "verify":
        return 0 if result["passed"] else 2
    return 0


def main() -> int:
    arguments = _parser().parse_args()
    try:
        return asyncio.run(_run(arguments))
    except KeyboardInterrupt:
        return 130
    except (FederationValidationError, TimeoutError, OSError, ValueError) as exc:
        if isinstance(exc, FederationValidationError):
            error = {"code": exc.code, "field": exc.field, "message": exc.message}
        else:
            error = {
                "code": "storage-f53-runtime-error",
                "field": "runtime",
                "message": str(exc) or type(exc).__name__,
            }
        print(json.dumps({"error": error}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
