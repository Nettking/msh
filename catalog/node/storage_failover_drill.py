"""Phase F5.2 physical failover drill and public evidence tooling.

F5.2 starts from a passed F5.1 three-machine deployment, observes physical
primary loss through the relay, and records evidence produced by the existing
F3 ``StorageFailoverCoordinator``.  It deliberately stops before former-primary
catch-up or replica reinstatement; those operations belong to F5.3.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.errors import FederationValidationError
from catalog.federation.live_failover import LiveFailoverStore
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    WriteAuthority,
)

from .client import RelayNodeClient
from .live_storage_agent import LiveStorageNodeAgent
from .storage_agent import StorageNodeConfig
from .storage_deployment import (
    EVIDENCE_SCHEMA as F51_EVIDENCE_SCHEMA,
    REPORT_SCHEMA as F51_REPORT_SCHEMA,
    StorageDeploymentError,
    ThreeMachineDeployment,
)

EVIDENCE_SCHEMA = "fcp.storage_physical_failover_evidence.v1"
REPORT_SCHEMA = "fcp.storage_physical_failover_report.v1"
_ALLOWED_STALE_REJECTIONS = {
    StorageErrorCode.NOT_PRIMARY.value,
    StorageErrorCode.STALE_TERM.value,
    StorageErrorCode.STALE_FENCING_TOKEN.value,
    StorageErrorCode.LEASE_EXPIRED.value,
    StorageErrorCode.UNKNOWN_GRANT.value,
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise StorageDeploymentError(
            "invalid-f52-timestamp", "captured_at", "must be timezone-aware"
        )
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise StorageDeploymentError(
            "invalid-f52-timestamp", field, "must be RFC 3339 text"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise StorageDeploymentError(
            "invalid-f52-timestamp", field, "must be RFC 3339 text"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise StorageDeploymentError(
            "invalid-f52-timestamp", field, "must be timezone-aware"
        )
    return parsed.astimezone(timezone.utc)


def _text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise StorageDeploymentError(
            "invalid-f52-field", field, "must be non-empty text"
        )
    return value


def _read_json(path: Path | str, *, field: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise StorageDeploymentError(
            "f52-file-unavailable", field, "could not read JSON file"
        ) from exc
    except json.JSONDecodeError as exc:
        raise StorageDeploymentError(
            "invalid-f52-json", field, "must contain valid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise StorageDeploymentError(
            "invalid-f52-json", field, "must contain a JSON object"
        )
    return value


def _write_json(path: Path | str, value: dict[str, Any], *, force: bool) -> None:
    output = Path(path)
    if output.exists() and not force:
        raise StorageDeploymentError(
            "f52-output-exists",
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


def _object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def validate_f51_baseline(
    deployment: ThreeMachineDeployment,
    *,
    baseline_report: dict[str, Any],
    baseline_authority: dict[str, Any],
) -> dict[str, Any]:
    """Fail closed unless the inputs describe one passed, normal F5.1 state."""

    if (
        baseline_report.get("schema") != F51_REPORT_SCHEMA
        or baseline_report.get("deployment_id") != deployment.deployment_id
        or baseline_report.get("passed") is not True
    ):
        raise StorageDeploymentError(
            "f52-baseline-not-passed",
            "baseline_report",
            "F5.2 requires a passed F5.1 report for the same deployment",
        )
    if (
        baseline_authority.get("schema") != F51_EVIDENCE_SCHEMA
        or baseline_authority.get("deployment_id") != deployment.deployment_id
        or baseline_authority.get("role") != "authority"
        or baseline_authority.get("session_id") != deployment.session_id
        or baseline_authority.get("group_id") != deployment.group_id
    ):
        raise StorageDeploymentError(
            "f52-baseline-authority-mismatch",
            "baseline_authority",
            "F5.1 authority evidence is missing or belongs to another deployment",
        )
    control = _object(baseline_authority.get("control"))
    grant = _object(control.get("grant"))
    manifest = _object(control.get("manifest"))
    if (
        control.get("primary_provider_id") != deployment.primary.provider_id
        or control.get("replica_provider_ids") != [deployment.replica.provider_id]
        or control.get("acknowledgement_mode")
        != AcknowledgementMode.ONE_REPLICA.value
        or control.get("degraded_state") is not None
    ):
        raise StorageDeploymentError(
            "f52-baseline-not-normal",
            "baseline_authority.control",
            "F5.2 must start from normal primary/replica operation",
        )
    if (
        grant.get("provider_id") != deployment.primary.provider_id
        or not isinstance(grant.get("grant_id"), str)
        or not isinstance(grant.get("term"), int)
        or not isinstance(grant.get("fencing_token"), int)
        or not isinstance(grant.get("lease_expires_at"), str)
    ):
        raise StorageDeploymentError(
            "f52-baseline-grant-invalid",
            "baseline_authority.control.grant",
            "F5.1 evidence must contain the original primary grant",
        )
    _parse_timestamp(grant["lease_expires_at"], "grant.lease_expires_at")
    if (
        not isinstance(manifest.get("revision"), int)
        or not isinstance(manifest.get("manifest_hash"), str)
    ):
        raise StorageDeploymentError(
            "f52-baseline-manifest-invalid",
            "baseline_authority.control.manifest",
            "F5.1 evidence must contain the authoritative manifest identity",
        )
    return {
        "schema": "fcp.storage_physical_failover_baseline.v1",
        "deployment_id": deployment.deployment_id,
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "original_primary": deployment.primary.to_dict(),
        "original_replica": deployment.replica.to_dict(),
        "source_grant": {
            "grant_id": grant["grant_id"],
            "term": grant["term"],
            "fencing_token": grant["fencing_token"],
            "lease_expires_at": grant["lease_expires_at"],
        },
        "manifest": {
            "revision": manifest["revision"],
            "manifest_hash": manifest["manifest_hash"],
        },
    }


async def post_failover_probe_with_transport(
    deployment: ThreeMachineDeployment,
    *,
    control: PhaseDControlPlane,
    transport: RelayStorageEndpoint,
    acknowledgements: DurableAcknowledgementStore,
    dataset_id: str,
    batch_id: str,
    idempotency_key: str,
    content: Any,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Commit one write only after the replica is the degraded primary."""

    snapshot = control.snapshot(deployment.session_id)
    assignment = snapshot.groups.get(deployment.group_id)
    grant = snapshot.leader_grants.get(deployment.group_id)
    degraded = control.storage_degraded_state(
        deployment.session_id, deployment.group_id
    )
    if (
        assignment is None
        or assignment.primary_provider_id != deployment.replica.provider_id
        or tuple(assignment.replica_provider_ids)
        or grant is None
        or grant.get("provider_id") != deployment.replica.provider_id
        or control.acknowledgement_policy(
            deployment.session_id, deployment.group_id
        ).mode
        is not AcknowledgementMode.PRIMARY
        or not isinstance(degraded, dict)
        or degraded.get("reason_code")
        != "automatic-failover-redundancy-lost"
    ):
        raise StorageDeploymentError(
            "f52-promotion-not-established",
            "control",
            "post-failover writes require the verified degraded promotion state",
        )
    outcome = await PhaseDLogicalStorageClient(
        session_id=deployment.session_id,
        actor_node_id=deployment.authority_node_id,
        control_plane=control,
        transport=transport,
        acknowledgements=acknowledgements,
    ).ingest_batch(
        group_id=deployment.group_id,
        dataset_id=_text(dataset_id, "dataset_id"),
        batch_id=_text(batch_id, "batch_id"),
        idempotency_key=_text(idempotency_key, "idempotency_key"),
        content=content,
        created_at=created_at or _now(),
    )
    if not outcome.committed or outcome.result is None:
        raise StorageDeploymentError(
            outcome.error_code or "f52-probe-not-committed",
            "batch_id",
            outcome.message or "post-failover probe was not committed",
        )
    acknowledgement = acknowledgements.status(
        deployment.session_id, deployment.group_id, batch_id
    )
    if (
        acknowledgement is None
        or not acknowledgement.committed
        or acknowledgement.required_replica_acks != 0
        or acknowledgement.acknowledged_replica_ids
    ):
        raise StorageDeploymentError(
            "f52-probe-acknowledgement-mismatch",
            "batch_id",
            "degraded write must commit under the explicit primary-only policy",
        )
    manifest = control.manifest(deployment.session_id, deployment.group_id)
    item = next(
        (value for value in manifest.items if value.item_id == batch_id), None
    )
    if item is None or item.content_hash != outcome.result.content_hash:
        raise StorageDeploymentError(
            "f52-probe-manifest-missing",
            "batch_id",
            "authoritative manifest does not contain the post-failover batch",
        )
    return {
        "schema": EVIDENCE_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "role": "post-failover-probe",
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "batch_id": batch_id,
        "content_hash": outcome.result.content_hash,
        "primary_provider_id": deployment.replica.provider_id,
        "required_replica_acks": 0,
        "acknowledged_replica_ids": [],
        "manifest_revision": manifest.revision,
        "manifest_hash": manifest.manifest_hash,
    }


async def post_failover_probe(
    deployment: ThreeMachineDeployment,
    *,
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
        return await post_failover_probe_with_transport(
            deployment,
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
    captured_at: datetime | None = None,
) -> dict[str, Any]:
    if role not in {"promoted-primary", "former-primary"}:
        raise StorageDeploymentError(
            "invalid-f52-storage-role",
            "role",
            "must be promoted-primary or former-primary",
        )
    expected = deployment.replica if role == "promoted-primary" else deployment.primary
    if (
        agent.node_id != expected.node_id
        or agent.storage.config.provider_id != expected.provider_id
        or agent.storage.config.session_id != deployment.session_id
        or agent.storage.config.relay_url != deployment.relay_url
    ):
        raise StorageDeploymentError(
            "f52-storage-identity-mismatch",
            "storage",
            "local storage identity or configuration differs from the deployment",
        )
    latest = agent.replica_store.latest(deployment.session_id)
    if latest is None:
        raise StorageDeploymentError(
            "f52-control-missing",
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
        },
    }


def storage_evidence(
    deployment: ThreeMachineDeployment,
    *,
    role: str,
    config_path: Path | str,
    pre_failure_batch_id: str,
    post_failover_batch_id: str,
    captured_at: datetime | None = None,
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
        captured_at=captured_at,
    )


async def stale_authority_probe_with_transport(
    deployment: ThreeMachineDeployment,
    *,
    baseline_authority: dict[str, Any],
    transport: RelayStorageEndpoint,
    batch_id: str,
    idempotency_key: str,
    content: Any,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    grant = _object(_object(baseline_authority.get("control")).get("grant"))
    request = BatchIngestRequest(
        authority=WriteAuthority(
            session_id=deployment.session_id,
            group_id=deployment.group_id,
            actor_node_id=deployment.primary.node_id,
            grant_id=_text(grant.get("grant_id"), "source_grant.grant_id"),
            term=grant.get("term"),
            fencing_token=grant.get("fencing_token"),
            lease_expires_at=_parse_timestamp(
                grant.get("lease_expires_at"), "source_grant.lease_expires_at"
            ),
        ),
        dataset_id="f52-stale-authority",
        batch_id=_text(batch_id, "batch_id"),
        idempotency_key=_text(idempotency_key, "idempotency_key"),
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        content=content,
        created_at=created_at or _now(),
    )
    response = await transport.request(
        target_node_id=deployment.primary.node_id,
        envelope=StorageRequestEnvelope(
            request_id=f"f52-stale-{deployment.deployment_id}-{batch_id}",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_INGEST,
            session_id=deployment.session_id,
            actor_node_id=deployment.authority_node_id,
            authorization_context={
                "kind": "storage-primary-route",
                "group_id": deployment.group_id,
                "provider_id": deployment.primary.provider_id,
            },
            payload=request.to_dict(),
        ),
    )
    if response.ok or response.error is None:
        raise StorageDeploymentError(
            "f52-stale-authority-accepted",
            "source_grant",
            "returning former primary accepted its obsolete authority",
        )
    return {
        "schema": EVIDENCE_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "role": "stale-authority-probe",
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "target_node_id": deployment.primary.node_id,
        "target_provider_id": deployment.primary.provider_id,
        "source_grant_id": grant.get("grant_id"),
        "source_term": grant.get("term"),
        "source_fencing_token": grant.get("fencing_token"),
        "accepted": False,
        "error": response.error.to_dict(),
    }


async def stale_authority_probe(
    deployment: ThreeMachineDeployment,
    *,
    baseline_authority: dict[str, Any],
    authority_state_directory: Path | str,
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
        return await stale_authority_probe_with_transport(
            deployment,
            baseline_authority=baseline_authority,
            transport=endpoint,
            batch_id=batch_id,
            idempotency_key=idempotency_key,
            content=content,
        )
    finally:
        await endpoint.close()
        await client.disconnect()


def _observation_id(
    deployment: ThreeMachineDeployment, source_grant_id: str
) -> str:
    value = (
        f"{deployment.session_id}\0{deployment.group_id}\0"
        f"{deployment.primary.provider_id}\0{source_grant_id}"
    ).encode()
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _failover_id(
    observation_id: str, promoted_provider_id: str, report_hash: str
) -> str:
    value = f"{observation_id}\0{promoted_provider_id}\0{report_hash}".encode()
    return "sha256:" + hashlib.sha256(value).hexdigest()


def build_authority_evidence(
    deployment: ThreeMachineDeployment,
    *,
    baseline_authority: dict[str, Any],
    control: PhaseDControlPlane,
    failover_store: LiveFailoverStore,
    relay_status: dict[str, Any],
    post_failover_batch_id: str,
    captured_at: datetime | None = None,
) -> dict[str, Any]:
    source_grant = _object(
        _object(baseline_authority.get("control")).get("grant")
    )
    snapshot = control.snapshot(deployment.session_id)
    assignment = snapshot.groups.get(deployment.group_id)
    if assignment is None:
        raise StorageDeploymentError(
            "f52-group-missing", "group_id", "storage group is not provisioned"
        )
    assessment = control.latest_storage_replica_assessment(
        deployment.session_id,
        deployment.group_id,
        deployment.replica.provider_id,
    )
    if assessment is None or assessment.report is None:
        raise StorageDeploymentError(
            "f52-selected-report-missing",
            "replica_report",
            "promotion requires a durable fresh replica assessment",
        )
    observation_id = _observation_id(
        deployment, _text(source_grant.get("grant_id"), "source_grant_id")
    )
    failover_id = _failover_id(
        observation_id, deployment.replica.provider_id, assessment.report_hash
    )
    record = failover_store.get(
        deployment.session_id, deployment.group_id, failover_id
    )
    if record is None:
        raise StorageDeploymentError(
            "f52-failover-record-missing",
            "failover_id",
            "durable failover record could not be derived from selected evidence",
        )
    manifest = control.manifest(deployment.session_id, deployment.group_id)
    post_item = next(
        (
            item
            for item in manifest.items
            if item.item_id == post_failover_batch_id
        ),
        None,
    )
    if post_item is None:
        raise StorageDeploymentError(
            "f52-post-batch-missing",
            "post_failover_batch_id",
            "authoritative manifest does not contain the post-failover batch",
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
                "post_failover_batch": {
                    "batch_id": post_item.item_id,
                    "content_hash": post_item.content_hash,
                    "idempotency_key": post_item.idempotency_key,
                },
            },
        },
        "selected_replica_assessment": assessment.to_dict(),
        "failover": record.to_dict(),
        "relay": {"nodes": nodes, "capabilities": capabilities},
    }


async def authority_evidence(
    deployment: ThreeMachineDeployment,
    *,
    baseline_authority: dict[str, Any],
    authority_state_directory: Path | str,
    control_database: Path | str,
    failover_database: Path | str,
    post_failover_batch_id: str,
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
        baseline_authority=baseline_authority,
        control=PhaseDControlPlane(control_database),
        failover_store=LiveFailoverStore(failover_database),
        relay_status=relay_status,
        post_failover_batch_id=post_failover_batch_id,
    )


def verify_evidence(
    deployment: ThreeMachineDeployment,
    *,
    baseline_report: dict[str, Any],
    baseline_authority: dict[str, Any],
    authority: dict[str, Any],
    promoted_primary: dict[str, Any],
    former_primary: dict[str, Any],
    post_failover_probe: dict[str, Any],
    stale_authority_probe: dict[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    evidence = (
        authority,
        promoted_primary,
        former_primary,
        post_failover_probe,
        stale_authority_probe,
    )
    check(
        "schemas",
        all(item.get("schema") == EVIDENCE_SCHEMA for item in evidence),
        "all F5.2 inputs use the public failover evidence schema",
    )
    check(
        "deployment-binding",
        all(item.get("deployment_id") == deployment.deployment_id for item in evidence),
        "all evidence belongs to the same deployment",
    )
    baseline_ok = True
    try:
        baseline = validate_f51_baseline(
            deployment,
            baseline_report=baseline_report,
            baseline_authority=baseline_authority,
        )
    except StorageDeploymentError:
        baseline_ok = False
        baseline = {}
    check(
        "f51-baseline",
        baseline_ok,
        "F5.2 starts from a passed normal F5.1 deployment",
    )
    source_grant = _object(baseline.get("source_grant"))
    baseline_manifest = _object(baseline.get("manifest"))
    control = _object(authority.get("control"))
    new_grant = _object(control.get("grant"))
    degraded = _object(control.get("degraded_state"))
    manifest = _object(control.get("manifest"))
    post_item = _object(manifest.get("post_failover_batch"))
    assessment = _object(authority.get("selected_replica_assessment"))
    selected_report = _object(assessment.get("report"))
    failover = _object(authority.get("failover"))

    check(
        "promoted-assignment",
        control.get("primary_provider_id") == deployment.replica.provider_id
        and control.get("replica_provider_ids") == [],
        "Machine C is primary and no active replica remains",
    )
    check(
        "degraded-policy",
        control.get("acknowledgement_mode") == AcknowledgementMode.PRIMARY.value
        and degraded.get("reason_code")
        == "automatic-failover-redundancy-lost",
        "redundancy loss and primary-only acknowledgement are explicit",
    )
    check(
        "advanced-authority",
        new_grant.get("provider_id") == deployment.replica.provider_id
        and isinstance(new_grant.get("term"), int)
        and isinstance(new_grant.get("fencing_token"), int)
        and isinstance(source_grant.get("term"), int)
        and isinstance(source_grant.get("fencing_token"), int)
        and new_grant.get("term") > source_grant.get("term")
        and new_grant.get("fencing_token")
        > source_grant.get("fencing_token"),
        "promotion advances both term and fencing token",
    )
    check(
        "published-failover",
        failover.get("state") == "published"
        and failover.get("failed_provider_id") == deployment.primary.provider_id
        and failover.get("failed_node_id") == deployment.primary.node_id
        and failover.get("promoted_provider_id") == deployment.replica.provider_id
        and failover.get("promoted_node_id") == deployment.replica.node_id
        and failover.get("source_grant_id") == source_grant.get("grant_id")
        and failover.get("grant_id") == new_grant.get("grant_id")
        and failover.get("term") == new_grant.get("term")
        and failover.get("fencing_token") == new_grant.get("fencing_token"),
        "durable published transaction binds failed and promoted authority",
    )
    check(
        "fresh-selected-report",
        assessment.get("provider_id") == deployment.replica.provider_id
        and assessment.get("accepted") is True
        and assessment.get("eligibility") is True
        and selected_report.get("integrity_verified") is True
        and selected_report.get("eligible") is True
        and assessment.get("report_revision")
        == failover.get("selected_report_revision")
        and assessment.get("report_hash") == failover.get("selected_report_hash"),
        "promotion uses the durable accepted integrity report selected by F3",
    )
    check(
        "manifest-bound-selection",
        selected_report.get("manifest_revision")
        == baseline_manifest.get("revision")
        and selected_report.get("manifest_hash")
        == baseline_manifest.get("manifest_hash"),
        "selected replica evidence is bound to the pre-failure authoritative manifest",
    )
    check(
        "manifest-progressed",
        isinstance(manifest.get("revision"), int)
        and isinstance(baseline_manifest.get("revision"), int)
        and manifest.get("revision") > baseline_manifest.get("revision"),
        "authoritative manifest advances after the degraded write",
    )
    check(
        "post-failover-write",
        post_failover_probe.get("role") == "post-failover-probe"
        and post_failover_probe.get("primary_provider_id")
        == deployment.replica.provider_id
        and post_failover_probe.get("required_replica_acks") == 0
        and post_failover_probe.get("acknowledged_replica_ids") == []
        and post_failover_probe.get("batch_id") == post_item.get("batch_id")
        and post_failover_probe.get("content_hash")
        == post_item.get("content_hash")
        and post_failover_probe.get("manifest_revision")
        == manifest.get("revision")
        and post_failover_probe.get("manifest_hash")
        == manifest.get("manifest_hash"),
        "Machine C commits a manifest-backed write under the degraded policy",
    )

    def provider_status(
        evidence_value: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        status = _object(evidence_value.get("status"))
        return _object(status.get("provider")), _object(status.get("control_sync"))

    promoted_provider, promoted_sync = provider_status(promoted_primary)
    former_provider, former_sync = provider_status(former_primary)
    promoted_batches = _object(promoted_primary.get("batches"))
    former_batches = _object(former_primary.get("batches"))
    promoted_pre = _object(promoted_batches.get("pre_failure"))
    promoted_post = _object(promoted_batches.get("post_failover"))
    former_pre = _object(former_batches.get("pre_failure"))
    former_post = _object(former_batches.get("post_failover"))
    check(
        "promoted-primary-state",
        promoted_primary.get("role") == "promoted-primary"
        and promoted_primary.get("node_id") == deployment.replica.node_id
        and promoted_primary.get("provider_id") == deployment.replica.provider_id
        and promoted_provider.get("groups")
        == [{"group_id": deployment.group_id, "role": "primary"}]
        and promoted_sync.get("ready") is True,
        "Machine C has verified signed primary control",
    )
    check(
        "promoted-primary-data",
        promoted_pre.get("present") is True
        and promoted_post.get("present") is True
        and promoted_post.get("batch_id") == post_item.get("batch_id")
        and promoted_post.get("content_hash") == post_item.get("content_hash"),
        "Machine C retains both pre-failure and post-failover data",
    )
    check(
        "former-primary-state",
        former_primary.get("role") == "former-primary"
        and former_primary.get("node_id") == deployment.primary.node_id
        and former_primary.get("provider_id") == deployment.primary.provider_id
        and former_provider.get("groups")
        == [{"group_id": deployment.group_id, "role": "unassigned"}]
        and former_sync.get("ready") is True,
        "Machine B reconnects with stable identity and verified unassigned control",
    )
    check(
        "former-primary-not-caught-up",
        former_pre.get("present") is True
        and former_post.get("present") is False,
        "Machine B is not caught up or reinstated during F5.2",
    )
    stale_error = _object(stale_authority_probe.get("error"))
    check(
        "stale-authority-rejected",
        stale_authority_probe.get("role") == "stale-authority-probe"
        and stale_authority_probe.get("target_node_id")
        == deployment.primary.node_id
        and stale_authority_probe.get("accepted") is False
        and stale_error.get("code") in _ALLOWED_STALE_REJECTIONS,
        "Machine B rejects the obsolete primary grant with a strict storage error",
    )
    check(
        "no-inbound-listeners",
        promoted_provider.get("inbound_listener_ports") == []
        and former_provider.get("inbound_listener_ports") == [],
        "neither storage machine exposes an inbound application listener",
    )
    relay = _object(authority.get("relay"))
    relay_nodes = {
        item.get("node_id"): item
        for item in _list(relay.get("nodes"))
        if isinstance(item, dict)
    }
    capabilities = [
        item
        for item in _list(relay.get("capabilities"))
        if isinstance(item, dict)
    ]
    check(
        "relay-connections",
        all(
            isinstance(relay_nodes.get(node.node_id), dict)
            and relay_nodes[node.node_id].get("connection_state") == "connected"
            and relay_nodes[node.node_id].get("revoked") is not True
            for node in (deployment.primary, deployment.replica)
        ),
        "promoted and returning storage nodes are connected and non-revoked",
    )
    check(
        "relay-storage-capabilities",
        all(
            any(
                capability.get("node_id") == node.node_id
                and capability.get("type") == "storage-provider"
                and capability.get("status") in {"ready", "unavailable"}
                and isinstance(capability.get("properties"), dict)
                and capability["properties"].get("provider_id")
                == node.provider_id
                for capability in capabilities
            )
            for node in (deployment.primary, deployment.replica)
        ),
        "relay status binds both storage capabilities to their stable providers",
    )
    report = {
        "schema": REPORT_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "original_primary": deployment.primary.to_dict(),
        "promoted_primary": deployment.replica.to_dict(),
        "source_grant": source_grant,
        "promoted_grant": {
            "grant_id": new_grant.get("grant_id"),
            "term": new_grant.get("term"),
            "fencing_token": new_grant.get("fencing_token"),
        },
        "selected_replica_report": {
            "revision": assessment.get("report_revision"),
            "hash": assessment.get("report_hash"),
            "manifest_revision": selected_report.get("manifest_revision"),
            "manifest_hash": selected_report.get("manifest_hash"),
        },
        "failover": {
            "failover_id": failover.get("failover_id"),
            "state": failover.get("state"),
        },
        "assignment": {
            "primary_provider_id": control.get("primary_provider_id"),
            "replica_provider_ids": control.get("replica_provider_ids"),
        },
        "acknowledgement_mode": control.get("acknowledgement_mode"),
        "degraded_reason": degraded.get("reason_code"),
        "manifest": {
            "revision": manifest.get("revision"),
            "manifest_hash": manifest.get("manifest_hash"),
        },
        "post_failover_batch": post_item,
        "former_primary": {
            "node_id": former_primary.get("node_id"),
            "provider_id": former_primary.get("provider_id"),
            "role": "unassigned",
            "control_ready": former_sync.get("ready"),
        },
        "stale_authority_rejection": stale_error,
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
            "invalid-f52-content", "content_json", "must contain valid JSON"
        ) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    baseline = commands.add_parser("validate-baseline")
    baseline.add_argument("--deployment", required=True)
    baseline.add_argument("--baseline-report", required=True)
    baseline.add_argument("--baseline-authority", required=True)

    probe = commands.add_parser("post-failover-probe")
    probe.add_argument("--deployment", required=True)
    probe.add_argument("--baseline-report", required=True)
    probe.add_argument("--baseline-authority", required=True)
    probe.add_argument("--authority-state-dir", required=True)
    probe.add_argument("--control-database", required=True)
    probe.add_argument("--acknowledgements-database", required=True)
    probe.add_argument("--dataset-id", default="f52-failover")
    probe.add_argument("--batch-id", required=True)
    probe.add_argument("--idempotency-key", required=True)
    source = probe.add_mutually_exclusive_group(required=True)
    source.add_argument("--content-json")
    source.add_argument("--content-file")
    probe.add_argument("--output", required=True)
    probe.add_argument("--timeout", type=float, default=15.0)
    probe.add_argument("--force", action="store_true")

    storage = commands.add_parser("capture-storage")
    storage.add_argument("--deployment", required=True)
    storage.add_argument("--baseline-report", required=True)
    storage.add_argument("--baseline-authority", required=True)
    storage.add_argument(
        "--role", choices=("promoted-primary", "former-primary"), required=True
    )
    storage.add_argument("--config", required=True)
    storage.add_argument("--pre-failure-batch-id", required=True)
    storage.add_argument("--post-failover-batch-id", required=True)
    storage.add_argument("--output", required=True)
    storage.add_argument("--force", action="store_true")

    stale = commands.add_parser("stale-authority-probe")
    stale.add_argument("--deployment", required=True)
    stale.add_argument("--baseline-report", required=True)
    stale.add_argument("--baseline-authority", required=True)
    stale.add_argument("--authority-state-dir", required=True)
    stale.add_argument("--batch-id", required=True)
    stale.add_argument("--idempotency-key", required=True)
    source = stale.add_mutually_exclusive_group(required=True)
    source.add_argument("--content-json")
    source.add_argument("--content-file")
    stale.add_argument("--output", required=True)
    stale.add_argument("--timeout", type=float, default=15.0)
    stale.add_argument("--force", action="store_true")

    authority = commands.add_parser("capture-authority")
    authority.add_argument("--deployment", required=True)
    authority.add_argument("--baseline-report", required=True)
    authority.add_argument("--baseline-authority", required=True)
    authority.add_argument("--authority-state-dir", required=True)
    authority.add_argument("--control-database", required=True)
    authority.add_argument("--failover-database", required=True)
    authority.add_argument("--post-failover-batch-id", required=True)
    authority.add_argument("--output", required=True)
    authority.add_argument("--timeout", type=float, default=15.0)
    authority.add_argument("--force", action="store_true")

    verify = commands.add_parser("verify")
    verify.add_argument("--deployment", required=True)
    verify.add_argument("--baseline-report", required=True)
    verify.add_argument("--baseline-authority", required=True)
    verify.add_argument("--authority", required=True)
    verify.add_argument("--promoted-primary", required=True)
    verify.add_argument("--former-primary", required=True)
    verify.add_argument("--post-failover-probe", required=True)
    verify.add_argument("--stale-authority-probe", required=True)
    verify.add_argument("--output", required=True)
    verify.add_argument("--force", action="store_true")
    return parser


def _load_baseline(arguments: argparse.Namespace) -> tuple[ThreeMachineDeployment, dict[str, Any], dict[str, Any]]:
    deployment = ThreeMachineDeployment.load(arguments.deployment)
    baseline_report = _read_json(
        arguments.baseline_report, field="baseline_report"
    )
    baseline_authority = _read_json(
        arguments.baseline_authority, field="baseline_authority"
    )
    validate_f51_baseline(
        deployment,
        baseline_report=baseline_report,
        baseline_authority=baseline_authority,
    )
    return deployment, baseline_report, baseline_authority


async def _run(arguments: argparse.Namespace) -> int:
    deployment, baseline_report, baseline_authority = _load_baseline(arguments)
    if arguments.command == "validate-baseline":
        result = validate_f51_baseline(
            deployment,
            baseline_report=baseline_report,
            baseline_authority=baseline_authority,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    if arguments.command == "post-failover-probe":
        result = await post_failover_probe(
            deployment,
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
        )
    elif arguments.command == "stale-authority-probe":
        result = await stale_authority_probe(
            deployment,
            baseline_authority=baseline_authority,
            authority_state_directory=arguments.authority_state_dir,
            batch_id=arguments.batch_id,
            idempotency_key=arguments.idempotency_key,
            content=_load_content(arguments),
            timeout=arguments.timeout,
        )
    elif arguments.command == "capture-authority":
        result = await authority_evidence(
            deployment,
            baseline_authority=baseline_authority,
            authority_state_directory=arguments.authority_state_dir,
            control_database=arguments.control_database,
            failover_database=arguments.failover_database,
            post_failover_batch_id=arguments.post_failover_batch_id,
            timeout=arguments.timeout,
        )
    else:
        result = verify_evidence(
            deployment,
            baseline_report=baseline_report,
            baseline_authority=baseline_authority,
            authority=_read_json(arguments.authority, field="authority"),
            promoted_primary=_read_json(
                arguments.promoted_primary, field="promoted_primary"
            ),
            former_primary=_read_json(
                arguments.former_primary, field="former_primary"
            ),
            post_failover_probe=_read_json(
                arguments.post_failover_probe, field="post_failover_probe"
            ),
            stale_authority_probe=_read_json(
                arguments.stale_authority_probe, field="stale_authority_probe"
            ),
        )
    _write_json(arguments.output, result, force=arguments.force)
    print(json.dumps(result, sort_keys=True))
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
                "code": "storage-f52-runtime-error",
                "field": "runtime",
                "message": str(exc) or type(exc).__name__,
            }
        print(json.dumps({"error": error}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
