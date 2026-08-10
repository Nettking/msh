"""Reproducible Phase F5.1 deployment and evidence tooling.

F5.1 deploys one relay/coordinator authority and two independent storage nodes.
It provisions normal primary/replica operation, performs one replicated write,
and produces public evidence that identities, roles, data, and control survive a
restart. Failure injection and automatic recovery remain F5.2 and F5.3.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import ipaddress
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.control_relay import FramedStorageControlRelayPublisher
from catalog.federation.control_sync import StorageControlPublicationStore
from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)

from .client import RelayNodeClient
from .identity import IdentityStore
from .live_storage_agent import LiveStorageNodeAgent
from .state import EnrollmentState
from .storage_agent import (
    DEFAULT_ENROLLMENT_TOKEN_ENV,
    STORAGE_NODE_CONFIG_SCHEMA,
    StorageNodeConfig,
)

DEPLOYMENT_SCHEMA = "fcp.storage_three_machine_deployment.v1"
BOOTSTRAP_SCHEMA = "fcp.storage_authority_bootstrap.v1"
EVIDENCE_SCHEMA = "fcp.storage_three_machine_evidence.v1"
REPORT_SCHEMA = "fcp.storage_three_machine_report.v1"
_EXPECTED_ACK_MODE = AcknowledgementMode.ONE_REPLICA
_FORBIDDEN_KEY_MARKERS = ("token", "password", "secret", "private_key")


class StorageDeploymentError(FederationValidationError):
    """Invalid or unsafe F5.1 deployment input."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise StorageDeploymentError(
            "invalid-deployment-timestamp", "captured_at", "must be timezone-aware"
        )
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise StorageDeploymentError(
            "invalid-deployment-field", field, "must be non-empty text"
        )
    return value


def _uint(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise StorageDeploymentError(
            "invalid-deployment-field", field, f"must be an integer >= {minimum}"
        )
    return value


def _read_json(path: Path | str, *, field: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise StorageDeploymentError(
            "deployment-file-unavailable", field, "could not read JSON file"
        ) from exc
    except json.JSONDecodeError as exc:
        raise StorageDeploymentError(
            "invalid-deployment-json", field, "must contain valid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise StorageDeploymentError(
            "invalid-deployment-json", field, "must contain a JSON object"
        )
    return value


def _write_json(path: Path | str, value: dict[str, Any], *, force: bool = False) -> None:
    output = Path(path)
    if output.exists() and not force:
        raise StorageDeploymentError(
            "deployment-output-exists",
            "output",
            "refusing to replace an existing file without --force",
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _contains_forbidden_key(value: Any) -> str | None:
    if isinstance(value, dict):
        for key, child in value.items():
            lowered = str(key).lower()
            if any(marker in lowered for marker in _FORBIDDEN_KEY_MARKERS):
                return str(key)
            found = _contains_forbidden_key(child)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _contains_forbidden_key(child)
            if found is not None:
                return found
    return None


def _relay_is_safe(relay_url: str) -> bool:
    parsed = urlparse(relay_url)
    if parsed.scheme == "wss" and parsed.hostname:
        return True
    if parsed.scheme != "ws" or not parsed.hostname:
        return False
    host = parsed.hostname.split("%", 1)[0]
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _allow_insecure_loopback(relay_url: str) -> bool:
    return urlparse(relay_url).scheme == "ws" and _relay_is_safe(relay_url)


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class DeploymentNode:
    node_id: str
    provider_id: str
    display_name: str

    @classmethod
    def from_dict(cls, value: Any, field: str) -> DeploymentNode:
        if not isinstance(value, dict):
            raise StorageDeploymentError(
                "invalid-deployment-node", field, "must be an object"
            )
        unknown = sorted(set(value).difference({"node_id", "provider_id", "display_name"}))
        if unknown:
            raise StorageDeploymentError(
                "invalid-deployment-node", f"{field}.{unknown[0]}", "unknown field"
            )
        return cls(
            _text(value.get("node_id"), f"{field}.node_id"),
            _text(value.get("provider_id"), f"{field}.provider_id"),
            _text(value.get("display_name"), f"{field}.display_name"),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "node_id": self.node_id,
            "provider_id": self.provider_id,
            "display_name": self.display_name,
        }


@dataclass(frozen=True)
class ThreeMachineDeployment:
    deployment_id: str
    relay_url: str
    session_id: str
    group_id: str
    authority_node_id: str
    authority_display_name: str
    primary: DeploymentNode
    replica: DeploymentNode

    @classmethod
    def from_dict(cls, value: Any) -> ThreeMachineDeployment:
        if not isinstance(value, dict):
            raise StorageDeploymentError(
                "invalid-deployment", "deployment", "must be an object"
            )
        forbidden = _contains_forbidden_key(value)
        if forbidden is not None:
            raise StorageDeploymentError(
                "credential-in-deployment", forbidden, "credentials must not be persisted"
            )
        allowed = {
            "schema",
            "deployment_id",
            "relay_url",
            "session_id",
            "group_id",
            "authority_node_id",
            "authority_display_name",
            "primary",
            "replica",
            "acknowledgement_mode",
            "protocol",
            "protocol_version",
        }
        unknown = sorted(set(value).difference(allowed))
        if unknown:
            raise StorageDeploymentError(
                "invalid-deployment", unknown[0], "unknown field"
            )
        if value.get("schema") != DEPLOYMENT_SCHEMA:
            raise StorageDeploymentError(
                "unsupported-deployment", "schema", f"expected {DEPLOYMENT_SCHEMA}"
            )
        if value.get("acknowledgement_mode") != _EXPECTED_ACK_MODE.value:
            raise StorageDeploymentError(
                "unsafe-deployment-policy",
                "acknowledgement_mode",
                "F5.1 requires one-replica acknowledgement",
            )
        if (
            value.get("protocol") != STORAGE_PROTOCOL
            or value.get("protocol_version") != STORAGE_PROTOCOL_VERSION
        ):
            raise StorageDeploymentError(
                "unsupported-deployment-protocol",
                "protocol",
                "deployment must use the current storage protocol",
            )
        result = cls(
            _text(value.get("deployment_id"), "deployment_id"),
            _text(value.get("relay_url"), "relay_url"),
            _text(value.get("session_id"), "session_id"),
            _text(value.get("group_id"), "group_id"),
            _text(value.get("authority_node_id"), "authority_node_id"),
            _text(value.get("authority_display_name"), "authority_display_name"),
            DeploymentNode.from_dict(value.get("primary"), "primary"),
            DeploymentNode.from_dict(value.get("replica"), "replica"),
        )
        result.validate()
        return result

    @classmethod
    def load(cls, path: Path | str) -> ThreeMachineDeployment:
        return cls.from_dict(_read_json(path, field="deployment"))

    def validate(self) -> None:
        if not _relay_is_safe(self.relay_url):
            raise StorageDeploymentError(
                "unsafe-deployment-relay",
                "relay_url",
                "physical deployment requires wss://; ws:// is loopback-only",
            )
        node_ids = {
            self.authority_node_id,
            self.primary.node_id,
            self.replica.node_id,
        }
        if len(node_ids) != 3:
            raise StorageDeploymentError(
                "duplicate-deployment-node",
                "node_id",
                "authority, primary, and replica require distinct identities",
            )
        if self.primary.provider_id == self.replica.provider_id:
            raise StorageDeploymentError(
                "duplicate-deployment-provider",
                "provider_id",
                "primary and replica require distinct provider IDs",
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": DEPLOYMENT_SCHEMA,
            "deployment_id": self.deployment_id,
            "relay_url": self.relay_url,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "authority_node_id": self.authority_node_id,
            "authority_display_name": self.authority_display_name,
            "primary": self.primary.to_dict(),
            "replica": self.replica.to_dict(),
            "acknowledgement_mode": _EXPECTED_ACK_MODE.value,
            "protocol": STORAGE_PROTOCOL,
            "protocol_version": STORAGE_PROTOCOL_VERSION,
        }


def render_storage_config(
    *,
    output: Path | str,
    relay_url: str,
    session_id: str,
    display_name: str,
    provider_id: str,
    allow_insecure_local: bool,
    force: bool = False,
) -> StorageNodeConfig:
    relay_url = _text(relay_url, "relay_url")
    parsed = urlparse(relay_url)
    if allow_insecure_local:
        if not _relay_is_safe(relay_url) or parsed.scheme != "ws":
            raise StorageDeploymentError(
                "unsafe-insecure-relay",
                "allow_insecure_local",
                "insecure mode is allowed only for a loopback ws:// relay",
            )
    elif not _relay_is_safe(relay_url) or parsed.scheme != "wss":
        raise StorageDeploymentError(
            "unsafe-deployment-relay",
            "relay_url",
            "physical storage nodes require wss://",
        )
    value = {
        "schema": STORAGE_NODE_CONFIG_SCHEMA,
        "state_directory": "state",
        "relay_url": relay_url,
        "display_name": _text(display_name, "display_name"),
        "provider_id": _text(provider_id, "provider_id"),
        "session_id": _text(session_id, "session_id"),
        "control_database": "state/control.sqlite3",
        "storage_directory": "state/storage",
        "outbox_database": "state/outbox.sqlite3",
        "acknowledgements_database": "state/acks.sqlite3",
        "allow_insecure_local": allow_insecure_local,
        "heartbeat_interval": 10,
        "request_timeout": 15,
    }
    _write_json(output, value, force=force)
    return StorageNodeConfig.load(output)


async def bootstrap_authority(
    *,
    relay_url: str,
    state_directory: Path | str,
    display_name: str,
    session_name: str,
    public_output: Path | str,
    invitation_ttl_seconds: int,
    allow_insecure_local: bool,
    force: bool = False,
) -> dict[str, Any]:
    invitation_ttl_seconds = _uint(
        invitation_ttl_seconds, "invitation_ttl_seconds", minimum=60
    )
    public_path = Path(public_output)
    client = RelayNodeClient(
        state_directory=state_directory,
        relay_url=relay_url,
        display_name=display_name,
        allow_insecure_local=allow_insecure_local,
    )
    enrollment_state = client.state.status()["enrollment_state"]
    token = (
        os.environ.get(DEFAULT_ENROLLMENT_TOKEN_ENV)
        if enrollment_state == EnrollmentState.UNENROLLED.value
        else None
    )
    if enrollment_state == EnrollmentState.UNENROLLED.value and not token:
        raise StorageDeploymentError(
            "authority-enrollment-required",
            DEFAULT_ENROLLMENT_TOKEN_ENV,
            "first authority startup requires a one-time enrollment token",
        )
    await client.connect(enrollment_token=token)
    try:
        if public_path.exists():
            public = _read_json(public_path, field="public_output")
            if (
                public.get("schema") != BOOTSTRAP_SCHEMA
                or public.get("authority_node_id") != client.node_id
                or public.get("relay_url") != relay_url
            ):
                raise StorageDeploymentError(
                    "authority-bootstrap-conflict",
                    "public_output",
                    "existing public bootstrap belongs to another authority or relay",
                )
            session_id = _text(public.get("session_id"), "session_id")
            if session_id not in {
                item.session_id for item in client.state.joined_sessions()
            }:
                raise StorageDeploymentError(
                    "authority-session-missing",
                    "session_id",
                    "authority no longer has durable membership in the session",
                )
        else:
            session = await client.create_session(_text(session_name, "session_name"))
            session_id = str(session["session_id"])
            public = {
                "schema": BOOTSTRAP_SCHEMA,
                "authority_node_id": client.node_id,
                "authority_display_name": display_name,
                "relay_url": relay_url,
                "session_id": session_id,
                "session_name": session_name,
            }
            _write_json(public_path, public, force=force)
        primary = await client.create_invitation(
            session_id, ttl_seconds=invitation_ttl_seconds, max_uses=1
        )
        replica = await client.create_invitation(
            session_id, ttl_seconds=invitation_ttl_seconds, max_uses=1
        )
        return {
            **public,
            "primary_session_invitation": primary["token"],
            "replica_session_invitation": replica["token"],
        }
    finally:
        await client.disconnect()


def render_topology(
    *,
    bootstrap_path: Path | str,
    output: Path | str,
    group_id: str,
    primary_node_id: str,
    primary_provider_id: str,
    primary_display_name: str,
    replica_node_id: str,
    replica_provider_id: str,
    replica_display_name: str,
    force: bool = False,
) -> ThreeMachineDeployment:
    bootstrap = _read_json(bootstrap_path, field="bootstrap")
    if bootstrap.get("schema") != BOOTSTRAP_SCHEMA:
        raise StorageDeploymentError(
            "unsupported-authority-bootstrap", "schema", f"expected {BOOTSTRAP_SCHEMA}"
        )
    public_without_id = {
        "relay_url": bootstrap.get("relay_url"),
        "session_id": bootstrap.get("session_id"),
        "group_id": group_id,
        "authority_node_id": bootstrap.get("authority_node_id"),
        "primary_node_id": primary_node_id,
        "primary_provider_id": primary_provider_id,
        "replica_node_id": replica_node_id,
        "replica_provider_id": replica_provider_id,
    }
    deployment = ThreeMachineDeployment(
        deployment_id="f51-" + _canonical_hash(public_without_id)[7:23],
        relay_url=_text(bootstrap.get("relay_url"), "relay_url"),
        session_id=_text(bootstrap.get("session_id"), "session_id"),
        group_id=_text(group_id, "group_id"),
        authority_node_id=_text(
            bootstrap.get("authority_node_id"), "authority_node_id"
        ),
        authority_display_name=_text(
            bootstrap.get("authority_display_name"), "authority_display_name"
        ),
        primary=DeploymentNode(
            _text(primary_node_id, "primary_node_id"),
            _text(primary_provider_id, "primary_provider_id"),
            _text(primary_display_name, "primary_display_name"),
        ),
        replica=DeploymentNode(
            _text(replica_node_id, "replica_node_id"),
            _text(replica_provider_id, "replica_provider_id"),
            _text(replica_display_name, "replica_display_name"),
        ),
    )
    deployment.validate()
    _write_json(output, deployment.to_dict(), force=force)
    return deployment


def _registration(
    deployment: ThreeMachineDeployment, node: DeploymentNode
) -> StorageProviderRegistration:
    return StorageProviderRegistration(
        session_id=deployment.session_id,
        provider_id=node.provider_id,
        node_id=node.node_id,
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        authorized=True,
        status="ready",
    )


def ensure_initial_control(
    deployment: ThreeMachineDeployment,
    *,
    authority_state_directory: Path | str,
    control_database: Path | str,
    lease_seconds: int,
    now: datetime | None = None,
) -> PhaseDControlPlane:
    lease_seconds = _uint(lease_seconds, "lease_seconds", minimum=300)
    timestamp = (now or _now()).astimezone(timezone.utc)
    credentials = IdentityStore(
        authority_state_directory,
        display_name=deployment.authority_display_name,
    ).load()
    if credentials.identity.node_id != deployment.authority_node_id:
        raise StorageDeploymentError(
            "deployment-authority-mismatch",
            "authority_node_id",
            "local authority identity differs from the topology",
        )
    actor = deployment.authority_node_id
    control = PhaseDControlPlane(control_database)
    snapshot = control.snapshot(deployment.session_id)
    if deployment.group_id not in snapshot.groups:
        control.create_group(deployment.session_id, actor, deployment.group_id)
        snapshot = control.snapshot(deployment.session_id)
    for node in (deployment.primary, deployment.replica):
        existing = snapshot.providers.get(node.provider_id)
        expected = _registration(deployment, node)
        if existing is None:
            control.register_provider(deployment.session_id, actor, expected)
            snapshot = control.snapshot(deployment.session_id)
        elif existing != expected:
            raise StorageDeploymentError(
                "deployment-provider-conflict",
                "provider_id",
                f"provider {node.provider_id} differs from the topology",
            )
    assignment = control.snapshot(deployment.session_id).groups[deployment.group_id]
    expected_replicas = (deployment.replica.provider_id,)
    new_assignment = (
        assignment.primary_provider_id is None
        and not assignment.replica_provider_ids
    )
    if new_assignment:
        control.change_assignment(
            deployment.session_id,
            actor,
            deployment.group_id,
            deployment.primary.provider_id,
            expected_replicas,
        )
    elif (
        assignment.primary_provider_id != deployment.primary.provider_id
        or tuple(assignment.replica_provider_ids) != expected_replicas
    ):
        raise StorageDeploymentError(
            "deployment-assignment-conflict",
            "group_id",
            "existing assignment differs from the F5.1 topology",
        )
    policy = control.acknowledgement_policy(
        deployment.session_id, deployment.group_id
    )
    if new_assignment:
        if policy.mode is not AcknowledgementMode.PRIMARY:
            raise StorageDeploymentError(
                "deployment-policy-conflict",
                "acknowledgement_mode",
                "new groups must begin with the default primary-only policy",
            )
        control.set_acknowledgement_policy(
            deployment.session_id, deployment.group_id, _EXPECTED_ACK_MODE
        )
    elif policy.mode is not _EXPECTED_ACK_MODE:
        raise StorageDeploymentError(
            "deployment-policy-conflict",
            "acknowledgement_mode",
            "existing deployment is not in normal one-replica mode",
        )
    if control.storage_degraded_state(
        deployment.session_id, deployment.group_id
    ) is not None:
        raise StorageDeploymentError(
            "deployment-is-degraded",
            "storage_degraded_state",
            "F5.1 provisioning cannot overwrite a degraded deployment",
        )
    snapshot = control.snapshot(deployment.session_id)
    grant = snapshot.leader_grants.get(deployment.group_id)
    if grant is None:
        grant_id = "f51-grant-" + hashlib.sha256(
            f"{deployment.deployment_id}\0{deployment.group_id}".encode()
        ).hexdigest()[:24]
        control.grant_leader(
            deployment.session_id,
            actor,
            deployment.group_id,
            deployment.primary.provider_id,
            grant_id,
            1,
            1,
            lease_expires_at=timestamp + timedelta(seconds=lease_seconds),
            occurred_at=timestamp,
        )
    else:
        try:
            lease_expires_at = datetime.fromisoformat(
                str(grant.get("lease_expires_at")).replace("Z", "+00:00")
            ).astimezone(timezone.utc)
        except (TypeError, ValueError) as exc:
            raise StorageDeploymentError(
                "deployment-grant-conflict",
                "lease_expires_at",
                "existing leader grant has an invalid lease",
            ) from exc
        if (
            grant.get("provider_id") != deployment.primary.provider_id
            or lease_expires_at <= timestamp
        ):
            raise StorageDeploymentError(
                "deployment-grant-conflict",
                "group_id",
                "existing leader grant is expired or belongs to another provider",
            )
    return control


async def provision_and_publish(
    deployment: ThreeMachineDeployment,
    *,
    authority_state_directory: Path | str,
    control_database: Path | str,
    publication_database: Path | str,
    lease_seconds: int,
    timeout: float,
) -> dict[str, Any]:
    control = ensure_initial_control(
        deployment,
        authority_state_directory=authority_state_directory,
        control_database=control_database,
        lease_seconds=lease_seconds,
    )
    credentials = IdentityStore(
        authority_state_directory,
        display_name=deployment.authority_display_name,
    ).load()
    plan = StorageControlPublicationStore(publication_database).issue(
        control, credentials, deployment.session_id
    )
    client = RelayNodeClient(
        state_directory=authority_state_directory,
        relay_url=deployment.relay_url,
        display_name=deployment.authority_display_name,
        allow_insecure_local=_allow_insecure_loopback(deployment.relay_url),
        request_timeout=timeout,
    )
    await client.connect()
    try:
        if deployment.session_id not in {
            item.session_id for item in client.state.joined_sessions()
        }:
            raise StorageDeploymentError(
                "authority-session-missing",
                "session_id",
                "authority is not a durable member of the deployment session",
            )
        result = await FramedStorageControlRelayPublisher(
            client, timeout=timeout
        ).publish(
            plan,
            (deployment.primary.node_id, deployment.replica.node_id),
        )
        return {
            "schema": "fcp.storage_three_machine_provisioning.v1",
            "deployment_id": deployment.deployment_id,
            "publication_id": result.publication_id,
            "publication_revision": result.publication_revision,
            "content_hash": plan.content_hash,
            "acknowledged_node_ids": list(result.acknowledged_node_ids),
        }
    finally:
        await client.disconnect()


async def probe_write(
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
    control = PhaseDControlPlane(control_database)
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
    acknowledgements = DurableAcknowledgementStore(acknowledgements_database)
    try:
        outcome = await PhaseDLogicalStorageClient(
            session_id=deployment.session_id,
            actor_node_id=deployment.authority_node_id,
            control_plane=control,
            transport=endpoint,
            acknowledgements=acknowledgements,
        ).ingest_batch(
            group_id=deployment.group_id,
            dataset_id=_text(dataset_id, "dataset_id"),
            batch_id=_text(batch_id, "batch_id"),
            idempotency_key=_text(idempotency_key, "idempotency_key"),
            content=content,
            created_at=_now(),
        )
        if not outcome.committed or outcome.result is None:
            raise StorageDeploymentError(
                outcome.error_code or "deployment-probe-not-committed",
                "batch_id",
                outcome.message or "probe write was not committed",
            )
        status = acknowledgements.status(
            deployment.session_id, deployment.group_id, batch_id
        )
        if status is None or not status.committed:
            raise StorageDeploymentError(
                "deployment-probe-acknowledgement-missing",
                "batch_id",
                "coordinator acknowledgement journal did not commit the probe",
            )
        if (
            status.required_replica_acks != 1
            or status.acknowledged_replica_ids
            != (deployment.replica.provider_id,)
        ):
            raise StorageDeploymentError(
                "deployment-probe-acknowledgement-mismatch",
                "acknowledged_replica_ids",
                "probe must be acknowledged by exactly the configured replica",
            )
        manifest = control.manifest(deployment.session_id, deployment.group_id)
        item = next((value for value in manifest.items if value.item_id == batch_id), None)
        if item is None or item.content_hash != outcome.result.content_hash:
            raise StorageDeploymentError(
                "deployment-probe-manifest-missing",
                "batch_id",
                "authoritative manifest does not contain the probe batch",
            )
        return {
            "schema": "fcp.storage_three_machine_probe.v1",
            "deployment_id": deployment.deployment_id,
            "batch_id": batch_id,
            "content_hash": outcome.result.content_hash,
            "required_replica_acks": status.required_replica_acks,
            "acknowledged_replica_ids": list(status.acknowledged_replica_ids),
            "manifest_revision": manifest.revision,
            "manifest_hash": manifest.manifest_hash,
        }
    finally:
        await endpoint.close()
        await client.disconnect()


def storage_evidence(
    deployment: ThreeMachineDeployment,
    *,
    role: str,
    config_path: Path | str,
    stage: str,
    batch_id: str,
    captured_at: datetime | None = None,
) -> dict[str, Any]:
    if role not in {"primary", "replica"}:
        raise StorageDeploymentError(
            "invalid-deployment-role", "role", "must be primary or replica"
        )
    if stage not in {"before-restart", "after-restart"}:
        raise StorageDeploymentError(
            "invalid-evidence-stage",
            "stage",
            "must be before-restart or after-restart",
        )
    expected = deployment.primary if role == "primary" else deployment.replica
    config = StorageNodeConfig.load(config_path)
    if (
        config.relay_url != deployment.relay_url
        or config.session_id != deployment.session_id
        or config.provider_id != expected.provider_id
        or config.display_name != expected.display_name
    ):
        raise StorageDeploymentError(
            "deployment-config-mismatch",
            "config",
            "storage configuration differs from the public topology",
        )
    agent = LiveStorageNodeAgent(
        config, control_authority_node_id=deployment.authority_node_id
    )
    if agent.node_id != expected.node_id:
        raise StorageDeploymentError(
            "deployment-node-mismatch",
            "node_id",
            "local persistent identity differs from the public topology",
        )
    latest = agent.replica_store.latest(deployment.session_id)
    if latest is None:
        raise StorageDeploymentError(
            "deployment-control-missing",
            "control_sync",
            "storage node has no durable signed control publication",
        )
    latest.verify(expected_authority_node_id=deployment.authority_node_id)
    agent.storage.validate_control_state()
    status = agent.status()
    status["control_sync"]["ready"] = True
    status["control_sync"]["ready_source"] = "durable-verified"
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
        "captured_at": _stamp(captured_at or _now()),
        "node_id": agent.node_id,
        "provider_id": expected.provider_id,
        "session_id": deployment.session_id,
        "group_id": deployment.group_id,
        "status": status,
        "batch": {
            "batch_id": batch_id,
            "present": identity is not None,
            "content_hash": None if identity is None else identity.content_hash,
            "idempotency_key": None if identity is None else identity.idempotency_key,
        },
    }


async def authority_evidence(
    deployment: ThreeMachineDeployment,
    *,
    authority_state_directory: Path | str,
    control_database: Path | str,
    captured_at: datetime | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    credentials = IdentityStore(
        authority_state_directory,
        display_name=deployment.authority_display_name,
    ).load()
    if credentials.identity.node_id != deployment.authority_node_id:
        raise StorageDeploymentError(
            "deployment-authority-mismatch",
            "authority_node_id",
            "local authority identity differs from the public topology",
        )
    control = PhaseDControlPlane(control_database)
    snapshot = control.snapshot(deployment.session_id)
    assignment = snapshot.groups.get(deployment.group_id)
    if assignment is None:
        raise StorageDeploymentError(
            "deployment-group-missing", "group_id", "storage group is not provisioned"
        )
    manifest = control.manifest(deployment.session_id, deployment.group_id)
    client = RelayNodeClient(
        state_directory=authority_state_directory,
        relay_url=deployment.relay_url,
        display_name=deployment.authority_display_name,
        allow_insecure_local=_allow_insecure_loopback(deployment.relay_url),
        request_timeout=timeout,
    )
    await client.connect()
    try:
        coordinator_status = await client.coordinator_status()
    finally:
        await client.disconnect()
    relevant_nodes = {
        deployment.authority_node_id,
        deployment.primary.node_id,
        deployment.replica.node_id,
    }
    nodes = [
        value
        for value in coordinator_status.get("nodes", [])
        if isinstance(value, dict) and value.get("node_id") in relevant_nodes
    ]
    capabilities = [
        value
        for value in coordinator_status.get("capabilities", [])
        if isinstance(value, dict) and value.get("node_id") in relevant_nodes
    ]
    return {
        "schema": EVIDENCE_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "role": "authority",
        "stage": "final",
        "captured_at": _stamp(captured_at or _now()),
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
            "grant": snapshot.leader_grants.get(deployment.group_id),
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
        "relay": {"nodes": nodes, "capabilities": capabilities},
    }


def verify_evidence(
    deployment: ThreeMachineDeployment,
    *,
    authority: dict[str, Any],
    primary_before: dict[str, Any],
    replica_before: dict[str, Any],
    primary_after: dict[str, Any],
    replica_after: dict[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    evidence = (
        authority,
        primary_before,
        replica_before,
        primary_after,
        replica_after,
    )
    check(
        "schemas",
        all(item.get("schema") == EVIDENCE_SCHEMA for item in evidence),
        "all inputs use the F5.1 evidence schema",
    )
    check(
        "deployment-binding",
        all(item.get("deployment_id") == deployment.deployment_id for item in evidence),
        "all evidence belongs to the same deployment",
    )
    check(
        "evidence-roles",
        authority.get("role") == "authority"
        and primary_before.get("role") == primary_after.get("role") == "primary"
        and replica_before.get("role") == replica_after.get("role") == "replica",
        "authority, primary, and replica evidence roles are explicit",
    )
    control = authority.get("control") if isinstance(authority.get("control"), dict) else {}
    check(
        "assignment",
        control.get("primary_provider_id") == deployment.primary.provider_id
        and control.get("replica_provider_ids") == [deployment.replica.provider_id],
        "authority retains the configured primary and replica",
    )
    check(
        "acknowledgement-policy",
        control.get("acknowledgement_mode") == _EXPECTED_ACK_MODE.value,
        "normal writes require one replica acknowledgement",
    )
    grant = control.get("grant") if isinstance(control.get("grant"), dict) else {}
    check(
        "leader-grant",
        grant.get("provider_id") == deployment.primary.provider_id
        and isinstance(grant.get("term"), int)
        and isinstance(grant.get("fencing_token"), int),
        "leader grant remains bound to the configured primary",
    )
    check(
        "not-degraded",
        control.get("degraded_state") is None,
        "F5.1 ends in normal redundant operation",
    )
    relay = authority.get("relay") if isinstance(authority.get("relay"), dict) else {}
    relay_nodes = {
        item.get("node_id"): item
        for item in relay.get("nodes", [])
        if isinstance(item, dict)
    }
    relay_capabilities = [
        item
        for item in relay.get("capabilities", [])
        if isinstance(item, dict)
    ]
    storage_nodes = (deployment.primary, deployment.replica)
    check(
        "relay-connections",
        all(
            isinstance(relay_nodes.get(node.node_id), dict)
            and relay_nodes[node.node_id].get("connection_state") == "connected"
            and relay_nodes[node.node_id].get("revoked") is not True
            for node in storage_nodes
        ),
        "primary and replica have active non-revoked relay connections",
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
                for capability in relay_capabilities
            )
            for node in storage_nodes
        ),
        "primary and replica advertise ready storage capabilities",
    )

    def validate_storage(
        before: dict[str, Any],
        after: dict[str, Any],
        expected: DeploymentNode,
        role: str,
    ) -> None:
        statuses = []
        for item in (before, after):
            status = item.get("status") if isinstance(item.get("status"), dict) else {}
            provider = (
                status.get("provider")
                if isinstance(status.get("provider"), dict)
                else {}
            )
            control_sync = (
                status.get("control_sync")
                if isinstance(status.get("control_sync"), dict)
                else {}
            )
            statuses.append((provider, control_sync))
        check(
            f"{role}-identity-stable",
            before.get("node_id") == after.get("node_id") == expected.node_id
            and before.get("provider_id") == after.get("provider_id") == expected.provider_id,
            f"{role} identity and provider survive restart",
        )
        check(
            f"{role}-role-stable",
            all(
                provider.get("groups")
                == [{"group_id": deployment.group_id, "role": role}]
                for provider, _sync in statuses
            ),
            f"{role} assignment survives restart",
        )
        check(
            f"{role}-control-ready",
            all(sync.get("ready") is True for _provider, sync in statuses),
            f"{role} has verified signed control before and after restart",
        )
        check(
            f"{role}-provider-ready",
            all(
                provider.get("registered") is True
                and provider.get("assignable") is True
                for provider, _sync in statuses
            ),
            f"{role} provider remains registered and assignable",
        )
        check(
            f"{role}-no-listener",
            all(provider.get("inbound_listener_ports") == [] for provider, _sync in statuses),
            f"{role} opens no inbound application listener",
        )
        before_batch = before.get("batch") if isinstance(before.get("batch"), dict) else {}
        after_batch = after.get("batch") if isinstance(after.get("batch"), dict) else {}
        check(
            f"{role}-batch-stable",
            before_batch.get("present") is True
            and after_batch.get("present") is True
            and before_batch.get("batch_id") == after_batch.get("batch_id")
            and before_batch.get("content_hash") == after_batch.get("content_hash"),
            f"{role} retains the probe batch and immutable hash through restart",
        )

    validate_storage(primary_before, primary_after, deployment.primary, "primary")
    validate_storage(replica_before, replica_after, deployment.replica, "replica")
    primary_batch = primary_after.get("batch", {})
    replica_batch = replica_after.get("batch", {})
    manifest = control.get("manifest") if isinstance(control.get("manifest"), dict) else {}
    manifest_items = manifest.get("items") if isinstance(manifest.get("items"), list) else []
    check(
        "replicated-batch",
        primary_batch.get("content_hash") is not None
        and primary_batch.get("content_hash") == replica_batch.get("content_hash")
        and any(
            isinstance(item, dict)
            and item.get("item_id") == primary_batch.get("batch_id")
            and item.get("content_hash") == primary_batch.get("content_hash")
            for item in manifest_items
        ),
        "primary, replica, and authoritative manifest agree on the probe batch",
    )
    report = {
        "schema": REPORT_SCHEMA,
        "deployment_id": deployment.deployment_id,
        "passed": all(item["passed"] for item in checks),
        "checks": checks,
    }
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    storage = commands.add_parser("render-storage-config")
    storage.add_argument("--output", required=True)
    storage.add_argument("--relay", required=True)
    storage.add_argument("--session-id", required=True)
    storage.add_argument("--display-name", required=True)
    storage.add_argument("--provider-id", required=True)
    storage.add_argument("--allow-insecure-local", action="store_true")
    storage.add_argument("--force", action="store_true")

    bootstrap = commands.add_parser("bootstrap-authority")
    bootstrap.add_argument("--relay", required=True)
    bootstrap.add_argument("--state-dir", required=True)
    bootstrap.add_argument("--display-name", required=True)
    bootstrap.add_argument("--session-name", required=True)
    bootstrap.add_argument("--public-output", required=True)
    bootstrap.add_argument("--invitation-ttl-seconds", type=int, default=900)
    bootstrap.add_argument("--allow-insecure-local", action="store_true")
    bootstrap.add_argument("--force", action="store_true")

    topology = commands.add_parser("render-topology")
    topology.add_argument("--bootstrap", required=True)
    topology.add_argument("--output", required=True)
    topology.add_argument("--group-id", default="storage-main")
    topology.add_argument("--primary-node-id", required=True)
    topology.add_argument("--primary-provider-id", default="provider-primary")
    topology.add_argument("--primary-display-name", required=True)
    topology.add_argument("--replica-node-id", required=True)
    topology.add_argument("--replica-provider-id", default="provider-replica")
    topology.add_argument("--replica-display-name", required=True)
    topology.add_argument("--force", action="store_true")

    validate = commands.add_parser("validate")
    validate.add_argument("--deployment", required=True)

    provision = commands.add_parser("provision")
    provision.add_argument("--deployment", required=True)
    provision.add_argument("--authority-state-dir", required=True)
    provision.add_argument("--control-database", required=True)
    provision.add_argument("--publication-database", required=True)
    provision.add_argument("--lease-seconds", type=int, default=86400)
    provision.add_argument("--timeout", type=float, default=15.0)

    probe = commands.add_parser("probe-write")
    probe.add_argument("--deployment", required=True)
    probe.add_argument("--authority-state-dir", required=True)
    probe.add_argument("--control-database", required=True)
    probe.add_argument("--acknowledgements-database", required=True)
    probe.add_argument("--dataset-id", default="f51-deployment")
    probe.add_argument("--batch-id", required=True)
    probe.add_argument("--idempotency-key", required=True)
    source = probe.add_mutually_exclusive_group(required=True)
    source.add_argument("--content-json")
    source.add_argument("--content-file")
    probe.add_argument("--timeout", type=float, default=15.0)

    storage_capture = commands.add_parser("capture-storage")
    storage_capture.add_argument("--deployment", required=True)
    storage_capture.add_argument("--role", choices=("primary", "replica"), required=True)
    storage_capture.add_argument("--config", required=True)
    storage_capture.add_argument(
        "--stage", choices=("before-restart", "after-restart"), required=True
    )
    storage_capture.add_argument("--batch-id", required=True)
    storage_capture.add_argument("--output", required=True)
    storage_capture.add_argument("--force", action="store_true")

    authority_capture = commands.add_parser("capture-authority")
    authority_capture.add_argument("--deployment", required=True)
    authority_capture.add_argument("--authority-state-dir", required=True)
    authority_capture.add_argument("--control-database", required=True)
    authority_capture.add_argument("--output", required=True)
    authority_capture.add_argument("--timeout", type=float, default=15.0)
    authority_capture.add_argument("--force", action="store_true")

    verify = commands.add_parser("verify")
    verify.add_argument("--deployment", required=True)
    verify.add_argument("--authority", required=True)
    verify.add_argument("--primary-before", required=True)
    verify.add_argument("--replica-before", required=True)
    verify.add_argument("--primary-after", required=True)
    verify.add_argument("--replica-after", required=True)
    verify.add_argument("--output", required=True)
    verify.add_argument("--force", action="store_true")
    return parser


def _load_content(arguments: argparse.Namespace) -> Any:
    if arguments.content_file:
        value = _read_json(arguments.content_file, field="content_file")
    else:
        try:
            value = json.loads(arguments.content_json)
        except json.JSONDecodeError as exc:
            raise StorageDeploymentError(
                "invalid-probe-content", "content_json", "must contain valid JSON"
            ) from exc
    return value


async def _run(arguments: argparse.Namespace) -> int:
    if arguments.command == "render-storage-config":
        config = render_storage_config(
            output=arguments.output,
            relay_url=arguments.relay,
            session_id=arguments.session_id,
            display_name=arguments.display_name,
            provider_id=arguments.provider_id,
            allow_insecure_local=arguments.allow_insecure_local,
            force=arguments.force,
        )
        print(json.dumps(config.public_summary(), sort_keys=True))
        return 0
    if arguments.command == "bootstrap-authority":
        result = await bootstrap_authority(
            relay_url=arguments.relay,
            state_directory=arguments.state_dir,
            display_name=arguments.display_name,
            session_name=arguments.session_name,
            public_output=arguments.public_output,
            invitation_ttl_seconds=arguments.invitation_ttl_seconds,
            allow_insecure_local=arguments.allow_insecure_local,
            force=arguments.force,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    if arguments.command == "render-topology":
        deployment = render_topology(
            bootstrap_path=arguments.bootstrap,
            output=arguments.output,
            group_id=arguments.group_id,
            primary_node_id=arguments.primary_node_id,
            primary_provider_id=arguments.primary_provider_id,
            primary_display_name=arguments.primary_display_name,
            replica_node_id=arguments.replica_node_id,
            replica_provider_id=arguments.replica_provider_id,
            replica_display_name=arguments.replica_display_name,
            force=arguments.force,
        )
        print(json.dumps(deployment.to_dict(), sort_keys=True))
        return 0
    deployment = ThreeMachineDeployment.load(arguments.deployment)
    if arguments.command == "validate":
        print(json.dumps(deployment.to_dict(), sort_keys=True))
        return 0
    if arguments.command == "provision":
        result = await provision_and_publish(
            deployment,
            authority_state_directory=arguments.authority_state_dir,
            control_database=arguments.control_database,
            publication_database=arguments.publication_database,
            lease_seconds=arguments.lease_seconds,
            timeout=arguments.timeout,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    if arguments.command == "probe-write":
        result = await probe_write(
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
        print(json.dumps(result, sort_keys=True))
        return 0
    if arguments.command == "capture-storage":
        result = storage_evidence(
            deployment,
            role=arguments.role,
            config_path=arguments.config,
            stage=arguments.stage,
            batch_id=arguments.batch_id,
        )
        _write_json(arguments.output, result, force=arguments.force)
        print(json.dumps(result, sort_keys=True))
        return 0
    if arguments.command == "capture-authority":
        result = await authority_evidence(
            deployment,
            authority_state_directory=arguments.authority_state_dir,
            control_database=arguments.control_database,
            timeout=arguments.timeout,
        )
        _write_json(arguments.output, result, force=arguments.force)
        print(json.dumps(result, sort_keys=True))
        return 0
    result = verify_evidence(
        deployment,
        authority=_read_json(arguments.authority, field="authority"),
        primary_before=_read_json(arguments.primary_before, field="primary_before"),
        replica_before=_read_json(arguments.replica_before, field="replica_before"),
        primary_after=_read_json(arguments.primary_after, field="primary_after"),
        replica_after=_read_json(arguments.replica_after, field="replica_after"),
    )
    _write_json(arguments.output, result, force=arguments.force)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["passed"] else 2


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
                "code": "storage-deployment-runtime-error",
                "field": "runtime",
                "message": str(exc) or type(exc).__name__,
            }
        print(json.dumps({"error": error}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
