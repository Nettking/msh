"""Signed full-snapshot control synchronization for live storage nodes.

A coordinator publishes a complete desired Phase D control snapshot.  Storage
nodes authenticate the relay sender, verify the Ed25519 signature, and reconcile
their local durable control plane idempotently.  Full snapshots make skipped
publications safe while stale or conflicting revisions fail closed.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from catalog.node.identity import (
    NodeCredentials,
    derive_node_id_from_public_key,
    verify_signature,
)

from .acknowledgement import AcknowledgementMode
from .errors import FederationValidationError
from .phase_d_control import PhaseDControlPlane
from .storage_control_plane import StorageProviderRegistration

STORAGE_CONTROL_PLAN_SCHEMA = "msh.storage_control_plan.v1"
STORAGE_CONTROL_RELAY_KIND = "msh-storage-control-v1"
STORAGE_CONTROL_RESULT_SCHEMA = "msh.storage_control_apply_result.v1"


def _utc(value: datetime | str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp", "issued_at", "must be RFC 3339"
            ) from exc
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-timestamp", "issued_at", "must be timezone-aware"
        )
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-storage-control-plan", field, "must be non-empty text"
        )
    return value


def _uint(value: Any, field: str, *, positive: bool = False) -> int:
    minimum = 1 if positive else 0
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise FederationValidationError(
            "invalid-storage-control-plan",
            field,
            f"must be an integer greater than or equal to {minimum}",
        )
    return value


def _canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _hash(value: dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


@dataclass(frozen=True)
class StorageControlPlan:
    schema: str
    publication_id: str
    publication_revision: int
    source_control_revision: int
    session_id: str
    authority_node_id: str
    authority_public_key: str
    issued_at: datetime
    providers: tuple[dict[str, Any], ...]
    groups: tuple[dict[str, Any], ...]
    content_hash: str
    signature: str

    def __post_init__(self) -> None:
        if self.schema != STORAGE_CONTROL_PLAN_SCHEMA:
            raise FederationValidationError(
                "unsupported-storage-control-plan",
                "schema",
                f"expected {STORAGE_CONTROL_PLAN_SCHEMA}",
            )
        for field in (
            "publication_id",
            "session_id",
            "authority_node_id",
            "authority_public_key",
            "content_hash",
            "signature",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(
            self,
            "publication_revision",
            _uint(self.publication_revision, "publication_revision", positive=True),
        )
        object.__setattr__(
            self,
            "source_control_revision",
            _uint(self.source_control_revision, "source_control_revision"),
        )
        object.__setattr__(self, "issued_at", _utc(self.issued_at))
        if not isinstance(self.providers, tuple) or not all(
            isinstance(item, dict) for item in self.providers
        ):
            raise FederationValidationError(
                "invalid-storage-control-plan", "providers", "must be an array of objects"
            )
        if not isinstance(self.groups, tuple) or not all(
            isinstance(item, dict) for item in self.groups
        ):
            raise FederationValidationError(
                "invalid-storage-control-plan", "groups", "must be an array of objects"
            )
        self._validate_unique_ids()

    def _validate_unique_ids(self) -> None:
        provider_ids = [_text(item.get("provider_id"), "provider_id") for item in self.providers]
        group_ids = [_text(item.get("group_id"), "group_id") for item in self.groups]
        if len(provider_ids) != len(set(provider_ids)):
            raise FederationValidationError(
                "invalid-storage-control-plan", "providers", "provider IDs must be unique"
            )
        if len(group_ids) != len(set(group_ids)):
            raise FederationValidationError(
                "invalid-storage-control-plan", "groups", "group IDs must be unique"
            )

    def unsigned_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "publication_id": self.publication_id,
            "publication_revision": self.publication_revision,
            "source_control_revision": self.source_control_revision,
            "session_id": self.session_id,
            "authority_node_id": self.authority_node_id,
            "authority_public_key": self.authority_public_key,
            "issued_at": _timestamp(self.issued_at),
            "providers": list(self.providers),
            "groups": list(self.groups),
        }

    def signing_bytes(self) -> bytes:
        return _canonical(self.unsigned_dict())

    def calculated_hash(self) -> str:
        return _hash(self.unsigned_dict())

    def verify(self, *, expected_authority_node_id: str) -> None:
        if self.authority_node_id != expected_authority_node_id:
            raise FederationValidationError(
                "storage-control-authority-mismatch",
                "authority_node_id",
                "plan authority is not the locally pinned coordinator",
            )
        if derive_node_id_from_public_key(self.authority_public_key) != self.authority_node_id:
            raise FederationValidationError(
                "storage-control-key-mismatch",
                "authority_public_key",
                "public key does not derive the declared authority identity",
            )
        if self.content_hash != self.calculated_hash():
            raise FederationValidationError(
                "storage-control-hash-mismatch",
                "content_hash",
                "plan content differs from its immutable hash",
            )
        if not verify_signature(
            self.authority_public_key,
            self.signing_bytes(),
            self.signature,
        ):
            raise FederationValidationError(
                "storage-control-signature-invalid",
                "signature",
                "plan signature is not valid for the pinned authority",
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.unsigned_dict(),
            "content_hash": self.content_hash,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> StorageControlPlan:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-storage-control-plan", "plan", "must be an object"
            )
        providers = value.get("providers")
        groups = value.get("groups")
        if not isinstance(providers, list) or not isinstance(groups, list):
            raise FederationValidationError(
                "invalid-storage-control-plan",
                "plan",
                "providers and groups must be arrays",
            )
        return cls(
            schema=value.get("schema"),
            publication_id=value.get("publication_id"),
            publication_revision=value.get("publication_revision"),
            source_control_revision=value.get("source_control_revision"),
            session_id=value.get("session_id"),
            authority_node_id=value.get("authority_node_id"),
            authority_public_key=value.get("authority_public_key"),
            issued_at=value.get("issued_at"),
            providers=tuple(providers),
            groups=tuple(groups),
            content_hash=value.get("content_hash"),
            signature=value.get("signature"),
        )

    @classmethod
    def issue(
        cls,
        *,
        credentials: NodeCredentials,
        publication_revision: int,
        source_control_revision: int,
        session_id: str,
        providers: tuple[dict[str, Any], ...],
        groups: tuple[dict[str, Any], ...],
        issued_at: datetime,
    ) -> StorageControlPlan:
        unsigned = {
            "schema": STORAGE_CONTROL_PLAN_SCHEMA,
            "publication_id": f"control-{uuid.uuid4().hex}",
            "publication_revision": publication_revision,
            "source_control_revision": source_control_revision,
            "session_id": session_id,
            "authority_node_id": credentials.identity.node_id,
            "authority_public_key": credentials.identity.public_key,
            "issued_at": _timestamp(issued_at),
            "providers": list(providers),
            "groups": list(groups),
        }
        return cls(
            **unsigned,
            issued_at=issued_at,
            providers=providers,
            groups=groups,
            content_hash=_hash(unsigned),
            signature=credentials.sign(_canonical(unsigned)),
        )


@dataclass(frozen=True)
class StorageControlApplyResult:
    status: str
    publication_id: str
    publication_revision: int
    content_hash: str
    local_control_revision: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": STORAGE_CONTROL_RESULT_SCHEMA,
            "status": self.status,
            "publication_id": self.publication_id,
            "publication_revision": self.publication_revision,
            "content_hash": self.content_hash,
            "local_control_revision": self.local_control_revision,
        }


class StorageControlPublicationStore:
    """Durably allocate authority publication revisions and signed snapshots."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS storage_control_publications (
                    session_id TEXT NOT NULL,
                    publication_revision INTEGER NOT NULL CHECK(publication_revision > 0),
                    publication_id TEXT NOT NULL UNIQUE,
                    content_hash TEXT NOT NULL,
                    plan_json TEXT NOT NULL CHECK(json_valid(plan_json)),
                    issued_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, publication_revision)
                )"""
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def issue(
        self,
        control: PhaseDControlPlane,
        credentials: NodeCredentials,
        session_id: str,
        *,
        now: datetime | None = None,
    ) -> StorageControlPlan:
        snapshot = control.snapshot(session_id)
        providers = tuple(
            {
                "session_id": provider.session_id,
                "provider_id": provider.provider_id,
                "node_id": provider.node_id,
                "protocol": provider.protocol,
                "protocol_version": provider.protocol_version,
                "authorized": provider.authorized,
                "status": provider.status,
            }
            for provider in sorted(
                snapshot.providers.values(), key=lambda item: item.provider_id
            )
        )
        groups: list[dict[str, Any]] = []
        for group_id, assignment in sorted(snapshot.groups.items()):
            grant = snapshot.leader_grants.get(group_id)
            groups.append(
                {
                    "group_id": group_id,
                    "primary_provider_id": assignment.primary_provider_id,
                    "replica_provider_ids": list(assignment.replica_provider_ids),
                    "acknowledgement_mode": control.acknowledgement_policy(
                        session_id, group_id
                    ).mode.value,
                    "grant": None
                    if grant is None
                    else {
                        "provider_id": grant["provider_id"],
                        "grant_id": grant["grant_id"],
                        "term": int(grant["term"]),
                        "fencing_token": int(grant["fencing_token"]),
                        "lease_expires_at": str(grant["lease_expires_at"]),
                        "scopes": list(grant["scopes"]),
                    },
                }
            )
        issued_at = _utc(now or datetime.now(timezone.utc))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            revision = int(
                connection.execute(
                    """SELECT COALESCE(MAX(publication_revision), 0) + 1
                       FROM storage_control_publications WHERE session_id=?""",
                    (session_id,),
                ).fetchone()[0]
            )
            plan = StorageControlPlan.issue(
                credentials=credentials,
                publication_revision=revision,
                source_control_revision=snapshot.revision,
                session_id=session_id,
                providers=providers,
                groups=tuple(groups),
                issued_at=issued_at,
            )
            connection.execute(
                """INSERT INTO storage_control_publications
                   (session_id, publication_revision, publication_id,
                    content_hash, plan_json, issued_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    session_id,
                    revision,
                    plan.publication_id,
                    plan.content_hash,
                    json.dumps(plan.to_dict(), sort_keys=True, separators=(",", ":")),
                    _timestamp(issued_at),
                ),
            )
            connection.commit()
        return plan


class StorageControlReplicaStore:
    """Apply complete authority snapshots to one local Phase D control plane."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS storage_control_replica_publications (
                    session_id TEXT PRIMARY KEY,
                    publication_revision INTEGER NOT NULL CHECK(publication_revision > 0),
                    publication_id TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    authority_node_id TEXT NOT NULL,
                    plan_json TEXT NOT NULL CHECK(json_valid(plan_json)),
                    applied_at TEXT NOT NULL
                )"""
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def latest(self, session_id: str) -> StorageControlPlan | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT plan_json FROM storage_control_replica_publications
                   WHERE session_id=?""",
                (session_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            return StorageControlPlan.from_dict(json.loads(row["plan_json"]))
        except (json.JSONDecodeError, FederationValidationError) as exc:
            raise FederationValidationError(
                "corrupt-storage-control-replica",
                "storage_control_replica_publications",
                "persisted control publication is malformed",
            ) from exc

    def apply(
        self,
        plan: StorageControlPlan,
        control: PhaseDControlPlane,
        *,
        expected_authority_node_id: str,
        now: datetime | None = None,
    ) -> StorageControlApplyResult:
        plan.verify(expected_authority_node_id=expected_authority_node_id)
        current = self.latest(plan.session_id)
        if current is not None:
            if plan.publication_revision < current.publication_revision:
                raise FederationValidationError(
                    "stale-storage-control-plan",
                    "publication_revision",
                    "publication is older than durable local control state",
                )
            if plan.publication_revision == current.publication_revision:
                if plan.content_hash != current.content_hash:
                    raise FederationValidationError(
                        "conflicting-storage-control-plan",
                        "publication_revision",
                        "the same revision has different signed content",
                    )
                return StorageControlApplyResult(
                    "duplicate",
                    plan.publication_id,
                    plan.publication_revision,
                    plan.content_hash,
                    control.snapshot(plan.session_id).revision,
                )
        self._reconcile(plan, control)
        stamp = _timestamp(_utc(now or datetime.now(timezone.utc)))
        encoded = json.dumps(plan.to_dict(), sort_keys=True, separators=(",", ":"))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """SELECT publication_revision, content_hash
                   FROM storage_control_replica_publications WHERE session_id=?""",
                (plan.session_id,),
            ).fetchone()
            if existing is not None and int(existing["publication_revision"]) > plan.publication_revision:
                connection.rollback()
                raise FederationValidationError(
                    "concurrent-storage-control-change",
                    "publication_revision",
                    "a newer publication was applied concurrently",
                )
            connection.execute(
                """INSERT INTO storage_control_replica_publications
                   (session_id, publication_revision, publication_id, content_hash,
                    authority_node_id, plan_json, applied_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(session_id) DO UPDATE SET
                       publication_revision=excluded.publication_revision,
                       publication_id=excluded.publication_id,
                       content_hash=excluded.content_hash,
                       authority_node_id=excluded.authority_node_id,
                       plan_json=excluded.plan_json,
                       applied_at=excluded.applied_at""",
                (
                    plan.session_id,
                    plan.publication_revision,
                    plan.publication_id,
                    plan.content_hash,
                    plan.authority_node_id,
                    encoded,
                    stamp,
                ),
            )
            connection.commit()
        return StorageControlApplyResult(
            "applied",
            plan.publication_id,
            plan.publication_revision,
            plan.content_hash,
            control.snapshot(plan.session_id).revision,
        )

    def _reconcile(self, plan: StorageControlPlan, control: PhaseDControlPlane) -> None:
        session_id = plan.session_id
        desired_providers = {
            str(value["provider_id"]): StorageProviderRegistration(
                session_id=session_id,
                provider_id=value.get("provider_id"),
                node_id=value.get("node_id"),
                protocol=value.get("protocol"),
                protocol_version=value.get("protocol_version"),
                authorized=value.get("authorized"),
                status=value.get("status"),
            )
            for value in plan.providers
        }
        desired_groups = {str(value["group_id"]): value for value in plan.groups}
        snapshot = control.snapshot(session_id)
        unexpected_groups = sorted(set(snapshot.groups).difference(desired_groups))
        unexpected_providers = sorted(set(snapshot.providers).difference(desired_providers))
        if unexpected_groups or unexpected_providers:
            raise FederationValidationError(
                "storage-control-local-state-conflict",
                "control_plane",
                "local control contains objects absent from the authority snapshot",
            )

        for group_id in sorted(desired_groups):
            if group_id not in control.snapshot(session_id).groups:
                control.create_group(session_id, plan.authority_node_id, group_id)

        for provider_id, desired in sorted(desired_providers.items()):
            current = control.snapshot(session_id).providers.get(provider_id)
            if current != desired:
                control.register_provider(
                    session_id,
                    plan.authority_node_id,
                    desired,
                )

        for group_id, desired in sorted(desired_groups.items()):
            primary = desired.get("primary_provider_id")
            if primary is not None:
                primary = _text(primary, "primary_provider_id")
            replicas_value = desired.get("replica_provider_ids")
            if not isinstance(replicas_value, list) or not all(
                isinstance(item, str) and item for item in replicas_value
            ):
                raise FederationValidationError(
                    "invalid-storage-control-plan",
                    "replica_provider_ids",
                    "must be an array of provider IDs",
                )
            replicas = tuple(replicas_value)
            assignment = control.snapshot(session_id).groups[group_id]
            if (
                assignment.primary_provider_id != primary
                or assignment.replica_provider_ids != replicas
            ):
                control.change_assignment(
                    session_id,
                    plan.authority_node_id,
                    group_id,
                    primary,
                    replicas,
                )
            try:
                mode = AcknowledgementMode(desired.get("acknowledgement_mode"))
            except ValueError as exc:
                raise FederationValidationError(
                    "invalid-storage-control-plan",
                    "acknowledgement_mode",
                    "unknown acknowledgement mode",
                ) from exc
            if control.acknowledgement_policy(session_id, group_id).mode is not mode:
                control.set_acknowledgement_policy(session_id, group_id, mode)
            self._reconcile_grant(plan, control, group_id, desired.get("grant"))

    @staticmethod
    def _reconcile_grant(
        plan: StorageControlPlan,
        control: PhaseDControlPlane,
        group_id: str,
        desired: Any,
    ) -> None:
        current = control.snapshot(plan.session_id).leader_grants.get(group_id)
        if desired is None:
            if current is not None:
                control.revoke_leader(
                    plan.session_id,
                    plan.authority_node_id,
                    group_id,
                    str(current["grant_id"]),
                )
            return
        if not isinstance(desired, dict):
            raise FederationValidationError(
                "invalid-storage-control-plan", "grant", "must be an object or null"
            )
        provider_id = _text(desired.get("provider_id"), "grant.provider_id")
        grant_id = _text(desired.get("grant_id"), "grant.grant_id")
        term = _uint(desired.get("term"), "grant.term", positive=True)
        fencing_token = _uint(
            desired.get("fencing_token"), "grant.fencing_token", positive=True
        )
        expiry = _utc(desired.get("lease_expires_at"))
        scopes_value = desired.get("scopes")
        if not isinstance(scopes_value, list) or not scopes_value or not all(
            isinstance(scope, str) and scope for scope in scopes_value
        ):
            raise FederationValidationError(
                "invalid-storage-control-plan",
                "grant.scopes",
                "must contain at least one operation",
            )
        scopes = tuple(scopes_value)
        if current is not None:
            current_binding = (
                current.get("provider_id"),
                current.get("grant_id"),
                int(current.get("term", -1)),
                int(current.get("fencing_token", -1)),
                _utc(str(current.get("lease_expires_at"))),
                tuple(current.get("scopes", ())),
            )
            desired_binding = (
                provider_id,
                grant_id,
                term,
                fencing_token,
                expiry,
                scopes,
            )
            if current_binding == desired_binding:
                return
            if term <= int(current["term"]) or fencing_token <= int(current["fencing_token"]):
                raise FederationValidationError(
                    "stale-storage-control-grant",
                    "grant",
                    "replacement grant must advance term and fencing token",
                )
            control.revoke_leader(
                plan.session_id,
                plan.authority_node_id,
                group_id,
                str(current["grant_id"]),
            )
        control.grant_leader(
            plan.session_id,
            plan.authority_node_id,
            group_id,
            provider_id,
            grant_id,
            term,
            fencing_token,
            lease_expires_at=expiry,
            scopes=scopes,
            occurred_at=plan.issued_at,
        )


class RelayMessageClient(Protocol):
    node_id: str

    async def send_message(
        self,
        *,
        session_id: str,
        target_node_id: str,
        payload: dict[str, Any],
        request_id: str | None = None,
    ) -> dict[str, Any]: ...

    async def receive_message(self, *, timeout: float | None = None): ...


@dataclass(frozen=True)
class StorageControlPublishResult:
    publication_id: str
    publication_revision: int
    acknowledged_node_ids: tuple[str, ...]


class StorageControlRelayPublisher:
    """Deliver one signed plan and require a bound acknowledgement per target."""

    def __init__(self, client: RelayMessageClient, *, timeout: float = 15.0) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.client = client
        self.timeout = float(timeout)

    async def publish(
        self,
        plan: StorageControlPlan,
        target_node_ids: tuple[str, ...],
    ) -> StorageControlPublishResult:
        if self.client.node_id != plan.authority_node_id:
            raise FederationValidationError(
                "storage-control-publisher-mismatch",
                "authority_node_id",
                "connected publisher does not own the signing identity",
            )
        targets = tuple(dict.fromkeys(_text(item, "target_node_id") for item in target_node_ids))
        if not targets:
            raise FederationValidationError(
                "missing-storage-control-target", "target_node_ids", "at least one target is required"
            )
        for target in targets:
            delivery = await self.client.send_message(
                session_id=plan.session_id,
                target_node_id=target,
                request_id=f"control-{plan.publication_id}-{target}",
                payload={
                    "kind": STORAGE_CONTROL_RELAY_KIND,
                    "message": "plan",
                    "plan": plan.to_dict(),
                },
            )
            if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
                raise FederationValidationError(
                    "storage-control-delivery-failed",
                    "target_node_id",
                    f"relay did not confirm delivery to {target}",
                )
        pending = set(targets)
        acknowledged: set[str] = set()
        deadline = time.monotonic() + self.timeout
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "storage control acknowledgement timeout for " + ", ".join(sorted(pending))
                )
            message = await self.client.receive_message(timeout=remaining)
            payload = getattr(message, "payload", None)
            actor = getattr(message, "actor_node_id", None)
            session_id = getattr(message, "session_id", None)
            if (
                actor not in pending
                or session_id != plan.session_id
                or not isinstance(payload, dict)
                or payload.get("kind") != STORAGE_CONTROL_RELAY_KIND
                or payload.get("message") != "response"
                or payload.get("publication_id") != plan.publication_id
                or payload.get("publication_revision") != plan.publication_revision
                or payload.get("content_hash") != plan.content_hash
            ):
                continue
            if payload.get("status") not in {"applied", "duplicate"}:
                error = payload.get("error")
                raise FederationValidationError(
                    "storage-control-target-rejected",
                    "target_node_id",
                    f"{actor}: {error}",
                )
            pending.remove(actor)
            acknowledged.add(actor)
        return StorageControlPublishResult(
            plan.publication_id,
            plan.publication_revision,
            tuple(sorted(acknowledged)),
        )
