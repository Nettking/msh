"""Replayable D3 storage-provider registration and assignment state."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import FederationValidationError, RevisionGapError
from .models import SessionEvent
from .storage_protocol import STORAGE_PROTOCOL, STORAGE_PROTOCOL_MAJOR

STORAGE_GROUP_CREATED = "storage.group.created"
STORAGE_PROVIDER_REGISTERED = "storage.provider.registered"
STORAGE_PROVIDER_REMOVED = "storage.provider.removed"
STORAGE_ASSIGNMENT_CHANGED = "storage.assignment.changed"
STORAGE_LEADER_GRANTED = "storage.leader.granted"
STORAGE_LEADER_REVOKED = "storage.leader.revoked"


def _required(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(char) < 32 for char in value):
        raise FederationValidationError("invalid-id", field_name, "must be non-empty opaque text")
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError("invalid-timestamp", "occurred_at", "must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class StorageProviderRegistration:
    session_id: str
    provider_id: str
    node_id: str
    protocol: str
    protocol_version: str
    authorized: bool
    status: str

    def __post_init__(self) -> None:
        for name in ("session_id", "provider_id", "node_id", "protocol", "protocol_version", "status"):
            _required(getattr(self, name), name)
        if not isinstance(self.authorized, bool):
            raise FederationValidationError("invalid-boolean", "authorized", "must be boolean")

    @property
    def assignable(self) -> bool:
        major = self.protocol_version.split(".", 1)[0]
        return (
            self.authorized
            and self.status == "ready"
            and self.protocol == STORAGE_PROTOCOL
            and major == str(STORAGE_PROTOCOL_MAJOR)
        )


@dataclass(frozen=True)
class StorageGroupAssignment:
    group_id: str
    primary_provider_id: str | None = None
    replica_provider_ids: tuple[str, ...] = ()


@dataclass
class StorageControlPlaneSnapshot:
    session_id: str
    revision: int = 0
    providers: dict[str, StorageProviderRegistration] = field(default_factory=dict)
    groups: dict[str, StorageGroupAssignment] = field(default_factory=dict)
    leader_grants: dict[str, dict[str, Any]] = field(default_factory=dict)

    def apply(self, event: SessionEvent) -> None:
        if event.session_id != self.session_id:
            raise FederationValidationError("session-mismatch", "session_id", "event belongs to another session")
        if event.revision != self.revision + 1:
            raise RevisionGapError("revision-gap", "event revision is not contiguous")
        payload = event.payload
        if event.event_type == STORAGE_GROUP_CREATED:
            group_id = _required(payload.get("group_id"), "group_id")
            if group_id in self.groups:
                raise FederationValidationError("group-exists", "group_id", "storage group already exists")
            self.groups[group_id] = StorageGroupAssignment(group_id)
        elif event.event_type == STORAGE_PROVIDER_REGISTERED:
            provider = StorageProviderRegistration(
                session_id=self.session_id,
                provider_id=payload.get("provider_id"),
                node_id=payload.get("node_id"),
                protocol=payload.get("protocol"),
                protocol_version=payload.get("protocol_version"),
                authorized=payload.get("authorized"),
                status=payload.get("status"),
            )
            self.providers[provider.provider_id] = provider
        elif event.event_type == STORAGE_PROVIDER_REMOVED:
            provider_id = _required(payload.get("provider_id"), "provider_id")
            for assignment in self.groups.values():
                if provider_id == assignment.primary_provider_id or provider_id in assignment.replica_provider_ids:
                    raise FederationValidationError("provider-assigned", "provider_id", "assigned provider cannot be removed")
            self.providers.pop(provider_id, None)
        elif event.event_type == STORAGE_ASSIGNMENT_CHANGED:
            group_id = _required(payload.get("group_id"), "group_id")
            if group_id not in self.groups:
                raise FederationValidationError("unknown-group", "group_id", "storage group is not registered")
            primary = payload.get("primary_provider_id")
            replicas = tuple(payload.get("replica_provider_ids", ()))
            if primary is not None:
                _required(primary, "primary_provider_id")
            if len(replicas) != len(set(replicas)) or primary in replicas:
                raise FederationValidationError("invalid-assignment", "replica_provider_ids", "providers must be unique")
            for provider_id in ((primary,) if primary else ()) + replicas:
                provider = self.providers.get(provider_id)
                if provider is None:
                    raise FederationValidationError("unknown-provider", "provider_id", "provider is not registered")
                if not provider.assignable:
                    raise FederationValidationError("provider-not-assignable", "provider_id", "provider is not ready, authorized, and protocol-compatible")
            self.groups[group_id] = StorageGroupAssignment(group_id, primary, replicas)
        elif event.event_type == STORAGE_LEADER_GRANTED:
            group_id = _required(payload.get("group_id"), "group_id")
            provider_id = _required(payload.get("provider_id"), "provider_id")
            assignment = self.groups.get(group_id)
            if assignment is None or assignment.primary_provider_id != provider_id:
                raise FederationValidationError("not-assigned-primary", "provider_id", "leader must be the assigned primary")
            self.leader_grants[group_id] = dict(payload)
        elif event.event_type == STORAGE_LEADER_REVOKED:
            group_id = _required(payload.get("group_id"), "group_id")
            self.leader_grants.pop(group_id, None)
        else:
            raise FederationValidationError("unsupported-event", "event_type", "unknown storage control-plane event")
        self.revision = event.revision


class StorageControlPlaneStore:
    """SQLite event log whose state is reconstructed exclusively by replay."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS storage_control_events (
                    session_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    actor_node_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
                    PRIMARY KEY(session_id, revision)
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def snapshot(self, session_id: str) -> StorageControlPlaneSnapshot:
        snapshot = StorageControlPlaneSnapshot(_required(session_id, "session_id"))
        for event in self.events(session_id):
            snapshot.apply(event)
        return snapshot

    def events(self, session_id: str) -> tuple[SessionEvent, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM storage_control_events WHERE session_id = ? ORDER BY revision",
                (session_id,),
            ).fetchall()
        return tuple(
            SessionEvent(
                session_id=row["session_id"],
                revision=row["revision"],
                event_id=row["event_id"],
                event_type=row["event_type"],
                occurred_at=datetime.fromisoformat(row["occurred_at"]),
                actor_node_id=row["actor_node_id"],
                payload=json.loads(row["payload_json"]),
            )
            for row in rows
        )

    def append(self, *, session_id: str, actor_node_id: str, event_type: str, payload: dict[str, Any], occurred_at: datetime | None = None) -> SessionEvent:
        session_id = _required(session_id, "session_id")
        actor_node_id = _required(actor_node_id, "actor_node_id")
        occurred_at = _utc(occurred_at or datetime.now(timezone.utc))
        if not isinstance(payload, dict):
            raise FederationValidationError("invalid-object", "payload", "must be an object")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            revision = connection.execute(
                "SELECT COALESCE(MAX(revision), 0) + 1 FROM storage_control_events WHERE session_id = ?",
                (session_id,),
            ).fetchone()[0]
            event = SessionEvent(
                session_id=session_id,
                revision=revision,
                event_id=uuid.uuid4().hex,
                event_type=event_type,
                occurred_at=occurred_at,
                actor_node_id=actor_node_id,
                payload=payload,
            )
            candidate = self.snapshot(session_id)
            candidate.apply(event)
            connection.execute(
                "INSERT INTO storage_control_events VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    event.session_id,
                    event.revision,
                    event.event_id,
                    event.event_type,
                    event.occurred_at.isoformat(),
                    event.actor_node_id,
                    json.dumps(event.payload, sort_keys=True, separators=(",", ":"), allow_nan=False),
                ),
            )
            connection.commit()
        return event

    def create_group(self, session_id: str, actor_node_id: str, group_id: str) -> SessionEvent:
        return self.append(session_id=session_id, actor_node_id=actor_node_id, event_type=STORAGE_GROUP_CREATED, payload={"group_id": group_id})

    def register_provider(self, session_id: str, actor_node_id: str, provider: StorageProviderRegistration) -> SessionEvent:
        if provider.session_id != session_id:
            raise FederationValidationError("session-mismatch", "session_id", "provider belongs to another session")
        return self.append(
            session_id=session_id,
            actor_node_id=actor_node_id,
            event_type=STORAGE_PROVIDER_REGISTERED,
            payload={
                "provider_id": provider.provider_id,
                "node_id": provider.node_id,
                "protocol": provider.protocol,
                "protocol_version": provider.protocol_version,
                "authorized": provider.authorized,
                "status": provider.status,
            },
        )

    def remove_provider(self, session_id: str, actor_node_id: str, provider_id: str) -> SessionEvent:
        return self.append(session_id=session_id, actor_node_id=actor_node_id, event_type=STORAGE_PROVIDER_REMOVED, payload={"provider_id": provider_id})

    def change_assignment(self, session_id: str, actor_node_id: str, group_id: str, primary_provider_id: str | None, replica_provider_ids: tuple[str, ...] = ()) -> SessionEvent:
        return self.append(
            session_id=session_id,
            actor_node_id=actor_node_id,
            event_type=STORAGE_ASSIGNMENT_CHANGED,
            payload={"group_id": group_id, "primary_provider_id": primary_provider_id, "replica_provider_ids": list(replica_provider_ids)},
        )

    def grant_leader(self, session_id: str, actor_node_id: str, group_id: str, provider_id: str, grant_id: str, term: int, fencing_token: int) -> SessionEvent:
        return self.append(
            session_id=session_id,
            actor_node_id=actor_node_id,
            event_type=STORAGE_LEADER_GRANTED,
            payload={"group_id": group_id, "provider_id": provider_id, "grant_id": grant_id, "term": term, "fencing_token": fencing_token},
        )

    def revoke_leader(self, session_id: str, actor_node_id: str, group_id: str, grant_id: str) -> SessionEvent:
        return self.append(session_id=session_id, actor_node_id=actor_node_id, event_type=STORAGE_LEADER_REVOKED, payload={"group_id": group_id, "grant_id": grant_id})
