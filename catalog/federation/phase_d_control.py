"""Operational Phase D control-plane facade with durable fencing and handover.

The original D3 event store intentionally modelled assignments and grants in isolation.
This facade adds the missing operational invariants required before Phase E:
monotonic term/fencing counters survive revocation, acknowledgement policy is durable,
and controlled handover is committed as one SQLite transaction.
"""

from __future__ import annotations

import copy
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .acknowledgement import AcknowledgementMode, AcknowledgementPolicy
from .errors import FederationValidationError
from .models import SessionEvent
from .storage_control_plane import (
    STORAGE_ASSIGNMENT_CHANGED,
    STORAGE_LEADER_GRANTED,
    STORAGE_LEADER_REVOKED,
    StorageControlPlaneSnapshot,
    StorageControlPlaneStore,
    StorageProviderRegistration,
)
from .storage_protocol import StorageOperation


def _utc(value: datetime | None = None) -> datetime:
    result = value or datetime.now(timezone.utc)
    if result.tzinfo is None or result.utcoffset() is None:
        raise FederationValidationError("invalid-timestamp", "occurred_at", "must be timezone-aware")
    return result.astimezone(timezone.utc)


@dataclass(frozen=True)
class HandoverCommit:
    session_id: str
    group_id: str
    source_provider_id: str
    target_provider_id: str
    grant_id: str
    term: int
    fencing_token: int
    revision: int


class PhaseDControlPlane:
    """Composition facade around :class:`StorageControlPlaneStore`.

    The underlying ordered event log remains the source of assignment/grant state.
    Two small metadata tables retain monotonic counters and per-group acknowledgement
    policy without introducing Phase E manifests or automatic promotion.
    """

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self.store = StorageControlPlaneStore(self.database)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS storage_fencing_counters (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    last_term INTEGER NOT NULL CHECK(last_term >= 0),
                    last_fencing_token INTEGER NOT NULL CHECK(last_fencing_token >= 0),
                    PRIMARY KEY(session_id, group_id)
                );
                CREATE TABLE IF NOT EXISTS storage_ack_policies (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('primary','one-replica','quorum','all')),
                    PRIMARY KEY(session_id, group_id)
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def snapshot(self, session_id: str) -> StorageControlPlaneSnapshot:
        return self.store.snapshot(session_id)

    def create_group(self, session_id: str, actor_node_id: str, group_id: str) -> SessionEvent:
        event = self.store.create_group(session_id, actor_node_id, group_id)
        self.set_acknowledgement_policy(session_id, group_id, AcknowledgementMode.PRIMARY)
        return event

    def register_provider(
        self,
        session_id: str,
        actor_node_id: str,
        provider: StorageProviderRegistration,
    ) -> SessionEvent:
        return self.store.register_provider(session_id, actor_node_id, provider)

    def remove_provider(self, session_id: str, actor_node_id: str, provider_id: str) -> SessionEvent:
        return self.store.remove_provider(session_id, actor_node_id, provider_id)

    def change_assignment(
        self,
        session_id: str,
        actor_node_id: str,
        group_id: str,
        primary_provider_id: str | None,
        replica_provider_ids: tuple[str, ...] = (),
    ) -> SessionEvent:
        return self.store.change_assignment(
            session_id,
            actor_node_id,
            group_id,
            primary_provider_id,
            replica_provider_ids,
        )

    def set_acknowledgement_policy(
        self,
        session_id: str,
        group_id: str,
        mode: AcknowledgementMode | str,
    ) -> AcknowledgementPolicy:
        policy = AcknowledgementPolicy(AcknowledgementMode(mode))
        if group_id not in self.snapshot(session_id).groups:
            raise FederationValidationError("unknown-storage-group", "group_id", "storage group is not registered")
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO storage_ack_policies(session_id, group_id, mode)
                   VALUES (?, ?, ?)
                   ON CONFLICT(session_id, group_id) DO UPDATE SET mode=excluded.mode""",
                (session_id, group_id, policy.mode.value),
            )
        return policy

    def acknowledgement_policy(self, session_id: str, group_id: str) -> AcknowledgementPolicy:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT mode FROM storage_ack_policies WHERE session_id=? AND group_id=?",
                (session_id, group_id),
            ).fetchone()
        return AcknowledgementPolicy(
            AcknowledgementMode.PRIMARY if row is None else AcknowledgementMode(row["mode"])
        )

    def _historical_maxima(self, session_id: str, group_id: str) -> tuple[int, int]:
        term = 0
        token = 0
        for event in self.store.events(session_id):
            if event.event_type != STORAGE_LEADER_GRANTED or event.payload.get("group_id") != group_id:
                continue
            term = max(term, int(event.payload["term"]))
            token = max(token, int(event.payload["fencing_token"]))
        with self._connect() as connection:
            row = connection.execute(
                """SELECT last_term, last_fencing_token FROM storage_fencing_counters
                   WHERE session_id=? AND group_id=?""",
                (session_id, group_id),
            ).fetchone()
        if row is not None:
            term = max(term, int(row["last_term"]))
            token = max(token, int(row["last_fencing_token"]))
        return term, token

    def grant_leader(
        self,
        session_id: str,
        actor_node_id: str,
        group_id: str,
        provider_id: str,
        grant_id: str,
        term: int,
        fencing_token: int,
        *,
        lease_expires_at: datetime | None = None,
        scopes: tuple[str, ...] = (StorageOperation.BATCH_INGEST.value,),
        occurred_at: datetime | None = None,
    ) -> SessionEvent:
        last_term, last_token = self._historical_maxima(session_id, group_id)
        if term <= last_term:
            raise FederationValidationError("stale-term", "term", "must exceed every previously issued term")
        if fencing_token <= last_token:
            raise FederationValidationError(
                "stale-fencing-token",
                "fencing_token",
                "must exceed every previously issued fencing token",
            )
        event = self.store.grant_leader(
            session_id,
            actor_node_id,
            group_id,
            provider_id,
            grant_id,
            term,
            fencing_token,
            lease_expires_at=lease_expires_at,
            scopes=scopes,
            occurred_at=occurred_at,
        )
        self._record_counter(session_id, group_id, term, fencing_token)
        return event

    def _record_counter(self, session_id: str, group_id: str, term: int, fencing_token: int) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO storage_fencing_counters
                       (session_id, group_id, last_term, last_fencing_token)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(session_id, group_id) DO UPDATE SET
                       last_term=MAX(last_term, excluded.last_term),
                       last_fencing_token=MAX(last_fencing_token, excluded.last_fencing_token)""",
                (session_id, group_id, term, fencing_token),
            )

    def revoke_leader(
        self,
        session_id: str,
        actor_node_id: str,
        group_id: str,
        grant_id: str,
    ) -> SessionEvent:
        return self.store.revoke_leader(session_id, actor_node_id, group_id, grant_id)

    def complete_handover(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        group_id: str,
        target_provider_id: str,
        grant_id: str,
        term: int,
        fencing_token: int,
        lease_expires_at: datetime | None = None,
        scopes: tuple[str, ...] = (StorageOperation.BATCH_INGEST.value,),
        occurred_at: datetime | None = None,
    ) -> HandoverCommit:
        """Atomically demote the source, promote the target, and issue its grant."""

        issued_at = _utc(occurred_at)
        expiry = _utc(lease_expires_at or issued_at + timedelta(minutes=5))
        snapshot = self.snapshot(session_id)
        assignment = snapshot.groups.get(group_id)
        if assignment is None or assignment.primary_provider_id is None:
            raise FederationValidationError("handover-no-primary", "group_id", "storage group has no assigned primary")
        source_provider_id = assignment.primary_provider_id
        if target_provider_id not in assignment.replica_provider_ids:
            raise FederationValidationError(
                "handover-target-not-replica",
                "target_provider_id",
                "target must be an assigned replica",
            )
        target = snapshot.providers.get(target_provider_id)
        if target is None or not target.assignable:
            raise FederationValidationError(
                "handover-target-unavailable", "target_provider_id", "target provider is not assignable"
            )
        active = snapshot.leader_grants.get(group_id)
        if active is None:
            raise FederationValidationError("unknown-grant", "group_id", "source primary has no active grant")
        last_term, last_token = self._historical_maxima(session_id, group_id)
        if term <= last_term:
            raise FederationValidationError("stale-term", "term", "must exceed every previously issued term")
        if fencing_token <= last_token:
            raise FederationValidationError(
                "stale-fencing-token", "fencing_token", "must exceed every previously issued fencing token"
            )
        if expiry <= issued_at:
            raise FederationValidationError("invalid-lease", "lease_expires_at", "must be later than issuance")

        new_replicas = tuple(
            provider_id
            for provider_id in (source_provider_id, *assignment.replica_provider_ids)
            if provider_id != target_provider_id
        )
        start_revision = snapshot.revision
        event_specs: list[tuple[str, dict[str, Any]]] = [
            (STORAGE_LEADER_REVOKED, {"group_id": group_id, "grant_id": active["grant_id"]}),
            (
                STORAGE_ASSIGNMENT_CHANGED,
                {
                    "group_id": group_id,
                    "primary_provider_id": target_provider_id,
                    "replica_provider_ids": list(dict.fromkeys(new_replicas)),
                },
            ),
            (
                STORAGE_LEADER_GRANTED,
                {
                    "group_id": group_id,
                    "provider_id": target_provider_id,
                    "grant_id": grant_id,
                    "term": term,
                    "fencing_token": fencing_token,
                    "lease_expires_at": expiry.isoformat(),
                    "scopes": list(scopes),
                },
            ),
        ]
        candidate = copy.deepcopy(snapshot)
        events: list[SessionEvent] = []
        for offset, (event_type, payload) in enumerate(event_specs, start=1):
            event = SessionEvent(
                session_id=session_id,
                revision=start_revision + offset,
                event_id=uuid.uuid4().hex,
                event_type=event_type,
                occurred_at=issued_at,
                actor_node_id=actor_node_id,
                payload=payload,
            )
            candidate.apply(event)
            events.append(event)

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current_revision = connection.execute(
                "SELECT COALESCE(MAX(revision), 0) FROM storage_control_events WHERE session_id=?",
                (session_id,),
            ).fetchone()[0]
            if current_revision != start_revision:
                connection.rollback()
                raise FederationValidationError(
                    "concurrent-control-change",
                    "revision",
                    "storage control state changed while handover was being prepared",
                )
            for event in events:
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
            connection.execute(
                """INSERT INTO storage_fencing_counters
                       (session_id, group_id, last_term, last_fencing_token)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(session_id, group_id) DO UPDATE SET
                       last_term=excluded.last_term,
                       last_fencing_token=excluded.last_fencing_token""",
                (session_id, group_id, term, fencing_token),
            )
            connection.commit()

        return HandoverCommit(
            session_id,
            group_id,
            source_provider_id,
            target_provider_id,
            grant_id,
            term,
            fencing_token,
            events[-1].revision,
        )
