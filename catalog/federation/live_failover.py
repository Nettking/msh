"""Automatic relay-aware failover for live storage providers.

The coordinator promotes only a connected assigned replica that is proven to
contain every item in the authoritative manifest.  A provider node has no
inbound listener, so loss of its authenticated relay connection is the network
fence.  Live storage agents add a second provider-local fence before reconnect.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from catalog.node.identity import NodeCredentials

from .acknowledgement import AcknowledgementMode
from .control_sync import (
    STORAGE_CONTROL_RELAY_KIND,
    StorageControlPlan,
    StorageControlPublicationStore,
)
from .errors import FederationValidationError
from .phase_d_control import PhaseDControlPlane

STORAGE_CONTROL_REFRESH_MESSAGE = "refresh-request"
LIVE_FAILOVER_SCHEMA = "msh.live_storage_failover.v1"
LIVE_FAILOVER_RESULT_SCHEMA = "msh.live_storage_failover_result.v1"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-timestamp", "now", "must be timezone-aware"
        )
    return value.astimezone(timezone.utc)


def _stamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-live-failover-field", field, "must be non-empty text"
        )
    return value


def _json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


@dataclass(frozen=True)
class LiveFailoverRecord:
    schema: str
    failover_id: str
    session_id: str
    group_id: str
    failed_provider_id: str
    failed_node_id: str
    promoted_provider_id: str
    retained_replica_provider_ids: tuple[str, ...]
    previous_acknowledgement_mode: AcknowledgementMode
    effective_acknowledgement_mode: AcknowledgementMode
    source_control_revision: int
    term: int
    fencing_token: int
    grant_id: str
    state: str
    publication: StorageControlPlan | None
    reason_code: str
    reason_detail: str
    detected_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if self.schema != LIVE_FAILOVER_SCHEMA:
            raise FederationValidationError(
                "unsupported-live-failover", "schema", f"expected {LIVE_FAILOVER_SCHEMA}"
            )
        for field in (
            "failover_id",
            "session_id",
            "group_id",
            "failed_provider_id",
            "failed_node_id",
            "promoted_provider_id",
            "grant_id",
            "reason_code",
            "reason_detail",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        replicas = tuple(sorted(_text(value, "retained_replica_provider_ids") for value in self.retained_replica_provider_ids))
        if len(replicas) != len(set(replicas)) or self.promoted_provider_id in replicas:
            raise FederationValidationError(
                "invalid-live-failover-replicas",
                "retained_replica_provider_ids",
                "replicas must be unique and exclude the promoted provider",
            )
        object.__setattr__(self, "retained_replica_provider_ids", replicas)
        for field in ("source_control_revision", "term", "fencing_token"):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise FederationValidationError(
                    "invalid-live-failover-counter", field, "must be a positive integer"
                )
        try:
            object.__setattr__(
                self,
                "previous_acknowledgement_mode",
                AcknowledgementMode(self.previous_acknowledgement_mode),
            )
            object.__setattr__(
                self,
                "effective_acknowledgement_mode",
                AcknowledgementMode(self.effective_acknowledgement_mode),
            )
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-live-failover-mode", "acknowledgement_mode", "unknown mode"
            ) from exc
        if self.state not in {"detected", "control-committed", "published", "failed"}:
            raise FederationValidationError(
                "invalid-live-failover-state", "state", "unknown failover state"
            )
        object.__setattr__(self, "detected_at", _utc(self.detected_at))
        object.__setattr__(self, "updated_at", _utc(self.updated_at))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "failover_id": self.failover_id,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "failed_provider_id": self.failed_provider_id,
            "failed_node_id": self.failed_node_id,
            "promoted_provider_id": self.promoted_provider_id,
            "retained_replica_provider_ids": list(self.retained_replica_provider_ids),
            "previous_acknowledgement_mode": self.previous_acknowledgement_mode.value,
            "effective_acknowledgement_mode": self.effective_acknowledgement_mode.value,
            "source_control_revision": self.source_control_revision,
            "term": self.term,
            "fencing_token": self.fencing_token,
            "grant_id": self.grant_id,
            "state": self.state,
            "publication": None if self.publication is None else self.publication.to_dict(),
            "reason_code": self.reason_code,
            "reason_detail": self.reason_detail,
            "detected_at": _stamp(self.detected_at),
            "updated_at": _stamp(self.updated_at),
        }

    @classmethod
    def from_dict(cls, value: Any) -> LiveFailoverRecord:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-live-failover", "record", "must be an object"
            )
        publication = value.get("publication")
        return cls(
            schema=value.get("schema"),
            failover_id=value.get("failover_id"),
            session_id=value.get("session_id"),
            group_id=value.get("group_id"),
            failed_provider_id=value.get("failed_provider_id"),
            failed_node_id=value.get("failed_node_id"),
            promoted_provider_id=value.get("promoted_provider_id"),
            retained_replica_provider_ids=tuple(value.get("retained_replica_provider_ids", ())),
            previous_acknowledgement_mode=value.get("previous_acknowledgement_mode"),
            effective_acknowledgement_mode=value.get("effective_acknowledgement_mode"),
            source_control_revision=value.get("source_control_revision"),
            term=value.get("term"),
            fencing_token=value.get("fencing_token"),
            grant_id=value.get("grant_id"),
            state=value.get("state"),
            publication=(
                None
                if publication is None
                else StorageControlPlan.from_dict(publication)
            ),
            reason_code=value.get("reason_code"),
            reason_detail=value.get("reason_detail"),
            detected_at=value.get("detected_at"),
            updated_at=value.get("updated_at"),
        )


@dataclass(frozen=True)
class LiveFailoverResult:
    status: str
    session_id: str
    group_id: str
    failed_provider_id: str | None
    promoted_provider_id: str | None
    reason: str
    publication_revision: int | None = None
    term: int | None = None
    fencing_token: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": LIVE_FAILOVER_RESULT_SCHEMA,
            "status": self.status,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "failed_provider_id": self.failed_provider_id,
            "promoted_provider_id": self.promoted_provider_id,
            "reason": self.reason,
            "publication_revision": self.publication_revision,
            "term": self.term,
            "fencing_token": self.fencing_token,
        }


class LiveFailoverStore:
    """Durable, restart-safe progress for one automatic failover per group."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS storage_live_failovers (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    failover_id TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(
                        state IN ('detected','control-committed','published','failed')
                    ),
                    record_json TEXT NOT NULL CHECK(json_valid(record_json)),
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, group_id, failover_id)
                )"""
            )
            connection.execute(
                """CREATE INDEX IF NOT EXISTS storage_live_failovers_active
                   ON storage_live_failovers(session_id, group_id, state, updated_at)"""
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _decode(row: sqlite3.Row) -> LiveFailoverRecord:
        try:
            record = LiveFailoverRecord.from_dict(json.loads(row["record_json"]))
        except (json.JSONDecodeError, FederationValidationError) as exc:
            raise FederationValidationError(
                "corrupt-live-failover", "record_json", "persisted failover is malformed"
            ) from exc
        if (
            row["session_id"] != record.session_id
            or row["group_id"] != record.group_id
            or row["failover_id"] != record.failover_id
            or row["state"] != record.state
        ):
            raise FederationValidationError(
                "live-failover-row-mismatch",
                "record_json",
                "indexed failover metadata differs from its canonical record",
            )
        return record

    def get(self, session_id: str, group_id: str, failover_id: str) -> LiveFailoverRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM storage_live_failovers
                   WHERE session_id=? AND group_id=? AND failover_id=?""",
                (session_id, group_id, failover_id),
            ).fetchone()
        return None if row is None else self._decode(row)

    def active(self, session_id: str, group_id: str) -> LiveFailoverRecord | None:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM storage_live_failovers
                   WHERE session_id=? AND group_id=?
                     AND state IN ('detected','control-committed')
                   ORDER BY updated_at DESC""",
                (session_id, group_id),
            ).fetchall()
        if len(rows) > 1:
            raise FederationValidationError(
                "multiple-active-live-failovers",
                "group_id",
                "more than one unfinished failover exists for the group",
            )
        return None if not rows else self._decode(rows[0])

    def save(self, record: LiveFailoverRecord) -> LiveFailoverRecord:
        existing = self.get(record.session_id, record.group_id, record.failover_id)
        if existing is not None:
            immutable = (
                "failed_provider_id",
                "failed_node_id",
                "promoted_provider_id",
                "retained_replica_provider_ids",
                "previous_acknowledgement_mode",
                "effective_acknowledgement_mode",
                "source_control_revision",
                "term",
                "fencing_token",
                "grant_id",
            )
            if any(getattr(existing, field) != getattr(record, field) for field in immutable):
                raise FederationValidationError(
                    "conflicting-live-failover",
                    "failover_id",
                    "durable failover identity was reused with different inputs",
                )
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO storage_live_failovers
                   (session_id, group_id, failover_id, state, record_json, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(session_id, group_id, failover_id) DO UPDATE SET
                       state=excluded.state,
                       record_json=excluded.record_json,
                       updated_at=excluded.updated_at""",
                (
                    record.session_id,
                    record.group_id,
                    record.failover_id,
                    record.state,
                    _json(record.to_dict()),
                    _stamp(record.updated_at),
                ),
            )
        return record


@dataclass
class _PendingPublication:
    plan: StorageControlPlan
    pending_node_ids: set[str]
    acknowledged_node_ids: set[str]
    future: asyncio.Future[tuple[str, ...]]


class StorageControlRelayChannel:
    """Demultiplex control publications and refresh requests on one relay endpoint."""

    def __init__(self, client: Any, endpoint: Any, *, timeout: float = 15.0) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.client = client
        self.endpoint = endpoint
        self.timeout = float(timeout)
        self._pending: dict[str, _PendingPublication] = {}
        self._receiver_task: asyncio.Task[None] | None = None
        self._refresh_tasks: set[asyncio.Task[None]] = set()
        self._refresh_handler: Callable[[str, str, dict[str, Any]], Awaitable[None]] | None = None

    def set_refresh_handler(
        self,
        handler: Callable[[str, str, dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._refresh_handler = handler

    async def start(self) -> None:
        if self._receiver_task is not None:
            return
        self._receiver_task = asyncio.create_task(
            self._receiver_loop(), name=f"msh-storage-control-channel-{self.client.node_id}"
        )

    async def close(self) -> None:
        task, self._receiver_task = self._receiver_task, None
        if task is not None:
            task.cancel()
        for refresh in tuple(self._refresh_tasks):
            refresh.cancel()
        await asyncio.gather(
            *(item for item in (task, *self._refresh_tasks) if item is not None),
            return_exceptions=True,
        )
        self._refresh_tasks.clear()
        for pending in tuple(self._pending.values()):
            if not pending.future.done():
                pending.future.set_exception(
                    FederationValidationError(
                        "storage-control-channel-closed",
                        "publication_id",
                        "control channel closed before acknowledgement",
                    )
                )
        self._pending.clear()

    async def publish(
        self,
        plan: StorageControlPlan,
        target_node_ids: tuple[str, ...],
    ) -> tuple[str, ...]:
        if self._receiver_task is None:
            raise FederationValidationError(
                "storage-control-channel-not-started",
                "channel",
                "control channel must be started before publication",
            )
        if self.client.node_id != plan.authority_node_id:
            raise FederationValidationError(
                "storage-control-publisher-mismatch",
                "authority_node_id",
                "connected publisher does not own the signed plan",
            )
        targets = tuple(dict.fromkeys(_text(value, "target_node_id") for value in target_node_ids))
        if not targets:
            raise FederationValidationError(
                "missing-storage-control-target",
                "target_node_ids",
                "at least one target is required",
            )
        if plan.publication_id in self._pending:
            raise FederationValidationError(
                "duplicate-storage-control-publication",
                "publication_id",
                "publication is already in flight",
            )
        loop = asyncio.get_running_loop()
        pending = _PendingPublication(
            plan=plan,
            pending_node_ids=set(targets),
            acknowledged_node_ids=set(),
            future=loop.create_future(),
        )
        self._pending[plan.publication_id] = pending
        frame = _json(plan.to_dict())
        try:
            for target in targets:
                delivery = await self.client.send_message(
                    session_id=plan.session_id,
                    target_node_id=target,
                    request_id=f"control-{plan.publication_id}-{target}",
                    payload={
                        "kind": STORAGE_CONTROL_RELAY_KIND,
                        "message": "plan",
                        "frame": frame,
                    },
                )
                if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
                    raise FederationValidationError(
                        "storage-control-delivery-failed",
                        "target_node_id",
                        f"relay did not confirm delivery to {target}",
                    )
            return await asyncio.wait_for(pending.future, timeout=self.timeout)
        finally:
            self._pending.pop(plan.publication_id, None)

    async def _receiver_loop(self) -> None:
        while True:
            message = await self.endpoint.receive_other()
            payload = getattr(message, "payload", None)
            if not isinstance(payload, dict) or payload.get("kind") != STORAGE_CONTROL_RELAY_KIND:
                continue
            message_kind = payload.get("message")
            if message_kind == "response":
                self._accept_response(message, payload)
            elif message_kind == STORAGE_CONTROL_REFRESH_MESSAGE:
                handler = self._refresh_handler
                actor = getattr(message, "actor_node_id", None)
                session_id = getattr(message, "session_id", None)
                if handler is None or not isinstance(actor, str) or not isinstance(session_id, str):
                    continue
                task = asyncio.create_task(handler(actor, session_id, payload))
                self._refresh_tasks.add(task)
                task.add_done_callback(self._refresh_tasks.discard)

    def _accept_response(self, message: Any, payload: dict[str, Any]) -> None:
        publication_id = payload.get("publication_id")
        actor = getattr(message, "actor_node_id", None)
        pending = self._pending.get(publication_id)
        if pending is None or actor not in pending.pending_node_ids:
            return
        plan = pending.plan
        if (
            getattr(message, "session_id", None) != plan.session_id
            or payload.get("publication_revision") != plan.publication_revision
            or payload.get("content_hash") != plan.content_hash
        ):
            return
        if payload.get("status") not in {"applied", "duplicate"}:
            error = payload.get("error")
            if not pending.future.done():
                pending.future.set_exception(
                    FederationValidationError(
                        "storage-control-target-rejected",
                        "target_node_id",
                        f"{actor}: {error}",
                    )
                )
            return
        pending.pending_node_ids.remove(actor)
        pending.acknowledged_node_ids.add(actor)
        if not pending.pending_node_ids and not pending.future.done():
            pending.future.set_result(tuple(sorted(pending.acknowledged_node_ids)))


class StorageFailoverCoordinator:
    """Detect unavailable primaries and publish a verified replacement authority."""

    def __init__(
        self,
        *,
        session_coordinator: Any,
        control_plane: PhaseDControlPlane,
        publication_store: StorageControlPublicationStore,
        credentials: NodeCredentials,
        channel: StorageControlRelayChannel,
        failover_store: LiveFailoverStore,
        session_id: str,
        lease_seconds: float = 300.0,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.session_coordinator = session_coordinator
        self.control_plane = control_plane
        self.publication_store = publication_store
        self.credentials = credentials
        self.channel = channel
        self.failover_store = failover_store
        self.session_id = _text(session_id, "session_id")
        self.lease_seconds = float(lease_seconds)
        self.clock = clock
        self.channel.set_refresh_handler(self._handle_refresh_request)

    async def start(self) -> None:
        await self.channel.start()

    async def close(self) -> None:
        await self.channel.close()

    async def publish_current(self, target_node_ids: tuple[str, ...]) -> StorageControlPlan:
        plan = self.publication_store.issue(
            self.control_plane,
            self.credentials,
            self.session_id,
            now=self.clock(),
        )
        await self.channel.publish(plan, target_node_ids)
        return plan

    async def _handle_refresh_request(
        self,
        actor_node_id: str,
        session_id: str,
        payload: dict[str, Any],
    ) -> None:
        if session_id != self.session_id:
            return
        provider_id = payload.get("provider_id")
        snapshot = self.control_plane.snapshot(self.session_id)
        provider = snapshot.providers.get(provider_id)
        if provider is None or provider.node_id != actor_node_id:
            return
        await self.publish_current((actor_node_id,))

    async def run_forever(self, *, scan_interval: float = 2.0) -> None:
        if scan_interval <= 0:
            raise ValueError("scan_interval must be positive")
        await self.start()
        while True:
            await self.scan_once()
            await asyncio.sleep(scan_interval)

    async def scan_once(self) -> tuple[LiveFailoverResult, ...]:
        status = self._status_snapshot()
        snapshot = self.control_plane.snapshot(self.session_id)
        results: list[LiveFailoverResult] = []
        for group_id, assignment in sorted(snapshot.groups.items()):
            active = self.failover_store.active(self.session_id, group_id)
            if active is not None:
                results.append(await self._resume(active, status))
                continue
            failed_provider_id = assignment.primary_provider_id
            if failed_provider_id is None:
                continue
            failed_provider = snapshot.providers.get(failed_provider_id)
            if failed_provider is None:
                continue
            if self._provider_online(failed_provider_id, failed_provider.node_id, status):
                continue
            manifest = self.control_plane.manifest(self.session_id, group_id)
            candidates = tuple(
                provider_id
                for provider_id in sorted(assignment.replica_provider_ids)
                if self._eligible_candidate(
                    provider_id,
                    snapshot.providers,
                    status,
                    manifest,
                )
            )
            if not candidates:
                reason = "no connected replica has complete authoritative manifest evidence"
                self.control_plane.set_storage_degraded_state(
                    self.session_id,
                    group_id,
                    manifest=manifest,
                    reason_code="automatic-failover-no-complete-candidate",
                    reason_detail=reason,
                    missing_ranges=(),
                    obligations={
                        "action": "restore-or-catch-up-a-complete-replica",
                        "failed_provider_id": failed_provider_id,
                    },
                    now=self.clock(),
                )
                results.append(
                    LiveFailoverResult(
                        "blocked",
                        self.session_id,
                        group_id,
                        failed_provider_id,
                        None,
                        reason,
                    )
                )
                continue
            promoted = candidates[0]
            retained = tuple(value for value in candidates[1:])
            prior_mode = self.control_plane.acknowledgement_policy(
                self.session_id, group_id
            ).mode
            effective_mode = self._available_mode(prior_mode, len(retained))
            last_term, last_token = self.control_plane._historical_maxima(  # noqa: SLF001 - package-level authority allocator
                self.session_id, group_id
            )
            term = last_term + 1
            token = last_token + 1
            failover_id = self._failover_id(
                self.session_id,
                group_id,
                failed_provider_id,
                promoted,
                snapshot.revision,
            )
            detected_at = _utc(self.clock())
            record = LiveFailoverRecord(
                schema=LIVE_FAILOVER_SCHEMA,
                failover_id=failover_id,
                session_id=self.session_id,
                group_id=group_id,
                failed_provider_id=failed_provider_id,
                failed_node_id=failed_provider.node_id,
                promoted_provider_id=promoted,
                retained_replica_provider_ids=retained,
                previous_acknowledgement_mode=prior_mode,
                effective_acknowledgement_mode=effective_mode,
                source_control_revision=snapshot.revision,
                term=term,
                fencing_token=token,
                grant_id=f"live-failover-grant-{failover_id[7:]}",
                state="detected",
                publication=None,
                reason_code="primary-relay-unavailable",
                reason_detail="primary node is disconnected or its storage capability is unavailable",
                detected_at=detected_at,
                updated_at=detected_at,
            )
            self.failover_store.save(record)
            results.append(await self._resume(record, status))
        return tuple(results)

    async def _resume(
        self,
        record: LiveFailoverRecord,
        status: dict[str, Any],
    ) -> LiveFailoverResult:
        current = record
        if current.state == "detected":
            snapshot = self.control_plane.snapshot(current.session_id)
            assignment = snapshot.groups.get(current.group_id)
            if assignment is None:
                return self._fail(current, "failover-group-disappeared", "storage group no longer exists")
            if assignment.primary_provider_id == current.failed_provider_id:
                self.control_plane.complete_handover(
                    session_id=current.session_id,
                    actor_node_id=self.credentials.identity.node_id,
                    group_id=current.group_id,
                    target_provider_id=current.promoted_provider_id,
                    grant_id=current.grant_id,
                    term=current.term,
                    fencing_token=current.fencing_token,
                    lease_expires_at=_utc(self.clock()) + timedelta(seconds=self.lease_seconds),
                    occurred_at=self.clock(),
                )
                snapshot = self.control_plane.snapshot(current.session_id)
                assignment = snapshot.groups[current.group_id]
            if assignment.primary_provider_id != current.promoted_provider_id:
                return self._fail(
                    current,
                    "concurrent-primary-change",
                    "another authority changed the primary during automatic failover",
                )
            desired_replicas = current.retained_replica_provider_ids
            if tuple(assignment.replica_provider_ids) != desired_replicas:
                self.control_plane.change_assignment(
                    current.session_id,
                    self.credentials.identity.node_id,
                    current.group_id,
                    current.promoted_provider_id,
                    desired_replicas,
                )
            if (
                self.control_plane.acknowledgement_policy(
                    current.session_id, current.group_id
                ).mode
                is not current.effective_acknowledgement_mode
            ):
                self.control_plane.set_acknowledgement_policy(
                    current.session_id,
                    current.group_id,
                    current.effective_acknowledgement_mode,
                )
            if (
                current.effective_acknowledgement_mode
                is not current.previous_acknowledgement_mode
            ):
                manifest = self.control_plane.manifest(
                    current.session_id, current.group_id
                )
                self.control_plane.set_storage_degraded_state(
                    current.session_id,
                    current.group_id,
                    manifest=manifest,
                    reason_code="automatic-failover-redundancy-lost",
                    reason_detail="writes continue with a reduced acknowledgement policy until redundancy is restored",
                    missing_ranges=(),
                    obligations={
                        "action": "restore-replication-and-original-acknowledgement-policy",
                        "previous_acknowledgement_mode": current.previous_acknowledgement_mode.value,
                        "failed_provider_id": current.failed_provider_id,
                    },
                    now=self.clock(),
                )
            current = replace(
                current,
                state="control-committed",
                updated_at=_utc(self.clock()),
            )
            self.failover_store.save(current)

        if current.state == "control-committed":
            plan = current.publication
            if plan is None:
                plan = self.publication_store.issue(
                    self.control_plane,
                    self.credentials,
                    current.session_id,
                    now=self.clock(),
                )
                current = replace(
                    current,
                    publication=plan,
                    updated_at=_utc(self.clock()),
                )
                self.failover_store.save(current)
            snapshot = self.control_plane.snapshot(current.session_id)
            target_provider_ids = (
                current.promoted_provider_id,
                *current.retained_replica_provider_ids,
            )
            target_nodes: list[str] = []
            for provider_id in target_provider_ids:
                provider = snapshot.providers.get(provider_id)
                if provider is None or not self._provider_online(
                    provider_id, provider.node_id, status
                ):
                    raise FederationValidationError(
                        "failover-publication-target-unavailable",
                        "provider_id",
                        f"provider {provider_id} became unavailable before control publication",
                    )
                target_nodes.append(provider.node_id)
            await self.channel.publish(plan, tuple(target_nodes))
            current = replace(
                current,
                state="published",
                updated_at=_utc(self.clock()),
            )
            self.failover_store.save(current)

        if current.state == "failed":
            return LiveFailoverResult(
                "failed",
                current.session_id,
                current.group_id,
                current.failed_provider_id,
                current.promoted_provider_id,
                current.reason_detail,
                term=current.term,
                fencing_token=current.fencing_token,
            )
        return LiveFailoverResult(
            "promoted",
            current.session_id,
            current.group_id,
            current.failed_provider_id,
            current.promoted_provider_id,
            "complete replica promoted and signed control publication acknowledged",
            publication_revision=(
                None if current.publication is None else current.publication.publication_revision
            ),
            term=current.term,
            fencing_token=current.fencing_token,
        )

    def _fail(
        self,
        record: LiveFailoverRecord,
        code: str,
        detail: str,
    ) -> LiveFailoverResult:
        failed = replace(
            record,
            state="failed",
            reason_code=code,
            reason_detail=detail,
            updated_at=_utc(self.clock()),
        )
        self.failover_store.save(failed)
        return LiveFailoverResult(
            "failed",
            failed.session_id,
            failed.group_id,
            failed.failed_provider_id,
            failed.promoted_provider_id,
            detail,
            term=failed.term,
            fencing_token=failed.fencing_token,
        )

    def _status_snapshot(self) -> dict[str, Any]:
        result = {"sessions": [], "nodes": [], "capabilities": []}
        cursor: str | None = None
        for _ in range(1_024):
            page = self.session_coordinator.status(
                actor_node_id=self.credentials.identity.node_id,
                cursor=cursor,
            )
            for section in result:
                values = page.get(section)
                if isinstance(values, list):
                    result[section].extend(values)
            pagination = page.get("pagination")
            if not isinstance(pagination, dict) or not pagination.get("has_more"):
                return result
            cursor = pagination.get("next_cursor")
            if not isinstance(cursor, str) or not cursor:
                break
        raise FederationValidationError(
            "coordinator-status-pagination-failed",
            "status",
            "could not read a complete coordinator status snapshot",
        )

    @staticmethod
    def _provider_online(
        provider_id: str,
        node_id: str,
        status: dict[str, Any],
    ) -> bool:
        nodes = {
            item.get("node_id"): item
            for item in status.get("nodes", ())
            if isinstance(item, dict)
        }
        node = nodes.get(node_id)
        if not isinstance(node, dict) or node.get("connection_state") != "connected":
            return False
        return any(
            isinstance(item, dict)
            and item.get("node_id") == node_id
            and item.get("type") == "storage-provider"
            and item.get("status") == "ready"
            and isinstance(item.get("properties"), dict)
            and item["properties"].get("provider_id") == provider_id
            for item in status.get("capabilities", ())
        )

    def _eligible_candidate(
        self,
        provider_id: str,
        providers: dict[str, Any],
        status: dict[str, Any],
        manifest: Any,
    ) -> bool:
        provider = providers.get(provider_id)
        return bool(
            provider is not None
            and provider.assignable
            and self._provider_online(provider_id, provider.node_id, status)
            and all(
                provider_id in item.acknowledged_provider_ids
                for item in manifest.items
            )
        )

    @staticmethod
    def _available_mode(
        requested: AcknowledgementMode,
        replica_count: int,
    ) -> AcknowledgementMode:
        from .acknowledgement import AcknowledgementPolicy

        try:
            AcknowledgementPolicy(requested).required_replica_acks(replica_count)
        except FederationValidationError:
            return AcknowledgementMode.PRIMARY
        return requested

    @staticmethod
    def _failover_id(
        session_id: str,
        group_id: str,
        failed_provider_id: str,
        promoted_provider_id: str,
        source_control_revision: int,
    ) -> str:
        value = (
            f"{session_id}\0{group_id}\0{failed_provider_id}\0"
            f"{promoted_provider_id}\0{source_control_revision}"
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(value).hexdigest()
