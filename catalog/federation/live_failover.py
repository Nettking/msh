"""Automatic relay-aware failover for live storage providers.

The relay connection is the only application route to a storage node. When it
is lost, the coordinator sees the node and its capabilities become unavailable,
while the node closes a provider-local write gate before reconnecting. F3 then
promotes only an online assigned replica that returns fresh, authenticated proof
that every authoritative manifest item is still present and intact.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from catalog.node.identity import NodeCredentials

from .acknowledgement import AcknowledgementMode, AcknowledgementPolicy
from .control_sync import (
    STORAGE_CONTROL_RELAY_KIND,
    StorageControlPlan,
    StorageControlPublicationStore,
)
from .errors import FederationValidationError
from .manifest import AuthoritativeStorageManifest, ManifestItemKind
from .phase_d_control import PhaseDControlPlane
from .reporting import (
    REPORT_PROTOCOL_VERSION,
    REPORT_SCHEMA,
    StorageReplicaAssessment,
    StorageReplicaReport,
)
from .selection import select_storage_promotion_candidate
from .storage_protocol import BatchIngestRequest

STORAGE_CONTROL_REFRESH_MESSAGE = "refresh-request"
STORAGE_FAILOVER_RELAY_KIND = "fcp-storage-failover-v1"
LIVE_FAILOVER_SCHEMA = "fcp.live_storage_failover.v1"
LIVE_FAILOVER_RESULT_SCHEMA = "fcp.live_storage_failover_result.v1"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _utc(value: datetime | str, field: str = "timestamp") -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp", field, "must be RFC 3339"
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise FederationValidationError(
            "invalid-timestamp", field, "must be timezone-aware"
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


def _positive(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise FederationValidationError(
            "invalid-live-failover-counter", field, "must be a positive integer"
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


class BatchEvidenceProvider(Protocol):
    def committed_identity(
        self,
        *,
        session_id: str,
        group_id: str,
        batch_id: str,
    ): ...

    def read(self, *, session_id: str, group_id: str, batch_id: str): ...

    def health(self) -> dict[str, object]: ...


def build_live_replica_report(
    provider: BatchEvidenceProvider,
    manifest: AuthoritativeStorageManifest,
    *,
    provider_id: str,
    report_revision: int,
    reported_at: datetime,
) -> StorageReplicaReport:
    """Verify authoritative batches against durable identity and file content."""

    reasons: list[str] = []
    committed_hashes: list[str] = []
    for item in manifest.items:
        if item.kind is not ManifestItemKind.BATCH:
            reasons.append("unsupported-authoritative-item")
            continue
        identity = provider.committed_identity(
            session_id=manifest.session_id,
            group_id=manifest.group_id,
            batch_id=item.item_id,
        )
        if identity is None:
            reasons.append("missing-authoritative-batch")
            continue
        if (
            identity.dataset_id != item.dataset_id
            or identity.dataset_schema_name != item.schema_name
            or identity.dataset_schema_version != item.schema_version
            or identity.idempotency_key != item.idempotency_key
            or identity.content_hash != item.content_hash
        ):
            reasons.append("committed-hash-conflict")
            continue
        content = provider.read(
            session_id=manifest.session_id,
            group_id=manifest.group_id,
            batch_id=item.item_id,
        )
        if BatchIngestRequest.calculate_content_hash(content) != item.content_hash:
            reasons.append("provider-content-integrity-failure")
            continue
        committed_hashes.append(identity.content_hash)
    health = provider.health()
    if health.get("status") != "ready":
        reasons.append("provider-unavailable")
    reasons = list(dict.fromkeys(reasons))
    complete = not reasons and len(committed_hashes) == len(manifest.items)
    if not complete and not reasons:
        reasons.append("manifest-item-count-mismatch")
    return StorageReplicaReport(
        schema=REPORT_SCHEMA,
        protocol_version=REPORT_PROTOCOL_VERSION,
        session_id=manifest.session_id,
        group_id=manifest.group_id,
        provider_id=_text(provider_id, "provider_id"),
        report_revision=_positive(report_revision, "report_revision"),
        manifest_revision=manifest.revision,
        manifest_hash=manifest.manifest_hash,
        committed_item_hashes=tuple(committed_hashes),
        datasets=manifest.datasets,
        integrity_verified=complete,
        synchronization_state="synchronized" if complete else "incomplete",
        eligible=complete,
        eligibility_reasons=("eligible",) if complete else tuple(reasons),
        reported_at=_utc(reported_at, "reported_at"),
    )


@dataclass(frozen=True)
class LiveFailoverRecord:
    schema: str
    failover_id: str
    session_id: str
    group_id: str
    failed_provider_id: str
    failed_node_id: str
    source_grant_id: str
    source_term: int
    source_fencing_token: int
    promoted_provider_id: str
    promoted_node_id: str
    selected_report_revision: int
    selected_report_hash: str
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
                "unsupported-live-failover",
                "schema",
                f"expected {LIVE_FAILOVER_SCHEMA}",
            )
        for field in (
            "failover_id",
            "session_id",
            "group_id",
            "failed_provider_id",
            "failed_node_id",
            "source_grant_id",
            "promoted_provider_id",
            "promoted_node_id",
            "selected_report_hash",
            "grant_id",
            "reason_code",
            "reason_detail",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        replicas = tuple(
            sorted(
                _text(value, "retained_replica_provider_ids")
                for value in self.retained_replica_provider_ids
            )
        )
        if (
            len(replicas) != len(set(replicas))
            or self.promoted_provider_id in replicas
            or self.failed_provider_id in replicas
        ):
            raise FederationValidationError(
                "invalid-live-failover-replicas",
                "retained_replica_provider_ids",
                "replicas must be unique and exclude failed and promoted providers",
            )
        object.__setattr__(self, "retained_replica_provider_ids", replicas)
        for field in (
            "source_term",
            "source_fencing_token",
            "selected_report_revision",
            "source_control_revision",
            "term",
            "fencing_token",
        ):
            object.__setattr__(self, field, _positive(getattr(self, field), field))
        if self.term <= self.source_term:
            raise FederationValidationError(
                "stale-live-failover-term", "term", "must exceed source term"
            )
        if self.fencing_token <= self.source_fencing_token:
            raise FederationValidationError(
                "stale-live-failover-token",
                "fencing_token",
                "must exceed source fencing token",
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
                "invalid-live-failover-mode",
                "acknowledgement_mode",
                "unknown mode",
            ) from exc
        if self.state not in {
            "detected",
            "control-committed",
            "published",
            "failed",
        }:
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
            "source_grant_id": self.source_grant_id,
            "source_term": self.source_term,
            "source_fencing_token": self.source_fencing_token,
            "promoted_provider_id": self.promoted_provider_id,
            "promoted_node_id": self.promoted_node_id,
            "selected_report_revision": self.selected_report_revision,
            "selected_report_hash": self.selected_report_hash,
            "retained_replica_provider_ids": list(
                self.retained_replica_provider_ids
            ),
            "previous_acknowledgement_mode": (
                self.previous_acknowledgement_mode.value
            ),
            "effective_acknowledgement_mode": (
                self.effective_acknowledgement_mode.value
            ),
            "source_control_revision": self.source_control_revision,
            "term": self.term,
            "fencing_token": self.fencing_token,
            "grant_id": self.grant_id,
            "state": self.state,
            "publication": (
                None if self.publication is None else self.publication.to_dict()
            ),
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
            source_grant_id=value.get("source_grant_id"),
            source_term=value.get("source_term"),
            source_fencing_token=value.get("source_fencing_token"),
            promoted_provider_id=value.get("promoted_provider_id"),
            promoted_node_id=value.get("promoted_node_id"),
            selected_report_revision=value.get("selected_report_revision"),
            selected_report_hash=value.get("selected_report_hash"),
            retained_replica_provider_ids=tuple(
                value.get("retained_replica_provider_ids", ())
            ),
            previous_acknowledgement_mode=value.get(
                "previous_acknowledgement_mode"
            ),
            effective_acknowledgement_mode=value.get(
                "effective_acknowledgement_mode"
            ),
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
                   ON storage_live_failovers(
                       session_id, group_id, state, updated_at
                   )"""
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
            record = LiveFailoverRecord.from_dict(
                json.loads(row["record_json"])
            )
        except (json.JSONDecodeError, FederationValidationError) as exc:
            raise FederationValidationError(
                "corrupt-live-failover",
                "record_json",
                "persisted failover is malformed",
            ) from exc
        if (
            row["session_id"] != record.session_id
            or row["group_id"] != record.group_id
            or row["failover_id"] != record.failover_id
            or row["state"] != record.state
            or row["updated_at"] != _stamp(record.updated_at)
        ):
            raise FederationValidationError(
                "live-failover-row-mismatch",
                "record_json",
                "indexed metadata differs from its canonical record",
            )
        return record

    def get(
        self,
        session_id: str,
        group_id: str,
        failover_id: str,
    ) -> LiveFailoverRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM storage_live_failovers
                   WHERE session_id=? AND group_id=? AND failover_id=?""",
                (session_id, group_id, failover_id),
            ).fetchone()
        return None if row is None else self._decode(row)

    def active(
        self,
        session_id: str,
        group_id: str,
    ) -> LiveFailoverRecord | None:
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
        existing = self.get(
            record.session_id, record.group_id, record.failover_id
        )
        if existing is not None:
            immutable = (
                "failed_provider_id",
                "failed_node_id",
                "source_grant_id",
                "source_term",
                "source_fencing_token",
                "promoted_provider_id",
                "promoted_node_id",
                "selected_report_revision",
                "selected_report_hash",
                "retained_replica_provider_ids",
                "previous_acknowledgement_mode",
                "effective_acknowledgement_mode",
                "source_control_revision",
                "term",
                "fencing_token",
                "grant_id",
            )
            if any(
                getattr(existing, field) != getattr(record, field)
                for field in immutable
            ):
                raise FederationValidationError(
                    "conflicting-live-failover",
                    "failover_id",
                    "durable identity was reused with different inputs",
                )
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO storage_live_failovers
                   (session_id, group_id, failover_id, state,
                    record_json, updated_at)
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


@dataclass
class _PendingReport:
    failover_id: str
    session_id: str
    provider_id: str
    node_id: str
    report_revision: int
    manifest_revision: int
    manifest_hash: str
    future: asyncio.Future[StorageReplicaReport]


class StorageControlRelayChannel:
    """Demultiplex control, refresh, and failover evidence on one relay queue."""

    def __init__(self, client: Any, endpoint: Any, *, timeout: float = 15.0) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.client = client
        self.endpoint = endpoint
        self.timeout = float(timeout)
        self._pending: dict[str, _PendingPublication] = {}
        self._pending_reports: dict[tuple[str, str], _PendingReport] = {}
        self._receiver_task: asyncio.Task[None] | None = None
        self._refresh_tasks: set[asyncio.Task[None]] = set()
        self._refresh_handler: (
            Callable[[str, str, dict[str, Any]], Awaitable[None]] | None
        ) = None

    def set_refresh_handler(
        self,
        handler: Callable[[str, str, dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._refresh_handler = handler

    async def start(self) -> None:
        if self._receiver_task is not None:
            return
        self._receiver_task = asyncio.create_task(
            self._receiver_loop(),
            name=f"fcp-storage-control-channel-{self.client.node_id}",
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
        error = FederationValidationError(
            "storage-control-channel-closed",
            "channel",
            "relay channel closed before its operation completed",
        )
        for pending in tuple(self._pending.values()):
            if not pending.future.done():
                pending.future.set_exception(error)
        for pending in tuple(self._pending_reports.values()):
            if not pending.future.done():
                pending.future.set_exception(error)
        self._pending.clear()
        self._pending_reports.clear()

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
        targets = tuple(
            dict.fromkeys(
                _text(value, "target_node_id") for value in target_node_ids
            )
        )
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
                    request_id=(
                        f"control-{plan.publication_id}-{target}-"
                        f"{uuid.uuid4().hex}"
                    ),
                    payload={
                        "kind": STORAGE_CONTROL_RELAY_KIND,
                        "message": "plan",
                        "frame": frame,
                    },
                )
                if (
                    not isinstance(delivery, dict)
                    or delivery.get("delivered") is not True
                ):
                    raise FederationValidationError(
                        "storage-control-delivery-failed",
                        "target_node_id",
                        f"relay did not confirm delivery to {target}",
                    )
            return await asyncio.wait_for(pending.future, timeout=self.timeout)
        finally:
            self._pending.pop(plan.publication_id, None)

    async def request_replica_report(
        self,
        *,
        failover_id: str,
        manifest: AuthoritativeStorageManifest,
        provider_id: str,
        node_id: str,
        report_revision: int,
        reported_at: datetime,
    ) -> StorageReplicaReport:
        if self._receiver_task is None:
            raise FederationValidationError(
                "storage-control-channel-not-started",
                "channel",
                "control channel must be started before report requests",
            )
        key = (failover_id, provider_id)
        if key in self._pending_reports:
            raise FederationValidationError(
                "duplicate-live-replica-report-request",
                "provider_id",
                "candidate report request is already in flight",
            )
        loop = asyncio.get_running_loop()
        pending = _PendingReport(
            failover_id=failover_id,
            session_id=manifest.session_id,
            provider_id=provider_id,
            node_id=node_id,
            report_revision=report_revision,
            manifest_revision=manifest.revision,
            manifest_hash=manifest.manifest_hash,
            future=loop.create_future(),
        )
        self._pending_reports[key] = pending
        try:
            delivery = await self.client.send_message(
                session_id=manifest.session_id,
                target_node_id=node_id,
                request_id=(
                    f"failover-report-{failover_id}-{provider_id}-"
                    f"{uuid.uuid4().hex}"
                ),
                payload={
                    "kind": STORAGE_FAILOVER_RELAY_KIND,
                    "message": "report-request",
                    "failover_id": failover_id,
                    "provider_id": provider_id,
                    "report_revision": report_revision,
                    "reported_at": _stamp(reported_at),
                    "manifest_frame": _json(manifest.to_dict()),
                },
            )
            if (
                not isinstance(delivery, dict)
                or delivery.get("delivered") is not True
            ):
                raise FederationValidationError(
                    "live-replica-report-delivery-failed",
                    "provider_id",
                    "relay did not confirm report request delivery",
                )
            return await asyncio.wait_for(pending.future, timeout=self.timeout)
        finally:
            self._pending_reports.pop(key, None)

    async def _receiver_loop(self) -> None:
        while True:
            message = await self.endpoint.receive_other()
            payload = getattr(message, "payload", None)
            if not isinstance(payload, dict):
                continue
            kind = payload.get("kind")
            message_kind = payload.get("message")
            if kind == STORAGE_CONTROL_RELAY_KIND:
                if message_kind == "response":
                    self._accept_response(message, payload)
                elif message_kind == STORAGE_CONTROL_REFRESH_MESSAGE:
                    self._accept_refresh(message, payload)
            elif (
                kind == STORAGE_FAILOVER_RELAY_KIND
                and message_kind == "report-response"
            ):
                self._accept_report(message, payload)

    def _accept_refresh(self, message: Any, payload: dict[str, Any]) -> None:
        handler = self._refresh_handler
        actor = getattr(message, "actor_node_id", None)
        session_id = getattr(message, "session_id", None)
        if (
            handler is None
            or not isinstance(actor, str)
            or not isinstance(session_id, str)
        ):
            return
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
            if not pending.future.done():
                pending.future.set_exception(
                    FederationValidationError(
                        "storage-control-target-rejected",
                        "target_node_id",
                        f"{actor}: {payload.get('error')}",
                    )
                )
            return
        pending.pending_node_ids.remove(actor)
        pending.acknowledged_node_ids.add(actor)
        if not pending.pending_node_ids and not pending.future.done():
            pending.future.set_result(
                tuple(sorted(pending.acknowledged_node_ids))
            )

    def _accept_report(self, message: Any, payload: dict[str, Any]) -> None:
        failover_id = payload.get("failover_id")
        provider_id = payload.get("provider_id")
        pending = self._pending_reports.get((failover_id, provider_id))
        if pending is None:
            return
        if (
            getattr(message, "actor_node_id", None) != pending.node_id
            or getattr(message, "session_id", None) != pending.session_id
        ):
            return
        if payload.get("status") != "reported":
            if not pending.future.done():
                pending.future.set_exception(
                    FederationValidationError(
                        "live-replica-report-rejected",
                        "provider_id",
                        str(payload.get("error")),
                    )
                )
            return
        try:
            report = StorageReplicaReport.from_dict(payload.get("report"))
        except FederationValidationError as exc:
            if not pending.future.done():
                pending.future.set_exception(exc)
            return
        if (
            report.provider_id != pending.provider_id
            or report.report_revision != pending.report_revision
            or report.manifest_revision != pending.manifest_revision
            or report.manifest_hash != pending.manifest_hash
        ):
            if not pending.future.done():
                pending.future.set_exception(
                    FederationValidationError(
                        "live-replica-report-mismatch",
                        "report",
                        "report is not bound to the requested manifest",
                    )
                )
            return
        if not pending.future.done():
            pending.future.set_result(report)


class StorageFailoverCoordinator:
    """Detect unavailable primaries and publish verified replacement authority."""

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

    async def publish_current(
        self, target_node_ids: tuple[str, ...]
    ) -> StorageControlPlan:
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
        for group_id, assignment in snapshot.groups.items():
            if assignment.primary_provider_id != provider_id:
                continue
            active = self.failover_store.active(self.session_id, group_id)
            if (
                active is not None
                and active.failed_provider_id == provider_id
                and active.state == "detected"
            ):
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
            grant = snapshot.leader_grants.get(group_id)
            if failed_provider is None or grant is None:
                continue
            if self._provider_online(
                failed_provider_id, failed_provider.node_id, status
            ):
                continue
            manifest = self.control_plane.manifest(self.session_id, group_id)
            observation_id = self._observation_id(
                self.session_id,
                group_id,
                failed_provider_id,
                str(grant["grant_id"]),
            )
            assessments = await self._candidate_assessments(
                observation_id,
                assignment.replica_provider_ids,
                snapshot.providers,
                status,
                manifest,
            )
            selection = select_storage_promotion_candidate(
                manifest,
                assessments,
                current_primary_provider_id=failed_provider_id,
            )
            if selection.selected_provider_id is None:
                reason = "no connected replica has complete live manifest proof"
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
            promoted = selection.selected_provider_id
            assessment = next(
                item for item in assessments if item.provider_id == promoted
            )
            promoted_provider = snapshot.providers[promoted]
            retained = tuple(
                provider_id
                for provider_id in selection.candidate_provider_ids
                if provider_id != promoted
            )
            prior_mode = self.control_plane.acknowledgement_policy(
                self.session_id, group_id
            ).mode
            effective_mode = self._available_mode(prior_mode, len(retained))
            source_term = int(grant["term"])
            source_token = int(grant["fencing_token"])
            term = source_term + 1
            token = source_token + 1
            failover_id = self._failover_id(
                observation_id,
                promoted,
                assessment.report_hash,
            )
            detected_at = _utc(self.clock(), "detected_at")
            record = LiveFailoverRecord(
                schema=LIVE_FAILOVER_SCHEMA,
                failover_id=failover_id,
                session_id=self.session_id,
                group_id=group_id,
                failed_provider_id=failed_provider_id,
                failed_node_id=failed_provider.node_id,
                source_grant_id=str(grant["grant_id"]),
                source_term=source_term,
                source_fencing_token=source_token,
                promoted_provider_id=promoted,
                promoted_node_id=promoted_provider.node_id,
                selected_report_revision=assessment.report_revision,
                selected_report_hash=assessment.report_hash,
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
                reason_detail=(
                    "primary node is disconnected or its storage capability "
                    "is unavailable"
                ),
                detected_at=detected_at,
                updated_at=detected_at,
            )
            self.failover_store.save(record)
            results.append(await self._resume(record, status))
        return tuple(results)

    async def _candidate_assessments(
        self,
        observation_id: str,
        provider_ids: tuple[str, ...],
        providers: dict[str, Any],
        status: dict[str, Any],
        manifest: AuthoritativeStorageManifest,
    ) -> tuple[StorageReplicaAssessment, ...]:
        assessments: list[StorageReplicaAssessment] = []
        for provider_id in sorted(provider_ids):
            provider = providers.get(provider_id)
            if (
                provider is None
                or not provider.assignable
                or not self._provider_online(
                    provider_id, provider.node_id, status
                )
            ):
                continue
            previous = self.control_plane.latest_storage_replica_assessment(
                self.session_id, manifest.group_id, provider_id
            )
            revision = 1 if previous is None else previous.report_revision + 1
            try:
                report = await self.channel.request_replica_report(
                    failover_id=observation_id,
                    manifest=manifest,
                    provider_id=provider_id,
                    node_id=provider.node_id,
                    report_revision=revision,
                    reported_at=_utc(self.clock(), "reported_at"),
                )
                assessment = self.control_plane.submit_storage_replica_report(
                    report,
                    actor_node_id=provider.node_id,
                )
            except (FederationValidationError, TimeoutError):
                continue
            assessments.append(assessment)
        return tuple(assessments)

    async def _resume(
        self,
        record: LiveFailoverRecord,
        status: dict[str, Any],
    ) -> LiveFailoverResult:
        current = record
        if current.state == "detected":
            manifest = self.control_plane.manifest(
                current.session_id, current.group_id
            )
            assessment = self.control_plane.latest_storage_replica_assessment(
                current.session_id,
                current.group_id,
                current.promoted_provider_id,
            )
            if (
                not self._assessment_current(assessment, manifest)
                or assessment.report_revision
                != current.selected_report_revision
                or assessment.report_hash != current.selected_report_hash
            ):
                return self._fail(
                    current,
                    "stale-live-replica-report",
                    "selected replica proof changed before authority commit",
                )
            snapshot = self.control_plane.snapshot(current.session_id)
            assignment = snapshot.groups.get(current.group_id)
            grant = snapshot.leader_grants.get(current.group_id)
            if assignment is None:
                return self._fail(
                    current,
                    "failover-group-disappeared",
                    "storage group no longer exists",
                )
            if assignment.primary_provider_id == current.failed_provider_id:
                if (
                    grant is None
                    or grant.get("grant_id") != current.source_grant_id
                    or int(grant.get("term", -1)) != current.source_term
                    or int(grant.get("fencing_token", -1))
                    != current.source_fencing_token
                ):
                    return self._fail(
                        current,
                        "source-authority-changed",
                        "source grant changed after failure detection",
                    )
                self.control_plane.complete_handover(
                    session_id=current.session_id,
                    actor_node_id=self.credentials.identity.node_id,
                    group_id=current.group_id,
                    target_provider_id=current.promoted_provider_id,
                    grant_id=current.grant_id,
                    term=current.term,
                    fencing_token=current.fencing_token,
                    lease_expires_at=(
                        _utc(self.clock())
                        + timedelta(seconds=self.lease_seconds)
                    ),
                    occurred_at=self.clock(),
                )
                snapshot = self.control_plane.snapshot(current.session_id)
                assignment = snapshot.groups[current.group_id]
                grant = snapshot.leader_grants.get(current.group_id)
            if (
                assignment.primary_provider_id != current.promoted_provider_id
                or grant is None
                or grant.get("grant_id") != current.grant_id
                or int(grant.get("term", -1)) != current.term
                or int(grant.get("fencing_token", -1))
                != current.fencing_token
            ):
                return self._fail(
                    current,
                    "concurrent-primary-change",
                    "another authority changed the primary during failover",
                )
            if (
                tuple(assignment.replica_provider_ids)
                != current.retained_replica_provider_ids
            ):
                self.control_plane.change_assignment(
                    current.session_id,
                    self.credentials.identity.node_id,
                    current.group_id,
                    current.promoted_provider_id,
                    current.retained_replica_provider_ids,
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
            self.control_plane.set_storage_degraded_state(
                current.session_id,
                current.group_id,
                manifest=manifest,
                reason_code="automatic-failover-redundancy-lost",
                reason_detail=(
                    "writes continue with available replicas while failed "
                    "storage is excluded until verified catch-up"
                ),
                missing_ranges=(),
                obligations={
                    "action": "restore-replication-and-acknowledgement-policy",
                    "previous_acknowledgement_mode": (
                        current.previous_acknowledgement_mode.value
                    ),
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
                        f"provider {provider_id} became unavailable",
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
            "live-verified replica promoted and signed control acknowledged",
            publication_revision=(
                None
                if current.publication is None
                else current.publication.publication_revision
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
            if (
                not isinstance(pagination, dict)
                or not pagination.get("has_more")
            ):
                return result
            cursor = pagination.get("next_cursor")
            if not isinstance(cursor, str) or not cursor:
                break
        raise FederationValidationError(
            "coordinator-status-pagination-failed",
            "status",
            "could not read a complete status snapshot",
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
        if (
            not isinstance(node, dict)
            or node.get("connection_state") != "connected"
            or node.get("revoked") is True
        ):
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

    @staticmethod
    def _assessment_current(
        assessment: StorageReplicaAssessment | None,
        manifest: AuthoritativeStorageManifest,
    ) -> bool:
        return bool(
            assessment is not None
            and assessment.accepted
            and assessment.eligibility
            and assessment.report is not None
            and assessment.report.manifest_revision == manifest.revision
            and assessment.report.manifest_hash == manifest.manifest_hash
        )

    @staticmethod
    def _available_mode(
        requested: AcknowledgementMode,
        replica_count: int,
    ) -> AcknowledgementMode:
        try:
            AcknowledgementPolicy(requested).required_replica_acks(replica_count)
        except FederationValidationError:
            return AcknowledgementMode.PRIMARY
        return requested

    @staticmethod
    def _observation_id(
        session_id: str,
        group_id: str,
        failed_provider_id: str,
        source_grant_id: str,
    ) -> str:
        value = (
            f"{session_id}\0{group_id}\0{failed_provider_id}\0"
            f"{source_grant_id}"
        ).encode()
        return "sha256:" + hashlib.sha256(value).hexdigest()

    @staticmethod
    def _failover_id(
        observation_id: str,
        promoted_provider_id: str,
        report_hash: str,
    ) -> str:
        value = (
            f"{observation_id}\0{promoted_provider_id}\0{report_hash}"
        ).encode()
        return "sha256:" + hashlib.sha256(value).hexdigest()