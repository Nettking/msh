"""Workload-drain primitives for rolling Federation software updates.

An update drain is a scheduling fence, not a cancellation mechanism.  The node
continues to own and execute work it already holds while its providers become
ineligible for new selection.  Replacement may begin only after durable job
ownership proves that every provider identity belonging to the node is
quiescent.

This module intentionally does not own update orchestration or activation.  It
provides the small domain boundary those layers need: persist the exact bounded
provider identity set being drained, convert READY reports to DRAINING without
changing live-work metrics, and query the existing F7 durable job store for
active ownership held by that set.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from catalog.federation.errors import FederationValidationError
from catalog.federation.host_resources import ProcessResourceAdmission
from catalog.federation.process_resource_admission import PROCESS_RESOURCE_ADMISSION

from .job_store import SQLiteJobStore
from .provider_reports import ProviderResourceReport, ProviderStatus

MAX_DRAIN_PROVIDER_IDS = 128
_MAX_TEXT_BYTES = 512
UPDATE_DRAIN_STORE_SCHEMA_VERSION = 1
UPDATE_DRAIN_INITIALIZATION_BYTES = 2 * 1024 * 1024
UPDATE_DRAIN_MUTATION_BYTES = 1024 * 1024
UPDATE_DRAIN_MUTATION_INODES = 4


def _text(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-text",
            field,
            "must be non-empty trimmed text without control characters",
        )
    if len(value.encode("utf-8")) > _MAX_TEXT_BYTES:
        raise FederationValidationError(
            "text-too-large",
            field,
            f"must not exceed {_MAX_TEXT_BYTES} UTF-8 bytes",
        )
    return value


def _utc(value: object, field: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset().total_seconds() != 0
    ):
        raise FederationValidationError(
            "invalid-timestamp", field, "must be a timezone-aware UTC datetime"
        )
    return value.astimezone(timezone.utc)


def _stamp(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


def _positive_revision(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FederationValidationError(
            "invalid-positive-integer", field, "must be a positive integer"
        )
    return value


@dataclass(frozen=True)
class NodeUpdateDrainTarget:
    """The durable provider identities that must quiesce before one node switches."""

    node_id: str
    provider_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_id", _text(self.node_id, "node_id"))
        provider_ids = tuple(
            _text(value, f"provider_ids[{index}]")
            for index, value in enumerate(self.provider_ids)
        )
        if len(provider_ids) > MAX_DRAIN_PROVIDER_IDS:
            raise FederationValidationError(
                "too-many-drain-providers",
                "provider_ids",
                f"must not exceed {MAX_DRAIN_PROVIDER_IDS} provider identities",
            )
        if len(provider_ids) != len(set(provider_ids)):
            raise FederationValidationError(
                "duplicate-drain-provider",
                "provider_ids",
                "must not contain duplicate provider identities",
            )
        object.__setattr__(self, "provider_ids", tuple(sorted(provider_ids)))

    def apply_to_report(self, report: ProviderResourceReport) -> ProviderResourceReport:
        """Fence new scheduling while preserving the provider's live-work metrics.

        Only an actually READY provider becomes DRAINING.  An unavailable,
        disabled or revoked provider must keep its stronger state instead of
        being made to look merely drained.  Reports outside this node/provider
        set are returned unchanged.
        """

        if not isinstance(report, ProviderResourceReport):
            raise FederationValidationError(
                "invalid-provider-report",
                "report",
                "must be a ProviderResourceReport",
            )
        if report.node_id != self.node_id or report.capability_id not in self.provider_ids:
            return report
        if report.status is ProviderStatus.DRAINING:
            return report
        if report.status is not ProviderStatus.READY:
            return report
        return replace(report, status=ProviderStatus.DRAINING)

    def active_ownership_count(self, store: SQLiteJobStore) -> int:
        """Count active F7 ownership for this node's providers from durable state.

        This is deliberately a single bounded aggregate query.  It does not
        enumerate job history, trust worker-local counters or infer quiescence
        from provider-health freshness.  A stale/missing health report therefore
        cannot hide a lease that still exists in the authoritative job store.
        """

        if not isinstance(store, SQLiteJobStore):
            raise FederationValidationError(
                "invalid-job-store",
                "store",
                "must be a SQLiteJobStore",
            )
        if not self.provider_ids:
            return 0
        placeholders = ",".join("?" for _ in self.provider_ids)
        with store._connect() as connection:  # package-internal read-only seam
            row = connection.execute(
                f"""SELECT COUNT(*) AS active_count
                    FROM capability_jobs
                    WHERE active_attempt_id IS NOT NULL
                      AND active_owner_provider_id IN ({placeholders})""",
                self.provider_ids,
            ).fetchone()
        assert row is not None
        return int(row["active_count"])

    def is_quiescent(self, store: SQLiteJobStore) -> bool:
        """Return true only when no durable active attempt belongs to this node."""

        return self.active_ownership_count(store) == 0


class NodeUpdateDrainState(str, Enum):
    """Persistent rolling-update scheduling state for one Federation member."""

    READY = "ready"
    DRAINING = "draining"


@dataclass(frozen=True)
class NodeUpdateDrainRecord:
    """One restart-safe drain decision and its exact provider identity set."""

    session_id: str
    target: NodeUpdateDrainTarget
    state: NodeUpdateDrainState
    revision: int
    updated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "session_id", _text(self.session_id, "session_id"))
        if not isinstance(self.target, NodeUpdateDrainTarget):
            raise FederationValidationError(
                "invalid-update-drain-target",
                "target",
                "must be a NodeUpdateDrainTarget",
            )
        try:
            object.__setattr__(self, "state", NodeUpdateDrainState(self.state))
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-update-drain-state",
                "state",
                "unknown update-drain state",
            ) from exc
        object.__setattr__(
            self,
            "revision",
            _positive_revision(self.revision, "revision"),
        )
        object.__setattr__(self, "updated_at", _utc(self.updated_at, "updated_at"))

    @property
    def node_id(self) -> str:
        return self.target.node_id

    @property
    def provider_ids(self) -> tuple[str, ...]:
        return self.target.provider_ids


@dataclass(frozen=True)
class NodeUpdateDrainMutation:
    record: NodeUpdateDrainRecord
    changed: bool


class SQLiteNodeUpdateDrainStore:
    """Durable node drain intent; job ownership stays in the existing F7 store."""

    def __init__(
        self,
        database: Path | str,
        *,
        resource_admission: ProcessResourceAdmission | None = None,
    ) -> None:
        self.database = str(database)
        self.resource_admission = resource_admission or PROCESS_RESOURCE_ADMISSION
        with self._resource_reservation(
            bytes_required=UPDATE_DRAIN_INITIALIZATION_BYTES,
            inodes_required=UPDATE_DRAIN_MUTATION_INODES,
        ):
            Path(self.database).parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS node_update_drain_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    INSERT OR IGNORE INTO node_update_drain_meta(key, value)
                        VALUES ('schema_version', '1');

                    CREATE TABLE IF NOT EXISTS node_update_drain (
                        session_id TEXT NOT NULL,
                        node_id TEXT NOT NULL,
                        provider_ids_json TEXT NOT NULL CHECK(json_valid(provider_ids_json)),
                        state TEXT NOT NULL CHECK(state IN ('ready', 'draining')),
                        revision INTEGER NOT NULL CHECK(revision > 0),
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY(session_id, node_id)
                    );
                    """
                )
                version = connection.execute(
                    "SELECT value FROM node_update_drain_meta WHERE key='schema_version'"
                ).fetchone()
                if (
                    version is None
                    or int(version["value"]) != UPDATE_DRAIN_STORE_SCHEMA_VERSION
                ):
                    raise FederationValidationError(
                        "unsupported-update-drain-store-schema",
                        "schema_version",
                        f"expected {UPDATE_DRAIN_STORE_SCHEMA_VERSION}",
                    )

    @contextmanager
    def _resource_reservation(
        self,
        *,
        bytes_required: int = UPDATE_DRAIN_MUTATION_BYTES,
        inodes_required: int = UPDATE_DRAIN_MUTATION_INODES,
    ):
        with self.resource_admission.reserve(
            self.database,
            bytes_required=bytes_required,
            inodes_required=inodes_required,
        ):
            yield

    @contextmanager
    def _admitted_connection(self):
        with self._resource_reservation(), self._connect() as connection:
            yield connection

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA wal_autocheckpoint=64")
        return connection

    @staticmethod
    def _provider_ids_json(target: NodeUpdateDrainTarget) -> str:
        return json.dumps(
            list(target.provider_ids),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )

    @staticmethod
    def _record(row: sqlite3.Row) -> NodeUpdateDrainRecord:
        provider_ids = json.loads(row["provider_ids_json"])
        if not isinstance(provider_ids, list):
            raise FederationValidationError(
                "invalid-update-drain-provider-set",
                "provider_ids_json",
                "must contain an array",
            )
        return NodeUpdateDrainRecord(
            session_id=row["session_id"],
            target=NodeUpdateDrainTarget(row["node_id"], tuple(provider_ids)),
            state=row["state"],
            revision=int(row["revision"]),
            updated_at=datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00")),
        )

    def get(self, *, session_id: str, node_id: str) -> NodeUpdateDrainRecord | None:
        session_id = _text(session_id, "session_id")
        node_id = _text(node_id, "node_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM node_update_drain WHERE session_id=? AND node_id=?",
                (session_id, node_id),
            ).fetchone()
        return None if row is None else self._record(row)

    def draining_target(
        self,
        *,
        session_id: str,
        node_id: str,
    ) -> NodeUpdateDrainTarget | None:
        record = self.get(session_id=session_id, node_id=node_id)
        if record is None or record.state is not NodeUpdateDrainState.DRAINING:
            return None
        return record.target

    def request_drain(
        self,
        *,
        session_id: str,
        target: NodeUpdateDrainTarget,
        now: datetime,
    ) -> NodeUpdateDrainMutation:
        """Persist one exact drain target; duplicate requests are idempotent."""

        session_id = _text(session_id, "session_id")
        if not isinstance(target, NodeUpdateDrainTarget):
            raise FederationValidationError(
                "invalid-update-drain-target",
                "target",
                "must be a NodeUpdateDrainTarget",
            )
        now = _utc(now, "now")
        encoded_ids = self._provider_ids_json(target)
        with self._admitted_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM node_update_drain WHERE session_id=? AND node_id=?",
                    (session_id, target.node_id),
                ).fetchone()
                if row is None:
                    connection.execute(
                        """INSERT INTO node_update_drain
                           (session_id, node_id, provider_ids_json, state, revision, updated_at)
                           VALUES (?, ?, ?, ?, 1, ?)""",
                        (
                            session_id,
                            target.node_id,
                            encoded_ids,
                            NodeUpdateDrainState.DRAINING.value,
                            _stamp(now),
                        ),
                    )
                    changed = True
                else:
                    existing = self._record(row)
                    if existing.state is NodeUpdateDrainState.DRAINING:
                        if existing.target != target:
                            raise FederationValidationError(
                                "update-drain-target-conflict",
                                "provider_ids",
                                "an active drain cannot change its provider identity set",
                            )
                        connection.commit()
                        return NodeUpdateDrainMutation(existing, False)
                    connection.execute(
                        """UPDATE node_update_drain
                           SET provider_ids_json=?, state=?, revision=revision+1, updated_at=?
                           WHERE session_id=? AND node_id=?""",
                        (
                            encoded_ids,
                            NodeUpdateDrainState.DRAINING.value,
                            _stamp(now),
                            session_id,
                            target.node_id,
                        ),
                    )
                    changed = True
                current = connection.execute(
                    "SELECT * FROM node_update_drain WHERE session_id=? AND node_id=?",
                    (session_id, target.node_id),
                ).fetchone()
                assert current is not None
                connection.commit()
                return NodeUpdateDrainMutation(self._record(current), changed)
            except Exception:
                connection.rollback()
                raise

    def clear_drain(
        self,
        *,
        session_id: str,
        node_id: str,
        expected_revision: int,
        now: datetime,
    ) -> NodeUpdateDrainMutation:
        """Return to READY only from the exact drain revision being retired."""

        session_id = _text(session_id, "session_id")
        node_id = _text(node_id, "node_id")
        expected_revision = _positive_revision(expected_revision, "expected_revision")
        now = _utc(now, "now")
        with self._admitted_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM node_update_drain WHERE session_id=? AND node_id=?",
                    (session_id, node_id),
                ).fetchone()
                if row is None:
                    raise FederationValidationError(
                        "update-drain-not-requested",
                        "node_id",
                        "cannot clear update drain before it has been requested",
                    )
                current = self._record(row)
                if current.revision != expected_revision:
                    raise FederationValidationError(
                        "update-drain-revision-conflict",
                        "expected_revision",
                        f"expected revision {expected_revision}, found {current.revision}",
                    )
                if current.state is NodeUpdateDrainState.READY:
                    connection.commit()
                    return NodeUpdateDrainMutation(current, False)
                connection.execute(
                    """UPDATE node_update_drain
                       SET state=?, revision=revision+1, updated_at=?
                       WHERE session_id=? AND node_id=? AND revision=?""",
                    (
                        NodeUpdateDrainState.READY.value,
                        _stamp(now),
                        session_id,
                        node_id,
                        expected_revision,
                    ),
                )
                updated_row = connection.execute(
                    "SELECT * FROM node_update_drain WHERE session_id=? AND node_id=?",
                    (session_id, node_id),
                ).fetchone()
                assert updated_row is not None
                connection.commit()
                return NodeUpdateDrainMutation(self._record(updated_row), True)
            except Exception:
                connection.rollback()
                raise


__all__ = [
    "MAX_DRAIN_PROVIDER_IDS",
    "NodeUpdateDrainMutation",
    "NodeUpdateDrainRecord",
    "NodeUpdateDrainState",
    "NodeUpdateDrainTarget",
    "SQLiteNodeUpdateDrainStore",
]
