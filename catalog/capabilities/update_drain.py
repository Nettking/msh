"""Workload-drain primitives for rolling Federation software updates.

An update drain is a scheduling admission fence, not a cancellation mechanism.
The node continues to own and execute work it already holds while its providers
become ineligible for new ownership. Replacement may begin only after durable F7
ownership proves that every provider identity belonging to the node is quiescent.

The critical ordering property lives in the same SQLite authority as F7 ownership:
a persistent trigger rejects ownership-granting updates for a draining provider.
Both first-attempt claims and retry claims therefore serialize with the drain
transition under SQLite's write transaction, instead of trusting an earlier health
snapshot.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum

from catalog.federation.errors import FederationValidationError

from .job_store import SQLiteJobStore
from .jobs import AttemptStatus
from .provider_reports import ProviderResourceReport, ProviderStatus

MAX_DRAIN_PROVIDER_IDS = 128
_MAX_TEXT_BYTES = 512
UPDATE_DRAIN_STORE_SCHEMA_VERSION = 1


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
class ActiveDrainOwnership:
    """Exact durable ownership evidence keeping a drain non-quiescent."""

    session_id: str
    job_id: str
    attempt_id: str
    attempt_number: int
    provider_id: str
    coordinator_id: str
    lease_id: str
    lease_generation: int
    attempt_status: AttemptStatus
    job_revision: int
    lease_expires_at: datetime


@dataclass(frozen=True)
class NodeUpdateDrainTarget:
    """Provider identities that must quiesce before one node switches."""

    node_id: str
    provider_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_id", _text(self.node_id, "node_id"))
        provider_ids = tuple(
            _text(value, f"provider_ids[{index}]")
            for index, value in enumerate(self.provider_ids)
        )
        if not provider_ids:
            raise FederationValidationError(
                "empty-drain-provider-set",
                "provider_ids",
                "must contain at least one provider identity",
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
        """Project READY to DRAINING without changing capacity or capability data."""

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

    def active_ownerships(
        self,
        store: SQLiteJobStore,
        *,
        session_id: str | None = None,
    ) -> tuple[ActiveDrainOwnership, ...]:
        """Return validated current F7 ownership rows for this drain target.

        When ``session_id`` is supplied, only ownership in that Federation
        session contributes to quiescence. This is the form rolling-update
        orchestration must use: provider identities can be stable across session
        changes and must not make unrelated sessions block one another.

        The canonical job JSON, normalized attempt rows, active ownership columns,
        attempt generation, and non-terminal attempt invariant are all validated by
        ``SQLiteJobStore._snapshot_from_row`` in one pinned read transaction. Any
        inconsistency raises instead of being misreported as quiescent.
        """

        if not isinstance(store, SQLiteJobStore):
            raise FederationValidationError(
                "invalid-job-store",
                "store",
                "must be a SQLiteJobStore",
            )
        if session_id is not None:
            session_id = _text(session_id, "session_id")
        placeholders = ",".join("?" for _ in self.provider_ids)
        if session_id is None:
            where = f"active_owner_provider_id IN ({placeholders})"
            parameters: tuple[str, ...] = self.provider_ids
        else:
            where = f"session_id=? AND active_owner_provider_id IN ({placeholders})"
            parameters = (session_id, *self.provider_ids)
        with store._connect() as connection:  # package-internal read-only seam
            connection.execute("BEGIN")
            rows = connection.execute(
                f"""SELECT * FROM capability_jobs
                    WHERE {where}
                    ORDER BY session_id, job_id""",
                parameters,
            ).fetchall()
            evidence: list[ActiveDrainOwnership] = []
            for row in rows:
                try:
                    snapshot = store._snapshot_from_row(connection, row)
                    ownership = snapshot.ownership
                    if ownership is None:
                        raise FederationValidationError(
                            "update-drain-ownership-integrity",
                            "ownership",
                            "active provider pointer has no valid durable ownership",
                        )
                    active_attempts = tuple(
                        attempt
                        for attempt in snapshot.job.attempts
                        if not attempt.terminal
                    )
                    if (
                        len(active_attempts) != 1
                        or active_attempts[0].attempt_id != ownership.attempt_id
                    ):
                        raise FederationValidationError(
                            "update-drain-ownership-integrity",
                            "attempts",
                            "active ownership does not name exactly one non-terminal attempt",
                        )
                    evidence.append(
                        ActiveDrainOwnership(
                            session_id=snapshot.job.session_id,
                            job_id=snapshot.job.job_id,
                            attempt_id=ownership.attempt_id,
                            attempt_number=ownership.attempt_number,
                            provider_id=ownership.owner_provider_id,
                            coordinator_id=ownership.granted_by_coordinator_id,
                            lease_id=ownership.lease_id,
                            lease_generation=ownership.lease_generation,
                            attempt_status=active_attempts[0].status,
                            job_revision=snapshot.revision,
                            lease_expires_at=ownership.lease_expires_at,
                        )
                    )
                except FederationValidationError:
                    raise
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise FederationValidationError(
                        "update-drain-ownership-integrity",
                        "ownership",
                        "durable active ownership is malformed",
                    ) from exc
        return tuple(evidence)

    def active_ownership_count(
        self,
        store: SQLiteJobStore,
        *,
        session_id: str | None = None,
    ) -> int:
        """Return the validated number of durable active ownerships."""

        return len(self.active_ownerships(store, session_id=session_id))

    def is_quiescent(
        self,
        store: SQLiteJobStore,
        *,
        session_id: str | None = None,
    ) -> bool:
        """Return true only after validated durable current ownership reaches zero."""

        return not self.active_ownerships(store, session_id=session_id)


class NodeUpdateDrainState(str, Enum):
    """Persistent rolling-update admission state for one Federation member."""

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

    @property
    def admission_epoch(self) -> int:
        return self.revision


@dataclass(frozen=True)
class NodeUpdateDrainMutation:
    record: NodeUpdateDrainRecord
    changed: bool


class SQLiteNodeUpdateDrainStore:
    """Durable drain state installed inside the authoritative F7 job database.

    The store deliberately takes an existing ``SQLiteJobStore`` instead of an
    arbitrary path. This prevents accidental cross-database admission checks:
    the drain transition and every ownership-granting claim contend for the same
    SQLite write lock and the persistent trigger below executes inside the claim.
    """

    def __init__(self, jobs: SQLiteJobStore) -> None:
        if not isinstance(jobs, SQLiteJobStore):
            raise FederationValidationError(
                "invalid-job-store",
                "jobs",
                "update drain must share the authoritative SQLiteJobStore",
            )
        self.jobs = jobs
        self.database = jobs.database
        with self.jobs._admitted_connection() as connection:
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

                CREATE TABLE IF NOT EXISTS node_update_drain_provider (
                    session_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    PRIMARY KEY(session_id, node_id, provider_id),
                    FOREIGN KEY(session_id, node_id)
                        REFERENCES node_update_drain(session_id, node_id)
                        ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS node_update_drain_provider_lookup
                    ON node_update_drain_provider(session_id, provider_id);

                CREATE TABLE IF NOT EXISTS node_update_drain_commands (
                    session_id TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    provider_ids_json TEXT NOT NULL CHECK(json_valid(provider_ids_json)),
                    result_revision INTEGER NOT NULL CHECK(result_revision > 0),
                    result_updated_at TEXT NOT NULL,
                    result_changed INTEGER NOT NULL CHECK(result_changed IN (0, 1)),
                    PRIMARY KEY(session_id, command_id)
                );

                CREATE TRIGGER IF NOT EXISTS capability_jobs_update_drain_fence
                BEFORE UPDATE OF active_owner_provider_id ON capability_jobs
                WHEN NEW.active_owner_provider_id IS NOT NULL
                 AND EXISTS (
                    SELECT 1
                      FROM node_update_drain_provider AS provider
                      JOIN node_update_drain AS drain
                        ON drain.session_id = provider.session_id
                       AND drain.node_id = provider.node_id
                     WHERE provider.session_id = NEW.session_id
                       AND provider.provider_id = NEW.active_owner_provider_id
                       AND drain.state = 'draining'
                 )
                BEGIN
                    SELECT RAISE(ABORT, 'provider-update-draining');
                END;
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
        try:
            provider_ids = json.loads(row["provider_ids_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-update-drain-provider-set",
                "provider_ids_json",
                "must contain valid JSON",
            ) from exc
        if not isinstance(provider_ids, list):
            raise FederationValidationError(
                "invalid-update-drain-provider-set",
                "provider_ids_json",
                "must contain an array",
            )
        try:
            updated_at = datetime.fromisoformat(
                row["updated_at"].replace("Z", "+00:00")
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-update-drain-timestamp",
                "updated_at",
                "stored update drain timestamp is invalid",
            ) from exc
        return NodeUpdateDrainRecord(
            session_id=row["session_id"],
            target=NodeUpdateDrainTarget(row["node_id"], tuple(provider_ids)),
            state=row["state"],
            revision=int(row["revision"]),
            updated_at=updated_at,
        )

    def get(self, *, session_id: str, node_id: str) -> NodeUpdateDrainRecord | None:
        session_id = _text(session_id, "session_id")
        node_id = _text(node_id, "node_id")
        with self.jobs._connect() as connection:
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

    def project_report(self, report: ProviderResourceReport) -> ProviderResourceReport:
        """Project a durable active drain into the provider's live health report."""

        if not isinstance(report, ProviderResourceReport):
            raise FederationValidationError(
                "invalid-provider-report",
                "report",
                "must be a ProviderResourceReport",
            )
        target = self.draining_target(
            session_id=report.session_id,
            node_id=report.node_id,
        )
        return report if target is None else target.apply_to_report(report)

    def active_ownerships(
        self,
        *,
        session_id: str,
        node_id: str,
    ) -> tuple[ActiveDrainOwnership, ...]:
        """Return session-scoped durable ownership blocking this node's drain."""

        session_id = _text(session_id, "session_id")
        node_id = _text(node_id, "node_id")
        target = self.draining_target(session_id=session_id, node_id=node_id)
        if target is None:
            return ()
        return target.active_ownerships(self.jobs, session_id=session_id)

    def active_ownership_count(self, *, session_id: str, node_id: str) -> int:
        """Return the session-scoped count of validated active ownerships."""

        return len(self.active_ownerships(session_id=session_id, node_id=node_id))

    def is_quiescent(self, *, session_id: str, node_id: str) -> bool:
        """Return true only when this session/node has no targeted active owner."""

        return not self.active_ownerships(session_id=session_id, node_id=node_id)

    def request_drain(
        self,
        *,
        session_id: str,
        target: NodeUpdateDrainTarget,
        command_id: str,
        now: datetime,
    ) -> NodeUpdateDrainMutation:
        """Atomically make DRAINING effective for claims and persist its target."""

        session_id = _text(session_id, "session_id")
        command_id = _text(command_id, "command_id")
        if not isinstance(target, NodeUpdateDrainTarget):
            raise FederationValidationError(
                "invalid-update-drain-target",
                "target",
                "must be a NodeUpdateDrainTarget",
            )
        now = _utc(now, "now")
        encoded_ids = self._provider_ids_json(target)
        with self.jobs._admitted_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                command = connection.execute(
                    """SELECT * FROM node_update_drain_commands
                       WHERE session_id=? AND command_id=?""",
                    (session_id, command_id),
                ).fetchone()
                if command is not None:
                    if (
                        command["node_id"] != target.node_id
                        or command["provider_ids_json"] != encoded_ids
                    ):
                        raise FederationValidationError(
                            "update-drain-command-conflict",
                            "command_id",
                            "command ID was already used for a different drain target",
                        )
                    replay = NodeUpdateDrainRecord(
                        session_id=session_id,
                        target=target,
                        state=NodeUpdateDrainState.DRAINING,
                        revision=int(command["result_revision"]),
                        updated_at=datetime.fromisoformat(
                            command["result_updated_at"].replace("Z", "+00:00")
                        ),
                    )
                    connection.commit()
                    return NodeUpdateDrainMutation(
                        replay, bool(int(command["result_changed"]))
                    )

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
                        changed = False
                    else:
                        connection.execute(
                            """UPDATE node_update_drain
                               SET provider_ids_json=?, state=?, revision=revision+1,
                                   updated_at=?
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

                if changed:
                    connection.execute(
                        """DELETE FROM node_update_drain_provider
                           WHERE session_id=? AND node_id=?""",
                        (session_id, target.node_id),
                    )
                    connection.executemany(
                        """INSERT INTO node_update_drain_provider
                           (session_id, node_id, provider_id)
                           VALUES (?, ?, ?)""",
                        (
                            (session_id, target.node_id, provider_id)
                            for provider_id in target.provider_ids
                        ),
                    )

                current_row = connection.execute(
                    "SELECT * FROM node_update_drain WHERE session_id=? AND node_id=?",
                    (session_id, target.node_id),
                ).fetchone()
                assert current_row is not None
                current = self._record(current_row)
                connection.execute(
                    """INSERT INTO node_update_drain_commands
                       (session_id, command_id, node_id, provider_ids_json,
                        result_revision, result_updated_at, result_changed)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        session_id,
                        command_id,
                        target.node_id,
                        encoded_ids,
                        current.revision,
                        _stamp(current.updated_at),
                        1 if changed else 0,
                    ),
                )
                connection.commit()
                return NodeUpdateDrainMutation(current, changed)
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
        """Return to READY only from the exact admission epoch being retired."""

        session_id = _text(session_id, "session_id")
        node_id = _text(node_id, "node_id")
        expected_revision = _positive_revision(expected_revision, "expected_revision")
        now = _utc(now, "now")
        with self.jobs._admitted_connection() as connection:
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
    "ActiveDrainOwnership",
    "NodeUpdateDrainMutation",
    "NodeUpdateDrainRecord",
    "NodeUpdateDrainState",
    "NodeUpdateDrainTarget",
    "SQLiteNodeUpdateDrainStore",
]
