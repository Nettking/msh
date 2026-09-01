"""Durable update-drain state and quiescence proof for capability providers.

R1 keeps update intent separate from job lifecycle authority.  A node-level drain
state says that new provider work must stop; durable F7 ownership remains the
source of truth for work that is already in flight.  The quiescence query reads
that ownership directly instead of trusting an in-memory active-job counter.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Iterable

from catalog.federation.errors import FederationValidationError
from catalog.federation.host_resources import ProcessResourceAdmission
from catalog.federation.process_resource_admission import PROCESS_RESOURCE_ADMISSION

from .job_store import SQLiteJobStore

UPDATE_DRAIN_STORE_SCHEMA_VERSION = 1
UPDATE_DRAIN_INITIALIZATION_BYTES = 2 * 1024 * 1024
UPDATE_DRAIN_MUTATION_BYTES = 1024 * 1024
UPDATE_DRAIN_MUTATION_INODES = 4
MAX_TEXT_BYTES = 512


class ProviderUpdateDrainState(str, Enum):
    """Durable scheduling state owned by the rolling-update protocol."""

    READY = "ready"
    DRAINING = "draining"


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
    if len(value.encode("utf-8")) > MAX_TEXT_BYTES:
        raise FederationValidationError(
            "text-too-large", field, f"must not exceed {MAX_TEXT_BYTES} UTF-8 bytes"
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


def _revision(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FederationValidationError(
            "invalid-positive-integer", field, "must be a positive integer"
        )
    return value


@dataclass(frozen=True)
class ProviderUpdateDrainRecord:
    session_id: str
    node_id: str
    state: ProviderUpdateDrainState
    revision: int
    updated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "session_id", _text(self.session_id, "session_id"))
        object.__setattr__(self, "node_id", _text(self.node_id, "node_id"))
        try:
            object.__setattr__(self, "state", ProviderUpdateDrainState(self.state))
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-update-drain-state", "state", "unknown update-drain state"
            ) from exc
        object.__setattr__(self, "revision", _revision(self.revision, "revision"))
        object.__setattr__(self, "updated_at", _utc(self.updated_at, "updated_at"))


@dataclass(frozen=True)
class ProviderUpdateDrainMutation:
    record: ProviderUpdateDrainRecord
    changed: bool


@dataclass(frozen=True)
class ActiveProviderOwnership:
    """One durable non-terminal ownership observed in the F7 job store."""

    job_id: str
    attempt_id: str
    provider_id: str


@dataclass(frozen=True)
class DurableProviderQuiescence:
    """Result of a durable ownership query for a complete provider-id set."""

    provider_ids: tuple[str, ...]
    active_ownerships: tuple[ActiveProviderOwnership, ...]

    @property
    def quiescent(self) -> bool:
        return not self.active_ownerships

    @property
    def active_count(self) -> int:
        return len(self.active_ownerships)


class SQLiteProviderUpdateDrainStore:
    """Restart-safe node drain state with revision-fenced recovery to READY."""

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
                    CREATE TABLE IF NOT EXISTS provider_update_drain_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    INSERT OR IGNORE INTO provider_update_drain_meta(key, value)
                        VALUES ('schema_version', '1');

                    CREATE TABLE IF NOT EXISTS provider_update_drain (
                        session_id TEXT NOT NULL,
                        node_id TEXT NOT NULL,
                        state TEXT NOT NULL CHECK(state IN ('ready', 'draining')),
                        revision INTEGER NOT NULL CHECK(revision > 0),
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY(session_id, node_id)
                    );
                    """
                )
                version = connection.execute(
                    "SELECT value FROM provider_update_drain_meta WHERE key='schema_version'"
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
    def _record(row: sqlite3.Row) -> ProviderUpdateDrainRecord:
        return ProviderUpdateDrainRecord(
            session_id=row["session_id"],
            node_id=row["node_id"],
            state=row["state"],
            revision=int(row["revision"]),
            updated_at=datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00")),
        )

    def get(self, *, session_id: str, node_id: str) -> ProviderUpdateDrainRecord | None:
        session_id = _text(session_id, "session_id")
        node_id = _text(node_id, "node_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM provider_update_drain WHERE session_id=? AND node_id=?",
                (session_id, node_id),
            ).fetchone()
        return None if row is None else self._record(row)

    def is_draining(self, *, session_id: str, node_id: str) -> bool:
        record = self.get(session_id=session_id, node_id=node_id)
        return record is not None and record.state is ProviderUpdateDrainState.DRAINING

    def request_drain(
        self,
        *,
        session_id: str,
        node_id: str,
        now: datetime,
    ) -> ProviderUpdateDrainMutation:
        """Persist DRAINING idempotently without touching existing job ownership."""

        session_id = _text(session_id, "session_id")
        node_id = _text(node_id, "node_id")
        now = _utc(now, "now")
        with self._admitted_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM provider_update_drain WHERE session_id=? AND node_id=?",
                    (session_id, node_id),
                ).fetchone()
                if row is None:
                    connection.execute(
                        """INSERT INTO provider_update_drain
                           (session_id, node_id, state, revision, updated_at)
                           VALUES (?, ?, ?, 1, ?)""",
                        (
                            session_id,
                            node_id,
                            ProviderUpdateDrainState.DRAINING.value,
                            _stamp(now),
                        ),
                    )
                    changed = True
                elif row["state"] == ProviderUpdateDrainState.DRAINING.value:
                    changed = False
                else:
                    connection.execute(
                        """UPDATE provider_update_drain
                           SET state=?, revision=revision+1, updated_at=?
                           WHERE session_id=? AND node_id=?""",
                        (
                            ProviderUpdateDrainState.DRAINING.value,
                            _stamp(now),
                            session_id,
                            node_id,
                        ),
                    )
                    changed = True
                current = connection.execute(
                    "SELECT * FROM provider_update_drain WHERE session_id=? AND node_id=?",
                    (session_id, node_id),
                ).fetchone()
                assert current is not None
                connection.commit()
                return ProviderUpdateDrainMutation(self._record(current), changed)
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
    ) -> ProviderUpdateDrainMutation:
        """Return to READY only from the exact drain revision being retired."""

        session_id = _text(session_id, "session_id")
        node_id = _text(node_id, "node_id")
        expected_revision = _revision(expected_revision, "expected_revision")
        now = _utc(now, "now")
        with self._admitted_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM provider_update_drain WHERE session_id=? AND node_id=?",
                    (session_id, node_id),
                ).fetchone()
                if row is None:
                    raise FederationValidationError(
                        "update-drain-not-requested",
                        "node_id",
                        "cannot clear update drain before it has been requested",
                    )
                actual_revision = int(row["revision"])
                if actual_revision != expected_revision:
                    raise FederationValidationError(
                        "update-drain-revision-conflict",
                        "expected_revision",
                        f"expected revision {expected_revision}, found {actual_revision}",
                    )
                if row["state"] == ProviderUpdateDrainState.READY.value:
                    connection.commit()
                    return ProviderUpdateDrainMutation(self._record(row), False)
                connection.execute(
                    """UPDATE provider_update_drain
                       SET state=?, revision=revision+1, updated_at=?
                       WHERE session_id=? AND node_id=? AND revision=?""",
                    (
                        ProviderUpdateDrainState.READY.value,
                        _stamp(now),
                        session_id,
                        node_id,
                        expected_revision,
                    ),
                )
                current = connection.execute(
                    "SELECT * FROM provider_update_drain WHERE session_id=? AND node_id=?",
                    (session_id, node_id),
                ).fetchone()
                assert current is not None
                connection.commit()
                return ProviderUpdateDrainMutation(self._record(current), True)
            except Exception:
                connection.rollback()
                raise


class DurableProviderQuiescenceQuery:
    """Read-only proof over the durable F7 ownership columns.

    Provider selection returns a capability/provider id, and F7 persists that id
    as ``active_owner_provider_id``.  R1 intentionally takes the complete set of
    provider ids belonging to the node from its caller rather than guessing a
    node mapping from short-lived health reports.  R2 can compose that set from
    the provider authority before activation.
    """

    def __init__(self, jobs: SQLiteJobStore) -> None:
        self.jobs = jobs

    def probe(self, provider_ids: Iterable[str]) -> DurableProviderQuiescence:
        identities = tuple(sorted({_text(value, "provider_id") for value in provider_ids}))
        if not identities:
            return DurableProviderQuiescence((), ())
        placeholders = ",".join("?" for _ in identities)
        query = (
            "SELECT job_id, active_attempt_id, active_owner_provider_id "
            "FROM capability_jobs "
            f"WHERE active_owner_provider_id IN ({placeholders}) "
            "ORDER BY job_id"
        )
        with self.jobs._connect() as connection:  # same package, read-only F7 query
            rows = connection.execute(query, identities).fetchall()
        active = tuple(
            ActiveProviderOwnership(
                job_id=row["job_id"],
                attempt_id=row["active_attempt_id"],
                provider_id=row["active_owner_provider_id"],
            )
            for row in rows
        )
        return DurableProviderQuiescence(identities, active)
