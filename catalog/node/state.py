"""Versioned durable local state and exact in-order event application."""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Final

from catalog.federation.errors import FederationValidationError
from catalog.federation.models import CapabilityAnnouncement, SessionEvent
from catalog.federation.redaction import redact_secrets

SCHEMA_VERSION: Final = 1
MAX_EVENT_BYTES: Final = 1_048_576
MAX_EVENT_PAGE_ITEMS: Final = 10_000
MAX_ID_LENGTH: Final = 512
MAX_RECONNECT_ATTEMPTS: Final = 100
MAX_RECONNECT_DELAY_SECONDS: Final = 3_600.0
_ERROR_CODE = re.compile(r"^[a-z0-9][a-z0-9.-]{0,127}$")


class NodeStateError(FederationValidationError):
    """A structured local state or replay invariant failure."""


class EnrollmentState(str, Enum):
    UNENROLLED = "unenrolled"
    ENROLLED = "enrolled"
    REVOKED = "revoked"
    ERROR = "error"


class ConnectionState(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    REPLAYING = "replaying"
    REVOKED = "revoked"
    ERROR = "error"


class SessionMembershipState(str, Enum):
    JOINED = "joined"
    REMOVED = "removed"


class EventApplyStatus(str, Enum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    GAP = "gap"


@dataclass(frozen=True)
class EventApplyResult:
    status: EventApplyStatus
    last_applied_revision: int
    expected_revision: int


@dataclass(frozen=True)
class JoinedSession:
    session_id: str
    membership_state: SessionMembershipState
    joined_at: datetime
    removed_at: datetime | None
    last_applied_revision: int
    replaying: bool
    replay_target_revision: int | None


@dataclass(frozen=True)
class ReconnectPolicy:
    """A finite deterministic retry schedule; waiting belongs to the client."""

    initial_delay_seconds: float = 0.25
    multiplier: float = 2.0
    max_delay_seconds: float = 30.0
    max_attempts: int = 8

    def __post_init__(self) -> None:
        values = (
            self.initial_delay_seconds,
            self.multiplier,
            self.max_delay_seconds,
        )
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
            for value in values
        ):
            raise NodeStateError(
                "invalid-reconnect-policy",
                "delay",
                "retry delays and multiplier must be positive numbers",
            )
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or not 1 <= self.max_attempts <= MAX_RECONNECT_ATTEMPTS
        ):
            raise NodeStateError(
                "invalid-reconnect-policy",
                "max_attempts",
                f"must be between 1 and {MAX_RECONNECT_ATTEMPTS}",
            )
        if self.max_delay_seconds > MAX_RECONNECT_DELAY_SECONDS:
            raise NodeStateError(
                "invalid-reconnect-policy",
                "max_delay_seconds",
                f"must not exceed {MAX_RECONNECT_DELAY_SECONDS:g} seconds",
            )
        if self.initial_delay_seconds > self.max_delay_seconds:
            raise NodeStateError(
                "invalid-reconnect-policy",
                "initial_delay_seconds",
                "must not exceed max_delay_seconds",
            )

    def delay_for_attempt(self, attempt: int) -> float:
        if (
            isinstance(attempt, bool)
            or not isinstance(attempt, int)
            or attempt < 0
            or attempt >= self.max_attempts
        ):
            raise NodeStateError(
                "reconnect-attempt-out-of-range",
                "attempt",
                "must be within the finite retry schedule",
            )
        return min(
            self.max_delay_seconds,
            self.initial_delay_seconds * (self.multiplier**attempt),
        )

    def delays(self) -> tuple[float, ...]:
        return tuple(
            self.delay_for_attempt(attempt)
            for attempt in range(self.max_attempts)
        )


def _opaque_text(value: str, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_ID_LENGTH
        or any(ord(character) < 32 for character in value)
    ):
        raise NodeStateError(
            "invalid-id", field, "must be bounded non-empty opaque text"
        )
    return value


def _utc(value: datetime, field: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset().total_seconds() != 0
    ):
        raise NodeStateError(
            "invalid-timestamp", field, "must be timezone-aware UTC"
        )
    return value.astimezone(timezone.utc)


def _time_text(value: datetime, field: str = "now") -> str:
    return _utc(value, field).isoformat().replace("+00:00", "Z")


def _decode_time(value: str, field: str) -> datetime:
    try:
        decoded = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise NodeStateError(
            "malformed-node-state", field, "persisted timestamp is invalid"
        ) from exc
    return _utc(decoded, field)


def _error_code(value: str | None) -> str | None:
    if value is not None and (
        not isinstance(value, str) or _ERROR_CODE.fullmatch(value) is None
    ):
        raise NodeStateError(
            "invalid-error-code",
            "error_code",
            "must be a stable lower-case machine code",
        )
    return value


class NodeState:
    """SQLite-backed state scoped and cryptographically bound to one node ID."""

    def __init__(
        self,
        database: Path | str,
        *,
        node_id: str,
        now: datetime | None = None,
    ) -> None:
        self.database = str(database)
        self.node_id = _opaque_text(node_id, "node_id")
        database_path = Path(self.database)
        database_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(database_path.parent, 0o700)
        except OSError as exc:
            raise NodeStateError(
                "node-state-permission-failed",
                "database",
                "could not restrict node state directory permissions",
            ) from exc
        self.initialize(now=now or datetime.now(timezone.utc))

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        database = self._connect()
        try:
            database.execute("BEGIN IMMEDIATE")
            yield database
            database.commit()
        except BaseException:
            database.rollback()
            raise
        finally:
            database.close()

    @contextmanager
    def _read_transaction(self) -> Iterator[sqlite3.Connection]:
        database = self._connect()
        try:
            database.execute("BEGIN")
            yield database
            database.commit()
        except BaseException:
            database.rollback()
            raise
        finally:
            database.close()

    def initialize(self, *, now: datetime) -> None:
        stamp = _time_text(now)
        with self._connect() as database:
            database.execute("PRAGMA journal_mode=WAL")
        with self._transaction() as database:
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS node_state_schema (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    version INTEGER NOT NULL CHECK(version > 0)
                )
                """
            )
            schema_row = database.execute(
                "SELECT version FROM node_state_schema WHERE singleton=1"
            ).fetchone()
            if schema_row is None:
                database.execute(
                    "INSERT INTO node_state_schema(singleton,version) VALUES(1,?)",
                    (SCHEMA_VERSION,),
                )
            elif schema_row["version"] != SCHEMA_VERSION:
                raise NodeStateError(
                    "unsupported-node-state-schema",
                    "version",
                    f"received {schema_row['version']}",
                )
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS node_runtime (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    node_id TEXT NOT NULL CHECK(length(node_id) > 0),
                    enrollment_state TEXT NOT NULL
                        CHECK(enrollment_state IN ('unenrolled','enrolled','revoked','error')),
                    connection_state TEXT NOT NULL
                        CHECK(connection_state IN
                            ('disconnected','connecting','connected','replaying','revoked','error')),
                    last_heartbeat TEXT,
                    last_error_code TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS joined_sessions (
                    session_id TEXT PRIMARY KEY CHECK(length(session_id) > 0),
                    membership_state TEXT NOT NULL
                        CHECK(membership_state IN ('joined','removed')),
                    joined_at TEXT NOT NULL,
                    removed_at TEXT,
                    last_applied_revision INTEGER NOT NULL DEFAULT 0
                        CHECK(last_applied_revision >= 0),
                    replaying INTEGER NOT NULL DEFAULT 0 CHECK(replaying IN (0,1)),
                    replay_target_revision INTEGER
                        CHECK(replay_target_revision IS NULL OR replay_target_revision >= 0)
                )
                """
            )
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS applied_events (
                    event_id TEXT PRIMARY KEY CHECK(length(event_id) > 0),
                    session_id TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK(revision > 0),
                    event_json TEXT NOT NULL
                        CHECK(length(event_json) <= 1048576 AND json_valid(event_json)),
                    applied_at TEXT NOT NULL,
                    FOREIGN KEY(session_id) REFERENCES joined_sessions(session_id),
                    UNIQUE(session_id, revision)
                )
                """
            )
            database.execute(
                """
                CREATE INDEX IF NOT EXISTS applied_events_replay
                ON applied_events(session_id,revision)
                """
            )
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS advertised_capabilities (
                    session_id TEXT NOT NULL,
                    capability_id TEXT NOT NULL,
                    capability_json TEXT NOT NULL
                        CHECK(length(capability_json) <= 1048576
                              AND json_valid(capability_json)),
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id,capability_id),
                    FOREIGN KEY(session_id) REFERENCES joined_sessions(session_id)
                )
                """
            )
            runtime = database.execute(
                "SELECT node_id FROM node_runtime WHERE singleton=1"
            ).fetchone()
            if runtime is None:
                database.execute(
                    """
                    INSERT INTO node_runtime(
                        singleton,node_id,enrollment_state,connection_state,
                        last_heartbeat,last_error_code,updated_at
                    ) VALUES(1,?,'unenrolled','disconnected',NULL,NULL,?)
                    """,
                    (self.node_id, stamp),
                )
            elif runtime["node_id"] != self.node_id:
                raise NodeStateError(
                    "node-state-identity-mismatch",
                    "node_id",
                    "node state belongs to a different persistent identity",
                )
        try:
            os.chmod(self.database, 0o600)
        except OSError as exc:
            raise NodeStateError(
                "node-state-permission-failed",
                "database",
                "could not restrict node state database permissions",
            ) from exc

    def set_enrollment_state(
        self,
        state: EnrollmentState | str,
        *,
        now: datetime,
        error_code: str | None = None,
    ) -> None:
        try:
            selected = EnrollmentState(state)
        except ValueError as exc:
            raise NodeStateError(
                "invalid-enrollment-state",
                "state",
                "unknown enrollment state",
            ) from exc
        code = _error_code(error_code)
        if selected is EnrollmentState.ERROR and code is None:
            raise NodeStateError(
                "missing-error-code",
                "error_code",
                "error state requires a stable error code",
            )
        stamp = _time_text(now)
        with self._transaction() as database:
            if selected is EnrollmentState.REVOKED:
                connection = ConnectionState.REVOKED.value
            else:
                current = database.execute(
                    "SELECT connection_state FROM node_runtime WHERE singleton=1"
                ).fetchone()["connection_state"]
                connection = (
                    ConnectionState.DISCONNECTED.value
                    if current == ConnectionState.REVOKED.value
                    else current
                )
            database.execute(
                """
                UPDATE node_runtime
                SET enrollment_state=?,connection_state=?,last_error_code=?,updated_at=?
                WHERE singleton=1
                """,
                (selected.value, connection, code, stamp),
            )

    def set_connection_state(
        self,
        state: ConnectionState | str,
        *,
        now: datetime,
        error_code: str | None = None,
    ) -> None:
        try:
            selected = ConnectionState(state)
        except ValueError as exc:
            raise NodeStateError(
                "invalid-connection-state",
                "state",
                "unknown connection state",
            ) from exc
        code = _error_code(error_code)
        if selected is ConnectionState.ERROR and code is None:
            raise NodeStateError(
                "missing-error-code",
                "error_code",
                "error state requires a stable error code",
            )
        stamp = _time_text(now)
        with self._transaction() as database:
            enrollment = database.execute(
                "SELECT enrollment_state FROM node_runtime WHERE singleton=1"
            ).fetchone()["enrollment_state"]
            if (
                enrollment == EnrollmentState.REVOKED.value
                and selected is not ConnectionState.REVOKED
            ):
                raise NodeStateError(
                    "node-revoked",
                    "connection_state",
                    "a revoked node cannot enter an accepted connection state",
                )
            database.execute(
                """
                UPDATE node_runtime
                SET connection_state=?,last_error_code=?,updated_at=?
                WHERE singleton=1
                """,
                (selected.value, code, stamp),
            )

    def record_heartbeat(self, *, now: datetime) -> None:
        stamp = _time_text(now)
        with self._transaction() as database:
            runtime = database.execute(
                "SELECT enrollment_state FROM node_runtime WHERE singleton=1"
            ).fetchone()
            if runtime["enrollment_state"] == EnrollmentState.REVOKED.value:
                raise NodeStateError(
                    "node-revoked",
                    "heartbeat",
                    "a revoked node cannot record an accepted heartbeat",
                )
            database.execute(
                """
                UPDATE node_runtime
                SET last_heartbeat=?,updated_at=?
                WHERE singleton=1
                """,
                (stamp, stamp),
            )

    def join_session(self, session_id: str, *, now: datetime) -> JoinedSession:
        session_id = _opaque_text(session_id, "session_id")
        stamp = _time_text(now)
        with self._transaction() as database:
            current = database.execute(
                "SELECT * FROM joined_sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            if current is None:
                database.execute(
                    """
                    INSERT INTO joined_sessions(
                        session_id,membership_state,joined_at,removed_at,
                        last_applied_revision,replaying,replay_target_revision
                    ) VALUES(?,'joined',?,NULL,0,0,NULL)
                    """,
                    (session_id, stamp),
                )
            elif current["membership_state"] == SessionMembershipState.REMOVED.value:
                database.execute(
                    """
                    UPDATE joined_sessions
                    SET membership_state='joined',joined_at=?,removed_at=NULL
                    WHERE session_id=?
                    """,
                    (stamp, session_id),
                )
        return self.get_session(session_id)

    def remove_session(self, session_id: str, *, now: datetime) -> JoinedSession:
        session_id = _opaque_text(session_id, "session_id")
        stamp = _time_text(now)
        with self._transaction() as database:
            current = database.execute(
                "SELECT session_id FROM joined_sessions WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if current is None:
                raise NodeStateError(
                    "session-not-joined",
                    "session_id",
                    "session is not in durable node state",
                )
            database.execute(
                """
                UPDATE joined_sessions
                SET membership_state='removed',removed_at=?,replaying=0,
                    replay_target_revision=NULL
                WHERE session_id=?
                """,
                (stamp, session_id),
            )
            database.execute(
                "DELETE FROM advertised_capabilities WHERE session_id=?",
                (session_id,),
            )
            self._clear_runtime_replaying_if_complete(database, stamp)
        return self.get_session(session_id)

    def get_session(self, session_id: str) -> JoinedSession:
        session_id = _opaque_text(session_id, "session_id")
        with self._connect() as database:
            row = database.execute(
                "SELECT * FROM joined_sessions WHERE session_id=?", (session_id,)
            ).fetchone()
        if row is None:
            raise NodeStateError(
                "session-not-joined",
                "session_id",
                "session is not in durable node state",
            )
        return self._decode_session(row)

    def joined_sessions(
        self, *, include_removed: bool = False
    ) -> tuple[JoinedSession, ...]:
        query = "SELECT * FROM joined_sessions"
        if not include_removed:
            query += " WHERE membership_state='joined'"
        query += " ORDER BY session_id"
        with self._connect() as database:
            return tuple(
                self._decode_session(row) for row in database.execute(query)
            )

    def last_applied_revision(self, session_id: str) -> int:
        return self.get_session(session_id).last_applied_revision

    def mark_session_replaying(
        self,
        session_id: str,
        *,
        now: datetime,
        target_revision: int | None = None,
    ) -> JoinedSession:
        session_id = _opaque_text(session_id, "session_id")
        if target_revision is not None and (
            isinstance(target_revision, bool)
            or not isinstance(target_revision, int)
            or target_revision < 0
        ):
            raise NodeStateError(
                "invalid-revision",
                "target_revision",
                "must be a non-negative integer",
            )
        stamp = _time_text(now)
        with self._transaction() as database:
            row = self._require_joined(database, session_id)
            target = target_revision
            if row["replay_target_revision"] is not None:
                target = max(target or 0, row["replay_target_revision"])
            database.execute(
                """
                UPDATE joined_sessions
                SET replaying=1,replay_target_revision=?
                WHERE session_id=?
                """,
                (target, session_id),
            )
            self._mark_runtime_replaying(database, stamp)
        return self.get_session(session_id)

    def complete_replay(
        self,
        session_id: str,
        *,
        final_revision: int,
        now: datetime,
    ) -> JoinedSession:
        """Clear replay only after durable local state reaches coordinator state."""

        session_id = _opaque_text(session_id, "session_id")
        if (
            isinstance(final_revision, bool)
            or not isinstance(final_revision, int)
            or final_revision < 0
        ):
            raise NodeStateError(
                "invalid-revision",
                "final_revision",
                "must be a non-negative integer",
            )
        stamp = _time_text(now)
        rejection: NodeStateError | None = None
        with self._transaction() as database:
            row = self._require_joined(database, session_id)
            current = row["last_applied_revision"]
            target = row["replay_target_revision"]
            required_revision = max(final_revision, target or 0)
            if current < final_revision:
                database.execute(
                    """
                    UPDATE joined_sessions
                    SET replaying=1,replay_target_revision=?
                    WHERE session_id=?
                    """,
                    (required_revision, session_id),
                )
                self._mark_runtime_replaying(database, stamp)
                rejection = NodeStateError(
                    "revision-gap",
                    "final_revision",
                    "coordinator has revisions that were not durably applied",
                )
            elif current < required_revision:
                database.execute(
                    """
                    UPDATE joined_sessions
                    SET replaying=1,replay_target_revision=?
                    WHERE session_id=?
                    """,
                    (required_revision, session_id),
                )
                self._mark_runtime_replaying(database, stamp)
            else:
                database.execute(
                    """
                    UPDATE joined_sessions
                    SET replaying=0,replay_target_revision=NULL
                    WHERE session_id=?
                    """,
                    (session_id,),
                )
                self._clear_runtime_replaying_if_complete(database, stamp)
        if rejection is not None:
            raise rejection
        return self.get_session(session_id)

    def save_capability(
        self, capability: CapabilityAnnouncement, *, now: datetime
    ) -> CapabilityAnnouncement:
        if not isinstance(capability, CapabilityAnnouncement):
            raise NodeStateError(
                "invalid-capability",
                "capability",
                "must be a CapabilityAnnouncement",
            )
        try:
            capability = CapabilityAnnouncement.from_dict(
                capability.to_dict()
            )
        except FederationValidationError as exc:
            raise NodeStateError(
                exc.code,
                exc.field,
                exc.message,
            ) from exc
        if capability.node_id != self.node_id:
            raise NodeStateError(
                "capability-node-mismatch",
                "node_id",
                "local capability must belong to this persistent identity",
            )
        try:
            encoded = json.dumps(
                capability.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise NodeStateError(
                "invalid-capability",
                "capability",
                "must contain JSON-compatible values",
            ) from exc
        if len(encoded.encode()) > MAX_EVENT_BYTES:
            raise NodeStateError(
                "capability-too-large",
                "capability",
                "capability exceeds the durable size bound",
            )
        with self._transaction() as database:
            self._require_joined(database, capability.session_id)
            existing = database.execute(
                """
                SELECT capability_json FROM advertised_capabilities
                WHERE session_id=? AND capability_id=?
                """,
                (capability.session_id, capability.capability_id),
            ).fetchone()
            if existing is not None:
                current = CapabilityAnnouncement.from_dict(
                    json.loads(existing["capability_json"])
                )
                if (
                    current.node_id != capability.node_id
                    or current.type != capability.type
                ):
                    raise NodeStateError(
                        "capability-identity-conflict",
                        "capability_id",
                        "capability identity cannot change node or type",
                    )
            database.execute(
                """
                INSERT INTO advertised_capabilities(
                    session_id,capability_id,capability_json,updated_at
                ) VALUES(?,?,?,?)
                ON CONFLICT(session_id,capability_id) DO UPDATE SET
                    capability_json=excluded.capability_json,
                    updated_at=excluded.updated_at
                """,
                (
                    capability.session_id,
                    capability.capability_id,
                    encoded,
                    _time_text(now),
                ),
            )
        return capability

    def advertised_capabilities(
        self, *, session_id: str | None = None
    ) -> tuple[CapabilityAnnouncement, ...]:
        query = """
            SELECT capability_json FROM advertised_capabilities
        """
        arguments: tuple[str, ...] = ()
        if session_id is not None:
            query += " WHERE session_id=?"
            arguments = (_opaque_text(session_id, "session_id"),)
        query += " ORDER BY session_id,capability_id"
        with self._connect() as database:
            rows = database.execute(query, arguments).fetchall()
        try:
            capabilities = tuple(
                CapabilityAnnouncement.from_dict(
                    json.loads(row["capability_json"])
                )
                for row in rows
            )
        except (json.JSONDecodeError, FederationValidationError) as exc:
            raise NodeStateError(
                "malformed-node-state",
                "advertised_capabilities",
                "persisted capability is invalid",
            ) from exc
        return tuple(
            sorted(
                capabilities,
                key=lambda item: (
                    item.session_id,
                    item.type,
                    item.capability_id,
                ),
            )
        )

    def remove_capability(
        self, *, session_id: str, capability_id: str
    ) -> bool:
        session_id = _opaque_text(session_id, "session_id")
        capability_id = _opaque_text(capability_id, "capability_id")
        with self._transaction() as database:
            cursor = database.execute(
                """
                DELETE FROM advertised_capabilities
                WHERE session_id=? AND capability_id=?
                """,
                (session_id, capability_id),
            )
            return cursor.rowcount == 1

    def apply_event(
        self, event: SessionEvent, *, now: datetime
    ) -> EventApplyResult:
        if not isinstance(event, SessionEvent):
            raise NodeStateError(
                "invalid-event", "event", "must be a SessionEvent"
            )
        if event.revision <= 0:
            raise NodeStateError(
                "invalid-event-revision",
                "revision",
                "session event revisions begin at one",
            )
        try:
            encoded = json.dumps(
                event.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise NodeStateError(
                "invalid-event-payload",
                "payload",
                "event must contain only JSON-compatible values",
            ) from exc
        if len(encoded.encode("utf-8")) > MAX_EVENT_BYTES:
            raise NodeStateError(
                "event-too-large", "event", "event exceeds the durable size bound"
            )
        stamp = _time_text(now)
        with self._transaction() as database:
            session = self._require_joined(database, event.session_id)
            current_revision = session["last_applied_revision"]
            expected = current_revision + 1
            existing = database.execute(
                "SELECT * FROM applied_events WHERE event_id=?",
                (event.event_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["session_id"] == event.session_id
                    and existing["revision"] == event.revision
                    and existing["event_json"] == encoded
                ):
                    return EventApplyResult(
                        EventApplyStatus.DUPLICATE,
                        current_revision,
                        expected,
                    )
                raise NodeStateError(
                    "event-id-conflict",
                    "event_id",
                    "event ID was reused with different content",
                )
            revision_row = database.execute(
                """
                SELECT event_id FROM applied_events
                WHERE session_id=? AND revision=?
                """,
                (event.session_id, event.revision),
            ).fetchone()
            if revision_row is not None:
                raise NodeStateError(
                    "event-revision-conflict",
                    "revision",
                    "revision is already occupied by another event",
                )
            if event.revision > expected:
                previous_target = session["replay_target_revision"]
                target = max(event.revision, previous_target or 0)
                database.execute(
                    """
                    UPDATE joined_sessions
                    SET replaying=1,replay_target_revision=?
                    WHERE session_id=?
                    """,
                    (target, event.session_id),
                )
                self._mark_runtime_replaying(database, stamp)
                return EventApplyResult(
                    EventApplyStatus.GAP,
                    current_revision,
                    expected,
                )
            if event.revision < expected:
                raise NodeStateError(
                    "stale-event-revision",
                    "revision",
                    "event is behind durable applied state",
                )
            database.execute(
                """
                INSERT INTO applied_events(
                    event_id,session_id,revision,event_json,applied_at
                ) VALUES(?,?,?,?,?)
                """,
                (
                    event.event_id,
                    event.session_id,
                    event.revision,
                    encoded,
                    stamp,
                ),
            )
            target = session["replay_target_revision"]
            remains_replaying = bool(session["replaying"]) and (
                target is None or event.revision < target
            )
            database.execute(
                """
                UPDATE joined_sessions
                SET last_applied_revision=?,replaying=?,replay_target_revision=?
                WHERE session_id=?
                """,
                (
                    event.revision,
                    int(remains_replaying),
                    target if remains_replaying else None,
                    event.session_id,
                ),
            )
            if not remains_replaying:
                self._clear_runtime_replaying_if_complete(database, stamp)
            return EventApplyResult(
                EventApplyStatus.APPLIED,
                event.revision,
                event.revision + 1,
            )

    def applied_events(
        self, session_id: str, *, after_revision: int = 0
    ) -> tuple[SessionEvent, ...]:
        session_id = _opaque_text(session_id, "session_id")
        if (
            isinstance(after_revision, bool)
            or not isinstance(after_revision, int)
            or after_revision < 0
        ):
            raise NodeStateError(
                "invalid-revision",
                "after_revision",
                "must be a non-negative integer",
            )
        self.get_session(session_id)
        with self._connect() as database:
            rows = database.execute(
                """
                SELECT event_json FROM applied_events
                WHERE session_id=? AND revision>?
                ORDER BY revision
                """,
                (session_id, after_revision),
            )
            try:
                return tuple(
                    SessionEvent.from_dict(json.loads(row["event_json"]))
                    for row in rows
                )
            except (json.JSONDecodeError, FederationValidationError) as exc:
                raise NodeStateError(
                    "malformed-node-state",
                    "applied_events",
                    "persisted event is invalid",
                ) from exc

    def applied_event_page(
        self,
        session_id: str,
        *,
        after_revision: int,
        through_revision: int,
        limit: int,
    ) -> tuple[SessionEvent, ...]:
        """Read one bounded contiguous prefix of already-applied events."""

        session_id = _opaque_text(session_id, "session_id")
        if (
            isinstance(after_revision, bool)
            or not isinstance(after_revision, int)
            or after_revision < 0
        ):
            raise NodeStateError(
                "invalid-revision",
                "after_revision",
                "must be a non-negative integer",
            )
        if (
            isinstance(through_revision, bool)
            or not isinstance(through_revision, int)
            or through_revision < after_revision
        ):
            raise NodeStateError(
                "invalid-revision",
                "through_revision",
                "must be an integer at or after after_revision",
            )
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_EVENT_PAGE_ITEMS
        ):
            raise NodeStateError(
                "invalid-page-limit",
                "limit",
                f"must be between 1 and {MAX_EVENT_PAGE_ITEMS}",
            )
        self.get_session(session_id)
        with self._connect() as database:
            rows = database.execute(
                """
                SELECT event_json FROM applied_events
                WHERE session_id=? AND revision>? AND revision<=?
                ORDER BY revision
                LIMIT ?
                """,
                (session_id, after_revision, through_revision, limit),
            )
            try:
                return tuple(
                    SessionEvent.from_dict(json.loads(row["event_json"]))
                    for row in rows
                )
            except (json.JSONDecodeError, FederationValidationError) as exc:
                raise NodeStateError(
                    "malformed-node-state",
                    "applied_events",
                    "persisted event is invalid",
                ) from exc

    def status(self) -> dict[str, object]:
        """Return deterministic public state without paths, events, or credentials."""
        with self._read_transaction() as database:
            runtime = database.execute(
                "SELECT * FROM node_runtime WHERE singleton=1"
            ).fetchone()
            sessions = tuple(
                self._decode_session(row)
                for row in database.execute(
                    """
                    SELECT * FROM joined_sessions
                    WHERE membership_state='joined'
                    ORDER BY session_id
                    """
                )
            )
            capability_rows = database.execute(
                """
                SELECT capability_json FROM advertised_capabilities
                ORDER BY session_id,capability_id
                """
            ).fetchall()
        try:
            capabilities = sorted(
                (
                    CapabilityAnnouncement.from_dict(
                        json.loads(row["capability_json"])
                    )
                    for row in capability_rows
                ),
                key=lambda item: (
                    item.session_id,
                    item.type,
                    item.capability_id,
                ),
            )
        except (json.JSONDecodeError, FederationValidationError) as exc:
            raise NodeStateError(
                "malformed-node-state",
                "advertised_capabilities",
                "persisted capability is invalid",
            ) from exc
        return {
            "schema": "msh.node_status.v1",
            "node_id": runtime["node_id"],
            "enrollment_state": runtime["enrollment_state"],
            "connection_state": runtime["connection_state"],
            "last_heartbeat": runtime["last_heartbeat"],
            "last_error_code": runtime["last_error_code"],
            "joined_sessions": [
                {
                    "session_id": session.session_id,
                    "state": (
                        ConnectionState.REPLAYING.value
                        if session.replaying
                        else session.membership_state.value
                    ),
                    "last_applied_revision": session.last_applied_revision,
                    "replay_target_revision": session.replay_target_revision,
                }
                for session in sessions
            ],
            "locally_advertised_capabilities": [
                {
                    "session_id": capability.session_id,
                    "capability_id": capability.capability_id,
                    "type": capability.type,
                    "protocol": capability.protocol,
                    "protocol_version": capability.protocol_version,
                    "status": capability.status.value,
                    "properties": redact_secrets(capability.properties),
                }
                for capability in capabilities
            ],
        }

    @staticmethod
    def _decode_session(row: sqlite3.Row) -> JoinedSession:
        return JoinedSession(
            session_id=row["session_id"],
            membership_state=SessionMembershipState(row["membership_state"]),
            joined_at=_decode_time(row["joined_at"], "joined_at"),
            removed_at=(
                _decode_time(row["removed_at"], "removed_at")
                if row["removed_at"] is not None
                else None
            ),
            last_applied_revision=row["last_applied_revision"],
            replaying=bool(row["replaying"]),
            replay_target_revision=row["replay_target_revision"],
        )

    @staticmethod
    def _require_joined(
        database: sqlite3.Connection, session_id: str
    ) -> sqlite3.Row:
        row = database.execute(
            "SELECT * FROM joined_sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if (
            row is None
            or row["membership_state"] != SessionMembershipState.JOINED.value
        ):
            raise NodeStateError(
                "session-not-joined",
                "session_id",
                "event session is not joined",
            )
        return row

    @staticmethod
    def _mark_runtime_replaying(
        database: sqlite3.Connection, stamp: str
    ) -> None:
        database.execute(
            """
            UPDATE node_runtime
            SET connection_state='replaying',updated_at=?
            WHERE singleton=1 AND enrollment_state!='revoked'
            """,
            (stamp,),
        )

    @staticmethod
    def _clear_runtime_replaying_if_complete(
        database: sqlite3.Connection, stamp: str
    ) -> None:
        remaining = database.execute(
            """
            SELECT 1 FROM joined_sessions
            WHERE membership_state='joined' AND replaying=1
            LIMIT 1
            """
        ).fetchone()
        if remaining is None:
            database.execute(
                """
                UPDATE node_runtime
                SET connection_state='connected',updated_at=?
                WHERE singleton=1 AND connection_state='replaying'
                """,
                (stamp,),
            )
