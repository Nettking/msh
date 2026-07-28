"""Transactional SQLite durable outbox for local, offline-first delivery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import json
import sqlite3
from pathlib import Path
from typing import Any

from .errors import FederationValidationError

SCHEMA_VERSION = 1
MAX_ERROR_LENGTH = 2048
MAX_PAYLOAD_BYTES = 1_048_576


class OutboxState(str, Enum):
    PREPARED = "prepared"
    PENDING = "pending"
    COMPLETED = "completed"


@dataclass(frozen=True)
class OutboxEntry:
    outbox_id: int
    session_id: str
    destination_id: str
    schema_id: str
    payload: Any
    idempotency_key: str
    content_hash: str
    state: OutboxState
    attempt_count: int
    created_at: datetime
    updated_at: datetime
    next_attempt_at: datetime
    last_error: str | None


def _time(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError("invalid-timestamp", "now", "must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()


class SQLiteOutbox:
    """Each mutation uses BEGIN IMMEDIATE; no delivery item is destructively claimed."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def initialize(self) -> None:
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS outbox_schema (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    version INTEGER NOT NULL CHECK (version > 0)
                );
                INSERT OR IGNORE INTO outbox_schema(singleton, version) VALUES(1, 1);
                CREATE TABLE IF NOT EXISTS outbox (
                    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL CHECK(length(session_id) > 0),
                    destination_id TEXT NOT NULL CHECK(length(destination_id) > 0),
                    schema_id TEXT NOT NULL CHECK(length(schema_id) > 0),
                    payload_json TEXT NOT NULL CHECK(length(payload_json) <= 1048576 AND json_valid(payload_json)),
                    idempotency_key TEXT NOT NULL CHECK(length(idempotency_key) > 0),
                    content_hash TEXT NOT NULL CHECK(length(content_hash) > 0),
                    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('prepared','pending','completed')),
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, next_attempt_at TEXT NOT NULL,
                    last_error TEXT CHECK(last_error IS NULL OR length(last_error) <= 2048),
                    UNIQUE(session_id, destination_id, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS outbox_pending_due
                    ON outbox(state, next_attempt_at, outbox_id);
            """)
            version = db.execute("SELECT version FROM outbox_schema WHERE singleton=1").fetchone()[0]
            if version != SCHEMA_VERSION:
                raise FederationValidationError("unsupported-outbox-schema", "version", str(version))

    def enqueue(self, *, session_id: str, destination_id: str, schema_id: str,
                payload: Any, idempotency_key: str, content_hash: str,
                now: datetime) -> tuple[OutboxEntry, bool]:
        for field, value in (("session_id", session_id), ("destination_id", destination_id),
                             ("schema_id", schema_id), ("idempotency_key", idempotency_key),
                             ("content_hash", content_hash)):
            if not isinstance(value, str) or not value:
                raise FederationValidationError("invalid-id", field, "must be non-empty text")
        try:
            encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                 separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise FederationValidationError("invalid-json", "payload",
                                            "must be JSON-compatible") from exc
        if len(encoded.encode()) > MAX_PAYLOAD_BYTES:
            raise FederationValidationError("payload-too-large", "payload", "exceeds durable bound")
        return self._insert(session_id=session_id, destination_id=destination_id,
                            schema_id=schema_id, payload_json=encoded,
                            idempotency_key=idempotency_key, content_hash=content_hash,
                            state=OutboxState.PENDING, now=now)

    def prepare(self, *, session_id: str, destination_id: str, schema_id: str,
                payload: Any, idempotency_key: str, content_hash: str,
                now: datetime) -> tuple[OutboxEntry, bool]:
        """Durably record immutable routing intent without making it deliverable."""
        # Reuse enqueue's validation without allowing a transient pending row.
        for field, value in (("session_id", session_id), ("destination_id", destination_id),
                             ("schema_id", schema_id), ("idempotency_key", idempotency_key),
                             ("content_hash", content_hash)):
            if not isinstance(value, str) or not value:
                raise FederationValidationError("invalid-id", field, "must be non-empty text")
        try:
            encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                 separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise FederationValidationError("invalid-json", "payload",
                                            "must be JSON-compatible") from exc
        if len(encoded.encode()) > MAX_PAYLOAD_BYTES:
            raise FederationValidationError("payload-too-large", "payload", "exceeds durable bound")
        return self._insert(session_id=session_id, destination_id=destination_id,
                            schema_id=schema_id, payload_json=encoded,
                            idempotency_key=idempotency_key, content_hash=content_hash,
                            state=OutboxState.PREPARED, now=now)

    def _insert(self, *, session_id: str, destination_id: str, schema_id: str,
                payload_json: str, idempotency_key: str, content_hash: str,
                state: OutboxState, now: datetime) -> tuple[OutboxEntry, bool]:
        timestamp = _time(now)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                cursor = db.execute("""INSERT INTO outbox
                    (session_id,destination_id,schema_id,payload_json,idempotency_key,content_hash,
                     state,created_at,updated_at,next_attempt_at) VALUES(?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(session_id,destination_id,idempotency_key) DO NOTHING""",
                    (session_id, destination_id, schema_id, payload_json, idempotency_key,
                     content_hash, state.value, timestamp, timestamp, timestamp))
                row = db.execute("SELECT * FROM outbox WHERE session_id=? AND destination_id=? AND idempotency_key=?",
                                 (session_id, destination_id, idempotency_key)).fetchone()
                if (row["content_hash"] != content_hash or row["schema_id"] != schema_id
                        or row["payload_json"] != payload_json):
                    raise FederationValidationError("idempotency-conflict", "idempotency_key",
                                                    "identity was reused with different content")
                if (state is OutboxState.PENDING
                        and row["state"] == OutboxState.PREPARED.value):
                    db.execute("UPDATE outbox SET state='pending',updated_at=?,next_attempt_at=? "
                               "WHERE outbox_id=?", (timestamp, timestamp, row["outbox_id"]))
                    row = db.execute("SELECT * FROM outbox WHERE outbox_id=?",
                                     (row["outbox_id"],)).fetchone()
                db.commit()
                return self._decode(row), cursor.rowcount == 1
            except Exception:
                db.rollback()
                raise

    def prepared(self) -> tuple[OutboxEntry, ...]:
        with self._connect() as db:
            return tuple(self._decode(row) for row in db.execute(
                "SELECT * FROM outbox WHERE state='prepared' ORDER BY outbox_id"))

    def activate(self, outbox_id: int, *, now: datetime) -> OutboxEntry:
        """Atomically make a prepared intent deliverable; safe to repeat."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state FROM outbox WHERE outbox_id=?",
                             (outbox_id,)).fetchone()
            if row is None:
                raise FederationValidationError("outbox-not-found", "outbox_id", "does not exist")
            if row["state"] == OutboxState.PREPARED.value:
                stamp = _time(now)
                db.execute("UPDATE outbox SET state='pending',updated_at=?,next_attempt_at=? "
                           "WHERE outbox_id=?", (stamp, stamp, outbox_id))
            db.commit()
        return self.get(outbox_id)  # type: ignore[return-value]

    def pending(self, *, now: datetime | None = None) -> tuple[OutboxEntry, ...]:
        query = "SELECT * FROM outbox WHERE state='pending'"
        args: tuple[str, ...] = ()
        if now is not None:
            query += " AND next_attempt_at<=?"
            args = (_time(now),)
        query += " ORDER BY next_attempt_at,outbox_id"
        with self._connect() as db:
            return tuple(self._decode(row) for row in db.execute(query, args))

    def get(self, outbox_id: int) -> OutboxEntry | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
        return self._decode(row) if row else None

    def acknowledge(self, outbox_id: int, *, now: datetime) -> OutboxEntry:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
            if row is None:
                raise FederationValidationError("outbox-not-found", "outbox_id", "does not exist")
            if row["state"] == OutboxState.PREPARED.value:
                raise FederationValidationError("outbox-not-pending", "outbox_id",
                                                "prepared entry cannot be acknowledged")
            if row["state"] != OutboxState.COMPLETED.value:
                db.execute("UPDATE outbox SET state='completed',updated_at=?,last_error=NULL WHERE outbox_id=?",
                           (_time(now), outbox_id))
            db.commit()
        return self.get(outbox_id)  # type: ignore[return-value]

    def record_failure(self, outbox_id: int, *, error: str, now: datetime,
                       base_delay_seconds: int = 1, max_delay_seconds: int = 3600) -> OutboxEntry:
        if base_delay_seconds <= 0 or max_delay_seconds <= 0:
            raise FederationValidationError("invalid-backoff", "base_delay_seconds",
                                            "backoff bounds must be positive")
        summary = str(error)[:MAX_ERROR_LENGTH]
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
            if row is None:
                raise FederationValidationError("outbox-not-found", "outbox_id", "does not exist")
            if row["state"] == OutboxState.PREPARED.value:
                raise FederationValidationError("outbox-not-pending", "outbox_id",
                                                "prepared entry cannot record delivery failure")
            if row["state"] == OutboxState.COMPLETED.value:
                raise FederationValidationError("outbox-completed", "outbox_id", "cannot retry completed entry")
            attempts = row["attempt_count"] + 1
            delay = min(max_delay_seconds, base_delay_seconds * (2 ** min(attempts - 1, 30)))
            next_at = now + timedelta(seconds=delay)
            db.execute("UPDATE outbox SET attempt_count=?,last_error=?,updated_at=?,next_attempt_at=? WHERE outbox_id=?",
                       (attempts, summary, _time(now), _time(next_at), outbox_id))
            db.commit()
        return self.get(outbox_id)  # type: ignore[return-value]

    @staticmethod
    def _decode(row: sqlite3.Row) -> OutboxEntry:
        try:
            parsed_times = [datetime.fromisoformat(row[name]) for name in
                            ("created_at", "updated_at", "next_attempt_at")]
            if any(value.tzinfo is None or value.utcoffset() is None for value in parsed_times):
                raise ValueError("persisted timestamps must be timezone-aware")
            return OutboxEntry(row["outbox_id"], row["session_id"], row["destination_id"],
                row["schema_id"], json.loads(row["payload_json"]), row["idempotency_key"],
                row["content_hash"], OutboxState(row["state"]), row["attempt_count"],
                parsed_times[0], parsed_times[1], parsed_times[2], row["last_error"])
        except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise FederationValidationError("malformed-outbox-row", "outbox", str(exc)) from exc
