"""Transactional SQLite durable outbox for local, offline-first delivery."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from .errors import FederationValidationError

SCHEMA_VERSION = 2
MAX_ERROR_LENGTH = 2048
MAX_PAYLOAD_BYTES = 1_048_576
MAX_COMPLETED_RECEIPT_BYTES = 4_096
MAX_COMPACTION_BATCH = 1_000
COMPLETED_RECEIPT_SCHEMA = "fcp.outbox.completed_receipt.v1"


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
        raise FederationValidationError(
            "invalid-timestamp", "now", "must be timezone-aware"
        )
    return value.astimezone(timezone.utc).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _completed_receipt_json(
    *,
    session_id: str,
    destination_id: str,
    schema_id: str,
    idempotency_key: str,
    content_hash: str,
) -> str:
    """Build bounded evidence while the authoritative identity stays in columns."""

    receipt = {
        "schema": COMPLETED_RECEIPT_SCHEMA,
        "schema_id": schema_id,
        "idempotency_key": idempotency_key,
        "content_hash": content_hash,
    }
    encoded = _canonical_json(receipt)
    if len(encoded.encode("utf-8")) <= MAX_COMPLETED_RECEIPT_BYTES:
        return encoded

    # Legacy databases may contain unbounded identity strings.  Hashing the
    # complete column identity keeps their receipts bounded without truncation.
    identity = _canonical_json(
        {
            "session_id": session_id,
            "destination_id": destination_id,
            "schema_id": schema_id,
            "idempotency_key": idempotency_key,
            "content_hash": content_hash,
        }
    ).encode("utf-8")
    return _canonical_json(
        {
            "schema": COMPLETED_RECEIPT_SCHEMA,
            "identity_hash": f"sha256:{hashlib.sha256(identity).hexdigest()}",
        }
    )


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
                INSERT OR IGNORE INTO outbox_schema(singleton, version) VALUES(1, 2);
                CREATE TABLE IF NOT EXISTS outbox (
                    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL CHECK(length(session_id) > 0),
                    destination_id TEXT NOT NULL CHECK(length(destination_id) > 0),
                    schema_id TEXT NOT NULL CHECK(length(schema_id) > 0),
                    payload_json TEXT NOT NULL CHECK(length(payload_json) <= 1048576 AND json_valid(payload_json)),
                    idempotency_key TEXT NOT NULL CHECK(length(idempotency_key) > 0),
                    content_hash TEXT NOT NULL CHECK(length(content_hash) > 0),
                    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('prepared','pending','completed')),
                    payload_compacted INTEGER NOT NULL DEFAULT 0
                        CHECK(
                            payload_compacted IN (0, 1)
                            AND (payload_compacted = 0 OR state = 'completed')
                        ),
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, next_attempt_at TEXT NOT NULL,
                    last_error TEXT CHECK(last_error IS NULL OR length(last_error) <= 2048),
                    UNIQUE(session_id, destination_id, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS outbox_pending_due
                    ON outbox(state, next_attempt_at, outbox_id);
            """)
            db.execute("BEGIN IMMEDIATE")
            try:
                schema_row = db.execute(
                    "SELECT version FROM outbox_schema WHERE singleton=1"
                ).fetchone()
                if schema_row is None:
                    raise FederationValidationError(
                        "unsupported-outbox-schema",
                        "version",
                        "schema version row is missing",
                    )
                version = schema_row["version"]
                columns = {
                    row["name"]
                    for row in db.execute("PRAGMA table_info(outbox)").fetchall()
                }
                if version == 1:
                    if "payload_compacted" not in columns:
                        db.execute(
                            "ALTER TABLE outbox ADD COLUMN payload_compacted "
                            "INTEGER NOT NULL DEFAULT 0 "
                            "CHECK(payload_compacted IN (0, 1) AND "
                            "(payload_compacted = 0 OR state = 'completed'))"
                        )
                    db.execute(
                        "UPDATE outbox_schema SET version=? WHERE singleton=1",
                        (SCHEMA_VERSION,),
                    )
                    version = SCHEMA_VERSION
                    columns.add("payload_compacted")
                if version != SCHEMA_VERSION or "payload_compacted" not in columns:
                    raise FederationValidationError(
                        "unsupported-outbox-schema",
                        "version",
                        str(version),
                    )
                db.commit()
            except Exception:
                db.rollback()
                raise

    def enqueue(
        self,
        *,
        session_id: str,
        destination_id: str,
        schema_id: str,
        payload: Any,
        idempotency_key: str,
        content_hash: str,
        now: datetime,
    ) -> tuple[OutboxEntry, bool]:
        for field, value in (
            ("session_id", session_id),
            ("destination_id", destination_id),
            ("schema_id", schema_id),
            ("idempotency_key", idempotency_key),
            ("content_hash", content_hash),
        ):
            if not isinstance(value, str) or not value:
                raise FederationValidationError(
                    "invalid-id", field, "must be non-empty text"
                )
        try:
            encoded = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-json", "payload", "must be JSON-compatible"
            ) from exc
        if len(encoded.encode()) > MAX_PAYLOAD_BYTES:
            raise FederationValidationError(
                "payload-too-large", "payload", "exceeds durable bound"
            )
        return self._insert(
            session_id=session_id,
            destination_id=destination_id,
            schema_id=schema_id,
            payload_json=encoded,
            idempotency_key=idempotency_key,
            content_hash=content_hash,
            state=OutboxState.PENDING,
            now=now,
        )

    def prepare(
        self,
        *,
        session_id: str,
        destination_id: str,
        schema_id: str,
        payload: Any,
        idempotency_key: str,
        content_hash: str,
        now: datetime,
    ) -> tuple[OutboxEntry, bool]:
        """Durably record immutable routing intent without making it deliverable."""
        # Reuse enqueue's validation without allowing a transient pending row.
        for field, value in (
            ("session_id", session_id),
            ("destination_id", destination_id),
            ("schema_id", schema_id),
            ("idempotency_key", idempotency_key),
            ("content_hash", content_hash),
        ):
            if not isinstance(value, str) or not value:
                raise FederationValidationError(
                    "invalid-id", field, "must be non-empty text"
                )
        try:
            encoded = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-json", "payload", "must be JSON-compatible"
            ) from exc
        if len(encoded.encode()) > MAX_PAYLOAD_BYTES:
            raise FederationValidationError(
                "payload-too-large", "payload", "exceeds durable bound"
            )
        return self._insert(
            session_id=session_id,
            destination_id=destination_id,
            schema_id=schema_id,
            payload_json=encoded,
            idempotency_key=idempotency_key,
            content_hash=content_hash,
            state=OutboxState.PREPARED,
            now=now,
        )

    def _insert(
        self,
        *,
        session_id: str,
        destination_id: str,
        schema_id: str,
        payload_json: str,
        idempotency_key: str,
        content_hash: str,
        state: OutboxState,
        now: datetime,
    ) -> tuple[OutboxEntry, bool]:
        timestamp = _time(now)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                cursor = db.execute(
                    """INSERT INTO outbox
                    (session_id,destination_id,schema_id,payload_json,idempotency_key,content_hash,
                     state,created_at,updated_at,next_attempt_at) VALUES(?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(session_id,destination_id,idempotency_key) DO NOTHING""",
                    (
                        session_id,
                        destination_id,
                        schema_id,
                        payload_json,
                        idempotency_key,
                        content_hash,
                        state.value,
                        timestamp,
                        timestamp,
                        timestamp,
                    ),
                )
                row = db.execute(
                    "SELECT * FROM outbox WHERE session_id=? AND destination_id=? AND idempotency_key=?",
                    (session_id, destination_id, idempotency_key),
                ).fetchone()
                completed = row["state"] == OutboxState.COMPLETED.value
                if (
                    row["content_hash"] != content_hash
                    or row["schema_id"] != schema_id
                    or (not completed and row["payload_json"] != payload_json)
                ):
                    raise FederationValidationError(
                        "idempotency-conflict",
                        "idempotency_key",
                        "identity was reused with different content",
                    )
                if (
                    state is OutboxState.PENDING
                    and row["state"] == OutboxState.PREPARED.value
                ):
                    db.execute(
                        "UPDATE outbox SET state='pending',updated_at=?,next_attempt_at=? "
                        "WHERE outbox_id=?",
                        (timestamp, timestamp, row["outbox_id"]),
                    )
                    row = db.execute(
                        "SELECT * FROM outbox WHERE outbox_id=?", (row["outbox_id"],)
                    ).fetchone()
                db.commit()
                return self._decode(row), cursor.rowcount == 1
            except Exception:
                db.rollback()
                raise

    def prepared(self) -> tuple[OutboxEntry, ...]:
        with self._connect() as db:
            return tuple(
                self._decode(row)
                for row in db.execute(
                    "SELECT * FROM outbox WHERE state='prepared' ORDER BY outbox_id"
                )
            )

    def activate(self, outbox_id: int, *, now: datetime) -> OutboxEntry:
        """Atomically make a prepared intent deliverable; safe to repeat."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT state FROM outbox WHERE outbox_id=?", (outbox_id,)
            ).fetchone()
            if row is None:
                raise FederationValidationError(
                    "outbox-not-found", "outbox_id", "does not exist"
                )
            if row["state"] == OutboxState.PREPARED.value:
                stamp = _time(now)
                db.execute(
                    "UPDATE outbox SET state='pending',updated_at=?,next_attempt_at=? "
                    "WHERE outbox_id=?",
                    (stamp, stamp, outbox_id),
                )
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
            row = db.execute(
                "SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)
            ).fetchone()
        return self._decode(row) if row else None

    def acknowledge(self, outbox_id: int, *, now: datetime) -> OutboxEntry:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)
                ).fetchone()
                if row is None:
                    raise FederationValidationError(
                        "outbox-not-found", "outbox_id", "does not exist"
                    )
                if row["state"] == OutboxState.PREPARED.value:
                    raise FederationValidationError(
                        "outbox-not-pending",
                        "outbox_id",
                        "prepared entry cannot be acknowledged",
                    )
                receipt_json = self._receipt_for_row(row)
                compacted = row["payload_compacted"]
                if compacted not in (0, 1):
                    raise FederationValidationError(
                        "malformed-outbox-row",
                        "outbox",
                        "payload compaction marker is invalid",
                    )
                if row["state"] == OutboxState.COMPLETED.value:
                    if compacted == 1 and row["payload_json"] != receipt_json:
                        raise FederationValidationError(
                            "malformed-outbox-row",
                            "outbox",
                            "completed receipt does not match immutable identity",
                        )
                    if compacted == 0:
                        db.execute(
                            "UPDATE outbox SET payload_json=?,payload_compacted=1 "
                            "WHERE outbox_id=? AND state='completed' "
                            "AND payload_compacted=0",
                            (receipt_json, outbox_id),
                        )
                else:
                    db.execute(
                        "UPDATE outbox SET state='completed',payload_json=?,"
                        "payload_compacted=1,updated_at=?,last_error=NULL "
                        "WHERE outbox_id=? AND state='pending'",
                        (receipt_json, _time(now), outbox_id),
                    )
                completed = db.execute(
                    "SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)
                ).fetchone()
                result = self._decode(completed)
                db.commit()
                return result
            except Exception:
                db.rollback()
                raise

    def compact_completed(self, *, limit: int = 100) -> int:
        """Replace legacy completed payloads with bounded receipts.

        The method performs no deletion or vacuuming.  It preserves completion
        timestamps and processes at most ``MAX_COMPACTION_BATCH`` rows in one
        immediate transaction so callers can schedule it incrementally.
        """

        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise FederationValidationError(
                "invalid-compaction-limit",
                "limit",
                "must be a positive integer",
            )
        bounded_limit = min(limit, MAX_COMPACTION_BATCH)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                rows = db.execute(
                    "SELECT * FROM outbox "
                    "WHERE state='completed' AND payload_compacted=0 "
                    "ORDER BY outbox_id LIMIT ?",
                    (bounded_limit,),
                ).fetchall()
                compacted = 0
                for row in rows:
                    cursor = db.execute(
                        "UPDATE outbox SET payload_json=?,payload_compacted=1 "
                        "WHERE outbox_id=? AND state='completed' "
                        "AND payload_compacted=0",
                        (self._receipt_for_row(row), row["outbox_id"]),
                    )
                    compacted += cursor.rowcount
                db.commit()
                return compacted
            except Exception:
                db.rollback()
                raise

    def record_failure(
        self,
        outbox_id: int,
        *,
        error: str,
        now: datetime,
        base_delay_seconds: int = 1,
        max_delay_seconds: int = 3600,
    ) -> OutboxEntry:
        if base_delay_seconds <= 0 or max_delay_seconds <= 0:
            raise FederationValidationError(
                "invalid-backoff",
                "base_delay_seconds",
                "backoff bounds must be positive",
            )
        summary = str(error)[:MAX_ERROR_LENGTH]
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)
            ).fetchone()
            if row is None:
                raise FederationValidationError(
                    "outbox-not-found", "outbox_id", "does not exist"
                )
            if row["state"] == OutboxState.PREPARED.value:
                raise FederationValidationError(
                    "outbox-not-pending",
                    "outbox_id",
                    "prepared entry cannot record delivery failure",
                )
            if row["state"] == OutboxState.COMPLETED.value:
                raise FederationValidationError(
                    "outbox-completed", "outbox_id", "cannot retry completed entry"
                )
            attempts = row["attempt_count"] + 1
            delay = min(
                max_delay_seconds, base_delay_seconds * (2 ** min(attempts - 1, 30))
            )
            next_at = now + timedelta(seconds=delay)
            db.execute(
                "UPDATE outbox SET attempt_count=?,last_error=?,updated_at=?,next_attempt_at=? WHERE outbox_id=?",
                (attempts, summary, _time(now), _time(next_at), outbox_id),
            )
            db.commit()
        return self.get(outbox_id)  # type: ignore[return-value]

    @staticmethod
    def _receipt_for_row(row: sqlite3.Row) -> str:
        return _completed_receipt_json(
            session_id=row["session_id"],
            destination_id=row["destination_id"],
            schema_id=row["schema_id"],
            idempotency_key=row["idempotency_key"],
            content_hash=row["content_hash"],
        )

    @staticmethod
    def _decode(row: sqlite3.Row) -> OutboxEntry:
        try:
            parsed_times = [
                datetime.fromisoformat(row[name])
                for name in ("created_at", "updated_at", "next_attempt_at")
            ]
            if any(
                value.tzinfo is None or value.utcoffset() is None
                for value in parsed_times
            ):
                raise ValueError("persisted timestamps must be timezone-aware")
            state = OutboxState(row["state"])
            payload_compacted = row["payload_compacted"]
            if payload_compacted not in (0, 1):
                raise ValueError("payload compaction marker is invalid")
            if payload_compacted == 1:
                if state is not OutboxState.COMPLETED:
                    raise ValueError("only completed rows may contain receipts")
                expected_receipt = SQLiteOutbox._receipt_for_row(row)
                if row["payload_json"] != expected_receipt:
                    raise ValueError(
                        "completed receipt does not match immutable identity"
                    )
            return OutboxEntry(
                row["outbox_id"],
                row["session_id"],
                row["destination_id"],
                row["schema_id"],
                json.loads(row["payload_json"]),
                row["idempotency_key"],
                row["content_hash"],
                state,
                row["attempt_count"],
                parsed_times[0],
                parsed_times[1],
                parsed_times[2],
                row["last_error"],
            )
        except (
            IndexError,
            KeyError,
            ValueError,
            TypeError,
            json.JSONDecodeError,
        ) as exc:
            raise FederationValidationError(
                "malformed-outbox-row", "outbox", str(exc)
            ) from exc
