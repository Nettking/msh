"""Transactional SQLite durable outbox for local, offline-first delivery."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Callable, Iterable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any

from .errors import FederationValidationError
from .host_resources import ProcessResourceAdmission
from .process_resource_admission import PROCESS_RESOURCE_ADMISSION

SCHEMA_VERSION = 3
MAX_ERROR_LENGTH = 2048
MAX_PAYLOAD_BYTES = 1_048_576
MAX_COMPLETED_RECEIPT_BYTES = 4_096
MAX_COMPACTION_BATCH = 1_000
MAX_RETIRED_SUMMARY_DATASETS = 1_000
MAX_RETIREMENT_REASON_LENGTH = 64
MAX_RETIREMENT_DATASET_LENGTH = 512
COMPLETED_RECEIPT_SCHEMA = "fcp.outbox.completed_receipt.v1"
_OUTBOX_MUTATION_FIXED_BYTES = 2 * 1024 * 1024
_OUTBOX_MUTATION_FIXED_INODES = 4
_OUTBOX_COMPACTION_BYTES = 8 * 1024 * 1024
_OUTBOX_MAX_MIGRATION_BYTES = 2 * 1024 * 1024 * 1024


def _admit_mutation(*, bytes_required: int) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Wrap one SQLite mutation in bounded process-wide admission."""

    def decorate(method: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(method)
        def admitted(self: Any, *args: Any, **kwargs: Any) -> Any:
            with self._mutation_admission(bytes_required=bytes_required):
                return method(self, *args, **kwargs)

        return admitted

    return decorate

#: A retirement reason is an operator-facing classification, never an error
#: message.  Keeping it a bounded lowercase token means it can be persisted,
#: compared, indexed and rendered without ever carrying remote text.  The
#: free-text cause stays in ``last_error``, which retirement preserves.
RETIREMENT_REASON = re.compile(
    rf"\A[a-z][a-z0-9-]{{0,{MAX_RETIREMENT_REASON_LENGTH - 1}}}\Z"
)


class OutboxState(str, Enum):
    PREPARED = "prepared"
    PENDING = "pending"
    COMPLETED = "completed"
    #: Terminal, deliberate isolation.  A retired row is durable evidence that
    #: this exact delivery identity was permanently withdrawn from delivery.
    #: It is never produced by a retryable failure, never reached by timing out
    #: or by counting attempts, and never deleted -- the row itself is the
    #: tombstone that stops reconciliation re-enqueuing the same evidence as
    #: new work forever.
    RETIRED = "retired"


#: Rows that will never be attempted again.  Both keep their identity columns.
TERMINAL_STATES = frozenset({OutboxState.COMPLETED, OutboxState.RETIRED})


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
    # Retirement metadata.  Present exactly when ``state`` is RETIRED, so a
    # reader cannot see a half-written tombstone.  ``retirement_dataset_id``
    # records which ordered dataset this withdrawal punched a hole in; it is a
    # column rather than payload so it survives receipt compaction, which is
    # what lets an operator still answer "where is the gap?" afterwards.  The
    # skipped sequence span stays recoverable from ``idempotency_key``, which
    # is immutable identity and is never compacted away.
    retired_at: datetime | None = None
    retirement_reason: str | None = None
    retirement_dataset_id: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES


@dataclass(frozen=True)
class RetiredDatasetCount:
    """How many retired rows one ordered dataset carries."""

    dataset_id: str | None
    rows: int


@dataclass(frozen=True)
class RetiredSummary:
    """A bounded, durably-derived view of terminal isolation.

    ``datasets`` is truncated to the caller's limit while ``total`` counts
    every matching row, so a health surface can report "degraded" truthfully
    without ever materialising an unbounded result set.
    """

    total: int
    datasets: tuple[RetiredDatasetCount, ...]
    truncated: bool


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


_OUTBOX_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS outbox_pending_due "
    "ON outbox(state, next_attempt_at, outbox_id);"
)

# Dataset identity lives inside the immutable outbox JSON payload. The fair
# delivery query uses this exact expression to find the oldest row for each
# ordered dataset. Keeping it indexed avoids decoding/sorting every pending
# payload on every cycle when a large offline backlog has accumulated.
_OUTBOX_DELIVERY_DATASET_KEY_SQL = """
CASE
    WHEN json_type(payload_json, '$.dataset_id') = 'text'
        AND length(json_extract(payload_json, '$.dataset_id')) > 0
    THEN json_extract(payload_json, '$.dataset_id')
    ELSE printf('__unkeyed-outbox-row:%lld', outbox_id)
END
""".strip()
_OUTBOX_DELIVERY_INDEX_DDL = f"""
CREATE INDEX IF NOT EXISTS outbox_pending_delivery_dataset
ON outbox(
    state,
    session_id,
    destination_id,
    schema_id,
    {_OUTBOX_DELIVERY_DATASET_KEY_SQL},
    outbox_id
)
WHERE state = 'pending';
"""


def _outbox_table_ddl(name: str, *, if_not_exists: bool) -> str:
    """Return the current outbox DDL.

    One definition serves both a fresh database and the v2 rebuild, so the
    migrated table can never drift from the table a new install creates.  The
    retirement columns are constrained to be present exactly when the row is
    retired: a half-written tombstone is rejected by the database itself, not
    only by the decoder.
    """

    guard = "IF NOT EXISTS " if if_not_exists else ""
    return f"""
                CREATE TABLE {guard}{name} (
                    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL CHECK(length(session_id) > 0),
                    destination_id TEXT NOT NULL CHECK(length(destination_id) > 0),
                    schema_id TEXT NOT NULL CHECK(length(schema_id) > 0),
                    payload_json TEXT NOT NULL CHECK(length(payload_json) <= {MAX_PAYLOAD_BYTES} AND json_valid(payload_json)),
                    idempotency_key TEXT NOT NULL CHECK(length(idempotency_key) > 0),
                    content_hash TEXT NOT NULL CHECK(length(content_hash) > 0),
                    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('prepared','pending','completed','retired')),
                    payload_compacted INTEGER NOT NULL DEFAULT 0
                        CHECK(
                            payload_compacted IN (0, 1)
                            AND (
                                payload_compacted = 0
                                OR state IN ('completed','retired')
                            )
                        ),
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, next_attempt_at TEXT NOT NULL,
                    last_error TEXT CHECK(last_error IS NULL OR length(last_error) <= {MAX_ERROR_LENGTH}),
                    retired_at TEXT
                        CHECK((state = 'retired') = (retired_at IS NOT NULL)),
                    retirement_reason TEXT
                        CHECK(
                            (state = 'retired') = (retirement_reason IS NOT NULL)
                            AND (
                                retirement_reason IS NULL
                                OR length(retirement_reason)
                                    BETWEEN 1 AND {MAX_RETIREMENT_REASON_LENGTH}
                            )
                        ),
                    retirement_dataset_id TEXT
                        CHECK(
                            (state = 'retired' OR retirement_dataset_id IS NULL)
                            AND (
                                retirement_dataset_id IS NULL
                                OR length(retirement_dataset_id)
                                    BETWEEN 1 AND {MAX_RETIREMENT_DATASET_LENGTH}
                            )
                        ),
                    UNIQUE(session_id, destination_id, idempotency_key)
                );
    """


class SQLiteOutbox:
    """Each mutation uses BEGIN IMMEDIATE; no delivery item is destructively claimed."""

    def __init__(
        self,
        database: Path | str,
        *,
        resource_admission: ProcessResourceAdmission | None = None,
    ) -> None:
        self.database = str(database)
        self.database_path = Path(self.database)
        self.resource_admission = resource_admission or PROCESS_RESOURCE_ADMISSION
        with self._reserve_resources(
            self._migration_requirements(),
        ) as reservations:
            self.database_path.parent.mkdir(parents=True, exist_ok=True)
            self._assert_resource_identity(reservations)
            self._initialize_unadmitted()
            self._assert_resource_identity(reservations)

    def _database_bytes(self) -> int:
        total = 0
        for suffix in ("", "-wal", "-shm"):
            try:
                total += int(Path(f"{self.database}{suffix}").stat().st_size)
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise FederationValidationError(
                    "outbox-resource-measurement-failed",
                    "database",
                    "could not measure the durable outbox before admission",
                ) from exc
        return total

    def _migration_requirements(self) -> tuple[tuple[Path, int, int], ...]:
        existing = self._database_bytes()
        if existing > _OUTBOX_MAX_MIGRATION_BYTES:
            raise FederationValidationError(
                "outbox-resource-envelope",
                "database",
                "durable outbox is too large for a bounded startup migration",
            )
        # A v2-to-v3 migration rebuilds the table while retaining the old one
        # until the transactional DROP. Reserve the old database plus a second
        # bounded copy, WAL/journal headroom, and the atomic temp identities.
        return (
            (
                self.database_path.parent,
                max(
                    _OUTBOX_MUTATION_FIXED_BYTES,
                    (2 * existing) + _OUTBOX_MUTATION_FIXED_BYTES,
                ),
                _OUTBOX_MUTATION_FIXED_INODES + 2,
            ),
        )

    @contextmanager
    def _reserve_resources(
        self,
        requirements: Iterable[tuple[Path, int, int]],
    ) -> Iterator[tuple[object, ...]]:
        reserve_many = getattr(self.resource_admission, "reserve_many", None)
        if callable(reserve_many):
            with reserve_many(requirements) as reservations:
                yield tuple(reservations)
            return
        with ExitStack() as stack:
            reservations = tuple(
                stack.enter_context(
                    self.resource_admission.reserve(
                        path,
                        bytes_required=bytes_required,
                        inodes_required=inodes_required,
                    )
                )
                for path, bytes_required, inodes_required in requirements
            )
            yield reservations

    def _assert_resource_identity(self, reservations: tuple[object, ...]) -> None:
        measure = getattr(self.resource_admission, "_measure", None)
        if not callable(measure):
            return
        resource_ids = {
            str(reservation.resource_id) for reservation in reservations
        }
        measurement = measure(self.database_path.parent)
        if not measurement.available or measurement.resource_id not in resource_ids:
            raise FederationValidationError(
                "outbox-backing-resource-changed",
                "database",
                "outbox path no longer resolves to its admitted backing resource",
            )

    @contextmanager
    def _mutation_admission(
        self,
        *,
        bytes_required: int = _OUTBOX_MUTATION_FIXED_BYTES,
        inodes_required: int = _OUTBOX_MUTATION_FIXED_INODES,
    ) -> Iterator[None]:
        with self._reserve_resources(
            ((self.database_path.parent, bytes_required, inodes_required),)
        ) as reservations:
            self._assert_resource_identity(reservations)
            try:
                yield
            finally:
                self._assert_resource_identity(reservations)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA wal_autocheckpoint=100")
        connection.execute("PRAGMA journal_size_limit=1048576")
        return connection

    def initialize(self) -> None:
        """Reconcile the schema under the same bounded startup admission."""

        with self._reserve_resources(self._migration_requirements()) as reservations:
            self._assert_resource_identity(reservations)
            self._initialize_unadmitted()
            self._assert_resource_identity(reservations)

    def _initialize_unadmitted(self) -> None:
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(f"""
                CREATE TABLE IF NOT EXISTS outbox_schema (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    version INTEGER NOT NULL CHECK (version > 0)
                );
                INSERT OR IGNORE INTO outbox_schema(singleton, version)
                    VALUES(1, {SCHEMA_VERSION});
                {_outbox_table_ddl("outbox", if_not_exists=True)}
                {_OUTBOX_INDEX_DDL}
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
                    db.execute("UPDATE outbox_schema SET version=2 WHERE singleton=1")
                    version = 2
                    columns.add("payload_compacted")
                if version == 2:
                    # SQLite cannot widen a CHECK constraint in place, and the
                    # v2 table pins ``state`` to three values.  Rebuild it so
                    # 'retired' is a real state rather than a flag some reader
                    # can forget to join against: a row that is not deliverable
                    # must not be able to look deliverable to any query.
                    #
                    # The whole rebuild runs inside this one BEGIN IMMEDIATE,
                    # and SQLite DDL is transactional, so a crash at any point
                    # rolls back to an intact v2 database and the migration is
                    # simply retried on the next open.  Explicit outbox_id
                    # values are carried over, which also carries the
                    # AUTOINCREMENT high-water mark to the rebuilt table.
                    self._migrate_v2_to_v3(db)
                    version = 3
                    columns = {
                        row["name"]
                        for row in db.execute("PRAGMA table_info(outbox)").fetchall()
                    }
                required = {
                    "payload_compacted",
                    "retired_at",
                    "retirement_reason",
                    "retirement_dataset_id",
                }
                if version != SCHEMA_VERSION or not required <= columns:
                    raise FederationValidationError(
                        "unsupported-outbox-schema",
                        "version",
                        str(version),
                    )
                # This is a derived lookup index, so it can be restored for
                # existing schema-v3 outboxes without changing their durable
                # row format or identity. It is deliberately created after a
                # possible v2 table rebuild above.
                db.execute(_OUTBOX_DELIVERY_INDEX_DDL)
                db.commit()
            except Exception:
                db.rollback()
                raise

    @staticmethod
    def _migrate_v2_to_v3(db: sqlite3.Connection) -> None:
        """Rebuild the v2 table so terminal retirement becomes representable."""

        db.execute(_outbox_table_ddl("outbox_v3", if_not_exists=False))
        db.execute(
            """INSERT INTO outbox_v3
                (outbox_id,session_id,destination_id,schema_id,payload_json,
                 idempotency_key,content_hash,state,payload_compacted,
                 attempt_count,created_at,updated_at,next_attempt_at,last_error,
                 retired_at,retirement_reason,retirement_dataset_id)
               SELECT outbox_id,session_id,destination_id,schema_id,payload_json,
                 idempotency_key,content_hash,state,payload_compacted,
                 attempt_count,created_at,updated_at,next_attempt_at,last_error,
                 NULL,NULL,NULL
               FROM outbox"""
        )
        db.execute("DROP TABLE outbox")
        db.execute("ALTER TABLE outbox_v3 RENAME TO outbox")
        db.execute(_OUTBOX_INDEX_DDL)
        db.execute("UPDATE outbox_schema SET version=3 WHERE singleton=1")

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

    @_admit_mutation(
        bytes_required=_OUTBOX_MUTATION_FIXED_BYTES + MAX_PAYLOAD_BYTES,
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
                    # A terminal row's payload is no longer authoritative: it will
                    # never be delivered from it again, and it may be a bounded
                    # identity receipt or the very corruption that got the row
                    # retired.  Comparing it against the payload reconciliation
                    # just rebuilt would raise a false idempotency conflict on
                    # every archive scan -- and because that conflict propagates
                    # out of the scan, one tombstone would abort reconciliation of
                    # the entire archive.  Identity is compared instead, and
                    # identity is what duplicate suppression depends on: a key
                    # genuinely reused for different content still has a different
                    # content hash and still fails closed.
                    terminal = row["state"] in (
                        OutboxState.COMPLETED.value,
                        OutboxState.RETIRED.value,
                    )
                    if (
                        row["content_hash"] != content_hash
                        or row["schema_id"] != schema_id
                        or (not terminal and row["payload_json"] != payload_json)
                    ):
                        raise FederationValidationError(
                            "idempotency-conflict",
                            "idempotency_key",
                            "identity was reused with different content",
                        )
                    # Only unactivated routing intent is promoted.  A retired row
                    # is deliberately terminal: re-observing the same archive must
                    # never turn a withdrawn identity back into deliverable work.
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

    @_admit_mutation(bytes_required=_OUTBOX_MUTATION_FIXED_BYTES)
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

    def has_pending(
        self,
        *,
        session_id: str | None = None,
        destination_id: str | None = None,
        schema_id: str | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Answer whether a scoped pending row exists without decoding rows.

        Restart ordering only needs an existence proof. Calling ``pending``
        for that purpose made a large offline outbox parse every payload before
        the worker had even decided whether archive reconciliation should wait.
        The query is deliberately identity/time-only and returns at most one
        row; it does not turn a backlog count into an in-memory snapshot.
        """

        query = "SELECT 1 FROM outbox WHERE state='pending'"
        args: list[str] = []
        for column, value in (
            ("session_id", session_id),
            ("destination_id", destination_id),
            ("schema_id", schema_id),
        ):
            if value is not None:
                query += f" AND {column}=?"
                args.append(value)
        if now is not None:
            query += " AND next_attempt_at<=?"
            args.append(_time(now))
        query += " LIMIT 1"
        with self._connect() as db:
            return db.execute(query, args).fetchone() is not None

    def pending_for_delivery(
        self,
        *,
        session_id: str,
        destination_id: str | None,
        schema_id: str,
        limit: int,
    ) -> tuple[OutboxEntry, ...]:
        """Return a bounded, fair window of pending delivery rows.

        A delivery worker must see the oldest row for every ordered
        ``(destination, dataset)`` pair so one unavailable route cannot hide
        healthy ones. Once those heads are represented, the window can include
        additional rows from each pair while never exceeding the worker's
        delivery limit. The dataset key is read from the JSON envelope only
        inside SQLite; no payload is decoded or retained by Python beyond the
        bounded result.

        Rows are intentionally not filtered by ``next_attempt_at`` here. A
        deferred head must still be visible so the delivery queue can fence
        newer rows behind it. The queue decides whether the head may receive
        its startup probe or is waiting for its durable backoff.
        """

        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit <= 0
        ):
            raise FederationValidationError(
                "invalid-limit",
                "limit",
                "must be a positive integer",
            )
        if not isinstance(session_id, str) or not session_id:
            raise FederationValidationError(
                "invalid-id", "session_id", "must be non-empty text"
            )
        if destination_id is not None and (
            not isinstance(destination_id, str) or not destination_id
        ):
            raise FederationValidationError(
                "invalid-id", "destination_id", "must be non-empty text"
            )
        if not isinstance(schema_id, str) or not schema_id:
            raise FederationValidationError(
                "invalid-id", "schema_id", "must be non-empty text"
            )

        # JSON validity is a table invariant. Non-string or absent dataset
        # values are assigned a unique synthetic key, matching the delivery
        # queue's rule that such a row has no ordering fence of its own.
        ordering_key = _OUTBOX_DELIVERY_DATASET_KEY_SQL
        where = "state='pending' AND session_id=? AND schema_id=?"
        args: list[object] = [session_id, schema_id]
        if destination_id is not None:
            where += " AND destination_id=?"
            args.append(destination_id)

        with self._connect() as db:
            ordering_group_count = int(
                db.execute(
                    f"SELECT COUNT(*) FROM ("
                    f"SELECT DISTINCT destination_id, {ordering_key} "
                    f"FROM outbox WHERE {where})",
                    args,
                ).fetchone()[0]
            )
            rows_per_dataset = max(1, limit // max(ordering_group_count, 1))
            rows = db.execute(
                f"""
                WITH ranked_outbox AS (
                    SELECT outbox_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY destination_id, {ordering_key}
                               ORDER BY outbox_id
                           ) AS delivery_rank
                    FROM outbox
                    WHERE {where}
                )
                SELECT entry.*
                FROM ranked_outbox AS ranked
                JOIN outbox AS entry ON entry.outbox_id = ranked.outbox_id
                WHERE ranked.delivery_rank <= ?
                ORDER BY ranked.outbox_id
                LIMIT ?
                """,
                [*args, rows_per_dataset, limit],
            ).fetchall()
        return tuple(self._decode(row) for row in rows)

    def get(self, outbox_id: int) -> OutboxEntry | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)
            ).fetchone()
        return self._decode(row) if row else None

    @_admit_mutation(
        bytes_required=_OUTBOX_MUTATION_FIXED_BYTES + MAX_PAYLOAD_BYTES,
    )
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
                if row["state"] == OutboxState.RETIRED.value:
                    # Retirement is terminal in the other direction.  Letting a
                    # racing worker acknowledge a withdrawn row would record a
                    # commit that never happened.
                    raise FederationValidationError(
                        "outbox-retired",
                        "outbox_id",
                        "retired entry cannot be acknowledged",
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

        return self._compact(OutboxState.COMPLETED, limit=limit)

    def compact_retired(self, *, limit: int = 100) -> int:
        """Reduce a tombstone to its bounded identity receipt.

        A row is usually retired because its payload is the problem, so leaving
        that payload durable forever is exactly the growth the retirement design
        has to answer.  Compaction bounds the bytes without deleting the row:
        the identity that suppresses re-enqueue, the ordering-gap dataset, the
        retirement reason, the recorded cause and the timestamps all survive,
        because those live in columns rather than in the payload.

        Nothing is deleted or vacuumed, and the underlying primary recorder
        evidence is untouched -- a compacted tombstone's content remains
        reconstructible from the archive it was built from.  The trade is
        explicit and one-way: :meth:`reinstate` refuses a compacted row rather
        than inventing a payload it cannot prove.
        """

        return self._compact(OutboxState.RETIRED, limit=limit)

    @_admit_mutation(bytes_required=_OUTBOX_COMPACTION_BYTES)
    def _compact(self, state: OutboxState, *, limit: int) -> int:
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
                    "WHERE state=? AND payload_compacted=0 "
                    "ORDER BY outbox_id LIMIT ?",
                    (state.value, bounded_limit),
                ).fetchall()
                compacted = 0
                for row in rows:
                    cursor = db.execute(
                        "UPDATE outbox SET payload_json=?,payload_compacted=1 "
                        "WHERE outbox_id=? AND state=? "
                        "AND payload_compacted=0",
                        (
                            self._receipt_for_row(row),
                            row["outbox_id"],
                            state.value,
                        ),
                    )
                    compacted += cursor.rowcount
                db.commit()
                return compacted
            except Exception:
                db.rollback()
                raise

    @_admit_mutation(bytes_required=_OUTBOX_MUTATION_FIXED_BYTES)
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
            if row["state"] == OutboxState.RETIRED.value:
                raise FederationValidationError(
                    "outbox-retired", "outbox_id", "cannot retry retired entry"
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

    @_admit_mutation(bytes_required=_OUTBOX_MUTATION_FIXED_BYTES)
    def retire(
        self,
        outbox_id: int,
        *,
        reason: str,
        now: datetime,
        dataset_id: str | None = None,
        verify: Callable[[OutboxEntry], bool] | None = None,
    ) -> OutboxEntry:
        """Withdraw one pending entry from delivery, permanently and durably.

        This is the only transition that ends delivery without a commit, and it
        is deliberately not reachable by counting attempts or by waiting: a
        retryable failure must stay retryable no matter how long it has been
        failing, because "the remote has been down for a week" and "this row can
        never be sent" are different facts.  The caller supplies the second one.

        The row is not deleted.  It keeps its ``(session_id, destination_id,
        idempotency_key)`` identity, so the reconciler that re-reads the same
        durable archive still collides with it and still declines to enqueue the
        same evidence as new work.  A tombstone that was deleted would be
        re-enqueued forever by the next scan.

        ``verify`` is re-evaluated against the freshly re-read durable row
        inside this transaction.  A caller that decided to retire from an
        earlier snapshot therefore cannot retire a row that has since been
        repaired underneath it; the transition fails closed instead.
        """

        if not isinstance(reason, str) or RETIREMENT_REASON.fullmatch(reason) is None:
            raise FederationValidationError(
                "invalid-retirement",
                "reason",
                "must be a bounded lowercase classification token",
            )
        if dataset_id is not None and (
            not isinstance(dataset_id, str)
            or not dataset_id
            or len(dataset_id) > MAX_RETIREMENT_DATASET_LENGTH
        ):
            raise FederationValidationError(
                "invalid-retirement",
                "dataset_id",
                "must be bounded non-empty text when supplied",
            )
        stamp = _time(now)
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
                if row["state"] != OutboxState.PENDING.value:
                    raise FederationValidationError(
                        "outbox-not-pending",
                        "outbox_id",
                        "only a pending entry can be retired",
                    )
                # Decode before writing so a corrupt row fails closed rather
                # than being quietly converted into an authoritative tombstone.
                entry = self._decode(row)
                if verify is not None and not verify(entry):
                    raise FederationValidationError(
                        "outbox-retirement-unverified",
                        "outbox_id",
                        "the durable row no longer justifies retirement",
                    )
                db.execute(
                    "UPDATE outbox SET state='retired',retired_at=?,"
                    "retirement_reason=?,retirement_dataset_id=?,updated_at=? "
                    "WHERE outbox_id=? AND state='pending'",
                    (stamp, reason, dataset_id, stamp, outbox_id),
                )
                retired = self._decode(
                    db.execute(
                        "SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)
                    ).fetchone()
                )
                db.commit()
                return retired
            except Exception:
                db.rollback()
                raise

    @_admit_mutation(bytes_required=_OUTBOX_MUTATION_FIXED_BYTES)
    def reinstate(self, outbox_id: int, *, now: datetime) -> OutboxEntry:
        """Return a retired entry to ordinary retryable delivery.

        Repair is an explicit operator transition, never an automatic one: a
        tombstone that could expire back into work on its own would reproduce
        the endless retry it was created to stop.  Backoff and the recorded
        cause are reset because the isolation, not the row's history, is what
        the operator resolved.

        A compacted tombstone is refused.  Its payload was reduced to an
        identity receipt, and fabricating a delivery from a receipt would be a
        false publication; the durable archive remains the recovery path.
        """

        stamp = _time(now)
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
                if row["state"] != OutboxState.RETIRED.value:
                    raise FederationValidationError(
                        "outbox-not-retired",
                        "outbox_id",
                        "only a retired entry can be reinstated",
                    )
                if row["payload_compacted"] == 1:
                    raise FederationValidationError(
                        "outbox-payload-compacted",
                        "outbox_id",
                        "a compacted tombstone has no payload to deliver",
                    )
                # Fail closed on corrupt retirement metadata rather than
                # laundering it back into deliverable work.
                self._decode(row)
                db.execute(
                    "UPDATE outbox SET state='pending',retired_at=NULL,"
                    "retirement_reason=NULL,retirement_dataset_id=NULL,"
                    "attempt_count=0,last_error=NULL,updated_at=?,"
                    "next_attempt_at=? WHERE outbox_id=? AND state='retired'",
                    (stamp, stamp, outbox_id),
                )
                reinstated = self._decode(
                    db.execute(
                        "SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)
                    ).fetchone()
                )
                db.commit()
                return reinstated
            except Exception:
                db.rollback()
                raise

    def retired(
        self,
        *,
        session_id: str | None = None,
        destination_id: str | None = None,
        schema_id: str | None = None,
    ) -> tuple[OutboxEntry, ...]:
        """Return the durable tombstones, oldest first."""

        query = "SELECT * FROM outbox WHERE state='retired'"
        args: list[str] = []
        for column, value in (
            ("session_id", session_id),
            ("destination_id", destination_id),
            ("schema_id", schema_id),
        ):
            if value is not None:
                query += f" AND {column}=?"
                args.append(value)
        query += " ORDER BY outbox_id"
        with self._connect() as db:
            return tuple(self._decode(row) for row in db.execute(query, args))

    def retired_summary(
        self,
        *,
        session_id: str | None = None,
        destination_id: str | None = None,
        schema_id: str | None = None,
        limit: int = 64,
    ) -> RetiredSummary:
        """Aggregate terminal isolation without materialising every tombstone.

        Health surfaces run this on every cycle, so it must stay O(1) in
        memory no matter how much terminal history has accumulated.  The
        per-dataset rows are truncated to ``limit``; ``total`` is counted
        separately and is never truncated, so "is this recorder degraded?"
        is always answered from complete durable truth.
        """

        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise FederationValidationError(
                "invalid-limit",
                "limit",
                "must be a positive integer",
            )
        bounded = min(limit, MAX_RETIRED_SUMMARY_DATASETS)
        where = " WHERE state='retired'"
        args: list[str] = []
        for column, value in (
            ("session_id", session_id),
            ("destination_id", destination_id),
            ("schema_id", schema_id),
        ):
            if value is not None:
                where += f" AND {column}=?"
                args.append(value)
        with self._connect() as db:
            total = int(
                db.execute(
                    "SELECT COUNT(*) AS total FROM outbox" + where, args
                ).fetchone()["total"]
            )
            grouped = db.execute(
                "SELECT retirement_dataset_id AS dataset_id, "
                "COUNT(*) AS row_count FROM outbox"
                + where
                + " GROUP BY retirement_dataset_id"
                " ORDER BY (dataset_id IS NULL), dataset_id LIMIT ?",
                [*args, bounded + 1],
            ).fetchall()
        truncated = len(grouped) > bounded
        return RetiredSummary(
            total=total,
            datasets=tuple(
                RetiredDatasetCount(row["dataset_id"], int(row["row_count"]))
                for row in grouped[:bounded]
            ),
            truncated=truncated,
        )

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
                if state not in TERMINAL_STATES:
                    raise ValueError("only terminal rows may contain receipts")
                expected_receipt = SQLiteOutbox._receipt_for_row(row)
                if row["payload_json"] != expected_receipt:
                    raise ValueError(
                        "terminal receipt does not match immutable identity"
                    )
            # Retirement metadata is validated as a unit.  A tombstone that
            # says "retired" without a timestamp and reason, or metadata left
            # behind on a row that is no longer retired, is corruption: it
            # would let a reader either lose the ordering gap or believe in a
            # withdrawal that never happened.  Both fail closed here rather
            # than being normalised into a plausible-looking answer.
            retired = state is OutboxState.RETIRED
            retired_at_text = row["retired_at"]
            reason = row["retirement_reason"]
            retirement_dataset_id = row["retirement_dataset_id"]
            if retired is not (retired_at_text is not None) or retired is not (
                reason is not None
            ):
                raise ValueError(
                    "retirement metadata must be present exactly when retired"
                )
            if not retired and retirement_dataset_id is not None:
                raise ValueError("only a retired row may name a retirement dataset")
            retired_at: datetime | None = None
            if retired:
                retired_at = datetime.fromisoformat(retired_at_text)
                if retired_at.tzinfo is None or retired_at.utcoffset() is None:
                    raise ValueError("retired_at must be timezone-aware")
                if (
                    not isinstance(reason, str)
                    or RETIREMENT_REASON.fullmatch(reason) is None
                ):
                    raise ValueError("retirement reason is not a bounded token")
                if retirement_dataset_id is not None and (
                    not isinstance(retirement_dataset_id, str)
                    or not 0
                    < len(retirement_dataset_id)
                    <= MAX_RETIREMENT_DATASET_LENGTH
                ):
                    raise ValueError("retirement dataset id is not bounded text")
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
                retired_at,
                reason,
                retirement_dataset_id,
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
