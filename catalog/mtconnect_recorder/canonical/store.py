"""Durable SQLite home for canonical observations and the batch ledger.

This is a **projection**, not a system of record. The recorder's compressed XML
plus its manifests remain the immutable evidence; everything in this database is
derived from them and can be deleted and rebuilt at any time. Nothing here ever
writes back to a capture.

Two tables:

``canonical_observations``
    One row per MTConnect observation, keyed by its durable natural identity
    ``(source_key, agent_instance_id, sequence)``. Each row carries the
    ``raw_batch_sha256`` it came from, so every observation traces back to the
    exact compressed batch that produced it.

``projected_batches``
    One row per raw batch the projection has already considered, keyed by
    ``(source_key, raw_sha256)``. ``raw_sha256`` is the digest of the
    *uncompressed* XML, so it is a content identity: replaying the same batch
    finds its own ledger row and does no work, which is what makes the
    projection idempotent and incremental. Rejected batches are recorded with a
    reason instead of being forgotten, so an unsupported or corrupt batch is
    visible rather than silently missing.

The query surface is deliberately narrow — time and data-item slices over
observations. It is not a general-purpose analytics database and it does not
aggregate anything away.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.federation.errors import FederationValidationError
from catalog.federation.sqlite_schema import SQLiteMigration, ensure_sqlite_schema

from .observation import CanonicalObservation, epoch_micros, parse_timestamp

CANONICAL_SCHEMA_NAME = "mtconnect_recorder.canonical_observations"
CANONICAL_SCHEMA_VERSION = 1

BATCH_STATUS_PROJECTED = "projected"
BATCH_STATUS_REJECTED = "rejected"

_OBSERVATION_COLUMNS = (
    "source_key",
    "agent_instance_id",
    "sequence",
    "source_name",
    "raw_batch_sha256",
    "observed_at",
    "observed_at_us",
    "timestamp_source",
    "machine_id",
    "device_uuid",
    "device_name",
    "component_type",
    "component_id",
    "component_name",
    "component_path",
    "data_item_id",
    "data_item_name",
    "category",
    "observation_type",
    "mtconnect_type",
    "sub_type",
    "value_kind",
    "value_number",
    "value_text",
    "native_value",
    "units",
    "native_units",
    "condition_state",
    "condition_native_code",
    "condition_native_severity",
    "condition_qualifier",
    "attributes",
)

_BATCH_COLUMNS = (
    "source_key",
    "raw_sha256",
    "source_name",
    "agent_instance_id",
    "manifest_path",
    "raw_path",
    "manifest_schema",
    "first_sequence",
    "last_sequence",
    "manifest_observation_count",
    "projected_count",
    "status",
    "reason",
    "detail",
    "projected_at",
)


@dataclass(frozen=True)
class ProjectedBatchRecord:
    """The ledger entry that ties observations back to one raw batch."""

    source_key: str
    raw_sha256: str
    source_name: str
    agent_instance_id: int
    manifest_path: str
    raw_path: str
    manifest_schema: str
    first_sequence: int
    last_sequence: int
    manifest_observation_count: int
    projected_count: int
    status: str
    reason: str | None
    detail: str | None
    projected_at: str

    @property
    def rejected(self) -> bool:
        return self.status == BATCH_STATUS_REJECTED


def _create_v1(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE canonical_observations (
            source_key TEXT NOT NULL,
            agent_instance_id INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            source_name TEXT NOT NULL,
            raw_batch_sha256 TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            observed_at_us INTEGER NOT NULL,
            timestamp_source TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            device_uuid TEXT,
            device_name TEXT,
            component_type TEXT,
            component_id TEXT,
            component_name TEXT,
            component_path TEXT,
            data_item_id TEXT,
            data_item_name TEXT,
            category TEXT NOT NULL,
            observation_type TEXT NOT NULL,
            mtconnect_type TEXT,
            sub_type TEXT,
            value_kind TEXT NOT NULL,
            value_number REAL,
            value_text TEXT,
            native_value TEXT NOT NULL,
            units TEXT,
            native_units TEXT,
            condition_state TEXT,
            condition_native_code TEXT,
            condition_native_severity TEXT,
            condition_qualifier TEXT,
            attributes TEXT NOT NULL,
            PRIMARY KEY (source_key, agent_instance_id, sequence)
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX idx_canonical_machine_time
            ON canonical_observations
               (source_key, machine_id, observed_at_us, agent_instance_id, sequence)
        """
    )
    connection.execute(
        """
        CREATE INDEX idx_canonical_data_item_time
            ON canonical_observations
               (source_key, data_item_id, observed_at_us, agent_instance_id, sequence)
        """
    )
    connection.execute(
        """
        CREATE INDEX idx_canonical_time
            ON canonical_observations (observed_at_us, source_key, agent_instance_id, sequence)
        """
    )
    connection.execute(
        """
        CREATE INDEX idx_canonical_raw_batch
            ON canonical_observations (raw_batch_sha256)
        """
    )
    connection.execute(
        """
        CREATE TABLE projected_batches (
            source_key TEXT NOT NULL,
            raw_sha256 TEXT NOT NULL,
            source_name TEXT NOT NULL,
            agent_instance_id INTEGER NOT NULL,
            manifest_path TEXT NOT NULL,
            raw_path TEXT NOT NULL,
            manifest_schema TEXT NOT NULL,
            first_sequence INTEGER NOT NULL,
            last_sequence INTEGER NOT NULL,
            manifest_observation_count INTEGER NOT NULL,
            projected_count INTEGER NOT NULL,
            status TEXT NOT NULL
                CHECK(status IN ('projected', 'rejected')),
            reason TEXT,
            detail TEXT,
            projected_at TEXT NOT NULL,
            PRIMARY KEY (source_key, raw_sha256)
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX idx_projected_batches_status
            ON projected_batches (source_key, status, first_sequence)
        """
    )


def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _detect_legacy_version(connection: sqlite3.Connection) -> int:
    """No released version predates the shared marker, so absent means absent."""

    if not _table_exists(connection, "canonical_observations"):
        return 0
    return CANONICAL_SCHEMA_VERSION


def _validate(connection: sqlite3.Connection) -> None:
    for table, expected in (
        ("canonical_observations", _OBSERVATION_COLUMNS),
        ("projected_batches", _BATCH_COLUMNS),
    ):
        if not _table_exists(connection, table):
            raise FederationValidationError(
                "unsupported-canonical-observation-schema",
                "schema",
                f"{table} table is missing",
            )
        missing = set(expected).difference(_columns(connection, table))
        if missing:
            raise FederationValidationError(
                "unsupported-canonical-observation-schema",
                "schema",
                f"{table} is missing required columns: {sorted(missing)!r}",
            )


def _as_micros(value: datetime | str | int | None) -> int | None:
    """Convert a query bound to the same integer scale rows are stored on.

    Deliberately routed through the observation module's ``epoch_micros`` and
    ``parse_timestamp`` rather than repeating the arithmetic: a query bound
    computed even one microsecond differently from the stored value would make
    range queries silently disagree with their own data at the boundary.
    """

    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("time bound must not be a bool")
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return epoch_micros(moment)
    return epoch_micros(parse_timestamp(str(value)))


def _row_to_observation(row: sqlite3.Row) -> CanonicalObservation:
    attributes = json.loads(row["attributes"])
    return CanonicalObservation(
        source_key=row["source_key"],
        agent_instance_id=int(row["agent_instance_id"]),
        sequence=int(row["sequence"]),
        source_name=row["source_name"],
        raw_batch_sha256=row["raw_batch_sha256"],
        observed_at=row["observed_at"],
        observed_at_us=int(row["observed_at_us"]),
        timestamp_source=row["timestamp_source"],
        machine_id=row["machine_id"],
        device_uuid=row["device_uuid"],
        device_name=row["device_name"],
        component_type=row["component_type"],
        component_id=row["component_id"],
        component_name=row["component_name"],
        component_path=row["component_path"],
        data_item_id=row["data_item_id"],
        data_item_name=row["data_item_name"],
        category=row["category"],
        observation_type=row["observation_type"],
        mtconnect_type=row["mtconnect_type"],
        sub_type=row["sub_type"],
        value_kind=row["value_kind"],
        value_number=row["value_number"],
        value_text=row["value_text"],
        native_value=row["native_value"],
        units=row["units"],
        native_units=row["native_units"],
        condition_state=row["condition_state"],
        condition_native_code=row["condition_native_code"],
        condition_native_severity=row["condition_native_severity"],
        condition_qualifier=row["condition_qualifier"],
        attributes=attributes if isinstance(attributes, dict) else {},
    )


def _row_to_batch(row: sqlite3.Row) -> ProjectedBatchRecord:
    return ProjectedBatchRecord(
        source_key=row["source_key"],
        raw_sha256=row["raw_sha256"],
        source_name=row["source_name"],
        agent_instance_id=int(row["agent_instance_id"]),
        manifest_path=row["manifest_path"],
        raw_path=row["raw_path"],
        manifest_schema=row["manifest_schema"],
        first_sequence=int(row["first_sequence"]),
        last_sequence=int(row["last_sequence"]),
        manifest_observation_count=int(row["manifest_observation_count"]),
        projected_count=int(row["projected_count"]),
        status=row["status"],
        reason=row["reason"],
        detail=row["detail"],
        projected_at=row["projected_at"],
    )


class CanonicalObservationStore:
    """Durable, rebuildable observation projection with a narrow query surface."""

    #: Deterministic total order. Time first so a consumer reading across Agent
    #: restarts sees event order; the identity columns break ties so the order is
    #: total rather than merely stable-by-accident.
    _EVENT_ORDER = "ORDER BY observed_at_us, source_key, agent_instance_id, sequence"

    def __init__(self, database_path: Path | str) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    # -- lifecycle ---------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            ensure_sqlite_schema(
                connection,
                schema_name=CANONICAL_SCHEMA_NAME,
                target_version=CANONICAL_SCHEMA_VERSION,
                migrations=(SQLiteMigration(1, _create_v1),),
                detect_legacy_version=_detect_legacy_version,
                validate=_validate,
            )

    # -- writes ------------------------------------------------------------

    def record_batch(
        self,
        *,
        observations: Sequence[CanonicalObservation],
        batch: ProjectedBatchRecord,
    ) -> None:
        """Commit one batch and its observations, or neither.

        The whole batch is a single transaction, so a batch that fails partway
        leaves no half-written observations behind. That is what keeps one bad
        batch from corrupting the projection.

        Observation writes are ``INSERT OR IGNORE``: an identity already present
        was produced by an earlier, digest-verified batch covering the same
        sequence, which is exactly the overlapping-replay case and must not be
        an error. Genuine conflicts are detected before this point.
        """

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            placeholders = ", ".join("?" for _ in _OBSERVATION_COLUMNS)
            connection.executemany(
                f"INSERT OR IGNORE INTO canonical_observations "  # fixed column tuple, never caller input
                f"({', '.join(_OBSERVATION_COLUMNS)}) VALUES ({placeholders})",
                [self._observation_row(item) for item in observations],
            )
            batch_placeholders = ", ".join("?" for _ in _BATCH_COLUMNS)
            connection.execute(
                f"INSERT OR REPLACE INTO projected_batches "  # fixed column tuple, never caller input
                f"({', '.join(_BATCH_COLUMNS)}) VALUES ({batch_placeholders})",
                [getattr(batch, column) for column in _BATCH_COLUMNS],
            )

    @staticmethod
    def _observation_row(item: CanonicalObservation) -> tuple[Any, ...]:
        return (
            item.source_key,
            item.agent_instance_id,
            item.sequence,
            item.source_name,
            item.raw_batch_sha256,
            item.observed_at,
            item.observed_at_us,
            item.timestamp_source,
            item.machine_id,
            item.device_uuid,
            item.device_name,
            item.component_type,
            item.component_id,
            item.component_name,
            item.component_path,
            item.data_item_id,
            item.data_item_name,
            item.category,
            item.observation_type,
            item.mtconnect_type,
            item.sub_type,
            item.value_kind,
            item.value_number,
            item.value_text,
            item.native_value,
            item.units,
            item.native_units,
            item.condition_state,
            item.condition_native_code,
            item.condition_native_severity,
            item.condition_qualifier,
            item.attributes_json(),
        )

    def existing_observations(
        self, *, source_key: str, agent_instance_id: int, sequences: Iterable[int]
    ) -> dict[int, CanonicalObservation]:
        """Return already-stored observations for the given sequences.

        The projection compares full content rather than merely noticing that an
        identity exists, so an overlapping batch that disagrees with what is
        already stored is caught instead of being silently ignored.
        """

        wanted = sorted({int(value) for value in sequences})
        if not wanted:
            return {}
        found: dict[int, CanonicalObservation] = {}
        with self._connect() as connection:
            for offset in range(0, len(wanted), 500):
                chunk = wanted[offset : offset + 500]
                placeholders = ", ".join("?" for _ in chunk)
                rows = connection.execute(
                    "SELECT * FROM canonical_observations "  # every value is a bound placeholder
                    "WHERE source_key = ? AND agent_instance_id = ? "
                    f"AND sequence IN ({placeholders})",
                    (source_key, int(agent_instance_id), *chunk),
                ).fetchall()
                for row in rows:
                    observation = _row_to_observation(row)
                    found[observation.sequence] = observation
        return found

    # -- ledger ------------------------------------------------------------

    def projected_digests(self, *, source_key: str) -> dict[str, str]:
        """Return ``raw_sha256 -> status`` for every batch already considered."""

        with self._connect() as connection:
            rows = connection.execute(
                "SELECT raw_sha256, status FROM projected_batches WHERE source_key = ?",
                (source_key,),
            ).fetchall()
        return {str(row[0]): str(row[1]) for row in rows}

    def batches(
        self, *, source_key: str | None = None, status: str | None = None
    ) -> tuple[ProjectedBatchRecord, ...]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if source_key is not None:
            clauses.append("source_key = ?")
            parameters.append(source_key)
        if status is not None:
            clauses.append("status = ?")
            parameters.append(status)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM projected_batches"  # every value is a bound placeholder
                f"{where} ORDER BY source_key, first_sequence, raw_sha256",
                parameters,
            ).fetchall()
        return tuple(_row_to_batch(row) for row in rows)

    def batch_for_digest(
        self, *, source_key: str, raw_sha256: str
    ) -> ProjectedBatchRecord | None:
        """Trace an observation's ``raw_batch_sha256`` back to its raw capture.

        Both halves of the ledger's key are required. ``raw_sha256`` is a
        content digest, so two sources that record byte-identical payloads —
        two identically configured machines, or the same capture restored under
        a second source — share it. Looking up by digest alone would return
        whichever partition happened to be found first and quietly attribute an
        observation to the wrong machine's evidence.
        """

        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM projected_batches WHERE source_key = ? AND raw_sha256 = ?",
                (source_key, raw_sha256),
            ).fetchone()
        return _row_to_batch(row) if row is not None else None

    def batch_for_observation(
        self, observation: CanonicalObservation
    ) -> ProjectedBatchRecord | None:
        """Return the raw batch one observation was projected from."""

        return self.batch_for_digest(
            source_key=observation.source_key,
            raw_sha256=observation.raw_batch_sha256,
        )

    # -- queries -----------------------------------------------------------

    def query_observations(
        self,
        *,
        source_key: str | None = None,
        machine_id: str | None = None,
        data_item_id: str | None = None,
        agent_instance_id: int | None = None,
        category: str | None = None,
        start: datetime | str | int | None = None,
        end: datetime | str | int | None = None,
        limit: int | None = None,
    ) -> tuple[CanonicalObservation, ...]:
        """Return observations in event order.

        ``start`` is inclusive and ``end`` is exclusive, so adjacent windows
        tile without double-counting an observation on the boundary.
        """

        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value in (
            ("source_key", source_key),
            ("machine_id", machine_id),
            ("data_item_id", data_item_id),
            ("category", category),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                parameters.append(value)
        if agent_instance_id is not None:
            clauses.append("agent_instance_id = ?")
            parameters.append(int(agent_instance_id))
        start_us = _as_micros(start)
        if start_us is not None:
            clauses.append("observed_at_us >= ?")
            parameters.append(start_us)
        end_us = _as_micros(end)
        if end_us is not None:
            clauses.append("observed_at_us < ?")
            parameters.append(end_us)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        suffix = ""
        if limit is not None:
            suffix = " LIMIT ?"
            parameters.append(int(limit))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM canonical_observations"  # every value is a bound placeholder
                f"{where} {self._EVENT_ORDER}{suffix}",
                parameters,
            ).fetchall()
        return tuple(_row_to_observation(row) for row in rows)

    def observations_for_machine(
        self,
        machine_id: str,
        *,
        start: datetime | str | int | None = None,
        end: datetime | str | int | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> tuple[CanonicalObservation, ...]:
        return self.query_observations(
            machine_id=machine_id,
            source_key=source_key,
            start=start,
            end=end,
            limit=limit,
        )

    def observations_for_data_item(
        self,
        data_item_id: str,
        *,
        start: datetime | str | int | None = None,
        end: datetime | str | int | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> tuple[CanonicalObservation, ...]:
        return self.query_observations(
            data_item_id=data_item_id,
            source_key=source_key,
            start=start,
            end=end,
            limit=limit,
        )

    def observations_for_agent_instance(
        self,
        *,
        source_key: str,
        agent_instance_id: int,
        limit: int | None = None,
    ) -> tuple[CanonicalObservation, ...]:
        """Return one Agent instance's observations in sequence order.

        Within one instance the sequence number *is* the Agent's publication
        order, so this is the strongest ordering the protocol offers.
        """

        parameters: list[Any] = [source_key, int(agent_instance_id)]
        suffix = ""
        if limit is not None:
            suffix = " LIMIT ?"
            parameters.append(int(limit))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM canonical_observations "  # every value is a bound placeholder
                "WHERE source_key = ? AND agent_instance_id = ? "
                f"ORDER BY sequence{suffix}",
                parameters,
            ).fetchall()
        return tuple(_row_to_observation(row) for row in rows)

    def latest_observation(
        self,
        data_item_id: str,
        *,
        source_key: str | None = None,
        machine_id: str | None = None,
    ) -> CanonicalObservation | None:
        clauses = ["data_item_id = ?"]
        parameters: list[Any] = [data_item_id]
        for column, value in (("source_key", source_key), ("machine_id", machine_id)):
            if value is not None:
                clauses.append(f"{column} = ?")
                parameters.append(value)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM canonical_observations "  # every value is a bound placeholder
                f"WHERE {' AND '.join(clauses)} "
                "ORDER BY observed_at_us DESC, agent_instance_id DESC, sequence DESC "
                "LIMIT 1",
                parameters,
            ).fetchone()
        return _row_to_observation(row) if row is not None else None

    def count_observations(self, *, source_key: str | None = None) -> int:
        with self._connect() as connection:
            if source_key is None:
                row = connection.execute(
                    "SELECT COUNT(*) FROM canonical_observations"
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT COUNT(*) FROM canonical_observations WHERE source_key = ?",
                    (source_key,),
                ).fetchone()
        return int(row[0])

    def content_fingerprint(self, *, source_key: str | None = None) -> str:
        """Return a digest of every observation row, ignoring ledger timestamps.

        Two projections of the same raw batches must have the same fingerprint.
        The ledger's ``projected_at`` is wall-clock and deliberately excluded:
        determinism is a property of the observations, not of when a rebuild ran.
        """

        from hashlib import sha256

        digest = sha256()
        where = " WHERE source_key = ?" if source_key is not None else ""
        parameters = (source_key,) if source_key is not None else ()
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT {', '.join(_OBSERVATION_COLUMNS)} FROM canonical_observations"  # fixed column tuple, never caller input
                f"{where} ORDER BY source_key, agent_instance_id, sequence",
                parameters,
            )
            for row in rows:
                digest.update(
                    json.dumps(list(row), ensure_ascii=False, sort_keys=True).encode(
                        "utf-8"
                    )
                )
                digest.update(b"\n")
        return digest.hexdigest()

    def describe(self) -> Mapping[str, Any]:
        with self._connect() as connection:
            observations = int(
                connection.execute(
                    "SELECT COUNT(*) FROM canonical_observations"
                ).fetchone()[0]
            )
            projected = int(
                connection.execute(
                    "SELECT COUNT(*) FROM projected_batches WHERE status = ?",
                    (BATCH_STATUS_PROJECTED,),
                ).fetchone()[0]
            )
            rejected = int(
                connection.execute(
                    "SELECT COUNT(*) FROM projected_batches WHERE status = ?",
                    (BATCH_STATUS_REJECTED,),
                ).fetchone()[0]
            )
        return {
            "database_path": str(self.database_path),
            "schema_name": CANONICAL_SCHEMA_NAME,
            "schema_version": CANONICAL_SCHEMA_VERSION,
            "observations": observations,
            "projected_batches": projected,
            "rejected_batches": rejected,
        }


__all__ = [
    "BATCH_STATUS_PROJECTED",
    "BATCH_STATUS_REJECTED",
    "CANONICAL_SCHEMA_NAME",
    "CANONICAL_SCHEMA_VERSION",
    "CanonicalObservationStore",
    "ProjectedBatchRecord",
]
