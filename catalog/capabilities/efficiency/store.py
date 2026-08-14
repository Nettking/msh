"""Durable federation execution-learning state.

Raw observations are the retained source of truth. Learned profiles are a
rebuildable cache and must always be equivalent to replaying the retained
observations in chronological order.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Self

from catalog.federation.errors import FederationValidationError
from catalog.federation.sqlite_schema import SQLiteMigration, ensure_sqlite_schema

from .contracts import ExecutionObservation, canonical_json, stamp
from .policy import EfficiencyPolicy
from .profiles import ANY, LearnedProfile, ProfileKey, observation_profile_keys

EFFICIENCY_SCHEMA_NAME = "capabilities.execution_efficiency"
EFFICIENCY_SCHEMA_VERSION = 1

DEFAULT_MAX_OBSERVATIONS = 200_000
DEFAULT_MAX_DECISIONS = 500

_OBSERVATION_COLUMNS = frozenset(
    {
        "observation_id",
        "session_id",
        "job_id",
        "attempt_id",
        "node_id",
        "capability_id",
        "capability_type",
        "workload_class",
        "environment_fingerprint",
        "succeeded",
        "observed_at",
        "recorded_at",
        "observation_json",
    }
)
_PROFILE_COLUMNS = frozenset(
    {
        "profile_key",
        "capability_type",
        "workload_class",
        "node_id",
        "capability_id",
        "environment_fingerprint",
        "observation_count",
        "profile_json",
        "updated_at",
    }
)
_DECISION_COLUMNS = frozenset(
    {"decision_id", "session_id", "job_id", "decided_at", "decision_json"}
)


def _create_v1(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS capability_execution_observations (
            observation_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            job_id TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            capability_id TEXT NOT NULL,
            capability_type TEXT NOT NULL,
            workload_class TEXT NOT NULL,
            environment_fingerprint TEXT NOT NULL,
            succeeded INTEGER NOT NULL CHECK(succeeded IN (0, 1)),
            observed_at TEXT NOT NULL,
            recorded_at TEXT NOT NULL,
            observation_json TEXT NOT NULL CHECK(json_valid(observation_json)),
            UNIQUE(session_id, job_id, attempt_id)
        );

        CREATE INDEX IF NOT EXISTS capability_execution_observations_order
            ON capability_execution_observations(observed_at, observation_id);
        CREATE INDEX IF NOT EXISTS capability_execution_observations_class
            ON capability_execution_observations(
                capability_type, workload_class, node_id, observed_at
            );

        CREATE TABLE IF NOT EXISTS capability_execution_profiles (
            profile_key TEXT PRIMARY KEY,
            capability_type TEXT NOT NULL,
            workload_class TEXT NOT NULL,
            node_id TEXT NOT NULL,
            capability_id TEXT NOT NULL,
            environment_fingerprint TEXT NOT NULL,
            observation_count INTEGER NOT NULL CHECK(observation_count >= 0),
            profile_json TEXT NOT NULL CHECK(json_valid(profile_json)),
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS capability_execution_profiles_class
            ON capability_execution_profiles(capability_type, workload_class);
        CREATE INDEX IF NOT EXISTS capability_execution_profiles_node
            ON capability_execution_profiles(node_id, capability_id);

        CREATE TABLE IF NOT EXISTS capability_scheduling_decisions (
            decision_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            job_id TEXT NOT NULL,
            decided_at TEXT NOT NULL,
            decision_json TEXT NOT NULL CHECK(json_valid(decision_json))
        );

        CREATE INDEX IF NOT EXISTS capability_scheduling_decisions_order
            ON capability_scheduling_decisions(decided_at, decision_id);
        """
    )


def _detect_legacy_version(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """SELECT name FROM sqlite_master
           WHERE type='table' AND name='capability_execution_observations'"""
    ).fetchone()
    return 0 if row is None else 1


def _columns(connection: sqlite3.Connection, table: str) -> frozenset[str]:
    return frozenset(
        str(row["name"])
        for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
    )


def _validate_schema(connection: sqlite3.Connection) -> None:
    expected = (
        ("capability_execution_observations", _OBSERVATION_COLUMNS),
        ("capability_execution_profiles", _PROFILE_COLUMNS),
        ("capability_scheduling_decisions", _DECISION_COLUMNS),
    )
    for table, columns in expected:
        missing = columns.difference(_columns(connection, table))
        if missing:
            raise FederationValidationError(
                "unsupported-efficiency-schema",
                "schema",
                f"{table} is missing {sorted(missing)}",
            )


class SQLiteExecutionLearningStore:
    """Restart-safe observations, derived profiles and decision history."""

    def __init__(
        self,
        database: Path | str,
        *,
        policy: EfficiencyPolicy | None = None,
        max_observations: int = DEFAULT_MAX_OBSERVATIONS,
        max_decisions: int = DEFAULT_MAX_DECISIONS,
    ) -> None:
        self.database = str(database)
        self.policy = policy or EfficiencyPolicy()
        if max_observations <= 0 or max_decisions <= 0:
            raise FederationValidationError(
                "invalid-retention",
                "max_observations",
                "retention bounds must be positive",
            )
        self.max_observations = int(max_observations)
        self.max_decisions = int(max_decisions)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._session() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            ensure_sqlite_schema(
                connection,
                schema_name=EFFICIENCY_SCHEMA_NAME,
                target_version=EFFICIENCY_SCHEMA_VERSION,
                migrations=(SQLiteMigration(1, _create_v1),),
                detect_legacy_version=_detect_legacy_version,
                validate=_validate_schema,
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        finally:
            connection.close()

    @property
    def half_life_seconds(self) -> int:
        return self.policy.half_life_seconds

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    # ------------------------------------------------------------------
    # observations
    # ------------------------------------------------------------------

    def record(
        self, observation: ExecutionObservation, *, now: datetime | None = None
    ) -> bool:
        """Persist one observation and update the rebuildable profile cache.

        Delayed federation delivery may insert an observation older than the
        newest retained sample. Incrementally folding such a sample would make
        exponential decay depend on arrival order. In that case the cache is
        rebuilt in chronological order inside the same transaction.
        """

        if not isinstance(observation, ExecutionObservation):
            raise FederationValidationError(
                "invalid-observation", "observation", "must be an ExecutionObservation"
            )
        recorded_at = observation.observed_at if now is None else now
        payload = observation.to_json()
        observed_stamp = stamp(observation.observed_at)
        with self._transaction() as connection:
            duplicate = connection.execute(
                """SELECT observation_id FROM capability_execution_observations
                   WHERE session_id=? AND job_id=? AND attempt_id=?""",
                (
                    observation.session_id,
                    observation.job_id,
                    observation.attempt_id,
                ),
            ).fetchone()
            if duplicate is not None:
                return False
            latest_row = connection.execute(
                "SELECT MAX(observed_at) AS latest FROM capability_execution_observations"
            ).fetchone()
            latest = None if latest_row is None else latest_row["latest"]
            out_of_order = latest is not None and observed_stamp < str(latest)
            connection.execute(
                """INSERT INTO capability_execution_observations(
                       observation_id, session_id, job_id, attempt_id,
                       node_id, capability_id, capability_type,
                       workload_class, environment_fingerprint, succeeded,
                       observed_at, recorded_at, observation_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    observation.observation_id,
                    observation.session_id,
                    observation.job_id,
                    observation.attempt_id,
                    observation.node_id,
                    observation.capability_id,
                    observation.capability_type,
                    observation.workload.class_key("exact"),
                    observation.environment_fingerprint,
                    1 if observation.measurements.succeeded else 0,
                    observed_stamp,
                    stamp(recorded_at),
                    payload,
                ),
            )
            if out_of_order:
                self._rebuild_profiles(connection)
            else:
                self._apply_profiles(connection, observation)
            removed = self._prune_observations(connection)
            if removed:
                # Retained observations are authoritative. Once history is
                # pruned, removed samples must disappear from learned state too.
                self._rebuild_profiles(connection)
        return True

    def _apply_profiles(
        self, connection: sqlite3.Connection, observation: ExecutionObservation
    ) -> None:
        for key in observation_profile_keys(observation):
            canonical = key.canonical()
            row = connection.execute(
                "SELECT profile_json FROM capability_execution_profiles "
                "WHERE profile_key=?",
                (canonical,),
            ).fetchone()
            current = _decode_profile(row) or LearnedProfile(key=key)
            updated = current.update(
                observation, half_life_seconds=self.half_life_seconds
            )
            self._write_profile(connection, canonical, updated)

    @staticmethod
    def _write_profile(
        connection: sqlite3.Connection,
        canonical: str,
        profile: LearnedProfile,
    ) -> None:
        updated_at = profile.last_observed_at or profile.first_observed_at
        if updated_at is None:
            raise FederationValidationError(
                "empty-efficiency-profile",
                "profile",
                "a persisted profile requires at least one observation",
            )
        key = profile.key
        connection.execute(
            """INSERT INTO capability_execution_profiles(
                   profile_key, capability_type, workload_class, node_id,
                   capability_id, environment_fingerprint,
                   observation_count, profile_json, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?)
               ON CONFLICT(profile_key) DO UPDATE SET
                   observation_count=excluded.observation_count,
                   profile_json=excluded.profile_json,
                   updated_at=excluded.updated_at""",
            (
                canonical,
                key.capability_type,
                key.workload_class,
                key.node_id,
                key.capability_id,
                key.environment_fingerprint,
                profile.observation_count,
                canonical_json(profile.to_dict(), "profile"),
                stamp(updated_at),
            ),
        )

    def _prune_observations(self, connection: sqlite3.Connection) -> int:
        total = int(
            connection.execute(
                "SELECT COUNT(*) AS total FROM capability_execution_observations"
            ).fetchone()["total"]
        )
        excess = total - self.max_observations
        if excess <= 0:
            return 0
        # Small limits are used by exact retention tests. For production-sized
        # histories leave a small reserve below the ceiling so cache rebuilds
        # are amortized rather than occurring on every subsequent insert.
        reserve = (
            0
            if self.max_observations < 1_000
            else min(1_000, max(1, self.max_observations // 100))
        )
        remove_count = min(total, excess + reserve)
        cursor = connection.execute(
            """DELETE FROM capability_execution_observations
               WHERE observation_id IN (
                   SELECT observation_id FROM capability_execution_observations
                   ORDER BY observed_at, observation_id LIMIT ?
               )""",
            (remove_count,),
        )
        return int(cursor.rowcount or 0)

    def observation_count(self) -> int:
        with self._session() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM capability_execution_observations"
            ).fetchone()
        return int(row["total"])

    def observations(
        self,
        *,
        capability_type: str | None = None,
        node_id: str | None = None,
        limit: int | None = None,
    ) -> tuple[ExecutionObservation, ...]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if capability_type is not None:
            clauses.append("capability_type = ?")
            parameters.append(capability_type)
        if node_id is not None:
            clauses.append("node_id = ?")
            parameters.append(node_id)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        statement = (
            "SELECT observation_json FROM capability_execution_observations"
            f"{where} ORDER BY observed_at, observation_id"
        )
        if limit is not None:
            statement += " LIMIT ?"
            parameters.append(int(limit))
        with self._session() as connection:
            rows = connection.execute(statement, tuple(parameters)).fetchall()
        return tuple(
            item
            for item in (_decode_observation(row) for row in rows)
            if item is not None
        )

    def _iter_observations(
        self, connection: sqlite3.Connection
    ) -> Iterator[ExecutionObservation]:
        cursor = connection.execute(
            "SELECT observation_json FROM capability_execution_observations "
            "ORDER BY observed_at, observation_id"
        )
        for row in cursor:
            decoded = _decode_observation(row)
            if decoded is not None:
                yield decoded

    # ------------------------------------------------------------------
    # profiles
    # ------------------------------------------------------------------

    def profile(self, key: ProfileKey) -> LearnedProfile | None:
        with self._session() as connection:
            row = connection.execute(
                "SELECT profile_json FROM capability_execution_profiles "
                "WHERE profile_key=?",
                (key.canonical(),),
            ).fetchone()
        return _decode_profile(row)

    def profiles(
        self,
        *,
        capability_type: str | None = None,
        workload_class: str | None = None,
        node_id: str | None = None,
        include_pooled: bool = True,
        limit: int | None = None,
    ) -> tuple[LearnedProfile, ...]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if capability_type is not None:
            clauses.append("capability_type = ?")
            parameters.append(capability_type)
        if workload_class is not None:
            clauses.append("workload_class = ?")
            parameters.append(workload_class)
        if node_id is not None:
            clauses.append("node_id = ?")
            parameters.append(node_id)
        elif not include_pooled:
            clauses.append("node_id != ?")
            parameters.append(ANY)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        statement = (
            "SELECT profile_json FROM capability_execution_profiles"
            f"{where} ORDER BY observation_count DESC, profile_key"
        )
        if limit is not None:
            statement += " LIMIT ?"
            parameters.append(int(limit))
        with self._session() as connection:
            rows = connection.execute(statement, tuple(parameters)).fetchall()
        return tuple(
            item for item in (_decode_profile(row) for row in rows) if item is not None
        )

    def _rebuild_profiles(self, connection: sqlite3.Connection) -> int:
        rebuilt: dict[str, LearnedProfile] = {}
        for observation in self._iter_observations(connection):
            for key in observation_profile_keys(observation):
                canonical = key.canonical()
                current = rebuilt.get(canonical) or LearnedProfile(key=key)
                rebuilt[canonical] = current.update(
                    observation, half_life_seconds=self.half_life_seconds
                )
        connection.execute("DELETE FROM capability_execution_profiles")
        for canonical, profile in sorted(rebuilt.items()):
            self._write_profile(connection, canonical, profile)
        return len(rebuilt)

    def rebuild_profiles(self) -> int:
        """Recompute every profile from retained observations atomically."""

        with self._transaction() as connection:
            return self._rebuild_profiles(connection)

    def forget_before(self, cutoff: datetime) -> int:
        """Drop observations older than ``cutoff`` and rebuild atomically."""

        with self._transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM capability_execution_observations WHERE observed_at < ?",
                (stamp(cutoff),),
            )
            removed = int(cursor.rowcount or 0)
            if removed:
                self._rebuild_profiles(connection)
            return removed

    def expire_stale_observations(self, *, now: datetime) -> int:
        cutoff = now - timedelta(seconds=self.policy.max_observation_age_seconds)
        return self.forget_before(cutoff)

    # ------------------------------------------------------------------
    # scheduling decisions
    # ------------------------------------------------------------------

    def record_decision(self, decision: Any) -> None:
        payload = decision.to_dict() if hasattr(decision, "to_dict") else decision
        if not isinstance(payload, dict):
            raise FederationValidationError(
                "invalid-decision", "decision", "must be an object"
            )
        decision_id = str(payload.get("decision_id") or "")
        job_id = str(payload.get("job_id") or "")
        session_id = str(payload.get("session_id") or "")
        decided_at = str(payload.get("decided_at") or "")
        if not decision_id or not job_id or not decided_at:
            raise FederationValidationError(
                "invalid-decision",
                "decision",
                "requires decision_id, job_id and decided_at",
            )
        with self._transaction() as connection:
            connection.execute(
                """INSERT OR REPLACE INTO capability_scheduling_decisions(
                       decision_id, session_id, job_id, decided_at, decision_json)
                   VALUES(?,?,?,?,?)""",
                (
                    decision_id,
                    session_id,
                    job_id,
                    decided_at,
                    canonical_json(payload, "decision"),
                ),
            )
            connection.execute(
                """DELETE FROM capability_scheduling_decisions
                   WHERE decision_id IN (
                       SELECT decision_id FROM capability_scheduling_decisions
                       ORDER BY decided_at DESC, decision_id DESC
                       LIMIT -1 OFFSET ?
                   )""",
                (self.max_decisions,),
            )

    def recent_decisions(self, *, limit: int = 20) -> tuple[dict[str, Any], ...]:
        with self._session() as connection:
            rows = connection.execute(
                "SELECT decision_json FROM capability_scheduling_decisions "
                "ORDER BY decided_at DESC, decision_id DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        decoded: list[dict[str, Any]] = []
        for row in rows:
            try:
                value = json.loads(row["decision_json"])
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                decoded.append(value)
        return tuple(decoded)

    # ------------------------------------------------------------------
    # read model
    # ------------------------------------------------------------------

    def node_summary(self) -> tuple[dict[str, Any], ...]:
        with self._session() as connection:
            rows = connection.execute(
                """SELECT node_id, capability_id, capability_type,
                          COUNT(*) AS observations,
                          SUM(succeeded) AS successes,
                          MAX(observed_at) AS last_observed_at
                   FROM capability_execution_observations
                   GROUP BY node_id, capability_id, capability_type
                   ORDER BY observations DESC, node_id"""
            ).fetchall()
        return tuple(
            {
                "node_id": row["node_id"],
                "capability_id": row["capability_id"],
                "capability_type": row["capability_type"],
                "observations": int(row["observations"]),
                "successes": int(row["successes"] or 0),
                "last_observed_at": row["last_observed_at"],
            }
            for row in rows
        )


def _decode_observation(row: sqlite3.Row | None) -> ExecutionObservation | None:
    if row is None:
        return None
    try:
        return ExecutionObservation.from_json(row["observation_json"])
    except Exception:  # noqa: BLE001 - a corrupt row must not stop scheduling
        return None


def _decode_profile(row: sqlite3.Row | None) -> LearnedProfile | None:
    if row is None:
        return None
    try:
        return LearnedProfile.from_dict(json.loads(row["profile_json"]))
    except Exception:  # noqa: BLE001 - corrupt cache degrades to lower evidence
        return None


__all__ = [
    "DEFAULT_MAX_DECISIONS",
    "DEFAULT_MAX_OBSERVATIONS",
    "EFFICIENCY_SCHEMA_NAME",
    "EFFICIENCY_SCHEMA_VERSION",
    "SQLiteExecutionLearningStore",
]
