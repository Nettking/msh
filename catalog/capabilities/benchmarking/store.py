"""Durable, append-only benchmark result and run-reservation storage."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock

from catalog.federation.onboarding_models import BenchmarkResult

from .errors import DuplicateRunIdError


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


class SQLiteBenchmarkResultStore:
    """SQLite store providing restart-safe run IDs and immutable results."""

    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._lock = RLock()
        self._connection = sqlite3.connect(
            self._path,
            check_same_thread=False,
            isolation_level=None,
        )
        self._connection.row_factory = sqlite3.Row
        with self._lock:
            self._connection.execute("PRAGMA foreign_keys = ON")
            if self._path != ":memory:":
                self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS benchmark_results (
                    run_id TEXT PRIMARY KEY,
                    request_fingerprint TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    finished_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS benchmark_run_reservations (
                    run_id TEXT PRIMARY KEY,
                    request_fingerprint TEXT NOT NULL,
                    deadline_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS benchmark_results_finished
                    ON benchmark_results (finished_at, run_id);
                """
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "SQLiteBenchmarkResultStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @staticmethod
    def _decode_result(payload: str) -> BenchmarkResult:
        return BenchmarkResult.from_dict(json.loads(payload))

    def reserve_run(
        self,
        *,
        run_id: str,
        request_fingerprint: str,
        deadline_at: datetime,
        now: datetime,
    ) -> BenchmarkResult | None:
        """Atomically reserve a run ID or replay its already durable result."""

        with self._lock:
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT request_fingerprint, result_json "
                    "FROM benchmark_results WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if existing is not None:
                    if existing["request_fingerprint"] != request_fingerprint:
                        raise DuplicateRunIdError(
                            f"run ID is already bound to another request: {run_id}"
                        )
                    durable = self._decode_result(existing["result_json"])
                    connection.execute("COMMIT")
                    return durable

                reservation = connection.execute(
                    "SELECT request_fingerprint, deadline_at "
                    "FROM benchmark_run_reservations WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if reservation is not None:
                    if _parse_timestamp(reservation["deadline_at"]) <= now:
                        connection.execute(
                            "DELETE FROM benchmark_run_reservations WHERE run_id = ?",
                            (run_id,),
                        )
                    else:
                        qualifier = (
                            "another request"
                            if reservation["request_fingerprint"]
                            != request_fingerprint
                            else "an active request"
                        )
                        raise DuplicateRunIdError(
                            f"run ID is already reserved by {qualifier}: {run_id}"
                        )

                connection.execute(
                    "INSERT INTO benchmark_run_reservations "
                    "(run_id, request_fingerprint, deadline_at) VALUES (?, ?, ?)",
                    (run_id, request_fingerprint, _timestamp(deadline_at)),
                )
                connection.execute("COMMIT")
                return None
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def complete_run(
        self,
        *,
        result: BenchmarkResult,
        request_fingerprint: str,
    ) -> BenchmarkResult:
        """Persist one immutable result and clear its matching reservation."""

        result_json = json.dumps(
            result.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        with self._lock:
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT request_fingerprint, result_json "
                    "FROM benchmark_results WHERE run_id = ?",
                    (result.run_id,),
                ).fetchone()
                if existing is not None:
                    if existing["request_fingerprint"] != request_fingerprint:
                        raise DuplicateRunIdError(
                            "run ID completed with a different request"
                        )
                    durable = self._decode_result(existing["result_json"])
                    if durable != result:
                        raise DuplicateRunIdError(
                            "run ID completed with a different result"
                        )
                    connection.execute("COMMIT")
                    return durable

                reservation = connection.execute(
                    "SELECT request_fingerprint FROM benchmark_run_reservations "
                    "WHERE run_id = ?",
                    (result.run_id,),
                ).fetchone()
                if reservation is None:
                    raise DuplicateRunIdError(
                        f"run ID is not reserved: {result.run_id}"
                    )
                if reservation["request_fingerprint"] != request_fingerprint:
                    raise DuplicateRunIdError(
                        f"run reservation does not match: {result.run_id}"
                    )

                connection.execute(
                    "INSERT INTO benchmark_results "
                    "(run_id, request_fingerprint, result_json, finished_at) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        result.run_id,
                        request_fingerprint,
                        result_json,
                        _timestamp(result.finished_at),
                    ),
                )
                connection.execute(
                    "DELETE FROM benchmark_run_reservations WHERE run_id = ?",
                    (result.run_id,),
                )
                connection.execute("COMMIT")
                return result
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def release_run(self, *, run_id: str, request_fingerprint: str) -> None:
        """Release a reservation only when it still belongs to this request."""

        with self._lock:
            self._connection.execute(
                "DELETE FROM benchmark_run_reservations "
                "WHERE run_id = ? AND request_fingerprint = ?",
                (run_id, request_fingerprint),
            )

    def load(self, run_id: str) -> BenchmarkResult | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT result_json FROM benchmark_results WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        return None if row is None else self._decode_result(row["result_json"])

    def list_results(self) -> tuple[BenchmarkResult, ...]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT result_json FROM benchmark_results "
                "ORDER BY finished_at, run_id"
            ).fetchall()
        return tuple(self._decode_result(row["result_json"]) for row in rows)
