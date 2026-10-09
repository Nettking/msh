from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation import outbox as outbox_module
from catalog.federation.errors import FederationValidationError
from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
)
from catalog.federation.outbox import (
    _OUTBOX_MAX_MIGRATION_BYTES,
    _OUTBOX_MUTATION_FIXED_BYTES,
    _OUTBOX_MUTATION_FIXED_INODES,
    OutboxState,
    SQLiteOutbox,
)
from catalog.federation.process_resource_admission import (
    SerializedProcessResourceAdmission,
)

NOW = datetime(2026, 8, 31, tzinfo=timezone.utc)


def _admission(free_bytes: int) -> SerializedProcessResourceAdmission:
    return SerializedProcessResourceAdmission(
        thresholds=PressureThresholds(
            critical_free_bytes=100,
            pressure_free_bytes=200,
            warning_free_bytes=300,
            critical_free_inodes=0,
            pressure_free_inodes=0,
            warning_free_inodes=0,
            max_measurement_age_seconds=60,
        ),
        measurer=lambda _path: FilesystemMeasurement(
            resource_id="outbox-resource",
            observed_at=NOW,
            total_bytes=10_000_000,
            free_bytes=free_bytes,
            total_inodes=None,
            free_inodes=None,
            available=True,
        ),
        clock=lambda: NOW,
    )


def _enqueue(outbox: SQLiteOutbox, key: str = "key-1"):
    return outbox.enqueue(
        session_id="session-1",
        destination_id="destination-1",
        schema_id="schema-1",
        payload={"key": key, "value": 42},
        idempotency_key=key,
        content_hash=f"sha256:{key}",
        now=NOW,
    )


class _OversizedSQLiteOutbox(SQLiteOutbox):
    """Exercise the large-file admission path without creating a huge fixture."""

    def _database_bytes(self) -> int:
        return _OUTBOX_MAX_MIGRATION_BYTES + 1


def _large_outbox(database: Path) -> _OversizedSQLiteOutbox:
    return _OversizedSQLiteOutbox(
        database,
        resource_admission=_admission(1_000_000_000),
    )


def test_outbox_startup_refusal_happens_before_parent_creation(tmp_path: Path) -> None:
    database = tmp_path / "managed-outbox" / "outbox.sqlite3"

    with pytest.raises(HostResourceRefused):
        SQLiteOutbox(database, resource_admission=_admission(200))

    assert not database.parent.exists()


def test_outbox_enqueue_refusal_leaves_durable_state_unchanged(tmp_path: Path) -> None:
    admission = _admission(1_000_000_000)
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3", resource_admission=admission)
    entry, created = _enqueue(outbox)
    assert created

    admission.measurer = lambda _path: FilesystemMeasurement(
        resource_id="outbox-resource",
        observed_at=NOW,
        total_bytes=10_000_000,
        free_bytes=200,
        total_inodes=None,
        free_inodes=None,
        available=True,
    )
    with pytest.raises(HostResourceRefused):
        _enqueue(outbox, key="key-2")

    assert outbox.get(entry.outbox_id).state is OutboxState.PENDING
    assert outbox.get(entry.outbox_id + 1) is None


def test_outbox_exception_unwinds_admission_for_the_next_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission = _admission(1_000_000_000)
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3", resource_admission=admission)
    original_connect = outbox._connect

    def fail_connect():
        raise OSError("injected outbox connection failure")

    monkeypatch.setattr(outbox, "_connect", fail_connect)
    with pytest.raises(OSError, match="injected outbox connection failure"):
        _enqueue(outbox)

    monkeypatch.setattr(outbox, "_connect", original_connect)
    entry, created = _enqueue(outbox)
    assert created
    assert entry.state is OutboxState.PENDING


def test_large_current_v3_outbox_uses_only_bounded_noop_startup(tmp_path: Path) -> None:
    database = tmp_path / "outbox.sqlite3"
    original = SQLiteOutbox(database, resource_admission=_admission(1_000_000_000))
    entry, _ = _enqueue(original)

    oversized = _OversizedSQLiteOutbox.__new__(_OversizedSQLiteOutbox)
    oversized.database = str(database)
    oversized.database_path = database
    oversized.resource_admission = _admission(1_000_000_000)
    requirements = oversized._migration_requirements()
    assert requirements == (
        (
            database.parent,
            _OUTBOX_MUTATION_FIXED_BYTES,
            _OUTBOX_MUTATION_FIXED_INODES + 2,
        ),
    )

    reopened = _large_outbox(database)
    preserved = reopened.get(entry.outbox_id)
    assert preserved == entry
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        assert connection.execute(
            "SELECT version FROM outbox_schema WHERE singleton=1"
        ).fetchone()[0] == 3
        assert connection.execute(
            "SELECT COUNT(*) FROM outbox"
        ).fetchone()[0] == 1


def test_large_outbox_with_old_schema_still_fails_closed(tmp_path: Path) -> None:
    database = tmp_path / "outbox.sqlite3"
    SQLiteOutbox(database, resource_admission=_admission(1_000_000_000))
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE outbox_schema SET version=2 WHERE singleton=1"
        )
        connection.commit()

    with pytest.raises(FederationValidationError) as rejected:
        _large_outbox(database)

    assert rejected.value.code == "outbox-resource-envelope"
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        assert connection.execute(
            "SELECT version FROM outbox_schema WHERE singleton=1"
        ).fetchone()[0] == 2


def test_large_outbox_with_missing_index_still_fails_closed(tmp_path: Path) -> None:
    database = tmp_path / "outbox.sqlite3"
    SQLiteOutbox(database, resource_admission=_admission(1_000_000_000))
    with sqlite3.connect(database) as connection:
        connection.execute("DROP INDEX outbox_pending_due")
        connection.commit()

    with pytest.raises(FederationValidationError) as rejected:
        _large_outbox(database)

    assert rejected.value.code == "outbox-resource-envelope"
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='outbox_pending_due'"
        ).fetchone() is None


def test_large_outbox_without_idempotency_constraint_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "outbox.sqlite3"
    original_ddl = outbox_module._outbox_table_ddl

    def without_idempotency_constraint(name: str, *, if_not_exists: bool) -> str:
        unique_clause = (
            ",\n"
            "                    UNIQUE(session_id, destination_id, idempotency_key)"
        )
        return original_ddl(name, if_not_exists=if_not_exists).replace(
            unique_clause, ""
        )

    monkeypatch.setattr(
        outbox_module, "_outbox_table_ddl", without_idempotency_constraint
    )
    SQLiteOutbox(database, resource_admission=_admission(1_000_000_000))
    monkeypatch.setattr(outbox_module, "_outbox_table_ddl", original_ddl)

    with pytest.raises(FederationValidationError) as rejected:
        _large_outbox(database)

    assert rejected.value.code == "outbox-resource-envelope"
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        create_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='outbox'"
        ).fetchone()[0]
        assert "UNIQUE(session_id, destination_id, idempotency_key)" not in create_sql
