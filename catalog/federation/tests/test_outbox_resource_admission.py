from __future__ import annotations

import sqlite3
import subprocess
import sys
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
    wal_path = Path(f"{database}-wal")
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
    assert requirements == (
        (
            database.parent,
            _OUTBOX_MUTATION_FIXED_BYTES + wal_bytes,
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


def test_large_current_v3_startup_does_not_checkpoint_unreserved_wal(
    tmp_path: Path,
) -> None:
    database = tmp_path / "outbox.sqlite3"
    original = SQLiteOutbox(database, resource_admission=_admission(1_000_000_000))
    entries = [_enqueue(original, key=f"key-{index}")[0] for index in range(3)]
    large_payload = '{"padding":"' + ("x" * 900_000) + '"}'

    # Model a process that crashed after committing a large WAL. Its abrupt
    # exit leaves the committed frames for the startup path to recover.
    write_large_wal = """
import os, sqlite3, sys
connection = sqlite3.connect(sys.argv[1])
connection.execute("PRAGMA wal_autocheckpoint=0")
payload = '{\\"padding\\":\\"' + ('x' * 900_000) + '\\"}'
connection.execute("UPDATE outbox SET payload_json=?", (payload,))
connection.commit()
os._exit(0)
"""
    subprocess.run([sys.executable, "-c", write_large_wal, str(database)], check=True)

    wal = Path(f"{database}-wal")
    assert wal.stat().st_size > _OUTBOX_MUTATION_FIXED_BYTES
    main_bytes_before = database.stat().st_size
    wal_bytes_before = wal.stat().st_size

    _large_outbox(database)

    # The large-schema no-op must not merge WAL pages into the main database:
    # that implicit last-connection checkpoint is outside its fixed reserve.
    assert database.stat().st_size == main_bytes_before
    assert wal.stat().st_size == wal_bytes_before
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        rows = connection.execute(
            "SELECT outbox_id, state, payload_json FROM outbox ORDER BY outbox_id"
        ).fetchall()
        assert rows == [
            (entry.outbox_id, "pending", large_payload) for entry in entries
        ]


def test_large_outbox_refusal_does_not_create_wal_sidecars_before_admission(
    tmp_path: Path,
) -> None:
    database = tmp_path / "outbox.sqlite3"
    write_wal_and_crash = '''
import os, sqlite3, sys
from catalog.federation.outbox import SQLiteOutbox
database = sys.argv[1]
SQLiteOutbox(database)
connection = sqlite3.connect(database)
connection.execute("PRAGMA wal_autocheckpoint=0")
payload = '{"padding":"' + ('x' * 900000) + '"}'
for index in range(3):
    connection.execute(
        "INSERT INTO outbox(session_id, destination_id, schema_id, payload_json, "
        "idempotency_key, content_hash, state, created_at, updated_at, next_attempt_at) "
        "VALUES(?, ?, ?, ?, ?, ?, 'pending', "
        "'2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00', "
        "'2026-01-01T00:00:00+00:00')",
        ('session', 'destination', 'schema', payload, f'key-{index}', f'hash-{index}'),
    )
connection.commit()
os._exit(0)
'''
    subprocess.run(
        [sys.executable, "-c", write_wal_and_crash, str(database)], check=True
    )

    wal = Path(f"{database}-wal")
    shm = Path(f"{database}-shm")
    assert wal.exists()
    assert shm.exists()
    # Remove the WAL index from a fresh process after the simulated crash. The
    # parent process has never opened this database, avoiding Windows mapped-
    # file locking while preserving the exact missing-sidecar startup case.
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import os, sys; os.unlink(sys.argv[1])",
            str(shm),
        ],
        check=True,
    )
    assert not shm.exists()
    wal_bytes_before = wal.stat().st_size
    main_bytes_before = database.stat().st_size
    assert wal_bytes_before > _OUTBOX_MUTATION_FIXED_BYTES

    oversized = _OversizedSQLiteOutbox.__new__(_OversizedSQLiteOutbox)
    oversized.database = str(database)
    oversized.database_path = database
    oversized.resource_admission = _admission(1_000_000_000)
    requirements = oversized._migration_requirements()
    assert requirements[0][1] == _OUTBOX_MUTATION_FIXED_BYTES + wal_bytes_before

    with pytest.raises(HostResourceRefused):
        _OversizedSQLiteOutbox(
            database,
            resource_admission=_admission(200),
        )

    assert not shm.exists()

    reopened = _large_outbox(database)
    assert shm.exists()
    assert database.stat().st_size == main_bytes_before
    assert wal.stat().st_size == wal_bytes_before
    assert len(reopened.pending()) == 3


def test_large_current_v3_outbox_accepts_quoted_table_identifier(
    tmp_path: Path,
) -> None:
    database = tmp_path / "outbox.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            "CREATE TABLE outbox_schema ("
            "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
            "version INTEGER NOT NULL CHECK (version > 0))"
        )
        connection.execute(
            "INSERT INTO outbox_schema(singleton, version) VALUES(1, 3)"
        )
        connection.execute(
            outbox_module._outbox_table_ddl('"outbox"', if_not_exists=False)
        )
        connection.execute(outbox_module._OUTBOX_INDEX_DDL)
        connection.execute(outbox_module._OUTBOX_DELIVERY_INDEX_DDL)
        connection.execute(outbox_module._OUTBOX_RETIRED_SUMMARY_INDEX_DDL)
        connection.commit()

    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        create_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='outbox'"
        ).fetchone()[0]
        assert 'CREATE TABLE "outbox"' in create_sql

    reopened = _large_outbox(database)
    assert reopened.pending() == ()
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        assert connection.execute(
            "SELECT version FROM outbox_schema WHERE singleton=1"
        ).fetchone()[0] == 3
        assert connection.execute(
            "SELECT COUNT(*) FROM outbox"
        ).fetchone()[0] == 0


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


def test_large_outbox_with_merged_primary_key_tokens_fails_closed(
    tmp_path: Path,
) -> None:
    database = tmp_path / "outbox.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            "CREATE TABLE outbox_schema ("
            "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
            "version INTEGER NOT NULL CHECK (version > 0))"
        )
        connection.execute(
            "INSERT INTO outbox_schema(singleton, version) VALUES(1, 3)"
        )
        current_ddl = outbox_module._outbox_table_ddl(
            "outbox", if_not_exists=False
        )
        malformed_ddl = current_ddl.replace(
            "outbox_id INTEGER PRIMARY KEY AUTOINCREMENT",
            "outbox_id INTEGERPRIMARYKEYAUTOINCREMENT",
        )
        assert malformed_ddl != current_ddl
        connection.execute(malformed_ddl)
        connection.execute(outbox_module._OUTBOX_INDEX_DDL)
        connection.execute(outbox_module._OUTBOX_DELIVERY_INDEX_DDL)
        connection.execute(outbox_module._OUTBOX_RETIRED_SUMMARY_INDEX_DDL)
        for idempotency_key in ("first", "second"):
            connection.execute(
                """
                INSERT INTO outbox(
                    outbox_id, session_id, destination_id, schema_id,
                    payload_json, idempotency_key, content_hash, state,
                    created_at, updated_at, next_attempt_at
                ) VALUES(1204, 'session', 'destination', 'schema', '{}', ?,
                    'sha256:test', 'pending', 'created', 'updated', 'next')
                """,
                (idempotency_key,),
            )
        connection.commit()

    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        outbox_id_info = next(
            row for row in connection.execute("PRAGMA table_info('outbox')")
            if row[1] == "outbox_id"
        )
        assert outbox_id_info[5] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM outbox WHERE outbox_id=1204"
        ).fetchone()[0] == 2

    with pytest.raises(FederationValidationError) as rejected:
        _large_outbox(database)

    assert rejected.value.code == "outbox-resource-envelope"
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        rows = connection.execute(
            "SELECT outbox_id, idempotency_key, state FROM outbox "
            "ORDER BY idempotency_key"
        ).fetchall()
        assert rows == [(1204, "first", "pending"), (1204, "second", "pending")]
