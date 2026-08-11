from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.outbox import (
    COMPLETED_RECEIPT_SCHEMA,
    MAX_COMPLETED_RECEIPT_BYTES,
    SCHEMA_VERSION,
    OutboxState,
    SQLiteOutbox,
)

NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)


def _enqueue(
    outbox: SQLiteOutbox,
    *,
    key: str = "delivery-1",
    schema_id: str = "fcp.delivery.v1",
    content_hash: str = "sha256:content-a",
    payload: object | None = None,
):
    return outbox.enqueue(
        session_id="session-1",
        destination_id="storage-a",
        schema_id=schema_id,
        payload={"blob": "x" * 250_000} if payload is None else payload,
        idempotency_key=key,
        content_hash=content_hash,
        now=NOW,
    )


def _raw_row(database: Path, outbox_id: int) -> sqlite3.Row:
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)
        ).fetchone()
        assert row is not None
        return row
    finally:
        connection.close()


def test_acknowledge_atomically_replaces_large_payload_with_bounded_receipt(
    tmp_path: Path,
) -> None:
    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    pending, created = _enqueue(outbox)
    before = _raw_row(database, pending.outbox_id)
    assert created is True
    assert len(before["payload_json"]) > 200_000
    assert before["payload_compacted"] == 0

    completed = outbox.acknowledge(pending.outbox_id, now=NOW)
    after = _raw_row(database, pending.outbox_id)

    assert completed.state is OutboxState.COMPLETED
    assert completed.payload == {
        "schema": COMPLETED_RECEIPT_SCHEMA,
        "schema_id": pending.schema_id,
        "idempotency_key": pending.idempotency_key,
        "content_hash": pending.content_hash,
    }
    assert after["payload_compacted"] == 1
    assert len(after["payload_json"].encode("utf-8")) <= MAX_COMPLETED_RECEIPT_BYTES
    assert "x" * 1_000 not in after["payload_json"]
    assert after["session_id"] == pending.session_id
    assert after["destination_id"] == pending.destination_id
    assert after["schema_id"] == pending.schema_id
    assert after["idempotency_key"] == pending.idempotency_key
    assert after["content_hash"] == pending.content_hash

    # A later acknowledgement after reopening performs no semantic mutation.
    repeated = SQLiteOutbox(database).acknowledge(
        pending.outbox_id,
        now=NOW + timedelta(days=1),
    )
    assert repeated == completed
    assert (
        _raw_row(database, pending.outbox_id)["payload_json"] == after["payload_json"]
    )


def test_completed_reenqueue_uses_schema_and_hash_but_pending_stays_exact(
    tmp_path: Path,
) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    pending, _ = _enqueue(outbox, payload={"original": True})

    with pytest.raises(FederationValidationError) as pending_conflict:
        _enqueue(outbox, payload={"different": True})
    assert pending_conflict.value.code == "idempotency-conflict"
    assert outbox.get(pending.outbox_id) == pending

    completed = outbox.acknowledge(pending.outbox_id, now=NOW)
    replay, created = _enqueue(
        outbox,
        payload={"the-original-payload-is-no-longer-required": True},
    )
    assert created is False
    assert replay == completed

    with pytest.raises(FederationValidationError) as schema_conflict:
        _enqueue(
            outbox,
            schema_id="fcp.delivery.v2",
            payload={"original": True},
        )
    assert schema_conflict.value.code == "idempotency-conflict"

    with pytest.raises(FederationValidationError) as hash_conflict:
        _enqueue(
            outbox,
            content_hash="sha256:content-b",
            payload={"original": True},
        )
    assert hash_conflict.value.code == "idempotency-conflict"
    assert outbox.get(pending.outbox_id) == completed


def test_receipt_remains_bounded_for_legacy_oversized_identity(tmp_path: Path) -> None:
    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    oversized_key = "legacy-key-" + "x" * (MAX_COMPLETED_RECEIPT_BYTES * 2)
    pending, _ = _enqueue(
        outbox,
        key=oversized_key,
        payload={"value": "small"},
    )

    completed = outbox.acknowledge(pending.outbox_id, now=NOW)
    row = _raw_row(database, pending.outbox_id)

    assert completed.idempotency_key == oversized_key
    assert completed.payload["schema"] == COMPLETED_RECEIPT_SCHEMA
    assert completed.payload["identity_hash"].startswith("sha256:")
    assert len(row["payload_json"].encode("utf-8")) <= MAX_COMPLETED_RECEIPT_BYTES
    assert row["idempotency_key"] == oversized_key


def test_compact_completed_is_bounded_and_never_touches_live_rows(
    tmp_path: Path,
) -> None:
    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    first, _ = _enqueue(outbox, key="first")
    second, _ = _enqueue(outbox, key="second")
    live, _ = _enqueue(outbox, key="pending")

    # Simulate two completed rows written by schema v1 before automatic
    # compaction existed.
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE outbox SET state='completed' WHERE outbox_id IN (?, ?)",
            (first.outbox_id, second.outbox_id),
        )
    timestamps = {
        row_id: _raw_row(database, row_id)["updated_at"]
        for row_id in (first.outbox_id, second.outbox_id, live.outbox_id)
    }

    assert outbox.compact_completed(limit=1) == 1
    assert _raw_row(database, first.outbox_id)["payload_compacted"] == 1
    assert _raw_row(database, second.outbox_id)["payload_compacted"] == 0
    assert _raw_row(database, live.outbox_id)["payload_compacted"] == 0
    assert outbox.compact_completed(limit=1) == 1
    assert outbox.compact_completed(limit=1) == 0

    assert outbox.get(first.outbox_id).state is OutboxState.COMPLETED  # type: ignore[union-attr]
    assert outbox.get(second.outbox_id).state is OutboxState.COMPLETED  # type: ignore[union-attr]
    assert outbox.get(live.outbox_id) == live
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 3
    assert {
        row_id: _raw_row(database, row_id)["updated_at"] for row_id in timestamps
    } == timestamps

    for invalid_limit in (0, -1, True, 1.5, "1"):
        with pytest.raises(FederationValidationError) as invalid:
            outbox.compact_completed(limit=invalid_limit)  # type: ignore[arg-type]
        assert invalid.value.code == "invalid-compaction-limit"


def test_schema_v1_migrates_without_compacting_pending_or_deleting_rows(
    tmp_path: Path,
) -> None:
    database = tmp_path / "legacy.sqlite3"
    timestamp = NOW.isoformat()
    original_payload = json.dumps({"blob": "legacy" * 10_000})
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE outbox_schema (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                version INTEGER NOT NULL CHECK (version > 0)
            );
            INSERT INTO outbox_schema(singleton, version) VALUES(1, 1);
            CREATE TABLE outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL CHECK(length(session_id) > 0),
                destination_id TEXT NOT NULL CHECK(length(destination_id) > 0),
                schema_id TEXT NOT NULL CHECK(length(schema_id) > 0),
                payload_json TEXT NOT NULL CHECK(
                    length(payload_json) <= 1048576 AND json_valid(payload_json)
                ),
                idempotency_key TEXT NOT NULL CHECK(length(idempotency_key) > 0),
                content_hash TEXT NOT NULL CHECK(length(content_hash) > 0),
                state TEXT NOT NULL DEFAULT 'pending' CHECK(
                    state IN ('prepared','pending','completed')
                ),
                attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                next_attempt_at TEXT NOT NULL,
                last_error TEXT CHECK(
                    last_error IS NULL OR length(last_error) <= 2048
                ),
                UNIQUE(session_id, destination_id, idempotency_key)
            );
            """
        )
        connection.executemany(
            """INSERT INTO outbox
               (session_id,destination_id,schema_id,payload_json,idempotency_key,
                content_hash,state,created_at,updated_at,next_attempt_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                (
                    "session-1",
                    "storage-a",
                    "fcp.delivery.v1",
                    original_payload,
                    "legacy-completed",
                    "sha256:legacy",
                    "completed",
                    timestamp,
                    timestamp,
                    timestamp,
                ),
                (
                    "session-1",
                    "storage-a",
                    "fcp.delivery.v1",
                    original_payload,
                    "legacy-pending",
                    "sha256:pending",
                    "pending",
                    timestamp,
                    timestamp,
                    timestamp,
                ),
            ),
        )

    migrated = SQLiteOutbox(database)
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute(
                "SELECT version FROM outbox_schema WHERE singleton=1"
            ).fetchone()[0]
            == SCHEMA_VERSION
        )
        columns = {row[1] for row in connection.execute("PRAGMA table_info(outbox)")}
        assert "payload_compacted" in columns
        assert connection.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 2

    assert len(migrated.pending()) == 1
    assert migrated.compact_completed(limit=10) == 1
    assert len(SQLiteOutbox(database).pending()) == 1
    assert _raw_row(database, 1)["payload_compacted"] == 1
    assert _raw_row(database, 2)["payload_compacted"] == 0


def test_tampered_completed_receipt_is_rejected_not_silently_rewritten(
    tmp_path: Path,
) -> None:
    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    pending, _ = _enqueue(outbox, payload={"value": 1})
    outbox.acknowledge(pending.outbox_id, now=NOW)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE outbox SET payload_json=? WHERE outbox_id=?",
            ('{"schema":"fcp.outbox.completed_receipt.v1"}', pending.outbox_id),
        )

    with pytest.raises(FederationValidationError) as malformed:
        outbox.get(pending.outbox_id)
    assert malformed.value.code == "malformed-outbox-row"
    with pytest.raises(FederationValidationError) as acknowledge:
        SQLiteOutbox(database).acknowledge(
            pending.outbox_id,
            now=NOW + timedelta(seconds=1),
        )
    assert acknowledge.value.code == "malformed-outbox-row"
    assert _raw_row(database, pending.outbox_id)["payload_json"] == (
        '{"schema":"fcp.outbox.completed_receipt.v1"}'
    )
