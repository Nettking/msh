from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
)
from catalog.federation.outbox import OutboxState, SQLiteOutbox
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
