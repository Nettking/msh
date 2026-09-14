"""Whole-chain integrity must cost one pass in one SQLite read snapshot."""
from __future__ import annotations

import gc
import json
import sqlite3
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest_store import AuthoritativeManifestStore
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.tests.test_phase_e1_manifest_store import proposal


def seeded(tmp_path: Path, count: int = 8):
    control = PhaseDControlPlane(tmp_path / "manifest.sqlite3")
    control.create_group("session-1", "coordinator-1", "storage-main")
    for index in range(count):
        control.manifests.commit(proposal(
            item_id=f"batch-{index}",
            idempotency_key=f"recorder:batch-{index}",
        ))
    return control


def read(control, method):
    if method == "phase-d-history":
        return control.manifest_history("session-1", "storage-main")
    if method == "revision":
        return control.manifests.at_revision("session-1", "storage-main", 1)
    return getattr(control.manifests, method)("session-1", "storage-main")


@pytest.mark.parametrize("count", [3, 8])
@pytest.mark.parametrize("method", ["head", "history", "revision", "phase-d-history"])
def test_every_read_validates_each_retained_revision_once(tmp_path, monkeypatch, count, method):
    control = seeded(tmp_path, count)
    decoded = []
    original = AuthoritativeManifestStore._decode_revision

    def observe(row):
        decoded.append(row["revision"])
        return original(row)

    monkeypatch.setattr(control.manifests, "_decode_revision", observe)
    result = read(control, method)
    assert decoded == list(range(count + 1))
    if isinstance(result, tuple):
        assert result[-1].revision == count
    else:
        assert result.revision == (1 if method == "revision" else count)


@pytest.mark.parametrize("method", ["head", "history", "revision", "phase-d-history"])
def test_a_prior_valid_read_does_not_trust_corrupted_history(tmp_path, method):
    control = seeded(tmp_path)
    read(control, method)
    with sqlite3.connect(control.database) as connection:
        encoded = json.loads(connection.execute(
            "SELECT manifest_json FROM storage_manifest_revisions WHERE revision=1"
        ).fetchone()[0])
        encoded["items"][0]["content_hash"] = "sha256:" + "f" * 64
        connection.execute(
            "UPDATE storage_manifest_revisions SET manifest_json=? WHERE revision=1",
            (json.dumps(encoded),),
        )
    with pytest.raises(FederationValidationError, match="hash"):
        read(control, method)


@pytest.mark.parametrize("method", ["head", "revision"])
def test_head_and_chain_share_a_snapshot_during_concurrent_commit(tmp_path, monkeypatch, method):
    control = seeded(tmp_path)
    writer = AuthoritativeManifestStore(control.database)
    original = control.manifests._history
    advanced = False

    def append_during_read(connection, session_id, group_id):
        nonlocal advanced
        if not advanced:
            advanced = True
            writer.commit(proposal(item_id="new-batch", idempotency_key="recorder:new-batch"))
        return original(connection, session_id, group_id)

    monkeypatch.setattr(control.manifests, "_history", append_during_read)
    result = read(control, method)
    assert advanced
    assert result.revision == (1 if method == "revision" else 8)
    assert writer.head("session-1", "storage-main").revision == 9


@pytest.mark.parametrize("method", ["history", "revision", "phase-d-history"])
def test_head_stays_in_the_snapshot_after_history_was_fetched(tmp_path, monkeypatch, method):
    control = seeded(tmp_path)
    writer = AuthoritativeManifestStore(control.database)
    original = control.manifests._history
    observed_heads = []

    def append_after_history(connection, session_id, group_id):
        history = original(connection, session_id, group_id)
        if not observed_heads:
            writer.commit(proposal(item_id="new-batch", idempotency_key="recorder:new-batch"))
            observed_heads.append(connection.execute(
                "SELECT revision FROM storage_manifest_heads WHERE session_id=? AND group_id=?",
                (session_id, group_id),
            ).fetchone()[0])
        return history

    monkeypatch.setattr(control.manifests, "_history", append_after_history)
    read(control, method)
    assert observed_heads == [8]
    assert writer.head("session-1", "storage-main").revision == 9


def test_history_keeps_group_validation_and_genesis_initialization(tmp_path):
    control = PhaseDControlPlane(tmp_path / "manifest.sqlite3")
    with pytest.raises(FederationValidationError) as unknown:
        control.manifest_history("session-1", "unknown")
    assert unknown.value.code == "unknown-storage-group"
    # The group was created before the manifest subsystem existed.
    control.store.create_group("session-1", "coordinator-1", "storage-main")
    assert [m.revision for m in control.manifest_history("session-1", "storage-main")] == [0]


@pytest.mark.parametrize("method", ["head", "history", "revision", "phase-d-history"])
def test_missing_database_after_a_read_is_not_answered_from_cache(tmp_path, method):
    control = seeded(tmp_path)
    read(control, method)
    # No product process owns this fresh test database.
    gc.collect()  # Release legacy constructor/writer SQLite statement cycles.
    Path(control.database).unlink()
    with pytest.raises(sqlite3.DatabaseError):
        read(control, method)
