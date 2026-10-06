"""Reuse is content-verified, bounded, and never replaces snapshot integrity."""
from __future__ import annotations

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from catalog.federation import manifest_store
from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest import AuthoritativeStorageManifest
from catalog.federation.manifest_store import AuthoritativeManifestStore
from catalog.federation.tests.test_manifest_read_snapshot import seeded
from catalog.federation.tests.test_phase_e1_manifest_store import proposal


def fresh(tmp_path, count=8):
    control = seeded(tmp_path, count)
    # Independent cold reader, rather than the writer's legitimately warm cache.
    return control, AuthoritativeManifestStore(control.database)


def count_reads(monkeypatch, store):
    decoded, fingerprints = [], []
    decode = store._decode_revision
    fingerprint = manifest_store._revision_fingerprint

    def observe(row, **kwargs):
        decoded.append(row["revision"])
        return decode(row, **kwargs)

    def observe_fingerprint(row):
        fingerprints.append(row["revision"])
        return fingerprint(row)

    monkeypatch.setattr(store, "_decode_revision", observe)
    monkeypatch.setattr(manifest_store, "_revision_fingerprint", observe_fingerprint)
    return decoded, fingerprints


def test_warm_read_fingerprints_every_row_and_rechecks_head(tmp_path, monkeypatch):
    _, store = fresh(tmp_path)
    decoded, fingerprints = count_reads(monkeypatch, store)
    expected = store.history("session-1", "storage-main")
    assert decoded == list(range(9))
    decoded.clear()
    fingerprints.clear()
    assert store.history("session-1", "storage-main") == expected
    assert decoded == []
    assert fingerprints == list(range(9))
    with sqlite3.connect(store.database) as connection:
        connection.execute("UPDATE storage_manifest_heads SET revision=1")
    with pytest.raises(FederationValidationError, match="head"):
        store.head("session-1", "storage-main")


def test_append_reuses_verified_prefix_and_immutable_components(tmp_path, monkeypatch):
    control, store = fresh(tmp_path)
    before = store.history("session-1", "storage-main")
    decoded, fingerprints = count_reads(monkeypatch, store)
    control.manifests.commit(proposal(item_id="appended", idempotency_key="recorder:appended"))
    fingerprints.clear()  # The separate writer also fingerprints its snapshot.
    after = store.history("session-1", "storage-main")
    assert decoded == [9]
    assert fingerprints == list(range(10))
    assert after[:-1] == before
    assert next(item for item in after[-1].items if item.item_id == "batch-0") is before[-1].items[0]
    assert store._verified_revisions.retained_bytes < 64 * 1024 * 1024


@pytest.mark.parametrize("sql,code", [
    ("UPDATE storage_manifest_revisions SET term=term+1 WHERE revision=1", "manifest-row-mismatch"),
    ("UPDATE storage_manifest_revisions SET updated_at='broken' WHERE revision=1", "malformed-manifest-row"),
    ("DELETE FROM storage_manifest_revisions WHERE revision=1", "manifest-revision-gap"),
    ("UPDATE storage_manifest_heads SET manifest_hash='sha256:'||printf('%064d',0)", "manifest-head-corrupt"),
])
def test_prior_valid_cache_cannot_hide_old_corruption(tmp_path, sql, code):
    _, store = fresh(tmp_path)
    store.history("session-1", "storage-main")
    with sqlite3.connect(store.database) as connection:
        connection.execute(sql)
    with pytest.raises(FederationValidationError) as failure:
        store.history("session-1", "storage-main")
    assert failure.value.code == code


@pytest.mark.parametrize("changed", [True, 1.0, "1"])
def test_json_scalar_type_change_is_not_a_hit(tmp_path, changed):
    _, store = fresh(tmp_path)
    store.head("session-1", "storage-main")
    with sqlite3.connect(store.database) as connection:
        raw = json.loads(connection.execute("SELECT manifest_json FROM storage_manifest_revisions WHERE revision=1").fetchone()[0])
        raw["items"][0]["schema_version"] = changed
        connection.execute("UPDATE storage_manifest_revisions SET manifest_json=? WHERE revision=1", (json.dumps(raw),))
    with pytest.raises(FederationValidationError) as failure:
        store.history("session-1", "storage-main")
    assert failure.value.field == "schema_version"


def test_exact_unknown_json_bytes_and_index_types_affect_fingerprint(tmp_path):
    _, store = fresh(tmp_path, 1)
    with store._connect() as connection:
        row = dict(connection.execute("SELECT * FROM storage_manifest_revisions WHERE revision=1").fetchone())
    original = manifest_store._revision_fingerprint(row)
    for value in (True, 1.0, "1"):
        changed = {**row, "term": value}
        assert manifest_store._revision_fingerprint(changed) != original
    raw = json.loads(row["manifest_json"])
    raw["future"] = True
    assert manifest_store._revision_fingerprint({**row, "manifest_json": json.dumps(raw)}) != original
    assert manifest_store._revision_fingerprint({**row, "manifest_json": "\ud800"}) is None


def test_rehashed_changed_predecessor_still_fails_chain(tmp_path):
    _, store = fresh(tmp_path, 3)
    old = store.history("session-1", "storage-main")[2]
    changed = AuthoritativeStorageManifest.build(
        session_id=old.session_id, group_id=old.group_id, revision=old.revision,
        term=old.term, previous_manifest_hash="sha256:" + "f" * 64,
        datasets=old.datasets, items=old.items, updated_at=old.updated_at,
    )
    with sqlite3.connect(store.database) as connection:
        connection.execute("UPDATE storage_manifest_revisions SET manifest_json=?, previous_manifest_hash=?, manifest_hash=? WHERE revision=2", (
            json.dumps(changed.to_dict()), changed.previous_manifest_hash, changed.manifest_hash,
        ))
    with pytest.raises(FederationValidationError) as failure:
        store.history("session-1", "storage-main")
    assert failure.value.code == "manifest-chain-mismatch"


@pytest.mark.parametrize("limit", ["_REVISION_CACHE_MAX_BYTES", "_REVISION_CACHE_MAX_ENTRIES", "_REVISION_CACHE_MAX_OBJECTS"])
def test_small_cache_falls_back_to_complete_validation(tmp_path, monkeypatch, limit):
    _, store = fresh(tmp_path)
    monkeypatch.setattr(manifest_store, limit, 1)
    decoded, _ = count_reads(monkeypatch, store)
    expected = store.history("session-1", "storage-main")
    assert store.history("session-1", "storage-main") == expected
    assert decoded == list(range(9)) * 2
    assert not store._verified_revisions.revisions


def test_lock_contention_falls_back_without_waiting_or_trusting_data(tmp_path, monkeypatch):
    _, store = fresh(tmp_path)
    expected = store.history("session-1", "storage-main")
    decoded, _ = count_reads(monkeypatch, store)
    with store._verified_revisions.lock:
        assert store.history("session-1", "storage-main") == expected
    assert decoded == list(range(9))


def test_concurrent_old_snapshot_and_writer_never_mix_chains(tmp_path, monkeypatch):
    control, store = fresh(tmp_path)
    store.history("session-1", "storage-main")
    entered, release = threading.Event(), threading.Event()
    original = manifest_store._revision_fingerprint

    def held(row):
        if threading.current_thread().name.startswith("old-snapshot") and row["revision"] == 1:
            entered.set()
            assert release.wait(3)
        return original(row)

    monkeypatch.setattr(manifest_store, "_revision_fingerprint", held)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="old-snapshot") as executor:
        old_read = executor.submit(store.head, "session-1", "storage-main")
        try:
            assert entered.wait(3)
            control.manifests.commit(proposal(item_id="appended", idempotency_key="recorder:appended"))
            assert store.head("session-1", "storage-main").revision == 9
        finally:
            release.set()
        assert old_read.result(3).revision == 8
    assert store.head("session-1", "storage-main").revision == 9


def test_retained_graph_counts_shared_components_and_rejects_mutable_graph():
    repeated = ("x" * 10000,)
    assert manifest_store._retained_graph_size((repeated,) * 1000) < 30000
    assert manifest_store._retained_graph_size(([1],)) is None
