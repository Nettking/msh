"""History validation reuses only identical, already validated components."""
from __future__ import annotations

import json
import sqlite3
from collections import Counter

import pytest

from catalog.federation import manifest_store
from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest import (
    AuthoritativeStorageManifest,
    DatasetManifest,
    ManifestItem,
)
from catalog.federation.tests.test_manifest_read_snapshot import seeded


def count_decodes(monkeypatch):
    calls = Counter()
    for component in (DatasetManifest, ManifestItem):
        original = component.from_dict.__func__

        def observe(cls, value, original=original):
            calls[cls.__name__] += 1
            return original(cls, value)

        monkeypatch.setattr(component, "from_dict", classmethod(observe))
    original_hash = AuthoritativeStorageManifest.calculated_hash

    def hash_each_manifest(manifest):
        calls["manifest_hash"] += 1
        return original_hash(manifest)

    monkeypatch.setattr(AuthoritativeStorageManifest, "calculated_hash", hash_each_manifest)
    return calls


def test_each_history_validates_unique_components_but_hashes_every_revision(tmp_path, monkeypatch):
    control = seeded(tmp_path)
    control.manifests._verified_revisions = None  # Isolate the per-history decoder.
    expected = control.manifests.history("session-1", "storage-main")
    calls = count_decodes(monkeypatch)
    actual = control.manifests.history("session-1", "storage-main")
    assert [m.to_dict() for m in actual] == [m.to_dict() for m in expected]
    assert calls == {"ManifestItem": 8, "DatasetManifest": 1, "manifest_hash": 9}
    # A later database read performs fresh validation, even on the same store.
    assert control.manifests.history("session-1", "storage-main") == actual
    assert calls == {"ManifestItem": 16, "DatasetManifest": 2, "manifest_hash": 18}


def test_public_manifest_decode_remains_independent(tmp_path, monkeypatch):
    control = seeded(tmp_path, 3)
    history = control.manifests.history("session-1", "storage-main")
    calls = count_decodes(monkeypatch)
    assert tuple(AuthoritativeStorageManifest.from_dict(m.to_dict()) for m in history) == history
    assert calls == {"ManifestItem": 6, "DatasetManifest": 3, "manifest_hash": 4}


@pytest.mark.parametrize("collection,field,value", [
    ("items", "schema_version", True),
    ("items", "schema_version", 1.0),
    ("datasets", "required", 1),
])
def test_equal_python_values_cannot_hide_invalid_json_types(tmp_path, collection, field, value):
    control = seeded(tmp_path, 3)
    with sqlite3.connect(control.database) as connection:
        raw = json.loads(connection.execute(
            "SELECT manifest_json FROM storage_manifest_revisions WHERE revision=2"
        ).fetchone()[0])
        raw[collection][0][field] = value
        connection.execute(
            "UPDATE storage_manifest_revisions SET manifest_json=? WHERE revision=2", (json.dumps(raw),)
        )
    with pytest.raises(FederationValidationError) as failure:
        control.manifests.history("session-1", "storage-main")
    assert failure.value.field == field


@pytest.mark.parametrize("tamper,code", [
    ("UPDATE storage_manifest_revisions SET term=term+1 WHERE revision=2", "manifest-row-mismatch"),
    ("DELETE FROM storage_manifest_revisions WHERE revision=2", "manifest-revision-gap"),
    ("UPDATE storage_manifest_heads SET revision=1", "manifest-head-corrupt"),
])
def test_history_still_rejects_index_gap_and_head_tampering(tmp_path, tamper, code):
    control = seeded(tmp_path, 3)
    control.manifests.history("session-1", "storage-main")
    with sqlite3.connect(control.database) as connection:
        connection.execute(tamper)
    with pytest.raises(FederationValidationError) as failure:
        control.manifests.history("session-1", "storage-main")
    assert failure.value.code == code


@pytest.mark.parametrize("limit", [
    "_COMPONENT_CACHE_MAX_ENTRIES",
    "_COMPONENT_CACHE_MAX_BYTES",
    "_COMPONENT_CACHE_MAX_KEY_BYTES",
])
def test_cache_limits_fall_back_without_changing_manifest_bytes(tmp_path, monkeypatch, limit):
    control = seeded(tmp_path)
    control.manifests._verified_revisions = None  # Exercise component limits, not revision reuse.
    expected = control.manifests.history("session-1", "storage-main")
    observed = []
    original_clear = manifest_store._HistoryComponentDecoder.clear

    def clear(decoder):
        observed.append((len(decoder._cache), decoder._bytes))
        original_clear(decoder)

    monkeypatch.setattr(manifest_store._HistoryComponentDecoder, "clear", clear)
    monkeypatch.setattr(manifest_store, limit, 1)
    calls = count_decodes(monkeypatch)
    actual = control.manifests.history("session-1", "storage-main")
    assert [m.to_dict() for m in actual] == [m.to_dict() for m in expected]
    assert calls["ManifestItem"] == 36
    assert calls["manifest_hash"] == 9
    assert len(observed) == 1
    assert observed[0][0] <= manifest_store._COMPONENT_CACHE_MAX_ENTRIES
    assert observed[0][1] <= manifest_store._COMPONENT_CACHE_MAX_BYTES


def test_unknown_fields_and_scalar_types_are_part_of_cache_identity(tmp_path, monkeypatch):
    raw = seeded(tmp_path, 1).manifests.head("session-1", "storage-main").items[0].to_dict()
    expected = ManifestItem.from_dict(raw)
    calls = count_decodes(monkeypatch)
    decoder = manifest_store._HistoryComponentDecoder()
    # Unknown fields keep existing forward-compatible decoding, but must not
    # cause two different raw documents to bypass independent validation.
    for value in (True, 1, 1.0, "1", None, [1], {"value": 1}):
        changed = {**raw, "future_extension": value}
        assert decoder.decode(ManifestItem, changed) == expected
        assert decoder.decode(ManifestItem, changed) == expected
    assert calls["ManifestItem"] == 7


def deep_extension():
    value = 0
    for _ in range(36):
        value = [value]
    return value


@pytest.mark.parametrize("extension", [
    pytest.param("x" * (64 * 1024), id="oversize"),
    pytest.param(float("nan"), id="nonfinite"),
    pytest.param("\ud800", id="surrogate"),
    pytest.param(deep_extension(), id="depth"),
])
def test_unkeyable_extensions_keep_ordinary_decoder_semantics(tmp_path, monkeypatch, extension):
    raw = seeded(tmp_path, 1).manifests.head("session-1", "storage-main").items[0].to_dict()
    raw["future_extension"] = extension
    expected = ManifestItem.from_dict(raw)
    calls = count_decodes(monkeypatch)
    decoder = manifest_store._HistoryComponentDecoder()
    assert decoder.decode(ManifestItem, raw) == expected
    assert decoder.decode(ManifestItem, raw) == expected
    assert calls["ManifestItem"] == 2
    assert not decoder._cache
    assert decoder._bytes == 0


def test_failed_history_releases_validated_components(tmp_path, monkeypatch):
    control = seeded(tmp_path, 3)
    with sqlite3.connect(control.database) as connection:
        connection.execute(
            "UPDATE storage_manifest_revisions SET manifest_json='null' WHERE revision=2"
        )
    retained = []
    original_clear = manifest_store._HistoryComponentDecoder.clear

    def clear(decoder):
        assert decoder._cache  # Revision 1 was decoded before the bad row.
        retained.append(decoder)
        original_clear(decoder)

    monkeypatch.setattr(manifest_store._HistoryComponentDecoder, "clear", clear)
    with pytest.raises(FederationValidationError) as failure:
        control.manifests.history("session-1", "storage-main")
    assert failure.value.code == "invalid-object"
    assert len(retained) == 1
    assert not retained[0]._cache
    assert retained[0]._bytes == 0


def test_rehashed_revision_with_wrong_predecessor_still_fails_chain(tmp_path):
    control = seeded(tmp_path, 3)
    with sqlite3.connect(control.database) as connection:
        raw = json.loads(connection.execute(
            "SELECT manifest_json FROM storage_manifest_revisions WHERE revision=2"
        ).fetchone()[0])
        original = AuthoritativeStorageManifest.from_dict(raw)
        changed = AuthoritativeStorageManifest.build(
            session_id=original.session_id, group_id=original.group_id,
            revision=original.revision, term=original.term,
            previous_manifest_hash="sha256:" + "f" * 64,
            datasets=original.datasets, items=original.items, updated_at=original.updated_at,
        )
        connection.execute(
            "UPDATE storage_manifest_revisions SET manifest_json=?, previous_manifest_hash=?, "
            "manifest_hash=? WHERE revision=2",
            (json.dumps(changed.to_dict()), changed.previous_manifest_hash, changed.manifest_hash),
        )
    with pytest.raises(FederationValidationError) as failure:
        control.manifests.history("session-1", "storage-main")
    assert failure.value.code == "manifest-chain-mismatch"
