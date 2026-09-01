from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from catalog.federation.tests.test_recorder_publication import PROBE_XML, SAMPLE_XML
from catalog.mtconnect_recorder import DurableRecorderStore, parse_probe, parse_streams
from catalog.mtconnect_recorder.model import MtconnectProtocolError
from catalog.mtconnect_recorder.publication_frontier import RecorderPublicationFrontier


def _stored_ref(store: DurableRecorderStore):
    probe = parse_probe(PROBE_XML)
    batch = parse_streams(SAMPLE_XML, source_name="Mazak", probe=probe)
    ref = store.store_raw_batch(
        source_name="Mazak",
        requested_from=10,
        xml_text=SAMPLE_XML,
        batch=batch,
    )
    return batch, ref


def test_pending_frontier_round_trips_without_rereading_raw_manifest(tmp_path, monkeypatch):
    store = DurableRecorderStore(tmp_path / "data")
    frontier = RecorderPublicationFrontier(store)
    batch, ref = _stored_ref(store)

    frontier.mark_pending(
        source_name="Mazak",
        archive_source_name="Mazak",
        instance_id=batch.header.instance_id,
        ref=ref,
    )

    original_read_text = Path.read_text

    def refuse_manifest(path: Path, *args, **kwargs):
        if path == ref.manifest_path:
            raise AssertionError("pending discovery reopened the raw manifest")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", refuse_manifest)

    pending = frontier.pending(
        source_name="Mazak",
        instance_id=batch.header.instance_id,
    )

    assert len(pending) == 1
    assert pending[0].ref.first_sequence == 10
    assert pending[0].ref.last_sequence == 12
    assert pending[0].ref.next_sequence == 13
    assert pending[0].ref.raw_sha256 == ref.raw_sha256
    assert pending[0].ref.raw_path == ref.raw_path
    assert pending[0].ref.manifest_path == ref.manifest_path


def test_retirement_removes_only_discovery_not_recorder_evidence(tmp_path):
    store = DurableRecorderStore(tmp_path / "data")
    frontier = RecorderPublicationFrontier(store)
    batch, ref = _stored_ref(store)
    frontier.mark_pending(
        source_name="Mazak",
        archive_source_name="Mazak",
        instance_id=batch.header.instance_id,
        ref=ref,
    )
    pending = frontier.pending(
        source_name="Mazak",
        instance_id=batch.header.instance_id,
    )

    frontier.retire(pending[0])

    assert frontier.pending(
        source_name="Mazak",
        instance_id=batch.header.instance_id,
    ) == ()
    assert ref.raw_path.exists()
    assert ref.manifest_path.exists()


def test_initialized_marker_is_explicit_and_source_instance_scoped(tmp_path):
    store = DurableRecorderStore(tmp_path / "data")
    frontier = RecorderPublicationFrontier(store)

    assert not frontier.initialized(source_name="Mazak", instance_id=77)

    state = frontier.mark_initialized(source_name="Mazak", instance_id=77)

    assert state.exists()
    assert frontier.initialized(source_name="Mazak", instance_id=77)
    assert not frontier.initialized(source_name="Mazak", instance_id=78)
    assert not frontier.initialized(source_name="Other", instance_id=77)


def test_frontier_rejects_tampered_path_components(tmp_path):
    store = DurableRecorderStore(tmp_path / "data")
    frontier = RecorderPublicationFrontier(store)
    batch, ref = _stored_ref(store)
    record = frontier.mark_pending(
        source_name="Mazak",
        archive_source_name="Mazak",
        instance_id=batch.header.instance_id,
        ref=ref,
    )
    text = record.read_text(encoding="utf-8")
    record.write_text(
        text.replace(f'"raw_name": "{ref.raw_path.name}"', '"raw_name": "../escape.xml.gz"'),
        encoding="utf-8",
    )

    with pytest.raises(MtconnectProtocolError, match="raw_name"):
        frontier.pending(source_name="Mazak", instance_id=batch.header.instance_id)


def test_frontier_rejects_archive_day_symlink_substitution(tmp_path):
    store = DurableRecorderStore(tmp_path / "data")
    frontier = RecorderPublicationFrontier(store)
    batch, ref = _stored_ref(store)
    frontier.mark_pending(
        source_name="Mazak",
        archive_source_name="Mazak",
        instance_id=batch.header.instance_id,
        ref=ref,
    )

    archive_day = ref.manifest_path.parent
    moved_day = store.raw_root / "moved-day"
    archive_day.rename(moved_day)
    try:
        archive_day.symlink_to(moved_day, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks are unavailable on this runner: {exc}")

    with pytest.raises(MtconnectProtocolError, match="symlink or reparse point"):
        frontier.pending(source_name="Mazak", instance_id=batch.header.instance_id)


def test_frontier_preserves_requested_from_in_pointer_identity(tmp_path):
    store = DurableRecorderStore(tmp_path / "data")
    frontier = RecorderPublicationFrontier(store)
    batch, ref = _stored_ref(store)
    ref = replace(ref, requested_from=7)
    frontier.mark_pending(
        source_name="Mazak",
        archive_source_name="Mazak",
        instance_id=batch.header.instance_id,
        ref=ref,
    )

    pending = frontier.pending(source_name="Mazak", instance_id=77)

    assert pending[0].ref.requested_from == 7
