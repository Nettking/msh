from __future__ import annotations

import json
from dataclasses import replace
from hashlib import sha256
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


@pytest.mark.parametrize(
    ("relative_root", "relative_evidence"),
    [(False, True), (True, False), (False, False), (True, True)],
)
def test_frontier_accepts_equivalent_relative_and_absolute_paths(
    tmp_path, monkeypatch, relative_root, relative_evidence
):
    monkeypatch.chdir(tmp_path)
    absolute_store = DurableRecorderStore(tmp_path / "data")
    batch, original = _stored_ref(absolute_store)
    evidence = replace(
        original,
        raw_path=(original.raw_path.relative_to(tmp_path)
                  if relative_evidence else original.raw_path),
        manifest_path=(original.manifest_path.relative_to(tmp_path)
                       if relative_evidence else original.manifest_path),
    )
    store = DurableRecorderStore(Path("data") if relative_root else tmp_path / "data")
    frontier = RecorderPublicationFrontier(store)
    manifest_before = original.manifest_path.read_bytes()
    raw_before = original.raw_path.read_bytes()

    frontier.mark_pending(
        source_name="Mazak", archive_source_name="Mazak",
        instance_id=batch.header.instance_id, ref=evidence,
    )

    pending = frontier.pending(source_name="Mazak", instance_id=batch.header.instance_id)
    assert len(pending) == 1
    assert pending[0].ref.raw_sha256 == original.raw_sha256
    assert pending[0].ref.raw_path.resolve() == original.raw_path
    assert pending[0].ref.manifest_path.resolve() == original.manifest_path
    assert original.manifest_path.read_bytes() == manifest_before
    assert original.raw_path.read_bytes() == raw_before


@pytest.mark.parametrize("failure", ["outside", "dotdot", "missing", "reparse", "root_reparse"])
def test_relative_frontier_evidence_still_rejects_unsafe_paths(
    tmp_path, monkeypatch, failure
):
    monkeypatch.chdir(tmp_path)
    store = DurableRecorderStore(tmp_path / "data")
    batch, original = _stored_ref(store)
    relative = original.raw_path.relative_to(tmp_path)
    if failure in {"outside", "dotdot"}:
        outside = tmp_path / "outside" / original.raw_path.parent.name
        outside.mkdir(parents=True)
        moved = outside / original.raw_path.name
        moved.write_bytes(original.raw_path.read_bytes())
        relative = (moved.relative_to(tmp_path) if failure == "outside" else
                    (store.root / ".." / ".." / ".." / "outside" /
                     original.raw_path.parent.name / original.raw_path.name).relative_to(tmp_path))
    elif failure == "missing":
        original.raw_path.unlink()
    else:
        # Model the existing cross-platform link/junction predicate without
        # requiring Windows symlink privileges for this representation case.
        from catalog.mtconnect_recorder import publication_frontier
        real_reparse = publication_frontier._is_reparse_point

        def reparse(path):
            target = store.root if failure == "root_reparse" else original.raw_path.parent
            return path == target or real_reparse(path)

        monkeypatch.setattr(publication_frontier, "_is_reparse_point", reparse)
    ref = replace(original, raw_path=relative)
    frontier = RecorderPublicationFrontier(store)
    with pytest.raises(MtconnectProtocolError, match="root|identity|regular file|reparse"):
        frontier.mark_pending(
            source_name="Mazak", archive_source_name="Mazak",
            instance_id=batch.header.instance_id, ref=ref,
        )
    assert not frontier.pending_root.exists()


def _blocked_frontier(tmp_path):
    store = DurableRecorderStore(tmp_path / "data")
    frontier = RecorderPublicationFrontier(store)
    _stored_ref(store)
    marker = frontier.mark_blocked(
        source_name="Mazak", instance_id=77, issue_count=1,
        issue_sample="2026-08-09/retained.manifest.json",
    )
    original = marker.read_bytes()
    return frontier, marker, original, sha256(original).hexdigest()


def test_explicit_blocked_retry_preserves_refusal_and_supports_replay(tmp_path):
    frontier, marker, original, digest = _blocked_frontier(tmp_path)
    receipt = frontier.retry_blocked_migration(
        source_name="Mazak", instance_id=77, expected_state_sha256=digest,
    )
    assert receipt.read_bytes() == original
    assert not marker.exists()
    assert frontier.migration_state(source_name="Mazak", instance_id=77) == "missing"
    assert frontier.retry_blocked_migration(
        source_name="Mazak", instance_id=77, expected_state_sha256=digest,
    ) == receipt
    assert receipt.read_bytes() == original


@pytest.mark.parametrize("failure", ["hash", "malformed", "initialized", "collision", "root"])
def test_explicit_blocked_retry_refuses_invalid_inputs_without_retiring_state(tmp_path, failure):
    frontier, marker, original, digest = _blocked_frontier(tmp_path)
    receipt = marker.with_name(f".blocked-{digest}.json")
    if failure == "hash":
        digest = "0" * 64
    elif failure == "malformed":
        payload = json.loads(original)
        payload["agent_instance_id"] = True
        marker.write_text(json.dumps(payload), encoding="utf-8")
        digest = sha256(marker.read_bytes()).hexdigest()
    elif failure == "initialized":
        frontier.mark_initialized(source_name="Mazak", instance_id=77)
        digest = sha256(marker.read_bytes()).hexdigest()
    elif failure == "collision":
        receipt.write_bytes(b"incomplete prior receipt")
    else:
        payload = json.loads(original)
        payload["archive_root_identity"] = "another-inode"
        marker.write_text(json.dumps(payload), encoding="utf-8")
        digest = sha256(marker.read_bytes()).hexdigest()
    before = marker.read_bytes()
    with pytest.raises(MtconnectProtocolError):
        frontier.retry_blocked_migration(
            source_name="Mazak", instance_id=77, expected_state_sha256=digest,
        )
    assert marker.read_bytes() == before
    if failure == "collision":
        assert receipt.read_bytes() == b"incomplete prior receipt"


def test_blocked_retry_resumes_after_interrupted_marker_removal(tmp_path, monkeypatch):
    frontier, marker, original, digest = _blocked_frontier(tmp_path)
    unlink = Path.unlink

    def interrupted(path, *args, **kwargs):
        if path == marker:
            raise OSError("fixture controller loss before marker removal")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", interrupted)
    with pytest.raises(MtconnectProtocolError, match="retry is unavailable"):
        frontier.retry_blocked_migration(
            source_name="Mazak", instance_id=77, expected_state_sha256=digest,
        )
    receipt = marker.with_name(f".blocked-{digest}.json")
    assert marker.read_bytes() == receipt.read_bytes() == original
    monkeypatch.setattr(Path, "unlink", unlink)
    assert frontier.retry_blocked_migration(
        source_name="Mazak", instance_id=77, expected_state_sha256=digest,
    ) == receipt
    frontier.mark_initialized(source_name="Mazak", instance_id=77)
    initialized = marker.read_bytes()
    with pytest.raises(MtconnectProtocolError, match="blocked migration"):
        frontier.retry_blocked_migration(
            source_name="Mazak", instance_id=77, expected_state_sha256=digest,
        )
    assert marker.read_bytes() == initialized
    assert receipt.read_bytes() == original


def test_blocked_retry_refuses_a_state_change_before_removal(tmp_path, monkeypatch):
    frontier, marker, original, digest = _blocked_frontier(tmp_path)
    read_bytes = Path.read_bytes
    calls = 0
    changed = original.replace(b'"issue_count": 1', b'"issue_count": 2')

    def replaced(path):
        nonlocal calls
        if path == marker:
            calls += 1
            if calls == 2:
                marker.write_bytes(changed)
        return read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", replaced)
    with pytest.raises(MtconnectProtocolError, match="hash mismatch"):
        frontier.retry_blocked_migration(
            source_name="Mazak", instance_id=77, expected_state_sha256=digest,
        )
    assert marker.read_bytes() == changed
    assert marker.with_name(f".blocked-{digest}.json").read_bytes() == original


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
