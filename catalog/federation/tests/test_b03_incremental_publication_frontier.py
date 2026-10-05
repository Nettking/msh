"""B03 consequence regressions for incremental recorder publication reconciliation."""
from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

from catalog.federation.incremental_recorder_publication import (
    IncrementalRecorderArchiveReconciler,
)
from catalog.federation.tests.test_recorder_publication import (
    SAMPLE_XML,
    RecordingClient,
    _build_reconciler,
    _second_sample,
    _store_sample,
    _write_checkpoint,
)
from catalog.mtconnect_recorder.model import MtconnectProtocolError, RawBatchRef
from catalog.mtconnect_recorder.publication_frontier import RecorderPublicationFrontier
from catalog.mtconnect_recorder.schema_compat import RAW_BATCH_MANIFEST_SCHEMA


def _incremental_from(existing):
    return IncrementalRecorderArchiveReconciler(
        store=existing.store,
        checkpoint_file=existing.checkpoint_file,
        queue=existing.queue,
        target=existing.target,
        max_content_bytes=existing.max_content_bytes,
    )


def _mark_stored_pending(frontier, *, batch, stored, source_name="Mazak"):
    first = batch.first_observation_sequence
    last = batch.last_observation_sequence
    assert first is not None and last is not None
    frontier.mark_pending(
        source_name=source_name,
        archive_source_name=source_name,
        instance_id=batch.header.instance_id,
        ref=RawBatchRef(
            raw_path=stored.raw_path,
            manifest_path=stored.raw_path.with_suffix(".manifest.json"),
            raw_sha256=stored.raw_sha256,
            requested_from=int(first),
            first_sequence=int(first),
            last_sequence=int(last),
            next_sequence=int(batch.header.next_sequence),
            observation_count=stored.observation_count,
            manifest_schema=RAW_BATCH_MANIFEST_SCHEMA,
            received_at=(
                str(batch.observations[0].get("received_at"))
                if batch.observations and batch.observations[0].get("received_at")
                else None
            ),
            source_name=source_name,
        ),
    )


def test_legacy_relative_raw_path_migrates_without_rewriting_evidence(
    tmp_path, monkeypatch
):
    """A relative writer and absolute publisher name the same real capture."""
    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=client
    )
    reconciler = _incremental_from(legacy)
    probe, _batch, stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    manifest_path = stored.raw_path.with_suffix(".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["raw_file"] = stored.raw_path.relative_to(tmp_path).as_posix()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    original_manifest = manifest_path.read_bytes()
    original_raw = stored.raw_path.read_bytes()
    monkeypatch.chdir(tmp_path)

    result = reconciler.reconcile()

    assert result.enqueued == 1
    assert result.quarantine.total == 0
    assert reconciler.frontier.initialized(source_name="Mazak", instance_id=77)
    assert reconciler.frontier.pending(source_name="Mazak", instance_id=77) == ()
    entry, = outbox.pending()
    assert entry.payload["content"]["raw_sha256"] == "sha256:" + stored.raw_sha256
    assert entry.payload["content"]["agent_instance_id"] == 77
    assert entry.payload["content"]["first_sequence"] == 10
    assert entry.payload["content"]["last_sequence"] == 12
    assert manifest_path.read_bytes() == original_manifest
    assert stored.raw_path.read_bytes() == original_raw


def test_repaired_blocked_migration_retries_explicitly_then_returns_to_bounded_path(
    tmp_path, monkeypatch
):
    """Operator retry does not mark migration complete or trigger repeated scans."""
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=RecordingClient()
    )
    reconciler = _incremental_from(legacy)
    probe, _batch, stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    mark_pending = reconciler.frontier.mark_pending

    def prior_path_failure(**kwargs):
        raise MtconnectProtocolError("Publication frontier raw evidence is outside its durable root.")

    monkeypatch.setattr(reconciler.frontier, "mark_pending", prior_path_failure)
    failed = reconciler.reconcile()
    assert failed.enqueued == 0 and failed.quarantine.total == 1
    marker = reconciler.frontier._state_path("Mazak", 77)
    original = marker.read_bytes()
    manifest = stored.raw_path.with_suffix(".manifest.json").read_bytes()
    raw = stored.raw_path.read_bytes()
    monkeypatch.setattr(reconciler.frontier, "mark_pending", mark_pending)
    # A fixed binary alone must not silently discard a preserved refusal.
    assert reconciler.reconcile().enqueued == 0
    receipt = reconciler.frontier.retry_blocked_migration(
        source_name="Mazak", instance_id=77,
        expected_state_sha256=sha256(original).hexdigest(),
    )
    assert receipt.read_bytes() == original
    assert not reconciler.frontier.initialized(source_name="Mazak", instance_id=77)
    repaired = reconciler.reconcile()
    assert repaired.enqueued == 1 and repaired.quarantine.total == 0
    assert len(outbox.pending()) == 1
    assert reconciler.frontier.initialized(source_name="Mazak", instance_id=77)
    assert stored.raw_path.read_bytes() == raw
    assert stored.raw_path.with_suffix(".manifest.json").read_bytes() == manifest
    assert receipt.read_bytes() == original

    def refuse_rescan(**kwargs):
        raise AssertionError("operator retry introduced a repeated lifetime scan")

    monkeypatch.setattr(store, "scan_raw_batches", refuse_rescan)
    assert reconciler.reconcile().enqueued == 0


def test_new_recorder_evidence_does_not_require_rereading_prior_manifests(
    tmp_path, monkeypatch
):
    """After migration, ordinary publication work is proportional to backlog.

    The first pass starts from a legacy archive, pays the explicit one-time scan,
    durably enqueues the committed batch and retires its discovery pointer. A
    second committed batch is then exposed through the same bounded discovery
    seam the recorder transaction writes in production.

    The second reconciliation is forbidden both from calling the lifetime raw
    scanner and from reopening the first manifest. It must still enqueue the new
    batch and retire that one discovery record.
    """

    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=client
    )
    reconciler = _incremental_from(legacy)
    probe, _first_batch, first_stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)

    first = reconciler.reconcile()
    assert first.enqueued == 1
    assert len(outbox.pending()) == 1
    assert reconciler.frontier.pending(source_name="Mazak", instance_id=77) == ()
    assert reconciler.frontier.initialized(source_name="Mazak", instance_id=77)

    old_manifest = first_stored.raw_path.with_suffix(".manifest.json")
    original_read_text = Path.read_text

    def refuse_old_manifest(path: Path, *args, **kwargs):
        if path == old_manifest:
            raise AssertionError(
                "later reconciliation reread an already-reconciled raw manifest"
            )
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", refuse_old_manifest)

    def refuse_lifetime_scan(*args, **kwargs):
        raise AssertionError("later reconciliation traversed the lifetime raw archive")

    monkeypatch.setattr(store, "iter_raw_batches", refuse_lifetime_scan)

    _probe, second_batch, second_stored = _store_sample(store, _second_sample())
    _mark_stored_pending(
        RecorderPublicationFrontier(store),
        batch=second_batch,
        stored=second_stored,
    )
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=16)

    second = reconciler.reconcile()

    assert second.scanned_batches == 1
    assert second.enqueued == 1
    assert len(outbox.pending()) == 2
    assert reconciler.frontier.pending(source_name="Mazak", instance_id=77) == ()


def test_discovery_record_survives_until_checkpoint_commit(tmp_path):
    """A pre-checkpoint frontier record cannot make uncommitted raw publishable."""

    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=client
    )
    reconciler = _incremental_from(legacy)
    probe, batch, stored = _store_sample(store, SAMPLE_XML)
    _mark_stored_pending(reconciler.frontier, batch=batch, stored=stored)
    # Mark migration complete explicitly: this models a current installation,
    # where the writer and publication worker already share the frontier seam.
    reconciler.frontier.mark_initialized(source_name="Mazak", instance_id=77)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=10)

    before_commit = reconciler.reconcile()

    assert before_commit.scanned_batches == 1
    assert before_commit.eligible_batches == 0
    assert before_commit.enqueued == 0
    assert outbox.pending() == ()
    assert len(reconciler.frontier.pending(source_name="Mazak", instance_id=77)) == 1

    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    after_commit = reconciler.reconcile()

    assert after_commit.enqueued == 1
    assert len(outbox.pending()) == 1
    assert reconciler.frontier.pending(source_name="Mazak", instance_id=77) == ()


def test_durable_outbox_survives_failure_before_frontier_retirement(
    tmp_path, monkeypatch
):
    """A crash after enqueue but before pointer retirement replays idempotently."""

    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=client
    )
    reconciler = _incremental_from(legacy)
    probe, batch, stored = _store_sample(store, SAMPLE_XML)
    _mark_stored_pending(reconciler.frontier, batch=batch, stored=stored)
    reconciler.frontier.mark_initialized(source_name="Mazak", instance_id=77)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)

    original_retire = reconciler.frontier.retire

    def crash_before_retire(_pending):
        raise OSError("simulated crash after durable outbox enqueue")

    monkeypatch.setattr(reconciler.frontier, "retire", crash_before_retire)
    try:
        reconciler.reconcile()
    except OSError as exc:
        assert "simulated crash" in str(exc)
    else:
        raise AssertionError("simulated retirement crash did not escape")

    assert len(outbox.pending()) == 1
    assert len(reconciler.frontier.pending(source_name="Mazak", instance_id=77)) == 1

    monkeypatch.setattr(reconciler.frontier, "retire", original_retire)
    replay = reconciler.reconcile()

    assert replay.enqueued == 0
    assert replay.already_enqueued == 1
    assert len(outbox.pending()) == 1
    assert reconciler.frontier.pending(source_name="Mazak", instance_id=77) == ()


def test_legacy_scan_with_unrepresentable_item_becomes_explicitly_blocked(
    tmp_path, monkeypatch
):
    """A malformed legacy item cannot force a lifetime scan on every cycle."""

    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=client
    )
    reconciler = _incremental_from(legacy)
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)

    malformed = (
        store.raw_root
        / "Mazak"
        / "77"
        / "2026-08-09"
        / "unreadable.manifest.json"
    )
    malformed.write_text("{not-json", encoding="utf-8")

    first = reconciler.reconcile()

    assert first.enqueued == 1
    assert first.quarantine.total == 1
    assert reconciler.frontier.migration_state(
        source_name="Mazak", instance_id=77
    ) == "blocked"

    def refuse_lifetime_scan(*args, **kwargs):
        raise AssertionError("blocked migration was rescanned")

    monkeypatch.setattr(store, "scan_raw_batches", refuse_lifetime_scan)
    second = reconciler.reconcile()

    assert second.enqueued == 0
    assert second.quarantine.total == 1
    assert outbox.pending()


def test_legacy_alias_migration_keeps_archive_and_logical_identity_separate(
    tmp_path,
):
    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=client
    )
    reconciler = _incremental_from(legacy)
    probe, _batch, _stored = _store_sample(
        store,
        SAMPLE_XML,
        archive_source_name="Mazak Legacy",
    )
    _write_checkpoint(
        checkpoint_file,
        probe_sha256=probe.sha256,
        next_sequence=13,
        storage_aliases=["Mazak Legacy"],
    )

    result = reconciler.reconcile()

    assert result.enqueued == 1
    assert result.quarantine.total == 0
    assert len(outbox.pending()) == 1
    assert reconciler.frontier.pending(source_name="Mazak", instance_id=77) == ()
    assert reconciler.frontier.initialized(
        source_name="Mazak Legacy", instance_id=77
    )


def test_reconciler_quarantines_manifest_identity_tampering(tmp_path):
    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=client
    )
    reconciler = _incremental_from(legacy)
    probe, batch, stored = _store_sample(store, SAMPLE_XML)
    _mark_stored_pending(reconciler.frontier, batch=batch, stored=stored)
    reconciler.frontier.mark_initialized(source_name="Mazak", instance_id=77)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)

    manifest_path = stored.raw_path.with_suffix(".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source_name"] = "Other"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    result = reconciler.reconcile()

    assert result.enqueued == 0
    assert result.quarantine.total == 1
    assert outbox.pending() == ()
    assert reconciler.frontier.pending(source_name="Mazak", instance_id=77)


def test_reconciler_quarantines_observation_identity_tampering(tmp_path):
    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=client
    )
    reconciler = _incremental_from(legacy)
    probe, batch, stored = _store_sample(store, SAMPLE_XML)
    _mark_stored_pending(reconciler.frontier, batch=batch, stored=stored)
    reconciler.frontier.mark_initialized(source_name="Mazak", instance_id=77)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)

    observations = [
        json.loads(line)
        for line in stored.observation_path.read_text(encoding="utf-8").splitlines()
    ]
    observations[0]["agent_instance_id"] = 78
    stored.observation_path.write_text(
        "".join(json.dumps(record) + "\n" for record in observations),
        encoding="utf-8",
    )

    result = reconciler.reconcile()

    assert result.enqueued == 0
    assert result.quarantine.total == 1
    assert outbox.pending() == ()
    assert reconciler.frontier.pending(source_name="Mazak", instance_id=77)


def test_malformed_initialized_marker_fails_closed_without_rescanning(
    tmp_path, monkeypatch
):
    client = RecordingClient()
    store, checkpoint_file, _outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=client
    )
    reconciler = _incremental_from(legacy)
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    marker = reconciler.frontier.mark_initialized(
        source_name="Mazak", instance_id=77
    )
    marker.write_text(
        json.dumps(
            {
                "schema": "fcp.mtconnect.publication_frontier.v1",
                "state": "initialized",
                "source_name": "Other",
                "agent_instance_id": 77,
            }
        ),
        encoding="utf-8",
    )

    def refuse_lifetime_scan(*args, **kwargs):
        raise AssertionError("malformed marker triggered an archive scan")

    monkeypatch.setattr(store, "scan_raw_batches", refuse_lifetime_scan)
    with pytest.raises(MtconnectProtocolError, match="state|identity"):
        reconciler.reconcile()


def test_recreated_archive_root_invalidates_initialized_marker(tmp_path):
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=RecordingClient()
    )
    del checkpoint_file, outbox
    reconciler = _incremental_from(legacy)
    _probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    reconciler.frontier.mark_initialized(source_name="Mazak", instance_id=77)
    archive_root = store.raw_root / "Mazak" / "77"
    moved_root = tmp_path / "moved-archive-root"
    archive_root.rename(moved_root)
    archive_root.mkdir(parents=True)

    assert not reconciler.frontier.initialized(source_name="Mazak", instance_id=77)


def test_large_pending_backlog_is_drained_in_bounded_passes(tmp_path):
    """Pending backlog size does not turn one reconciliation into an unbounded pass."""

    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, legacy = _build_reconciler(
        tmp_path, client=client
    )
    reconciler = _incremental_from(legacy)
    probe = None
    for offset in range(64 + 7):
        first = 10 + offset * 3
        last = first + 2
        following = last + 1
        xml = SAMPLE_XML
        for old, new in (
            ('firstSequence="10"', f'firstSequence="{first}"'),
            ('lastSequence="12"', f'lastSequence="{last}"'),
            ('nextSequence="13"', f'nextSequence="{following}"'),
            ('sequence="10"', f'sequence="{first}"'),
            ('sequence="11"', f'sequence="{first + 1}"'),
            ('sequence="12"', f'sequence="{last}"'),
        ):
            xml = xml.replace(old, new)
        probe, batch, stored = _store_sample(store, xml)
        _mark_stored_pending(reconciler.frontier, batch=batch, stored=stored)
    assert probe is not None
    _write_checkpoint(
        checkpoint_file,
        probe_sha256=probe.sha256,
        next_sequence=10 + (64 + 7) * 3,
    )
    reconciler.frontier.mark_initialized(source_name="Mazak", instance_id=77)

    first = reconciler.reconcile()

    assert first.scanned_batches == 64
    assert len(reconciler.frontier.pending(source_name="Mazak", instance_id=77)) == 7
    assert len(outbox.pending()) == 64

    second = reconciler.reconcile()

    assert second.scanned_batches == 7
    assert reconciler.frontier.pending(source_name="Mazak", instance_id=77) == ()
