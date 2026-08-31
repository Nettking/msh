"""B03 consequence regressions for incremental recorder publication reconciliation."""
from __future__ import annotations

from pathlib import Path

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
from catalog.mtconnect_recorder.model import RawBatchRef
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
