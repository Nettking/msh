from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from catalog.federation.outbox import OutboxState, SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import (
    RECORDER_STORAGE_SCHEMA,
    DurableRecorderDeliveryQueue,
)
from catalog.federation.recorder_publication import (
    RECORDER_DATASET_SCHEMA_NAME,
    RecorderArchiveReconciler,
    RecorderFederationDeliveryWorker,
    RecorderFederationPublisher,
    RecorderPublicationTarget,
)
from catalog.mtconnect_recorder import (
    DurableRecorderStore,
    SourceCheckpoint,
    parse_probe,
    parse_streams,
)

PROBE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.3">
  <Header creationTime="2026-08-09T03:00:00Z" sender="agent" instanceId="77" bufferSize="4096"/>
  <Devices>
    <Device id="d1" name="Mazak" uuid="MAZAK-001">
      <Description serialNumber="SERIAL-1">Mazak test machine</Description>
      <Components>
        <Axes id="axes" name="base">
          <Components>
            <Linear id="x" name="X">
              <DataItems>
                <DataItem category="SAMPLE" id="xp" name="Xabs" type="POSITION" units="MILLIMETER"/>
                <DataItem category="CONDITION" id="xt" name="Xtravel" type="POSITION"/>
              </DataItems>
            </Linear>
          </Components>
        </Axes>
      </Components>
    </Device>
  </Devices>
</MTConnectDevices>
"""


SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7">
  <Header creationTime="2026-08-09T03:00:03Z" sender="agent" instanceId="77" bufferSize="4096" firstSequence="10" lastSequence="12" nextSequence="13"/>
  <Streams>
    <DeviceStream name="Mazak" uuid="MAZAK-001">
      <ComponentStream component="Linear" componentId="x" name="X">
        <Samples>
          <Position dataItemId="xp" sequence="10" timestamp="2026-08-09T03:00:01Z">1.25</Position>
          <Position dataItemId="xp" sequence="11" timestamp="2026-08-09T03:00:02Z">2.50</Position>
        </Samples>
        <Condition>
          <Fault dataItemId="xt" sequence="12" timestamp="2026-08-09T03:00:03Z" nativeCode="X42">Travel exceeded</Fault>
        </Condition>
      </ComponentStream>
    </DeviceStream>
  </Streams>
</MTConnectStreams>
"""


def _second_sample() -> str:
    value = SAMPLE_XML
    replacements = (
        ('firstSequence="10"', 'firstSequence="13"'),
        ('lastSequence="12"', 'lastSequence="15"'),
        ('nextSequence="13"', 'nextSequence="16"'),
        ('sequence="10"', 'sequence="13"'),
        ('sequence="11"', 'sequence="14"'),
        ('sequence="12"', 'sequence="15"'),
    )
    for old, new in replacements:
        value = value.replace(old, new)
    return value


class RecordingClient:
    def __init__(self, *, committed: bool = True, fail: bool = False) -> None:
        self.committed = committed
        self.fail = fail
        self.calls: list[dict[str, object]] = []

    async def ingest_batch(self, **kwargs):
        self.calls.append(dict(kwargs))
        if self.fail:
            raise OSError("Federation unavailable")
        return PhaseDIngestOutcome(
            committed=self.committed,
            message=None if self.committed else "acknowledgement pending",
        )


def _write_checkpoint(
    path: Path,
    *,
    probe_sha256: str,
    next_sequence: int,
    storage_aliases: list[str] | None = None,
) -> None:
    checkpoint = SourceCheckpoint(
        source_name="Mazak",
        base_url="http://agent:5000",
        machine_id="MAZAK-001",
        agent_instance_id=77,
        next_sequence=next_sequence,
        probe_sha256=probe_sha256,
        storage_aliases=list(storage_aliases or []),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema": "fcp.mtconnect_recorder.checkpoints.v3",
                "updated_at": "2026-08-09T03:00:04Z",
                "sources": {"Mazak": checkpoint.to_dict()},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _build_reconciler(
    tmp_path: Path,
    *,
    client: RecordingClient,
    max_content_bytes: int = 900_000,
):
    data_dir = tmp_path / "data"
    checkpoint_file = data_dir / "source_state" / "mtconnect_recorder_state.json"
    store = DurableRecorderStore(data_dir)
    outbox = SQLiteOutbox(tmp_path / "publisher" / "outbox.sqlite3")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=client,
        session_id="session-1",
    )
    target = RecorderPublicationTarget(
        session_id="session-1",
        group_id="telemetry-storage",
        recorder_node_id="node-recorder-1",
    )
    reconciler = RecorderArchiveReconciler(
        store=store,
        checkpoint_file=checkpoint_file,
        queue=queue,
        target=target,
        max_content_bytes=max_content_bytes,
    )
    return store, checkpoint_file, outbox, queue, reconciler


def _store_sample(
    store: DurableRecorderStore,
    xml_text: str,
    *,
    archive_source_name: str = "Mazak",
):
    probe = parse_probe(PROBE_XML)
    batch = parse_streams(xml_text, source_name="Mazak", probe=probe)
    stored = store.store_batch(
        source_name=archive_source_name,
        requested_from=int(batch.first_observation_sequence or 0),
        xml_text=xml_text,
        batch=batch,
    )
    return probe, batch, stored


def test_reconcile_publishes_detailed_observations_and_keeps_local_jsonl(tmp_path):
    client = RecordingClient()
    store, checkpoint_file, outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)

    result = reconciler.reconcile()

    assert result.scanned_batches == 1
    assert result.eligible_batches == 1
    assert result.publication_chunks == 1
    assert result.enqueued == 1
    assert stored.normalized_path.exists()
    assert stored.observation_path.exists()

    pending = outbox.pending()
    assert len(pending) == 1
    payload = pending[0].payload
    assert payload["dataset_schema_name"] == RECORDER_DATASET_SCHEMA_NAME
    assert payload["dataset_schema_version"] == 1
    content = payload["content"]
    assert content["schema"] == "fcp.mtconnect.observations.v1"
    assert [row["sequence"] for row in content["observations"]] == [10, 11, 12]
    assert content["observations"][0]["data_item_id"] == "xp"
    assert "Xabs" not in content["observations"][0]

    again = reconciler.reconcile()
    assert again.enqueued == 0
    assert again.already_enqueued == 1
    assert len(outbox.pending()) == 1

    delivery = asyncio.run(queue.run_once())
    assert delivery.committed == 1
    assert len(client.calls) == 1
    assert client.calls[0]["dataset_schema_name"] == RECORDER_DATASET_SCHEMA_NAME
    assert outbox.pending() == ()


def test_reconcile_orders_and_deduplicates_batches_across_storage_aliases(
    tmp_path,
):
    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, reconciler = _build_reconciler(
        tmp_path,
        client=client,
    )
    probe, _current_batch, _current_stored = _store_sample(
        store,
        _second_sample(),
    )
    _store_sample(
        store,
        SAMPLE_XML,
        archive_source_name="Mazak Legacy",
    )
    _store_sample(
        store,
        SAMPLE_XML,
        archive_source_name="Mazak Backup",
    )
    _write_checkpoint(
        checkpoint_file,
        probe_sha256=probe.sha256,
        next_sequence=16,
        storage_aliases=["Mazak Legacy", "Mazak Backup"],
    )

    result = reconciler.reconcile()

    assert result.scanned_batches == 2
    assert result.eligible_batches == 2
    assert result.publication_chunks == 2
    assert result.enqueued == 2
    assert [
        entry.payload["content"]["first_sequence"] for entry in outbox.pending()
    ] == [10, 13]


def test_uncommitted_raw_archive_is_not_publishable(tmp_path):
    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=10)

    result = reconciler.reconcile()

    assert result.scanned_batches == 1
    assert result.eligible_batches == 0
    assert result.enqueued == 0
    assert outbox.pending() == ()


def test_federation_failure_leaves_local_capture_and_outbox_intact(tmp_path):
    client = RecordingClient(fail=True)
    store, checkpoint_file, outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    checkpoint_before = checkpoint_file.read_bytes()

    publisher = RecorderFederationPublisher(reconciler=reconciler, queue=queue)
    result = asyncio.run(publisher.run_once())

    assert result.reconcile.enqueued == 1
    assert result.delivery.pending == 1
    assert len(outbox.pending()) == 1
    assert stored.raw_path.exists()
    assert stored.observation_path.exists()
    assert stored.normalized_path.exists()
    assert checkpoint_file.read_bytes() == checkpoint_before


def test_delivery_failure_fences_newer_batches_for_same_dataset(tmp_path):
    now = [datetime(2026, 8, 9, 3, 0, tzinfo=timezone.utc)]
    client = RecordingClient(fail=True)
    outbox = SQLiteOutbox(tmp_path / "ordered-outbox.sqlite3")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=client,
        session_id="session-1",
        clock=lambda: now[0],
    )
    for sequence in (10, 11):
        queue.enqueue(
            session_id="session-1",
            group_id="telemetry-storage",
            dataset_id="mtconnect:node-1:Mazak",
            batch_id=f"Mazak:77:{sequence}:{sequence}:hash",
            idempotency_key=f"session-1:dataset:batch-{sequence}",
            content={"sequence": sequence},
            created_at=now[0],
        )

    first = asyncio.run(queue.run_once())
    before_retry = asyncio.run(queue.run_once())

    assert first.attempted == 1
    assert first.pending == 1
    assert before_retry.attempted == 0
    assert [call["batch_id"] for call in client.calls] == [
        "Mazak:77:10:10:hash"
    ]

    now[0] += timedelta(seconds=1)
    client.fail = False
    recovered = asyncio.run(queue.run_once())

    assert recovered.attempted == 2
    assert recovered.committed == 2
    assert [call["batch_id"] for call in client.calls] == [
        "Mazak:77:10:10:hash",
        "Mazak:77:10:10:hash",
        "Mazak:77:11:11:hash",
    ]
    assert outbox.pending() == ()


def test_delivery_skips_rows_from_a_different_federation_session(tmp_path):
    now = datetime(2026, 8, 9, 3, 0, tzinfo=timezone.utc)
    client = RecordingClient()
    outbox = SQLiteOutbox(tmp_path / "session-bound-outbox.sqlite3")
    old_session_queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=client,
        session_id="session-old",
        clock=lambda: now,
    )
    current_session_queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=client,
        session_id="session-current",
        clock=lambda: now,
    )
    old_session_queue.enqueue(
        session_id="session-old",
        group_id="telemetry-storage",
        dataset_id="mtconnect:node-1:Mazak",
        batch_id="Mazak:77:10:10:old",
        idempotency_key="session-old:dataset:batch-10",
        content={"sequence": 10},
        created_at=now,
    )
    current_session_queue.enqueue(
        session_id="session-current",
        group_id="telemetry-storage",
        dataset_id="mtconnect:node-1:Mazak",
        batch_id="Mazak:77:11:11:current",
        idempotency_key="session-current:dataset:batch-11",
        content={"sequence": 11},
        created_at=now,
    )

    result = asyncio.run(current_session_queue.run_once())

    assert result.attempted == 1
    assert result.committed == 1
    assert [call["batch_id"] for call in client.calls] == [
        "Mazak:77:11:11:current"
    ]
    remaining = outbox.pending()
    assert len(remaining) == 1
    assert remaining[0].session_id == "session-old"


def test_delivery_skips_rows_for_a_previous_storage_group(tmp_path):
    now = datetime(2026, 8, 9, 3, 0, tzinfo=timezone.utc)
    client = RecordingClient()
    outbox = SQLiteOutbox(tmp_path / "group-bound-outbox.sqlite3")
    old_group_queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=client,
        session_id="session-current",
        destination_id="storage-old",
        clock=lambda: now,
    )
    current_group_queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=client,
        session_id="session-current",
        destination_id="storage-current",
        clock=lambda: now,
    )
    old_group_queue.enqueue(
        session_id="session-current",
        group_id="storage-old",
        dataset_id="mtconnect:node-1:Mazak",
        batch_id="Mazak:77:10:10:old-group",
        idempotency_key="session-current:old-group:batch-10",
        content={"sequence": 10},
        created_at=now,
    )
    current_group_queue.enqueue(
        session_id="session-current",
        group_id="storage-current",
        dataset_id="mtconnect:node-1:Mazak",
        batch_id="Mazak:77:11:11:current-group",
        idempotency_key="session-current:current-group:batch-11",
        content={"sequence": 11},
        created_at=now,
    )

    result = asyncio.run(current_group_queue.run_once())

    assert result.attempted == 1
    assert result.committed == 1
    assert [call["batch_id"] for call in client.calls] == [
        "Mazak:77:11:11:current-group"
    ]
    remaining = outbox.pending()
    assert len(remaining) == 1
    assert remaining[0].destination_id == "storage-old"


def test_large_committed_batch_is_split_at_observation_boundaries(tmp_path):
    client = RecordingClient()
    store, checkpoint_file, outbox, _queue, reconciler = _build_reconciler(
        tmp_path,
        client=client,
        max_content_bytes=2_000,
    )
    source = "Mazak"
    instance = 77
    day = "2026-08-09"
    raw_digest = "a" * 64
    raw_path = (
        store.raw_root
        / source
        / str(instance)
        / day
        / f"seq-1-30-next-31-{raw_digest[:12]}.xml.gz"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"placeholder")
    manifest_path = raw_path.with_suffix(".manifest.json")
    manifest_path.write_text(
        json.dumps(
            {
                "schema": "fcp.mtconnect.raw_batch_manifest.v1",
                "source_name": source,
                "agent_instance_id": instance,
                "requested_from": 1,
                "first_observation_sequence": 1,
                "last_observation_sequence": 30,
                "next_sequence": 31,
                "agent_first_sequence": 1,
                "agent_last_sequence": 30,
                "observation_count": 30,
                "received_at": "2026-08-09T03:00:00Z",
                "raw_sha256": raw_digest,
                "raw_file": str(raw_path),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    observation_path = (
        store.observation_root
        / source
        / str(instance)
        / day
        / "seq-1-30-next-31.ndjson"
    )
    observation_path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "sequence": sequence,
            "received_at": "2026-08-09T03:00:00Z",
            "timestamp": "2026-08-09T03:00:00Z",
            "machine_id": "MAZAK-001",
            "data_item_id": "payload",
            "value": "x" * 450,
        }
        for sequence in range(1, 31)
    ]
    observation_path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )
    _write_checkpoint(
        checkpoint_file,
        probe_sha256="b" * 64,
        next_sequence=31,
    )

    result = reconciler.reconcile()

    assert result.publication_chunks > 1
    assert result.enqueued == result.publication_chunks
    entries = outbox.pending()
    assert len(entries) == result.publication_chunks
    covered: list[int] = []
    for entry in entries:
        content = entry.payload["content"]
        encoded = json.dumps(
            content,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        assert len(encoded) <= 2_000
        covered.extend(row["sequence"] for row in content["observations"])
    assert covered == list(range(1, 31))


def test_worker_reconciles_immediately_when_recorder_checkpoint_changes(tmp_path):
    client = RecordingClient()
    store, checkpoint_file, _outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    worker = RecorderFederationDeliveryWorker(reconciler=reconciler, queue=queue)

    first = asyncio.run(worker.run_cycle())
    second = asyncio.run(worker.run_cycle())

    assert first.checkpoint_changed is True
    assert first.reconcile is not None and first.reconcile.enqueued == 1
    assert first.delivery.committed == 1
    assert second.checkpoint_changed is False
    assert second.reconcile is None

    _probe, second_batch, _stored = _store_sample(store, _second_sample())
    assert second_batch.first_observation_sequence == 13
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=16)

    third = asyncio.run(worker.run_cycle())

    assert third.checkpoint_changed is True
    assert third.reconcile is not None
    assert third.reconcile.enqueued == 1
    assert third.reconcile.already_enqueued == 1
    assert third.delivery.committed == 1
    assert len(client.calls) == 2

# --------------------------------------------------------------------------
# B03: a stuck historical item must not stop newer evidence making progress
# --------------------------------------------------------------------------


class _PoisonClient:
    """The first batch ever offered is permanently unprocessable.

    This models the B03 case directly: a historical item that is malformed,
    oversized or otherwise refused by the authority every single time. It has
    no terminal state to reach, so it stays pending for the life of the
    recorder.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.poison_dataset: str | None = None

    async def ingest_batch(self, **kwargs):
        self.calls.append(dict(kwargs))
        if self.poison_dataset is None:
            self.poison_dataset = str(kwargs.get("dataset_id"))
        if str(kwargs.get("dataset_id")) == self.poison_dataset:
            raise OSError("permanently unprocessable historical item")
        return PhaseDIngestOutcome(committed=True, message=None)


def test_a_stuck_item_does_not_starve_reconciliation_of_newer_evidence(tmp_path):
    """The regression this delivery closes.

    ``run_cycle`` deferred reconciliation whenever the outbox held *any*
    pending row for this queue, asking for them without a due filter. A
    permanently failing row is pending forever, so reconciliation was
    suppressed on every later cycle and newly committed recorder evidence
    never became a durable outbox row at all -- the recorder kept capturing,
    the checkpoint kept advancing, and none of it was ever queued for
    delivery.

    Against the previous implementation this test hangs at one pending row
    with ``reconcile`` ``None`` on every cycle after the first.
    """

    client = _PoisonClient()
    store, checkpoint_file, outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    worker = RecorderFederationDeliveryWorker(reconciler=reconciler, queue=queue)

    first = asyncio.run(worker.run_cycle())
    assert first.reconcile is not None and first.reconcile.enqueued == 1
    assert first.delivery.committed == 0
    stuck = outbox.pending()
    assert len(stuck) == 1
    stuck_id = stuck[0].outbox_id

    # It keeps failing, and its durable backoff pushes it past due.
    for _ in range(2):
        asyncio.run(worker.run_cycle())

    # Newer evidence is committed locally and the checkpoint advances.
    _probe, second_batch, _stored = _store_sample(store, _second_sample())
    assert second_batch.first_observation_sequence == 13
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=16)

    reconciled = None
    for _ in range(5):
        result = asyncio.run(worker.run_cycle())
        if result.reconcile is not None and result.reconcile.enqueued:
            reconciled = result
            break

    assert reconciled is not None, (
        "a stuck item suppressed reconciliation of newer recorder evidence"
    )
    assert reconciled.reconcile.enqueued == 1
    # The newer evidence is durable now, which is what the stuck item used to
    # prevent entirely.
    assert len(outbox.pending()) == 2

    # And the stuck item is still exactly where it was: never deleted, never
    # completed, still carrying its failure for replay.
    still = outbox.get(stuck_id)
    assert still is not None
    assert still.state is OutboxState.PENDING
    assert still.attempt_count >= 1
    assert still.last_error


def test_due_backlog_heads_do_not_starve_reconciliation_after_startup_probe(tmp_path):
    """A multi-row due backlog must not keep new raw evidence out of the outbox.

    A single poison row moves into retry backoff and leaves a moment with no due
    work. A recovered recorder can have many old rows due at once, however. If
    one failed head fences the rest of its dataset, the unattempted older
    timestamps remain due indefinitely and the reconciliation gate never sees
    an empty due set.
    """

    client = _PoisonClient()
    store, checkpoint_file, outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    worker = RecorderFederationDeliveryWorker(
        reconciler=reconciler,
        queue=queue,
        delivery_limit=1,
    )
    now = datetime.now(UTC)
    dataset_id = "mtconnect:node-recorder-1:Mazak"
    for sequence in range(3):
        queue.enqueue(
            session_id="session-1",
            group_id="telemetry-storage",
            dataset_id=dataset_id,
            batch_id=f"Mazak:77:{sequence}:{sequence}:backlog-{sequence}",
            idempotency_key=f"session-1:{dataset_id}:backlog-{sequence}",
            content={"sequence": sequence},
            created_at=now,
        )

    probe = parse_probe(PROBE_XML)
    _write_checkpoint(
        checkpoint_file,
        probe_sha256=probe.sha256,
        next_sequence=10,
    )
    startup = asyncio.run(worker.run_cycle())
    assert startup.reconcile is None
    assert startup.delivery.attempted == 1
    assert len(outbox.pending(now=queue.clock())) == 2

    _probe, batch, _stored = _store_sample(store, SAMPLE_XML)
    assert batch.first_observation_sequence == 10
    _write_checkpoint(
        checkpoint_file,
        probe_sha256=probe.sha256,
        next_sequence=13,
    )

    resumed = asyncio.run(worker.run_cycle())

    assert resumed.reconcile is not None, (
        "unattempted due backlog rows fenced behind a failed head suppressed "
        "reconciliation of newer capture"
    )
    assert resumed.reconcile.enqueued == 1
    assert len(outbox.pending()) == 4


def test_a_stuck_dataset_does_not_fence_an_unrelated_dataset(tmp_path):
    """Other datasets keep committing, and the fenced one is surfaced."""

    client = _PoisonClient()
    outbox = SQLiteOutbox(tmp_path / "publisher" / "outbox.sqlite3")
    outbox.initialize()
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox, client=client, session_id="session-1"
    )
    now = datetime(2026, 8, 9, 3, 0, tzinfo=UTC)

    def _enqueue(dataset_id: str, batch_id: str) -> int:
        payload = {
            "group_id": "telemetry-storage",
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "idempotency_key": batch_id,
            "content": {"observations": []},
            "created_at": "2026-08-09T03:00:00Z",
        }
        entry, _created = outbox.enqueue(
            session_id="session-1",
            destination_id="fcp-local-storage",
            schema_id=RECORDER_STORAGE_SCHEMA,
            payload=payload,
            idempotency_key=batch_id,
            content_hash="c" * 64,
            now=now,
        )
        return entry.outbox_id

    poison_id = _enqueue("mtconnect:node-recorder-1:Mazak", "mazak-1")
    healthy_id = _enqueue("mtconnect:node-recorder-1:Okuma", "okuma-1")

    result = asyncio.run(queue.run_once())

    assert client.poison_dataset == "mtconnect:node-recorder-1:Mazak"
    # The healthy dataset commits in the very same cycle as the stuck one.
    assert result.committed == 1
    assert outbox.get(healthy_id).state is OutboxState.COMPLETED
    # The stuck one stays durable and retryable, and is named as fenced.
    assert outbox.get(poison_id).state is OutboxState.PENDING
    assert result.blocked_datasets == ("mtconnect:node-recorder-1:Mazak",)


def test_a_storage_failure_never_looks_like_a_successful_publication(tmp_path):
    """A refused item is retryable, not silently completed or dropped."""

    client = _PoisonClient()
    store, checkpoint_file, outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    worker = RecorderFederationDeliveryWorker(reconciler=reconciler, queue=queue)

    asyncio.run(worker.run_cycle())
    entry = outbox.pending()[0]
    first_attempts = entry.attempt_count

    # Bounded work per cycle: the stuck row is attempted at most once per due
    # window, and never disappears.
    for _ in range(3):
        asyncio.run(worker.run_cycle())

    after = outbox.get(entry.outbox_id)
    assert after is not None
    assert after.state is OutboxState.PENDING
    assert after.attempt_count >= first_attempts
    assert after.payload == entry.payload
    # Duplicate suppression still keys off the same durable identity, so a
    # replay of the same evidence is still recognised rather than re-sent.
    assert after.idempotency_key == entry.idempotency_key
    assert after.content_hash == entry.content_hash

def test_restart_probes_deferred_backlog_before_full_archive_reconcile(tmp_path):
    """A deferred backlog still gets its route probe first on a fresh runtime.

    Two invariants meet here and pull in opposite directions:

    * a permanently failing row must never suppress reconciliation forever,
      which is why the backlog gate asks only for rows that are *due*; and
    * on a process restart an existing backlog must get its bounded
      one-head-per-dataset route probe before the expensive archive scan,
      which is why ``run_once`` retries one deferred head per dataset on a
      fresh queue.

    Asking only for due rows made a restart with a wholly deferred backlog
    look like no backlog at all, so the archive reconcile ran first and the
    startup probe -- the thing that proves the route and lets the recorder
    report a live heartbeat quickly -- was pushed behind it.
    """

    order: list[str] = []

    class _OrderedClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def ingest_batch(self, **kwargs):
            self.calls.append(dict(kwargs))
            order.append("deliver")
            raise OSError("Federation still unavailable")

    client = _OrderedClient()
    # The queue this helper builds is discarded: the restart below needs a
    # fresh one over the same durable outbox.
    store, checkpoint_file, outbox, _queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )

    # A durable row that is already deferred: enqueued, attempted, and pushed
    # into exponential backoff so its next attempt is in the future.
    # Anchored to the queue's real clock, so the backoff genuinely lands in
    # the future rather than in a fixture's past.
    started = datetime.now(UTC)
    entry, _created = outbox.enqueue(
        session_id="session-1",
        destination_id="fcp-local-storage",
        schema_id=RECORDER_STORAGE_SCHEMA,
        payload={
            "group_id": "telemetry-storage",
            "dataset_id": "mtconnect:node-recorder-1:Mazak",
            "batch_id": "mazak-backlog-1",
            "idempotency_key": "mazak-backlog-1",
            "content": {"observations": []},
            "created_at": "2026-08-09T03:00:00Z",
        },
        idempotency_key="mazak-backlog-1",
        content_hash="d" * 64,
        now=started,
    )
    for _ in range(6):
        outbox.record_failure(
            entry.outbox_id, error="Federation unavailable", now=started
        )
    deferred = outbox.get(entry.outbox_id)
    assert deferred.next_attempt_at > datetime.now(UTC), (
        "the fixture must model a backlog that is not yet due"
    )
    assert outbox.pending(now=datetime.now(UTC)) == ()

    # New local evidence and an advanced checkpoint, exactly as a recorder
    # that kept capturing through the outage would leave them.
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)

    # A fresh queue and worker over the same durable outbox: a process restart.
    restarted_queue = DurableRecorderDeliveryQueue(
        outbox=outbox, client=client, session_id="session-1"
    )
    original_reconcile = reconciler.reconcile

    def _tracked_reconcile():
        order.append("reconcile")
        return original_reconcile()

    reconciler.reconcile = _tracked_reconcile
    worker = RecorderFederationDeliveryWorker(
        reconciler=reconciler, queue=restarted_queue
    )

    asyncio.run(worker.run_cycle())

    assert order, "the first cycle did neither delivery nor reconciliation"
    assert order[0] == "deliver", (
        "the startup route probe must run before the archive reconcile; "
        f"observed order {order}"
    )
    # The probe is bounded: one head for the one deferred dataset.
    assert len(client.calls) == 1
    # And it changed nothing durable beyond the ordinary failure record.
    still = outbox.get(entry.outbox_id)
    assert still.state is OutboxState.PENDING
    assert still.payload == deferred.payload
    assert still.idempotency_key == deferred.idempotency_key


def test_failed_reconciliation_retains_the_publication_cycle_stage(tmp_path):
    _store, _checkpoint_file, _outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=RecordingClient()
    )

    def _fail_reconcile():
        raise OSError("test-only reconciliation failure")

    reconciler.reconcile = _fail_reconcile
    worker = RecorderFederationDeliveryWorker(
        reconciler=reconciler,
        queue=queue,
    )

    try:
        asyncio.run(worker.run_cycle(force_reconcile=True))
    except OSError as exc:
        assert str(exc) == "test-only reconciliation failure"
    else:
        raise AssertionError("the test reconciler should fail")

    assert worker.last_cycle_stage == "reconcile"


# --------------------------------------------------------------------------
# B03: durable retirement, and the degraded health it must make visible
# --------------------------------------------------------------------------


def _enqueue_row(
    outbox: SQLiteOutbox,
    *,
    dataset_id: str,
    batch_id: str,
    poison: bool = False,
) -> int:
    """Enqueue one delivery row, optionally one that can never be sent.

    The defect is a missing ``created_at``: the delivery path cannot build a
    request from it at all, on this attempt or any future one, whatever the
    remote authority is doing.
    """

    payload = {
        "group_id": "telemetry-storage",
        "dataset_id": dataset_id,
        "batch_id": batch_id,
        "idempotency_key": batch_id,
        "content": {"observations": []},
        "created_at": "2026-08-09T03:00:00Z",
    }
    if poison:
        del payload["created_at"]
    entry, _created = outbox.enqueue(
        session_id="session-1",
        destination_id="fcp-local-storage",
        schema_id=RECORDER_STORAGE_SCHEMA,
        payload=payload,
        idempotency_key=batch_id,
        content_hash="c" * 64,
        now=datetime(2026, 8, 9, 3, 0, tzinfo=UTC),
    )
    return entry.outbox_id


def test_an_undeliverable_row_stops_fencing_its_own_dataset(tmp_path):
    """The regression this delivery closes.

    A row whose payload cannot produce a request had no terminal state to
    reach. It stayed pending forever and, because per-dataset ordering fences
    newer rows behind an uncommitted older one, every later batch of *that same
    dataset* stayed fenced behind it forever too. Correct ordering, permanent
    starvation.

    Against the previous implementation both assertions below fail: the
    poisoned row is still ``PENDING`` and the newer batch of its dataset has
    never been delivered.
    """

    client = RecordingClient()
    outbox = SQLiteOutbox(tmp_path / "publisher" / "outbox.sqlite3")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox, client=client, session_id="session-1"
    )
    dataset = "mtconnect:node-recorder-1:Mazak"
    poison_id = _enqueue_row(
        outbox, dataset_id=dataset, batch_id="mazak-1", poison=True
    )
    newer_id = _enqueue_row(outbox, dataset_id=dataset, batch_id="mazak-2")

    first = asyncio.run(queue.run_once())
    second = asyncio.run(queue.run_once())

    # The newer evidence of the same dataset is delivered.
    assert outbox.get(newer_id).state is OutboxState.COMPLETED
    # And the row that was fencing it is terminal, not pending.
    assert outbox.get(poison_id).state is OutboxState.RETIRED

    assert first.retired == 1
    assert first.retired_datasets == (dataset,)
    # The fence is released with the row, not held by a tombstone.
    assert second.blocked_datasets == ()
    # A withdrawal is never reported as a commit.
    assert first.committed + second.committed == 1
    assert [call["batch_id"] for call in client.calls] == ["mazak-2"]


def test_retirement_records_the_dataset_ordering_gap_it_creates(tmp_path):
    """Releasing the fence is only safe because the gap is durable."""

    client = RecordingClient()
    outbox = SQLiteOutbox(tmp_path / "publisher" / "outbox.sqlite3")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox, client=client, session_id="session-1"
    )
    dataset = "mtconnect:node-recorder-1:Mazak"
    poison_id = _enqueue_row(
        outbox, dataset_id=dataset, batch_id="mazak-1", poison=True
    )

    asyncio.run(queue.run_once())

    tombstone = outbox.get(poison_id)
    assert tombstone.state is OutboxState.RETIRED
    assert tombstone.retirement_reason == "payload-field-missing"
    # Which ordered dataset now has a hole in it, and where that hole is: the
    # dataset in a column, the sequence span still inside the immutable
    # idempotency key. Both survive receipt compaction.
    assert tombstone.retirement_dataset_id == dataset
    assert tombstone.idempotency_key == "mazak-1"
    assert tombstone.last_error
    assert outbox.retired_summary().datasets[0].dataset_id == dataset


def test_a_storage_failure_is_never_converted_into_retirement(tmp_path):
    """Time and attempt count must never promote a retryable failure.

    "The remote has been down for a week" and "this row can never be sent" are
    different facts, and only the second may end delivery. A federation outage
    that quietly retired evidence would lose exactly what the outbox exists to
    keep.
    """

    client = RecordingClient(fail=True)
    outbox = SQLiteOutbox(tmp_path / "publisher" / "outbox.sqlite3")
    dataset = "mtconnect:node-recorder-1:Mazak"
    row_id = _enqueue_row(outbox, dataset_id=dataset, batch_id="mazak-1")

    for _ in range(8):
        # A fresh queue each cycle also spends a fresh startup probe, so the
        # row is genuinely re-attempted rather than waiting out its backoff.
        result = asyncio.run(
            DurableRecorderDeliveryQueue(
                outbox=outbox, client=client, session_id="session-1"
            ).run_once()
        )
        assert result.retired == 0
        assert result.committed == 0

    survivor = outbox.get(row_id)
    assert survivor.state is OutboxState.PENDING
    assert survivor.attempt_count == 8
    assert survivor.last_error == "Federation unavailable"
    assert outbox.retired_summary().total == 0
    # The payload is intact, so the evidence is still deliverable when the
    # authority returns.
    assert survivor.payload["batch_id"] == "mazak-1"


def test_a_retired_dataset_does_not_fence_an_unrelated_dataset(tmp_path):
    client = RecordingClient()
    outbox = SQLiteOutbox(tmp_path / "publisher" / "outbox.sqlite3")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox, client=client, session_id="session-1"
    )
    poison_id = _enqueue_row(
        outbox,
        dataset_id="mtconnect:node-recorder-1:Mazak",
        batch_id="mazak-1",
        poison=True,
    )
    healthy_id = _enqueue_row(
        outbox, dataset_id="mtconnect:node-recorder-1:Okuma", batch_id="okuma-1"
    )

    result = asyncio.run(queue.run_once())

    assert outbox.get(healthy_id).state is OutboxState.COMPLETED
    assert outbox.get(poison_id).state is OutboxState.RETIRED
    assert result.blocked_datasets == ()
    assert result.retired_datasets == ("mtconnect:node-recorder-1:Mazak",)


def test_a_retirement_that_cannot_commit_leaves_the_row_exactly_as_it_was(
    tmp_path,
):
    """Failing to withdraw is safe; it is never a silent drop."""

    client = RecordingClient()
    outbox = SQLiteOutbox(tmp_path / "publisher" / "outbox.sqlite3")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox, client=client, session_id="session-1"
    )
    dataset = "mtconnect:node-recorder-1:Mazak"
    poison_id = _enqueue_row(
        outbox, dataset_id=dataset, batch_id="mazak-1", poison=True
    )

    def _refuse(*_args, **_kwargs):
        raise OSError("outbox database is unavailable")

    outbox.retire = _refuse  # type: ignore[method-assign]
    result = asyncio.run(queue.run_once())

    assert result.retired == 0
    assert result.pending == 1
    # Still durable, still retryable, still fencing its own dataset -- which is
    # the safe outcome, and still visible as blocked.
    assert outbox.get(poison_id).state is OutboxState.PENDING
    assert result.blocked_datasets == (dataset,)


def test_a_retired_row_is_not_re_enqueued_by_archive_reconciliation(tmp_path):
    """Duplicate reconciliation against the real archive cannot resurrect it.

    Reconciliation re-derives rows from the durable recorder archive on every
    scan. The archive is primary evidence and is never removed, so the only
    thing that can stop the same batch being enqueued again forever is the
    durable row itself.
    """

    client = RecordingClient()
    store, checkpoint_file, outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    assert reconciler.reconcile().enqueued == 1
    row = outbox.pending()[0]

    # Corrupt the durable payload the way a partially written or
    # partially migrated historical row would be.
    with sqlite3.connect(tmp_path / "publisher" / "outbox.sqlite3") as connection:
        payload = dict(row.payload)
        payload.pop("created_at")
        connection.execute(
            "UPDATE outbox SET payload_json=? WHERE outbox_id=?",
            (json.dumps(payload, sort_keys=True, separators=(",", ":")),
             row.outbox_id),
        )

    assert asyncio.run(queue.run_once()).retired == 1
    assert outbox.get(row.outbox_id).state is OutboxState.RETIRED

    # Every later scan of the same untouched archive collides with the
    # tombstone instead of creating fresh work.
    for _ in range(3):
        again = reconciler.reconcile()
        assert again.enqueued == 0
        assert again.already_enqueued == 1
    assert outbox.pending() == ()
    assert len(outbox.retired()) == 1

    # Even once the tombstone has been reduced to its identity receipt.
    assert outbox.compact_retired(limit=10) == 1
    compacted_scan = reconciler.reconcile()
    assert compacted_scan.enqueued == 0
    assert compacted_scan.already_enqueued == 1
    assert outbox.pending() == ()


def test_no_primary_recorder_evidence_is_deleted_by_retirement(tmp_path):
    client = RecordingClient()
    store, checkpoint_file, outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    reconciler.reconcile()
    row = outbox.pending()[0]

    def _archive() -> dict[str, bytes]:
        root = tmp_path / "data"
        return {
            str(path.relative_to(root)): path.read_bytes()
            for path in sorted(root.rglob("*"))
            if path.is_file()
        }

    before = _archive()
    assert before

    with sqlite3.connect(tmp_path / "publisher" / "outbox.sqlite3") as connection:
        payload = dict(row.payload)
        payload.pop("created_at")
        connection.execute(
            "UPDATE outbox SET payload_json=? WHERE outbox_id=?",
            (json.dumps(payload, sort_keys=True, separators=(",", ":")),
             row.outbox_id),
        )
    asyncio.run(queue.run_once())
    outbox.compact_retired(limit=10)

    assert outbox.get(row.outbox_id).state is OutboxState.RETIRED
    # Retirement is a publication decision. The recorder's own evidence is
    # byte-identical, so the withdrawn batch remains fully reconstructible.
    assert _archive() == before


# --------------------------------------------------------------------------
# B03: degraded health is observed from durable truth, never remembered
# --------------------------------------------------------------------------


def test_degraded_publication_health_survives_restart(tmp_path):
    client = RecordingClient()
    store, checkpoint_file, outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    reconciler.reconcile()
    row = outbox.pending()[0]
    with sqlite3.connect(tmp_path / "publisher" / "outbox.sqlite3") as connection:
        payload = dict(row.payload)
        payload.pop("created_at")
        connection.execute(
            "UPDATE outbox SET payload_json=? WHERE outbox_id=?",
            (json.dumps(payload, sort_keys=True, separators=(",", ":")),
             row.outbox_id),
        )

    worker = RecorderFederationDeliveryWorker(reconciler=reconciler, queue=queue)
    first = asyncio.run(worker.run_cycle())
    assert first.delivery.retired == 1
    assert first.retirement.total == 1

    # A restart: new worker, new queue, same durable database. Nothing about
    # the degraded state was carried in memory, so nothing can be lost with it.
    restarted_outbox = SQLiteOutbox(tmp_path / "publisher" / "outbox.sqlite3")
    restarted_queue = DurableRecorderDeliveryQueue(
        outbox=restarted_outbox, client=client, session_id="session-1"
    )
    restarted = RecorderFederationDeliveryWorker(
        reconciler=reconciler, queue=restarted_queue
    )
    later = asyncio.run(restarted.run_cycle())

    # This cycle retired nothing at all and still reports degraded, because it
    # reports what the database says rather than what it just did.
    assert later.delivery.retired == 0
    assert later.retirement.total == 1
    assert later.retirement.datasets[0].dataset_id.endswith("Mazak")


def test_degraded_health_clears_only_from_durable_repair(tmp_path):
    client = RecordingClient()
    outbox = SQLiteOutbox(tmp_path / "publisher" / "outbox.sqlite3")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox, client=client, session_id="session-1"
    )
    poison_id = _enqueue_row(
        outbox,
        dataset_id="mtconnect:node-recorder-1:Mazak",
        batch_id="mazak-1",
        poison=True,
    )
    asyncio.run(queue.run_once())
    assert outbox.retired_summary().total == 1

    # Repairing the payload is not enough on its own: retirement is terminal
    # until an operator explicitly reinstates the row.
    with sqlite3.connect(tmp_path / "publisher" / "outbox.sqlite3") as connection:
        payload = dict(outbox.get(poison_id).payload)
        payload["created_at"] = "2026-08-09T03:00:00Z"
        connection.execute(
            "UPDATE outbox SET payload_json=? WHERE outbox_id=?",
            (json.dumps(payload, sort_keys=True, separators=(",", ":")), poison_id),
        )
    assert outbox.retired_summary().total == 1

    outbox.reinstate(poison_id, now=datetime(2026, 8, 9, 4, 0, tzinfo=UTC))

    assert outbox.retired_summary().total == 0
    assert len(outbox.pending()) == 1
    result = asyncio.run(
        DurableRecorderDeliveryQueue(
            outbox=outbox, client=client, session_id="session-1"
        ).run_once()
    )
    assert result.committed == 1
    assert outbox.get(poison_id).state is OutboxState.COMPLETED


def test_a_failing_publication_cycle_is_reported_not_swallowed(tmp_path):
    """The required-loop boundary must not absorb failures in silence.

    ``run_forever`` caught every cycle failure and discarded it, so a recorder
    whose durable state had become unreadable spun this loop indefinitely while
    every health surface still described an ordinary running publisher.
    """

    client = RecordingClient()
    _store, _checkpoint, _outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )

    def _unreadable():
        raise OSError("recorder publication outbox is unreadable")

    reconciler.reconcile = _unreadable  # type: ignore[method-assign]
    reports: list[object] = []
    stop = asyncio.Event()

    class _FailingWorker(RecorderFederationDeliveryWorker):
        async def run_cycle(self, *, force_reconcile: bool = False):
            raise OSError("recorder publication outbox is unreadable")

    worker = _FailingWorker(
        reconciler=reconciler,
        queue=queue,
        poll_interval_seconds=0.01,
        cycle_observer=reports.append,
    )

    async def _drive() -> None:
        task = asyncio.create_task(worker.run_forever(stop))
        while len(reports) < 3:
            await asyncio.sleep(0)
        stop.set()
        await task

    asyncio.run(_drive())

    assert [report.state for report in reports[:3]] == ["failing"] * 3
    assert reports[0].error_code == "OSError"
    assert reports[0].result is None
    # Consecutive failures are counted, so an owner can escalate rather than
    # watch an identical failure scroll past forever.
    assert [report.consecutive_failures for report in reports[:3]] == [1, 2, 3]


def test_a_healthy_cycle_reports_publishing_and_clears_earlier_failure(tmp_path):
    client = RecordingClient()
    store, checkpoint_file, _outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    reports: list[object] = []
    stop = asyncio.Event()
    failures = {"remaining": 1}
    original = RecorderFederationDeliveryWorker.run_cycle

    class _FlakyWorker(RecorderFederationDeliveryWorker):
        async def run_cycle(self, *, force_reconcile: bool = False):
            if failures["remaining"]:
                failures["remaining"] -= 1
                raise OSError("transient")
            return await original(self, force_reconcile=force_reconcile)

    worker = _FlakyWorker(
        reconciler=reconciler,
        queue=queue,
        poll_interval_seconds=0.01,
        cycle_observer=reports.append,
    )

    async def _drive() -> None:
        task = asyncio.create_task(worker.run_forever(stop))
        while len(reports) < 2:
            await asyncio.sleep(0)
        stop.set()
        await task

    asyncio.run(_drive())

    assert reports[0].state == "failing"
    assert reports[1].state == "publishing"
    assert reports[1].healthy is True
    assert reports[1].consecutive_failures == 0


def test_a_health_sink_that_raises_cannot_strand_durable_delivery(tmp_path):
    client = RecordingClient()
    store, checkpoint_file, outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    stop = asyncio.Event()
    seen: list[object] = []
    hostile = {"active": True}

    def _sink(report: object) -> None:
        seen.append(report)
        if hostile["active"]:
            raise RuntimeError("health sink is broken")

    worker = RecorderFederationDeliveryWorker(
        reconciler=reconciler,
        queue=queue,
        poll_interval_seconds=0.01,
        cycle_observer=_sink,
    )

    async def _drive() -> None:
        task = asyncio.create_task(worker.run_forever(stop))
        while len(seen) < 2:
            await asyncio.sleep(0)
        hostile["active"] = False
        while len(seen) < 3:
            await asyncio.sleep(0)
        stop.set()
        await task

    asyncio.run(_drive())

    # The evidence was still delivered while the health sink was failing.
    assert client.calls
    assert outbox.pending() == ()
    # And the hand-offs it dropped were counted, not discarded: the first
    # report that gets through carries them.
    assert seen[0].dropped_reports == 0
    assert seen[2].dropped_reports == 2


def test_one_tombstone_cannot_abort_reconciliation_of_the_rest_of_the_archive(
    tmp_path,
):
    """A retired row's payload is no longer authoritative.

    A row is often retired precisely because its stored payload is corrupt, so
    it no longer matches what reconciliation rebuilds from the archive.
    Comparing the two would raise an idempotency conflict out of the middle of
    the scan and abort reconciliation of every remaining batch -- reproducing,
    through the tombstone, the same global starvation the tombstone exists to
    end. Identity is compared instead, so a key genuinely reused for different
    content still fails closed.
    """

    client = RecordingClient()
    store, checkpoint_file, outbox, queue, reconciler = _build_reconciler(
        tmp_path, client=client
    )
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    assert reconciler.reconcile().enqueued == 1
    first = outbox.pending()[0]

    with sqlite3.connect(tmp_path / "publisher" / "outbox.sqlite3") as connection:
        payload = dict(first.payload)
        payload.pop("created_at")
        connection.execute(
            "UPDATE outbox SET payload_json=? WHERE outbox_id=?",
            (
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                first.outbox_id,
            ),
        )
    assert asyncio.run(queue.run_once()).retired == 1

    # Later evidence is committed locally, and the scan that must pick it up
    # walks straight past the tombstone of the earlier batch.
    _probe, second_batch, _stored = _store_sample(store, _second_sample())
    assert second_batch.first_observation_sequence == 13
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=16)

    result = reconciler.reconcile()

    assert result.enqueued == 1
    assert result.already_enqueued == 1
    assert asyncio.run(queue.run_once()).committed == 1
    assert outbox.get(first.outbox_id).state is OutboxState.RETIRED
    assert outbox.pending() == ()
