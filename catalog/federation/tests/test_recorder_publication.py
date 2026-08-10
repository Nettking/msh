from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import DurableRecorderDeliveryQueue
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
