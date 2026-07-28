from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha256
import sqlite3

import pytest

from federation import CapabilityAnnouncement, CapabilityStatus, StorageBatch
from federation.errors import FederationValidationError
from federation.filesystem_storage import FilesystemRecorderStorageProvider
from federation.outbox import MAX_ERROR_LENGTH, OutboxState, SQLiteOutbox
from federation.registry import LocalCapabilityRegistry
from mtconnect_recorder.parsing import parse_probe, parse_streams
from mtconnect_recorder.storage import DurableRecorderStore

NOW = datetime(2026, 7, 28, tzinfo=timezone.utc)
PROBE = '''<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.3"><Header instanceId="7"/><Devices><Device id="d" name="M"><DataItems><DataItem id="x" name="Xabs" category="SAMPLE" type="POSITION"/></DataItems></Device></Devices></MTConnectDevices>'''
SAMPLE = '''<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7"><Header instanceId="7" firstSequence="1" lastSequence="1" nextSequence="2"/><Streams><DeviceStream name="M"><ComponentStream><Samples><Position dataItemId="x" sequence="1" timestamp="2026-07-28T00:00:00Z">1</Position></Samples></ComponentStream></DeviceStream></Streams></MTConnectStreams>'''


def capability(capability_id: str, node: str = "node-a", kind: str = "storage",
               session: str = "session-a") -> CapabilityAnnouncement:
    return CapabilityAnnouncement(capability_id, node, session, kind, "local", "1",
                                  CapabilityStatus.READY, {"label": capability_id}, NOW)


def test_f1_007_multiple_capabilities_of_one_type_and_deterministic_filters():
    registry = LocalCapabilityRegistry()
    for item in (capability("z", "same"), capability("a", "same"),
                 capability("lm2", "node-b", "language-model"),
                 capability("lm1", "node-a", "language-model")):
        registry.register(item)
    assert [item.capability_id for item in registry.list()] == ["lm1", "lm2", "a", "z"]
    assert len(registry.list(capability_type="storage", node_id="same")) == 2


def test_f1_008_capability_removal_and_isolated_update():
    registry = LocalCapabilityRegistry()
    one, two = capability("one"), capability("two")
    registry.register(one)
    registry.register(two)
    registry.update(replace(one, status=CapabilityStatus.DRAINING))
    assert registry.get("session-a", "two") == two
    registry.remove("session-a", "one")
    assert registry.list() == (two,)


def test_registry_scopes_identity_rejects_reuse_and_secret_metadata():
    registry = LocalCapabilityRegistry()
    registry.register(capability("same", session="one"))
    registry.register(capability("same", session="two"))
    with pytest.raises(FederationValidationError, match="identity-conflict"):
        registry.register(capability("same", node="other", session="one"))
    with pytest.raises(FederationValidationError, match="secret-property"):
        replace(capability("bad"), properties={"nested": {"api_key": "no"}})
    with pytest.raises(FederationValidationError, match="invalid-json"):
        replace(capability("bad-json"), properties={"object": object()})


def enqueue(outbox: SQLiteOutbox, hash_: str = "sha256:a"):
    return outbox.enqueue(session_id="s", destination_id="cap", schema_id="schema.v1",
                          payload={"x": 1}, idempotency_key="key", content_hash=hash_, now=NOW)


def storage_batch(content_hash: str) -> StorageBatch:
    return StorageBatch(
        session_id="s",
        group_id="storage-main",
        dataset_id="telemetry",
        batch_id="batch-1",
        source_node_id="recorder-node",
        idempotency_key="machine:7:1-1",
        content_hash=content_hash,
        record_count=1,
        first_sequence=1,
        last_sequence=1,
        created_at=NOW,
    )


def test_f1_002_atomic_outbox_enqueue_and_conflict_rollback(tmp_path):
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    entry, created = enqueue(outbox)
    assert created and entry.outbox_id == 1
    with pytest.raises(FederationValidationError, match="idempotency-conflict"):
        enqueue(outbox, "sha256:different")
    assert outbox.pending() == (entry,)
    with pytest.raises(FederationValidationError, match="invalid-json"):
        outbox.enqueue(session_id="s", destination_id="cap", schema_id="schema.v1",
                       payload={"bad": object()}, idempotency_key="other",
                       content_hash="sha256:x", now=NOW)
    assert outbox.pending() == (entry,)


def test_f1_003_pending_survives_restart_and_identical_enqueue(tmp_path):
    path = tmp_path / "outbox.db"
    first, _ = enqueue(SQLiteOutbox(path))
    reopened = SQLiteOutbox(path)
    repeated, created = enqueue(reopened)
    assert not created and repeated == first and reopened.pending() == (first,)


def test_f1_004_acknowledgement_is_idempotent(tmp_path):
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    entry, _ = enqueue(outbox)
    first = outbox.acknowledge(entry.outbox_id, now=NOW)
    second = SQLiteOutbox(tmp_path / "outbox.db").acknowledge(entry.outbox_id, now=NOW)
    assert first == second and second.state is OutboxState.COMPLETED and not outbox.pending()


def test_f1_005_retry_metadata_is_bounded_and_deterministic(tmp_path):
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    entry, _ = enqueue(outbox)
    one = outbox.record_failure(entry.outbox_id, error="x" * 9999, now=NOW,
                                base_delay_seconds=10, max_delay_seconds=15)
    two = outbox.record_failure(entry.outbox_id, error="again", now=NOW,
                                base_delay_seconds=10, max_delay_seconds=15)
    assert one.attempt_count == 1 and len(one.last_error or "") == MAX_ERROR_LENGTH
    assert (one.next_attempt_at - NOW).total_seconds() == 10
    assert two.attempt_count == 2 and (two.next_attempt_at - NOW).total_seconds() == 15


def test_f1_006_content_mismatch_on_reused_idempotency_key(tmp_path):
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    enqueue(outbox)
    with pytest.raises(FederationValidationError, match="idempotency-conflict"):
        enqueue(outbox, "sha256:b")


def test_schema_initialization_repeats_and_malformed_row_is_structured(tmp_path):
    path = tmp_path / "outbox.db"
    outbox = SQLiteOutbox(path)
    SQLiteOutbox(path)
    entry, _ = enqueue(outbox)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE outbox SET created_at='not-a-time' WHERE outbox_id=?", (entry.outbox_id,))
    with pytest.raises(FederationValidationError, match="malformed-outbox-row"):
        outbox.get(entry.outbox_id)


def test_f1_001_filesystem_adapter_exact_recorder_output_and_outbox_recovery(tmp_path):
    probe = parse_probe(PROBE)
    batch = parse_streams(SAMPLE, source_name="machine", probe=probe)
    direct_root, adapter_root = tmp_path / "direct", tmp_path / "adapter"
    DurableRecorderStore(direct_root).store_batch(source_name="machine", requested_from=1,
                                                   xml_text=SAMPLE, batch=batch)
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    adapter = FilesystemRecorderStorageProvider(adapter_root, outbox)
    adapter.store_batch(source_name="machine", requested_from=1, xml_text=SAMPLE, batch=batch,
                        session_id="s", destination_id="cap", now=NOW)
    direct = {p.relative_to(direct_root): p.read_bytes() for p in direct_root.rglob("*") if p.is_file()}
    adapted = {p.relative_to(adapter_root): p.read_bytes() for p in adapter_root.rglob("*") if p.is_file()}
    assert direct.keys() == adapted.keys()
    for name in direct:
        if name.name.endswith("manifest.json"):
            import json
            left, right = json.loads(direct[name]), json.loads(adapted[name])
            left["raw_file"] = right["raw_file"] = name.name
            assert left == right
        else:
            assert direct[name] == adapted[name]
    assert len(outbox.pending()) == 1
    with sqlite3.connect(tmp_path / "outbox.db") as db:
        db.execute("UPDATE outbox SET state='prepared'")
    assert FilesystemRecorderStorageProvider(
        adapter_root, SQLiteOutbox(tmp_path / "outbox.db")
    ).reconcile(now=NOW) == 1


def test_incomplete_recorder_prepare_is_not_enqueued(tmp_path):
    probe = parse_probe(PROBE)
    batch = parse_streams(SAMPLE, source_name="machine", probe=probe)
    store = DurableRecorderStore(tmp_path / "data")
    store.store_raw_batch(source_name="machine", requested_from=1, xml_text=SAMPLE, batch=batch)
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    adapter = FilesystemRecorderStorageProvider(tmp_path / "data", outbox)
    adapter.prepare_delivery(source_name="machine", instance_id=7, requested_from=1,
                             first=1, last=1, next_sequence=2,
                             digest=sha256(SAMPLE.encode()).hexdigest(), count=1,
                             session_id="s", destination_id="cap", now=NOW)
    assert adapter.reconcile(now=NOW) == 0


def test_filesystem_provider_write_with_outbox_persists_and_enqueues(tmp_path):
    parsed = parse_streams(SAMPLE, source_name="machine", probe=parse_probe(PROBE))
    digest = sha256(SAMPLE.encode("utf-8")).hexdigest()
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    provider = FilesystemRecorderStorageProvider(tmp_path / "data", outbox)

    outcome = provider.write(storage_batch(f"sha256:{digest}"), {
        "source_name": "machine",
        "requested_from": 1,
        "xml_text": SAMPLE,
        "batch": parsed,
        "delivery": {"destination_id": "cap", "now": NOW},
    })

    assert outcome.durable and outcome.created and outcome.error is None
    assert len(list((tmp_path / "data").rglob("*.xml.gz"))) == 1
    assert len(list((tmp_path / "data").rglob("*.ndjson"))) == 1
    assert len(list((tmp_path / "data").rglob("*.jsonl"))) == 1
    assert len(outbox.pending()) == 1


def test_hash_mismatch_rejects_without_any_durable_mutation(tmp_path):
    parsed = parse_streams(SAMPLE, source_name="machine", probe=parse_probe(PROBE))
    data_dir = tmp_path / "data"
    checkpoint = tmp_path / "checkpoints.json"
    checkpoint.write_text('{"next_sequence":1}', encoding="utf-8")
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    provider = FilesystemRecorderStorageProvider(data_dir, outbox)

    outcome = provider.write(storage_batch("sha256:not-the-content-hash"), {
        "source_name": "machine",
        "requested_from": 1,
        "xml_text": SAMPLE,
        "batch": parsed,
        "delivery": {"destination_id": "cap", "now": NOW},
    })

    assert not outcome.durable and not outcome.created
    assert outcome.error and outcome.error.code.value == "conflict"
    assert not data_dir.exists()
    assert checkpoint.read_text(encoding="utf-8") == '{"next_sequence":1}'
    assert outbox.pending() == ()


def test_distinct_empty_batch_coordinates_have_distinct_delivery_identities(tmp_path):
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    provider = FilesystemRecorderStorageProvider(tmp_path / "data", outbox)
    common = {"source_name": "machine", "instance_id": 7, "first": None,
              "last": None, "count": 0, "session_id": "s",
              "destination_id": "cap", "now": NOW}

    assert provider.enqueue_delivery(requested_from=1, next_sequence=2,
                                     digest="a" * 64, **common)
    assert provider.enqueue_delivery(requested_from=2, next_sequence=3,
                                     digest="b" * 64, **common)
    assert len(outbox.pending()) == 2


def test_crash_after_delivery_prepare_before_recorder_write(tmp_path, monkeypatch):
    parsed = parse_streams(SAMPLE, source_name="machine", probe=parse_probe(PROBE))
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    provider = FilesystemRecorderStorageProvider(tmp_path / "data", outbox)
    monkeypatch.setattr(provider.store, "store_batch",
                        lambda **kwargs: (_ for _ in ()).throw(OSError("crash")))

    with pytest.raises(OSError, match="crash"):
        provider.store_batch(source_name="machine", requested_from=1,
                             xml_text=SAMPLE, batch=parsed, session_id="original-session",
                             destination_id="original-capability", now=NOW)

    assert outbox.pending() == ()
    prepared = SQLiteOutbox(tmp_path / "outbox.db").prepared()
    assert len(prepared) == 1
    assert prepared[0].session_id == "original-session"
    assert prepared[0].destination_id == "original-capability"


def test_local_durable_activation_failure_is_truthful_and_restart_reconciles(
        tmp_path, monkeypatch):
    parsed = parse_streams(SAMPLE, source_name="machine", probe=parse_probe(PROBE))
    digest = sha256(SAMPLE.encode()).hexdigest()
    path = tmp_path / "outbox.db"
    outbox = SQLiteOutbox(path)
    provider = FilesystemRecorderStorageProvider(tmp_path / "data", outbox)
    real_activate = outbox.activate
    monkeypatch.setattr(outbox, "activate",
                        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("crash")))

    outcome = provider.write(storage_batch(f"sha256:{digest}"), {
        "source_name": "machine", "requested_from": 1, "xml_text": SAMPLE,
        "batch": parsed,
        "delivery": {"destination_id": "original-capability", "now": NOW},
    })

    assert outcome.durable and outcome.created
    assert outcome.error and outcome.error.code.value == "reconciliation-required"
    assert len(outbox.prepared()) == 1 and outbox.pending() == ()
    monkeypatch.setattr(outbox, "activate", real_activate)
    restarted = FilesystemRecorderStorageProvider(tmp_path / "data", SQLiteOutbox(path))
    assert restarted.reconcile(now=NOW) == 1
    pending = restarted.outbox.pending()
    assert len(pending) == 1
    assert pending[0].session_id == "s"
    assert pending[0].destination_id == "original-capability"


def test_delivery_identity_conflict_is_rejected_before_recorder_mutation(tmp_path):
    parsed = parse_streams(SAMPLE, source_name="machine", probe=parse_probe(PROBE))
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    provider = FilesystemRecorderStorageProvider(tmp_path / "data", outbox)
    provider.prepare_delivery(source_name="machine", instance_id=7, requested_from=1,
                              first=1, last=1, next_sequence=2, digest="0" * 64,
                              count=1, session_id="s", destination_id="cap", now=NOW)

    with pytest.raises(FederationValidationError, match="idempotency-conflict"):
        provider.store_batch(source_name="machine", requested_from=1, xml_text=SAMPLE,
                             batch=parsed, session_id="s", destination_id="cap", now=NOW)

    assert not (tmp_path / "data").exists()
    assert len(outbox.prepared()) == 1 and outbox.pending() == ()


def test_outbox_prepare_transition_matrix(tmp_path):
    for existing_state in (None, "prepared", "pending", "completed"):
        outbox = SQLiteOutbox(tmp_path / f"prepare-{existing_state}.db")
        if existing_state is not None:
            entry, _ = outbox.prepare(session_id="s", destination_id="cap",
                                      schema_id="schema.v1", payload={"x": 1},
                                      idempotency_key="key", content_hash="sha256:a", now=NOW)
            if existing_state in {"pending", "completed"}:
                outbox.activate(entry.outbox_id, now=NOW)
            if existing_state == "completed":
                outbox.acknowledge(entry.outbox_id, now=NOW)
        result, created = outbox.prepare(session_id="s", destination_id="cap",
                                         schema_id="schema.v1", payload={"x": 1},
                                         idempotency_key="key", content_hash="sha256:a", now=NOW)
        assert result.state.value == (existing_state or "prepared")
        assert created is (existing_state is None)


def test_outbox_enqueue_transition_matrix(tmp_path):
    for existing_state in (None, "prepared", "pending", "completed"):
        outbox = SQLiteOutbox(tmp_path / f"enqueue-{existing_state}.db")
        if existing_state is not None:
            entry, _ = outbox.prepare(session_id="s", destination_id="cap",
                                      schema_id="schema.v1", payload={"x": 1},
                                      idempotency_key="key", content_hash="sha256:a", now=NOW)
            if existing_state in {"pending", "completed"}:
                outbox.activate(entry.outbox_id, now=NOW)
            if existing_state == "completed":
                outbox.acknowledge(entry.outbox_id, now=NOW)
        result, created = enqueue(outbox)
        assert result.state.value == ("completed" if existing_state == "completed" else "pending")
        assert created is (existing_state is None)


def test_outbox_activate_transition_matrix_and_conflict_preserves_state(tmp_path):
    outbox = SQLiteOutbox(tmp_path / "activate.db")
    entry, _ = outbox.prepare(session_id="s", destination_id="cap",
                              schema_id="schema.v1", payload={"x": 1},
                              idempotency_key="key", content_hash="sha256:a", now=NOW)
    assert outbox.activate(entry.outbox_id, now=NOW).state is OutboxState.PENDING
    assert outbox.activate(entry.outbox_id, now=NOW).state is OutboxState.PENDING
    outbox.acknowledge(entry.outbox_id, now=NOW)
    assert outbox.activate(entry.outbox_id, now=NOW).state is OutboxState.COMPLETED
    with pytest.raises(FederationValidationError, match="idempotency-conflict"):
        enqueue(outbox, "sha256:different")
    assert outbox.get(entry.outbox_id).state is OutboxState.COMPLETED


def test_provider_replay_completed_is_successful_and_not_created(tmp_path):
    parsed = parse_streams(SAMPLE, source_name="machine", probe=parse_probe(PROBE))
    digest = sha256(SAMPLE.encode()).hexdigest()
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    provider = FilesystemRecorderStorageProvider(tmp_path / "data", outbox)
    payload = {"source_name": "machine", "requested_from": 1, "xml_text": SAMPLE,
               "batch": parsed, "delivery": {"destination_id": "cap", "now": NOW}}
    first = provider.write(storage_batch(f"sha256:{digest}"), payload)
    outbox.acknowledge(outbox.pending()[0].outbox_id, now=NOW)

    replay = provider.write(storage_batch(f"sha256:{digest}"), payload)

    assert first.created and first.error is None
    assert replay.durable and not replay.created and replay.error is None


def test_provider_maps_conflict_invalid_and_io_errors(tmp_path, monkeypatch):
    parsed = parse_streams(SAMPLE, source_name="machine", probe=parse_probe(PROBE))
    digest = sha256(SAMPLE.encode()).hexdigest()
    outbox = SQLiteOutbox(tmp_path / "outbox.db")
    provider = FilesystemRecorderStorageProvider(tmp_path / "data", outbox)
    provider.prepare_delivery(source_name="machine", instance_id=7, requested_from=1,
                              first=1, last=1, next_sequence=2, digest="0" * 64,
                              count=1, session_id="s", destination_id="cap", now=NOW)
    payload = {"source_name": "machine", "requested_from": 1, "xml_text": SAMPLE,
               "batch": parsed, "delivery": {"destination_id": "cap", "now": NOW}}
    conflict = provider.write(storage_batch(f"sha256:{digest}"), payload)
    invalid = provider.write(storage_batch(f"sha256:{digest}"), {"batch": parsed,
                             "xml_text": SAMPLE, "delivery": payload["delivery"]})
    io_provider = FilesystemRecorderStorageProvider(tmp_path / "io-data")
    monkeypatch.setattr(io_provider.store, "store_batch",
                        lambda **kwargs: (_ for _ in ()).throw(OSError("disk failed")))
    io_error = io_provider.write(storage_batch(f"sha256:{digest}"), payload)

    assert conflict.error and conflict.error.code.value == "conflict"
    assert invalid.error and invalid.error.code.value == "invalid"
    assert io_error.error and io_error.error.code.value == "io-error"
