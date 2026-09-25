"""Actual operation timings are bounded evidence, never capture policy."""
from __future__ import annotations

import json

import pytest

from catalog.federation.incremental_recorder_publication import (
    IncrementalRecorderArchiveReconciler,
)
from catalog.federation.recorder_publication import (
    QuarantineSummary,
    RecorderArchiveReconciler,
    RecorderPublicationTarget,
    RecorderReconcileResult,
)
from catalog.mtconnect_recorder import acceptance_observability as obs
from catalog.mtconnect_recorder import runtime as rt
from catalog.mtconnect_recorder.model import ProbeModel, SourceCheckpoint
from catalog.mtconnect_recorder.storage import DurableRecorderStore

CANDIDATE = "a" * 40
CONTEXT = {"source_alias": "b" * 64, "session_id": "session-test", "node_id": "node-test"}
PROGRESS = {"next_sequence_after": 3, "advanced_sequences": 2}


@pytest.fixture
def registry(monkeypatch):
    value = obs.ObservationRegistry()
    monkeypatch.setattr(obs, "_REGISTRY", value)
    monkeypatch.setenv("FCP_BUILD_COMMIT", CANDIDATE)
    monkeypatch.delenv("FCP_RECORDER_BUILD_COMMIT", raising=False)
    monkeypatch.delenv("FCP_RECORDER_SUPERVISOR_SESSION", raising=False)
    return value


def _observed(function, *, context=None, progress=None):
    return obs.observe_operation(
        "recorder-recovery", context=context or (lambda *_a, **_k: CONTEXT),
        progress=progress or (lambda _result, _context: PROGRESS),
    )(function)


def _only(registry):
    records = registry.snapshot()["operations"]
    assert len(records) == 1
    return records[0]


def test_completed_operation_binds_exact_result_clocks_and_process(registry, monkeypatch):
    ticks = iter([1_000, 1_300, 1_400])
    monkeypatch.setattr(obs.time, "monotonic_ns", lambda: next(ticks))
    result = object()
    calls = []

    @_observed
    def operation():
        calls.append(True)
        record = next(iter(registry._operations.values()))
        assert record["outcome"] == "incomplete"
        assert record["duration_ns"] is None
        return result

    assert operation() is result
    assert calls == [True]
    record = _only(registry)
    assert record["outcome"] == "completed"
    assert record["started_at_monotonic_ns"] == 1_000
    assert record["ended_at_monotonic_ns"] == 1_300
    assert record["duration_ns"] == 300
    assert record["started_at_utc"].endswith("Z")
    assert record["ended_at_utc"].endswith("Z")
    assert record["progress"] == PROGRESS
    assert len(record["operation_id"]) == 32
    assert record["operation_sequence"] == 1
    assert record["provenance"]["candidate_sha"] == CANDIDATE
    assert record["provenance"]["supervisor_generation"] is None
    assert record["context"] == CONTEXT


@pytest.mark.parametrize("failure, outcome", [(ValueError("private endpoint"), "failed"),
                                               (KeyboardInterrupt(), "interrupted")])
def test_failed_or_interrupted_business_operation_preserves_exception(registry, failure, outcome):
    @_observed
    def operation():
        raise failure

    with pytest.raises(type(failure)) as raised:
        operation()
    assert raised.value is failure
    record = _only(registry)
    assert record["outcome"] == outcome
    assert record["progress"] is None
    assert record["error_type"] == type(failure).__name__
    assert "private endpoint" not in json.dumps(record)


def test_new_incomplete_attempt_replaces_old_completion_and_ignores_late_result(registry):
    old = registry.begin("recorder-recovery", CONTEXT)
    registry.finish(old, progress=PROGRESS)
    newer = registry.begin("recorder-recovery", CONTEXT)
    registry.finish(old, progress=PROGRESS)
    record = _only(registry)
    assert record["operation_id"] == newer[1]
    assert record["operation_sequence"] == 2
    assert record["outcome"] == "incomplete"
    assert record["progress"] is record["duration_ns"] is None


def test_registry_retains_bounded_latest_scopes_and_detached_snapshots(registry):
    for index in range(obs.MAX_OPERATIONS + 3):
        token = registry.begin("recorder-recovery", {"source_alias": f"source-{index}"})
        registry.finish(token, progress=PROGRESS)
    snapshot = registry.snapshot()
    assert len(snapshot["operations"]) == obs.MAX_OPERATIONS
    assert snapshot["max_operations"] == obs.MAX_OPERATIONS
    assert snapshot["evicted_operations"] == 3
    assert snapshot["retention_truncated"] is True
    assert snapshot["operations"][0]["operation_sequence"] == 4
    snapshot["operations"][0]["outcome"] = "invented"
    assert registry.snapshot()["operations"][0]["outcome"] == "completed"


def test_contended_observation_never_blocks_or_prevents_business_work(registry):
    calls = []
    operation = _observed(lambda: calls.append("called"))
    registry._lock.acquire()
    try:
        operation()
        assert obs.snapshot()["available"] is False
        assert obs.process_provenance()["runtime_generation"] is None
    finally:
        registry._lock.release()
    assert calls == ["called"]
    snapshot = registry.snapshot()
    assert snapshot["dropped_updates"] >= 1
    assert snapshot["operations"] == []


def test_new_real_invocation_recovers_observation_after_a_missed_attempt(registry):
    first = registry.begin("recorder-recovery", CONTEXT)
    registry.finish(first, progress=PROGRESS)
    calls = []
    registry._lock.acquire()
    try:
        _observed(lambda: calls.append("unobserved business call"))()
    finally:
        registry._lock.release()
    lost = registry.snapshot()
    assert calls == ["unobserved business call"]
    assert lost["dropped_updates"] == 1
    assert lost["operations"][0]["observation_loss_count"] == 0
    assert lost["operations"][0]["operation_id"] == first[1]

    fresh = registry.begin("recorder-recovery", CONTEXT)
    registry.finish(fresh, progress=PROGRESS)
    restored = registry.snapshot()
    assert restored["dropped_updates"] == 1  # Lifetime loss remains explicit.
    assert restored["operations"][0]["operation_id"] == fresh[1] != first[1]
    assert restored["operations"][0]["outcome"] == "completed"
    assert restored["operations"][0]["observation_loss_count"] == restored["dropped_updates"]


def test_loss_during_span_is_not_hidden_by_its_later_success(registry):
    token = registry.begin("recorder-recovery", CONTEXT)
    registry._lock.acquire()
    try:
        assert registry.begin("recorder-recovery", CONTEXT) is None
    finally:
        registry._lock.release()
    registry.finish(token, progress=PROGRESS)
    snapshot = registry.snapshot()
    record = snapshot["operations"][0]
    assert record["outcome"] == "completed"  # This operation did return normally.
    assert record["observation_loss_count"] == 0
    assert snapshot["dropped_updates"] == 1  # It cannot prove the latest attempt.


def test_refreshing_one_scope_does_not_refresh_a_stale_loss_epoch_in_another(registry):
    other = {**CONTEXT, "source_alias": "c" * 64}
    for context in (CONTEXT, other):
        registry.finish(registry.begin("recorder-recovery", context), progress=PROGRESS)
    registry._lock.acquire()
    try:
        assert registry.begin("recorder-recovery", other) is None
    finally:
        registry._lock.release()
    registry.finish(registry.begin("recorder-recovery", CONTEXT), progress=PROGRESS)
    snapshot = registry.snapshot()
    records = {record["context"]["source_alias"]: record for record in snapshot["operations"]}
    assert records[CONTEXT["source_alias"]]["observation_loss_count"] == snapshot["dropped_updates"]
    assert records[other["source_alias"]]["observation_loss_count"] < snapshot["dropped_updates"]


@pytest.mark.parametrize("failure_point", ["context", "start_clock", "progress", "end_clock"])
def test_observer_failure_cannot_change_success_or_fabricate_completion(
    registry, monkeypatch, failure_point,
):
    original = registry.begin("recorder-recovery", CONTEXT)
    registry.finish(original, progress=PROGRESS)
    calls = []

    def fail(*_args, **_kwargs):
        raise RuntimeError("observer failed")

    def operation():
        calls.append(True)
        if failure_point == "end_clock":
            monkeypatch.setattr(obs.time, "monotonic_ns", fail)
        return 7

    if failure_point == "start_clock":
        monkeypatch.setattr(obs.time, "monotonic_ns", fail)
    observed = _observed(operation, context=fail if failure_point == "context" else None,
                         progress=fail if failure_point == "progress" else None)
    assert observed() == 7
    assert calls == [True]
    monkeypatch.setattr(obs.time, "monotonic_ns", lambda: 9_999_999)
    result = registry.snapshot()
    assert result["dropped_updates"] >= 1
    assert all(record["outcome"] != "completed" for record in result["operations"])


def test_observer_failure_does_not_replace_original_business_exception(registry, monkeypatch):
    failure = ValueError("original failure")

    def broken_finish(*_args, **_kwargs):
        raise OSError("observer failed")

    monkeypatch.setattr(registry, "finish", broken_finish)

    @_observed
    def operation():
        raise failure

    with pytest.raises(ValueError) as raised:
        operation()
    assert raised.value is failure
    assert _only(registry)["outcome"] == "incomplete"


@pytest.mark.parametrize("progress", [{}, {"next_sequence_after": 1},
    {"next_sequence_after": 1, "advanced_sequences": None},
    {"next_sequence_after": 1, "advanced_sequences": True},
    {"next_sequence_after": 1, "advanced_sequences": -1}])
def test_invalid_progress_is_unavailable_without_changing_result(registry, progress):
    operation = _observed(lambda: "unchanged", progress=lambda _result, _context: progress)
    assert operation() == "unchanged"
    assert _only(registry)["outcome"] == "incomplete"
    assert registry.snapshot()["dropped_updates"] == 1


@pytest.mark.parametrize("ending", [99, True, None])
def test_invalid_monotonic_end_never_produces_completed_zero(registry, monkeypatch, ending):
    ticks = iter([100, ending, 101])
    monkeypatch.setattr(obs.time, "monotonic_ns", lambda: next(ticks))
    token = registry.begin("recorder-recovery", CONTEXT)
    registry.finish(token, progress=PROGRESS)
    record = _only(registry)
    assert record["outcome"] == "incomplete"
    assert record["duration_ns"] is None


def test_zero_duration_is_valid_only_with_an_actual_completed_span(registry, monkeypatch):
    monkeypatch.setattr(obs.time, "monotonic_ns", lambda: 100)
    token = registry.begin("recorder-recovery", CONTEXT)
    registry.finish(token, progress={"next_sequence_after": 1, "advanced_sequences": 0})
    record = _only(registry)
    assert record["duration_ns"] == 0
    assert record["outcome"] == "completed"
    assert record["operation_id"]
    assert record["progress"]["advanced_sequences"] == 0


def test_process_generation_changes_after_pid_change_and_old_operations_disappear(registry, monkeypatch):
    before = registry.provenance()
    token = registry.begin("recorder-recovery", CONTEXT)
    registry.finish(token, progress=PROGRESS)
    monkeypatch.setattr(obs.os, "getpid", lambda: before["pid"] + 1)
    after = registry.snapshot()
    assert after["provenance"]["runtime_generation"] != before["runtime_generation"]
    assert after["operations"] == []


def test_ambiguous_candidate_and_missing_supervisor_are_not_invented(registry, monkeypatch):
    monkeypatch.setenv("FCP_RECORDER_BUILD_COMMIT", "c" * 40)
    value = registry.provenance()
    assert value["candidate_sha"] is None
    assert value["supervisor_generation"] is None
    monkeypatch.setenv("FCP_RECORDER_BUILD_COMMIT", CANDIDATE)
    monkeypatch.setenv("FCP_RECORDER_SUPERVISOR_SESSION", "D" * 32)
    value = registry.provenance()
    assert value["candidate_sha"] == CANDIDATE
    assert value["supervisor_generation"] == "d" * 32


def test_snapshot_observer_failure_is_explicitly_unavailable(registry, monkeypatch):
    def fail():
        raise RuntimeError("snapshot failed")

    monkeypatch.setattr(registry, "snapshot", fail)
    assert obs.snapshot() == {"schema": obs.SCHEMA, "available": False, "operations": []}


@pytest.fixture
def recorder(tmp_path, monkeypatch):
    monkeypatch.setattr(rt, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(rt, "STATUS_FILE", tmp_path / "status.json")
    monkeypatch.setattr(rt, "_FEDERATION_STATUS_PROVIDER", lambda: {
        "node_id": "node-test", "session_id": "session-test",
    })
    service = rt.RecorderRuntime()
    service.store = DurableRecorderStore(tmp_path / "data")
    try:
        yield service
    finally:
        service.executor.shutdown(wait=True, cancel_futures=True)
        rt.unregister_stop_target(service)


def _recovery(service):
    return service._recover_archived_batches(
        source_name="machine-private", base_url="http://private-agent:5000",
        instance_id=7, expected=1, probe=ProbeModel(sha256="a" * 64, devices={}, data_items={}),
    )


def test_actual_recovery_noop_is_measured_and_does_not_expose_source_or_url(registry, recorder):
    assert _recovery(recorder) == 1
    record = _only(registry)
    assert record["outcome"] == "completed"
    assert record["context"] == {
        "source_alias": obs.source_alias("machine-private"), "agent_instance_id": 7,
        "next_sequence_before": 1, "node_id": "node-test", "session_id": "session-test",
    }
    assert record["progress"] == {"next_sequence_after": 1, "advanced_sequences": 0}
    assert "private-agent" not in json.dumps(record)
    assert "machine-private" not in json.dumps(record)


def test_actual_recovery_preserves_checkpoint_result_and_observes_progress(registry, recorder):
    xml = (
        '<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7">'
        '<Header instanceId="7" firstSequence="1" lastSequence="1" nextSequence="2"/>'
        '<Streams><DeviceStream name="machine" uuid="machine-1">'
        '<ComponentStream component="Linear" componentId="x"><Samples>'
        '<Position dataItemId="x" sequence="1" timestamp="2026-08-25T00:00:00Z">1</Position>'
        '</Samples></ComponentStream></DeviceStream></Streams></MTConnectStreams>'
    )
    batch = rt.parse_streams(xml, source_name="machine-private", probe=None,
                             max_observations=100, max_sequence_span=100)
    recorder.store.store_raw_batch(source_name="machine-private", requested_from=1,
                                  xml_text=xml, batch=batch)
    recorder.checkpoints["machine-private"] = SourceCheckpoint(
        source_name="machine-private", base_url="http://private-agent:5000", machine_id="machine-1",
        agent_instance_id=7, next_sequence=1, probe_sha256="a" * 64,
    )
    assert _recovery(recorder) == 2
    assert recorder.checkpoints["machine-private"].next_sequence == 2
    assert _only(registry)["progress"] == {"next_sequence_after": 2, "advanced_sequences": 1}
    assert list(recorder.store.observation_root.rglob("*.ndjson"))


def test_actual_recovery_failure_preserves_exception_and_marks_failed(registry, recorder, monkeypatch):
    error = OSError("private path")

    def fail(**_kwargs):
        raise error

    monkeypatch.setattr(recorder.store, "iter_raw_batches", fail)
    with pytest.raises(OSError) as raised:
        _recovery(recorder)
    assert raised.value is error
    assert _only(registry)["outcome"] == "failed"


@pytest.mark.parametrize("counts", [(0, 0, 0, 0, 0), (3, 2, 2, 1, 1)])
def test_actual_incremental_reconcile_span_covers_result_not_delivery(registry, monkeypatch, counts):
    expected = RecorderReconcileResult(*counts, quarantine=QuarantineSummary(total=1))
    calls = []

    def reconcile(_self):
        calls.append("reconcile")
        assert _only(registry)["outcome"] == "incomplete"
        return expected

    monkeypatch.setattr(RecorderArchiveReconciler, "reconcile", reconcile)
    service = object.__new__(IncrementalRecorderArchiveReconciler)
    service.target = RecorderPublicationTarget("session-test", "group-test", "node-test")
    assert service.reconcile() is expected
    assert calls == ["reconcile"]
    record = _only(registry)
    assert record["operation"] == "publication-reconcile"
    assert record["context"] == {"session_id": "session-test", "storage_group": "group-test",
                                  "node_id": "node-test"}
    assert record["progress"] == dict(zip(
        ["scanned_batches", "eligible_batches", "publication_chunks", "enqueued", "already_enqueued", "quarantined"],
        [*counts, 1], strict=True,
    ))


def test_actual_incremental_reconcile_exception_is_unchanged(registry, monkeypatch):
    error = ValueError("business failure")

    def reconcile(_self):
        raise error

    monkeypatch.setattr(RecorderArchiveReconciler, "reconcile", reconcile)
    service = object.__new__(IncrementalRecorderArchiveReconciler)
    service.target = RecorderPublicationTarget("session-test", "group-test", "node-test")
    with pytest.raises(ValueError) as raised:
        service.reconcile()
    assert raised.value is error
    assert _only(registry)["outcome"] == "failed"


def test_existing_heartbeat_carries_observations_without_extra_writes(registry, recorder, monkeypatch):
    writes = []
    monkeypatch.setattr(rt, "_write_json_atomic", lambda path, value: writes.append((path, value)))
    monkeypatch.setattr(rt.time, "monotonic", lambda: 1_000)
    token = registry.begin("recorder-recovery", CONTEXT)
    registry.finish(token, progress=PROGRESS)
    recorder.publish_status(force=True)
    recorder.publish_status()
    assert len(writes) == 1
    assert writes[0][0] == rt.STATUS_FILE
    payload = writes[0][1]
    assert payload["schema"] == "fcp.mtconnect_recorder.status.v2"
    assert payload["acceptance_observability"]["operations"][0]["operation_id"] == token[1]
    assert payload["acceptance_observability"]["operations"][0]["outcome"] == "completed"
