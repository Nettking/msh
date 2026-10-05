"""Current stage is owned diagnostics, never inferred acceptance completion."""
from __future__ import annotations

import asyncio
import json
import shutil
import threading
from types import SimpleNamespace

import pytest

from catalog.federation.incremental_recorder_publication import (
    IncrementalRecorderArchiveReconciler,
)
from catalog.federation.recorder_delivery import RecorderDeliveryRunResult
from catalog.federation.recorder_publication import (
    RecorderFederationDeliveryWorker,
    RecorderReconcileResult,
    observed_reconciliation,
)
from catalog.federation.tests.test_recorder_publication import (
    SAMPLE_XML,
    RecordingClient,
    _build_reconciler,
    _store_sample,
    _write_checkpoint,
)
from catalog.mtconnect_recorder import acceptance_observability as obs
from catalog.mtconnect_recorder import federation_node as node_module
from catalog.mtconnect_recorder import managed_service as managed
from catalog.mtconnect_recorder import runtime as capture
from catalog.mtconnect_recorder.federation_node import RecorderFederationNode
from catalog.mtconnect_recorder.tests.test_federation_node import _authority, _status

# These are bound identity fixtures, not observations of the physical runtime.
PROCESS = {"candidate_sha": "78ee3c57695753b3d254ea113017ba77bb57dd2b",
           "runtime_generation": "738ed67cfc7642bbaa96041530ccd3a7",
           "supervisor_generation": None, "pid": 123}
WORKER = "29963446e1024a8abc597e9833f2249d"
SESSION, NODE = "session-1", "node-recorder"


@pytest.fixture
def current(monkeypatch):
    process = dict(PROCESS)
    monkeypatch.setattr(obs, "process_provenance", lambda: dict(process))
    monkeypatch.setattr(managed, "process_provenance", lambda: dict(process))
    value = obs.PublicationCycleObservation()
    value.new_worker()
    token = begin(value, "a" * 32)
    assert token is not None
    return value, token, process


def snapshot(value):
    return value.snapshot(expected_session=SESSION, expected_node=NODE)


def begin(value, cycle_id):
    return value.begin(cycle_id, session_id=SESSION, node_id=NODE,
                       worker_generation=value._worker_generation)


def test_running_is_a_real_transition_with_unknown_progress_no_terminal_or_duration(current):
    value, token, _ = current
    value.transition(token, "delivery", cycle_stage="reconcile", storage_group="telemetry",
                     authority_node_id="node-owner")
    row = snapshot(value)
    assert row["available"] is True and row["outcome"] == "running"
    assert row["stage"] == "delivery" and row["cycle_stage"] == "reconcile"
    assert row["ended_at_utc"] is row["ended_at_monotonic_ns"] is None
    assert all(count is None for count in row["progress"].values())
    assert "duration_ns" not in row and row["acceptance_completion_evidence"] is False
    assert 0 < row["started_at_monotonic_ns"] <= row["transition_at_monotonic_ns"] <= row["observed_at_monotonic_ns"]
    assert row["context"]["storage_group"] == "telemetry"
    row["context"]["storage_group"] = "forged"
    assert snapshot(value)["context"]["storage_group"] == "telemetry"


@pytest.mark.parametrize("outcome", ["completed", "waiting", "failed", "interrupted"])
def test_terminal_records_actual_boundary_and_never_reopens(current, outcome):
    value, token, _ = current
    error = RuntimeError("http://user:password@private/?token=secret") if outcome == "failed" else None
    value.transition(token, "delivery", cycle_stage="delivery", progress={"committed": 2})
    if error:
        value.failure(token, error)
    value.finish(token, outcome=outcome, error=error)
    row = snapshot(value)
    assert row["outcome"] == outcome and row["ended_at_utc"] is not None
    assert row["ended_at_monotonic_ns"] >= row["started_at_monotonic_ns"]
    assert row["progress"]["committed"] == 2
    value.transition(token, "status")
    assert snapshot(value)["stage"] == "delivery"
    assert "password" not in json.dumps(row) and "secret" not in json.dumps(row)
    assert row["acceptance_completion_evidence"] is False


def test_original_failure_stays_visible_during_real_recovery_and_cancel(current):
    value, token, _ = current
    value.transition(token, "delivery", cycle_stage="reconcile")
    value.failure(token, OSError("private path"))
    value.transition(token, "retry-pending-read")
    value.finish(token, outcome="interrupted", error=asyncio.CancelledError())
    row = snapshot(value)
    assert row["error_type"] == "OSError" and row["failure_cycle_stage"] == "reconcile"
    assert row["failure_stage"] == "delivery" and row["stage"] == "retry-pending-read"
    assert row["terminal_error_type"] == "CancelledError"
    assert row["terminal_scope"] == "cycle-awaiting-coroutine"  # Not thread-drain evidence.


def test_missed_update_is_unavailable_until_new_owned_cycle(current):
    value, token, _ = current
    value._lock.acquire()
    try:
        value.transition(token, "delivery", cycle_stage="reconcile")
    finally:
        value._lock.release()
    value.transition(token, "delivery", cycle_stage="delivery")
    value.finish(token, outcome="completed")
    assert snapshot(value)["available"] is False
    new = begin(value, "b" * 32)
    value.transition(new, "status")
    assert snapshot(value)["available"] is True and snapshot(value)["dropped_updates"] == 1


def test_old_callbacks_and_terminal_cannot_modify_new_cycle_or_worker(current):
    value, old, _ = current
    new = begin(value, "b" * 32)
    value.transition(new, "status")
    value.finish(old, outcome="failed", error=RuntimeError())
    value.observation_failed(old)
    assert snapshot(value)["cycle_id"] == "b" * 32 and snapshot(value)["available"] is True
    generation = snapshot(value)["publication_loop_generation"]
    value.new_worker()
    next_token = begin(value, "c" * 32)
    value.transition(new, "pending-read")
    assert snapshot(value)["publication_loop_generation"] != generation
    assert snapshot(value)["cycle_id"] == next_token[1]
    assert value.begin("d" * 32, session_id=SESSION, node_id=NODE, worker_generation=generation) is None
    assert snapshot(value)["cycle_id"] == next_token[1]


def test_failed_replacement_admission_permanently_fences_previous_coroutine(current):
    value, old, _ = current
    old_generation = value._worker_generation
    value._lock.acquire()
    try:
        assert value.new_worker() is None
    finally:
        value._lock.release()
    assert value.begin("b" * 32, session_id=SESSION, node_id=NODE,
                       worker_generation=old_generation) is None
    value.transition(old, "status")
    value.finish(old, outcome="completed")
    assert snapshot(value)["available"] is False
    assert value.new_worker() != old_generation
    new = begin(value, "c" * 32)
    value.transition(new, "status")
    assert snapshot(value)["available"] is True


@pytest.mark.parametrize("field,new", [("candidate_sha", "c" * 40), ("runtime_generation", "d" * 32),
                                      ("pid", 321), ("supervisor_generation", "e" * 32)])
def test_runtime_change_requires_new_actual_worker_not_just_fresh_snapshot(current, field, new):
    value, _token, process = current
    process[field] = new
    assert snapshot(value)["available"] is False
    assert begin(value, "c" * 32) is None
    value.new_worker()
    assert begin(value, "d" * 32) is not None
    assert snapshot(value)["available"] is True


@pytest.mark.parametrize("field,new", [("candidate_sha", 1), ("runtime_generation", 1),
                                      ("pid", True), ("pid", 0), ("supervisor_generation", False)])
def test_unknown_or_coerced_provenance_never_passes(current, field, new):
    value, _token, process = current
    process[field] = new
    assert snapshot(value)["available"] is False
    value.new_worker()
    assert begin(value, "b" * 32) is None


@pytest.mark.parametrize("stage,cycle_stage,progress", [
    ("private-url/http://x", None, None), ("delivery", "unknown", None),
    ("delivery", "delivery", {"committed": True}), ("delivery", "delivery", {"committed": -1}),
    ("delivery", "delivery", {"committed": obs.MAX_COUNTER + 1}),
    ("delivery", "delivery", {"committed": "0"}),
])
def test_bad_stage_or_counter_causes_explicit_loss_not_coercion(current, stage, cycle_stage, progress):
    value, token, _ = current
    value.transition(token, stage, cycle_stage=cycle_stage, progress=progress)
    assert snapshot(value)["available"] is False


@pytest.mark.parametrize("field,stamp", [("transition_at_monotonic_ns", 1 << 120),
                                        ("transition_at_utc", "2099-01-01T00:00:00Z"),
                                        ("started_at_utc", "2026-01-01T00:00:00")])
def test_future_or_ambiguous_clock_is_unavailable(current, field, stamp):
    value, _token, _ = current
    value._record[field] = stamp
    assert snapshot(value)["available"] is False


def test_unknown_private_fields_are_not_exported_and_output_is_bounded(current):
    value, token, _ = current
    value.transition(token, "delivery", cycle_stage="delivery",
                     progress={"committed": 0, "secret": "private"})
    encoded = json.dumps(snapshot(value))
    assert len(encoded.encode()) < 4096 and "secret" not in encoded and "private" not in encoded
    assert value.snapshot(expected_session="foreign", expected_node=NODE)["available"] is False


@pytest.mark.parametrize("gate,mode", [(stage, "cancel") for stage in (
    "connect", "status", "reconcile", "delivery", "jsonl", "pending-read"
)] + [("delivery", "retry"), ("delivery", "fatal")])
def test_real_node_worker_and_heartbeat_expose_only_actual_current_stage(current, tmp_path, monkeypatch, gate, mode):
    _unused, _token, _process = current

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        thread_release = threading.Event()
        loop = asyncio.get_running_loop()

        async def async_gate(stage):
            if gate == stage:
                entered.set()
                await release.wait()

        def sync_gate(stage):
            if gate == stage:
                loop.call_soon_threadsafe(entered.set)
                assert thread_release.wait(5)

        async def coordinator_status():
            await async_gate("status")
            return _status(_authority())

        async def connect(_state):
            await async_gate("connect")

        runtime = SimpleNamespace(_ensure_connected=connect,
                                  _connected_client=lambda: SimpleNamespace(coordinator_status=coordinator_status))
        node = RecorderFederationNode(data_directory=tmp_path, display_name="Fixture", source_names=(),
            service=SimpleNamespace(relay_runtime=runtime), jsonl_publisher=object(), publication_poll_seconds=60)
        node._set_snapshot(status="connected", session_id=SESSION, node_id=NODE)
        state = SimpleNamespace(binding=SimpleNamespace(internal_session_id=SESSION, device_id=NODE))

        async def noop(*_args, **_kwargs):
            return None

        def reconcile():
            sync_gate("reconcile")
            return RecorderReconcileResult(3, 2, 2, 1, 1)

        def pending():
            sync_gate("pending-read")
            return ()

        async def deliver(**_kwargs):
            await async_gate("delivery")
            if mode == "retry":
                raise OSError("private user:password URL")
            if mode == "fatal":
                raise AssertionError("private user:password URL")
            return RecorderDeliveryRunResult(attempted=1, committed=1, pending=0)

        async def jsonl(*_args, **_kwargs):
            await async_gate("jsonl")
            return SimpleNamespace(published_chunks=0)

        outbox = SimpleNamespace(pending=pending, retired_summary=lambda **_kwargs: SimpleNamespace(total=0))
        worker = RecorderFederationDeliveryWorker(reconciler=SimpleNamespace(
            checkpoint_file=tmp_path / "absent-checkpoint", reconcile=reconcile),
            queue=SimpleNamespace(startup_probe_pending=False, session_id=SESSION, destination_id="telemetry",
                                  run_once=deliver, outbox=outbox))
        node._announce_connected = noop
        node._worker = lambda **_kwargs: (worker, outbox)
        node._publish_jsonl_once = jsonl
        monkeypatch.setattr(node_module, "RelayRecorderStorageClient", lambda *_args, **_kwargs:
                            SimpleNamespace(start=noop, close=noop))
        task = asyncio.create_task(node._publication_loop(state))
        node._publication_future = task
        companion = managed.ManagedRecorderFederationRuntime(data_directory=tmp_path)
        companion._node = node
        companion._health_publication = task
        companion._health_publication_generation = WORKER
        capture.set_federation_status_provider(companion.federation_snapshot)
        try:
            await asyncio.wait_for(entered.wait(), 3)
            row = capture._federation_status()["publication_cycle"]
            assert row["available"] is True and row["outcome"] == "running"
            assert row["provenance"] == PROCESS and row["worker_generation"] == WORKER
            assert row["context"]["session_id"] == SESSION and row["context"]["node_id"] == NODE
            assert row["stage"] == ("delivery" if gate == "reconcile" else gate)
            assert row["cycle_stage"] == ("reconcile" if gate == "reconcile" else "delivery" if gate == "delivery" else None)
            if gate in {"delivery", "jsonl", "pending-read"}:
                assert row["progress"]["scanned_batches"] == 3
            assert row["ended_at_utc"] is None and row["acceptance_completion_evidence"] is False
            if mode != "cancel":
                release.set()

                async def failed():
                    while node.publication_cycle_snapshot(session_id=SESSION, node_id=NODE).get("outcome") != "failed":
                        await asyncio.sleep(0.001)

                await asyncio.wait_for(failed(), 3)
                row = node.publication_cycle_snapshot(session_id=SESSION, node_id=NODE)
                assert row["failure_cycle_stage"] == "delivery" and row["outcome"] == "failed"
                assert row["error_type"] == ("OSError" if mode == "retry" else "AssertionError")
                assert "password" not in json.dumps(row)
                if mode == "fatal":
                    with pytest.raises(AssertionError, match="private user:password URL"):
                        await task
                else:
                    assert not task.done()  # Existing bounded retry is still running.
        finally:
            thread_release.set()
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            capture.set_federation_status_provider(None)
        terminal = node.publication_cycle_snapshot(session_id=SESSION, node_id=NODE)
        assert terminal["outcome"] == ("interrupted" if mode == "cancel" else "failed")
        if mode == "cancel":
            assert terminal["terminal_error_type"] == "CancelledError"

    asyncio.run(scenario())


def test_diagnostic_sink_error_does_not_change_actual_worker_success(current, tmp_path):
    value, token, _process = current
    calls = []

    async def scenario():
        async def deliver(**_kwargs):
            calls.append("delivery")
            return RecorderDeliveryRunResult(1, 1, 0)

        worker = RecorderFederationDeliveryWorker(reconciler=SimpleNamespace(
            checkpoint_file=tmp_path / "absent", reconcile=lambda: RecorderReconcileResult(1, 1, 1, 1, 0)),
            queue=SimpleNamespace(startup_probe_pending=False, session_id=SESSION, destination_id="telemetry",
                run_once=deliver, outbox=SimpleNamespace(retired_summary=lambda **_kw: SimpleNamespace(total=0))))

        def fail(*_args):
            raise RuntimeError("broken diagnostic sink")

        worker.cycle_stage_observer = fail
        worker.cycle_stage_observation_failed = lambda: value.observation_failed(token)
        result = await worker.run_cycle()
        assert result.delivery.committed == 1 and calls == ["delivery"]
        assert worker.last_cycle_stage is None and snapshot(value)["available"] is False

    asyncio.run(scenario())


@pytest.mark.parametrize("change", ["node", "future", "worker_generation", "process", "context", "error", "type"])
def test_managed_projection_refuses_changed_owner_or_bad_diagnostics_without_hiding_federation(
    current, tmp_path, change,
):
    from concurrent.futures import Future

    value, _token, process = current
    node = object.__new__(RecorderFederationNode)
    node._lock = threading.RLock()
    node._snapshot = node_module.RecorderFederationSnapshot(status="connected", session_id=SESSION, node_id=NODE)
    node._publication_diagnostics = value
    node._publication_future = Future()
    companion = managed.ManagedRecorderFederationRuntime(data_directory=tmp_path)
    companion._node = node
    companion._health_publication = node._publication_future
    companion._health_publication_generation = WORKER
    original = node.publication_cycle_snapshot

    def getter(**kwargs):
        row = original(**kwargs)
        if change == "node":
            companion._node = object()
        elif change == "future":
            node._publication_future = Future()
        elif change == "worker_generation":
            companion._health_publication_generation = "f" * 32
        elif change == "process":
            companion._health_process = {**process, "pid": 456}
        elif change == "context":
            node._snapshot = node_module.RecorderFederationSnapshot(status="connected", session_id="foreign", node_id=NODE)
            return original(**kwargs)
        elif change == "type":
            return "not a diagnostic snapshot"
        else:
            raise RuntimeError("private credential")
        return row

    node.publication_cycle_snapshot = getter
    exported = companion.federation_snapshot()
    assert exported["status"] == "connected" and exported["publication_cycle"]["available"] is False
    assert "credential" not in json.dumps(exported)


def test_overlapping_worker_calls_keep_each_captured_observer_token(current, tmp_path):
    value, first_token, _process = current

    async def scenario():
        entered = [asyncio.Event(), asyncio.Event()]
        release = [asyncio.Event(), asyncio.Event()]
        calls = []

        async def deliver(**_kwargs):
            index = len(calls)
            calls.append(index)
            entered[index].set()
            await release[index].wait()
            return RecorderDeliveryRunResult(1, 1, 0)

        worker = RecorderFederationDeliveryWorker(reconciler=SimpleNamespace(checkpoint_file=tmp_path / "absent",
            reconcile=lambda: RecorderReconcileResult(1, 1, 1, 1, 0)), queue=SimpleNamespace(
                startup_probe_pending=False, session_id=SESSION, destination_id="telemetry", run_once=deliver,
                outbox=SimpleNamespace(retired_summary=lambda **_kw: SimpleNamespace(total=0))))

        def attach(token):
            worker.cycle_stage_observer = lambda stage, progress: value.transition(
                token, "delivery", cycle_stage=stage, progress=progress)
            worker.cycle_stage_observation_failed = lambda: value.observation_failed(token)

        attach(first_token)
        first = asyncio.create_task(worker.run_cycle())
        await asyncio.wait_for(entered[0].wait(), 3)
        new_token = begin(value, "b" * 32)
        attach(new_token)
        second = asyncio.create_task(worker.run_cycle())
        try:
            await asyncio.wait_for(entered[1].wait(), 3)
            release[0].set()
            await asyncio.wait_for(first, 3)
            assert snapshot(value)["cycle_id"] == "b" * 32
            assert snapshot(value)["cycle_stage"] == "delivery"  # Not the old call's retirement/None.
        finally:
            release[0].set()
            release[1].set()
            await asyncio.gather(first, second)

    asyncio.run(scenario())


def incremental_fixture(tmp_path):
    store, checkpoint_file, outbox, queue, legacy = _build_reconciler(tmp_path, client=RecordingClient())
    reconciler = IncrementalRecorderArchiveReconciler(
        store=store, checkpoint_file=checkpoint_file, queue=queue, target=legacy.target,
        max_content_bytes=legacy.max_content_bytes,
    )
    probe, _batch, stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint_file, probe_sha256=probe.sha256, next_sequence=13)
    return reconciler, outbox, stored


def test_actual_optional_reconcile_reports_scan_write_marker_and_enqueue_without_rewriting_raw(tmp_path):
    reconciler, outbox, stored = incremental_fixture(tmp_path)
    raw = stored.raw_path.read_bytes()
    manifest = stored.raw_path.with_suffix('.manifest.json').read_bytes()
    reports = []
    result = reconciler.reconcile_with_diagnostics(observer=reports.append)
    assert result.enqueued == 1 and len(outbox.pending()) == 1
    assert stored.raw_path.read_bytes() == raw
    assert stored.raw_path.with_suffix('.manifest.json').read_bytes() == manifest
    stages = [row['stage'] for row in reports]
    assert stages[:5] == ['legacy-scan', 'legacy-frontier-write', 'legacy-frontier-write',
                          'legacy-marker', 'legacy-marker-published']
    assert stages[5:] == ['pending-discovery', 'pending-validation', 'derived-read',
                         'outbox-enqueue', 'pending-retirement']
    writes = [row['progress'] for row in reports if row['stage'] == 'legacy-frontier-write']
    assert writes == [{'refs_total': 1, 'issues': 0, 'write_attempts': 0, 'writes_completed': 0},
                      {'refs_total': 1, 'issues': 0, 'write_attempts': 1, 'writes_completed': 1}]
    assert all(row['source_alias'] == obs.source_alias('Mazak') for row in reports)
    assert 'Mazak' not in json.dumps(reports)
    reports.clear()
    reconciler.reconcile_with_diagnostics(observer=reports.append)
    assert [row['stage'] for row in reports] == ['pending-discovery']  # No new legacy scan.
    assert reconciler._diagnostics.observer is None


@pytest.mark.parametrize('gate,expected', [('scan_raw_batches', 'legacy-scan'),
    ('mark_pending', 'legacy-frontier-write'), ('_read_observations', 'derived-read'),
    ('enqueue', 'outbox-enqueue')])
def test_actual_worker_distinguishes_current_reconcile_subphase_and_old_cancelled_thread_is_fenced(
    current, tmp_path, monkeypatch, gate, expected,
):
    value, token, _process = current
    reconciler, _outbox, _stored = incremental_fixture(tmp_path)

    async def scenario():
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        owner = reconciler.store if gate == 'scan_raw_batches' else reconciler.frontier if gate == 'mark_pending' else reconciler.queue if gate == 'enqueue' else reconciler
        original = getattr(owner, gate)

        def blocked(*args, **kwargs):
            entered.set()
            assert release.wait(5)
            try:
                return original(*args, **kwargs)
            finally:
                finished.set()

        monkeypatch.setattr(owner, gate, blocked)
        worker = RecorderFederationDeliveryWorker(reconciler=reconciler, queue=reconciler.queue)
        node = object.__new__(RecorderFederationNode)
        node._publication_diagnostics = value
        node._attach_cycle_diagnostics(worker, token)
        task = asyncio.create_task(worker.run_cycle())
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            row = snapshot(value)
            assert row['available'] is True and row['outcome'] == 'running'
            assert row['cycle_stage'] == 'reconcile'
            assert row['reconciliation']['stage'] == expected
            assert row['reconciliation']['source_alias'] == obs.source_alias('Mazak')
            if gate == 'mark_pending':
                assert row['reconciliation']['progress']['refs_total'] == 1
                assert row['reconciliation']['progress']['writes_completed'] == 0
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            value.finish(token, outcome='interrupted', error=asyncio.CancelledError())
            interrupted = snapshot(value)
            assert interrupted['ended_at_utc'] is not None and not finished.is_set()
            assert interrupted['acceptance_completion_evidence'] is False
            new = begin(value, 'd' * 32)
            value.transition(new, 'status')
            release.set()
            assert await asyncio.to_thread(finished.wait, 3)
            # The thread still performs normal durable work after cancellation,
            # but its captured callback cannot regain current cycle ownership.
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert snapshot(value)['cycle_id'] == 'd' * 32
        assert snapshot(value)['stage'] == 'status'
        assert snapshot(value)['reconciliation'] is None

    asyncio.run(scenario())


def test_legacy_frontier_progress_is_rate_bounded_and_exact_on_success_or_issue(tmp_path, monkeypatch):
    from catalog.mtconnect_recorder.model import MtconnectProtocolError

    reconciler, _outbox, _stored = incremental_fixture(tmp_path)
    scan = reconciler.store.scan_raw_batches(source_name='Mazak', instance_id=77)
    ref = scan.refs[0]
    monkeypatch.setattr(reconciler.store, 'scan_raw_batches', lambda **_kwargs:
                        SimpleNamespace(refs=(ref,) * 130, issues=()))
    attempts = []

    def mark(**_kwargs):
        attempts.append(1)
        if len(attempts) == 70:
            raise MtconnectProtocolError('isolated existing migration issue')

    monkeypatch.setattr(reconciler.frontier, 'mark_pending', mark)
    reports = []
    reconciler._diagnostics.observer = (reports.append, None)
    checkpoint = SimpleNamespace(agent_instance_id=77)
    reconciler._seed_legacy_archive(source_name='Mazak', archive_source_name='Mazak', checkpoint=checkpoint)
    writes = [row['progress'] for row in reports if row['stage'] == 'legacy-frontier-write']
    assert [row['write_attempts'] for row in writes] == [0, 64, 128, 130]
    assert [row['writes_completed'] for row in writes] == [0, 64, 127, 129]
    assert writes[-1]['refs_total'] == 130 and writes[-1]['issues'] == 1
    assert reconciler.frontier.migration_state(source_name='Mazak', instance_id=77) == 'blocked'
    assert reports[-1]['stage'] == 'legacy-marker-published'


def test_optional_reconcile_observer_failure_is_loss_not_archive_failure(current, tmp_path):
    value, token, _process = current
    reconciler, outbox, _stored = incremental_fixture(tmp_path)
    calls = []

    def broken(_detail):
        calls.append(1)
        raise RuntimeError('private URL or token')

    result = reconciler.reconcile_with_diagnostics(
        observer=broken, observation_failed=lambda: value.observation_failed(token),
    )
    assert result.enqueued == 1 and len(outbox.pending()) == 1 and calls
    assert snapshot(value)['available'] is False
    assert reconciler._diagnostics.observer is None


@pytest.mark.parametrize('change', ['alias', 'stage', 'counter', 'type', 'scope'])
def test_reconcile_detail_types_and_scope_never_coerce_or_claim_availability(current, change):
    value, token, _process = current
    detail = {'stage': 'legacy-scan', 'source_alias': 'a' * 64, 'progress': {}}
    cycle_stage = 'reconcile'
    if change == 'alias':
        detail['source_alias'] = 'private-source'
    elif change == 'stage':
        detail['stage'] = 'secret URL'
    elif change == 'counter':
        detail['progress'] = {'writes_completed': True}
    elif change == 'type':
        detail['progress'] = None
    else:
        cycle_stage = 'delivery'
    value.transition(token, 'delivery', cycle_stage=cycle_stage, reconciliation=detail)
    assert snapshot(value)['available'] is False


def test_exact_failed_reconciliation_stage_survives_normal_retry_and_cancellation(current):
    value, token, _process = current
    detail = {'stage': 'legacy-frontier-write', 'source_alias': 'a' * 64,
              'progress': {'refs_total': 130, 'writes_completed': 64, 'write_attempts': 64}}
    value.transition(token, 'delivery', cycle_stage='reconcile', reconciliation=detail)
    value.failure(token, OSError('private-data-path'))
    value.transition(token, 'retry-pending-read')
    value.finish(token, outcome='interrupted', error=asyncio.CancelledError())
    result = snapshot(value)
    assert result['reconciliation'] is None and result['failure_reconciliation'] == detail
    assert result['error_type'] == 'OSError' and result['terminal_error_type'] == 'CancelledError'
    assert 'private-data-path' not in json.dumps(result)


def test_shared_dispatcher_without_optional_observer_preserves_original_reconcile_only():
    calls = []

    class OriginalClient:
        @property
        def reconcile_with_diagnostics(self):
            raise AssertionError('default clients must not inspect the optional diagnostic entry')

        def reconcile(self):
            calls.append(1)
            return 'actual-original-result'

    assert observed_reconciliation(OriginalClient()) == 'actual-original-result'
    assert calls == [1]


@pytest.mark.parametrize('gate,expected,mode', [('scan_raw_batches', 'legacy-scan', 'cancel'),
    ('mark_pending', 'legacy-frontier-write', 'cancel'), ('_read_observations', 'derived-read', 'cancel'),
    ('_read_observations', 'derived-read', 'failure')])
def test_actual_node_local_retry_dispatches_current_cycle_not_previous_worker_callback(
    current, tmp_path, monkeypatch, gate, expected, mode,
):
    value, _token, _process = current
    reconciler, outbox, stored = incremental_fixture(tmp_path)

    async def scenario():
        entered, release = threading.Event(), threading.Event()
        connects = []
        local = [False]

        async def connect(_state):
            connects.append(1)
            if len(connects) > 1:
                local[0] = True
                raise OSError('isolated genuine-shaped transport failure')

        async def noop(*_args, **_kwargs):
            return None

        async def status():
            return _status(_authority())

        runtime = SimpleNamespace(_ensure_connected=connect,
                                  _connected_client=lambda: SimpleNamespace(coordinator_status=status))
        node = RecorderFederationNode(data_directory=tmp_path, display_name='Fixture', source_names=(),
            service=SimpleNamespace(relay_runtime=runtime), jsonl_publisher=object(), publication_poll_seconds=1)
        node._publication_diagnostics = value
        node._set_snapshot(status='connected', session_id=SESSION, node_id=NODE)
        node.publication_poll_seconds = 0  # Accelerated isolated fixture, no timed acceptance credit.
        node._announce_connected = noop
        worker = RecorderFederationDeliveryWorker(reconciler=reconciler, queue=reconciler.queue)
        node._worker = lambda **_kwargs: (worker, outbox)
        state = SimpleNamespace(binding=SimpleNamespace(internal_session_id=SESSION, device_id=NODE))
        owner = reconciler.store if gate == 'scan_raw_batches' else reconciler.frontier if gate == 'mark_pending' else reconciler
        original = getattr(owner, gate)

        def blocked(*args, **kwargs):
            if local[0]:
                entered.set()
                assert release.wait(5)
                if mode == 'failure':
                    raise FileNotFoundError('private recovery data path')
            return original(*args, **kwargs)

        monkeypatch.setattr(owner, gate, blocked)

        async def jsonl(*_args, **_kwargs):
            # Recreate only this tiny isolated archive root with the exact raw
            # bytes. The real dev/inode migration predicate legitimately pays
            # a new compatibility scan; no production marker is edited.
            archive_root = stored.raw_path.parent.parent
            retained = tmp_path/'retained-fixture-root'
            archive_root.rename(retained)
            shutil.copytree(retained, archive_root)
            return SimpleNamespace(published_chunks=0)

        node._publish_jsonl_once = jsonl
        monkeypatch.setattr(node_module, 'RelayRecorderStorageClient', lambda *_args, **_kwargs:
                            SimpleNamespace(start=noop, close=noop))
        task = asyncio.create_task(node._publication_loop(state))
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            row = snapshot(value)
            assert row['stage'] == 'local-reconcile' and row['cycle_stage'] == 'reconcile'
            assert row['reconciliation']['stage'] == expected
            assert row['reconciliation']['source_alias'] == obs.source_alias('Mazak')
            assert row['cycle_sequence'] == 2 and row['error_type'] == 'OSError'
            if mode == 'failure':
                release.set()
                async def failed():
                    while snapshot(value).get('outcome') != 'failed':
                        await asyncio.sleep(0.001)
                await asyncio.wait_for(failed(), 3)
                row = snapshot(value)
                assert row['error_type'] == 'OSError' and row['failure_stage'] == 'connect'
                assert row['last_failure']['error_type'] == 'FileNotFoundError'
                assert row['last_failure']['stage'] == 'local-reconcile'
                assert row['last_failure']['reconciliation']['stage'] == expected
                assert row['terminal_error_type'] == 'FileNotFoundError'
                assert 'private recovery data path' not in json.dumps(row)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            if mode == 'cancel':
                assert snapshot(value)['outcome'] == 'interrupted' and not release.is_set()
            new = begin(value, 'e' * 32)
            value.transition(new, 'status')
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())  # Also joins the real continuing to_thread work.
    row = snapshot(value)
    assert row['cycle_id'] == 'e' * 32 and row['stage'] == 'status'
    assert row['reconciliation'] is None and row['available'] is True
