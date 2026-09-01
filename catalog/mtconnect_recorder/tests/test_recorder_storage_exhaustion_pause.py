"""Host-storage refusal inside an admitted recorder transaction.

Admission reserves against an *estimate* of what a capture transaction will
write. A concurrent writer, another process, or an underestimate can still
leave the host with no room by the time the admitted write runs, and the
filesystem refuses it directly with ``ENOSPC``.

Before this boundary existed that refusal reached
``RecorderRuntime.capture_source``'s remote-source ``except Exception``. The
consequences were all misattribution: a healthy MTConnect Agent was recorded as
the failing party, its ``last_error`` carried a host disk message, and its
backoff doubled toward ``BACKOFF_MAX`` on every retry -- so the recorder backed
away from a source that was never at fault, while the local condition that
actually stopped capture was never published as one. That is the same
amplification shape the recorder status-I/O boundary already refuses for the
heartbeat, on the capture path.

These tests pin the reclassification and, just as importantly, its limits: only
an observable exhaustion inside an admitted transaction is reclassified, and
primary evidence is never removed to make room.
"""

from __future__ import annotations

import errno
import json
from dataclasses import replace
from pathlib import Path

import pytest

from catalog.federation.host_resources import FilesystemMeasurement
from catalog.mtconnect_recorder import _resource_pressure_impl as pressure_impl
from catalog.mtconnect_recorder import bounded_storage, publication_frontier
from catalog.mtconnect_recorder import recovery_frontier as recovery_frontier_module
from catalog.mtconnect_recorder import runtime as recorder_runtime
from catalog.mtconnect_recorder import storage as storage_module
from catalog.mtconnect_recorder.recovery_frontier import RecorderRecoveryFrontier
from catalog.mtconnect_recorder.resource_pressure import (
    STORAGE_EXHAUSTED,
    RecorderAdmissionController,
    RecorderResourcePaused,
    attach_runtime_resource_pressure,
)
from catalog.mtconnect_recorder.storage import DurableRecorderStore
from catalog.mtconnect_recorder.tests.test_recorder_resource_pressure import (
    BASE_URL,
    NOW,
    SOURCE,
    _budget,
    _close,
    _controller,
    _prepare_pending_raw,
    _probe,
    _runtime,
    _streams_xml,
    _thresholds,
)

# Enough measured headroom that admission grants. The refusal under test is the
# filesystem's, arriving after that estimate was accepted.
ADMITTED_FREE_BYTES = 90_000


def _client(sample_xml: str):
    class Client:
        def __init__(self, base_url: str, *, timeout: float) -> None:
            del base_url, timeout

        def fetch_current(self) -> str:
            return sample_xml

        def fetch_sample(self, *, from_sequence: int, count: int) -> str:
            del count, from_sequence
            return sample_xml

    return Client


def _refuse(error_number: int):
    def _raise(*_args: object, **_kwargs: object):
        raise OSError(error_number, "injected")

    return _raise


def _captured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    runtime, guard, _controller_value, state_file = _runtime(
        tmp_path,
        monkeypatch,
        free_bytes=ADMITTED_FREE_BYTES,
    )
    sample_xml = _streams_xml([1])
    monkeypatch.setattr(recorder_runtime, "MtconnectClient", _client(sample_xml))
    monkeypatch.setattr(runtime, "_load_probe", lambda **_kwargs: _probe())
    return runtime, guard, state_file


def test_exhausted_host_storage_is_not_reported_as_a_failing_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The core regression: the Agent is not blamed and is not backed off."""

    runtime, _guard, state_file = _captured(tmp_path, monkeypatch)
    try:
        monkeypatch.setattr(
            runtime.store, "store_raw_batch", _refuse(errno.ENOSPC)
        )

        source, success, error = runtime.capture_source(SOURCE, BASE_URL)

        assert source == SOURCE
        assert success is True
        assert error == ""
        status = runtime.source_status[SOURCE]
        assert status["last_error"] == ""
        admission = status["resource_admission"]
        assert admission["state"] == "paused"
        assert admission["code"] == STORAGE_EXHAUSTED
        # Backoff is reset, not doubled: a full host disk is not evidence about
        # the Agent, so the recorder must not walk away from a healthy source.
        assert runtime.backoff[SOURCE] == recorder_runtime.BACKOFF_INITIAL
        # checkpoint-last held: nothing advanced past evidence that never landed.
        assert SOURCE not in runtime.checkpoints
        assert not state_file.exists()

        runtime._harvest_capture_results()
        assert runtime.state == "degraded"
        assert runtime.last_error == ""
        assert "primary evidence is retained" in runtime.message
    finally:
        _close(runtime)


def test_checkpoint_refusal_keeps_the_raw_evidence_it_would_have_committed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """raw-first/checkpoint-last across the narrowest crash window.

    The raw batch reaches disk and the checkpoint write is then refused. The
    recorder must keep that raw evidence and leave the checkpoint behind it, so
    the next run replays the batch rather than skipping it.
    """

    runtime, _guard, state_file = _captured(tmp_path, monkeypatch)
    try:
        monkeypatch.setattr(
            pressure_impl, "_write_bytes_atomic", _refuse(errno.ENOSPC)
        )

        _source, success, error = runtime.capture_source(SOURCE, BASE_URL)

        assert success is True
        assert error == ""
        assert runtime.source_status[SOURCE]["last_error"] == ""
        assert (
            runtime.source_status[SOURCE]["resource_admission"]["code"]
            == STORAGE_EXHAUSTED
        )
        # Primary evidence survives the refusal that stopped the transaction.
        assert len(tuple(runtime.store.raw_root.rglob("*.xml.gz"))) == 1
        # No *durable* checkpoint moved past that evidence. The in-memory
        # checkpoint does lead here, and legitimately so: raw, observation and
        # normalized all landed, so the batch really was stored and only its
        # record was refused. Recovery replays from the durable checkpoint,
        # which is still behind, and the content-addressed batch is idempotent.
        assert not state_file.exists()
        pending = json.loads(
            next(runtime.store.raw_root.rglob(".recovery-frontier.json")).read_text(
                encoding="utf-8"
            )
        )
        assert pending["state"] == "pending"
    finally:
        _close(runtime)


def test_a_permission_failure_keeps_its_ordinary_source_error_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reclassification is narrow on purpose.

    Only an out-of-room condition is local pressure. A permission or I/O fault
    is a real failure and must keep surfacing as one, or this boundary would
    become a way to present broken storage as a healthy pause.
    """

    runtime, _guard, _state_file = _captured(tmp_path, monkeypatch)
    try:
        monkeypatch.setattr(
            runtime.store, "store_raw_batch", _refuse(errno.EACCES)
        )

        _source, success, error = runtime.capture_source(SOURCE, BASE_URL)

        assert success is False
        assert "PermissionError" in error or "OSError" in error
        status = runtime.source_status[SOURCE]
        assert status["last_error"] == error
        assert "resource_admission" not in status
        assert runtime.backoff[SOURCE] > recorder_runtime.BACKOFF_INITIAL
    finally:
        _close(runtime)


def test_an_unmeasurable_resource_pauses_without_inventing_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed measurement is reported as one; it is never blamed on the Agent.

    The refusal itself is first-hand evidence that the host had no room, so it
    stays a local pause even when the follow-up measurement of that resource
    fails. What the pause must not do is invent capacity: the shared controller
    represents an unobtainable measurement as an explicitly unavailable one, so
    the published state carries ``measurement_unavailable`` and no free-space
    numbers rather than a healthy-looking figure.

    The measurement is broken at the controller's ``measurer`` -- the seam the
    host measurement actually enters through. ``assessment`` itself cannot raise
    in production: ``ProcessResourceAdmission`` converts a measurement failure
    into an unavailable measurement rather than an exception, so a test that
    made *that* raise would be pinning behavior the recorder never reaches.
    """

    runtime, guard, _state_file = _captured(tmp_path, monkeypatch)
    admitted = {"granted": False}
    measurer = guard.controller.measurer

    def _unmeasurable_after_admission(path: object):
        # Admission has to succeed first, or the refusal under test never runs.
        if admitted["granted"]:
            raise OSError(errno.EIO, "cannot stat")
        return measurer(path)

    def _refuse_once(*_args: object, **_kwargs: object):
        admitted["granted"] = True
        raise OSError(errno.ENOSPC, "injected")

    try:
        monkeypatch.setattr(guard.controller, "measurer", _unmeasurable_after_admission)
        monkeypatch.setattr(runtime.store, "store_raw_batch", _refuse_once)

        _source, success, error = runtime.capture_source(SOURCE, BASE_URL)

        assert success is True
        assert error == ""
        status = runtime.source_status[SOURCE]
        assert status["last_error"] == ""
        admission = status["resource_admission"]
        assert admission["code"] == STORAGE_EXHAUSTED
        assert admission["level"] == "critical"
        assert admission["reasons"] == ["measurement_unavailable"]
        # Nothing is fabricated: no capacity is reported at all.
        assert admission["effective_free_bytes"] is None
        assert admission["effective_free_inodes"] is None
        assert runtime.backoff[SOURCE] == recorder_runtime.BACKOFF_INITIAL
    finally:
        _close(runtime)


def test_checkpoint_write_outside_an_admitted_transaction_is_untouched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pause signal is control flow only the capture/recovery wrappers catch.

    ``save_state`` also runs outside a capture -- checkpoint alias
    reconciliation, for one. Raising the pause signal there would escape into a
    caller with nothing to handle it, so an ordinary ``OSError`` must survive.
    """

    runtime, _guard, _state_file = _captured(tmp_path, monkeypatch)
    try:
        monkeypatch.setattr(
            pressure_impl, "_write_bytes_atomic", _refuse(errno.ENOSPC)
        )

        with pytest.raises(OSError) as raised:
            runtime.save_state()

        assert raised.value.errno == errno.ENOSPC
    finally:
        _close(runtime)


def test_recovery_reports_exhaustion_through_its_documented_pause_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct recovery keeps its ``RecorderResourcePaused`` contract.

    The archived raw batch stays on disk and the checkpoint stays behind it.
    """

    state_file = tmp_path / "state.json"
    monkeypatch.setattr(recorder_runtime, "STATE_FILE", state_file)
    runtime = recorder_runtime.RecorderRuntime()
    runtime.store = DurableRecorderStore(tmp_path / "data")
    runtime.sources = {SOURCE: BASE_URL}
    _prepare_pending_raw(runtime)
    attach_runtime_resource_pressure(
        runtime,
        controller=_controller(free_bytes=ADMITTED_FREE_BYTES),
        budget=_budget(),
        state_file=state_file,
    )
    try:
        # Inject beneath the wrapper chain, at the writer the real observation
        # publication actually calls, so this exercises the installed boundary
        # rather than replacing the method that carries it.
        monkeypatch.setattr(
            bounded_storage, "_write_bounded_jsonl_atomic", _refuse(errno.ENOSPC)
        )

        with pytest.raises(RecorderResourcePaused) as raised:
            runtime._recover_archived_batches(
                source_name=SOURCE,
                base_url=BASE_URL,
                instance_id=7,
                expected=1,
                probe=_probe(),
            )

        assert raised.value.pause.code == STORAGE_EXHAUSTED
        assert runtime.checkpoints[SOURCE].next_sequence == 1
        assert len(tuple(runtime.store.raw_root.rglob("*.xml.gz"))) == 1
        assert not state_file.exists()
    finally:
        _close(runtime)


def test_frontier_pending_refusal_pauses_instead_of_blaming_the_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The transaction's first durable write is the first one a full host refuses.

    Admission grants, then the recovery frontier's pending marker -- the write
    that opens the raw-first order -- is refused. Reclassifying only at the
    store writers misses it: the marker's own unwind ends the transaction before
    the refusal reaches the capture wrapper, so by then there is no admitted
    transaction left to recognise and the Agent is blamed for a full disk.
    """

    runtime, _guard, state_file = _captured(tmp_path, monkeypatch)
    try:
        monkeypatch.setattr(
            recovery_frontier_module, "_write_json_atomic", _refuse(errno.ENOSPC)
        )

        _source, success, error = runtime.capture_source(SOURCE, BASE_URL)

        assert success is True
        assert error == ""
        status = runtime.source_status[SOURCE]
        assert status["last_error"] == ""
        assert status["resource_admission"]["code"] == STORAGE_EXHAUSTED
        assert runtime.backoff[SOURCE] == recorder_runtime.BACKOFF_INITIAL
        # Refused before anything durable started: nothing to retain, nothing
        # advanced, and no pointer left behind claiming a transaction ran.
        assert tuple(runtime.store.raw_root.rglob("*.xml.gz")) == ()
        assert tuple(runtime.store.raw_root.rglob(".recovery-frontier.json")) == ()
        assert SOURCE not in runtime.checkpoints
        assert not state_file.exists()
    finally:
        _close(runtime)


def test_frontier_clear_refusal_leaves_a_committed_batch_replay_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The transaction's last durable write is refused after the commit landed.

    Everything committed except the pointer that records the commit, so the
    stale ``pending`` marker now sits behind a durable checkpoint that already
    moved past it. That is the self-healing case the frontier documents, and it
    must resolve to no replay candidate: replaying would duplicate the batch,
    and refusing to resolve it would strand the source.
    """

    runtime, _guard, state_file = _captured(tmp_path, monkeypatch)
    refuse_clear = {"armed": True}
    write_json = recovery_frontier_module._write_json_atomic

    def _refuse_clear_marker(path: Path, payload: dict):
        if refuse_clear["armed"] and payload.get("state") == "clear":
            raise OSError(errno.ENOSPC, "injected")
        return write_json(path, payload)

    try:
        monkeypatch.setattr(
            recovery_frontier_module, "_write_json_atomic", _refuse_clear_marker
        )

        _source, success, error = runtime.capture_source(SOURCE, BASE_URL)

        assert success is True
        assert error == ""
        status = runtime.source_status[SOURCE]
        assert status["last_error"] == ""
        assert status["resource_admission"]["code"] == STORAGE_EXHAUSTED
        assert runtime.backoff[SOURCE] == recorder_runtime.BACKOFF_INITIAL
        # The batch itself committed: raw evidence and its durable checkpoint.
        assert len(tuple(runtime.store.raw_root.rglob("*.xml.gz"))) == 1
        assert state_file.exists()
        assert runtime.checkpoints[SOURCE].next_sequence == 2
        marker = next(runtime.store.raw_root.rglob(".recovery-frontier.json"))
        assert json.loads(marker.read_text(encoding="utf-8"))["state"] == "pending"

        # Recovery reads that stale pointer against the durable checkpoint and
        # resolves it to nothing: no duplicate replay, no stranded sequence.
        # The base frontier is used deliberately -- this resolution is read-side
        # and owns no transaction of its own.
        refuse_clear["armed"] = False
        lookup = RecorderRecoveryFrontier(runtime.store).lookup(
            source_name=SOURCE,
            instance_id=7,
            expected=runtime.checkpoints[SOURCE].next_sequence,
        )
        assert lookup.initialized is True
        assert lookup.ref is None
        assert json.loads(marker.read_text(encoding="utf-8"))["state"] == "clear"
    finally:
        _close(runtime)


def _recovering_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    store_class: type,
):
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(recorder_runtime, "STATE_FILE", state_file)
    runtime = recorder_runtime.RecorderRuntime()
    runtime.store = store_class(tmp_path / "data")
    runtime.sources = {SOURCE: BASE_URL}
    _prepare_pending_raw(runtime)
    attach_runtime_resource_pressure(
        runtime,
        controller=_controller(free_bytes=ADMITTED_FREE_BYTES),
        budget=_budget(),
        state_file=state_file,
    )
    return runtime, state_file


def test_recovery_pauses_when_the_compatibility_view_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery writes the compatibility view outside the wrapped store method.

    ``store_batch`` covers the three derived writers for new capture, but
    recovery publishes them one at a time and calls ``store_normalized_batch``
    directly. A refusal there is inside the completion reservation just the same,
    so it must reach the pause result rather than the caller's error path.
    """

    runtime, state_file = _recovering_runtime(
        tmp_path, monkeypatch, store_class=DurableRecorderStore
    )
    bounded_write = bounded_storage._write_bounded_jsonl_atomic

    def _refuse_compatibility_view(path: Path, records, **kwargs: object):
        if path.suffix == ".jsonl":
            raise OSError(errno.ENOSPC, "injected")
        return bounded_write(path, records, **kwargs)

    try:
        monkeypatch.setattr(
            bounded_storage,
            "_write_bounded_jsonl_atomic",
            _refuse_compatibility_view,
        )

        with pytest.raises(RecorderResourcePaused) as raised:
            runtime._recover_archived_batches(
                source_name=SOURCE,
                base_url=BASE_URL,
                instance_id=7,
                expected=1,
                probe=_probe(),
            )

        assert raised.value.pause.code == STORAGE_EXHAUSTED
        # The archived raw evidence stays, and the checkpoint stays behind it.
        assert len(tuple(runtime.store.raw_root.rglob("*.xml.gz"))) == 1
        assert runtime.checkpoints[SOURCE].next_sequence == 1
        assert not state_file.exists()
    finally:
        _close(runtime)


def test_recovery_pauses_when_the_publication_record_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The composed runtime store writes one more record after the wrapped writer.

    Startup composes publication discovery on top of admission, so the store the
    recorder actually runs writes its discovery record *after* the wrapped
    observation writer has returned -- outside that writer's scope. A refusal
    there is still inside the completion reservation, and a boundary that only
    wrapped the writers it knew about would let it out as a source error.
    """

    runtime, state_file = _recovering_runtime(
        tmp_path,
        monkeypatch,
        # The class startup composes, not the one the other tests construct.
        store_class=recorder_runtime.DurableRecorderStore,
    )
    assert getattr(runtime.store, "_publication_frontier_runtime_store", False)
    try:
        monkeypatch.setattr(
            publication_frontier, "_write_json_atomic", _refuse(errno.ENOSPC)
        )

        with pytest.raises(RecorderResourcePaused) as raised:
            runtime._recover_archived_batches(
                source_name=SOURCE,
                base_url=BASE_URL,
                instance_id=7,
                expected=1,
                probe=_probe(),
            )

        assert raised.value.pause.code == STORAGE_EXHAUSTED
        assert len(tuple(runtime.store.raw_root.rglob("*.xml.gz"))) == 1
        assert runtime.checkpoints[SOURCE].next_sequence == 1
        assert not state_file.exists()
    finally:
        _close(runtime)


def _split_probe_controller(*, probe_free_bytes: int) -> RecorderAdmissionController:
    """Measure the probe archive as its own resource, so the pause can name it."""

    def measure(path: Path | str) -> FilesystemMeasurement:
        probe_resource = "probe" in Path(path).parts
        return FilesystemMeasurement(
            resource_id="device:probe" if probe_resource else "device:data",
            observed_at=NOW,
            total_bytes=100_000,
            free_bytes=probe_free_bytes if probe_resource else ADMITTED_FREE_BYTES,
            total_inodes=2000,
            free_inodes=1000,
            available=True,
        )

    return RecorderAdmissionController(
        thresholds=_thresholds(),
        measurer=measure,
        clock=lambda: NOW,
    )


PROBE_XML = (
    '<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.7">'
    '<Header instanceId="7"/>'
    '<Devices><Device id="machine" name="machine" uuid="machine-1">'
    '<DataItems><DataItem id="x" category="SAMPLE" type="POSITION"/>'
    "</DataItems></Device></Devices></MTConnectDevices>"
)


def test_a_refused_probe_write_measures_the_archive_that_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pause reports the resource that refused, not a neighbouring one.

    The probe archive is its own root. When the write into it is refused, the
    measurement carried by the pause has to come from that root; measuring the
    raw archive instead would publish another resource's headroom as the reason
    capture stopped.
    """

    state_file = tmp_path / "state.json"
    monkeypatch.setattr(recorder_runtime, "STATE_FILE", state_file)
    runtime = recorder_runtime.RecorderRuntime()
    runtime.store = DurableRecorderStore(tmp_path / "data")
    runtime.sources = {SOURCE: BASE_URL}
    runtime.enabled = True
    runtime.configuration_ready = True
    attach_runtime_resource_pressure(
        runtime,
        controller=_split_probe_controller(probe_free_bytes=41_000),
        # Bound the probe reservation the same way the shared budget bounds the
        # sample one, so admission grants and the filesystem is what refuses.
        budget=replace(_budget(), probe_bytes=512, probe_inodes=1),
        state_file=state_file,
    )

    sample_xml = _streams_xml([1])

    class Client(_client(sample_xml)):
        def fetch_probe(self) -> str:
            return PROBE_XML

    write_bytes = storage_module._write_bytes_atomic

    def _refuse_probe_payload(path: Path, payload: bytes):
        if "probe" in path.parts:
            raise OSError(errno.ENOSPC, "injected")
        return write_bytes(path, payload)

    try:
        monkeypatch.setattr(recorder_runtime, "MtconnectClient", Client)
        monkeypatch.setattr(
            storage_module, "_write_bytes_atomic", _refuse_probe_payload
        )

        _source, success, error = runtime.capture_source(SOURCE, BASE_URL)

        assert success is True
        assert error == ""
        admission = runtime.source_status[SOURCE]["resource_admission"]
        assert admission["code"] == STORAGE_EXHAUSTED
        # The probe archive's headroom, not the raw archive's.
        assert admission["effective_free_bytes"] == 41_000
        assert tuple(runtime.store.probe_root.rglob("*.xml.gz")) == ()
    finally:
        _close(runtime)
