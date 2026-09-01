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
from pathlib import Path

import pytest

from catalog.mtconnect_recorder import _resource_pressure_impl as pressure_impl
from catalog.mtconnect_recorder import bounded_storage
from catalog.mtconnect_recorder import runtime as recorder_runtime
from catalog.mtconnect_recorder.resource_pressure import (
    STORAGE_EXHAUSTED,
    RecorderResourcePaused,
    attach_runtime_resource_pressure,
)
from catalog.mtconnect_recorder.storage import DurableRecorderStore
from catalog.mtconnect_recorder.tests.test_recorder_resource_pressure import (
    BASE_URL,
    SOURCE,
    _budget,
    _close,
    _controller,
    _prepare_pending_raw,
    _probe,
    _runtime,
    _streams_xml,
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


def test_an_unmeasurable_resource_keeps_the_original_refusal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pause carries a real measurement or it is not claimed at all.

    If the resource that just refused cannot be assessed, this boundary has
    nothing to report and must not invent a pressure state.
    """

    runtime, guard, _state_file = _captured(tmp_path, monkeypatch)
    try:
        monkeypatch.setattr(
            runtime.store, "store_raw_batch", _refuse(errno.ENOSPC)
        )

        def _unmeasurable(_path: object) -> None:
            raise OSError(errno.EIO, "cannot stat")

        monkeypatch.setattr(guard.controller, "assessment", _unmeasurable)

        _source, success, error = runtime.capture_source(SOURCE, BASE_URL)

        assert success is False
        assert "No space left" in error or "injected" in error
        assert "resource_admission" not in runtime.source_status[SOURCE]
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
