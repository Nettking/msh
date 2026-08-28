"""Crash-safe retirement of per-request host trial result publications."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.flask_app.services import federation_update_events as events
from catalog.flask_app.services.federation_update_handoff import HostUpdateHandoff

TRIAL_RESULT_SCHEMA = "fcp.host-trial-result.v1"
NODE_ID = "node-trial-retirement"
HOST_REQUEST = "host-trial-settled"
TARGET = "b" * 40
FEDERATION_REQUEST = "federation-request-1"


def _result_path(directory: Path, request_id: str) -> Path:
    digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
    return directory / f"trial-result-{digest}.json"


def _write_trial_result(directory: Path, request_id: str, state: str) -> Path:
    path = _result_path(directory, request_id)
    path.write_text(
        json.dumps(
            {
                "schema": TRIAL_RESULT_SCHEMA,
                "request_id": request_id,
                "action": "trial",
                "state": state,
                "target_commit": TARGET,
                "branch": "main",
            }
        ),
        encoding="utf-8",
    )
    return path


def _pending_state() -> dict[str, object]:
    return {
        "schema": events.PROCESSOR_SCHEMA,
        "last_revision": 7,
        "authority_node_id": NODE_ID,
        "pending": {
            FEDERATION_REQUEST: {
                "kind": "trial",
                "host_request_id": HOST_REQUEST,
                "target_commit": TARGET,
            }
        },
    }


def _processor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[events.FederationUpdateEventProcessor, HostUpdateHandoff, Path]:
    directory = tmp_path / "update-agent"
    directory.mkdir(parents=True)
    handoff = HostUpdateHandoff(directory)
    state_file = tmp_path / "processor-state.json"
    monkeypatch.setattr(events, "_append_remote_event", lambda *_args, **_kwargs: None)
    processor = events.FederationUpdateEventProcessor(
        SimpleNamespace(),
        handoff,
        state_file,
    )
    return processor, handoff, state_file


def _context() -> SimpleNamespace:
    return SimpleNamespace(
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=NODE_ID))
    )


def test_settled_trial_is_retired_only_after_pending_removal_is_durable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor, handoff, state_file = _processor(tmp_path, monkeypatch)
    path = _write_trial_result(handoff.directory, HOST_REQUEST, "safe_restored")
    state = _pending_state()
    events._write_state(state_file, state)

    processor._finish_pending(_context(), state)

    persisted = events._read_state(state_file)
    assert persisted["pending"] == {}
    assert persisted[events.TRIAL_RETIREMENT_STATE_KEY] == []
    assert not path.exists()


def test_unsettled_trial_keeps_its_result_and_pending_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor, handoff, state_file = _processor(tmp_path, monkeypatch)
    path = _write_trial_result(handoff.directory, HOST_REQUEST, "trial_preparing")
    state = _pending_state()
    events._write_state(state_file, state)

    processor._finish_pending(_context(), state)

    persisted = events._read_state(state_file)
    assert FEDERATION_REQUEST in persisted["pending"]
    assert path.exists()
    assert persisted.get(events.TRIAL_RETIREMENT_STATE_KEY) in (None, [])


def test_failed_state_commit_never_unlinks_the_only_reconciliation_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor, handoff, state_file = _processor(tmp_path, monkeypatch)
    path = _write_trial_result(handoff.directory, HOST_REQUEST, "safe_restored")
    initial = _pending_state()
    events._write_state(state_file, initial)
    state = events._read_state(state_file)
    retired: list[str] = []

    monkeypatch.setattr(
        processor,
        "_retire_trial_result",
        lambda request_id: retired.append(request_id) or True,
    )
    monkeypatch.setattr(
        events,
        "_write_state",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("simulated crash")),
    )

    with pytest.raises(OSError, match="simulated crash"):
        processor._finish_pending(_context(), state)

    assert retired == []
    assert path.exists()
    on_disk = json.loads(state_file.read_text(encoding="utf-8"))
    assert FEDERATION_REQUEST in on_disk["pending"]


def test_crash_after_retirement_intent_is_recovered_on_next_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor, handoff, state_file = _processor(tmp_path, monkeypatch)
    path = _write_trial_result(handoff.directory, HOST_REQUEST, "safe_restored")
    state = _pending_state()
    events._write_state(state_file, state)

    monkeypatch.setattr(processor, "_retire_trial_result", lambda _request_id: False)
    processor._finish_pending(_context(), state)

    persisted = events._read_state(state_file)
    assert persisted["pending"] == {}
    assert persisted[events.TRIAL_RETIREMENT_STATE_KEY] == [HOST_REQUEST]
    assert path.exists()

    restarted = events.FederationUpdateEventProcessor(
        SimpleNamespace(),
        handoff,
        state_file,
    )
    restarted._drain_trial_retirements(persisted)

    recovered = events._read_state(state_file)
    assert recovered[events.TRIAL_RETIREMENT_STATE_KEY] == []
    assert not path.exists()


def test_crash_after_unlink_before_queue_clear_is_idempotently_recovered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor, handoff, state_file = _processor(tmp_path, monkeypatch)
    path = _write_trial_result(handoff.directory, HOST_REQUEST, "safe_restored")
    initial = _pending_state()
    events._write_state(state_file, initial)
    state = events._read_state(state_file)
    real_write = events._write_state
    writes = 0

    def fail_second_write(path_arg: Path, value: dict[str, object]) -> None:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("simulated crash after unlink")
        real_write(path_arg, value)

    monkeypatch.setattr(events, "_write_state", fail_second_write)
    with pytest.raises(OSError, match="simulated crash after unlink"):
        processor._finish_pending(_context(), state)

    assert not path.exists()
    persisted = events._read_state(state_file)
    assert persisted["pending"] == {}
    assert persisted[events.TRIAL_RETIREMENT_STATE_KEY] == [HOST_REQUEST]

    monkeypatch.setattr(events, "_write_state", real_write)
    restarted = events.FederationUpdateEventProcessor(
        SimpleNamespace(),
        handoff,
        state_file,
    )
    restarted._drain_trial_retirements(persisted)

    recovered = events._read_state(state_file)
    assert recovered[events.TRIAL_RETIREMENT_STATE_KEY] == []


def test_retirement_touches_only_the_named_trial_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor, handoff, state_file = _processor(tmp_path, monkeypatch)
    mine = _write_trial_result(handoff.directory, HOST_REQUEST, "safe_restored")
    other = _write_trial_result(handoff.directory, "host-trial-other", "safe_restored")
    mirror = handoff.directory / "trial-result.json"
    update_result = handoff.directory / "result-unrelated.json"
    mirror.write_text("{}", encoding="utf-8")
    update_result.write_text("{}", encoding="utf-8")
    state = events._empty_state()
    events._write_state(state_file, state)

    processor._persist_trial_retirement(state, HOST_REQUEST)

    assert not mine.exists()
    assert other.exists()
    assert mirror.exists()
    assert update_result.exists()
    assert events._read_state(state_file)[events.TRIAL_RETIREMENT_STATE_KEY] == []


def test_malformed_retirement_state_is_sanitized_without_directory_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor, _handoff, state_file = _processor(tmp_path, monkeypatch)
    state = events._empty_state()
    state[events.TRIAL_RETIREMENT_STATE_KEY] = [None, 3, "", HOST_REQUEST, HOST_REQUEST]
    events._write_state(state_file, state)

    processor._drain_trial_retirements(state)

    assert events._read_state(state_file)[events.TRIAL_RETIREMENT_STATE_KEY] == []
