from __future__ import annotations

import importlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.service_incarnation import (
    STATE_CRASH_LOOP,
    STOP_COMPLETED,
    STOP_OPERATOR,
    STOP_UPDATE,
    incarnation_state_file,
    read_restart_state,
    record_service_start,
)
from catalog.mtconnect_recorder import managed_service
from catalog.relay import provider_service, service
from catalog.relay.service import RelayRuntimeError

NOW = datetime(2026, 9, 2, 9, 0, tzinfo=timezone.utc)


def test_repeated_flask_exceptions_are_fcp_visible(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    flask_entrypoint = importlib.import_module("catalog.flask_app.app")
    monkeypatch.setenv("FCP_DATA_ROOT", str(tmp_path))

    class BrokenApp:
        def run(self, **_kwargs: object) -> None:
            raise RuntimeError("flask-runtime-failed")

    path = incarnation_state_file(tmp_path, "flask")
    # The first record represents the incarnation that Docker is about to
    # restart. The following three exception exits are three restart-worthy
    # failures, not three clean completions.
    record_service_start(path, service="flask", now=NOW)
    for _ in range(3):
        with pytest.raises(RuntimeError, match="flask-runtime-failed"):
            flask_entrypoint.run_flask_server(
                BrokenApp(),
                host="127.0.0.1",
                port=5000,
                debug=False,
            )

    state = read_restart_state(path, service="flask")
    assert state.state == STATE_CRASH_LOOP
    assert state.consecutive_unclean >= 3


def test_real_flask_recovery_from_failure_sequence_deescalates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    flask_entrypoint = importlib.import_module("catalog.flask_app.app")
    monkeypatch.setenv("FCP_DATA_ROOT", str(tmp_path))

    class BrokenApp:
        def run(self, **_kwargs: object) -> None:
            raise RuntimeError("flask-runtime-failed")

    class HealthyApp:
        def run(self, **_kwargs: object) -> None:
            return None

    path = incarnation_state_file(tmp_path, "flask")
    record_service_start(path, service="flask", now=NOW)
    for _ in range(3):
        with pytest.raises(RuntimeError, match="flask-runtime-failed"):
            flask_entrypoint.run_flask_server(
                BrokenApp(),
                host="127.0.0.1",
                port=5000,
                debug=False,
            )

    assert read_restart_state(path, service="flask").state == STATE_CRASH_LOOP

    flask_entrypoint.run_flask_server(
        HealthyApp(),
        host="127.0.0.1",
        port=5000,
        debug=False,
    )
    # The successful run is recorded as completed. The next start is the
    # first incarnation whose predecessor is clean, so the FCP state clears.
    recovered = record_service_start(path, service="flask")
    assert recovered.state != STATE_CRASH_LOOP
    assert recovered.consecutive_unclean == 0


def test_repeated_managed_recorder_failures_are_fcp_visible(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FCP_RECORDER_DATA_DIR", str(tmp_path))

    def broken_recorder() -> None:
        raise RuntimeError("recorder-runtime-failed")

    monkeypatch.setattr(managed_service, "run_managed_recorder", broken_recorder)
    path = incarnation_state_file(tmp_path, "recorder")
    record_service_start(path, service="recorder", now=NOW)
    for _ in range(3):
        with pytest.raises(RuntimeError, match="recorder-runtime-failed"):
            managed_service.main()

    state = read_restart_state(path, service="recorder")
    assert state.state == STATE_CRASH_LOOP
    assert state.consecutive_unclean >= 3


@pytest.mark.parametrize(
    ("runtime_reason", "expected_reason"),
    [
        (None, STOP_COMPLETED),
        ("signal", STOP_OPERATOR),
        ("external-activation", STOP_UPDATE),
    ],
)
def test_managed_intentional_exits_remain_clean(
    runtime_reason: str | None,
    expected_reason: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FCP_RECORDER_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(managed_service, "run_managed_recorder", lambda: None)
    runtime = importlib.import_module("catalog.mtconnect_recorder.runtime")
    monkeypatch.setattr(runtime, "last_stop_reason", lambda: runtime_reason)

    for _ in range(2):
        assert managed_service.main() == 0

    state = read_restart_state(
        incarnation_state_file(tmp_path, "recorder"),
        service="recorder",
    )
    assert state.state != STATE_CRASH_LOOP
    assert state.consecutive_unclean == 0
    assert state.last_stop_reason == expected_reason


@pytest.mark.parametrize("entrypoint", [service, provider_service])
def test_repeated_relay_nonzero_failures_are_fcp_visible(
    entrypoint: object,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class BrokenRelay:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        async def start(self) -> None:
            return None

        async def wait_stopped(self) -> None:
            raise RelayRuntimeError(
                "relay-background-task-failed",
                "required relay background task failed",
            )

        async def stop(self) -> None:
            return None

    monkeypatch.setattr(entrypoint, "SessionCoordinator", lambda path: path)
    relay_class = (
        "ProviderAuthorityRelayServer"
        if entrypoint is provider_service
        else "RelayServer"
    )
    monkeypatch.setattr(entrypoint, relay_class, BrokenRelay)
    database = tmp_path / "control.sqlite3"
    path = incarnation_state_file(tmp_path, "relay")
    record_service_start(path, service="relay", now=NOW)

    for _ in range(3):
        assert entrypoint.main(["serve", "--database", str(database)]) == 2

    state = read_restart_state(path, service="relay")
    assert state.state == STATE_CRASH_LOOP
    assert state.consecutive_unclean >= 3
