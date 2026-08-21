"""The standalone recorder's own participation in "Update all devices".

Two properties matter most here and are asserted directly rather than inferred:

* update participation reuses the recorder's *existing* authenticated relay
  connection -- no second Federation node, client, or reader; and
* capture stops for an update only when the stop request is provably for this
  exact process and for Federation work this recorder is actually waiting on,
  and an operator's Ctrl+C is never turned into an automatic relaunch.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.federation.software_update import UpdateInspection
from catalog.flask_app.services import federation_update_events as events
from catalog.mtconnect_recorder import runtime as recorder_runtime
from catalog.mtconnect_recorder.federation_update import (
    RecorderFederationUpdateWorker,
    RecorderUpdateActivationWatcher,
    approved_update_restart,
)
from catalog.mtconnect_recorder.native_identity import (
    PROCESS_NONCE_ENV,
    SUPERVISOR_SESSION_ENV,
)
from catalog.mtconnect_recorder.native_update import (
    ACTIVATION_SCHEMA,
    NativeRecorderUpdatePaths,
    activation_intent,
    evaluate_activation,
    stamp,
    utc_now,
    write_json_atomic,
)

SESSION = "session-a"
RECORDER = "node-recorder"
CREATOR = "node-creator"
SUPERVISOR = "1" * 32
NONCE = "2" * 32
OTHER_NONCE = "3" * 32
PID = 9191
TARGET = "a" * 40
HOST_REQUEST = "fed-hostrequest"


def _paths(tmp_path: Path) -> NativeRecorderUpdatePaths:
    paths = NativeRecorderUpdatePaths(tmp_path)
    paths.directory.mkdir(parents=True, exist_ok=True)
    return paths


def _write_pending(paths: NativeRecorderUpdatePaths, target: str = TARGET) -> None:
    write_json_atomic(
        paths.federation_state_file,
        {
            "schema": "fcp.federation-update-processor.v1",
            "last_revision": 3,
            "pending": {
                "federation-request": {
                    "host_request_id": HOST_REQUEST,
                    "target_commit": target,
                }
            },
        },
    )


def _intent(**changes: Any) -> dict[str, object]:
    value = activation_intent(
        request_id=HOST_REQUEST,
        target_commit=TARGET,
        supervisor_session=SUPERVISOR,
        recorder_pid=PID,
        process_nonce=NONCE,
    )
    value.update(changes)
    return value


def _watcher(tmp_path: Path, **changes: str) -> RecorderUpdateActivationWatcher:
    environment = {
        SUPERVISOR_SESSION_ENV: SUPERVISOR,
        PROCESS_NONCE_ENV: NONCE,
    }
    environment.update(changes)
    return RecorderUpdateActivationWatcher(
        data_directory=tmp_path,
        environment=environment,
        pid=PID,
    )


# --------------------------------------------------------------------------
# activation correlation
# --------------------------------------------------------------------------


def test_a_correlated_activation_for_a_pending_update_is_accepted(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    _write_pending(paths)
    write_json_atomic(paths.activation_file, _intent())

    decision = _watcher(tmp_path).evaluate_once()

    assert decision.accepted is True
    assert decision.request_id == HOST_REQUEST
    assert decision.target_commit == TARGET


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"recorder_pid": PID + 1}, "wrong_recorder_pid"),
        ({"process_nonce": OTHER_NONCE}, "wrong_process_nonce"),
        ({"supervisor_session": "4" * 32}, "wrong_supervisor_session"),
        ({"target_commit": "b" * 40}, "no_pending_federation_update"),
        ({"schema": "fcp.something-else.v1"}, "malformed_activation"),
    ],
)
def test_an_uncorrelated_activation_never_stops_capture(
    tmp_path: Path, changes: dict[str, object], reason: str
) -> None:
    paths = _paths(tmp_path)
    _write_pending(paths)
    write_json_atomic(paths.activation_file, _intent(**changes))

    watcher = _watcher(tmp_path)
    decision = watcher.evaluate_once()

    assert decision.accepted is False
    assert decision.reason == reason
    assert watcher.poll_once() is False
    assert watcher.activated is False


def test_an_expired_activation_never_stops_capture(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    _write_pending(paths)
    past = utc_now() - timedelta(minutes=30)
    write_json_atomic(
        paths.activation_file,
        _intent(
            created_at=stamp(past),
            expires_at=stamp(past + timedelta(seconds=60)),
        ),
    )

    assert _watcher(tmp_path).evaluate_once().reason == "expired_activation"


def test_an_activation_without_a_pending_update_never_stops_capture(
    tmp_path: Path,
) -> None:
    """A marker alone is not authority: the Federation request must exist."""

    paths = _paths(tmp_path)
    write_json_atomic(paths.activation_file, _intent())

    assert (
        _watcher(tmp_path).evaluate_once().reason == "no_pending_federation_update"
    )


def test_a_marker_left_by_an_earlier_process_cannot_restart_a_new_recorder(
    tmp_path: Path,
) -> None:
    """The stale-marker restart loop, pinned.

    The previous recorder was activated and exited. Its marker is still on disk
    when a fresh recorder starts with a new nonce and PID. That recorder must
    ignore it completely.
    """

    paths = _paths(tmp_path)
    _write_pending(paths)
    write_json_atomic(paths.activation_file, _intent())

    replacement = RecorderUpdateActivationWatcher(
        data_directory=tmp_path,
        environment={
            SUPERVISOR_SESSION_ENV: SUPERVISOR,
            PROCESS_NONCE_ENV: OTHER_NONCE,
        },
        pid=PID + 100,
    )

    assert replacement.poll_once() is False
    assert replacement.activated is False


def test_an_unsupervised_recorder_ignores_activation_entirely(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    _write_pending(paths)
    write_json_atomic(paths.activation_file, _intent())

    watcher = RecorderUpdateActivationWatcher(
        data_directory=tmp_path, environment={}, pid=PID
    )
    watcher.start()

    assert watcher.supervised is False
    assert watcher.poll_once() is False


def test_activation_evaluation_requires_every_field(tmp_path: Path) -> None:
    """The evaluator is the recorder's own independent second check."""

    accepted = evaluate_activation(
        _intent(),
        pid=PID,
        process_nonce=NONCE,
        supervisor_session=SUPERVISOR,
        pending_host_request_ids={HOST_REQUEST: TARGET},
    )
    assert accepted.accepted is True

    for missing in ("process_nonce", "supervisor_session"):
        decision = evaluate_activation(
            _intent(),
            pid=PID,
            process_nonce=None if missing == "process_nonce" else NONCE,
            supervisor_session=(
                None if missing == "supervisor_session" else SUPERVISOR
            ),
            pending_host_request_ids={HOST_REQUEST: TARGET},
        )
        assert decision.accepted is False


# --------------------------------------------------------------------------
# graceful stop and exit code 75
# --------------------------------------------------------------------------


class _StopTarget:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def request_stop(self, signum: int | None = None, **kwargs: object) -> None:
        self.calls.append({"signum": signum, **kwargs})


@pytest.fixture()
def clean_stop_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(recorder_runtime, "_STOP_TARGETS", [])
    monkeypatch.setattr(recorder_runtime, "_STOP_REASON", None)


def test_an_accepted_activation_stops_capture_gracefully(
    tmp_path: Path, clean_stop_state: None
) -> None:
    """The stop is a cooperative request, never an injected exception or kill."""

    paths = _paths(tmp_path)
    _write_pending(paths)
    write_json_atomic(paths.activation_file, _intent())
    target = _StopTarget()
    recorder_runtime.register_stop_target(target)  # type: ignore[arg-type]

    watcher = _watcher(tmp_path)
    assert watcher.poll_once() is True

    assert target.calls == [{"signum": None, "external": True}]
    assert recorder_runtime.last_stop_reason() == (
        recorder_runtime.EXTERNAL_STOP_REASON
    )
    assert watcher.activated_request_id == HOST_REQUEST
    assert watcher.activated_target_commit == TARGET


def test_an_activation_arriving_before_capture_starts_is_retried(
    tmp_path: Path, clean_stop_state: None
) -> None:
    """A stop nothing received must never look like an approved update restart."""

    paths = _paths(tmp_path)
    _write_pending(paths)
    write_json_atomic(paths.activation_file, _intent())
    watcher = _watcher(tmp_path)

    # No capture runtime has registered yet.
    assert watcher.poll_once() is False
    assert watcher.activated is False
    assert recorder_runtime.last_stop_reason() is None
    assert approved_update_restart(watcher) is False

    # Once capture is up, the same activation is accepted on the next pass.
    recorder_runtime.register_stop_target(_StopTarget())  # type: ignore[arg-type]
    assert watcher.poll_once() is True
    assert approved_update_restart(watcher) is True


def test_an_approved_update_restart_requires_both_halves(
    tmp_path: Path, clean_stop_state: None
) -> None:
    paths = _paths(tmp_path)
    _write_pending(paths)
    write_json_atomic(paths.activation_file, _intent())
    recorder_runtime.register_stop_target(_StopTarget())  # type: ignore[arg-type]
    watcher = _watcher(tmp_path)
    watcher.poll_once()

    assert approved_update_restart(watcher) is True


def test_no_activation_is_never_an_approved_update_restart(
    tmp_path: Path, clean_stop_state: None
) -> None:
    assert approved_update_restart(None) is False
    assert approved_update_restart(_watcher(tmp_path)) is False


def test_ctrl_c_during_an_activation_is_never_an_update_restart(
    tmp_path: Path, clean_stop_state: None
) -> None:
    """An operator stop must win, or a supervisor relaunches behind their back."""

    paths = _paths(tmp_path)
    _write_pending(paths)
    write_json_atomic(paths.activation_file, _intent())
    recorder_runtime.register_stop_target(_StopTarget())  # type: ignore[arg-type]
    watcher = _watcher(tmp_path)
    watcher.poll_once()
    assert approved_update_restart(watcher) is True

    # The operator interrupts while the activation is in flight.
    recorder_runtime._record_stop_reason(recorder_runtime.SIGNAL_STOP_REASON)

    assert approved_update_restart(watcher) is False


def test_an_ordinary_signal_stop_is_not_an_update_restart(
    tmp_path: Path, clean_stop_state: None
) -> None:
    recorder_runtime._record_stop_reason(recorder_runtime.SIGNAL_STOP_REASON)

    assert approved_update_restart(_watcher(tmp_path)) is False


def test_start_recorder_reserves_seventy_five_for_approved_updates() -> None:
    import start_recorder

    assert start_recorder.APPROVED_UPDATE_RESTART_EXIT_CODE == 75
    source = Path(start_recorder.__file__).read_text(encoding="utf-8")
    # Exactly two paths may return it -- an approved update, and a branch trial
    # that could not prove a healthy startup -- plus the definition itself. Each
    # is guarded by its own two-half check, and an operator stop still loses
    # neither guard.
    assert source.count("APPROVED_UPDATE_RESTART_EXIT_CODE") == 3
    assert "if approved_update_restart(activation):" in source
    assert "if trial_fallback_restart(host_update):" in source


# --------------------------------------------------------------------------
# the existing authenticated connection is the only one used
# --------------------------------------------------------------------------


class _Node:
    """A recorder node exposing exactly the seams the real one exposes."""

    def __init__(self, tmp_path: Path, replay: tuple[Any, ...]) -> None:
        self.appended: list[dict[str, Any]] = []
        self.replay_calls = 0
        self.remote_store = SimpleNamespace(load=lambda: "saved-remote")

        def replay_page(**_kwargs: Any) -> tuple[tuple[Any, ...], int]:
            self.replay_calls += 1
            return replay, replay[-1].revision if replay else 0

        coordinator = SimpleNamespace(
            coordinator_id="fcp-relay-coordinator",
            store=SimpleNamespace(get_session=lambda _s: SimpleNamespace()),
            replay_page=replay_page,
            runtime=self,
            state="saved-remote",
        )
        self.context = SimpleNamespace(
            credentials=SimpleNamespace(
                identity=SimpleNamespace(node_id=RECORDER)
            ),
            binding=SimpleNamespace(internal_session_id=SESSION),
            coordinator=coordinator,
        )
        self.service = SimpleNamespace(
            authorized_context=lambda: self.context,
            remote_store=self.remote_store,
            relay_runtime=self,
        )

    # The relay runtime seam the update processor is allowed to use.
    def append_session_event(self, remote: object, **kwargs: Any) -> object:
        self.appended.append({"remote": remote, **kwargs})
        return SimpleNamespace(revision=99)


def _command(revision: int, event_type: str) -> SimpleNamespace:
    now = utc_now()
    return SimpleNamespace(
        revision=revision,
        session_id=SESSION,
        event_type=event_type,
        actor_node_id=CREATOR,
        payload=events.command_payload(
            request_id="update-1",
            target_commit=TARGET,
            target_node_ids=(RECORDER,),
            created_at=now,
            expires_at=now + timedelta(minutes=10),
        ),
    )


def _session_created(revision: int = 1) -> SimpleNamespace:
    return SimpleNamespace(
        revision=revision,
        session_id=SESSION,
        event_type="session.created",
        actor_node_id=CREATOR,
        payload={"session_id": SESSION},
    )


class _Handoff:
    def __init__(self) -> None:
        self.checks: list[str | None] = []
        self.applies: list[tuple[str, str | None]] = []

    def inspect(self, *, target: str | None = None, fetch: bool = True) -> Any:
        del fetch
        self.checks.append(target)
        return UpdateInspection("update_available", "c" * 40, target)

    def apply(self, target: str, *, request_id: str | None = None) -> Any:
        self.applies.append((target, request_id))
        return UpdateInspection(
            "activation_queued",
            target_commit=target,
            code="host_activation_queued",
            request_id=request_id,
        )

    def result_for(self, _request_id: str) -> None:
        return None


def test_a_check_command_is_answered_over_the_existing_client(
    tmp_path: Path,
) -> None:
    node = _Node(tmp_path, (_session_created(), _command(2, events.CHECK_REQUEST_EVENT)))
    handoff = _Handoff()
    worker = RecorderFederationUpdateWorker(
        node, data_directory=tmp_path, handoff=handoff
    )

    assert worker.process_once() is True

    assert handoff.checks == [TARGET]
    assert node.replay_calls >= 1
    reported = [item for item in node.appended if item["event_type"].endswith("reported")]
    assert len(reported) == 1
    assert reported[0]["event_type"] == events.CHECK_REPORT_EVENT
    assert reported[0]["session_id"] == SESSION
    assert reported[0]["payload"]["node_id"] == RECORDER


def test_an_apply_command_queues_a_host_activation_and_records_it_durably(
    tmp_path: Path,
) -> None:
    node = _Node(tmp_path, (_session_created(), _command(2, events.APPLY_REQUEST_EVENT)))
    handoff = _Handoff()
    worker = RecorderFederationUpdateWorker(
        node, data_directory=tmp_path, handoff=handoff
    )

    worker.process_once()

    assert len(handoff.applies) == 1
    target, host_request_id = handoff.applies[0]
    assert target == TARGET
    # The durable pending record is what later lets the recorder consent to a
    # stop and lets a replacement report the result after a restart.
    pending = worker.paths.federation_state_file
    stored = __import__("json").loads(pending.read_text(encoding="utf-8"))
    assert stored["pending"]["update-1"] == {
        "host_request_id": host_request_id,
        "target_commit": TARGET,
    }


def test_update_participation_never_opens_a_second_relay_connection(
    tmp_path: Path,
) -> None:
    """No second Federation node, client, reader, or transport is constructed."""

    node = _Node(tmp_path, (_session_created(), _command(2, events.APPLY_REQUEST_EVENT)))
    worker = RecorderFederationUpdateWorker(
        node, data_directory=tmp_path, handoff=_Handoff()
    )

    worker.process_once()

    # Everything went through the node's own service/runtime seams.
    assert all(item["remote"] == "saved-remote" for item in node.appended)

    import catalog.mtconnect_recorder.federation_update as module

    source = Path(module.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "RelayNodeClient",
        "RecorderFederationNode(",
        "connect(",
        "request_replay",
        "PairingRelayRuntime(",
        "websockets",
    ):
        assert forbidden not in source


def test_the_update_worker_never_starts_without_saved_membership(
    tmp_path: Path,
) -> None:
    node = _Node(tmp_path, ())
    node.remote_store.load = lambda: None
    worker = RecorderFederationUpdateWorker(
        node, data_directory=tmp_path, handoff=_Handoff()
    )

    worker.start()

    assert worker._thread is None


def test_a_transport_failure_never_ends_capture(tmp_path: Path) -> None:
    node = _Node(tmp_path, ())

    class _Exploding:
        def process(self, _context: object) -> None:
            raise RuntimeError("relay unavailable")

    worker = RecorderFederationUpdateWorker(
        node,
        data_directory=tmp_path,
        handoff=_Handoff(),
        processor=_Exploding(),
        poll_seconds=0.01,
    )
    worker._stop.set()

    # The loop body must absorb the failure rather than propagate it.
    worker._run()


def test_the_activation_file_lives_beside_the_recorder_update_agent(
    tmp_path: Path,
) -> None:
    """The native agent must never share a directory with the Compose agent."""

    paths = NativeRecorderUpdatePaths(tmp_path)

    assert paths.directory.name == "recorder-update-agent"
    assert paths.directory.parent.name == "federation"
    assert paths.activation_file.parent == paths.directory
    assert paths.federation_state_file.parent.name == "recorder_update"
    # The Compose recorder agent's directory is a different one entirely.
    assert paths.directory != tmp_path / "federation" / "update-agent"


def test_activation_intent_rejects_every_malformed_field() -> None:
    for changes in (
        {"request_id": "../escape"},
        {"target_commit": "not-a-commit"},
        {"supervisor_session": "short"},
        {"process_nonce": "short"},
        {"recorder_pid": 0},
        {"recorder_pid": True},
    ):
        with pytest.raises(ValueError):
            activation_intent(
                **{
                    "request_id": HOST_REQUEST,
                    "target_commit": TARGET,
                    "supervisor_session": SUPERVISOR,
                    "recorder_pid": PID,
                    "process_nonce": NONCE,
                    **changes,
                }
            )


def test_an_activation_schema_change_is_a_breaking_change() -> None:
    assert ACTIVATION_SCHEMA == "fcp.recorder-update-activation.v1"

# --------------------------------------------------------------------------
# shutdown ordering
# --------------------------------------------------------------------------


def test_a_stuck_update_worker_is_a_bounded_reported_failure(
    tmp_path: Path,
) -> None:
    """The relay client must never be closed silently under a live operation."""

    import threading

    node = _Node(tmp_path, ())
    release = threading.Event()

    class _Wedged:
        def process(self, _context: object) -> None:
            release.wait(30.0)

    worker = RecorderFederationUpdateWorker(
        node,
        data_directory=tmp_path,
        handoff=_Handoff(),
        processor=_Wedged(),
        poll_seconds=0.01,
    )
    worker.start()
    try:
        assert worker.stop(timeout=0.2) is False
    finally:
        release.set()


def test_the_update_shutdown_limit_exceeds_one_bounded_host_operation(
    tmp_path: Path,
) -> None:
    from catalog.mtconnect_recorder import federation_update as module

    assert module.SHUTDOWN_TIMEOUT_SECONDS > module.HANDOFF_TIMEOUT_SECONDS
    worker = RecorderFederationUpdateWorker(_Node(tmp_path, ()), data_directory=tmp_path)
    assert worker.handoff.timeout == module.HANDOFF_TIMEOUT_SECONDS


def test_one_failing_cleanup_never_skips_the_others(capsys: Any) -> None:
    """Recorder control, publication and the relay client all still shut down."""

    import start_recorder

    stopped: list[str] = []

    def exploding() -> None:
        stopped.append("update")
        raise RuntimeError("bounded host operation failed")

    start_recorder._shut_down("Federation update worker", exploding)
    start_recorder._shut_down(
        "Federation control worker", lambda: stopped.append("control")
    )
    start_recorder._shut_down("Federation node", lambda: stopped.append("node"))

    assert stopped == ["update", "control", "node"]
    assert "did not shut down cleanly" in capsys.readouterr().err


def test_capture_stops_before_the_update_path_is_torn_down() -> None:
    """Capture must be able to stop even when update processing is slow."""

    import start_recorder

    source = Path(start_recorder.__file__).read_text(encoding="utf-8")
    teardown = source[source.index("    finally:\n        # Stop the update path") :]
    order = [
        teardown.index("update activation watcher"),
        teardown.index("Federation update worker"),
        teardown.index("Federation control worker"),
        teardown.index("Federation node"),
    ]
    assert order == sorted(order)
    # The activation watcher signals capture directly, so a wedged update worker
    # can never hold the stop back.
    assert "request_external_stop()" in Path(
        __import__(
            "catalog.mtconnect_recorder.federation_update", fromlist=["x"]
        ).__file__
    ).read_text(encoding="utf-8")

# --------------------------------------------------------------------------
# the host agent runs inside the recorder process
# --------------------------------------------------------------------------


def test_the_host_agent_runs_in_process_only_when_supervised(tmp_path: Path) -> None:
    from catalog.mtconnect_recorder.federation_update import (
        RecorderHostUpdateAgentWorker,
    )

    unsupervised = RecorderHostUpdateAgentWorker(
        data_directory=tmp_path, environment={}
    )
    unsupervised.start()

    assert unsupervised.supervised is False
    assert unsupervised.poll_once() is False
    assert unsupervised._thread is None

    passes: list[int] = []

    class _Agent:
        def poll_once(self) -> bool:
            passes.append(1)
            return True

    supervised = RecorderHostUpdateAgentWorker(
        data_directory=tmp_path,
        environment={SUPERVISOR_SESSION_ENV: SUPERVISOR},
        agent=_Agent(),
    )

    assert supervised.supervised is True
    assert supervised.poll_once() is True
    assert passes == [1]


def test_the_host_agent_resolves_its_repository_locally() -> None:
    """Never from a peer, an argument, or an environment value."""

    from catalog.mtconnect_recorder import federation_update as module

    assert (module.REPOSITORY_ROOT / "start_recorder.py").is_file()
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "Path(__file__).resolve().parents[2]" in source
    assert "os.environ" not in source


def test_a_host_agent_failure_never_ends_capture(tmp_path: Path) -> None:
    from catalog.mtconnect_recorder.federation_update import (
        RecorderHostUpdateAgentWorker,
    )

    class _Exploding:
        def poll_once(self) -> bool:
            raise RuntimeError("git unavailable")

    worker = RecorderHostUpdateAgentWorker(
        data_directory=tmp_path,
        environment={SUPERVISOR_SESSION_ENV: SUPERVISOR},
        agent=_Exploding(),
        poll_seconds=0.01,
    )
    worker._stop.set()

    worker._run()


def test_the_agent_script_forwards_recorder_arguments_as_single_tokens(
    tmp_path: Path,
) -> None:
    """``--recorder-arg --data-dir`` would be parsed as an option name."""

    import subprocess
    import sys

    from catalog.mtconnect_recorder.federation_update import REPOSITORY_ROOT

    script = REPOSITORY_ROOT / "scripts" / "fcp_native_recorder_update_agent.py"
    # An absolute directory outside the checkout: resolving a relative one would
    # write updater state into the repository and dirty the working tree.
    data_directory = tmp_path / "recorder-data"
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--repo-root",
            str(REPOSITORY_ROOT),
            "--supervisor-session",
            SUPERVISOR,
            "--recorder-arg=--data-dir",
            f"--recorder-arg={data_directory}",
            "--finalize",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPOSITORY_ROOT,
        timeout=120,
    )

    # No pending activation, so it refuses -- but it parsed and resolved.
    assert completed.returncode == 1, completed.stderr
    assert "no_pending_activation" in completed.stdout
    assert (data_directory / "federation" / "recorder-update-agent").is_dir()
    # Nothing was written inside the checkout.
    assert not (REPOSITORY_ROOT / "recorder-data").exists()


# --------------------------------------------------------------------------
# a failed branch trial ends its own capture, and an operator still wins
# --------------------------------------------------------------------------


def _stop() -> Any:
    from catalog.mtconnect_recorder.federation_update import RecorderTrialStop

    return RecorderTrialStop()


def test_a_failed_trial_stops_capture_cooperatively_and_asks_for_a_restart(
    clean_stop_state: None,
) -> None:
    """The trial process ends its own capture; nothing force-kills anything."""

    target = _StopTarget()
    recorder_runtime.register_stop_target(target)  # type: ignore[arg-type]
    stop = _stop()

    assert stop.request() is True

    assert target.calls == [{"signum": None, "external": True}]
    assert stop.latched is True
    assert stop.delivered is True
    assert recorder_runtime.last_stop_reason() == recorder_runtime.EXTERNAL_STOP_REASON
    assert stop.restart_required() is True


def test_an_operator_stop_during_a_trial_is_never_turned_into_a_restart(
    clean_stop_state: None,
) -> None:
    recorder_runtime.register_stop_target(_StopTarget())  # type: ignore[arg-type]
    stop = _stop()
    stop.request()

    # Ctrl+C after the trial latched its failure. An operator stop always wins.
    recorder_runtime._record_stop_reason(recorder_runtime.SIGNAL_STOP_REASON)

    assert stop.restart_required() is False


def test_a_trial_that_never_started_capture_interrupts_its_own_launcher(
    clean_stop_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A branch that hangs before capture exists still has to be recoverable."""

    from catalog.mtconnect_recorder import federation_update as module

    interrupted: list[bool] = []
    monkeypatch.setattr(module, "interrupt_main", lambda: interrupted.append(True))
    stop = _stop()

    assert stop.request() is False

    assert interrupted == [True]
    assert stop.latched is True
    assert stop.delivered is False
    # Nothing else stopped this process, so the fallback is still owed.
    assert stop.restart_required() is True


def test_an_undelivered_trial_stop_never_overrides_an_operator_signal(
    clean_stop_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from catalog.mtconnect_recorder import federation_update as module

    monkeypatch.setattr(module, "interrupt_main", lambda: None)
    stop = _stop()
    stop.request()

    recorder_runtime._record_stop_reason(recorder_runtime.SIGNAL_STOP_REASON)

    assert stop.restart_required() is False


def test_a_trial_that_was_never_latched_never_asks_for_a_restart(
    clean_stop_state: None,
) -> None:
    assert _stop().restart_required() is False


def test_a_supervised_child_uses_the_production_root_it_was_given(
    tmp_path: Path,
) -> None:
    """A trial child runs from a worktree, so its own location is not the one."""

    from catalog.mtconnect_recorder.native_identity import (
        PRODUCTION_ROOT_ENV,
        production_root,
    )

    production = tmp_path / "v2"
    (production / ".git").mkdir(parents=True)

    assert production_root({PRODUCTION_ROOT_ENV: str(production)}) == production
    # Anything that is not a real checkout is refused rather than believed.
    assert production_root({PRODUCTION_ROOT_ENV: str(tmp_path / "missing")}) is None
    assert production_root({PRODUCTION_ROOT_ENV: ""}) is None
    assert production_root({}) is None
