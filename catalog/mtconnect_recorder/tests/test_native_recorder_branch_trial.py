"""Branch-trial regressions for the native standalone MTConnect recorder.

These drive the real state machine against a real Git checkout: real commits,
a real remote, a real ``ls-remote``, a real ``fetch`` and a real ``git worktree``
holding a real detached commit. Only three things are simulated, each at the
smallest seam the shipped code offers: whether a PID is alive, whether the
fixture's local origin counts as the approved repository, and the bounded
interpreter probe (which cannot import a product that is not in the fixture).

Everything else below -- what is refused before capture stops, what is pinned,
what the supervisor is told to launch, what counts as a healthy startup, and
what happens when it is not healthy -- is evidence about the shipped path.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from catalog.federation.software_trial import (
    ROLLBACK_FAILED,
    ROLLBACK_VERIFYING,
    SAFE_RESTORED,
    TRIAL_FAILED,
    TRIAL_RUNNING,
    TRIAL_STARTING,
    TRIAL_STARTUP_TIMEOUT_SECONDS,
    TrialRefused,
    TrialSelection,
    trial_request_document,
)
from catalog.federation.software_update import (
    APPROVED_REPOSITORY,
    BRANCH_RE,
    GitUpdateAdapter,
)
from catalog.mtconnect_recorder import native_trial, native_update, native_update_agent
from catalog.mtconnect_recorder.native_identity import (
    NATIVE_RUNTIME_SCHEMA,
    NATIVE_RUNTIME_TYPE,
)
from catalog.mtconnect_recorder.native_trial import (
    TRIAL_ROOT_SUFFIX,
    TrialWorktrees,
    evaluate_startup,
    trial_root_for,
)
from catalog.mtconnect_recorder.native_trial_agent import (
    PROBE_CODE,
    NativeRecorderTrialAgent,
)
from catalog.mtconnect_recorder.native_update import (
    NativeRecorderUpdatePaths,
    stamp,
    utc_now,
)
from catalog.mtconnect_recorder.native_update_agent import NativeRecorderUpdateAgent

SUPERVISOR = "a" * 32
RECORDER_NONCE = "b" * 32
TRIAL_NONCE = "c" * 32
ROLLBACK_NONCE = "d" * 32
RECORDER_PID = 4242
TRIAL_PID = 5252
ROLLBACK_PID = 6262
TRIAL_BRANCH = "fix/new-recorder-behaviour"


def _git(*args: str, cwd: Path) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    return completed.stdout.strip()


class _FixtureAdapter(GitUpdateAdapter):
    """The production adapter with the fixture's local origin allowed.

    Only the repository allow-list is widened. Branch discovery, fetch,
    ancestry, status and the fast-forward remain the shipped implementation.
    """

    @staticmethod
    def _approved_remote(value: str) -> bool:
        return bool(value.strip())


class Clock:
    """A controllable ``now`` so bounded windows can be tested without waiting."""

    def __init__(self) -> None:
        self.value = utc_now()

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value = self.value + timedelta(seconds=seconds)


class Probe:
    """Stand in for the bounded interpreter probe, and record how it was run."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.returncode = 0

    def __call__(self, argv: list[str], **kwargs: Any):
        self.calls.append({"argv": list(argv), **kwargs})
        return subprocess.CompletedProcess(argv, self.returncode, "", "")


class Fixture:
    def __init__(self, tmp_path: Path) -> None:
        self.remote = tmp_path / "remote.git"
        self.root = tmp_path / "recorder"
        self.data = tmp_path / "recorder-data"
        _git("init", "-q", "--bare", str(self.remote), cwd=tmp_path)
        _git("symbolic-ref", "HEAD", "refs/heads/main", cwd=self.remote)
        _git("init", "-q", str(self.root), cwd=tmp_path)
        for name, value in (
            ("user.email", "recorder@example.invalid"),
            ("user.name", "FCP recorder"),
            ("commit.gpgsign", "false"),
        ):
            _git("config", name, value, cwd=self.root)
        (self.root / "start_recorder.py").write_text("one\n", encoding="utf-8")
        _git("add", "-A", cwd=self.root)
        _git("commit", "-qm", "one", cwd=self.root)
        _git("branch", "-M", "main", cwd=self.root)
        _git("remote", "add", "origin", str(self.remote), cwd=self.root)
        _git("push", "-q", "origin", "main", cwd=self.root)
        self.safe = _git("rev-parse", "--verify", "HEAD^{commit}", cwd=self.root)

        # A published test branch, and a later main, both only on the remote.
        self.upstream = tmp_path / "upstream"
        _git("clone", "-q", str(self.remote), str(self.upstream), cwd=tmp_path)
        for name, value in (
            ("user.email", "ci@example.invalid"),
            ("user.name", "FCP ci"),
            ("commit.gpgsign", "false"),
        ):
            _git("config", name, value, cwd=self.upstream)
        _git("checkout", "-q", "-b", TRIAL_BRANCH, cwd=self.upstream)
        (self.upstream / "start_recorder.py").write_text("trial\n", encoding="utf-8")
        _git("add", "-A", cwd=self.upstream)
        _git("commit", "-qm", "trial", cwd=self.upstream)
        _git("push", "-q", "origin", TRIAL_BRANCH, cwd=self.upstream)
        self.trial = _git("rev-parse", "--verify", "HEAD^{commit}", cwd=self.upstream)

        self.paths = NativeRecorderUpdatePaths(self.data)
        self.paths.directory.mkdir(parents=True, exist_ok=True)
        self.paths.status_file.parent.mkdir(parents=True, exist_ok=True)
        self.clock = Clock()
        self.probe = Probe()

    # -- the approved remote moves on, without the trial noticing -----------

    def publish_new_main(self) -> str:
        _git("checkout", "-q", "main", cwd=self.upstream)
        (self.upstream / "start_recorder.py").write_text("two\n", encoding="utf-8")
        _git("add", "-A", cwd=self.upstream)
        _git("commit", "-qm", "two", cwd=self.upstream)
        _git("push", "-q", "origin", "main", cwd=self.upstream)
        return _git("rev-parse", "--verify", "HEAD^{commit}", cwd=self.upstream)

    def publish_dependency_change(self) -> str:
        _git("checkout", "-q", TRIAL_BRANCH, cwd=self.upstream)
        (self.upstream / "requirements.txt").write_text("new==1.0\n", encoding="utf-8")
        _git("add", "-A", cwd=self.upstream)
        _git("commit", "-qm", "dependency change", cwd=self.upstream)
        _git("push", "-q", "origin", TRIAL_BRANCH, cwd=self.upstream)
        return _git("rev-parse", "--verify", "HEAD^{commit}", cwd=self.upstream)

    def head(self) -> str:
        return _git("rev-parse", "--verify", "HEAD^{commit}", cwd=self.root)

    def branch(self) -> str:
        return _git("symbolic-ref", "--short", "HEAD", cwd=self.root)

    def is_clean(self) -> bool:
        return not _git(
            "status", "--porcelain=v1", "--untracked-files=all", cwd=self.root
        )

    # -- recorder heartbeat -------------------------------------------------

    def write_status(
        self,
        *,
        pid: int = RECORDER_PID,
        nonce: str | None = RECORDER_NONCE,
        supervisor: str | None = SUPERVISOR,
        commit: str | None = None,
        heartbeat_offset: float = 0.0,
        federation_status: str = "connected",
        state: str = "recording",
    ) -> None:
        native_update.write_json_atomic(
            self.paths.status_file,
            {
                "schema": "fcp.mtconnect_recorder.status.v2",
                "heartbeat_at": stamp(
                    self.clock() + timedelta(seconds=heartbeat_offset)
                ),
                "state": state,
                "native_runtime": {
                    "schema": NATIVE_RUNTIME_SCHEMA,
                    "runtime_type": NATIVE_RUNTIME_TYPE,
                    "pid": pid,
                    "process_nonce": nonce,
                    "supervisor_session": supervisor,
                    "build_commit": commit or self.safe,
                },
                "federation": {
                    "status": federation_status,
                    "node_id": "node-recorder",
                    "session_id": "session-a",
                },
            },
        )

    def write_pending(self, request_id: str, target: str) -> None:
        native_update.write_json_atomic(
            self.paths.federation_state_file,
            {
                "schema": "fcp.federation-update-processor.v1",
                "last_revision": 4,
                "pending": {
                    "fed-request": {
                        "kind": "trial",
                        "host_request_id": request_id,
                        "target_commit": target,
                    }
                },
            },
        )

    # -- agent under test ---------------------------------------------------

    def agent(self, *, stopper: Any = None) -> NativeRecorderUpdateAgent:
        adapter = _FixtureAdapter(self.root)
        trial = NativeRecorderTrialAgent(
            repository_root=self.root,
            paths=self.paths,
            supervisor_session=SUPERVISOR,
            adapter=adapter,
            now=self.clock,
            interpreter="python-under-test",
            runner=self.probe,
            request_capture_stop=stopper,
        )
        return NativeRecorderUpdateAgent(
            repository_root=self.root,
            data_directory=self.data,
            supervisor_session=SUPERVISOR,
            adapter=adapter,
            now=self.clock,
            sleep=lambda _seconds: None,
            trial=trial,
        )

    def queue_trial(
        self,
        request_id: str,
        *,
        branch: str = TRIAL_BRANCH,
        commit: str | None = None,
    ) -> None:
        selection = TrialSelection(APPROVED_REPOSITORY, branch, commit or self.trial)
        native_update.write_json_atomic(
            self.paths.request_file,
            trial_request_document(
                request_id=request_id,
                selection=selection,
                created_at=self.clock(),
            ),
        )

    def trial_result(self, request_id: str) -> dict[str, Any] | None:
        import hashlib

        digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
        return native_update.read_json(
            self.paths.directory / f"trial-result-{digest}.json"
        )


@pytest.fixture()
def fixture(tmp_path: Path) -> Fixture:
    return Fixture(tmp_path)


@pytest.fixture()
def alive(monkeypatch: pytest.MonkeyPatch) -> set[int]:
    """Control process liveness at the one seam the real probe would use."""

    live: set[int] = {RECORDER_PID}

    def probe(pid: object) -> bool:
        return isinstance(pid, int) and pid in live

    monkeypatch.setattr(native_update, "process_is_running", probe)
    monkeypatch.setattr(native_update_agent, "process_is_running", probe)
    monkeypatch.setattr(native_trial, "process_is_running", probe)
    from catalog.mtconnect_recorder import native_trial_agent

    monkeypatch.setattr(native_trial_agent, "process_is_running", probe)
    return live


class Stopper:
    """Record the trial's request that its own capture end."""

    def __init__(self, fixture: Fixture, alive: set[int]) -> None:
        self.fixture = fixture
        self.alive = alive
        self.calls = 0

    def __call__(self) -> bool:
        self.calls += 1
        # The real recorder finishes its cycle and exits; here the process
        # simply stops being alive, which is the only thing the agent observes.
        self.alive.discard(TRIAL_PID)
        return True


def _start_trial(fixture: Fixture, request_id: str = "req-trial") -> dict[str, Any]:
    """Drive prevalidation and the stop request, leaving capture stopped."""

    fixture.write_status()
    fixture.write_pending(request_id, fixture.trial)
    fixture.queue_trial(request_id)
    assert fixture.agent().poll_once() is True
    record = native_trial.TrialJournal(fixture.paths.trial_journal_file).active()
    assert record is not None
    return record


def _launch_trial(fixture: Fixture, alive: set[int]) -> dict[str, object]:
    """Let the supervisor take the trial from a stopped recorder to a child."""

    alive.discard(RECORDER_PID)
    agent = fixture.agent()
    plan = agent.finalize_after_exit()
    assert agent.mark_relaunched(TRIAL_NONCE) is True
    alive.add(TRIAL_PID)
    # Time passes while a child starts. A replacement's heartbeat has to be
    # newer than the activation, so a heartbeat written at the same instant is
    # correctly not yet proof of anything.
    fixture.clock.advance(1)
    return plan


# --------------------------------------------------------------------------
# the branch list
# --------------------------------------------------------------------------


def test_the_branch_list_comes_from_the_approved_repository(
    fixture: Fixture,
) -> None:
    listed = _FixtureAdapter(fixture.root).approved_branches()

    assert [item.name for item in listed] == ["main", TRIAL_BRANCH]
    # Approved main is always first, and every entry carries an exact commit.
    assert listed[0].is_approved_branch
    assert listed[0].commit == fixture.safe
    assert {item.commit for item in listed} == {fixture.safe, fixture.trial}


def test_an_unapproved_origin_publishes_no_branches_at_all(
    fixture: Fixture,
) -> None:
    """The shipped allow-list, not the fixture's widened one."""

    with pytest.raises(RuntimeError) as refused:
        GitUpdateAdapter(fixture.root).approved_branches()

    assert str(refused.value) == "unapproved_remote"


def test_branch_names_git_would_not_accept_are_never_offered() -> None:
    for hostile in (
        "--upload-pack=/bin/sh",
        "-oProxyCommand=curl evil",
        "../../etc/passwd",
        "main;rm -rf /",
        "refs/heads/../../evil",
        "branch@{0}",
        "https://evil.example/repo.git",
    ):
        assert not BRANCH_RE.fullmatch(hostile)


def test_a_peer_can_only_name_a_repository_branch_and_commit(
    fixture: Fixture,
) -> None:
    """There is no field a URL, path, command or environment value fits in."""

    document = trial_request_document(
        request_id="req-shape",
        selection=TrialSelection(APPROVED_REPOSITORY, TRIAL_BRANCH, fixture.trial),
        created_at=utc_now(),
    )

    assert set(document) == {
        "schema",
        "request_id",
        "action",
        "repository",
        "branch",
        "target_commit",
        "created_at",
        "expires_at",
    }
    for hostile in (
        {"repository": "attacker/evil"},
        {"repository": "https://evil.example/repo.git"},
        {"branch": "--upload-pack=/bin/sh"},
        {"target_commit": "$(rm -rf /)"},
        {"target_commit": "HEAD"},
    ):
        with pytest.raises(TrialRefused):
            native_update_agent.validate_trial_request(
                {**document, **hostile}, now=utc_now()
            )


# --------------------------------------------------------------------------
# prevalidation: every refusal happens while capture is still recording
# --------------------------------------------------------------------------


def _refusal(fixture: Fixture, request_id: str = "req-trial", **kwargs: Any) -> dict:
    fixture.write_status()
    fixture.write_pending(request_id, kwargs.get("commit") or fixture.trial)
    fixture.queue_trial(request_id, **kwargs)
    fixture.agent().poll_once()
    result = fixture.trial_result(request_id)
    assert result is not None
    return result


def test_a_dirty_checkout_refuses_before_the_recorder_is_stopped(
    fixture: Fixture, alive: set[int]
) -> None:
    (fixture.root / "operator-notes.txt").write_text("edit\n", encoding="utf-8")

    result = _refusal(fixture)

    assert result["state"] == "dirty"
    assert not fixture.paths.activation_file.exists()
    assert fixture.head() == fixture.safe
    assert not trial_root_for(fixture.root).exists()


def test_a_branch_that_does_not_exist_refuses_before_the_recorder_is_stopped(
    fixture: Fixture, alive: set[int]
) -> None:
    result = _refusal(fixture, branch="feat/never-pushed")

    assert result["code"] == "branch_unavailable"
    assert not fixture.paths.activation_file.exists()
    assert fixture.head() == fixture.safe


def test_a_commit_not_on_the_named_branch_refuses_before_the_stop(
    fixture: Fixture, alive: set[int]
) -> None:
    """A stale dropdown must fail closed, not run an unpublished object."""

    other = fixture.publish_new_main()

    result = _refusal(fixture, branch=TRIAL_BRANCH, commit=other)

    assert result["code"] == "target_not_on_branch"
    assert not fixture.paths.activation_file.exists()


def test_a_detached_or_off_main_checkout_refuses_before_the_stop(
    fixture: Fixture, alive: set[int]
) -> None:
    _git("checkout", "-q", "--detach", "HEAD", cwd=fixture.root)

    result = _refusal(fixture)

    assert result["code"] == "detached_head"
    assert not fixture.paths.activation_file.exists()


def test_a_dependency_change_refuses_before_the_recorder_is_stopped(
    fixture: Fixture, alive: set[int]
) -> None:
    changed = fixture.publish_dependency_change()

    result = _refusal(fixture, commit=changed)

    assert result["code"] == "dependency_change_requires_manual_trial"
    assert not fixture.paths.activation_file.exists()
    assert fixture.head() == fixture.safe


def test_an_interpreter_that_cannot_load_the_branch_refuses_before_the_stop(
    fixture: Fixture, alive: set[int]
) -> None:
    fixture.probe.returncode = 1

    result = _refusal(fixture)

    assert result["code"] == "trial_interpreter_unusable"
    assert not fixture.paths.activation_file.exists()
    assert fixture.head() == fixture.safe


def test_a_recorder_whose_commit_disagrees_with_its_checkout_is_refused(
    fixture: Fixture, alive: set[int]
) -> None:
    """No exact known-good commit could be pinned, so no trial may start."""

    request_id = "req-mismatch"
    fixture.write_status(commit="f" * 40)
    fixture.write_pending(request_id, fixture.trial)
    fixture.queue_trial(request_id)

    fixture.agent().poll_once()

    assert fixture.trial_result(request_id)["code"] == "running_commit_mismatch"
    assert not fixture.paths.activation_file.exists()


def test_the_probe_runs_in_the_trial_worktree_with_no_peer_supplied_input(
    fixture: Fixture, alive: set[int]
) -> None:
    _start_trial(fixture)

    assert len(fixture.probe.calls) == 1
    call = fixture.probe.calls[0]
    # Locally resolved interpreter, a local constant probe body, and the trial
    # worktree as the working directory. Nothing a peer said reaches any of it.
    assert call["argv"] == ["python-under-test", "-c", PROBE_CODE]
    assert Path(call["cwd"]).parent.name.endswith(TRIAL_ROOT_SUFFIX)
    assert call["shell"] is False
    # The probe is pointed at the production data directory, so it validates the
    # configuration the real launch uses and cannot create one in the worktree.
    assert call["env"]["FCP_RECORDER_DATA_DIR"] == str(fixture.data.resolve())
    assert "FCP_RECORDER_SUPERVISOR_SESSION" not in call["env"]
    assert not (Path(call["cwd"]) / "data").exists()


# --------------------------------------------------------------------------
# pinning the fallback, and the worktree that makes it free
# --------------------------------------------------------------------------


def test_the_exact_running_commit_is_pinned_before_anything_stops(
    fixture: Fixture, alive: set[int]
) -> None:
    record = _start_trial(fixture)

    assert record["safe_branch"] == "main"
    assert record["safe_commit"] == fixture.safe
    assert record["safe_root"] == str(fixture.root)
    assert record["trial_branch"] == TRIAL_BRANCH
    assert record["trial_commit"] == fixture.trial
    assert record["stage"] == TRIAL_STARTING
    # Pinned strictly before the stop request: the activation is what ends
    # capture, and it is written after the record exists.
    assert fixture.paths.activation_file.exists()


def test_the_fallback_stays_the_pinned_commit_when_main_moves(
    fixture: Fixture, alive: set[int]
) -> None:
    """Rollback must not follow approved main forward mid-trial."""

    record = _start_trial(fixture)
    moved = fixture.publish_new_main()
    assert moved != fixture.safe

    _launch_trial(fixture, alive)
    agent = fixture.agent()
    fixture.write_status(
        pid=TRIAL_PID, nonce=TRIAL_NONCE, commit=fixture.trial,
        federation_status="connecting",
    )
    fixture.clock.advance(TRIAL_STARTUP_TIMEOUT_SECONDS + 1)
    agent.trial.verify_once()
    alive.discard(TRIAL_PID)
    plan = fixture.agent().finalize_after_exit()

    assert plan["mode"] == "rollback"
    assert plan["build_commit"] == record["safe_commit"] == fixture.safe
    assert plan["build_commit"] != moved


def test_the_trial_runs_from_a_sibling_worktree_and_never_moves_the_checkout(
    fixture: Fixture, alive: set[int]
) -> None:
    record = _start_trial(fixture)
    trial_root = Path(str(record["trial_root"]))

    # A sibling, never inside the checkout: an in-tree worktree would make
    # ``status --untracked-files=all`` permanently dirty and disable updates.
    assert trial_root.parent == trial_root_for(fixture.root)
    assert fixture.root not in trial_root.parents
    assert trial_root.is_dir()
    assert (
        _git("rev-parse", "--verify", "HEAD^{commit}", cwd=trial_root)
        == fixture.trial
    )
    # The production checkout is untouched, still on main, still clean, still
    # the exact known-good commit.
    assert fixture.head() == fixture.safe
    assert fixture.branch() == "main"
    assert fixture.is_clean()


def test_the_supervisor_is_told_to_launch_the_trial_worktree_with_real_data(
    fixture: Fixture, alive: set[int]
) -> None:
    record = _start_trial(fixture)

    plan = _launch_trial(fixture, alive)

    assert plan["relaunch"] is True
    assert plan["mode"] == "trial"
    assert plan["launch_root"] == record["trial_root"]
    assert plan["build_commit"] == fixture.trial
    # The *production* data directory: identity, pairing, checkpoints,
    # recordings, outbox and backlog never move for a trial.
    assert plan["data_directory"] == str(fixture.data.resolve())


def test_the_checkout_is_never_fast_forwarded_for_a_trial(
    fixture: Fixture, alive: set[int]
) -> None:
    fixture.publish_new_main()
    _start_trial(fixture)

    _launch_trial(fixture, alive)

    assert fixture.head() == fixture.safe
    assert fixture.branch() == "main"


def test_the_trial_is_not_launched_while_the_recorder_is_still_running(
    fixture: Fixture, alive: set[int]
) -> None:
    _start_trial(fixture)

    plan = fixture.agent().finalize_after_exit()

    assert plan["relaunch"] is False
    assert plan["code"] == "recorder_still_running"


# --------------------------------------------------------------------------
# startup acceptance: a process existing is never success
# --------------------------------------------------------------------------


def _verify(
    fixture: Fixture,
    alive: set[int],
    *,
    expire: bool = True,
    **status: Any,
) -> dict[str, Any]:
    fixture.write_status(pid=TRIAL_PID, nonce=TRIAL_NONCE, **status)
    if expire:
        fixture.clock.advance(TRIAL_STARTUP_TIMEOUT_SECONDS + 1)
    stopper = Stopper(fixture, alive)
    fixture.agent(stopper=stopper).trial.verify_once()
    journal = native_trial.TrialJournal(fixture.paths.trial_journal_file)
    record = journal.active() or journal.latest()
    assert record is not None
    return record


def test_a_proven_replacement_keeps_the_trial_branch_running(
    fixture: Fixture, alive: set[int]
) -> None:
    _start_trial(fixture)
    _launch_trial(fixture, alive)

    record = _verify(fixture, alive, commit=fixture.trial, expire=False)

    assert record["stage"] == TRIAL_RUNNING
    result = fixture.trial_result("req-trial")
    assert result["state"] == TRIAL_RUNNING
    assert result["target_commit"] == fixture.trial
    assert result["branch"] == TRIAL_BRANCH
    # The fallback stays visible while the trial runs.
    assert result["safe_commit"] == fixture.safe
    assert result["acceptance"]["accepted"] is True
    assert result["acceptance"]["failures"] == []


@pytest.mark.parametrize(
    ("status", "condition"),
    [
        ({"commit": "e" * 40}, "expected_commit"),
        ({"commit": None}, "expected_commit"),
        ({"federation_status": "connecting"}, "federation_connected"),
        ({"state": "error"}, "recorder_healthy"),
        ({"state": "stopped"}, "recorder_healthy"),
        ({"heartbeat_offset": -10_000}, "fresh_heartbeat"),
    ],
)
def test_every_required_startup_condition_is_actually_required(
    fixture: Fixture,
    alive: set[int],
    status: dict[str, Any],
    condition: str,
) -> None:
    _start_trial(fixture)
    _launch_trial(fixture, alive)
    status = {"commit": fixture.trial, **status}

    record = _verify(fixture, alive, **status)

    assert record["stage"] == TRIAL_FAILED
    assert condition in record["acceptance"]["failures"]


def test_a_survivor_is_never_accepted_as_the_trial_process(
    fixture: Fixture, alive: set[int]
) -> None:
    """Same nonce, same PID: the old recorder, not a replacement."""

    _start_trial(fixture)
    _launch_trial(fixture, alive)
    alive.add(RECORDER_PID)

    fixture.write_status(
        pid=RECORDER_PID, nonce=RECORDER_NONCE, commit=fixture.trial
    )
    fixture.clock.advance(TRIAL_STARTUP_TIMEOUT_SECONDS + 1)
    fixture.agent(stopper=Stopper(fixture, alive)).trial.verify_once()

    journal = native_trial.TrialJournal(fixture.paths.trial_journal_file)
    record = journal.active()
    assert record["stage"] == TRIAL_FAILED
    assert "distinct_instance" in record["acceptance"]["failures"]


def test_a_process_the_supervisor_did_not_start_is_not_the_trial(
    fixture: Fixture, alive: set[int]
) -> None:
    _start_trial(fixture)
    _launch_trial(fixture, alive)
    alive.add(9999)

    fixture.write_status(pid=9999, nonce="f" * 32, commit=fixture.trial)
    fixture.clock.advance(TRIAL_STARTUP_TIMEOUT_SECONDS + 1)
    fixture.agent(stopper=Stopper(fixture, alive)).trial.verify_once()

    record = native_trial.TrialJournal(fixture.paths.trial_journal_file).active()
    assert "distinct_instance" in record["acceptance"]["failures"]


def test_a_trial_is_never_failed_for_a_fault_the_safe_version_already_had(
    fixture: Fixture, alive: set[int]
) -> None:
    """A recorder whose source was already unreachable must still be testable."""

    request_id = "req-baseline"
    fixture.write_status(state="starting")
    fixture.write_pending(request_id, fixture.trial)
    fixture.queue_trial(request_id)
    fixture.agent().poll_once()
    _launch_trial(fixture, alive)

    record = _verify(
        fixture, alive, commit=fixture.trial, state="starting", expire=False
    )

    assert record["stage"] == TRIAL_RUNNING


def test_a_recorder_that_is_not_running_at_all_is_never_healthy() -> None:
    """The baseline allowance never stretches to "no recorder"."""

    status = native_update.RecorderRuntimeStatus(
        pid=None,
        process_nonce=None,
        supervisor_session=SUPERVISOR,
        build_commit="a" * 40,
        heartbeat_at=utc_now(),
        state="error",
        federation_status="connected",
        federation_node_id=None,
        federation_session_id=None,
    )

    acceptance = evaluate_startup(
        status,
        native_trial.StartupExpectation(
            expected_commit="a" * 40,
            supervisor_session=SUPERVISOR,
            activated_at=utc_now() - timedelta(seconds=5),
            deadline_at=utc_now(),
            baseline_state="error",
        ),
    )

    assert acceptance.accepted is False
    assert "recorder_healthy" in acceptance.failures()


# --------------------------------------------------------------------------
# automatic, verified fallback
# --------------------------------------------------------------------------


def _fail_trial(fixture: Fixture, alive: set[int]) -> Stopper:
    """Take a trial all the way to "this branch did not come back"."""

    _start_trial(fixture)
    _launch_trial(fixture, alive)
    fixture.write_status(
        pid=TRIAL_PID,
        nonce=TRIAL_NONCE,
        commit=fixture.trial,
        federation_status="connecting",
    )
    fixture.clock.advance(TRIAL_STARTUP_TIMEOUT_SECONDS + 1)
    stopper = Stopper(fixture, alive)
    assert fixture.agent(stopper=stopper).trial.verify_once() is True
    return stopper


def test_a_failed_startup_stops_the_trial_and_asks_for_the_safe_version(
    fixture: Fixture, alive: set[int]
) -> None:
    stopper = _fail_trial(fixture, alive)

    # The trial process ends its *own* capture. Nothing force-kills anything.
    assert stopper.calls == 1
    record = native_trial.TrialJournal(fixture.paths.trial_journal_file).active()
    assert record["stage"] == TRIAL_FAILED
    assert "Federation membership did not reconnect." in record["failure_reason"]

    plan = fixture.agent().finalize_after_exit()

    assert plan["relaunch"] is True
    assert plan["mode"] == "rollback"
    assert plan["launch_root"] == str(fixture.root)
    assert plan["build_commit"] == fixture.safe
    assert plan["data_directory"] == str(fixture.data.resolve())


def test_a_trial_that_crashes_outright_still_gets_the_safe_version_back(
    fixture: Fixture, alive: set[int]
) -> None:
    """The common case: the branch cannot even start, so nothing self-reports."""

    _start_trial(fixture)
    _launch_trial(fixture, alive)
    alive.discard(TRIAL_PID)

    plan = fixture.agent().finalize_after_exit()

    assert plan["relaunch"] is True
    assert plan["mode"] == "rollback"
    assert plan["build_commit"] == fixture.safe


def test_the_safe_version_is_never_started_beside_a_live_trial_process(
    fixture: Fixture, alive: set[int]
) -> None:
    _start_trial(fixture)
    _launch_trial(fixture, alive)
    fixture.write_status(pid=TRIAL_PID, nonce=TRIAL_NONCE, commit=fixture.trial)

    plan = fixture.agent().finalize_after_exit()

    assert plan["relaunch"] is False
    assert plan["code"] == "trial_still_running"


def _rollback(fixture: Fixture, alive: set[int]) -> NativeRecorderUpdateAgent:
    _fail_trial(fixture, alive)
    agent = fixture.agent()
    plan = agent.finalize_after_exit()
    assert plan["mode"] == "rollback"
    assert agent.mark_relaunched(ROLLBACK_NONCE) is True
    alive.add(ROLLBACK_PID)
    fixture.clock.advance(1)
    return agent


def test_the_restored_safe_version_must_prove_itself_before_it_counts(
    fixture: Fixture, alive: set[int]
) -> None:
    agent = _rollback(fixture, alive)
    record = native_trial.TrialJournal(fixture.paths.trial_journal_file).active()
    assert record["stage"] == ROLLBACK_VERIFYING

    fixture.write_status(
        pid=ROLLBACK_PID, nonce=ROLLBACK_NONCE, commit=fixture.safe
    )
    assert agent.trial.verify_once() is True

    result = fixture.trial_result("req-trial")
    assert result["state"] == SAFE_RESTORED
    assert result["safe_commit"] == fixture.safe
    assert result["recovery"] == {
        "trial_process_stopped": True,
        "safe_version_started": True,
        "recorder_running": True,
        "federation_connected": True,
        "recording_resumed": True,
        "safe_branch": "main",
        "safe_commit": fixture.safe,
    }
    assert "Federation membership did not reconnect." in result["message"]


def test_the_rollback_must_be_a_different_process_instance(
    fixture: Fixture, alive: set[int]
) -> None:
    """Reviving the failed trial process is not a restored safe version."""

    agent = _rollback(fixture, alive)
    alive.add(TRIAL_PID)

    fixture.write_status(pid=TRIAL_PID, nonce=TRIAL_NONCE, commit=fixture.safe)
    fixture.clock.advance(TRIAL_STARTUP_TIMEOUT_SECONDS + 1)
    agent.trial.verify_once()

    result = fixture.trial_result("req-trial")
    assert result["state"] == ROLLBACK_FAILED
    assert "distinct_instance" in result["acceptance"]["failures"]


def test_the_restored_version_must_report_the_exact_pinned_commit(
    fixture: Fixture, alive: set[int]
) -> None:
    agent = _rollback(fixture, alive)

    fixture.write_status(
        pid=ROLLBACK_PID, nonce=ROLLBACK_NONCE, commit=fixture.trial
    )
    fixture.clock.advance(TRIAL_STARTUP_TIMEOUT_SECONDS + 1)
    agent.trial.verify_once()

    result = fixture.trial_result("req-trial")
    assert result["state"] == ROLLBACK_FAILED
    assert "expected_commit" in result["acceptance"]["failures"]


def test_a_failed_rollback_is_reported_explicitly_and_never_looped(
    fixture: Fixture, alive: set[int]
) -> None:
    agent = _rollback(fixture, alive)

    fixture.write_status(
        pid=ROLLBACK_PID,
        nonce=ROLLBACK_NONCE,
        commit=fixture.safe,
        state="error",
        federation_status="offline",
    )
    fixture.clock.advance(TRIAL_STARTUP_TIMEOUT_SECONDS + 1)
    agent.trial.verify_once()

    result = fixture.trial_result("req-trial")
    assert result["state"] == ROLLBACK_FAILED
    assert result["code"] == "rollback_failed"
    assert result["recovery"]["recording_resumed"] is False
    assert result["recovery"]["safe_version_started"] is False
    # Terminal: the journal holds no active trial to retry, so nothing loops.
    assert native_trial.TrialJournal(fixture.paths.trial_journal_file).active() is None
    assert fixture.agent().finalize_after_exit()["relaunch"] is False


def test_rollback_refuses_when_the_pinned_checkout_is_no_longer_intact(
    fixture: Fixture, alive: set[int]
) -> None:
    _fail_trial(fixture, alive)
    (fixture.root / "someone-edited-this.txt").write_text("x\n", encoding="utf-8")

    plan = fixture.agent().finalize_after_exit()

    assert plan["relaunch"] is False
    assert plan["code"] == "rollback_failed"
    assert fixture.trial_result("req-trial")["state"] == ROLLBACK_FAILED


def test_an_operator_stop_during_a_trial_relaunches_nothing(
    fixture: Fixture, alive: set[int]
) -> None:
    _start_trial(fixture)
    _launch_trial(fixture, alive)
    agent = fixture.agent()
    assert agent.trial.mark_operator_stopped() is True
    alive.discard(TRIAL_PID)

    plan = fixture.agent().finalize_after_exit()

    assert plan["relaunch"] is False
    assert plan["code"] == "trial_operator_stopped"


# --------------------------------------------------------------------------
# returning to the pinned version, and refusing to stack trials
# --------------------------------------------------------------------------


def _running_trial(fixture: Fixture, alive: set[int]) -> None:
    _start_trial(fixture)
    _launch_trial(fixture, alive)
    fixture.write_status(pid=TRIAL_PID, nonce=TRIAL_NONCE, commit=fixture.trial)
    assert fixture.agent().trial.verify_once() is True


def test_a_second_trial_cannot_stack_on_an_unproven_one(
    fixture: Fixture, alive: set[int]
) -> None:
    _running_trial(fixture, alive)
    other = fixture.publish_new_main()
    fixture.write_pending("req-second", other)
    fixture.queue_trial("req-second", branch="main", commit=other)

    fixture.agent().poll_once()

    assert (
        fixture.trial_result("req-second")["code"]
        == "return_to_pinned_version_required"
    )


def test_an_operator_can_return_a_device_to_its_pinned_version(
    fixture: Fixture, alive: set[int]
) -> None:
    _running_trial(fixture, alive)
    fixture.write_pending("req-restore", fixture.safe)
    fixture.queue_trial("req-restore", branch="main", commit=fixture.safe)

    fixture.agent().poll_once()

    record = native_trial.TrialJournal(fixture.paths.trial_journal_file).active()
    assert record["stage"] == TRIAL_STARTING
    assert record["restore"] is True
    alive.discard(TRIAL_PID)
    agent = fixture.agent()
    plan = agent.finalize_after_exit()
    assert plan["mode"] == "rollback"
    assert plan["build_commit"] == fixture.safe
    assert agent.mark_relaunched(ROLLBACK_NONCE) is True
    alive.add(ROLLBACK_PID)
    fixture.clock.advance(1)
    fixture.write_status(
        pid=ROLLBACK_PID, nonce=ROLLBACK_NONCE, commit=fixture.safe
    )
    assert agent.trial.verify_once() is True
    assert fixture.trial_result("req-restore")["state"] == SAFE_RESTORED


def test_an_update_is_refused_while_a_device_runs_a_test_branch(
    fixture: Fixture, alive: set[int]
) -> None:
    """Fast-forwarding a checkout nothing is running from is never an update."""

    _running_trial(fixture, alive)
    target = fixture.publish_new_main()
    native_update.write_json_atomic(
        fixture.paths.request_file,
        {
            "schema": native_update.REQUEST_SCHEMA,
            "request_id": "req-update",
            "action": "apply",
            "repository": APPROVED_REPOSITORY,
            "branch": "main",
            "target_commit": target,
            "created_at": stamp(fixture.clock()),
            "expires_at": stamp(fixture.clock() + timedelta(seconds=120)),
            "activate_after": stamp(fixture.clock()),
        },
    )

    fixture.agent().poll_once()

    import hashlib

    digest = hashlib.sha256(b"req-update").hexdigest()
    result = native_update.read_json(
        fixture.paths.directory / f"result-{digest}.json"
    )
    assert result["code"] == "trial_active"
    assert fixture.head() == fixture.safe


def test_a_check_reports_the_test_branch_instead_of_claiming_up_to_date(
    fixture: Fixture, alive: set[int]
) -> None:
    _running_trial(fixture, alive)

    result = fixture.agent().handle_check("req-check", None)

    assert result["state"] == "trial_running"
    assert result["code"] == "trial_active"
    assert result["current_commit"] == fixture.safe
    assert result["running_commit"] == fixture.trial
    assert result["trial"]["branch"] == TRIAL_BRANCH
    assert result["trial"]["safe_commit"] == fixture.safe
    assert result["trial"]["active"] is True


# --------------------------------------------------------------------------
# what Git is ever asked to do, and what recorder data is ever touched
# --------------------------------------------------------------------------


def _audited_agent(
    fixture: Fixture, seen: list[list[str]]
) -> NativeRecorderUpdateAgent:
    def record(argv, **kwargs):  # type: ignore[no-untyped-def]
        seen.append(list(argv))
        kwargs.pop("check", None)
        return subprocess.run(argv, check=False, **kwargs)

    adapter = _FixtureAdapter(fixture.root, runner=record)
    trial = NativeRecorderTrialAgent(
        repository_root=fixture.root,
        paths=fixture.paths,
        supervisor_session=SUPERVISOR,
        adapter=adapter,
        now=fixture.clock,
        interpreter="python-under-test",
        runner=fixture.probe,
    )
    return NativeRecorderUpdateAgent(
        repository_root=fixture.root,
        data_directory=fixture.data,
        supervisor_session=SUPERVISOR,
        adapter=adapter,
        now=fixture.clock,
        sleep=lambda _seconds: None,
        trial=trial,
    )


def test_a_trial_never_asks_git_to_reset_clean_stash_or_move_the_checkout(
    fixture: Fixture, alive: set[int], monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[list[str]] = []
    worktree: list[list[str]] = []

    real_git_output = native_trial.git_output

    def audited(root, arguments, **kwargs):  # type: ignore[no-untyped-def]
        worktree.append(list(arguments))
        return real_git_output(root, arguments, **kwargs)

    monkeypatch.setattr(native_trial, "git_output", audited)

    fixture.write_status()
    fixture.write_pending("req-audit", fixture.trial)
    fixture.queue_trial("req-audit")
    _audited_agent(fixture, seen).poll_once()
    alive.discard(RECORDER_PID)
    _audited_agent(fixture, seen).finalize_after_exit()

    forbidden = {
        "reset",
        "clean",
        "stash",
        "checkout",
        "rebase",
        "push",
        "filter-branch",
        "merge",
        "switch",
    }
    assert seen, "a trial must actually run Git"
    for command in seen:
        assert command[0] == "git"
        assert not forbidden.intersection(command), command
    # The only worktree command is an additive detached add; nothing is removed
    # with force and nothing touches the production working tree.
    adds = [item for item in worktree if item[:1] == ["worktree"]]
    assert [item[:4] for item in adds] == [
        ["worktree", "add", "--detach", "--"]
    ]
    assert adds[0][-1] == fixture.trial
    assert not any("--force" in item for item in worktree)
    assert fixture.head() == fixture.safe
    assert fixture.branch() == "main"
    assert fixture.is_clean()


def test_a_trial_and_its_rollback_never_touch_durable_recorder_data(
    fixture: Fixture, alive: set[int]
) -> None:
    """Everything the recorder owns must survive a failed trial byte for byte."""

    preserved = {
        "federation/device/identity.json": '{"identity":"keep"}',
        "federation/onboarding/remote_pairing.json": '{"pairing":"keep"}',
        "federation/recorder_publication/outbox.sqlite3": "outbox-bytes",
        "federation/recorder_publication/backlog.sqlite3": "backlog-bytes",
        "source_state/mtconnect_recorder_state.json": '{"checkpoint":42}',
        "raw/2026/observations.jsonl": '{"observation":1}',
        "capability_config.json": '{"sources":{"Mazak":"http://x:5000"}}',
    }
    for relative, content in preserved.items():
        path = fixture.data / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    agent = _rollback(fixture, alive)
    fixture.write_status(
        pid=ROLLBACK_PID, nonce=ROLLBACK_NONCE, commit=fixture.safe
    )
    assert agent.trial.verify_once() is True

    assert fixture.trial_result("req-trial")["state"] == SAFE_RESTORED
    for relative, content in preserved.items():
        assert (fixture.data / relative).read_text(encoding="utf-8") == content
    # The trial journal is a separate file; the update journal is untouched.
    assert fixture.paths.trial_journal_file != fixture.paths.journal_file


def test_a_trial_worktree_holding_a_data_directory_is_never_removed(
    fixture: Fixture, alive: set[int]
) -> None:
    """Recorder data is never in a trial tree by design; if it were, stop."""

    worktrees = TrialWorktrees(fixture.root)
    kept: list[Path] = []
    for _ in range(native_trial.MAX_RETAINED_TRIALS + 2):
        identifier = native_trial.new_trial_id()
        kept.append(worktrees.create(identifier, fixture.safe))
    for path in kept:
        (path / "data").mkdir(parents=True, exist_ok=True)
        (path / "data" / "keep.json").write_text("{}", encoding="utf-8")

    worktrees.prune()

    for path in kept:
        assert (path / "data" / "keep.json").read_text(encoding="utf-8") == "{}"


# --------------------------------------------------------------------------
# the existing main updater is unchanged
# --------------------------------------------------------------------------


def test_an_ordinary_main_update_still_fast_forwards_and_verifies(
    fixture: Fixture, alive: set[int]
) -> None:
    """The path branch trials must not have changed, exercised end to end."""

    target = fixture.publish_new_main()
    fixture.write_status()
    fixture.write_pending("req-plain", target)
    native_update.write_json_atomic(
        fixture.paths.request_file,
        {
            "schema": native_update.REQUEST_SCHEMA,
            "request_id": "req-plain",
            "action": "apply",
            "repository": APPROVED_REPOSITORY,
            "branch": "main",
            "target_commit": target,
            "created_at": stamp(fixture.clock()),
            "expires_at": stamp(fixture.clock() + timedelta(seconds=120)),
            "activate_after": stamp(fixture.clock()),
        },
    )

    fixture.agent().poll_once()
    assert fixture.paths.activation_file.exists()
    assert fixture.head() == fixture.safe

    alive.discard(RECORDER_PID)
    agent = fixture.agent()
    plan = agent.finalize_after_exit()

    assert plan["relaunch"] is True
    assert plan["mode"] == "update"
    # No trial ever ran, so the plan names nothing and the supervisor keeps its
    # existing behaviour exactly.
    assert plan["launch_root"] is None
    assert plan["data_directory"] is None
    assert plan["build_commit"] is None
    assert fixture.head() == target

    assert agent.mark_relaunched(TRIAL_NONCE) is True
    alive.add(TRIAL_PID)
    fixture.clock.advance(1)
    fixture.write_status(pid=TRIAL_PID, nonce=TRIAL_NONCE, commit=target)
    agent.resume()

    import hashlib

    digest = hashlib.sha256(b"req-plain").hexdigest()
    result = native_update.read_json(
        fixture.paths.directory / f"result-{digest}.json"
    )
    assert result["state"] == "runtime_verified"
    # A device that has never run a trial reports no trial summary at all.
    assert "trial" not in result


def test_an_update_request_can_never_name_a_branch_other_than_main(
    fixture: Fixture,
) -> None:
    """The update request contract is untouched by branch trials."""

    document = {
        "schema": native_update.REQUEST_SCHEMA,
        "request_id": "req-branch",
        "action": "apply",
        "repository": APPROVED_REPOSITORY,
        "branch": TRIAL_BRANCH,
        "target_commit": fixture.trial,
        "created_at": stamp(utc_now()),
        "expires_at": stamp(utc_now() + timedelta(seconds=120)),
        "activate_after": stamp(utc_now()),
    }

    with pytest.raises(native_update_agent.UpdateRefused) as refused:
        native_update_agent.validate_request(document)

    assert refused.value.code == "unapproved_source"


def test_a_trial_request_is_never_accepted_as_an_update_request(
    fixture: Fixture,
) -> None:
    document = trial_request_document(
        request_id="req-cross",
        selection=TrialSelection(APPROVED_REPOSITORY, TRIAL_BRANCH, fixture.trial),
        created_at=utc_now(),
    )

    with pytest.raises(native_update_agent.UpdateRefused):
        native_update_agent.validate_request(document)


def test_a_trial_and_an_update_can_never_run_at_the_same_time(
    fixture: Fixture, alive: set[int]
) -> None:
    _start_trial(fixture)
    target = fixture.publish_new_main()
    fixture.write_pending("req-collide", target)

    result = fixture.agent().handle_apply("req-collide", target)

    assert result["code"] == "trial_active"
    assert fixture.head() == fixture.safe


# --------------------------------------------------------------------------
# a trial can always be got out of
# --------------------------------------------------------------------------


def test_a_normal_restart_returns_the_device_to_main_and_says_so(
    fixture: Fixture, alive: set[int]
) -> None:
    """The supervisor relaunches from the production checkout on any restart.

    The journal still records the trial as the last thing that happened, but
    the device is demonstrably back on the pinned commit -- so it must not keep
    reporting a test branch, and must not stay excluded from updates.
    """

    _running_trial(fixture, alive)
    assert fixture.agent().active_trial() is not None

    # The operator restarted the recorder; the supervisor launched main.
    alive.discard(TRIAL_PID)
    alive.add(RECORDER_PID)
    fixture.clock.advance(1)
    fixture.write_status(pid=RECORDER_PID, nonce=RECORDER_NONCE, commit=fixture.safe)
    agent = fixture.agent()

    assert agent.active_trial() is None
    result = agent.handle_check("req-after-restart", None)
    assert result["state"] != "trial_running"
    assert result["trial"]["active"] is False
    assert result["trial"]["branch"] == TRIAL_BRANCH


def test_an_activation_that_never_took_effect_releases_the_trial(
    fixture: Fixture, alive: set[int]
) -> None:
    """A recorder that never consents must not block trials and updates forever."""

    _start_trial(fixture)
    fixture.clock.advance(native_update.ACTIVATION_TTL_SECONDS + 1)

    assert fixture.agent().trial.verify_once() is True

    assert native_trial.TrialJournal(fixture.paths.trial_journal_file).active() is None
    assert fixture.trial_result("req-trial")["code"] == "trial_activation_expired"
    # Capture was never interrupted and the checkout never moved.
    assert fixture.head() == fixture.safe
    assert fixture.branch() == "main"


class _BrokenGit:
    """A Git that is simply not there, at the seam the adapter would use."""

    def __call__(self, *_args: Any, **_kwargs: Any):
        raise FileNotFoundError("git")


def _broken_agent(fixture: Fixture) -> NativeRecorderUpdateAgent:
    adapter = _FixtureAdapter(fixture.root, runner=_BrokenGit())
    trial = NativeRecorderTrialAgent(
        repository_root=fixture.root,
        paths=fixture.paths,
        supervisor_session=SUPERVISOR,
        adapter=adapter,
        now=fixture.clock,
        interpreter="python-under-test",
        runner=fixture.probe,
    )
    return NativeRecorderUpdateAgent(
        repository_root=fixture.root,
        data_directory=fixture.data,
        supervisor_session=SUPERVISOR,
        adapter=adapter,
        now=fixture.clock,
        sleep=lambda _seconds: None,
        trial=trial,
    )


def test_git_being_unavailable_is_a_refusal_rather_than_a_crash(
    fixture: Fixture, alive: set[int]
) -> None:
    fixture.write_status()
    fixture.write_pending("req-nogit", fixture.trial)
    fixture.queue_trial("req-nogit")

    assert _broken_agent(fixture).poll_once() is True

    result = fixture.trial_result("req-nogit")
    assert result["code"] == "trial_prevalidation_failed"
    assert not fixture.paths.activation_file.exists()


def test_a_plan_that_cannot_be_computed_never_ends_the_supervisor_loop(
    fixture: Fixture, alive: set[int]
) -> None:
    _fail_trial(fixture, alive)

    plan = _broken_agent(fixture).finalize_after_exit()

    assert plan["relaunch"] is False
    assert plan["code"].startswith("trial_plan_failed_")
    assert plan["launch_root"] is None
