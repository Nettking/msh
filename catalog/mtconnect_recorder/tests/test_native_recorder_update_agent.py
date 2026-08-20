"""Native standalone-recorder host update lifecycle regressions.

These tests drive the real state machine against a real Git checkout: real
commits, a real remote, real ``fetch``/``merge --ff-only`` and real ancestry.
Only two things are simulated, and both are simulated at the smallest possible
seam: whether a PID is alive, and whether the fixture's local origin counts as
the approved repository. Every ordering and refusal property below is therefore
evidence about the shipped code path, not about a mock.
"""

from __future__ import annotations

import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from catalog.federation.software_update import GitUpdateAdapter
from catalog.mtconnect_recorder import native_update, native_update_agent
from catalog.mtconnect_recorder.native_identity import (
    NATIVE_RUNTIME_SCHEMA,
    NATIVE_RUNTIME_TYPE,
)
from catalog.mtconnect_recorder.native_update import (
    STAGE_COMPLETED,
    STAGE_FAILED,
    STAGE_SOURCE_UPDATED,
    STAGE_STOP_REQUESTED,
    NativeRecorderUpdatePaths,
    stamp,
    utc_now,
)
from catalog.mtconnect_recorder.native_update_agent import (
    NativeRecorderUpdateAgent,
    UpdateRefused,
)

SUPERVISOR = "a" * 32
RECORDER_NONCE = "b" * 32
REPLACEMENT_NONCE = "c" * 32
RECORDER_PID = 4242
REPLACEMENT_PID = 5252


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

    Only the repository allow-list is widened. Fetch, ancestry, status and the
    fast-forward itself remain exactly the shipped implementation, so ordering
    and mutation properties are still tested against real Git.
    """

    @staticmethod
    def _approved_remote(value: str) -> bool:
        return bool(value.strip())


class Fixture:
    def __init__(self, tmp_path: Path) -> None:
        self.remote = tmp_path / "remote.git"
        self.root = tmp_path / "recorder"
        self.data = tmp_path / "recorder-data"
        _git("init", "-q", "--bare", str(self.remote), cwd=tmp_path)
        _git("symbolic-ref", "HEAD", "refs/heads/main", cwd=self.remote)
        _git("init", "-q", str(self.root), cwd=tmp_path)
        _git("config", "user.email", "recorder@example.invalid", cwd=self.root)
        _git("config", "user.name", "FCP recorder", cwd=self.root)
        _git("config", "commit.gpgsign", "false", cwd=self.root)
        (self.root / "start_recorder.py").write_text("one\n", encoding="utf-8")
        _git("add", "-A", cwd=self.root)
        _git("commit", "-qm", "one", cwd=self.root)
        _git("branch", "-M", "main", cwd=self.root)
        _git("remote", "add", "origin", str(self.remote), cwd=self.root)
        _git("push", "-q", "origin", "main", cwd=self.root)
        self.current = _git("rev-parse", "--verify", "HEAD^{commit}", cwd=self.root)

        # A second commit published on approved main but not yet checked out.
        self.upstream = tmp_path / "upstream"
        _git("clone", "-q", str(self.remote), str(self.upstream), cwd=tmp_path)
        _git("config", "user.email", "ci@example.invalid", cwd=self.upstream)
        _git("config", "user.name", "FCP ci", cwd=self.upstream)
        _git("config", "commit.gpgsign", "false", cwd=self.upstream)
        (self.upstream / "start_recorder.py").write_text("two\n", encoding="utf-8")
        _git("add", "-A", cwd=self.upstream)
        _git("commit", "-qm", "two", cwd=self.upstream)
        _git("push", "-q", "origin", "main", cwd=self.upstream)
        self.target = _git(
            "rev-parse", "--verify", "HEAD^{commit}", cwd=self.upstream
        )

        self.paths = NativeRecorderUpdatePaths(self.data)
        self.paths.directory.mkdir(parents=True, exist_ok=True)
        self.paths.status_file.parent.mkdir(parents=True, exist_ok=True)

    def publish_dependency_change(self) -> str:
        (self.upstream / "requirements.txt").write_text("new-pin==1.0\n", encoding="utf-8")
        _git("add", "-A", cwd=self.upstream)
        _git("commit", "-qm", "dependency change", cwd=self.upstream)
        _git("push", "-q", "origin", "main", cwd=self.upstream)
        return _git("rev-parse", "--verify", "HEAD^{commit}", cwd=self.upstream)

    def head(self) -> str:
        return _git("rev-parse", "--verify", "HEAD^{commit}", cwd=self.root)

    def write_status(
        self,
        *,
        pid: int = RECORDER_PID,
        nonce: str | None = RECORDER_NONCE,
        supervisor: str | None = SUPERVISOR,
        commit: str | None = None,
        heartbeat_offset: float = 0.0,
        federation_status: str = "connected",
    ) -> None:
        native_update.write_json_atomic(
            self.paths.status_file,
            {
                "schema": "fcp.mtconnect_recorder.status.v2",
                "heartbeat_at": stamp(
                    utc_now() + timedelta(seconds=heartbeat_offset)
                ),
                "state": "recording",
                "native_runtime": {
                    "schema": NATIVE_RUNTIME_SCHEMA,
                    "runtime_type": NATIVE_RUNTIME_TYPE,
                    "pid": pid,
                    "process_nonce": nonce,
                    "supervisor_session": supervisor,
                    "build_commit": commit or self.current,
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
                        "host_request_id": request_id,
                        "target_commit": target,
                    }
                },
            },
        )

    def agent(self, **kwargs: Any) -> NativeRecorderUpdateAgent:
        return NativeRecorderUpdateAgent(
            repository_root=self.root,
            data_directory=self.data,
            supervisor_session=SUPERVISOR,
            adapter=_FixtureAdapter(self.root),
            **kwargs,
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
    return live


def _request(action: str, request_id: str, target: str | None) -> dict[str, object]:
    now = utc_now()
    value: dict[str, object] = {
        "schema": native_update.REQUEST_SCHEMA,
        "request_id": request_id,
        "action": action,
        "repository": "Nettking/msh",
        "branch": "main",
        "target_commit": target,
        "created_at": stamp(now),
        "expires_at": stamp(now + timedelta(seconds=120)),
    }
    if action == "apply":
        value["activate_after"] = stamp(now)
    return value


def _queue(fixture: Fixture, action: str, request_id: str, target: str | None) -> None:
    native_update.write_json_atomic(
        fixture.paths.request_file,
        _request(action, request_id, target),
    )


def _result(fixture: Fixture, request_id: str) -> dict[str, Any] | None:
    import hashlib

    digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
    return native_update.read_json(fixture.paths.directory / f"result-{digest}.json")


# --------------------------------------------------------------------------
# check
# --------------------------------------------------------------------------


def test_check_reports_the_native_recorder_commit_not_a_container(
    fixture: Fixture, alive: set[int]
) -> None:
    """The physical recorder has no container to exec into for its commit."""

    fixture.write_status()
    _queue(fixture, "check", "req-check", fixture.target)

    assert fixture.agent().poll_once() is True

    result = _result(fixture, "req-check")
    assert result is not None
    assert result["state"] == "update_available"
    assert result["current_commit"] == fixture.current
    assert result["target_commit"] == fixture.target
    # Proven from the running native process's own heartbeat.
    assert result["running_commit"] == fixture.current


def test_check_never_reports_a_commit_for_a_dead_recorder(
    fixture: Fixture, alive: set[int]
) -> None:
    fixture.write_status()
    alive.clear()
    _queue(fixture, "check", "req-dead", fixture.target)

    fixture.agent().poll_once()

    assert _result(fixture, "req-dead")["running_commit"] is None


# --------------------------------------------------------------------------
# prevalidation: every refusal happens while capture is still running
# --------------------------------------------------------------------------


def _refusal(fixture: Fixture, request_id: str = "req-apply") -> dict[str, Any]:
    fixture.write_pending(request_id, fixture.target)
    _queue(fixture, "apply", request_id, fixture.target)
    fixture.agent().poll_once()
    result = _result(fixture, request_id)
    assert result is not None
    return result


def test_a_dirty_checkout_is_refused_before_capture_is_stopped(
    fixture: Fixture, alive: set[int]
) -> None:
    fixture.write_status()
    (fixture.root / "operator-notes.txt").write_text("local edit\n", encoding="utf-8")

    result = _refusal(fixture)

    assert result["state"] == "dirty"
    assert not fixture.paths.activation_file.exists()
    assert fixture.head() == fixture.current


def test_an_untracked_file_alone_is_enough_to_refuse(
    fixture: Fixture, alive: set[int]
) -> None:
    """``--untracked-files=all``: an unreviewed new file still blocks a merge."""

    fixture.write_status()
    (fixture.root / "unreviewed.txt").write_text("new\n", encoding="utf-8")

    result = _refusal(fixture)

    assert result["state"] == "dirty"
    assert not fixture.paths.activation_file.exists()


def test_a_branch_other_than_main_is_refused(
    fixture: Fixture, alive: set[int]
) -> None:
    fixture.write_status()
    _git("checkout", "-q", "-b", "operator-branch", cwd=fixture.root)

    result = _refusal(fixture)

    assert result["state"] == "unsupported_checkout"
    assert result["code"] == "detached_head"
    assert fixture.head() == fixture.current


def test_a_detached_head_is_refused(fixture: Fixture, alive: set[int]) -> None:
    fixture.write_status()
    _git("checkout", "-q", "--detach", "HEAD", cwd=fixture.root)

    result = _refusal(fixture)

    assert result["code"] == "detached_head"
    assert not fixture.paths.activation_file.exists()


def test_an_unapproved_origin_is_refused_by_the_shipped_adapter() -> None:
    """The fixture widens the allow-list; the shipped rule must still be strict."""

    approved = GitUpdateAdapter._approved_remote
    assert approved("https://github.com/Nettking/msh.git")
    assert not approved("https://github.com/attacker/msh.git")
    assert not approved("https://token@github.com/Nettking/msh.git")
    assert not approved("/tmp/somewhere/remote.git")


def test_a_target_that_is_not_on_approved_main_is_refused(
    fixture: Fixture, alive: set[int]
) -> None:
    fixture.write_status()
    _git("checkout", "-q", "-b", "unpublished", cwd=fixture.root)
    (fixture.root / "foreign.txt").write_text("foreign\n", encoding="utf-8")
    _git("add", "-A", cwd=fixture.root)
    _git("commit", "-qm", "foreign", cwd=fixture.root)
    foreign = _git("rev-parse", "--verify", "HEAD^{commit}", cwd=fixture.root)
    _git("checkout", "-q", "main", cwd=fixture.root)

    fixture.write_pending("req-foreign", foreign)
    _queue(fixture, "apply", "req-foreign", foreign)
    fixture.agent().poll_once()

    result = _result(fixture, "req-foreign")
    assert result["code"] == "target_unavailable"
    assert fixture.head() == fixture.current


def test_a_checkout_ahead_of_approved_main_is_refused(
    fixture: Fixture, alive: set[int]
) -> None:
    fixture.write_status()
    _git("fetch", "-q", "--no-tags", "origin", "main", cwd=fixture.root)
    _git("merge", "-q", "--ff-only", fixture.target, cwd=fixture.root)
    (fixture.root / "ahead.txt").write_text("ahead\n", encoding="utf-8")
    _git("add", "-A", cwd=fixture.root)
    _git("commit", "-qm", "ahead", cwd=fixture.root)
    ahead = fixture.head()
    fixture.write_status(commit=ahead)

    fixture.write_pending("req-ahead", fixture.target)
    _queue(fixture, "apply", "req-ahead", fixture.target)
    fixture.agent().poll_once()

    result = _result(fixture, "req-ahead")
    assert result["state"] == "ahead"
    assert fixture.head() == ahead


def test_a_diverged_checkout_is_refused(fixture: Fixture, alive: set[int]) -> None:
    fixture.write_status()
    (fixture.root / "diverged.txt").write_text("diverged\n", encoding="utf-8")
    _git("add", "-A", cwd=fixture.root)
    _git("commit", "-qm", "diverged", cwd=fixture.root)
    diverged = fixture.head()
    fixture.write_status(commit=diverged)

    fixture.write_pending("req-diverged", fixture.target)
    _queue(fixture, "apply", "req-diverged", fixture.target)
    fixture.agent().poll_once()

    result = _result(fixture, "req-diverged")
    assert result["state"] == "diverged"
    assert fixture.head() == diverged
    assert not fixture.paths.activation_file.exists()


def test_a_dependency_change_is_refused_before_capture_stops(
    fixture: Fixture, alive: set[int]
) -> None:
    """The one failure this path must never produce is a recorder that cannot
    restart. A changed dependency input is refused while capture is healthy."""

    dependency_target = fixture.publish_dependency_change()
    fixture.write_status()
    fixture.write_pending("req-deps", dependency_target)
    _queue(fixture, "apply", "req-deps", dependency_target)

    fixture.agent().poll_once()

    result = _result(fixture, "req-deps")
    assert result["code"] == "dependency_change_requires_manual_update"
    assert "requirements.txt" in result["message"]
    assert not fixture.paths.activation_file.exists()
    assert fixture.head() == fixture.current


def test_an_unsupervised_recorder_cannot_be_activated(
    fixture: Fixture, alive: set[int]
) -> None:
    fixture.write_status(supervisor=None)

    result = _refusal(fixture)

    assert result["code"] == "recorder_not_supervised"
    assert not fixture.paths.activation_file.exists()


def test_a_stale_heartbeat_cannot_be_activated(
    fixture: Fixture, alive: set[int]
) -> None:
    fixture.write_status(heartbeat_offset=-600)

    result = _refusal(fixture)

    assert result["code"] == "recorder_heartbeat_stale"
    assert not fixture.paths.activation_file.exists()


def test_an_update_with_no_pending_federation_request_is_refused(
    fixture: Fixture, alive: set[int]
) -> None:
    """A host request must correspond to real, already authenticated work."""

    fixture.write_status()
    _queue(fixture, "apply", "req-unbacked", fixture.target)

    # The bounded wait for the pending record is exhausted, not skipped.
    waits: list[float] = []
    fixture.agent(sleep=waits.append).poll_once()
    assert len(waits) == native_update_agent.PENDING_VISIBILITY_ATTEMPTS - 1

    result = _result(fixture, "req-unbacked")
    assert result["code"] == "no_pending_federation_update"
    assert not fixture.paths.activation_file.exists()


# --------------------------------------------------------------------------
# activation and stop-before-mutate
# --------------------------------------------------------------------------


def _activate(fixture: Fixture, request_id: str = "req-apply") -> dict[str, Any]:
    fixture.write_status()
    fixture.write_pending(request_id, fixture.target)
    _queue(fixture, "apply", request_id, fixture.target)
    agent = fixture.agent()
    agent.poll_once()
    intent = native_update.read_json(fixture.paths.activation_file)
    assert intent is not None
    return intent


def test_activation_correlates_request_target_supervisor_pid_and_nonce(
    fixture: Fixture, alive: set[int]
) -> None:
    intent = _activate(fixture)

    assert intent["schema"] == native_update.ACTIVATION_SCHEMA
    assert intent["request_id"] == "req-apply"
    assert intent["target_commit"] == fixture.target
    assert intent["supervisor_session"] == SUPERVISOR
    assert intent["recorder_pid"] == RECORDER_PID
    assert intent["process_nonce"] == RECORDER_NONCE
    assert native_update.parse_stamp(intent["expires_at"]) is not None


def test_requesting_a_stop_never_reports_success_and_never_mutates(
    fixture: Fixture, alive: set[int]
) -> None:
    """A queued activation is not an applied update."""

    _activate(fixture)

    assert fixture.head() == fixture.current
    # No terminal result yet: the update is only reported once a replacement
    # has actually been proven.
    assert _result(fixture, "req-apply") is None
    assert fixture.agent().journal.active()["stage"] == STAGE_STOP_REQUESTED


def test_the_checkout_is_not_fast_forwarded_while_the_recorder_still_runs(
    fixture: Fixture, alive: set[int]
) -> None:
    """Stop-before-mutate: the shipped ordering, proven against real Git."""

    _activate(fixture)
    agent = fixture.agent()

    outcome = agent.finalize_after_exit()

    assert outcome["relaunch"] is False
    assert outcome["code"] == "recorder_still_running"
    assert fixture.head() == fixture.current
    assert _result(fixture, "req-apply")["code"] == "recorder_still_running"


def test_the_checkout_is_fast_forwarded_only_after_the_process_exits(
    fixture: Fixture, alive: set[int]
) -> None:
    _activate(fixture)
    alive.discard(RECORDER_PID)
    agent = fixture.agent()

    outcome = agent.finalize_after_exit()

    assert outcome == {
        "relaunch": True,
        "code": "source_updated",
        "target_commit": fixture.target,
    }
    assert fixture.head() == fixture.target
    assert agent.journal.active()["stage"] == STAGE_SOURCE_UPDATED
    # A fast-forward alone is still not success.
    assert _result(fixture, "req-apply") is None


def test_the_fast_forward_is_the_only_mutation_git_ever_sees(
    fixture: Fixture, alive: set[int]
) -> None:
    """No reset, no clean, no stash, no checkout -- observed at the Git seam."""

    _activate(fixture)
    alive.discard(RECORDER_PID)

    seen: list[list[str]] = []

    def record(argv, **kwargs):  # type: ignore[no-untyped-def]
        seen.append(list(argv))
        kwargs.pop("check", None)
        return subprocess.run(argv, check=False, **kwargs)

    agent = NativeRecorderUpdateAgent(
        repository_root=fixture.root,
        data_directory=fixture.data,
        supervisor_session=SUPERVISOR,
        adapter=_FixtureAdapter(fixture.root, runner=record),
    )
    agent.finalize_after_exit()

    mutations = {"reset", "clean", "stash", "checkout", "rebase", "push", "filter-branch"}
    assert seen, "the finalize step must actually run Git"
    for command in seen:
        assert command[0] == "git"
        assert not mutations.intersection(command), command
    assert ["git", "merge", "--ff-only", fixture.target] in seen
    assert fixture.head() == fixture.target


# --------------------------------------------------------------------------
# replacement proof
# --------------------------------------------------------------------------


def _reach_source_updated(
    fixture: Fixture, alive: set[int]
) -> NativeRecorderUpdateAgent:
    _activate(fixture)
    alive.discard(RECORDER_PID)
    agent = fixture.agent()
    agent.finalize_after_exit()
    return agent


def test_a_survivor_is_never_accepted_as_a_replacement(
    fixture: Fixture, alive: set[int]
) -> None:
    agent = _reach_source_updated(fixture, alive)
    agent.mark_relaunched(REPLACEMENT_NONCE)
    # Same process, same nonce, now somehow reporting the new commit.
    alive.add(RECORDER_PID)
    fixture.write_status(commit=fixture.target)

    assert agent.resume() is False
    assert agent.journal.active()["stage"] != STAGE_COMPLETED
    assert _result(fixture, "req-apply") is None


def test_a_replacement_running_the_wrong_commit_is_not_success(
    fixture: Fixture, alive: set[int]
) -> None:
    agent = _reach_source_updated(fixture, alive)
    agent.mark_relaunched(REPLACEMENT_NONCE)
    alive.add(REPLACEMENT_PID)
    fixture.write_status(
        pid=REPLACEMENT_PID,
        nonce=REPLACEMENT_NONCE,
        commit=fixture.current,
    )

    assert agent.resume() is False
    assert _result(fixture, "req-apply") is None


def test_a_replacement_without_federation_membership_is_not_success(
    fixture: Fixture, alive: set[int]
) -> None:
    agent = _reach_source_updated(fixture, alive)
    agent.mark_relaunched(REPLACEMENT_NONCE)
    alive.add(REPLACEMENT_PID)
    fixture.write_status(
        pid=REPLACEMENT_PID,
        nonce=REPLACEMENT_NONCE,
        commit=fixture.target,
        federation_status="retrying",
    )

    assert agent.resume() is False
    assert _result(fixture, "req-apply") is None


def test_a_replacement_the_supervisor_did_not_start_is_not_success(
    fixture: Fixture, alive: set[int]
) -> None:
    agent = _reach_source_updated(fixture, alive)
    agent.mark_relaunched(REPLACEMENT_NONCE)
    alive.add(REPLACEMENT_PID)
    fixture.write_status(
        pid=REPLACEMENT_PID,
        nonce="d" * 32,
        commit=fixture.target,
    )

    assert agent.resume() is False
    assert _result(fixture, "req-apply") is None


def test_a_heartbeat_older_than_the_activation_is_not_success(
    fixture: Fixture, alive: set[int]
) -> None:
    agent = _reach_source_updated(fixture, alive)
    agent.mark_relaunched(REPLACEMENT_NONCE)
    alive.add(REPLACEMENT_PID)
    fixture.write_status(
        pid=REPLACEMENT_PID,
        nonce=REPLACEMENT_NONCE,
        commit=fixture.target,
        heartbeat_offset=-90,
    )

    assert agent.resume() is False
    assert _result(fixture, "req-apply") is None


def test_a_proven_replacement_reports_a_verified_runtime(
    fixture: Fixture, alive: set[int]
) -> None:
    agent = _reach_source_updated(fixture, alive)
    agent.mark_relaunched(REPLACEMENT_NONCE)
    alive.add(REPLACEMENT_PID)
    fixture.write_status(
        pid=REPLACEMENT_PID,
        nonce=REPLACEMENT_NONCE,
        commit=fixture.target,
    )

    assert agent.resume() is True

    result = _result(fixture, "req-apply")
    assert result["state"] == "runtime_verified"
    assert result["code"] == "updated"
    assert result["current_commit"] == fixture.target
    assert result["running_commit"] == fixture.target
    assert agent.journal.active() is None
    assert not fixture.paths.activation_file.exists()


# --------------------------------------------------------------------------
# idempotency, crash recovery and journal bounds
# --------------------------------------------------------------------------


def test_a_duplicate_request_never_updates_or_relaunches_twice(
    fixture: Fixture, alive: set[int]
) -> None:
    agent = _reach_source_updated(fixture, alive)
    agent.mark_relaunched(REPLACEMENT_NONCE)
    alive.add(REPLACEMENT_PID)
    fixture.write_status(
        pid=REPLACEMENT_PID, nonce=REPLACEMENT_NONCE, commit=fixture.target
    )
    agent.resume()
    first = _result(fixture, "req-apply")

    _queue(fixture, "apply", "req-apply", fixture.target)
    fixture.agent().poll_once()

    second = _result(fixture, "req-apply")
    assert second["state"] == "runtime_verified"
    assert second["completed_at"] == first["completed_at"]
    assert fixture.head() == fixture.target


def test_a_crash_before_the_stop_request_re_drives_prevalidation(
    fixture: Fixture, alive: set[int]
) -> None:
    """Nothing durable happened yet, so resuming is safe and must happen."""

    fixture.write_status()
    fixture.write_pending("req-crash", fixture.target)
    agent = fixture.agent()
    agent.journal.claim(request_id="req-crash", target_commit=fixture.target)
    fixture.paths.activation_file.unlink(missing_ok=True)

    assert fixture.agent().resume() is True

    intent = native_update.read_json(fixture.paths.activation_file)
    assert intent["request_id"] == "req-crash"
    assert fixture.head() == fixture.current


def test_a_crash_after_the_fast_forward_never_mutates_again(
    fixture: Fixture, alive: set[int]
) -> None:
    _reach_source_updated(fixture, alive)
    assert fixture.head() == fixture.target

    # A restarted supervisor calls finalize again after an interrupted cycle.
    outcome = fixture.agent().finalize_after_exit()

    assert outcome["relaunch"] is True
    assert fixture.head() == fixture.target
    assert fixture.agent().journal.active()["stage"] == STAGE_SOURCE_UPDATED


def test_a_superseded_activation_fails_instead_of_looping(
    fixture: Fixture, alive: set[int]
) -> None:
    """A new unrelated recorder must never inherit an old stop request."""

    _activate(fixture)
    alive.discard(RECORDER_PID)
    alive.add(REPLACEMENT_PID)
    # A different recorder started without the update ever activating.
    fixture.write_status(pid=REPLACEMENT_PID, nonce="e" * 32)

    assert fixture.agent().resume() is True

    result = _result(fixture, "req-apply")
    assert result["code"] == "activation_superseded"
    assert fixture.agent().journal.active() is None
    assert not fixture.paths.activation_file.exists()
    assert fixture.head() == fixture.current


def test_a_second_request_is_refused_while_one_activation_is_running(
    fixture: Fixture, alive: set[int]
) -> None:
    _activate(fixture)
    fixture.write_pending("req-second", fixture.target)
    _queue(fixture, "apply", "req-second", fixture.target)

    fixture.agent().poll_once()

    assert _result(fixture, "req-second")["code"] == "host_update_busy"
    assert fixture.head() == fixture.current


def test_the_journal_history_stays_bounded(fixture: Fixture, tmp_path: Path) -> None:
    journal = native_update.NativeUpdateJournal(tmp_path / "journal.json")
    for index in range(native_update.MAX_JOURNAL_HISTORY * 3):
        journal.claim(request_id=f"req-{index}", target_commit="f" * 40)
        journal.finish(STAGE_FAILED, result={"request_id": f"req-{index}"})

    stored = native_update.read_json(tmp_path / "journal.json")
    assert len(stored["history"]) == native_update.MAX_JOURNAL_HISTORY
    assert stored["active"] is None


def test_the_journal_records_every_declared_stage_in_order(
    fixture: Fixture, alive: set[int]
) -> None:
    stages: list[str] = []
    _activate(fixture)
    stages.append(fixture.agent().journal.active()["stage"])
    alive.discard(RECORDER_PID)
    agent = fixture.agent()
    agent.finalize_after_exit()
    stages.append(agent.journal.active()["stage"])
    agent.mark_relaunched(REPLACEMENT_NONCE)
    stages.append(agent.journal.active()["stage"])
    alive.add(REPLACEMENT_PID)
    fixture.write_status(
        pid=REPLACEMENT_PID, nonce=REPLACEMENT_NONCE, commit=fixture.target
    )
    agent.resume()

    assert stages == [
        STAGE_STOP_REQUESTED,
        STAGE_SOURCE_UPDATED,
        native_update.STAGE_REPLACEMENT_STARTED,
    ]
    history = native_update.read_json(fixture.paths.journal_file)["history"]
    assert history[-1]["stage"] == STAGE_COMPLETED


# --------------------------------------------------------------------------
# request validation
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ({"schema": "other"}, "malformed_message"),
        ({"repository": "attacker/msh"}, "unapproved_source"),
        ({"branch": "release"}, "unapproved_source"),
        ({"target_commit": "not-a-commit"}, "malformed_target"),
        ({"request_id": "../escape"}, "malformed_request_id"),
        ({"action": "run"}, "malformed_action"),
    ],
)
def test_malformed_requests_are_refused(
    mutation: dict[str, object], code: str
) -> None:
    request = _request("apply", "req-valid", "0" * 40)
    request.update(mutation)

    with pytest.raises(UpdateRefused) as refused:
        native_update_agent.validate_request(request)

    assert refused.value.code == code


def test_an_expired_request_is_refused() -> None:
    request = _request("apply", "req-old", "0" * 40)
    request["expires_at"] = stamp(utc_now() - timedelta(seconds=1))

    with pytest.raises(UpdateRefused) as refused:
        native_update_agent.validate_request(request)

    assert refused.value.code == "expired_or_invalid_request"


def test_a_request_carrying_a_command_cannot_smuggle_one(
    fixture: Fixture, alive: set[int]
) -> None:
    """Peer-supplied process shape is not merely rejected -- it is not read."""

    fixture.write_status()
    fixture.write_pending("req-smuggle", fixture.target)
    request = _request("apply", "req-smuggle", fixture.target)
    request.update(
        {
            "command": "powershell.exe",
            "arguments": ["-c", "whoami"],
            "python": "C:/evil/python.exe",
            "remote": "https://github.com/attacker/msh.git",
            "environment": {"PATH": "C:/evil"},
        }
    )
    native_update.write_json_atomic(fixture.paths.request_file, request)

    fixture.agent().poll_once()

    intent = native_update.read_json(fixture.paths.activation_file)
    assert set(intent) == {
        "schema",
        "request_id",
        "target_commit",
        "supervisor_session",
        "recorder_pid",
        "process_nonce",
        "created_at",
        "expires_at",
    }
    source = Path(native_update_agent.__file__).read_text(encoding="utf-8")
    for smuggled in ("command", "arguments", "environment"):
        assert f'value.get("{smuggled}")' not in source
        assert f'request.get("{smuggled}")' not in source


# --------------------------------------------------------------------------
# physical state is never touched
# --------------------------------------------------------------------------


def test_an_update_never_touches_capture_pairing_or_publication_state(
    fixture: Fixture, alive: set[int]
) -> None:
    """Everything durable the recorder owns must survive an update byte for byte."""

    preserved = {
        "federation/onboarding/remote_pairing.json": '{"pairing":"keep"}',
        "federation/device/identity.json": '{"identity":"keep"}',
        "federation/recorder_publication/outbox.sqlite3": "outbox-bytes",
        "source_state/mtconnect_recorder_state.json": '{"checkpoint":42}',
        "raw/2026/observations.jsonl": '{"observation":1}',
    }
    for relative, content in preserved.items():
        path = fixture.data / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    agent = _reach_source_updated(fixture, alive)
    agent.mark_relaunched(REPLACEMENT_NONCE)
    alive.add(REPLACEMENT_PID)
    fixture.write_status(
        pid=REPLACEMENT_PID, nonce=REPLACEMENT_NONCE, commit=fixture.target
    )
    agent.resume()

    assert _result(fixture, "req-apply")["state"] == "runtime_verified"
    for relative, content in preserved.items():
        assert (fixture.data / relative).read_text(encoding="utf-8") == content


def test_a_pending_record_written_moments_later_still_activates(
    fixture: Fixture, alive: set[int]
) -> None:
    """The recorder queues the host request before persisting its pending record.

    Refusing in that window would fail a real update for a write ordering the
    recorder cannot make atomic.
    """

    fixture.write_status()
    _queue(fixture, "apply", "req-late", fixture.target)
    writes: list[float] = []

    def late_write(seconds: float) -> None:
        writes.append(seconds)
        if len(writes) == 2:
            fixture.write_pending("req-late", fixture.target)

    fixture.agent(sleep=late_write).poll_once()

    intent = native_update.read_json(fixture.paths.activation_file)
    assert intent is not None
    assert intent["request_id"] == "req-late"
    assert _result(fixture, "req-late") is None
