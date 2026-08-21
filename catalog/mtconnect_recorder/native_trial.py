"""Durable local state and evidence for one native recorder branch trial.

A trial is the one operation that deliberately runs the recorder on something
other than approved ``main``, so every part of it is built to make the *return*
trivial rather than to make the departure clever:

``the production checkout never moves``
    A trial runs from a separate detached worktree, a sibling of the checkout.
    ``main`` stays checked out, clean, and at the exact commit that was working,
    for the whole trial. Rolling back therefore needs no Git operation at all --
    which is the point, because rollback happens while capture is stopped.
``the safe version is pinned, not looked up``
    The exact commit the running recorder *proved* in its own heartbeat is
    written down before anything stops. If approved main moves during the
    trial, rollback still returns to the commit that was actually working.
``a process is not a success``
    Startup acceptance is six independent conditions, and the recorder-health
    one is judged against how the known-good version was really behaving, so a
    trial is never failed for a fault the safe version already had.

No path, command, interpreter or environment value in this module comes from a
peer. A peer may name an approved repository branch and commit; the rest is
derived locally from the repository root.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from catalog.federation.software_trial import (
    OFF_MAIN_STAGES,
    ROLLBACK_FAILED,
    ROLLBACK_STARTING,
    ROLLBACK_VERIFYING,
    SAFE_RESTORED,
    TERMINAL_TRIAL_STAGES,
    TRIAL_FAILED,
    TRIAL_ID_RE,
    TRIAL_JOURNAL_SCHEMA,
    TRIAL_OPERATOR_STOPPED,
    TRIAL_PREPARING,
    TRIAL_RUNNING,
    TRIAL_STARTING,
    TRIAL_VERIFYING,
    TrialSelection,
)
from catalog.federation.software_update import BRANCH_RE, OID_RE

from .native_identity import NONCE_RE
from .native_update import (
    MAX_JOURNAL_HISTORY,
    RecorderRuntimeStatus,
    git_output,
    parse_stamp,
    process_is_running,
    read_json,
    stamp,
    utc_now,
    write_json_atomic,
)

#: Suffix of the sibling directory trial worktrees live in.
#:
#: Sibling, never a subdirectory: a worktree inside the checkout would make
#: ``git status --untracked-files=all`` permanently dirty, and the ordinary
#: updater refuses a dirty checkout -- so an in-tree trial directory would
#: quietly disable normal updates forever.
TRIAL_ROOT_SUFFIX = "-fcp-trials"

#: Retained trial worktrees. Older ones are pruned, but only when they hold no
#: data directory and Git itself is willing to remove them without ``--force``.
MAX_RETAINED_TRIALS = 3

#: How healthy a recorder state is. Anything unknown ranks as unhealthy.
RECORDER_STATE_RANK: dict[str, int] = {
    "error": 0,
    "stopped": 0,
    "starting": 1,
    "recording": 2,
}
HEALTHY_RECORDER_STATE = "recording"

#: Names of the six required startup conditions, in the order they are shown.
ACCEPTANCE_CONDITIONS: tuple[str, ...] = (
    "process_present",
    "distinct_instance",
    "expected_commit",
    "fresh_heartbeat",
    "federation_connected",
    "recorder_healthy",
)

_TRIAL_DIRECTORY_RE = re.compile(r"^[0-9a-f]{32}$")


def new_trial_id() -> str:
    """Generate a trial identity locally. Never accepted from a peer."""

    return uuid.uuid4().hex


def trial_root_for(repository_root: Path | str) -> Path:
    """Resolve the trial worktree root beside one repository checkout."""

    root = Path(repository_root).resolve()
    return root.with_name(root.name + TRIAL_ROOT_SUFFIX)


def state_rank(state: object) -> int:
    return RECORDER_STATE_RANK.get(state, 0) if isinstance(state, str) else 0


# ---------------------------------------------------------------------------
# startup acceptance
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StartupExpectation:
    """Exactly what a replacement process has to prove, and by when."""

    expected_commit: str
    supervisor_session: str
    activated_at: datetime
    deadline_at: datetime
    replacement_nonce: str | None = None
    previous_pid: int | None = None
    previous_nonce: str | None = None
    baseline_state: str | None = None


@dataclass(frozen=True)
class StartupAcceptance:
    """Per-condition evidence, so a failure can say which condition failed."""

    process_present: bool = False
    distinct_instance: bool = False
    expected_commit: bool = False
    fresh_heartbeat: bool = False
    federation_connected: bool = False
    recorder_healthy: bool = False
    observed_commit: str | None = None
    observed_state: str | None = None
    observed_pid: int | None = None
    observed_nonce: str | None = None

    @property
    def accepted(self) -> bool:
        return all(getattr(self, name) for name in ACCEPTANCE_CONDITIONS)

    def failures(self) -> tuple[str, ...]:
        return tuple(
            name for name in ACCEPTANCE_CONDITIONS if not getattr(self, name)
        )

    def to_dict(self) -> dict[str, object]:
        value: dict[str, object] = {
            name: bool(getattr(self, name)) for name in ACCEPTANCE_CONDITIONS
        }
        value.update(
            {
                "accepted": self.accepted,
                "failures": list(self.failures()),
                "observed_commit": self.observed_commit,
                "observed_state": self.observed_state,
                "observed_pid": self.observed_pid,
                "observed_nonce": self.observed_nonce,
            }
        )
        return value


def evaluate_startup(
    status: RecorderRuntimeStatus,
    expectation: StartupExpectation,
    *,
    now: datetime | None = None,
) -> StartupAcceptance:
    """Decide whether a replacement recorder has proven itself.

    Every condition is independent and none of them is "a Python process
    exists". ``distinct_instance`` in particular requires both a different PID
    and a different process-instance nonce than the process that was replaced,
    and -- when the supervisor recorded which child it started -- exactly that
    child's nonce.
    """

    moment = now or utc_now()
    present = status.present and status.is_running()
    distinct = bool(
        present
        and status.pid != expectation.previous_pid
        and status.process_nonce is not None
        and status.process_nonce != expectation.previous_nonce
        and (
            expectation.replacement_nonce is None
            or status.process_nonce == expectation.replacement_nonce
        )
        and status.supervisor_session == expectation.supervisor_session
    )
    commit_ok = bool(
        status.build_commit is not None
        and status.build_commit == expectation.expected_commit
    )
    heartbeat_ok = bool(
        status.is_fresh(now=moment)
        and status.heartbeat_at is not None
        and status.heartbeat_at > expectation.activated_at
    )
    federation_ok = bool(present and status.federation_is_healthy())
    required = min(
        state_rank(HEALTHY_RECORDER_STATE),
        state_rank(expectation.baseline_state),
    )
    observed = state_rank(status.state)
    # A trial is never failed for a fault the pinned safe version already had,
    # but "not running at all" is never healthy either.
    healthy = bool(present and observed > 0 and observed >= required)
    return StartupAcceptance(
        process_present=present,
        distinct_instance=distinct,
        expected_commit=commit_ok,
        fresh_heartbeat=heartbeat_ok,
        federation_connected=federation_ok,
        recorder_healthy=healthy,
        observed_commit=status.build_commit,
        observed_state=status.state,
        observed_pid=status.pid,
        observed_nonce=status.process_nonce,
    )


FAILURE_REASONS: dict[str, str] = {
    "process_present": "The replacement recorder process was not running.",
    "distinct_instance": (
        "No new supervised recorder instance replaced the previous one."
    ),
    "expected_commit": "The running recorder did not report the exact commit.",
    "fresh_heartbeat": (
        "Recorder heartbeat did not become healthy within {seconds} seconds."
    ),
    "federation_connected": "Federation membership did not reconnect.",
    "recorder_healthy": "The recorder did not reach a healthy recording state.",
}


def failure_reason(acceptance: StartupAcceptance, *, seconds: int) -> str:
    """One operator-readable sentence for the first unmet condition."""

    for name in acceptance.failures():
        return FAILURE_REASONS[name].format(seconds=seconds)
    return "The replacement recorder proved every required condition."


# ---------------------------------------------------------------------------
# durable journal
# ---------------------------------------------------------------------------


@dataclass
class TrialPlan:
    """What the supervisor is told to launch next. Entirely host-resolved."""

    relaunch: bool
    mode: str
    launch_root: str | None = None
    data_directory: str | None = None
    build_commit: str | None = None
    code: str = ""

    def to_dict(self) -> dict[str, object]:
        # One fixed shape for trial and update plans alike. The supervisor
        # reads this under strict mode, where a missing property is an error,
        # so every key is always present.
        return {
            "relaunch": self.relaunch,
            "mode": self.mode,
            "code": self.code,
            "target_commit": self.build_commit,
            "launch_root": self.launch_root,
            "data_directory": self.data_directory,
            "build_commit": self.build_commit,
        }


class TrialJournal:
    """Bounded durable record of one trial at a time, plus its history.

    Kept separate from the update journal on purpose. A trial and an update can
    never run together, and keeping the two records apart means adding trials
    could not change a single byte of what the existing updater reads or writes.
    """

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def _load(self) -> dict[str, Any]:
        value = read_json(self.path)
        if value is None or value.get("schema") != TRIAL_JOURNAL_SCHEMA:
            return {"schema": TRIAL_JOURNAL_SCHEMA, "active": None, "history": []}
        value.setdefault("active", None)
        history = value.get("history")
        value["history"] = history if isinstance(history, list) else []
        return value

    def _save(self, value: dict[str, Any]) -> None:
        history = value.get("history")
        if isinstance(history, list) and len(history) > MAX_JOURNAL_HISTORY:
            value["history"] = history[-MAX_JOURNAL_HISTORY:]
        write_json_atomic(self.path, value)

    def active(self) -> dict[str, Any] | None:
        record = self._load().get("active")
        return record if isinstance(record, dict) else None

    def history(self) -> list[dict[str, Any]]:
        return [
            record
            for record in self._load().get("history", [])
            if isinstance(record, dict)
        ]

    def latest(self) -> dict[str, Any] | None:
        """The active trial, or the most recent finished one."""

        value = self._load()
        active = value.get("active")
        if isinstance(active, dict):
            return active
        for record in reversed(value.get("history", [])):
            if isinstance(record, dict):
                return record
        return None

    def history_entry(self, request_id: str) -> dict[str, Any] | None:
        for record in reversed(self._load().get("history", [])):
            if isinstance(record, dict) and record.get("request_id") == request_id:
                return record
        return None

    @staticmethod
    def _active_of(value: dict[str, Any]) -> dict[str, Any] | None:
        record = value.get("active")
        return record if isinstance(record, dict) else None

    def open(self, record: dict[str, Any]) -> dict[str, Any]:
        value = self._load()
        active = self._active_of(value)
        if active is not None and active.get("request_id") == record.get(
            "request_id"
        ):
            return active
        if active is not None:
            raise RuntimeError("recorder_trial_already_active")
        moment = stamp(utc_now())
        opened = {**record, "created_at": moment, "updated_at": moment}
        value["active"] = opened
        self._save(value)
        return opened

    def advance(self, stage: str, **fields: object) -> dict[str, Any]:
        value = self._load()
        active = self._active_of(value)
        if active is None:
            raise RuntimeError("recorder_trial_journal_has_no_active_trial")
        active.update(fields)
        active["stage"] = stage
        active["updated_at"] = stamp(utc_now())
        value["active"] = active
        self._save(value)
        return active

    def finish(self, stage: str, **fields: object) -> dict[str, Any]:
        if stage not in TERMINAL_TRIAL_STAGES:
            raise ValueError("recorder_trial_stage_is_not_terminal")
        value = self._load()
        active = self._active_of(value)
        if active is None:
            raise RuntimeError("recorder_trial_journal_has_no_active_trial")
        active.update(fields)
        active["stage"] = stage
        active["updated_at"] = stamp(utc_now())
        history = value.get("history", [])
        history.append(active)
        value["history"] = history
        # ``trial_running`` keeps the device on the branch; the record is
        # retained in history as the durable answer to "what is this device
        # running, and what does it fall back to".
        value["active"] = None
        self._save(value)
        return active

    def discard_active(self) -> None:
        value = self._load()
        value["active"] = None
        self._save(value)


def running_trial(journal: TrialJournal) -> dict[str, Any] | None:
    """The trial whose branch this device is currently running, if any."""

    record = journal.latest()
    if record is None:
        return None
    return record if record.get("stage") in OFF_MAIN_STAGES else None


# ---------------------------------------------------------------------------
# worktrees
# ---------------------------------------------------------------------------


@dataclass
class TrialWorktrees:
    """Create and retire detached trial worktrees beside one checkout.

    Nothing here mutates the production working tree. ``git worktree add`` only
    writes the new directory and one administrative record under ``.git``, so a
    trial cannot make the production checkout dirty, move its ``HEAD``, or
    change what the ordinary updater sees.
    """

    repository_root: Path
    timeout: float = 60.0
    _root: Path = field(init=False)

    def __post_init__(self) -> None:
        self.repository_root = Path(self.repository_root).resolve()
        self._root = trial_root_for(self.repository_root)

    @property
    def root(self) -> Path:
        return self._root

    def path_for(self, trial_id: str) -> Path:
        if not TRIAL_ID_RE.fullmatch(trial_id):
            raise ValueError("malformed_trial_id")
        return self._root / trial_id

    def create(self, trial_id: str, commit: str) -> Path:
        """Check out one exact commit into a fresh detached worktree."""

        if not OID_RE.fullmatch(commit):
            raise ValueError("malformed_trial_commit")
        target = self.path_for(trial_id)
        self._root.mkdir(parents=True, exist_ok=True)
        code, _ = git_output(
            self.repository_root,
            ["worktree", "add", "--detach", "--", str(target), commit],
            timeout=self.timeout,
        )
        if code != 0:
            raise RuntimeError("trial_worktree_unavailable")
        head = self.head_commit(target)
        if head != commit:
            raise RuntimeError("trial_worktree_unavailable")
        return target

    def head_commit(self, path: Path) -> str | None:
        code, output = git_output(
            path,
            ["rev-parse", "--verify", "HEAD^{commit}"],
            timeout=self.timeout,
        )
        value = output.strip().lower()
        return value if code == 0 and OID_RE.fullmatch(value) else None

    def prune(self, *, keep: str | None = None) -> None:
        """Retire old trial worktrees conservatively.

        Never ``--force`` and never a directory that holds a ``data`` folder.
        A worktree Git will not remove cleanly is simply left on disk: leaking a
        few megabytes is not a failure mode worth risking a delete for.
        """

        git_output(
            self.repository_root, ["worktree", "prune"], timeout=self.timeout
        )
        if not self._root.is_dir():
            return
        candidates = sorted(
            (
                child
                for child in self._root.iterdir()
                if child.is_dir()
                and _TRIAL_DIRECTORY_RE.fullmatch(child.name)
                and child.name != keep
            ),
            key=lambda child: child.stat().st_mtime,
        )
        excess = len(candidates) + (1 if keep else 0) - MAX_RETAINED_TRIALS
        for child in candidates[: max(0, excess)]:
            if (child / "data").exists():
                # Recorder data is never in a trial worktree by design. If one
                # ever appears, that is a reason to stop, not to delete.
                continue
            git_output(
                self.repository_root,
                ["worktree", "remove", "--", str(child)],
                timeout=self.timeout,
            )


def deadline_passed(record: dict[str, Any], *, now: datetime | None = None) -> bool:
    deadline = parse_stamp(record.get("deadline_at"))
    if deadline is None:
        return True
    return (now or utc_now()) >= deadline


def expectation_from(
    record: dict[str, Any],
    *,
    expected_commit: str,
    supervisor_session: str,
) -> StartupExpectation:
    """Rebuild the acceptance expectation from the durable record."""

    activated = parse_stamp(record.get("activated_at")) or (
        utc_now() - timedelta(days=1)
    )
    deadline = parse_stamp(record.get("deadline_at")) or utc_now()
    replacement = record.get("replacement_nonce")
    previous_nonce = record.get("predecessor_nonce")
    previous_pid = record.get("predecessor_pid")
    return StartupExpectation(
        expected_commit=expected_commit,
        supervisor_session=supervisor_session,
        activated_at=activated,
        deadline_at=deadline,
        replacement_nonce=(
            replacement
            if isinstance(replacement, str) and NONCE_RE.fullmatch(replacement)
            else None
        ),
        previous_pid=previous_pid if isinstance(previous_pid, int) else None,
        previous_nonce=(
            previous_nonce if isinstance(previous_nonce, str) else None
        ),
        baseline_state=(
            record.get("safe_state")
            if isinstance(record.get("safe_state"), str)
            else None
        ),
    )


def selection_of(record: dict[str, Any]) -> TrialSelection | None:
    branch = record.get("trial_branch")
    commit = record.get("trial_commit")
    repository = record.get("repository")
    if (
        not isinstance(branch, str)
        or not BRANCH_RE.fullmatch(branch)
        or not isinstance(commit, str)
        or not OID_RE.fullmatch(commit)
        or not isinstance(repository, str)
    ):
        return None
    return TrialSelection(repository, branch, commit)


__all__ = [
    "ACCEPTANCE_CONDITIONS",
    "HEALTHY_RECORDER_STATE",
    "MAX_RETAINED_TRIALS",
    "RECORDER_STATE_RANK",
    "ROLLBACK_FAILED",
    "ROLLBACK_STARTING",
    "ROLLBACK_VERIFYING",
    "SAFE_RESTORED",
    "TRIAL_FAILED",
    "TRIAL_OPERATOR_STOPPED",
    "TRIAL_PREPARING",
    "TRIAL_ROOT_SUFFIX",
    "TRIAL_RUNNING",
    "TRIAL_STARTING",
    "TRIAL_VERIFYING",
    "StartupAcceptance",
    "StartupExpectation",
    "TrialJournal",
    "TrialPlan",
    "TrialWorktrees",
    "deadline_passed",
    "evaluate_startup",
    "expectation_from",
    "failure_reason",
    "new_trial_id",
    "process_is_running",
    "running_trial",
    "selection_of",
    "state_rank",
    "trial_root_for",
]
