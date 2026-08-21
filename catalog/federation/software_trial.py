"""The constrained protocol for running one device on an approved test branch.

A branch trial is the only Federation operation that names something other than
approved ``main``, so the shape of what a peer may say is the whole security
boundary. This module is that shape, and nothing else: it has no transport, no
filesystem, no subprocess and no Git.

A peer may name exactly three things:

``repository``
    which must equal the approved repository, and is compared rather than used;
``branch``
    a branch name matching :data:`~catalog.federation.software_update.BRANCH_RE`,
    which the receiving host re-resolves against *its own* approved remote; and
``target_commit``
    one exact 40-character object ID, which the receiving host requires to be
    contained in that branch on that remote.

Everything else -- checkout paths, worktree locations, interpreters, arguments,
environment values, data directories, timeouts -- is resolved locally by the
host and is not expressible here. There is deliberately no field a URL, a path,
a command or an environment value could travel in.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .software_update import (
    APPROVED_BRANCH,
    APPROVED_REPOSITORY,
    BRANCH_RE,
    OID_RE,
)

#: A locally generated process-instance value. Never accepted from a peer.
NONCE_RE = re.compile(r"^[0-9a-f]{32}$")

TRIAL_REQUEST_SCHEMA = "fcp.host-trial-request.v1"
TRIAL_RESULT_SCHEMA = "fcp.host-trial-result.v1"
#: Asking the local host which branches the approved repository publishes.
#: The Flask process never runs Git itself, so the dropdown is filled by the
#: same host-owned agent that would have to run the branch anyway.
BRANCHES_REQUEST_SCHEMA = "fcp.host-branches-request.v1"
BRANCHES_RESULT_SCHEMA = "fcp.host-branches-result.v1"
#: What one bounded handoff document can carry without exceeding its limit.
MAX_LISTED_BRANCHES = 60
TRIAL_JOURNAL_SCHEMA = "fcp.recorder-trial-journal.v1"
#: The one thing a trial process is ever asked to do by another process.
#:
#: The verdict on a trial is deliberately *not* made by the branch under test --
#: a branch that omits or breaks the watchdog would otherwise never fail. It is
#: made by a watchdog running from the permanent checkout, which writes this
#: marker when the trial did not prove itself. The trial process only reads it
#: and ends its own capture, exactly as an operator's Ctrl+C would.
TRIAL_STOP_SCHEMA = "fcp.recorder-trial-stop.v1"
#: How long a stop marker stays actionable.
TRIAL_STOP_TTL_SECONDS = 300

TRIAL_ID_RE = re.compile(r"^[0-9a-f]{32}$")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")

#: The bounded window a trial has to prove a healthy replacement recorder.
#: Chosen against the existing runtime semantics: a supervised relaunch plus a
#: Federation rejoin plus one status publication comfortably fits, and the
#: existing ``STATUS_FRESHNESS_SECONDS`` tolerance is twice as wide again.
TRIAL_STARTUP_TIMEOUT_SECONDS = 60
#: Rollback gets the same window. It is a *known-good* version starting on a
#: checkout that never moved, so needing longer would itself be a fault.
ROLLBACK_STARTUP_TIMEOUT_SECONDS = 60
#: How long a queued trial request stays actionable.
TRIAL_REQUEST_TTL_SECONDS = 120

# ---------------------------------------------------------------------------
# durable stages
# ---------------------------------------------------------------------------

#: Prevalidating while the known-good recorder is still recording.
TRIAL_PREPARING = "preparing"
#: The recorder consented to stop; the supervisor owns the relaunch.
TRIAL_STARTING = "trial_starting"
#: The trial process is up and inside its bounded acceptance window.
TRIAL_VERIFYING = "trial_verifying"
#: The trial proved every required condition and is the running version.
TRIAL_RUNNING = "trial_running"
#: The trial did not prove itself; the pinned safe version must come back.
TRIAL_FAILED = "trial_failed"
#: The supervisor was told to relaunch the pinned safe version.
ROLLBACK_STARTING = "rollback_starting"
#: The safe version is up and inside its bounded acceptance window.
ROLLBACK_VERIFYING = "rollback_verifying"
#: The pinned safe version proved itself. Recording resumed.
SAFE_RESTORED = "safe_restored"
#: Recovery itself failed. Reported explicitly; never silently retried.
ROLLBACK_FAILED = "rollback_failed"
#: An operator ended the trial process themselves. Nothing is relaunched.
TRIAL_OPERATOR_STOPPED = "trial_operator_stopped"

TRIAL_STAGES: tuple[str, ...] = (
    TRIAL_PREPARING,
    TRIAL_STARTING,
    TRIAL_VERIFYING,
    TRIAL_RUNNING,
    TRIAL_FAILED,
    ROLLBACK_STARTING,
    ROLLBACK_VERIFYING,
    SAFE_RESTORED,
    ROLLBACK_FAILED,
    TRIAL_OPERATOR_STOPPED,
)

#: Stages that end an activation. ``trial_running`` is terminal for the
#: activation but *not* for the trial: the device stays on the branch until an
#: operator brings it back, which is an ordinary update to main.
TERMINAL_TRIAL_STAGES = frozenset(
    {
        TRIAL_RUNNING,
        SAFE_RESTORED,
        ROLLBACK_FAILED,
        TRIAL_OPERATOR_STOPPED,
    }
)

#: Stages in which a device is running something other than approved main.
OFF_MAIN_STAGES = frozenset({TRIAL_STARTING, TRIAL_VERIFYING, TRIAL_RUNNING})


class TrialRefused(ValueError):
    """A bounded, reportable refusal of a trial request."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class TrialSelection:
    """One approved repository branch and the exact commit to run from it."""

    repository: str
    branch: str
    target_commit: str

    def validate(self) -> TrialSelection:
        if self.repository != APPROVED_REPOSITORY:
            raise TrialRefused("unapproved_source")
        if not isinstance(self.branch, str) or not BRANCH_RE.fullmatch(self.branch):
            raise TrialRefused("malformed_branch")
        if not isinstance(self.target_commit, str) or not OID_RE.fullmatch(
            self.target_commit
        ):
            raise TrialRefused("malformed_target")
        return self

    @property
    def is_approved_branch(self) -> bool:
        return self.branch == APPROVED_BRANCH

    def to_dict(self) -> dict[str, object]:
        return {
            "repository": self.repository,
            "branch": self.branch,
            "target_commit": self.target_commit,
        }


def trial_selection(value: object) -> TrialSelection:
    """Read a peer-supplied selection, refusing anything outside the shape.

    Extra keys are ignored rather than echoed. Nothing a caller did not ask for
    can be smuggled through into the local side of the protocol.
    """

    if not isinstance(value, dict):
        raise TrialRefused("malformed_message")
    return TrialSelection(
        repository=str(value.get("repository") or ""),
        branch=str(value.get("branch") or ""),
        target_commit=str(value.get("target_commit") or "").lower(),
    ).validate()


def trial_request_document(
    *,
    request_id: str,
    selection: TrialSelection,
    created_at: datetime,
    ttl_seconds: int = TRIAL_REQUEST_TTL_SECONDS,
) -> dict[str, object]:
    """Build the bounded declarative request a host agent will act on."""

    if not REQUEST_ID_RE.fullmatch(request_id):
        raise TrialRefused("malformed_request_id")
    if not 1 <= int(ttl_seconds) <= TRIAL_REQUEST_TTL_SECONDS:
        raise TrialRefused("invalid_trial_ttl")
    selection.validate()
    expires = created_at + timedelta(seconds=int(ttl_seconds))
    return {
        "schema": TRIAL_REQUEST_SCHEMA,
        "request_id": request_id,
        "action": "trial",
        **selection.to_dict(),
        "created_at": _stamp(created_at),
        "expires_at": _stamp(expires),
    }


def validate_trial_request(
    value: object,
    *,
    now: datetime,
) -> tuple[str, TrialSelection]:
    """Re-read a queued trial request with no trust in how it was produced."""

    if not isinstance(value, dict) or value.get("schema") != TRIAL_REQUEST_SCHEMA:
        raise TrialRefused("malformed_message")
    request_id = value.get("request_id")
    if not isinstance(request_id, str) or not REQUEST_ID_RE.fullmatch(request_id):
        raise TrialRefused("malformed_request_id")
    if value.get("action") != "trial":
        raise TrialRefused("malformed_action")
    created = parse_stamp(value.get("created_at"))
    expires = parse_stamp(value.get("expires_at"))
    if created is None or expires is None:
        raise TrialRefused("malformed_timestamp")
    if (
        created > now + timedelta(minutes=1)
        or expires <= now
        or (expires - created) > timedelta(seconds=TRIAL_REQUEST_TTL_SECONDS)
    ):
        raise TrialRefused("expired_or_invalid_request")
    return request_id, trial_selection(value)


def trial_stop_request(
    *,
    trial_id: str,
    request_id: str,
    supervisor_session: str,
    recorder_pid: int,
    process_nonce: str,
    reason: str,
    created_at: datetime,
    ttl_seconds: int = TRIAL_STOP_TTL_SECONDS,
) -> dict[str, object]:
    """Ask one exact trial process, and only it, to end its own capture."""

    if not TRIAL_ID_RE.fullmatch(trial_id):
        raise TrialRefused("malformed_trial_id")
    if not REQUEST_ID_RE.fullmatch(request_id):
        raise TrialRefused("malformed_request_id")
    if not NONCE_RE.fullmatch(supervisor_session) or not NONCE_RE.fullmatch(
        process_nonce
    ):
        raise TrialRefused("malformed_process_identity")
    if (
        isinstance(recorder_pid, bool)
        or not isinstance(recorder_pid, int)
        or recorder_pid <= 0
    ):
        raise TrialRefused("malformed_recorder_pid")
    return {
        "schema": TRIAL_STOP_SCHEMA,
        "trial_id": trial_id,
        "request_id": request_id,
        "supervisor_session": supervisor_session,
        "recorder_pid": recorder_pid,
        "process_nonce": process_nonce,
        "reason": reason[:256],
        "created_at": _stamp(created_at),
        "expires_at": _stamp(created_at + timedelta(seconds=int(ttl_seconds))),
    }


def trial_stop_applies(
    value: object,
    *,
    trial_id: object,
    pid: int,
    process_nonce: str | None,
    supervisor_session: str | None,
    now: datetime,
) -> bool:
    """Decide whether a stop marker names *this* exact trial process.

    Every field must match. A marker for another trial, another supervisor,
    another process instance or an expired window is ignored, so one left
    behind on disk can never end an unrelated recorder.
    """

    if not isinstance(value, dict) or value.get("schema") != TRIAL_STOP_SCHEMA:
        return False
    if value.get("trial_id") != trial_id or value.get("recorder_pid") != pid:
        return False
    if process_nonce is None or value.get("process_nonce") != process_nonce:
        return False
    if (
        supervisor_session is None
        or value.get("supervisor_session") != supervisor_session
    ):
        return False
    created = parse_stamp(value.get("created_at"))
    expires = parse_stamp(value.get("expires_at"))
    if created is None or expires is None:
        return False
    return created <= now + timedelta(minutes=1) and expires > now


def branches_request_document(
    *,
    request_id: str,
    created_at: datetime,
    ttl_seconds: int = TRIAL_REQUEST_TTL_SECONDS,
) -> dict[str, object]:
    """Ask the local host agent to list the approved repository's branches."""

    if not REQUEST_ID_RE.fullmatch(request_id):
        raise TrialRefused("malformed_request_id")
    return {
        "schema": BRANCHES_REQUEST_SCHEMA,
        "request_id": request_id,
        "action": "branches",
        "repository": APPROVED_REPOSITORY,
        "created_at": _stamp(created_at),
        "expires_at": _stamp(created_at + timedelta(seconds=int(ttl_seconds))),
    }


def branches_result_document(
    *,
    request_id: str,
    branches: object,
    code: str | None = None,
    message: str = "",
) -> dict[str, object]:
    """Publish a bounded branch list, or a bounded reason there is none."""

    listed: list[dict[str, str]] = []
    if isinstance(branches, (list, tuple)):
        for item in branches:
            name = getattr(item, "name", None)
            commit = getattr(item, "commit", None)
            if isinstance(item, dict):
                name = item.get("name")
                commit = item.get("commit")
            if (
                isinstance(name, str)
                and BRANCH_RE.fullmatch(name)
                and isinstance(commit, str)
                and OID_RE.fullmatch(commit)
            ):
                listed.append({"name": name, "commit": commit})
            if len(listed) >= MAX_LISTED_BRANCHES:
                break
    return {
        "schema": BRANCHES_RESULT_SCHEMA,
        "request_id": request_id,
        "action": "branches",
        "state": "error" if code else "branches",
        "repository": APPROVED_REPOSITORY,
        "branches": listed,
        "code": code,
        "message": message[:512],
    }


def branches_from_result(value: object) -> tuple[dict[str, str], ...]:
    """Read a branch list back, keeping only well-formed approved entries."""

    if not isinstance(value, dict) or value.get("schema") != BRANCHES_RESULT_SCHEMA:
        return ()
    if value.get("repository") != APPROVED_REPOSITORY:
        return ()
    listed = value.get("branches")
    if not isinstance(listed, list):
        return ()
    result: list[dict[str, str]] = []
    for item in listed[:MAX_LISTED_BRANCHES]:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        commit = item.get("commit")
        if (
            isinstance(name, str)
            and BRANCH_RE.fullmatch(name)
            and isinstance(commit, str)
            and OID_RE.fullmatch(commit)
        ):
            result.append({"name": name, "commit": commit.lower()})
    return tuple(result)


#: Keys of the bounded per-device trial summary carried on update reports.
TRIAL_SUMMARY_KEYS: tuple[str, ...] = (
    "stage",
    "branch",
    "commit",
    "safe_branch",
    "safe_commit",
    "failure_reason",
    "active",
)


def trial_summary(
    *,
    stage: object,
    branch: object,
    commit: object,
    safe_branch: object,
    safe_commit: object,
    failure_reason: object = None,
    active: bool | None = None,
) -> dict[str, object] | None:
    """Build the bounded per-device summary the Federation UI renders.

    Returns ``None`` rather than a partial record: a device that has never run
    a trial is on approved main, and saying nothing is the accurate report.
    """

    if stage not in TRIAL_STAGES:
        return None
    value: dict[str, object] = {
        "stage": stage,
        "branch": branch if isinstance(branch, str) and BRANCH_RE.fullmatch(branch) else None,
        "commit": commit if isinstance(commit, str) and OID_RE.fullmatch(commit) else None,
        "safe_branch": (
            safe_branch
            if isinstance(safe_branch, str) and BRANCH_RE.fullmatch(safe_branch)
            else None
        ),
        "safe_commit": (
            safe_commit
            if isinstance(safe_commit, str) and OID_RE.fullmatch(safe_commit)
            else None
        ),
        "failure_reason": (
            failure_reason[:256] if isinstance(failure_reason, str) else None
        ),
        # Derived from the stage unless the caller knows better. A device that
        # was restarted normally is back on the production checkout even though
        # its last recorded stage still names a branch, so the party that can
        # see the running commit gets to say so.
        "active": (stage in OFF_MAIN_STAGES) if active is None else bool(active),
    }
    return value


def validate_trial_summary(value: object) -> dict[str, object] | None:
    """Re-read a peer-reported trial summary, keeping only the bounded shape.

    Unknown keys are dropped rather than passed through, so a report cannot
    become a channel for anything the UI was not designed to render.
    """

    if not isinstance(value, dict):
        return None
    reported = value.get("active")
    return trial_summary(
        stage=value.get("stage"),
        branch=value.get("branch"),
        commit=value.get("commit"),
        safe_branch=value.get("safe_branch"),
        safe_commit=value.get("safe_commit"),
        failure_reason=value.get("failure_reason"),
        active=reported if isinstance(reported, bool) else None,
    )


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_stamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


__all__ = [
    "BRANCHES_REQUEST_SCHEMA",
    "BRANCHES_RESULT_SCHEMA",
    "MAX_LISTED_BRANCHES",
    "OFF_MAIN_STAGES",
    "REQUEST_ID_RE",
    "ROLLBACK_FAILED",
    "ROLLBACK_STARTING",
    "ROLLBACK_STARTUP_TIMEOUT_SECONDS",
    "ROLLBACK_VERIFYING",
    "SAFE_RESTORED",
    "TERMINAL_TRIAL_STAGES",
    "TRIAL_FAILED",
    "TRIAL_ID_RE",
    "TRIAL_JOURNAL_SCHEMA",
    "TRIAL_OPERATOR_STOPPED",
    "TRIAL_PREPARING",
    "TRIAL_REQUEST_SCHEMA",
    "TRIAL_REQUEST_TTL_SECONDS",
    "TRIAL_RESULT_SCHEMA",
    "TRIAL_RUNNING",
    "TRIAL_STAGES",
    "TRIAL_STARTING",
    "TRIAL_STARTUP_TIMEOUT_SECONDS",
    "TRIAL_STOP_SCHEMA",
    "TRIAL_STOP_TTL_SECONDS",
    "TRIAL_SUMMARY_KEYS",
    "TRIAL_VERIFYING",
    "TrialRefused",
    "TrialSelection",
    "branches_from_result",
    "branches_request_document",
    "branches_result_document",
    "parse_stamp",
    "trial_request_document",
    "trial_selection",
    "trial_stop_applies",
    "trial_stop_request",
    "trial_summary",
    "validate_trial_request",
    "validate_trial_summary",
]
