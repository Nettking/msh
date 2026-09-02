"""Self-observed restart history for FCP's supervised container services.

Docker restarts a failed service indefinitely at a bounded rate. From outside
the container that is visible as a growing restart count; from inside FCP it is
invisible, so a service that has been crash-looping for hours is indistinguishable
from one that has just started. B06 requires that condition to become an
FCP-visible state.

The observation is deliberately made by the service about *itself* rather than
by anything watching Docker:

* it needs no Docker socket, so no FCP process gains host mutation authority
  merely to answer a health question;
* it introduces no second supervisor -- Docker still owns restarting, and this
  module only records what already happened;
* it does not depend on a Docker ``healthcheck``, which reports whether a
  service answers now, never whether it has been dying repeatedly; and
* it works identically on Windows and POSIX, because it is one small file.

Each supervised service writes one bounded record when it starts and, when it
is stopped in a way it can observe, when it stops. A start whose predecessor
never recorded an intentional stop is an unclean start. A short run of those
is a crash loop; an operator stop, an update or a trial exit is not, which is
what keeps the ordinary product lifecycle from reading as a failure. An
observed exception/nonzero exit remains unclean evidence even though its stop
record was written before returning to Docker.

Every write here is best effort. A service must never fail to start because it
could not journal its own restart history, and a health read must never fail
because that history is missing or malformed.
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCHEMA = "fcp.service-incarnation.v1"

#: Retained incarnations. Bounded so a service that restarts forever writes a
#: file of fixed size rather than the unbounded history B07 forbids.
MAX_INCARNATIONS = 16

#: Consecutive unclean starts that turn "restarting" into "crash-loop".
CRASH_LOOP_THRESHOLD = 3

#: Those starts must also be recent. A device that lost power three times over a
#: year is not crash-looping.
CRASH_LOOP_WINDOW_SECONDS = 900

_MAX_TEXT = 128

#: Stop reasons that are part of the ordinary product lifecycle. None of these
#: counts toward a crash loop, which is what preserves operator stop, update and
#: trial semantics.
STOP_OPERATOR = "operator-stop"
STOP_UPDATE = "update"
STOP_TRIAL = "trial"
STOP_COMPLETED = "completed"
# A service may have observed a failure and still reach its own shutdown
# handler. This is deliberately outside ``INTENTIONAL_STOP_REASONS``: the
# supervisor remains Docker, while the next incarnation can now count this
# observed nonzero/exception exit as restart-worthy evidence.
STOP_FAILURE = "restart-worthy-failure"
INTENTIONAL_STOP_REASONS = frozenset(
    {STOP_OPERATOR, STOP_UPDATE, STOP_TRIAL, STOP_COMPLETED}
)

#: Restart states, ordered from healthy to worst.
STATE_UNKNOWN = "unknown"
STATE_STABLE = "stable"
STATE_RESTARTING = "restarting"
STATE_CRASH_LOOP = "crash-loop"

_PRECEDED_NONE = "none"
_PRECEDED_CLEAN = "clean"
_PRECEDED_UNCLEAN = "unclean"


@dataclass(frozen=True)
class ServiceRestartState:
    """What this service's own restart history says about it."""

    service: str
    state: str
    consecutive_unclean: int
    #: Start time of the earliest unclean start in the current run, if any.
    since: str | None
    #: Last stop reason the service managed to record, if any.
    last_stop_reason: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "service": self.service,
            "state": self.state,
            "consecutive_unclean": self.consecutive_unclean,
            "since": self.since,
            "last_stop_reason": self.last_stop_reason,
        }


def incarnation_state_file(root: Path | str, service: str) -> Path:
    """Return the per-service record path under a service's own durable root."""

    return Path(root) / "service-state" / f"{_text(service)}.incarnation.json"


def _text(value: object) -> str:
    return str(value or "").strip()[:_MAX_TEXT]


def _utc(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _stamp(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _parse(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _is_plain_file(path: Path) -> bool:
    """Reject a symlink or reparse point standing where the record belongs."""

    try:
        metadata = path.lstat()
    except (FileNotFoundError, NotADirectoryError):
        return False
    except OSError:
        return False
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        return False
    return not bool(getattr(metadata, "st_reparse_tag", 0))


def _write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _load(path: Path, service: str) -> list[dict[str, object]]:
    """Return the retained incarnations, or nothing when they cannot be trusted."""

    if not _is_plain_file(path):
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(payload, dict):
        return []
    if payload.get("schema") != SCHEMA or _text(payload.get("service")) != _text(service):
        return []
    entries = payload.get("incarnations")
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict)][-MAX_INCARNATIONS:]


def _publish(path: Path, service: str, entries: list[dict[str, object]]) -> None:
    payload = {
        "schema": SCHEMA,
        "service": _text(service),
        "incarnations": entries[-MAX_INCARNATIONS:],
    }
    _write_atomic(
        path,
        (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(
            "utf-8"
        ),
    )


def record_service_start(
    path: Path | str,
    *,
    service: str,
    now: datetime | None = None,
    pid: int | None = None,
) -> ServiceRestartState:
    """Journal one incarnation and report what the history now says.

    Best effort by contract: a service that cannot write this record still
    starts. The returned state is computed from what was actually retained, so
    a failed write degrades to ``unknown`` rather than to a false ``stable``.
    """

    moment = _utc(now)
    target = Path(path)
    try:
        entries = _load(target, service)
        previous = entries[-1] if entries else None
        if previous is None:
            preceded = _PRECEDED_NONE
        elif (
            _text(previous.get("stopped_at"))
            and _text(previous.get("reason")) in INTENTIONAL_STOP_REASONS
        ):
            preceded = _PRECEDED_CLEAN
        else:
            preceded = _PRECEDED_UNCLEAN
        entries.append(
            {
                "started_at": _stamp(moment),
                "preceded_by": preceded,
                "pid": int(pid if pid is not None else os.getpid()),
            }
        )
        _publish(target, service, entries)
    except OSError:
        return ServiceRestartState(
            service=_text(service),
            state=STATE_UNKNOWN,
            consecutive_unclean=0,
            since=None,
            last_stop_reason=None,
        )
    return read_restart_state(target, service=service, now=moment)


def record_service_stop(
    path: Path | str,
    *,
    service: str,
    reason: str,
    now: datetime | None = None,
) -> None:
    """Mark the live incarnation as stopped for a reason the service observed.

    Only an intentional lifecycle reason makes the next start clean. A service
    can reach its own error handler and return nonzero, so a recorded failure
    stop must remain restart-worthy evidence even though the process observed
    and journaled its exit.
    """

    moment = _utc(now)
    target = Path(path)
    try:
        entries = _load(target, service)
        if not entries:
            return
        live = entries[-1]
        if _text(live.get("stopped_at")):
            return
        live["stopped_at"] = _stamp(moment)
        live["reason"] = _text(reason) or STOP_COMPLETED
        _publish(target, service, entries)
    except OSError:
        return


def read_restart_state(
    path: Path | str,
    *,
    service: str,
    now: datetime | None = None,
) -> ServiceRestartState:
    """Classify a service's own restart history. Never raises."""

    moment = _utc(now)
    name = _text(service)
    entries = _load(Path(path), service)
    if not entries:
        return ServiceRestartState(
            service=name,
            state=STATE_UNKNOWN,
            consecutive_unclean=0,
            since=None,
            last_stop_reason=None,
        )

    last_stop_reason: str | None = None
    for entry in reversed(entries):
        reason = _text(entry.get("reason"))
        if reason:
            last_stop_reason = reason
            break

    # Count the trailing run of starts whose predecessor never recorded a stop.
    unclean: list[dict[str, object]] = []
    for entry in reversed(entries):
        if _text(entry.get("preceded_by")) != _PRECEDED_UNCLEAN:
            break
        unclean.append(entry)

    if not unclean:
        return ServiceRestartState(
            service=name,
            state=STATE_STABLE,
            consecutive_unclean=0,
            since=None,
            last_stop_reason=last_stop_reason,
        )

    earliest = _parse(unclean[-1].get("started_at"))
    since = _stamp(earliest) if earliest is not None else None
    # The window is deliberately one-sided. A host clock that jumped backwards
    # puts the run's start ahead of ``moment``, and that still reads as recent
    # rather than as ancient history, because the count is the real gate: a
    # clock change cannot manufacture unclean starts, so the worst a skewed
    # clock does here is report a device that genuinely restarted three times
    # without stopping. Erring the other way would hide exactly that device.
    recent = (
        earliest is not None
        and moment - earliest <= timedelta(seconds=CRASH_LOOP_WINDOW_SECONDS)
    )
    state = (
        STATE_CRASH_LOOP
        if len(unclean) >= CRASH_LOOP_THRESHOLD and recent
        else STATE_RESTARTING
    )
    return ServiceRestartState(
        service=name,
        state=state,
        consecutive_unclean=len(unclean),
        since=since,
        last_stop_reason=last_stop_reason,
    )


__all__ = [
    "CRASH_LOOP_THRESHOLD",
    "CRASH_LOOP_WINDOW_SECONDS",
    "INTENTIONAL_STOP_REASONS",
    "MAX_INCARNATIONS",
    "SCHEMA",
    "STATE_CRASH_LOOP",
    "STATE_RESTARTING",
    "STATE_STABLE",
    "STATE_UNKNOWN",
    "STOP_COMPLETED",
    "STOP_FAILURE",
    "STOP_OPERATOR",
    "STOP_TRIAL",
    "STOP_UPDATE",
    "ServiceRestartState",
    "incarnation_state_file",
    "read_restart_state",
    "record_service_start",
    "record_service_stop",
]
