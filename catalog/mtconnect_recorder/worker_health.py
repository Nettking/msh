"""Observable health for the recorder's required background loops.

A standalone recorder is a headless device. Its Federation update, host update,
activation and control loops each catch every failure and retry from durable
state on the next poll, which is the right lifecycle -- none of them may end
capture. But catching a failure and discarding it means a loop that has failed
on every pass for a day is indistinguishable from a loop with nothing to do:
recorder-control commands an operator issued from ``/federation/recorders`` are
never applied, and update participation never happens, while the device stays a
connected member reporting no problem at all.

This record is the small amount of state that makes such a loop visible. It is
carried in the recorder's own status heartbeat, which is the only operator
surface a headless recorder has, so a stuck loop is read from the same file that
already proves liveness and membership.

Nothing here decides a lifecycle. A loop keeps retrying exactly as before; the
failure simply stops being invisible.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

#: The published record is rewritten in place rather than appended to, but a
#: bound keeps one unusually verbose failure from dominating the heartbeat an
#: operator reads.
MAX_ERROR_CHARS = 200


def error_code(exc: BaseException) -> str:
    """Name a failure the way the rest of the product names one."""

    code = getattr(exc, "code", None)
    if isinstance(code, str) and code:
        return code[:MAX_ERROR_CHARS]
    return type(exc).__name__[:MAX_ERROR_CHARS]


class WorkerHealth:
    """Consecutive-failure state for one required recorder loop.

    Consecutive failures rather than a total: the question an operator has is
    whether the loop is working *now*, and a lifetime counter answers a
    different one. A pass that succeeds clears the record completely, so a
    condition that has been repaired cannot survive in it.
    """

    def __init__(self, name: str, *, log: logging.Logger | None = None) -> None:
        self.name = name
        self._log = log or logging.getLogger("recorder")
        self._lock = threading.Lock()
        self._failures = 0
        self._last_error: str = ""
        self._started = False

    def started(self) -> None:
        """Record that this loop's thread actually began running."""

        with self._lock:
            self._started = True

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._last_error = ""

    def record_failure(self, exc: BaseException) -> None:
        code = error_code(exc)
        with self._lock:
            repeated = self._last_error == code
            self._failures += 1
            self._last_error = code
            failures = self._failures
        # A loop that keeps failing usually keeps failing for the same reason,
        # and one warning per poll would answer a stuck loop with a log that
        # grows at poll rate on the same device. The condition is announced
        # when it appears or changes; while it persists unchanged it stays at
        # debug level and the counter carries the persistence.
        self._log.log(
            logging.DEBUG if repeated else logging.WARNING,
            "Recorder %s loop failed (%s); retrying, consecutive failures: %s",
            self.name,
            code,
            failures,
        )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "started": self._started,
                "consecutive_failures": self._failures,
                "last_error_code": self._last_error,
            }


__all__ = ["MAX_ERROR_CHARS", "WorkerHealth", "error_code"]
