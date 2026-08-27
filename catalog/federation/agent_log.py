"""Bound the POSIX update agent's own log.

Both supported POSIX launchers start the update agent with its output appended
to ``<data>/federation/update-agent/agent.log`` - ``start.sh`` through a shell
``>>`` redirect, ``headless_fcp.py`` through a file opened in append mode.
Nothing truncates or rotates it. The agent then polls for the lifetime of the
device, and every line it or its ``git``/``docker`` children write to stderr
lands there, so the file only ever grows.

It is not merely a slow drip either. A durable request that cannot complete -
the checkout lock held elsewhere, a refused disk preflight - is deliberately
left in place and retried on the next poll, so a host in that state writes a
warning every poll second until someone intervenes. That is the same shape as
the container logs #356 bounded, on a path #356 does not cover: the agent is a
host process, not a Compose service, so Docker's logging driver never sees it.

The bound is copy-then-truncate rather than rename-then-create, because the
writer is a long-lived process holding an inherited append-mode descriptor.
Renaming the file would leave that descriptor pointing at the renamed inode and
the agent would go on filling the "rotated" file forever, which is how a
rotation that looks correct silently fails to bound anything. Truncating in
place keeps the descriptor valid, and O_APPEND means the next write lands at the
new end rather than leaving a sparse hole.

Only the agent process itself calls this. It holds the update-agent singleton
lock, so there is exactly one writer deciding when to rotate.
"""

from __future__ import annotations

import os
from pathlib import Path

AGENT_LOG_NAME = "agent.log"
PREVIOUS_AGENT_LOG_NAME = "agent.log.1"

# The live log is bounded at the same 10 MiB per generation the FCP container
# log policy uses, so one operator-facing bound covers host and service logs
# alike.
MAX_AGENT_LOG_BYTES = 10 * 1024**2
# Carried into the previous generation at each rotation. Small on purpose: this
# is copied synchronously by the agent's own poll loop, and the point is that
# recent history survives a rotation, not that the whole generation does.
RETAINED_TAIL_BYTES = 2 * 1024**2


def _retained_tail(path: Path, size: int) -> bytes:
    with path.open("rb") as stream:
        stream.seek(max(0, size - RETAINED_TAIL_BYTES))
        tail = stream.read()
    if size > RETAINED_TAIL_BYTES:
        # The seek almost certainly landed mid-line. Keep whole lines only, so
        # the retained generation never opens on half a message.
        newline = tail.find(b"\n")
        tail = tail[newline + 1 :] if newline >= 0 else b""
    return tail


def bound_agent_log(directory: Path) -> bool:
    """Hold the agent log to one bounded live generation plus a recent tail.

    Returns whether a rotation happened. Never raises: a log that cannot be
    bounded is a diagnostic problem, and taking the update agent's poll loop
    down over it would be a much larger one.
    """

    path = directory / AGENT_LOG_NAME
    try:
        size = path.stat().st_size
    except OSError:
        return False
    if size <= MAX_AGENT_LOG_BYTES:
        return False

    previous = directory / PREVIOUS_AGENT_LOG_NAME
    staged = directory / f"{PREVIOUS_AGENT_LOG_NAME}.tmp-{os.getpid()}"
    try:
        tail = _retained_tail(path, size)
        with staged.open("wb") as stream:
            stream.write(tail)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(staged, previous)
    except OSError:
        try:
            staged.unlink()
        except OSError:
            pass
        return False

    try:
        # In place, so the agent's inherited append-mode descriptor stays valid.
        os.truncate(path, 0)
    except OSError:
        return False
    return True
