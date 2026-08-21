"""Exact identity of one supervised native standalone-recorder process.

The Compose recorder can be identified by its image and container. The native
Windows recorder is an ordinary Python process in an ordinary checkout, so a
host updater that only compared Git commits could "verify" a replacement that
never actually started -- or verify the *old* process still running from the
same directory.

Every supervised recorder therefore carries three local secrets-free values:

``supervisor_session``
    One value per supervisor. It proves a replacement belongs to the supervisor
    that requested the update rather than to an unrelated recorder someone
    started by hand.
``process_nonce``
    One value per launched child. It is the only field that *must* differ
    between the recorder that was asked to stop and the recorder that replaced
    it, so a supervisor cannot mistake a survivor for a replacement.
``build_commit``
    The exact 40-character commit the process was launched from.

These are generated locally by the supervisor and are never accepted from a
Federation peer. Unsupervised runs still publish identity so the same status
readers work; they simply generate their own values and report no supervisor.
"""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path
from typing import Any

NATIVE_RUNTIME_SCHEMA = "fcp.recorder-native-runtime.v1"
NATIVE_RUNTIME_TYPE = "native-python"

SUPERVISOR_SESSION_ENV = "FCP_RECORDER_SUPERVISOR_SESSION"
PROCESS_NONCE_ENV = "FCP_RECORDER_PROCESS_NONCE"
BUILD_COMMIT_ENV = "FCP_RECORDER_BUILD_COMMIT"
#: The permanent known-good checkout this child belongs to.
#:
#: A branch trial runs the recorder from a separate worktree, so a process
#: cannot infer the production checkout from where its own code lives. The
#: supervisor -- which resolved that path locally long before any peer was
#: involved -- states it instead.
PRODUCTION_ROOT_ENV = "FCP_RECORDER_PRODUCTION_ROOT"

NONCE_RE = re.compile(r"^[0-9a-f]{32}$")
OID_RE = re.compile(r"^[0-9a-f]{40}$")


def new_nonce() -> str:
    return uuid.uuid4().hex


def _bounded_nonce(value: str | None) -> str | None:
    if isinstance(value, str) and NONCE_RE.fullmatch(value.strip().lower()):
        return value.strip().lower()
    return None


def _bounded_commit(value: str | None) -> str | None:
    if isinstance(value, str) and OID_RE.fullmatch(value.strip().lower()):
        return value.strip().lower()
    return None


def supervisor_session(environment: Any = None) -> str | None:
    source = os.environ if environment is None else environment
    return _bounded_nonce(source.get(SUPERVISOR_SESSION_ENV))


def process_nonce(environment: Any = None) -> str | None:
    source = os.environ if environment is None else environment
    return _bounded_nonce(source.get(PROCESS_NONCE_ENV))


def build_commit(environment: Any = None) -> str | None:
    source = os.environ if environment is None else environment
    return _bounded_commit(source.get(BUILD_COMMIT_ENV))


def production_root(environment: Any = None) -> Path | None:
    """Resolve the supervisor's production checkout, or ``None``.

    Validated as an existing Git checkout before it is believed. An absent or
    unusable value simply means "not supervised into a separate worktree", and
    callers fall back to their own location exactly as they did before.
    """

    source = os.environ if environment is None else environment
    value = source.get(PRODUCTION_ROOT_ENV)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        candidate = Path(value.strip()).resolve()
    except (OSError, ValueError):
        return None
    return candidate if (candidate / ".git").exists() else None


def runtime_identity(
    environment: Any = None,
    *,
    pid: int | None = None,
) -> dict[str, object]:
    """Describe this process for a host updater, without any peer input.

    A malformed or absent supervisor value is reported as ``None`` rather than
    echoed back. A host updater requires an exact match, so an unverifiable
    value must never be able to satisfy it by being passed through.
    """

    return {
        "schema": NATIVE_RUNTIME_SCHEMA,
        "runtime_type": NATIVE_RUNTIME_TYPE,
        "pid": int(os.getpid() if pid is None else pid),
        "process_nonce": process_nonce(environment),
        "supervisor_session": supervisor_session(environment),
        "build_commit": build_commit(environment),
    }


def identity_matches(
    value: object,
    *,
    pid: int,
    process_nonce: str,
    supervisor_session: str,
) -> bool:
    """Confirm a published identity is exactly this supervised process."""

    if not isinstance(value, dict):
        return False
    return (
        value.get("schema") == NATIVE_RUNTIME_SCHEMA
        and value.get("runtime_type") == NATIVE_RUNTIME_TYPE
        and value.get("pid") == pid
        and value.get("process_nonce") == process_nonce
        and value.get("supervisor_session") == supervisor_session
    )


__all__ = [
    "BUILD_COMMIT_ENV",
    "NATIVE_RUNTIME_SCHEMA",
    "NATIVE_RUNTIME_TYPE",
    "NONCE_RE",
    "OID_RE",
    "PROCESS_NONCE_ENV",
    "PRODUCTION_ROOT_ENV",
    "SUPERVISOR_SESSION_ENV",
    "build_commit",
    "identity_matches",
    "new_nonce",
    "process_nonce",
    "production_root",
    "runtime_identity",
    "supervisor_session",
]
