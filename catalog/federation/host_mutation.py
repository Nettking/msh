"""Checkout-scoped host mutation serialization shared by supported actors.

The normal launchers and host update agents must agree on one checkout mutation
boundary. Windows actors share the named ``Global\FCPHostMutation-...`` mutex;
POSIX actors share the Git-scoped ``fcp-host-mutation.lock`` file from
``host_build``. The launcher entrypoint below can hold that same boundary while
a child launcher performs build, Compose activation, and readiness checks.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import ntpath
import os
import subprocess
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

HOST_MUTATION_LOCK_TIMEOUT_SECONDS = 30.0
HOST_MUTATION_LEASE_ENV = "FCP_HOST_MUTATION_LEASE_ACTIVE"
HOST_MUTATION_LEASE_OWNER_ENV = "FCP_HOST_MUTATION_LEASE_OWNER_PID"
_WINDOWS_MUTEX_PREFIX = "Global\\FCPHostMutation-"
_WAIT_OBJECT_0 = 0x00000000
_WAIT_ABANDONED = 0x00000080
_WAIT_TIMEOUT = 0x00000102
_MAX_WAIT_MILLISECONDS = 0xFFFFFFFE


class HostMutationLockError(RuntimeError):
    """The checkout mutation boundary could not be acquired safely."""


def normalize_windows_directory(value: Path | str) -> str:
    """Mirror ``fcp_host_build.ps1`` directory normalization deterministically."""

    full = ntpath.abspath(os.fspath(value))
    drive, tail = ntpath.splitdrive(full)
    root = drive + ("\\" if tail.startswith(("\\", "/")) else "")
    if not drive and full.startswith(("\\", "/")):
        root = full[:1]
    if len(full) <= len(root):
        return full
    return full.rstrip("\\/")


def windows_host_mutation_mutex_name(value: Path | str) -> str:
    """Return the exact named mutex used by ``fcp_host_build.ps1``."""

    normalized = normalize_windows_directory(value)
    digest = hashlib.sha256(normalized.lower().encode("utf-8")).hexdigest()
    return _WINDOWS_MUTEX_PREFIX + digest[:24].upper()


@contextmanager
def _windows_host_mutation_lock(
    root: Path,
    *,
    timeout_seconds: float,
) -> Iterator[None]:
    if timeout_seconds < 0:
        raise ValueError("timeout_seconds must be non-negative")
    loader = getattr(ctypes, "WinDLL", None)
    if loader is None:
        raise HostMutationLockError("host_mutation_lock_unsupported")
    kernel32 = loader("kernel32", use_last_error=True)
    kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
    kernel32.CreateMutexW.restype = ctypes.c_void_p
    kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    kernel32.WaitForSingleObject.restype = ctypes.c_uint32
    kernel32.ReleaseMutex.argtypes = [ctypes.c_void_p]
    kernel32.ReleaseMutex.restype = ctypes.c_int
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel32.CloseHandle.restype = ctypes.c_int

    handle = kernel32.CreateMutexW(
        None,
        False,
        windows_host_mutation_mutex_name(root),
    )
    if not handle:
        raise HostMutationLockError("host_mutation_lock_unavailable")
    acquired = False
    try:
        milliseconds = min(
            int(timeout_seconds * 1000),
            _MAX_WAIT_MILLISECONDS,
        )
        result = int(kernel32.WaitForSingleObject(handle, milliseconds))
        if result in {_WAIT_OBJECT_0, _WAIT_ABANDONED}:
            acquired = True
        elif result == _WAIT_TIMEOUT:
            raise HostMutationLockError("host_mutation_busy")
        else:
            raise HostMutationLockError("host_mutation_lock_unavailable")
        yield
    finally:
        if acquired:
            kernel32.ReleaseMutex(handle)
        kernel32.CloseHandle(handle)


@contextmanager
def host_mutation_lock(
    root: Path,
    *,
    timeout_seconds: float = HOST_MUTATION_LOCK_TIMEOUT_SECONDS,
) -> Iterator[None]:
    """Enter the same finite checkout mutation boundary on Windows and POSIX."""

    root = Path(root)
    if os.name == "nt":
        with _windows_host_mutation_lock(root, timeout_seconds=timeout_seconds):
            yield
        return

    # Keep one POSIX implementation and one lock path: the launcher/update
    # lifecycle introduced by B04 already owns this primitive in host_build.
    from catalog.federation.host_build import host_mutation_lock as posix_lock

    context = posix_lock(root, timeout_seconds=timeout_seconds)
    try:
        context.__enter__()
    except (RuntimeError, ValueError) as exc:
        raise HostMutationLockError(str(exc)) from exc
    try:
        yield
    finally:
        context.__exit__(None, None, None)


def run_command_under_host_mutation_lock(
    root: Path,
    command: Sequence[str],
    *,
    timeout_seconds: float = HOST_MUTATION_LOCK_TIMEOUT_SECONDS,
) -> int:
    """Run one supported launcher child while this process owns the host lease."""

    if not command:
        raise ValueError("host_mutation_command_required")
    root = root.resolve()
    environment = os.environ.copy()
    environment[HOST_MUTATION_LEASE_ENV] = "1"
    environment[HOST_MUTATION_LEASE_OWNER_ENV] = str(os.getpid())
    with host_mutation_lock(root, timeout_seconds=timeout_seconds):
        completed = subprocess.run(
            list(command),
            cwd=root,
            env=environment,
            shell=False,
            check=False,
        )
    return int(completed.returncode)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", required=True)
    parser.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=HOST_MUTATION_LOCK_TIMEOUT_SECONDS,
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    try:
        return run_command_under_host_mutation_lock(
            Path(args.repo_root),
            command,
            timeout_seconds=args.lock_timeout_seconds,
        )
    except (HostMutationLockError, RuntimeError, ValueError, OSError) as exc:
        print(f"FCP host mutation lease refused: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
