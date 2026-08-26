"""Checkout-scoped host mutation serialization shared by supported actors.

The normal Windows launcher owns this mutex from PowerShell while it proves and
builds one exact source tree. Native Python actors cannot inherit that mutex, so
this module reproduces the *same named object* for source mutation performed by
the supervised standalone recorder. POSIX callers delegate to the existing
``host_build.host_mutation_lock`` and therefore share its Git lock file exactly.
"""

from __future__ import annotations

import ctypes
import hashlib
import ntpath
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

HOST_MUTATION_LOCK_TIMEOUT_SECONDS = 30.0
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
