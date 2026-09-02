r"""Cross-platform host lease for native writers of one FCP data directory.

Compose-managed writers are quiesced by Compose during backup.  A native
``start_recorder.py`` process is outside that container lifecycle, so a mere
status-file check leaves a race: a recorder can start after the check and before
the copy.  Native recorder startup and the backup path therefore share this
host-local lease for the data directory.

Windows uses a named mutex keyed by the normalized data-directory path. POSIX
uses one flock file inside the data directory. The lease is intentionally local;
it grants no Federation authority and never travels over the network.
"""

from __future__ import annotations

import ctypes
import hashlib
import ntpath
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

DATA_WRITER_LOCK_TIMEOUT_SECONDS = 0.0
_WINDOWS_MUTEX_PREFIX = "Global\\FCPDataWriter-"
_WAIT_OBJECT_0 = 0x00000000
_WAIT_ABANDONED = 0x00000080
_WAIT_TIMEOUT = 0x00000102
_MAX_WAIT_MILLISECONDS = 0xFFFFFFFE
_LOCK_RELATIVE = Path("source_state") / ".fcp-data-writer.lock"


class DataWriterLockError(RuntimeError):
    """The native data-writer boundary could not be acquired safely."""


def _normalize_windows_directory(value: Path | str) -> str:
    full = ntpath.abspath(os.fspath(value))
    drive, tail = ntpath.splitdrive(full)
    root = drive + ("\\" if tail.startswith(("\\", "/")) else "")
    if not drive and full.startswith(("\\", "/")):
        root = full[:1]
    if len(full) <= len(root):
        return full
    return full.rstrip("\\/")


def windows_data_writer_mutex_name(value: Path | str) -> str:
    normalized = _normalize_windows_directory(value)
    digest = hashlib.sha256(normalized.lower().encode("utf-8")).hexdigest()
    return _WINDOWS_MUTEX_PREFIX + digest[:24].upper()


@contextmanager
def _windows_lock(data_dir: Path, *, timeout_seconds: float) -> Iterator[None]:
    if timeout_seconds < 0:
        raise ValueError("timeout_seconds must be non-negative")
    loader = getattr(ctypes, "WinDLL", None)
    if loader is None:
        raise DataWriterLockError("data_writer_lock_unsupported")
    kernel32 = loader("kernel32", use_last_error=True)
    kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
    kernel32.CreateMutexW.restype = ctypes.c_void_p
    kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    kernel32.WaitForSingleObject.restype = ctypes.c_uint32
    kernel32.ReleaseMutex.argtypes = [ctypes.c_void_p]
    kernel32.ReleaseMutex.restype = ctypes.c_int
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel32.CloseHandle.restype = ctypes.c_int

    handle = kernel32.CreateMutexW(None, False, windows_data_writer_mutex_name(data_dir))
    if not handle:
        raise DataWriterLockError("data_writer_lock_unavailable")
    acquired = False
    try:
        milliseconds = min(int(timeout_seconds * 1000), _MAX_WAIT_MILLISECONDS)
        result = int(kernel32.WaitForSingleObject(handle, milliseconds))
        if result in {_WAIT_OBJECT_0, _WAIT_ABANDONED}:
            acquired = True
        elif result == _WAIT_TIMEOUT:
            raise DataWriterLockError("data_writer_busy")
        else:
            raise DataWriterLockError("data_writer_lock_unavailable")
        yield
    finally:
        if acquired:
            kernel32.ReleaseMutex(handle)
        kernel32.CloseHandle(handle)


@contextmanager
def _posix_lock(data_dir: Path, *, timeout_seconds: float) -> Iterator[None]:
    if timeout_seconds < 0:
        raise ValueError("timeout_seconds must be non-negative")
    try:
        import fcntl
    except ImportError as exc:  # pragma: no cover - non-POSIX runtime
        raise DataWriterLockError("data_writer_lock_unsupported") from exc

    lock_path = data_dir / _LOCK_RELATIVE
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("a+b")
    except OSError as exc:
        raise DataWriterLockError("data_writer_lock_unavailable") from exc

    acquired = False
    deadline = time.monotonic() + timeout_seconds
    try:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                if time.monotonic() >= deadline:
                    raise DataWriterLockError("data_writer_busy") from exc
                time.sleep(min(0.05, max(deadline - time.monotonic(), 0.0)))
                continue
            except OSError as exc:
                raise DataWriterLockError("data_writer_lock_unavailable") from exc
            acquired = True
            break
        yield
    finally:
        if acquired:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


@contextmanager
def data_writer_lock(
    data_dir: Path | str,
    *,
    timeout_seconds: float = DATA_WRITER_LOCK_TIMEOUT_SECONDS,
) -> Iterator[None]:
    """Exclude another native recorder/backup using this data directory."""

    root = Path(data_dir).resolve()
    if os.name == "nt":
        with _windows_lock(root, timeout_seconds=timeout_seconds):
            yield
        return
    with _posix_lock(root, timeout_seconds=timeout_seconds):
        yield


__all__ = [
    "DATA_WRITER_LOCK_TIMEOUT_SECONDS",
    "DataWriterLockError",
    "data_writer_lock",
    "windows_data_writer_mutex_name",
]
