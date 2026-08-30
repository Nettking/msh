"""Stable directory handles for race-safe managed filesystem writes.

The helpers in this module keep a managed destination anchored while files are
created or replaced. POSIX uses non-following descriptor-relative traversal and
operations. Windows pins every directory component with reparse-aware handles
that deny delete sharing, preventing an ancestor from being renamed/rebound for
the lifetime of the boundary.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import secrets
import stat
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO


class StableFilesystemError(OSError):
    """The requested managed path cannot be held behind a stable boundary."""


def _relative_parts(relative: Path) -> tuple[str, ...]:
    if relative.is_absolute():
        raise StableFilesystemError("managed relative path must not be absolute")
    parts = tuple(part for part in relative.parts if part not in ("", "."))
    if any(part == ".." for part in parts):
        raise StableFilesystemError("managed relative path must not traverse upward")
    return parts


def _posix_directory_flags() -> int:
    required = ("O_DIRECTORY", "O_NOFOLLOW")
    if any(not hasattr(os, name) for name in required):
        raise StableFilesystemError("platform lacks non-following directory handles")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0)
    return flags


def _open_posix_absolute_directory(path: Path) -> int:
    if not path.is_absolute():
        raise StableFilesystemError("managed directory must be absolute")
    if os.open not in os.supports_dir_fd or os.mkdir not in os.supports_dir_fd:
        raise StableFilesystemError("platform lacks descriptor-relative filesystem operations")
    flags = _posix_directory_flags()
    anchor = path.anchor
    if not anchor:
        raise StableFilesystemError("managed directory has no filesystem anchor")
    current = os.open(anchor, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        for part in path.parts[1:]:
            next_fd = os.open(part, flags, dir_fd=current)
            os.close(current)
            current = next_fd
        return current
    except BaseException:
        os.close(current)
        raise


def _posix_resource_id(fd: int) -> str:
    return f"device:{int(os.fstat(fd).st_dev)}"


if os.name == "nt":
    import ctypes
    import msvcrt
    from ctypes import wintypes

    _INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
    _GENERIC_READ = 0x80000000
    _GENERIC_WRITE = 0x40000000
    _FILE_READ_ATTRIBUTES = 0x0080
    _FILE_SHARE_READ = 0x00000001
    _FILE_SHARE_WRITE = 0x00000002
    _FILE_SHARE_DELETE = 0x00000004
    _CREATE_NEW = 1
    _OPEN_EXISTING = 3
    _FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
    _FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
    _FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
    _FILE_ATTRIBUTE_TAG_INFO_CLASS = 9

    class _FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [
            ("FileAttributes", wintypes.DWORD),
            ("ReparseTag", wintypes.DWORD),
        ]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _CreateFileW = _kernel32.CreateFileW
    _CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    _CreateFileW.restype = wintypes.HANDLE
    _CloseHandle = _kernel32.CloseHandle
    _CloseHandle.argtypes = [wintypes.HANDLE]
    _CloseHandle.restype = wintypes.BOOL
    _GetFileInformationByHandleEx = _kernel32.GetFileInformationByHandleEx
    _GetFileInformationByHandleEx.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    _GetFileInformationByHandleEx.restype = wintypes.BOOL


def _raise_windows_error(message: str) -> None:
    if os.name != "nt":
        raise AssertionError("Windows helper called on non-Windows platform")
    error = ctypes.get_last_error()  # type: ignore[name-defined]
    raise StableFilesystemError(error, message)


def _windows_open_directory(path: Path) -> int:
    handle = _CreateFileW(  # type: ignore[name-defined]
        str(path),
        _FILE_READ_ATTRIBUTES,  # type: ignore[name-defined]
        _FILE_SHARE_READ | _FILE_SHARE_WRITE,  # type: ignore[name-defined]
        None,
        _OPEN_EXISTING,  # type: ignore[name-defined]
        _FILE_FLAG_BACKUP_SEMANTICS | _FILE_FLAG_OPEN_REPARSE_POINT,  # type: ignore[name-defined]
        None,
    )
    if handle == _INVALID_HANDLE_VALUE:  # type: ignore[name-defined]
        _raise_windows_error(f"cannot pin managed directory: {path}")
    info = _FileAttributeTagInfo()  # type: ignore[name-defined]
    if not _GetFileInformationByHandleEx(  # type: ignore[name-defined]
        handle,
        _FILE_ATTRIBUTE_TAG_INFO_CLASS,  # type: ignore[name-defined]
        ctypes.byref(info),  # type: ignore[name-defined]
        ctypes.sizeof(info),  # type: ignore[name-defined]
    ):
        _CloseHandle(handle)  # type: ignore[name-defined]
        _raise_windows_error(f"cannot inspect managed directory: {path}")
    if info.FileAttributes & _FILE_ATTRIBUTE_REPARSE_POINT:  # type: ignore[name-defined]
        _CloseHandle(handle)  # type: ignore[name-defined]
        raise StableFilesystemError(f"managed directory is a reparse point: {path}")
    return int(handle)


def _windows_open_file(path: Path, *, writable: bool = False, create_new: bool = False) -> int:
    access = _GENERIC_READ  # type: ignore[name-defined]
    if writable:
        access |= _GENERIC_WRITE  # type: ignore[name-defined]
    handle = _CreateFileW(  # type: ignore[name-defined]
        str(path),
        access,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,  # type: ignore[name-defined]
        None,
        _CREATE_NEW if create_new else _OPEN_EXISTING,  # type: ignore[name-defined]
        _FILE_FLAG_OPEN_REPARSE_POINT,  # type: ignore[name-defined]
        None,
    )
    if handle == _INVALID_HANDLE_VALUE:  # type: ignore[name-defined]
        _raise_windows_error(f"cannot open managed file: {path}")
    info = _FileAttributeTagInfo()  # type: ignore[name-defined]
    if not _GetFileInformationByHandleEx(  # type: ignore[name-defined]
        handle,
        _FILE_ATTRIBUTE_TAG_INFO_CLASS,  # type: ignore[name-defined]
        ctypes.byref(info),  # type: ignore[name-defined]
        ctypes.sizeof(info),  # type: ignore[name-defined]
    ):
        _CloseHandle(handle)  # type: ignore[name-defined]
        _raise_windows_error(f"cannot inspect managed file: {path}")
    if info.FileAttributes & _FILE_ATTRIBUTE_REPARSE_POINT:  # type: ignore[name-defined]
        _CloseHandle(handle)  # type: ignore[name-defined]
        raise StableFilesystemError(f"managed file is a reparse point: {path}")
    flags = os.O_BINARY | (os.O_RDWR if writable else os.O_RDONLY)
    return msvcrt.open_osfhandle(int(handle), flags)  # type: ignore[name-defined]


class StableDirectory:
    """One directory held stable for descriptor/handle-relative write operations."""

    def __init__(self, path: Path, *, fd: int | None = None, handles: list[int] | None = None) -> None:
        self.path = path
        self._fd = fd
        self._handles = handles or []

    @property
    def resource_id(self) -> str:
        if os.name != "nt":
            assert self._fd is not None
            return _posix_resource_id(self._fd)
        value = self.path.stat()
        device = int(value.st_dev)
        if device:
            return f"volume:{device}"
        anchor = self.path.anchor.casefold()
        if not anchor:
            raise StableFilesystemError("Windows managed path has no resource identity")
        return f"volume-anchor:{anchor}"

    def close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        if os.name == "nt":
            while self._handles:
                _CloseHandle(self._handles.pop())  # type: ignore[name-defined]

    def _full(self, name: str) -> Path:
        if Path(name).name != name or name in ("", ".", ".."):
            raise StableFilesystemError("managed file name must be one path component")
        return self.path / name

    def stat(self, name: str) -> os.stat_result:
        if os.name != "nt":
            assert self._fd is not None
            value = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            if stat.S_ISLNK(value.st_mode):
                raise StableFilesystemError("managed file is a symbolic link")
            return value
        fd = _windows_open_file(self._full(name))
        try:
            return os.fstat(fd)
        finally:
            os.close(fd)

    def is_file(self, name: str) -> bool:
        try:
            return stat.S_ISREG(self.stat(name).st_mode)
        except FileNotFoundError:
            return False

    @contextlib.contextmanager
    def open_read(self, name: str) -> Iterator[BinaryIO]:
        if os.name != "nt":
            assert self._fd is not None
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(name, flags, dir_fd=self._fd)
        else:
            fd = _windows_open_file(self._full(name))
        with os.fdopen(fd, "rb") as handle:
            yield handle

    def sha256(self, name: str) -> str:
        digest = hashlib.sha256()
        with self.open_read(name) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return f"sha256:{digest.hexdigest()}"

    @contextlib.contextmanager
    def temporary_file(self, *, prefix: str = "tmp", suffix: str = "") -> Iterator[tuple[str, BinaryIO]]:
        fd: int | None = None
        name = ""
        for _attempt in range(128):
            name = f"{prefix}{secrets.token_hex(12)}{suffix}"
            try:
                if os.name != "nt":
                    assert self._fd is not None
                    flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
                    flags |= getattr(os, "O_NOFOLLOW", 0)
                    fd = os.open(name, flags, 0o600, dir_fd=self._fd)
                else:
                    fd = _windows_open_file(self._full(name), writable=True, create_new=True)
                break
            except FileExistsError:
                continue
        if fd is None:
            raise StableFilesystemError("could not allocate a unique managed temp file")
        try:
            with os.fdopen(fd, "w+b") as handle:
                yield name, handle
        finally:
            self.unlink(name, missing_ok=True)

    def replace(self, source_name: str, destination_name: str) -> None:
        self._full(source_name)
        self._full(destination_name)
        if os.name != "nt":
            assert self._fd is not None
            if os.rename not in os.supports_dir_fd:
                raise StableFilesystemError("platform lacks descriptor-relative atomic rename")
            os.rename(
                source_name,
                destination_name,
                src_dir_fd=self._fd,
                dst_dir_fd=self._fd,
            )
        else:
            os.replace(self._full(source_name), self._full(destination_name))

    def unlink(self, name: str, *, missing_ok: bool = False) -> None:
        self._full(name)
        try:
            if os.name != "nt":
                assert self._fd is not None
                os.unlink(name, dir_fd=self._fd)
            else:
                os.unlink(self._full(name))
        except FileNotFoundError:
            if not missing_ok:
                raise

    def fsync(self) -> None:
        if os.name != "nt":
            assert self._fd is not None
            os.fsync(self._fd)


@contextlib.contextmanager
def stable_directory(root: Path, relative: Path = Path("."), *, create: bool = False) -> Iterator[StableDirectory]:
    """Open ``root / relative`` without following managed directory redirects."""

    root = Path(root)
    parts = _relative_parts(Path(relative))
    if os.name != "nt":
        current = _open_posix_absolute_directory(root)
        try:
            flags = _posix_directory_flags()
            for part in parts:
                try:
                    next_fd = os.open(part, flags, dir_fd=current)
                except FileNotFoundError:
                    if not create:
                        raise
                    try:
                        os.mkdir(part, 0o700, dir_fd=current)
                    except FileExistsError:
                        pass
                    next_fd = os.open(part, flags, dir_fd=current)
                os.close(current)
                current = next_fd
            boundary = StableDirectory(root.joinpath(*parts), fd=current)
            current = -1
            try:
                yield boundary
            finally:
                boundary.close()
        finally:
            if current >= 0:
                os.close(current)
        return

    if not root.is_absolute():
        raise StableFilesystemError("managed directory must be absolute")
    handles: list[int] = []
    current_path = Path(root.anchor)
    try:
        handles.append(_windows_open_directory(current_path))
        for part in root.parts[1:]:
            current_path = current_path / part
            handles.append(_windows_open_directory(current_path))
        for part in parts:
            current_path = current_path / part
            try:
                handles.append(_windows_open_directory(current_path))
            except StableFilesystemError:
                if not create or current_path.exists():
                    raise
                try:
                    os.mkdir(current_path)
                except FileExistsError:
                    pass
                handles.append(_windows_open_directory(current_path))
        boundary = StableDirectory(current_path, handles=handles)
        handles = []
        try:
            yield boundary
        finally:
            boundary.close()
    finally:
        while handles:
            _CloseHandle(handles.pop())  # type: ignore[name-defined]


__all__ = ["StableDirectory", "StableFilesystemError", "stable_directory"]
