"""Stable directory handles for race-safe managed filesystem writes.

The helpers in this module keep a managed destination anchored while files are
created or replaced. POSIX uses non-following descriptor-relative traversal and
operations. Windows uses handle-relative NT opens plus handle-relative rename
and deletion so rebinding a lexical path cannot redirect a managed write.
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
    _FILE_LIST_DIRECTORY = 0x0001
    _FILE_READ_DATA = 0x0001
    _FILE_WRITE_DATA = 0x0002
    _FILE_READ_ATTRIBUTES = 0x0080
    _FILE_WRITE_ATTRIBUTES = 0x0100
    _DELETE = 0x00010000
    _SYNCHRONIZE = 0x00100000
    _FILE_SHARE_READ = 0x00000001
    _FILE_SHARE_WRITE = 0x00000002
    _FILE_SHARE_DELETE = 0x00000004
    _FILE_CREATE = 2
    _FILE_OPEN = 1
    _FILE_OPEN_IF = 3
    _FILE_ATTRIBUTE_NORMAL = 0x00000080
    _FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
    _FILE_DIRECTORY_FILE = 0x00000001
    _FILE_SYNCHRONOUS_IO_NONALERT = 0x00000020
    _FILE_NON_DIRECTORY_FILE = 0x00000040
    _OBJ_CASE_INSENSITIVE = 0x00000040
    _OBJ_DONT_REPARSE = 0x00001000
    _FILE_ATTRIBUTE_TAG_INFO_CLASS = 9
    _FILE_RENAME_INFO_CLASS = 3
    _FILE_DISPOSITION_INFO_CLASS = 4

    class _UnicodeString(ctypes.Structure):
        _fields_ = [
            ("Length", wintypes.USHORT),
            ("MaximumLength", wintypes.USHORT),
            ("Buffer", wintypes.LPWSTR),
        ]

    class _ObjectAttributes(ctypes.Structure):
        _fields_ = [
            ("Length", wintypes.ULONG),
            ("RootDirectory", wintypes.HANDLE),
            ("ObjectName", ctypes.POINTER(_UnicodeString)),
            ("Attributes", wintypes.ULONG),
            ("SecurityDescriptor", wintypes.LPVOID),
            ("SecurityQualityOfService", wintypes.LPVOID),
        ]

    class _IoStatusBlock(ctypes.Structure):
        _fields_ = [("Status", wintypes.LPVOID), ("Information", ctypes.c_size_t)]

    class _FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [
            ("FileAttributes", wintypes.DWORD),
            ("ReparseTag", wintypes.DWORD),
        ]

    class _ByHandleFileInformation(ctypes.Structure):
        _fields_ = [
            ("dwFileAttributes", wintypes.DWORD),
            ("ftCreationTime", wintypes.FILETIME),
            ("ftLastAccessTime", wintypes.FILETIME),
            ("ftLastWriteTime", wintypes.FILETIME),
            ("dwVolumeSerialNumber", wintypes.DWORD),
            ("nFileSizeHigh", wintypes.DWORD),
            ("nFileSizeLow", wintypes.DWORD),
            ("nNumberOfLinks", wintypes.DWORD),
            ("nFileIndexHigh", wintypes.DWORD),
            ("nFileIndexLow", wintypes.DWORD),
        ]

    class _FileRenameInfo(ctypes.Structure):
        _fields_ = [
            ("ReplaceIfExists", wintypes.DWORD),
            ("RootDirectory", wintypes.HANDLE),
            ("FileNameLength", wintypes.DWORD),
            ("FileName", wintypes.WCHAR * 1),
        ]

    class _FileDispositionInfo(ctypes.Structure):
        _fields_ = [("DeleteFile", wintypes.BOOL)]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _ntdll = ctypes.WinDLL("ntdll", use_last_error=True)

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
    _GetFileInformationByHandle = _kernel32.GetFileInformationByHandle
    _GetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    ]
    _GetFileInformationByHandle.restype = wintypes.BOOL
    _GetFileInformationByHandleEx = _kernel32.GetFileInformationByHandleEx
    _GetFileInformationByHandleEx.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    _GetFileInformationByHandleEx.restype = wintypes.BOOL
    _SetFileInformationByHandle = _kernel32.SetFileInformationByHandle
    _SetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    _SetFileInformationByHandle.restype = wintypes.BOOL
    _NtCreateFile = _ntdll.NtCreateFile
    _NtCreateFile.argtypes = [
        ctypes.POINTER(wintypes.HANDLE),
        wintypes.DWORD,
        ctypes.POINTER(_ObjectAttributes),
        ctypes.POINTER(_IoStatusBlock),
        wintypes.LPVOID,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.LPVOID,
        wintypes.ULONG,
    ]
    _NtCreateFile.restype = wintypes.LONG
    _RtlNtStatusToDosError = _ntdll.RtlNtStatusToDosError
    _RtlNtStatusToDosError.argtypes = [wintypes.LONG]
    _RtlNtStatusToDosError.restype = wintypes.ULONG


def _windows_error(error: int, message: str) -> OSError:
    if error in (2, 3):
        return FileNotFoundError(error, message)
    if error in (80, 183):
        return FileExistsError(error, message)
    return StableFilesystemError(error, message)


def _raise_windows_last_error(message: str) -> None:
    if os.name != "nt":
        raise AssertionError("Windows helper called on non-Windows platform")
    raise _windows_error(ctypes.get_last_error(), message)  # type: ignore[name-defined]


def _raise_ntstatus(status: int, message: str) -> None:
    if os.name != "nt":
        raise AssertionError("Windows helper called on non-Windows platform")
    error = int(_RtlNtStatusToDosError(status))  # type: ignore[name-defined]
    raise _windows_error(error, message)


def _windows_reject_reparse(handle: int, *, path: str) -> None:
    info = _FileAttributeTagInfo()  # type: ignore[name-defined]
    if not _GetFileInformationByHandleEx(  # type: ignore[name-defined]
        wintypes.HANDLE(handle),  # type: ignore[name-defined]
        _FILE_ATTRIBUTE_TAG_INFO_CLASS,  # type: ignore[name-defined]
        ctypes.byref(info),  # type: ignore[name-defined]
        ctypes.sizeof(info),  # type: ignore[name-defined]
    ):
        _raise_windows_last_error(f"cannot inspect managed object: {path}")
    if info.FileAttributes & _FILE_ATTRIBUTE_REPARSE_POINT:  # type: ignore[name-defined]
        raise StableFilesystemError(f"managed object is a reparse point: {path}")


def _windows_resource_id(handle: int) -> str:
    info = _ByHandleFileInformation()  # type: ignore[name-defined]
    if not _GetFileInformationByHandle(  # type: ignore[name-defined]
        wintypes.HANDLE(handle), ctypes.byref(info)  # type: ignore[name-defined]
    ):
        _raise_windows_last_error("cannot identify managed filesystem resource")
    return f"volume:{int(info.dwVolumeSerialNumber)}"


def _windows_open_anchor(path: Path) -> int:
    handle = _CreateFileW(  # type: ignore[name-defined]
        str(path),
        _FILE_READ_ATTRIBUTES,  # type: ignore[name-defined]
        _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,  # type: ignore[name-defined]
        None,
        3,
        0x02000000 | 0x00200000,
        None,
    )
    if handle == _INVALID_HANDLE_VALUE:  # type: ignore[name-defined]
        _raise_windows_last_error(f"cannot open managed filesystem anchor: {path}")
    value = int(handle)
    try:
        _windows_reject_reparse(value, path=str(path))
        return value
    except BaseException:
        _CloseHandle(wintypes.HANDLE(value))  # type: ignore[name-defined]
        raise


def _windows_nt_open(
    root_handle: int,
    name: str,
    *,
    desired_access: int,
    create_disposition: int,
    create_options: int,
    file_attributes: int = 0,
) -> int:
    encoded_length = len(name.encode("utf-16-le"))
    name_buffer = ctypes.create_unicode_buffer(name)  # type: ignore[name-defined]
    unicode_name = _UnicodeString(  # type: ignore[name-defined]
        Length=encoded_length,
        MaximumLength=encoded_length + 2,
        Buffer=ctypes.cast(name_buffer, wintypes.LPWSTR),  # type: ignore[name-defined]
    )
    attributes = _ObjectAttributes(  # type: ignore[name-defined]
        Length=ctypes.sizeof(_ObjectAttributes),  # type: ignore[name-defined]
        RootDirectory=wintypes.HANDLE(root_handle),  # type: ignore[name-defined]
        ObjectName=ctypes.pointer(unicode_name),  # type: ignore[name-defined]
        Attributes=_OBJ_CASE_INSENSITIVE | _OBJ_DONT_REPARSE,  # type: ignore[name-defined]
        SecurityDescriptor=None,
        SecurityQualityOfService=None,
    )
    iosb = _IoStatusBlock()  # type: ignore[name-defined]
    handle = wintypes.HANDLE()  # type: ignore[name-defined]
    status = int(
        _NtCreateFile(  # type: ignore[name-defined]
            ctypes.byref(handle),  # type: ignore[name-defined]
            desired_access,
            ctypes.byref(attributes),  # type: ignore[name-defined]
            ctypes.byref(iosb),  # type: ignore[name-defined]
            None,
            file_attributes,
            _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,  # type: ignore[name-defined]
            create_disposition,
            create_options,
            None,
            0,
        )
    )
    if status < 0:
        _raise_ntstatus(status, f"cannot open managed child: {name}")
    if not handle.value:
        raise StableFilesystemError("NtCreateFile returned no managed child handle")
    value = int(handle.value)
    try:
        _windows_reject_reparse(value, path=name)
        return value
    except BaseException:
        _CloseHandle(wintypes.HANDLE(value))  # type: ignore[name-defined]
        raise


def _windows_open_relative_directory(parent_handle: int, name: str, *, create: bool) -> int:
    disposition = _FILE_OPEN_IF if create else _FILE_OPEN  # type: ignore[name-defined]
    return _windows_nt_open(
        parent_handle,
        name,
        desired_access=_FILE_LIST_DIRECTORY | _FILE_READ_ATTRIBUTES | _SYNCHRONIZE,  # type: ignore[name-defined]
        create_disposition=disposition,
        create_options=_FILE_DIRECTORY_FILE | _FILE_SYNCHRONOUS_IO_NONALERT,  # type: ignore[name-defined]
    )


def _windows_open_relative_file(
    parent_handle: int, name: str, *, writable: bool = False, create_new: bool = False
) -> int:
    access = _FILE_READ_DATA | _FILE_READ_ATTRIBUTES | _SYNCHRONIZE  # type: ignore[name-defined]
    if writable:
        access |= _FILE_WRITE_DATA | _FILE_WRITE_ATTRIBUTES | _DELETE  # type: ignore[name-defined]
    raw = _windows_nt_open(
        parent_handle,
        name,
        desired_access=access,
        create_disposition=_FILE_CREATE if create_new else _FILE_OPEN,  # type: ignore[name-defined]
        create_options=_FILE_NON_DIRECTORY_FILE | _FILE_SYNCHRONOUS_IO_NONALERT,  # type: ignore[name-defined]
        file_attributes=_FILE_ATTRIBUTE_NORMAL,  # type: ignore[name-defined]
    )
    flags = os.O_BINARY | (os.O_RDWR if writable else os.O_RDONLY)
    try:
        return msvcrt.open_osfhandle(raw, flags)  # type: ignore[name-defined]
    except BaseException:
        _CloseHandle(wintypes.HANDLE(raw))  # type: ignore[name-defined]
        raise


def _windows_replace_relative(parent_handle: int, source_name: str, destination_name: str) -> None:
    source = _windows_nt_open(
        parent_handle,
        source_name,
        desired_access=_DELETE | _FILE_READ_ATTRIBUTES | _SYNCHRONIZE,  # type: ignore[name-defined]
        create_disposition=_FILE_OPEN,  # type: ignore[name-defined]
        create_options=_FILE_NON_DIRECTORY_FILE | _FILE_SYNCHRONOUS_IO_NONALERT,  # type: ignore[name-defined]
    )
    try:
        encoded = destination_name.encode("utf-16-le")
        offset = _FileRenameInfo.FileName.offset  # type: ignore[name-defined]
        buffer = ctypes.create_string_buffer(offset + len(encoded))  # type: ignore[name-defined]
        info = _FileRenameInfo.from_buffer(buffer)  # type: ignore[name-defined]
        info.ReplaceIfExists = 1
        info.RootDirectory = wintypes.HANDLE(parent_handle)  # type: ignore[name-defined]
        info.FileNameLength = len(encoded)
        ctypes.memmove(ctypes.addressof(buffer) + offset, encoded, len(encoded))  # type: ignore[name-defined]
        if not _SetFileInformationByHandle(  # type: ignore[name-defined]
            wintypes.HANDLE(source),  # type: ignore[name-defined]
            _FILE_RENAME_INFO_CLASS,  # type: ignore[name-defined]
            buffer,
            len(buffer),
        ):
            _raise_windows_last_error(
                f"cannot replace managed file {source_name} with {destination_name}"
            )
    finally:
        _CloseHandle(wintypes.HANDLE(source))  # type: ignore[name-defined]


def _windows_unlink_relative(parent_handle: int, name: str, *, missing_ok: bool) -> None:
    try:
        target = _windows_nt_open(
            parent_handle,
            name,
            desired_access=_DELETE | _FILE_READ_ATTRIBUTES | _SYNCHRONIZE,  # type: ignore[name-defined]
            create_disposition=_FILE_OPEN,  # type: ignore[name-defined]
            create_options=_FILE_NON_DIRECTORY_FILE | _FILE_SYNCHRONOUS_IO_NONALERT,  # type: ignore[name-defined]
        )
    except FileNotFoundError:
        if missing_ok:
            return
        raise
    try:
        disposition = _FileDispositionInfo(DeleteFile=True)  # type: ignore[name-defined]
        if not _SetFileInformationByHandle(  # type: ignore[name-defined]
            wintypes.HANDLE(target),  # type: ignore[name-defined]
            _FILE_DISPOSITION_INFO_CLASS,  # type: ignore[name-defined]
            ctypes.byref(disposition),  # type: ignore[name-defined]
            ctypes.sizeof(disposition),  # type: ignore[name-defined]
        ):
            _raise_windows_last_error(f"cannot unlink managed file: {name}")
    finally:
        _CloseHandle(wintypes.HANDLE(target))  # type: ignore[name-defined]


class StableDirectory:
    """One directory held stable for descriptor/handle-relative write operations."""

    def __init__(
        self,
        path: Path,
        *,
        fd: int | None = None,
        handle: int | None = None,
        handles: list[int] | None = None,
    ) -> None:
        self.path = path
        self._fd = fd
        self._handle = handle
        self._handles = handles or []

    @property
    def resource_id(self) -> str:
        if os.name != "nt":
            assert self._fd is not None
            return _posix_resource_id(self._fd)
        assert self._handle is not None
        return _windows_resource_id(self._handle)

    def close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        if os.name == "nt":
            if self._handle is not None:
                _CloseHandle(wintypes.HANDLE(self._handle))  # type: ignore[name-defined]
                self._handle = None
            while self._handles:
                _CloseHandle(wintypes.HANDLE(self._handles.pop()))  # type: ignore[name-defined]

    def _name(self, name: str) -> str:
        if Path(name).name != name or name in ("", ".", ".."):
            raise StableFilesystemError("managed file name must be one path component")
        return name

    def stat(self, name: str) -> os.stat_result:
        name = self._name(name)
        if os.name != "nt":
            assert self._fd is not None
            value = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            if stat.S_ISLNK(value.st_mode):
                raise StableFilesystemError("managed file is a symbolic link")
            return value
        assert self._handle is not None
        fd = _windows_open_relative_file(self._handle, name)
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
        name = self._name(name)
        if os.name != "nt":
            assert self._fd is not None
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(name, flags, dir_fd=self._fd)
        else:
            assert self._handle is not None
            fd = _windows_open_relative_file(self._handle, name)
        with os.fdopen(fd, "rb") as handle:
            yield handle

    def sha256(self, name: str) -> str:
        digest = hashlib.sha256()
        with self.open_read(name) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return f"sha256:{digest.hexdigest()}"

    @contextlib.contextmanager
    def temporary_file(
        self, *, prefix: str = "tmp", suffix: str = ""
    ) -> Iterator[tuple[str, BinaryIO]]:
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
                    assert self._handle is not None
                    fd = _windows_open_relative_file(
                        self._handle, name, writable=True, create_new=True
                    )
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
        source_name = self._name(source_name)
        destination_name = self._name(destination_name)
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
            assert self._handle is not None
            _windows_replace_relative(self._handle, source_name, destination_name)

    def unlink(self, name: str, *, missing_ok: bool = False) -> None:
        name = self._name(name)
        if os.name != "nt":
            assert self._fd is not None
            try:
                os.unlink(name, dir_fd=self._fd)
            except FileNotFoundError:
                if not missing_ok:
                    raise
            return
        assert self._handle is not None
        _windows_unlink_relative(self._handle, name, missing_ok=missing_ok)

    def fsync(self) -> None:
        if os.name != "nt":
            assert self._fd is not None
            os.fsync(self._fd)


@contextlib.contextmanager
def stable_directory(
    root: Path, relative: Path = Path("."), *, create: bool = False
) -> Iterator[StableDirectory]:
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
    anchor = Path(root.anchor)
    if not root.anchor:
        raise StableFilesystemError("Windows managed path has no filesystem anchor")
    current = _windows_open_anchor(anchor)
    ancestors: list[int] = []
    try:
        for part in (*root.parts[1:], *parts):
            next_handle = _windows_open_relative_directory(current, part, create=create)
            ancestors.append(current)
            current = next_handle
        boundary = StableDirectory(
            root.joinpath(*parts), handle=current, handles=ancestors
        )
        current = -1
        ancestors = []
        try:
            yield boundary
        finally:
            boundary.close()
    finally:
        if current >= 0:
            _CloseHandle(wintypes.HANDLE(current))  # type: ignore[name-defined]
        while ancestors:
            _CloseHandle(wintypes.HANDLE(ancestors.pop()))  # type: ignore[name-defined]


__all__ = ["StableDirectory", "StableFilesystemError", "stable_directory"]
