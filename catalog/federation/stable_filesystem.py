"""Stable filesystem public surface with pinned Windows volume identity.

The implementation is kept in ``_stable_filesystem_impl``. On Windows,
CPython 3.12 may expose a 64-bit volume serial through ``st_dev``. The managed
boundary compares against the same identity from the already-open handle and
uses native handle-relative rename so lexical path rebinding cannot redirect a
managed replacement.
"""

from __future__ import annotations

import os
from pathlib import Path

from . import _stable_filesystem_impl as _impl

StableFilesystemError = _impl.StableFilesystemError
TemporaryScavengeReport = _impl.TemporaryScavengeReport
TEMPORARY_OWNER_SUFFIX = _impl._TEMP_OWNER_SUFFIX


if os.name == "nt":
    import ctypes
    from ctypes import wintypes

    _FILE_ID_INFO_CLASS = 18
    _FILE_RENAME_INFORMATION_CLASS = 10
    _FILE_RENAME_INFORMATION_EX_CLASS = 65
    _FILE_RENAME_REPLACE_IF_EXISTS = 0x00000001
    _FILE_RENAME_POSIX_SEMANTICS = 0x00000002

    class _FileId128(ctypes.Structure):
        _fields_ = [("Identifier", ctypes.c_ubyte * 16)]

    class _FileIdInfo(ctypes.Structure):
        _fields_ = [
            ("VolumeSerialNumber", ctypes.c_ulonglong),
            ("FileId", _FileId128),
        ]

    class _FileRenameInformation(ctypes.Structure):
        _fields_ = [
            ("ReplaceIfExists", ctypes.c_ubyte),
            ("RootDirectory", wintypes.HANDLE),
            ("FileNameLength", wintypes.ULONG),
            ("FileName", wintypes.WCHAR * 1),
        ]

    class _FileRenameInformationEx(ctypes.Structure):
        _fields_ = [
            ("Flags", wintypes.ULONG),
            ("RootDirectory", wintypes.HANDLE),
            ("FileNameLength", wintypes.ULONG),
            ("FileName", wintypes.WCHAR * 1),
        ]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _GetFileInformationByHandleEx = _kernel32.GetFileInformationByHandleEx
    _GetFileInformationByHandleEx.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    _GetFileInformationByHandleEx.restype = wintypes.BOOL

    _NtSetInformationFile = _impl._ntdll.NtSetInformationFile
    _NtSetInformationFile.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_impl._IoStatusBlock),
        wintypes.LPVOID,
        wintypes.ULONG,
        ctypes.c_int,
    ]
    _NtSetInformationFile.restype = wintypes.LONG

    def _pinned_windows_resource_id(directory: _impl.StableDirectory) -> str:
        handle = directory._handle
        if handle is None:
            raise StableFilesystemError("managed Windows directory handle is closed")
        info = _FileIdInfo()
        if not _GetFileInformationByHandleEx(
            wintypes.HANDLE(handle),
            _FILE_ID_INFO_CLASS,
            ctypes.byref(info),
            ctypes.sizeof(info),
        ):
            error = ctypes.get_last_error()
            raise StableFilesystemError(
                error,
                "cannot identify managed filesystem resource from pinned handle",
            )
        return f"volume:{int(info.VolumeSerialNumber)}"

    def _replace_relative(
        parent_handle: int,
        source_name: str,
        destination_name: str,
        *,
        destination_parent_handle: int | None = None,
        preserve_readers: bool = False,
    ) -> None:
        source = _impl._windows_nt_open(
            parent_handle,
            source_name,
            desired_access=(
                _impl._DELETE | _impl._FILE_READ_ATTRIBUTES | _impl._SYNCHRONIZE
            ),
            create_disposition=_impl._FILE_OPEN,
            create_options=(
                _impl._FILE_NON_DIRECTORY_FILE | _impl._FILE_SYNCHRONOUS_IO_NONALERT
            ),
        )
        try:
            encoded = destination_name.encode("utf-16-le")
            structure = (
                _FileRenameInformationEx if preserve_readers else _FileRenameInformation
            )
            offset = structure.FileName.offset
            buffer = ctypes.create_string_buffer(offset + len(encoded))
            info = structure.from_buffer(buffer)
            if preserve_readers:
                info.Flags = (
                    _FILE_RENAME_REPLACE_IF_EXISTS | _FILE_RENAME_POSIX_SEMANTICS
                )
            else:
                info.ReplaceIfExists = 1
            info.RootDirectory = wintypes.HANDLE(
                parent_handle
                if destination_parent_handle is None
                else destination_parent_handle
            )
            info.FileNameLength = len(encoded)
            ctypes.memmove(ctypes.addressof(buffer) + offset, encoded, len(encoded))
            iosb = _impl._IoStatusBlock()
            status = int(
                _NtSetInformationFile(
                    wintypes.HANDLE(source),
                    ctypes.byref(iosb),
                    buffer,
                    len(buffer),
                    _FILE_RENAME_INFORMATION_EX_CLASS
                    if preserve_readers
                    else _FILE_RENAME_INFORMATION_CLASS,
                )
            )
            if status < 0:
                _impl._raise_ntstatus(
                    status,
                    f"cannot replace managed file {source_name} with {destination_name}",
                )
        finally:
            _impl._CloseHandle(wintypes.HANDLE(source))

    _impl.StableDirectory.resource_id = property(_pinned_windows_resource_id)  # type: ignore[assignment]
    _impl._windows_replace_relative = _replace_relative


StableDirectory = _impl.StableDirectory
stable_directory = _impl.stable_directory


def replace_preserving_readers(source: Path, destination: Path) -> None:
    """Atomically publish a file while existing readers retain the old version.

    Both parents are pinned and must belong to the same filesystem. Windows
    requires FileRenameInformationEx with POSIX semantics; unsupported operations
    fail closed, with no delete-first or copy fallback. Other replacement callers
    retain their existing semantics.
    """
    with (
        stable_directory(source.parent) as origin,
        stable_directory(destination.parent) as target,
    ):
        if origin.resource_id != target.resource_id:
            raise StableFilesystemError(
                "atomic publication crossed filesystem resources"
            )
        if os.name == "nt":
            assert origin._handle is not None and target._handle is not None
            _replace_relative(
                origin._handle,
                source.name,
                destination.name,
                destination_parent_handle=target._handle,
                preserve_readers=True,
            )
        else:
            if os.rename not in os.supports_dir_fd:
                raise StableFilesystemError(
                    "platform lacks descriptor-relative atomic rename"
                )
            assert origin._fd is not None and target._fd is not None
            os.rename(
                source.name,
                destination.name,
                src_dir_fd=origin._fd,
                dst_dir_fd=target._fd,
            )


__all__ = [
    "TEMPORARY_OWNER_SUFFIX",
    "StableDirectory",
    "StableFilesystemError",
    "TemporaryScavengeReport",
    "replace_preserving_readers",
    "stable_directory",
]
