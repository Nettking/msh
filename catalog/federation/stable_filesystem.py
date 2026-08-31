"""Stable filesystem public surface with pinned Windows volume identity.

The implementation is kept in ``_stable_filesystem_impl``. On Windows,
CPython 3.12 may expose a 64-bit volume serial through ``st_dev``. The managed
boundary compares against the same identity from the already-open handle and
uses native handle-relative rename so lexical path rebinding cannot redirect a
managed replacement.
"""

from __future__ import annotations

import os

from . import _stable_filesystem_impl as _impl

StableFilesystemError = _impl.StableFilesystemError
TemporaryScavengeReport = _impl.TemporaryScavengeReport
TEMPORARY_OWNER_SUFFIX = _impl._TEMP_OWNER_SUFFIX


if os.name == "nt":
    import ctypes
    from ctypes import wintypes

    _FILE_ID_INFO_CLASS = 18
    _FILE_RENAME_INFORMATION_CLASS = 10

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

    def _replace_relative(parent_handle: int, source_name: str, destination_name: str) -> None:
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
            offset = _FileRenameInformation.FileName.offset
            buffer = ctypes.create_string_buffer(offset + len(encoded))
            info = _FileRenameInformation.from_buffer(buffer)
            info.ReplaceIfExists = 1
            info.RootDirectory = wintypes.HANDLE(parent_handle)
            info.FileNameLength = len(encoded)
            ctypes.memmove(ctypes.addressof(buffer) + offset, encoded, len(encoded))
            iosb = _impl._IoStatusBlock()
            status = int(
                _NtSetInformationFile(
                    wintypes.HANDLE(source),
                    ctypes.byref(iosb),
                    buffer,
                    len(buffer),
                    _FILE_RENAME_INFORMATION_CLASS,
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

__all__ = [
    "StableDirectory",
    "StableFilesystemError",
    "TEMPORARY_OWNER_SUFFIX",
    "TemporaryScavengeReport",
    "stable_directory",
]
