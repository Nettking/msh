"""Stable filesystem public surface with pinned Windows volume identity.

The implementation is kept in ``_stable_filesystem_impl``.  On Windows,
CPython 3.12 may expose a 64-bit volume serial through ``st_dev``.  The managed
boundary must compare against the same 64-bit identity from the already-open
handle rather than the legacy 32-bit ``BY_HANDLE_FILE_INFORMATION`` value.
"""

from __future__ import annotations

import os

from . import _stable_filesystem_impl as _impl

StableFilesystemError = _impl.StableFilesystemError


if os.name == "nt":
    import ctypes
    from ctypes import wintypes

    _FILE_ID_INFO_CLASS = 18

    class _FileId128(ctypes.Structure):
        _fields_ = [("Identifier", ctypes.c_ubyte * 16)]

    class _FileIdInfo(ctypes.Structure):
        _fields_ = [
            ("VolumeSerialNumber", ctypes.c_ulonglong),
            ("FileId", _FileId128),
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

    def _pinned_windows_resource_id(directory: _impl.StableDirectory) -> str:
        handle = directory._handle  # noqa: SLF001 - pinned handle is the security boundary
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

    _impl.StableDirectory.resource_id = property(_pinned_windows_resource_id)  # type: ignore[assignment]


StableDirectory = _impl.StableDirectory
stable_directory = _impl.stable_directory

__all__ = ["StableDirectory", "StableFilesystemError", "stable_directory"]
