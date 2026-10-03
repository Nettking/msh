"""Operation-scoped Windows Job Object for bounded Recorder final sync.

The final-sync root is created suspended, assigned to a named job, and only
then resumed. Normal descendants inherit job membership; breakaway is not
enabled. The controller keeps one handle and the independent guard keeps
another, so either side can terminate the complete operation without relying
on periodic parent-PID snapshots.
"""

from __future__ import annotations

import ctypes
import os
import re
import subprocess
import time
from collections.abc import Mapping, Sequence
from ctypes import wintypes
from dataclasses import dataclass
from typing import Self

_ERROR_ALREADY_EXISTS = 183
_ERROR_FILE_NOT_FOUND = 2
_ERROR_INVALID_NAME = 123
_JOB_OBJECT_QUERY = 0x0004
_JOB_OBJECT_TERMINATE = 0x0008
_JOB_OBJECT_SET_ATTRIBUTES = 0x0010
_JOB_OBJECT_ASSIGN_PROCESS = 0x0001
_JOB_OBJECT_ALL_COPY_RIGHTS = (
    _JOB_OBJECT_QUERY | _JOB_OBJECT_TERMINATE | _JOB_OBJECT_ASSIGN_PROCESS
)
_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
_JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION = 1
_CREATE_SUSPENDED = 0x00000004
_CREATE_UNICODE_ENVIRONMENT = 0x00000400
_CREATE_NO_WINDOW = 0x08000000
_STARTF_USESHOWWINDOW = 0x00000001
_SW_HIDE = 0
_STILL_ACTIVE = 259
_INFINITE = 0xFFFFFFFF
_WAIT_OBJECT_0 = 0
_WAIT_TIMEOUT = 258
_FILETIME_EPOCH_OFFSET_SECONDS = 11_644_473_600


class JobObjectError(RuntimeError):
    """A fail-closed Job Object operation failed."""


class _IoCounters(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_ulonglong),
        ("WriteOperationCount", ctypes.c_ulonglong),
        ("OtherOperationCount", ctypes.c_ulonglong),
        ("ReadTransferCount", ctypes.c_ulonglong),
        ("WriteTransferCount", ctypes.c_ulonglong),
        ("OtherTransferCount", ctypes.c_ulonglong),
    ]


class _BasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_longlong),
        ("PerJobUserTimeLimit", ctypes.c_longlong),
        ("LimitFlags", wintypes.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", wintypes.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", wintypes.DWORD),
        ("SchedulingClass", wintypes.DWORD),
    ]


class _ExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimitInformation),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class _BasicAccountingInformation(ctypes.Structure):
    _fields_ = [
        ("TotalUserTime", ctypes.c_longlong),
        ("TotalKernelTime", ctypes.c_longlong),
        ("ThisPeriodTotalUserTime", ctypes.c_longlong),
        ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
        ("TotalPageFaultCount", wintypes.DWORD),
        ("TotalProcesses", wintypes.DWORD),
        ("ActiveProcesses", wintypes.DWORD),
        ("TotalTerminatedProcesses", wintypes.DWORD),
    ]


class _StartupInfo(ctypes.Structure):
    _fields_ = [
        ("cb", wintypes.DWORD),
        ("lpReserved", wintypes.LPWSTR),
        ("lpDesktop", wintypes.LPWSTR),
        ("lpTitle", wintypes.LPWSTR),
        ("dwX", wintypes.DWORD),
        ("dwY", wintypes.DWORD),
        ("dwXSize", wintypes.DWORD),
        ("dwYSize", wintypes.DWORD),
        ("dwXCountChars", wintypes.DWORD),
        ("dwYCountChars", wintypes.DWORD),
        ("dwFillAttribute", wintypes.DWORD),
        ("dwFlags", wintypes.DWORD),
        ("wShowWindow", wintypes.WORD),
        ("cbReserved2", wintypes.WORD),
        ("lpReserved2", ctypes.POINTER(ctypes.c_ubyte)),
        ("hStdInput", wintypes.HANDLE),
        ("hStdOutput", wintypes.HANDLE),
        ("hStdError", wintypes.HANDLE),
    ]


class _ProcessInformation(ctypes.Structure):
    _fields_ = [
        ("hProcess", wintypes.HANDLE),
        ("hThread", wintypes.HANDLE),
        ("dwProcessId", wintypes.DWORD),
        ("dwThreadId", wintypes.DWORD),
    ]


class _SecurityAttributes(ctypes.Structure):
    _fields_ = [
        ("nLength", wintypes.DWORD),
        ("lpSecurityDescriptor", ctypes.c_void_p),
        ("bInheritHandle", wintypes.BOOL),
    ]


class _FileTime(ctypes.Structure):
    _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]


def operation_job_name(operation_id: str) -> str:
    if len(operation_id) != 32 or any(c not in "0123456789abcdef" for c in operation_id):
        raise ValueError("operation_id must be a lowercase 32-character hex identity")
    return f"Global\\FCPRecorderFinalSync-{operation_id}"


def _validate_sid(sid: str) -> str:
    if not isinstance(sid, str) or not re.fullmatch(r"S-1-(?:[0-9]+-){1,14}[0-9]+", sid):
        raise ValueError("copy-controller SID is invalid")
    return sid


def _kernel32():
    if os.name != "nt":
        raise OSError("Recorder final-sync Job Objects require Windows")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.OpenJobObjectW.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR]
    kernel32.OpenJobObjectW.restype = wintypes.HANDLE
    kernel32.QueryInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    kernel32.QueryInformationJobObject.restype = wintypes.BOOL
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.CreateProcessW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.BOOL,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.LPCWSTR,
        ctypes.POINTER(_StartupInfo),
        ctypes.POINTER(_ProcessInformation),
    ]
    kernel32.CreateProcessW.restype = wintypes.BOOL
    kernel32.ResumeThread.argtypes = [wintypes.HANDLE]
    kernel32.ResumeThread.restype = wintypes.DWORD
    kernel32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateProcess.restype = wintypes.BOOL
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateJobObject.restype = wintypes.BOOL
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_FileTime),
        ctypes.POINTER(_FileTime),
        ctypes.POINTER(_FileTime),
        ctypes.POINTER(_FileTime),
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    return kernel32


def _raise_last_error(operation: str) -> None:
    error = ctypes.get_last_error()
    raise JobObjectError(f"{operation} failed with Win32 error {error}")


@dataclass
class SuspendedJobProcess:
    """A child already assigned to its operation Job Object but not resumed."""

    job: OperationJob
    pid: int
    _process: int
    _thread: int
    _resumed: bool = False

    def resume(self) -> None:
        if self._resumed:
            raise JobObjectError("final-sync process was already resumed")
        kernel32 = _kernel32()
        previous = kernel32.ResumeThread(self._thread)
        if previous == 0xFFFFFFFF:
            _raise_last_error("ResumeThread")
        self._resumed = True

    def wait(self, timeout_seconds: float | None = None) -> int | None:
        kernel32 = _kernel32()
        timeout_ms = _INFINITE if timeout_seconds is None else max(
            0, min(_INFINITE - 1, int(timeout_seconds * 1000))
        )
        wait_result = kernel32.WaitForSingleObject(self._process, timeout_ms)
        if wait_result == _WAIT_TIMEOUT:
            return None
        if wait_result != _WAIT_OBJECT_0:
            _raise_last_error("WaitForSingleObject(process)")
        exit_code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(self._process, ctypes.byref(exit_code)):
            _raise_last_error("GetExitCodeProcess")
        return int(exit_code.value)

    def creation_time_utc(self) -> str:
        kernel32 = _kernel32()
        created = _FileTime()
        exited = _FileTime()
        kernel = _FileTime()
        user = _FileTime()
        if not kernel32.GetProcessTimes(
            self._process,
            ctypes.byref(created),
            ctypes.byref(exited),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            _raise_last_error("GetProcessTimes")
        ticks = (int(created.dwHighDateTime) << 32) | int(created.dwLowDateTime)
        seconds, remainder = divmod(ticks, 10_000_000)
        milliseconds = remainder // 10_000
        from datetime import datetime, timedelta, timezone

        created_at = datetime.fromtimestamp(
            seconds - _FILETIME_EPOCH_OFFSET_SECONDS, tz=timezone.utc
        ) + timedelta(milliseconds=milliseconds)
        return created_at.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def close(self) -> None:
        kernel32 = _kernel32()
        try:
            if self._process and not self._resumed:
                # A failure between suspended creation and ResumeThread must
                # not leave an operation-bound process waiting in the job.
                if not kernel32.TerminateProcess(self._process, 0xEE):
                    exit_code = wintypes.DWORD()
                    if not kernel32.GetExitCodeProcess(self._process, ctypes.byref(exit_code)):
                        _raise_last_error("TerminateProcess(unresumed final-sync)")
                    if exit_code.value == _STILL_ACTIVE:
                        _raise_last_error("TerminateProcess(unresumed final-sync)")
                wait_result = kernel32.WaitForSingleObject(self._process, 5_000)
                if wait_result == _WAIT_TIMEOUT:
                    raise JobObjectError("unresumed final-sync process did not exit")
                if wait_result != _WAIT_OBJECT_0:
                    _raise_last_error("WaitForSingleObject(unresumed final-sync)")
        finally:
            for name in ("_thread", "_process"):
                handle = getattr(self, name)
                if handle:
                    kernel32.CloseHandle(handle)
                    setattr(self, name, 0)


class OperationJob:
    """A named, operation-bound Job Object with kill-on-last-handle-close."""

    def __init__(self, handle: int, name: str, *, created: bool):
        self.handle = handle
        self.name = name
        self.created = created
        self._kernel32 = _kernel32()

    @classmethod
    def open_or_create(
        cls, operation_id: str, *, copy_controller_sid: str
    ) -> OperationJob:
        name = operation_job_name(operation_id)
        kernel32 = _kernel32()
        sid = _validate_sid(copy_controller_sid)
        advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
        advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(wintypes.DWORD),
        ]
        advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW.restype = wintypes.BOOL
        kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        kernel32.LocalFree.restype = ctypes.c_void_p
        descriptor = ctypes.c_void_p()
        sddl = f"D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;0x000D;;;{sid})"
        if not advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl, 1, ctypes.byref(descriptor), None
        ):
            _raise_last_error("ConvertStringSecurityDescriptorToSecurityDescriptorW")
        security = _SecurityAttributes(
            nLength=ctypes.sizeof(_SecurityAttributes),
            lpSecurityDescriptor=descriptor,
            bInheritHandle=False,
        )
        ctypes.set_last_error(0)
        try:
            handle = kernel32.CreateJobObjectW(ctypes.byref(security), name)
            create_error = ctypes.get_last_error()
        finally:
            kernel32.LocalFree(descriptor)
        if not handle:
            raise JobObjectError(f"CreateJobObjectW failed with Win32 error {create_error}")
        already_exists = create_error == _ERROR_ALREADY_EXISTS
        job = cls(int(handle), name, created=not already_exists)
        try:
            if already_exists:
                info = job._query_extended()
                if not (info.BasicLimitInformation.LimitFlags & _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE):
                    raise JobObjectError("existing operation Job Object lacks kill-on-close")
            else:
                info = _ExtendedLimitInformation()
                info.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                if not kernel32.SetInformationJobObject(
                    handle,
                    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                    ctypes.byref(info),
                    ctypes.sizeof(info),
                ):
                    _raise_last_error("SetInformationJobObject")
            return job
        except Exception:
            job.close()
            raise

    @classmethod
    def open_existing(cls, operation_id: str) -> OperationJob:
        name = operation_job_name(operation_id)
        kernel32 = _kernel32()
        handle = kernel32.OpenJobObjectW(_JOB_OBJECT_ALL_COPY_RIGHTS, False, name)
        if not handle:
            _raise_last_error("OpenJobObjectW")
        job = cls(int(handle), name, created=False)
        try:
            info = job._query_extended()
            if not (
                info.BasicLimitInformation.LimitFlags
                & _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            ):
                raise JobObjectError("operation Job Object lacks kill-on-close")
            return job
        except Exception:
            job.close()
            raise

    def _query_extended(self) -> _ExtendedLimitInformation:
        info = _ExtendedLimitInformation()
        returned = wintypes.DWORD()
        if not self._kernel32.QueryInformationJobObject(
            self.handle,
            _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(info),
            ctypes.sizeof(info),
            ctypes.byref(returned),
        ):
            _raise_last_error("QueryInformationJobObject(extended)")
        return info

    def active_process_count(self) -> int:
        info = _BasicAccountingInformation()
        returned = wintypes.DWORD()
        if not self._kernel32.QueryInformationJobObject(
            self.handle,
            _JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION,
            ctypes.byref(info),
            ctypes.sizeof(info),
            ctypes.byref(returned),
        ):
            _raise_last_error("QueryInformationJobObject(accounting)")
        return int(info.ActiveProcesses)

    def create_suspended(
        self,
        command: Sequence[str],
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> SuspendedJobProcess:
        if not command or any(not isinstance(item, str) for item in command):
            raise ValueError("final-sync command must be a nonempty string sequence")
        if self.active_process_count() != 0:
            raise JobObjectError("an operation final-sync process is already active")
        startup = _StartupInfo()
        startup.cb = ctypes.sizeof(startup)
        startup.dwFlags = _STARTF_USESHOWWINDOW
        startup.wShowWindow = _SW_HIDE
        process_info = _ProcessInformation()
        command_line = ctypes.create_unicode_buffer(subprocess.list2cmdline(list(command)))
        environment_buffer = None
        environment_pointer = None
        flags = _CREATE_SUSPENDED | _CREATE_NO_WINDOW
        if env is not None:
            environment_values = "\0".join(
                f"{key}={value}" for key, value in sorted(env.items(), key=lambda pair: pair[0].casefold())
            ) + "\0\0"
            environment_buffer = ctypes.create_unicode_buffer(environment_values)
            environment_pointer = ctypes.cast(environment_buffer, ctypes.c_void_p)
            flags |= _CREATE_UNICODE_ENVIRONMENT
        if not self._kernel32.CreateProcessW(
            None,
            command_line,
            None,
            None,
            False,
            flags,
            environment_pointer,
            cwd,
            ctypes.byref(startup),
            ctypes.byref(process_info),
        ):
            _raise_last_error("CreateProcessW(suspended)")
        child = SuspendedJobProcess(
            job=self,
            pid=int(process_info.dwProcessId),
            _process=int(process_info.hProcess),
            _thread=int(process_info.hThread),
        )
        if not self._kernel32.AssignProcessToJobObject(self.handle, child._process):
            try:
                self._kernel32.TerminateProcess(child._process, 0xEE)
                child.wait(5.0)
            finally:
                child.close()
            _raise_last_error("AssignProcessToJobObject")
        return child

    def terminate_and_wait(self, timeout_seconds: float) -> bool:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be nonnegative")
        if self.active_process_count() == 0:
            return True
        if not self._kernel32.TerminateJobObject(self.handle, 0xEF):
            _raise_last_error("TerminateJobObject")
        deadline = time.monotonic() + timeout_seconds
        while True:
            if self.active_process_count() == 0:
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))

    def close(self) -> None:
        if self.handle:
            self._kernel32.CloseHandle(self.handle)
            self.handle = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
