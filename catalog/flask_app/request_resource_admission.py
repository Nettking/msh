"""FCP-owned request-body spooling for the supported upload endpoint."""

from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path
from typing import BinaryIO, Self

from flask import Request, current_app
from werkzeug.exceptions import RequestEntityTooLarge

from catalog.common.managed_temporary import (
    ManagedTemporaryFile,
    ManagedTemporaryRoot,
    scavenge_managed_temporary_root,
)
from catalog.federation.host_resources import (
    HostResourceRefused,
    ProcessResourceAdmission,
)
from catalog.federation.process_resource_admission import PROCESS_RESOURCE_ADMISSION

MULTIPART_OVERHEAD_BYTES = 256 * 1024
REQUEST_SPOOL_FIXED_BYTES = 64 * 1024
REQUEST_SPOOL_FIXED_INODES = 8
REQUEST_SPOOL_NAMESPACE = "data-upload-request-spool"


def _configured_int(name: str, default: int) -> int:
    try:
        value = int(current_app.config.get(name, default))
    except (TypeError, ValueError):
        value = default
    return max(value, 0)


def _admission() -> ProcessResourceAdmission:
    try:
        injected = current_app.extensions.get("process_resource_admission")
    except RuntimeError:
        injected = None
    return injected if isinstance(injected, ProcessResourceAdmission) else PROCESS_RESOURCE_ADMISSION


class _RequestSpoolSession:
    """One request's admitted spool and its lifetime reservation."""

    def __init__(self, root: Path, *, maximum_bytes: int, maximum_files: int) -> None:
        self.root = ManagedTemporaryRoot(root, namespace=REQUEST_SPOOL_NAMESPACE)
        self.maximum_bytes = maximum_bytes
        self.maximum_files = maximum_files
        self.bytes_written = 0
        self.files_opened = 0
        self._files: list[ManagedTemporaryFile] = []
        self._reservation: AbstractContextManager | None = None
        self._closed = False

    def open(self) -> None:
        controller = _admission()
        self._reservation = controller.reserve(
            self.root.root,
            bytes_required=(
                self.maximum_bytes
                + MULTIPART_OVERHEAD_BYTES
                + REQUEST_SPOOL_FIXED_BYTES
            ),
            inodes_required=(2 * self.maximum_files) + REQUEST_SPOOL_FIXED_INODES,
        )
        try:
            self._reservation.__enter__()
            self.root.ensure()
        except BaseException:
            self.close()
            raise

    def open_file(self, *, filename: str | None, content_length: int | None) -> BinaryIO:
        if self.files_opened >= self.maximum_files:
            raise RequestEntityTooLarge()
        if content_length is not None and content_length > self._file_limit:
            raise RequestEntityTooLarge()
        temporary = self.root.allocate(prefix="fcp-upload-body-", suffix=".part")
        self.files_opened += 1
        self._files.append(temporary)
        return _BudgetedRequestFile(self, temporary, filename=filename)

    @property
    def _file_limit(self) -> int:
        return _configured_int("DATA_UPLOAD_MAX_FILE_BYTES", self.maximum_bytes)

    def account_write(self, size: int, *, file_size: int) -> None:
        if size < 0 or file_size > self._file_limit or self.bytes_written + size > self.maximum_bytes:
            raise RequestEntityTooLarge()
        self.bytes_written += size

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for temporary in reversed(self._files):
            temporary.close()
        if self._reservation is not None:
            self._reservation.__exit__(None, None, None)
            self._reservation = None


class _BudgetedRequestFile:
    """File-like adapter enforcing per-file and request-wide byte limits."""

    def __init__(
        self,
        session: _RequestSpoolSession,
        temporary: ManagedTemporaryFile,
        *,
        filename: str | None,
    ) -> None:
        self._session = session
        self._temporary = temporary
        self.filename = filename
        self._file_bytes = 0

    def __getattr__(self, name: str):
        return getattr(self._temporary, name)

    def write(self, data: bytes) -> int:
        size = len(data)
        self._session.account_write(
            size,
            file_size=self._file_bytes + size,
        )
        written = self._temporary.write(data)
        if written != size:
            self._session.bytes_written -= size - written
        self._file_bytes += written
        return written

    def close(self) -> None:
        self._temporary.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class FCPRequest(Request):
    """Request class that bounds and owns multipart upload spooling."""

    def __init__(self, *args, **kwargs) -> None:
        self._fcp_spool_session: _RequestSpoolSession | None = None
        super().__init__(*args, **kwargs)

    def _is_supported_upload_multipart(self) -> bool:
        return (
            self.path.rstrip("/") == "/data-upload"
            and self.mimetype == "multipart/form-data"
        )

    @property
    def max_content_length(self) -> int | None:
        configured = super().max_content_length
        if not self._is_supported_upload_multipart():
            return configured
        service_limit = _configured_int(
            "DATA_UPLOAD_MAX_TOTAL_BYTES",
            1024 * 1024 * 1024,
        ) + MULTIPART_OVERHEAD_BYTES
        if configured is None:
            return service_limit
        return min(int(configured), service_limit)

    def _load_form_data(self) -> None:
        if not self._is_supported_upload_multipart():
            super()._load_form_data()
            return
        # Werkzeug deliberately returns an empty safe-fallback stream when an
        # unknown-length WSGI input has not been marked as terminated. Do not
        # create even the managed root in that case: the application cannot
        # prove that it owns or can bound the upstream stream.
        if self.content_length is None and "wsgi.input_terminated" not in self.environ:
            super()._load_form_data()
            return
        if self._fcp_spool_session is not None:
            super()._load_form_data()
            return
        root = Path(
            current_app.config.get(
                "DATA_UPLOAD_REQUEST_SPOOL_DIRECTORY",
                "data/imports/request-spool",
            )
        )
        session = _RequestSpoolSession(
            root,
            maximum_bytes=_configured_int(
                "DATA_UPLOAD_MAX_TOTAL_BYTES",
                1024 * 1024 * 1024,
            ),
            maximum_files=_configured_int("DATA_UPLOAD_MAX_FILES", 50),
        )
        self._fcp_spool_session = session
        try:
            session.open()
            super()._load_form_data()
        except BaseException:
            session.close()
            self._fcp_spool_session = None
            raise

    def _get_file_stream(
        self,
        total_content_length: int | None,
        content_type: str | None,
        filename: str | None = None,
        content_length: int | None = None,
    ) -> BinaryIO:
        session = self._fcp_spool_session
        if session is None or not self._is_supported_upload_multipart():
            return super()._get_file_stream(
                total_content_length,
                content_type,
                filename,
                content_length,
            )
        return session.open_file(filename=filename, content_length=content_length)

    def close(self) -> None:
        try:
            super().close()
        finally:
            session = self._fcp_spool_session
            self._fcp_spool_session = None
            if session is not None:
                session.close()


def scavenge_request_spool(root: Path | str) -> None:
    """Best-effort startup cleanup under a small admitted root envelope."""

    controller = _admission()
    try:
        with controller.reserve(
            Path(root),
            bytes_required=REQUEST_SPOOL_FIXED_BYTES,
            inodes_required=REQUEST_SPOOL_FIXED_INODES,
        ):
            scavenge_managed_temporary_root(
                root,
                namespace=REQUEST_SPOOL_NAMESPACE,
            )
    except (HostResourceRefused, OSError, RuntimeError, ValueError):
        # Startup must remain available under pressure.  The next request or
        # operator restart can retry; no ambiguous file is deleted here.
        return


__all__ = [
    "MULTIPART_OVERHEAD_BYTES",
    "REQUEST_SPOOL_FIXED_BYTES",
    "REQUEST_SPOOL_FIXED_INODES",
    "REQUEST_SPOOL_NAMESPACE",
    "FCPRequest",
    "scavenge_request_spool",
]
