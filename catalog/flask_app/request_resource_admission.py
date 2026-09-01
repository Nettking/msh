"""FCP-owned request-body spooling for the supported upload endpoint."""

from __future__ import annotations

import logging
import os
import stat
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Self

from flask import Request, current_app
from werkzeug.exceptions import RequestEntityTooLarge

if TYPE_CHECKING:  # pragma: no cover - typing only
    from flask import Flask

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

_LOGGER = logging.getLogger(__name__)


class UnsupportedFileIngress(RequestEntityTooLarge):
    """Refusal for a multipart file part outside the supported upload endpoint.

    Werkzeug materializes every multipart part that carries a filename through
    :meth:`Request._get_file_stream`. Its default factory is a
    ``SpooledTemporaryFile`` that rolls over to the operating-system temporary
    directory after 500 KiB, which is host storage this process neither owns,
    measures, nor reserves. Only ``/data-upload`` reads ``request.files``, so
    every other file part is unconsumed data. Refusing it at the part header --
    before the parser obtains a write target -- keeps the whole application on
    one admitted spool rather than two storage regimes.
    """

    description = (
        "This endpoint does not accept file uploads. The file part was refused "
        "before any request byte reached host storage."
    )


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
        _record_wsgi_input_materialization(self.environ)
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
            # Fail closed. Delegating here would reach Werkzeug's
            # ``default_stream_factory`` and spool an arbitrary number of bytes
            # into the operating-system temporary directory with no reservation
            # against measured free space or inodes. The parser calls this at
            # the part header, so refusing costs nothing that already reached
            # storage.
            raise UnsupportedFileIngress()
        return session.open_file(filename=filename, content_length=content_length)

    def close(self) -> None:
        try:
            super().close()
        finally:
            session = self._fcp_spool_session
            self._fcp_spool_session = None
            if session is not None:
                session.close()


_WSGI_INPUT_MATERIALIZATION: str | None = None


def _classify_wsgi_input(environ: dict) -> str:
    """Classify whether the WSGI layer already put this body on host storage.

    ``"host-file"`` means an upstream server or proxy buffered the request body
    into a regular file before the application was invoked. ``"stream"`` means
    the application received a socket or pipe, so no request byte has reached
    storage yet. ``"unknown"`` covers in-memory and synthetic streams that
    expose no file descriptor.
    """

    stream = environ.get("wsgi.input")
    for candidate in (stream, getattr(stream, "_stream", None)):
        if candidate is None:
            continue
        try:
            mode = os.fstat(candidate.fileno()).st_mode
        except (AttributeError, OSError, ValueError, TypeError):
            continue
        return "host-file" if stat.S_ISREG(mode) else "stream"
    return "unknown"


def _record_wsgi_input_materialization(environ: dict) -> str:
    """Latch the observed ingress shape once per process and warn on storage.

    The application cannot un-write bytes an upstream layer already spooled, so
    this does not refuse the request. It makes the deployment prerequisite
    observable instead of assumed: a supported deployment must report
    ``"stream"``.
    """

    global _WSGI_INPUT_MATERIALIZATION
    if _WSGI_INPUT_MATERIALIZATION is not None:
        return _WSGI_INPUT_MATERIALIZATION
    observed = _classify_wsgi_input(environ)
    _WSGI_INPUT_MATERIALIZATION = observed
    if observed == "host-file":
        _LOGGER.warning(
            "WSGI request body arrived as a regular file: an upstream server or "
            "proxy buffered it to host storage before FCP admission. The "
            "supported deployment streams the body from the socket; see "
            "docs/server_setup.md."
        )
    return observed


def observed_wsgi_input_materialization() -> str | None:
    """Return the latched ingress shape, or ``None`` before the first upload."""

    return _WSGI_INPUT_MATERIALIZATION


def reset_observed_wsgi_input_materialization() -> None:
    """Clear the latch. Used by tests that exercise several ingress shapes."""

    global _WSGI_INPUT_MATERIALIZATION
    _WSGI_INPUT_MATERIALIZATION = None


def _positive_config_int(app: Flask, name: str) -> int:
    value = app.config.get(name)
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"{name} must be a positive integer for the bounded upload ingress "
            f"contract; got {value!r}."
        ) from exc
    if parsed <= 0:
        raise RuntimeError(
            f"{name} must be a positive integer for the bounded upload ingress "
            f"contract; got {parsed!r}."
        )
    return parsed


def validate_request_ingress_contract(app: Flask) -> dict[str, int | str]:
    """Fail closed unless the application-level ingress bound is actually installed.

    Two prerequisites are enforceable from inside the process and are therefore
    startup errors rather than documentation:

    * ``request_class`` must route every multipart file part through
      :class:`FCPRequest`. Without it Werkzeug spools parts to the
      operating-system temporary directory with no reservation.
    * ``MAX_CONTENT_LENGTH`` must be a positive bound. Werkzeug can only cap a
      terminated unknown-length stream when this is set, so leaving it unset
      admits an unbounded body.

    The upload budget is validated with them because a non-positive budget
    silently refuses every upload instead of bounding one.
    """

    request_class = getattr(app, "request_class", None)
    installed = isinstance(request_class, type) and issubclass(request_class, FCPRequest)
    if not installed:
        raise RuntimeError(
            "app.request_class must be FCPRequest (or a subclass) so multipart "
            "file parts are spooled through the admitted FCP-owned root; got "
            f"{request_class!r}."
        )
    contract: dict[str, int | str] = {
        "request_class": request_class.__name__,
        "max_content_length": _positive_config_int(app, "MAX_CONTENT_LENGTH"),
        "max_total_bytes": _positive_config_int(app, "DATA_UPLOAD_MAX_TOTAL_BYTES"),
        "max_files": _positive_config_int(app, "DATA_UPLOAD_MAX_FILES"),
        "spool_directory": str(app.config["DATA_UPLOAD_REQUEST_SPOOL_DIRECTORY"]),
    }
    return contract


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
    "UnsupportedFileIngress",
    "observed_wsgi_input_materialization",
    "reset_observed_wsgi_input_materialization",
    "scavenge_request_spool",
    "validate_request_ingress_contract",
]
