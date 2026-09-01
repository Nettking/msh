from __future__ import annotations

import io
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from flask import Flask
from werkzeug.datastructures import FileStorage
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.test import EnvironBuilder

from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
    ProcessResourceAdmission,
)
from catalog.federation.process_resource_admission import (
    SerializedProcessResourceAdmission,
)
from catalog.flask_app import data_upload_routes
from catalog.flask_app.request_resource_admission import (
    FCPRequest,
    UnsupportedFileIngress,
    observed_wsgi_input_materialization,
    reset_observed_wsgi_input_materialization,
    validate_request_ingress_contract,
)
from catalog.flask_app.services.data_upload_resource_admission import (
    enqueue_with_resource_admission,
)
from catalog.flask_app.services.data_upload_service import (
    DataUploadError,
    DataUploadService,
)


class _FakeUploadService:
    def __init__(
        self,
        staging_root: Path,
        *,
        max_total_bytes: int = 150,
        max_files: int = 2,
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self.staging_root = staging_root
        self.max_total_bytes = max_total_bytes
        self.max_files = max_files
        self.entered = entered
        self.release = release
        self.calls = 0

    def enqueue(self, files: Any) -> dict[str, Any]:
        self.calls += 1
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            assert self.release.wait(timeout=5)
        return {"batch_id": "upload-test", "files": tuple(files)}


def _measurement(resource_id: str, *, free_bytes: int) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id=resource_id,
        observed_at=datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc),
        total_bytes=10_000,
        free_bytes=free_bytes,
        total_inodes=None,
        free_inodes=None,
        available=True,
    )


def _admission(measurer: Any) -> ProcessResourceAdmission:
    return ProcessResourceAdmission(
        thresholds=PressureThresholds(
            critical_free_bytes=100,
            pressure_free_bytes=200,
            warning_free_bytes=300,
            critical_free_inodes=0,
            pressure_free_inodes=0,
            warning_free_inodes=0,
            max_measurement_age_seconds=60,
        ),
        measurer=measurer,
        clock=lambda: datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc),
    )


def test_resource_pressure_refuses_before_upload_writer_runs(tmp_path: Path) -> None:
    service = _FakeUploadService(tmp_path)
    admission = _admission(lambda _path: _measurement("data", free_bytes=200))

    with pytest.raises(DataUploadError) as raised:
        enqueue_with_resource_admission(service, (), admission=admission)

    assert raised.value.code == "upload-resource-pressure"
    assert service.calls == 0


def test_concurrent_uploads_cannot_double_spend_one_resource(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    first = _FakeUploadService(tmp_path, entered=entered, release=release)
    second = _FakeUploadService(tmp_path)
    admission = _admission(lambda _path: _measurement("data", free_bytes=350))
    outcome: list[BaseException] = []

    def run_first() -> None:
        try:
            enqueue_with_resource_admission(first, (), admission=admission)
        except BaseException as exc:  # noqa: BLE001 - captured for test assertion
            outcome.append(exc)

    worker = threading.Thread(target=run_first)
    worker.start()
    assert entered.wait(timeout=5)
    try:
        with pytest.raises(DataUploadError) as raised:
            enqueue_with_resource_admission(second, (), admission=admission)
        assert raised.value.code == "upload-resource-pressure"
        assert second.calls == 0
    finally:
        release.set()
        worker.join(timeout=5)

    assert not worker.is_alive()
    assert outcome == []
    assert first.calls == 1


def test_reservation_is_released_after_upload_call(tmp_path: Path) -> None:
    admission = _admission(lambda _path: _measurement("data", free_bytes=350))
    first = _FakeUploadService(tmp_path)
    second = _FakeUploadService(tmp_path)

    enqueue_with_resource_admission(first, (), admission=admission)
    enqueue_with_resource_admission(second, (), admission=admission)

    assert first.calls == 1
    assert second.calls == 1


def test_reservation_is_released_when_upload_call_fails(tmp_path: Path) -> None:
    class _FailingUploadService(_FakeUploadService):
        def enqueue(self, files: Any) -> dict[str, Any]:
            self.calls += 1
            raise DataUploadError("upload-invalid-json", "bad upload")

    admission = _admission(lambda _path: _measurement("data", free_bytes=350))
    failing = _FailingUploadService(tmp_path)
    succeeding = _FakeUploadService(tmp_path)

    with pytest.raises(DataUploadError) as raised:
        enqueue_with_resource_admission(failing, (), admission=admission)
    assert raised.value.code == "upload-invalid-json"

    enqueue_with_resource_admission(succeeding, (), admission=admission)
    assert succeeding.calls == 1


def test_distinct_backing_resources_have_independent_envelopes(tmp_path: Path) -> None:
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    first = _FakeUploadService(root_a)
    second = _FakeUploadService(root_b)

    def measure(path: Path | str) -> FilesystemMeasurement:
        resource_id = "a" if Path(path) == root_a else "b"
        return _measurement(resource_id, free_bytes=350)

    admission = _admission(measure)
    with admission.reserve(root_a, bytes_required=150):
        enqueue_with_resource_admission(second, (), admission=admission)

    assert first.calls == 0
    assert second.calls == 1


def test_service_admission_refuses_before_staging_request_bytes(
    tmp_path: Path,
) -> None:
    mode = {"free_bytes": 10_000_000}

    def measure(_path: Path | str) -> FilesystemMeasurement:
        return _measurement("upload-resource", free_bytes=mode["free_bytes"])

    admission = SerializedProcessResourceAdmission(
        thresholds=PressureThresholds(
            critical_free_bytes=100,
            pressure_free_bytes=200,
            warning_free_bytes=300,
            critical_free_inodes=0,
            pressure_free_inodes=0,
            warning_free_inodes=0,
            max_measurement_age_seconds=60,
        ),
        measurer=measure,
        clock=lambda: datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc),
    )
    service = DataUploadService(
        database=tmp_path / "imports" / "uploads.sqlite3",
        staging_root=tmp_path / "imports" / "staging",
        published_root=tmp_path / "data" / "uploads",
        runtime_manager=_FakeUploadService(tmp_path),
        max_files=2,
        max_file_bytes=1024,
        max_total_bytes=2048,
        resource_admission=admission,
    )
    payload = FileStorage(stream=io.BytesIO(b'{"ok":true}\n'), filename="data.jsonl")
    mode["free_bytes"] = 200

    with pytest.raises(DataUploadError) as raised:
        service.enqueue((payload,))

    assert raised.value.code == "upload-resource-pressure"
    assert tuple(service.staging_root.iterdir()) == ()


def test_async_import_pressure_keeps_batch_queued_and_hidden(tmp_path: Path, monkeypatch) -> None:
    mode = {"free_bytes": 10_000_000}

    def measure(_path: Path | str) -> FilesystemMeasurement:
        return _measurement("upload-resource", free_bytes=mode["free_bytes"])

    admission = SerializedProcessResourceAdmission(
        thresholds=PressureThresholds(
            critical_free_bytes=100,
            pressure_free_bytes=200,
            warning_free_bytes=300,
            critical_free_inodes=0,
            pressure_free_inodes=0,
            warning_free_inodes=0,
            max_measurement_age_seconds=60,
        ),
        measurer=measure,
        clock=lambda: datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc),
    )
    service = DataUploadService(
        database=tmp_path / "imports" / "uploads.sqlite3",
        staging_root=tmp_path / "imports" / "staging",
        published_root=tmp_path / "data" / "uploads",
        runtime_manager=_FakeUploadService(tmp_path),
        max_files=2,
        max_file_bytes=1024,
        max_total_bytes=2048,
        resource_admission=admission,
    )
    monkeypatch.setattr(service, "_start_import", lambda *args, **kwargs: True)
    payload = FileStorage(stream=io.BytesIO(b'{"ok":true}\n'), filename="data.jsonl")
    batch = service.enqueue((payload,))
    batch_id = str(batch["batch_id"])
    mode["free_bytes"] = 200

    service._import_batch_serialized(batch_id)

    assert service.batch(batch_id)["status"] == "queued"
    assert not (service.published_root / batch_id).exists()
    assert (service.staging_root / batch_id).is_dir()


def test_declared_multipart_limit_is_checked_before_request_files_access() -> None:
    app = Flask(__name__)
    app.config["TESTING"] = True

    with app.test_request_context(
        "/data-upload/",
        method="POST",
    ):
        from flask import request

        request.environ["CONTENT_LENGTH"] = "400000"
        with pytest.raises(DataUploadError) as raised:
            data_upload_routes._validate_declared_upload_size(
                type("Service", (), {"max_total_bytes": 100})()
            )

    assert raised.value.code == "upload-request-too-large"


def _request_app(
    tmp_path: Path,
    admission: ProcessResourceAdmission,
    *,
    max_total_bytes: int = 4096,
    max_file_bytes: int = 2048,
) -> Flask:
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        MAX_CONTENT_LENGTH=1024 * 1024,
        DATA_UPLOAD_MAX_TOTAL_BYTES=max_total_bytes,
        DATA_UPLOAD_MAX_FILE_BYTES=max_file_bytes,
        DATA_UPLOAD_MAX_FILES=2,
        DATA_UPLOAD_REQUEST_SPOOL_DIRECTORY=str(tmp_path / "request-spool"),
    )
    app.request_class = FCPRequest
    app.extensions["process_resource_admission"] = admission
    return app


def _multipart_environ(body: bytes, *, content_length: int | None, terminated: bool) -> dict:
    builder = EnvironBuilder(
        path="/data-upload/",
        method="POST",
        input_stream=io.BytesIO(body),
        content_type="multipart/form-data; boundary=boundary",
        content_length=content_length,
    )
    environ = builder.get_environ()
    if content_length is None:
        environ.pop("CONTENT_LENGTH", None)
    if terminated:
        environ["wsgi.input_terminated"] = True
    return environ


def _multipart_file_body(payload: bytes) -> bytes:
    return (
        b"--boundary\r\n"
        b'Content-Disposition: form-data; name="files"; filename="data.jsonl"\r\n'
        b"Content-Type: application/jsonl\r\n\r\n"
        + payload
        + b"\r\n--boundary--\r\n"
    )


def test_supported_upload_uses_admitted_fcp_owned_request_spool(tmp_path: Path) -> None:
    admission = _admission(lambda _path: _measurement("request", free_bytes=10_000_000))
    app = _request_app(tmp_path, admission)
    body = _multipart_file_body(b'{"ok":true}\n')

    with app.request_context(
        _multipart_environ(body, content_length=len(body), terminated=False)
    ):
        from flask import request

        files = request.files.getlist("files")
        assert len(files) == 1
        stream_path = Path(files[0].stream.path)
        assert stream_path.parent == tmp_path / "request-spool"
        assert stream_path.name.startswith("fcp-upload-body-")
        assert stream_path.exists()

    assert not any((tmp_path / "request-spool").glob("fcp-upload-body-*"))
    assert not any((tmp_path / "request-spool").glob(".*.fcp-owner.json"))


def test_streaming_equivalent_body_is_bounded_before_uncontrolled_spool(
    tmp_path: Path,
) -> None:
    admission = _admission(lambda _path: _measurement("request", free_bytes=10_000_000))
    app = _request_app(
        tmp_path,
        admission,
        max_total_bytes=32,
        max_file_bytes=400_000,
    )
    # The request has no Content-Length and relies on the WSGI terminated-stream
    # signal. The body exceeds the FCP total-body ceiling (including framing),
    # so Werkzeug's LimitedStream must stop it before parser materialization can
    # continue indefinitely.
    body = _multipart_file_body(b"x" * 300_000)

    with app.request_context(
        _multipart_environ(body, content_length=None, terminated=True)
    ):
        from flask import request

        with pytest.raises(RequestEntityTooLarge):
            request.files.getlist("files")

    spool_root = tmp_path / "request-spool"
    assert not any(spool_root.glob("fcp-upload-body-*"))
    assert not any(spool_root.glob(".*.fcp-owner.json"))


def test_unknown_length_without_wsgi_termination_fails_closed_without_spooling(
    tmp_path: Path,
) -> None:
    admission = _admission(lambda _path: _measurement("request", free_bytes=10_000_000))
    app = _request_app(tmp_path, admission)
    body = _multipart_file_body(b'{"ok":true}\n')

    with app.request_context(
        _multipart_environ(body, content_length=None, terminated=False)
    ):
        from flask import request

        assert request.files.getlist("files") == []

    assert not (tmp_path / "request-spool").exists()


def test_request_spool_pressure_refuses_before_root_or_body_creation(
    tmp_path: Path,
) -> None:
    admission = _admission(lambda _path: _measurement("request", free_bytes=200))
    app = _request_app(tmp_path, admission)
    body = _multipart_file_body(b'{"ok":true}\n')

    with app.request_context(
        _multipart_environ(body, content_length=len(body), terminated=False)
    ):
        from flask import request

        with pytest.raises(HostResourceRefused):
            request.files.getlist("files")

    assert not (tmp_path / "request-spool").exists()


def _unsupported_multipart_environ(body: bytes, *, path: str = "/federation/nodes") -> dict:
    return EnvironBuilder(
        path=path,
        method="POST",
        input_stream=io.BytesIO(body),
        content_type="multipart/form-data; boundary=boundary",
        content_length=len(body),
    ).get_environ()


def _forbid_werkzeug_default_spool(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make Werkzeug's unadmitted temporary-file factory a hard failure.

    ``default_stream_factory`` returns a ``SpooledTemporaryFile`` that rolls
    over to the operating-system temporary directory after 500 KiB. Reaching it
    at all means bytes are about to land on host storage this process does not
    measure, so the tests below treat the call itself as the defect.
    """

    def _refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError(
            "Werkzeug's unadmitted default_stream_factory was reached; "
            "a multipart part would have spooled outside FCP admission."
        )

    monkeypatch.setattr(
        "werkzeug.wrappers.request.default_stream_factory",
        _refuse,
    )


def test_file_part_outside_supported_upload_never_reaches_host_storage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admission = _admission(lambda _path: _measurement("request", free_bytes=10_000_000))
    app = _request_app(tmp_path, admission)
    # The production default from create_app(). It caps one request body but
    # says nothing about where those bytes go, or about concurrent requests.
    app.config["MAX_CONTENT_LENGTH"] = 1100 * 1024 * 1024
    _forbid_werkzeug_default_spool(monkeypatch)
    # Far above the 500 KiB rollover, so the unfixed path materialized a real
    # file. Only /data-upload reads request.files, so no route consumes this.
    body = _multipart_file_body(b"x" * 2_000_000)

    with app.request_context(_unsupported_multipart_environ(body)):
        from flask import request

        with pytest.raises(UnsupportedFileIngress):
            request.files.getlist("files")

    assert not (tmp_path / "request-spool").exists()


def test_unsupported_file_ingress_is_refused_before_any_body_byte(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admission = _admission(lambda _path: _measurement("request", free_bytes=10_000_000))
    app = _request_app(tmp_path, admission)
    app.config["MAX_CONTENT_LENGTH"] = 1100 * 1024 * 1024
    _forbid_werkzeug_default_spool(monkeypatch)
    # The parser asks for a write target at the part header. Record how much of
    # the body it had consumed when the refusal happened.
    payload = b"y" * 1_000_000
    body = _multipart_file_body(payload)
    stream = io.BytesIO(body)
    environ = EnvironBuilder(
        path="/federation/nodes",
        method="POST",
        input_stream=stream,
        content_type="multipart/form-data; boundary=boundary",
        content_length=len(body),
    ).get_environ()

    with app.request_context(environ):
        from flask import request

        with pytest.raises(UnsupportedFileIngress) as raised:
            request.files.getlist("files")

    assert raised.value.code == 413
    # Refusal happens at the part header, so the payload was never written
    # anywhere: no destination stream was ever created for it.
    assert not (tmp_path / "request-spool").exists()


def test_multipart_form_fields_without_files_still_parse_outside_upload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refusal is scoped to file parts, not to multipart requests.

    Werkzeug only routes a part through the file-stream factory when it
    declares a filename. Plain fields stay in the bounded in-memory form
    budget, so CSRF-token and operator forms are unaffected.
    """

    admission = _admission(lambda _path: _measurement("request", free_bytes=10_000_000))
    app = _request_app(tmp_path, admission)
    _forbid_werkzeug_default_spool(monkeypatch)
    body = (
        b"--boundary\r\n"
        b'Content-Disposition: form-data; name="_csrf_token"\r\n\r\n'
        b"token-value"
        b"\r\n--boundary--\r\n"
    )

    with app.request_context(_unsupported_multipart_environ(body)):
        from flask import request

        assert request.form.get("_csrf_token") == "token-value"
        assert request.files.getlist("files") == []

    assert not (tmp_path / "request-spool").exists()


def test_ingress_contract_refuses_a_request_class_without_the_spool_funnel(
    tmp_path: Path,
) -> None:
    admission = _admission(lambda _path: _measurement("request", free_bytes=10_000_000))
    app = _request_app(tmp_path, admission)
    app.request_class = Flask.request_class

    with pytest.raises(RuntimeError) as raised:
        validate_request_ingress_contract(app)

    assert "FCPRequest" in str(raised.value)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("MAX_CONTENT_LENGTH", None),
        ("MAX_CONTENT_LENGTH", 0),
        ("DATA_UPLOAD_MAX_TOTAL_BYTES", 0),
        ("DATA_UPLOAD_MAX_FILES", -1),
    ],
)
def test_ingress_contract_refuses_an_unbounded_or_empty_budget(
    tmp_path: Path,
    name: str,
    value: object,
) -> None:
    admission = _admission(lambda _path: _measurement("request", free_bytes=10_000_000))
    app = _request_app(tmp_path, admission)
    app.config[name] = value

    with pytest.raises(RuntimeError) as raised:
        validate_request_ingress_contract(app)

    assert name in str(raised.value)


def test_ingress_contract_reports_the_installed_bounds(tmp_path: Path) -> None:
    admission = _admission(lambda _path: _measurement("request", free_bytes=10_000_000))
    app = _request_app(tmp_path, admission)

    contract = validate_request_ingress_contract(app)

    assert contract["request_class"] == "FCPRequest"
    assert contract["max_content_length"] == 1024 * 1024
    assert contract["max_total_bytes"] == 4096
    assert contract["max_files"] == 2
    assert contract["spool_directory"] == str(tmp_path / "request-spool")


def test_pre_spooled_wsgi_input_is_detected_and_reported(tmp_path: Path) -> None:
    """A server that buffered the body to disk must be observable, not assumed.

    The application cannot un-write those bytes, so this does not refuse the
    request. It records that the deployment prerequisite in docs/server_setup.md
    did not hold, instead of silently claiming an ingress guarantee this
    process cannot make.
    """

    reset_observed_wsgi_input_materialization()
    try:
        admission = _admission(
            lambda _path: _measurement("request", free_bytes=10_000_000)
        )
        app = _request_app(tmp_path, admission)
        body = _multipart_file_body(b'{"ok":true}\n')
        buffered = tmp_path / "upstream-buffered-body"
        buffered.write_bytes(body)

        with buffered.open("rb") as handle:
            environ = _multipart_environ(
                body, content_length=len(body), terminated=False
            )
            environ["wsgi.input"] = handle
            with app.request_context(environ):
                from flask import request

                assert len(request.files.getlist("files")) == 1

        assert observed_wsgi_input_materialization() == "host-file"
    finally:
        reset_observed_wsgi_input_materialization()


def test_socket_backed_wsgi_input_is_reported_as_a_stream(tmp_path: Path) -> None:
    """The supported deployment hands the application a socket, not a file.

    Werkzeug's development server sets ``wsgi.input`` to the connection's
    ``rfile`` and wraps chunked bodies in ``DechunkedInput``; neither touches
    storage. A pipe stands in for the socket here because the classification
    only distinguishes regular files from everything else.
    """

    import os as _os

    reset_observed_wsgi_input_materialization()
    read_fd, write_fd = _os.pipe()
    try:
        admission = _admission(
            lambda _path: _measurement("request", free_bytes=10_000_000)
        )
        app = _request_app(tmp_path, admission)
        body = _multipart_file_body(b'{"ok":true}\n')
        _os.write(write_fd, body)
        _os.close(write_fd)
        write_fd = -1

        with _os.fdopen(read_fd, "rb", buffering=0) as handle:
            read_fd = -1
            environ = _multipart_environ(
                body, content_length=len(body), terminated=False
            )
            environ["wsgi.input"] = handle
            with app.request_context(environ):
                from flask import request

                assert len(request.files.getlist("files")) == 1

        assert observed_wsgi_input_materialization() == "stream"
    finally:
        for fd in (read_fd, write_fd):
            if fd >= 0:
                _os.close(fd)
        reset_observed_wsgi_input_materialization()
