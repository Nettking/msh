from __future__ import annotations

import io
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from flask import Flask
from werkzeug.datastructures import FileStorage

from catalog.federation.host_resources import (
    FilesystemMeasurement,
    PressureThresholds,
    ProcessResourceAdmission,
)
from catalog.federation.process_resource_admission import SerializedProcessResourceAdmission
from catalog.flask_app import data_upload_routes
from catalog.flask_app.services.data_upload_resource_admission import (
    enqueue_with_resource_admission,
)
from catalog.flask_app.services.data_upload_service import DataUploadError, DataUploadService


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
