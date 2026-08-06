from __future__ import annotations

import io
import sqlite3
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from flask import Flask, current_app, url_for
from werkzeug.datastructures import FileStorage

from catalog.common.data_loading import iter_jsonl_files
from catalog.flask_app import data_upload_routes as upload_routes
from catalog.flask_app.data_upload_routes import data_upload_web
from catalog.flask_app.services.data_upload_service import DataUploadService


class _Runtime:
    def __init__(self, *, accepted: bool = True) -> None:
        self.accepted = accepted
        self.requests = 0

    def request_refresh(self) -> bool:
        self.requests += 1
        return self.accepted


class _Jobs:
    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.started: list[str] = []
        self.failed: list[tuple[str, str]] = []

    def submit_batch(self, batch: dict) -> str:
        job_id = f"analysis-{batch['batch_id']}"
        self.submitted.append(job_id)
        return job_id

    def start_tracking(self, job_id: str) -> None:
        self.started.append(job_id)

    def fail(self, job_id: str, error_code: str) -> None:
        self.failed.append((job_id, error_code))


def _service(tmp_path: Path, runtime: _Runtime | None = None) -> DataUploadService:
    return DataUploadService(
        database=tmp_path / "imports" / "uploads.sqlite3",
        staging_root=tmp_path / "imports" / "staging",
        published_root=tmp_path / "data" / "uploads",
        runtime_manager=runtime or _Runtime(),
        max_files=8,
        max_file_bytes=1024 * 1024,
        max_total_bytes=4 * 1024 * 1024,
        max_line_bytes=64 * 1024,
    )


def _wait_for_terminal(service: DataUploadService, batch_id: str) -> dict:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        batch = service.batch(batch_id)
        if batch["status"] in {"ready", "failed"}:
            return batch
        time.sleep(0.02)
    raise AssertionError("upload batch did not reach a terminal state")


def _test_app(service: DataUploadService) -> Flask:
    app = Flask(__name__)
    app.config.update(TESTING=True, SECRET_KEY="test-secret")
    app.extensions["data_upload_service"] = service

    def _dummy() -> str:
        return "ok"

    for endpoint, path, methods in (
        ("web.overview", "/", ("GET",)),
        ("web.live", "/live", ("GET",)),
        ("web.playback", "/playback", ("GET",)),
        ("web.status", "/status", ("GET",)),
        ("web.guide", "/guide", ("GET",)),
        ("web.startup", "/startup", ("GET",)),
        ("web.rescan", "/rescan", ("POST",)),
    ):
        app.add_url_rule(path, endpoint, _dummy, methods=list(methods))

    @app.context_processor
    def _navigation() -> dict[str, object]:
        def endpoint_url(endpoint: str, fallback: str) -> str:
            if endpoint in current_app.view_functions:
                return url_for(endpoint)
            return fallback

        return {"endpoint_url": endpoint_url}

    app.register_blueprint(data_upload_web)
    return app


def test_multiple_jsonl_files_import_then_create_durable_background_job(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime = _Runtime()
    service = _service(tmp_path, runtime)
    jobs = _Jobs()
    monkeypatch.setattr(
        upload_routes,
        "get_upload_analysis_job_service",
        lambda: jobs,
    )
    app = _test_app(service)
    client = app.test_client()

    response = client.get("/data-upload/")
    assert response.status_code == 200
    with client.session_transaction() as session:
        csrf = session["data_upload_csrf_token"]

    response = client.post(
        "/data-upload/",
        data={
            "_csrf_token": csrf,
            "files": [
                (
                    io.BytesIO(
                        b'{"timestamp":"2026-08-05T10:00:00Z","machine":"A"}\n'
                    ),
                    "first.jsonl",
                ),
                (
                    io.BytesIO(
                        b'{"timestamp":"2026-08-06T10:00:00Z","machine":"B"}\n'
                        b'{"timestamp":"2026-08-06T10:01:00Z","machine":"B"}\n'
                    ),
                    "second.jsonl",
                ),
            ],
        },
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    assert response.status_code == 302
    batch_id = parse_qs(urlparse(response.headers["Location"]).query)["batch"][0]
    batch = _wait_for_terminal(service, batch_id)

    assert batch["status"] == "ready"
    assert batch["file_count"] == 2
    assert batch["imported_records"] == 3
    assert batch["ready_for_analysis"] is True
    assert sorted(path.name for path in iter_jsonl_files(tmp_path / "data")) == [
        "first.jsonl",
        "second.jsonl",
    ]
    with sqlite3.connect(service.database) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM data_upload_records WHERE batch_id=?",
            (batch_id,),
        ).fetchone()[0] == 3

    page = client.get(f"/data-upload/?batch={batch_id}")
    assert page.status_code == 200
    assert b"Start background analysis" in page.data
    assert b"Drop JSONL files here" in page.data

    response = client.post(
        f"/data-upload/{batch_id}/analyze",
        data={"_csrf_token": csrf},
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert runtime.requests == 1
    assert service.batch(batch_id)["analysis_state"] == "requested"
    expected_job_id = f"analysis-{batch_id}"
    assert jobs.submitted == [expected_job_id]
    assert jobs.started == [expected_job_id]
    assert jobs.failed == []


def test_invalid_json_rolls_back_database_and_never_publishes_batch(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    batch = service.enqueue(
        (
            FileStorage(
                stream=io.BytesIO(b'{"ok":true}\nnot-json\n'),
                filename="broken.jsonl",
            ),
        )
    )
    terminal = _wait_for_terminal(service, batch["batch_id"])

    assert terminal["status"] == "failed"
    assert terminal["error_code"] == "upload-invalid-json"
    assert list(iter_jsonl_files(tmp_path / "data")) == []
    with sqlite3.connect(service.database) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM data_upload_records WHERE batch_id=?",
            (batch["batch_id"],),
        ).fetchone()[0] == 0


def test_jsonl_discovery_hides_directory_until_publish_marker_is_removed(
    tmp_path: Path,
) -> None:
    batch_dir = tmp_path / "data" / "uploads" / "upload-test"
    batch_dir.mkdir(parents=True)
    file_path = batch_dir / "data.jsonl"
    file_path.write_text('{"value":1}\n', encoding="utf-8")
    marker = batch_dir / ".msh-importing"
    marker.write_text("upload-test", encoding="utf-8")

    assert list(iter_jsonl_files(tmp_path / "data")) == []

    marker.unlink()
    assert list(iter_jsonl_files(tmp_path / "data")) == [file_path]


def test_restart_marks_interrupted_batch_failed_and_removes_partial_directories(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    batch_id = "upload-interrupted"
    with sqlite3.connect(service.database) as connection:
        connection.execute(
            """
            INSERT INTO data_upload_batches(
                batch_id,status,file_count,total_bytes,created_at
            ) VALUES(?, 'publishing', 1, 10, '2026-08-06T10:00:00Z')
            """,
            (batch_id,),
        )
    staging = service.staging_root / batch_id
    published = service.published_root / batch_id
    staging.mkdir(parents=True)
    published.mkdir(parents=True)
    (staging / "data.uploading").write_bytes(b"partial")
    (published / ".msh-importing").write_text(batch_id, encoding="utf-8")
    (published / "data.jsonl").write_text('{"partial":true}\n', encoding="utf-8")

    restarted = _service(tmp_path)
    recovered = restarted.batch(batch_id)

    assert recovered["status"] == "failed"
    assert recovered["error_code"] == "import-interrupted"
    assert not staging.exists()
    assert not published.exists()
