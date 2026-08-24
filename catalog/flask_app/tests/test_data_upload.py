from __future__ import annotations

import errno
import hashlib
import io
import json
import sqlite3
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from flask import Flask, current_app, url_for
from werkzeug.datastructures import FileStorage

from catalog.common.data_loading import iter_jsonl_files
from catalog.flask_app import data_upload_routes as upload_routes
from catalog.flask_app.data_upload_routes import data_upload_web
from catalog.flask_app.services import data_upload_service as upload_service_module
from catalog.flask_app.services.data_upload_service import DataUploadService

_STAGING_OWNER = ".fcp-upload-owner.json"
_STAGING_OWNER_SCHEMA = "fcp-data-upload-staging-v1"
_IMPORT_MARKER = ".fcp-importing"


class _Runtime:
    def __init__(self, *, accepted: bool = True) -> None:
        self.accepted = accepted
        self.requests = 0

    def request_refresh(self, *, execution_id: str | None = None) -> bool:
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


class _CommitThenRaise:
    """SQLite connection proxy for the acknowledged-after-commit crash window."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def __enter__(self):
        self.connection.__enter__()
        return self

    def __exit__(self, *args):
        return self.connection.__exit__(*args)

    def __getattr__(self, name: str):
        return getattr(self.connection, name)

    def commit(self) -> None:
        self.connection.commit()
        raise sqlite3.OperationalError("simulated error after durable commit")


class _CommitRaiseBeforeDurable(_CommitThenRaise):
    def commit(self) -> None:
        raise sqlite3.OperationalError("simulated error before durable commit")


def _service(
    tmp_path: Path,
    runtime: _Runtime | None = None,
    *,
    max_pending_imports: int = 8,
) -> DataUploadService:
    return DataUploadService(
        database=tmp_path / "imports" / "uploads.sqlite3",
        staging_root=tmp_path / "imports" / "staging",
        published_root=tmp_path / "data" / "uploads",
        runtime_manager=runtime or _Runtime(),
        max_files=8,
        max_file_bytes=1024 * 1024,
        max_total_bytes=4 * 1024 * 1024,
        max_line_bytes=64 * 1024,
        max_pending_imports=max_pending_imports,
    )


def _wait_for_terminal(service: DataUploadService, batch_id: str) -> dict:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        batch = service.batch(batch_id)
        if batch["status"] == "failed" or (
            batch["status"] == "ready" and batch["ready_for_analysis"]
        ):
            return batch
        time.sleep(0.02)
    raise AssertionError("upload batch did not reach a terminal state")


def _insert_batch(
    service: DataUploadService,
    *,
    batch_id: str,
    status: str,
    payloads: dict[str, bytes],
) -> None:
    with sqlite3.connect(service.database) as connection:
        connection.execute(
            """
            INSERT INTO data_upload_batches(
                batch_id,status,file_count,total_bytes,created_at,imported_records,
                published_path
            ) VALUES(?,?,?,?, '2026-08-06T10:00:00Z', ?, ?)
            """,
            (
                batch_id,
                status,
                len(payloads),
                sum(map(len, payloads.values())),
                len(payloads),
                str(service.published_root / batch_id) if status == "ready" else None,
            ),
        )
        for index, (name, payload) in enumerate(payloads.items()):
            connection.execute(
                """
                INSERT INTO data_upload_files(
                    file_id,batch_id,original_name,published_name,staged_name,
                    size_bytes,content_sha256,imported_records,status
                ) VALUES(?,?,?,?,?,?,?,1,'imported')
                """,
                (
                    f"file-{batch_id}-{index}",
                    batch_id,
                    name,
                    name,
                    f"{name}.uploading",
                    len(payload),
                    hashlib.sha256(payload).hexdigest(),
                ),
            )


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
                    io.BytesIO(b'{"timestamp":"2026-08-05T10:00:00Z","machine":"A"}\n'),
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
        # JSONL remains the single durable payload copy; SQLite stores lifecycle
        # metadata rather than amplifying large uploads with a second copy.
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM data_upload_records WHERE batch_id=?",
                (batch_id,),
            ).fetchone()[0]
            == 0
        )

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
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM data_upload_records WHERE batch_id=?",
                (batch["batch_id"],),
            ).fetchone()[0]
            == 0
        )


def test_jsonl_discovery_hides_directory_until_publish_marker_is_removed(
    tmp_path: Path,
) -> None:
    batch_dir = tmp_path / "data" / "uploads" / "upload-test"
    batch_dir.mkdir(parents=True)
    file_path = batch_dir / "data.jsonl"
    file_path.write_text('{"value":1}\n', encoding="utf-8")
    marker = batch_dir / ".fcp-importing"
    marker.write_text("upload-test", encoding="utf-8")

    assert list(iter_jsonl_files(tmp_path / "data")) == []

    marker.unlink()
    assert list(iter_jsonl_files(tmp_path / "data")) == [file_path]


def test_jsonl_discovery_fails_closed_for_non_file_publish_marker(
    tmp_path: Path,
) -> None:
    batch_dir = tmp_path / "data" / "uploads" / "upload-test"
    batch_dir.mkdir(parents=True)
    file_path = batch_dir / "data.jsonl"
    file_path.write_text('{"value":1}\n', encoding="utf-8")
    (batch_dir / _IMPORT_MARKER).mkdir()

    assert list(iter_jsonl_files(tmp_path / "data")) == []


def test_directory_fsync_propagates_supported_filesystem_failures(
    tmp_path: Path,
    monkeypatch,
) -> None:
    closed: list[int] = []

    def _fail_fsync(_descriptor: int) -> None:
        raise OSError(errno.ENOSPC, "simulated full filesystem")

    monkeypatch.setattr(upload_service_module.os, "name", "posix")
    monkeypatch.setattr(upload_service_module.os, "open", lambda *_args: 41)
    monkeypatch.setattr(upload_service_module.os, "fsync", _fail_fsync)
    monkeypatch.setattr(upload_service_module.os, "close", closed.append)

    with pytest.raises(OSError, match="simulated full filesystem"):
        upload_service_module._fsync_directory(tmp_path)

    assert closed == [41]


def test_staging_is_owned_before_the_first_payload_byte(tmp_path: Path) -> None:
    service = _service(tmp_path)

    class _InspectingStream(io.BytesIO):
        inspected = False

        def read(self, size: int = -1) -> bytes:
            if not self.inspected:
                children = tuple(service.staging_root.iterdir())
                assert len(children) == 1
                owner = json.loads((children[0] / _STAGING_OWNER).read_text("utf-8"))
                assert owner == {
                    "batch_id": children[0].name,
                    "schema": _STAGING_OWNER_SCHEMA,
                }
                self.inspected = True
            return super().read(size)

    stream = _InspectingStream(b'{"owned":true}\n')
    batch = service.enqueue((FileStorage(stream=stream, filename="data.jsonl"),))

    assert stream.inspected is True
    assert _wait_for_terminal(service, batch["batch_id"])["status"] == "ready"


def test_database_insert_commit_ambiguity_is_adopted_in_process(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _service(tmp_path)
    original_connect = service._connect
    wrapped = False

    def _connect():
        nonlocal wrapped
        connection = original_connect()
        if not wrapped:
            wrapped = True
            return _CommitThenRaise(connection)
        return connection

    monkeypatch.setattr(service, "_connect", _connect)

    batch = service.enqueue(
        (
            FileStorage(
                stream=io.BytesIO(b'{"durable":true}\n'),
                filename="data.jsonl",
            ),
        )
    )

    assert _wait_for_terminal(service, batch["batch_id"])["status"] == "ready"
    assert not (service.staging_root / batch["batch_id"]).exists()


def test_database_insert_precommit_failure_cleans_and_reraises(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _service(tmp_path)
    original_connect = service._connect
    wrapped = False

    def _connect():
        nonlocal wrapped
        connection = original_connect()
        if not wrapped:
            wrapped = True
            return _CommitRaiseBeforeDurable(connection)
        return connection

    monkeypatch.setattr(service, "_connect", _connect)

    with pytest.raises(sqlite3.OperationalError, match="before durable commit"):
        service.enqueue(
            (
                FileStorage(
                    stream=io.BytesIO(b'{"durable":false}\n'),
                    filename="data.jsonl",
                ),
            )
        )

    assert tuple(service.staging_root.iterdir()) == ()
    with sqlite3.connect(service.database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM data_upload_batches").fetchone()[0] == 0


def test_database_insert_unknown_commit_preserves_and_reraises(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _service(tmp_path)
    original_connect = service._connect
    wrapped = False

    def _connect():
        nonlocal wrapped
        connection = original_connect()
        if not wrapped:
            wrapped = True
            return _CommitRaiseBeforeDurable(connection)
        return connection

    monkeypatch.setattr(service, "_connect", _connect)
    monkeypatch.setattr(service, "_batch_record_exists", lambda _batch_id: None)

    with pytest.raises(sqlite3.OperationalError, match="before durable commit"):
        service.enqueue(
            (
                FileStorage(
                    stream=io.BytesIO(b'{"durable":"unknown"}\n'),
                    filename="data.jsonl",
                ),
            )
        )

    staged = tuple(service.staging_root.iterdir())
    assert len(staged) == 1
    assert (staged[0] / _STAGING_OWNER).is_file()
    assert len(tuple(staged[0].glob("*.uploading"))) == 1


def test_restart_removes_only_owned_pre_database_staging(tmp_path: Path) -> None:
    service = _service(tmp_path)
    owned_id = "upload-11111111111111111111111111111111"
    owned = service.staging_root / owned_id
    owned.mkdir()
    (owned / _STAGING_OWNER).write_text(
        json.dumps({"batch_id": owned_id, "schema": _STAGING_OWNER_SCHEMA}),
        encoding="utf-8",
    )
    (owned / "001-44444444444444444444444444444444.uploading").write_bytes(
        b"orphan"
    )

    legacy = service.staging_root / "upload-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    legacy.mkdir()
    (legacy / "001-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.uploading").write_bytes(
        b"legacy orphan"
    )

    unowned = service.staging_root / "upload-22222222222222222222222222222222"
    unowned.mkdir()
    (unowned / "user-data.jsonl").write_bytes(b"do not remove")

    mismatched = service.staging_root / "upload-33333333333333333333333333333333"
    mismatched.mkdir()
    (mismatched / _STAGING_OWNER).write_text(
        json.dumps({"batch_id": owned_id, "schema": _STAGING_OWNER_SCHEMA}),
        encoding="utf-8",
    )
    (mismatched / "user-data.jsonl").write_bytes(b"do not remove")

    _service(tmp_path)

    assert not owned.exists()
    assert not legacy.exists()
    assert (unowned / "user-data.jsonl").read_bytes() == b"do not remove"
    assert (mismatched / "user-data.jsonl").read_bytes() == b"do not remove"


def test_restart_bounds_and_type_checks_owned_orphan_cleanup(tmp_path: Path) -> None:
    service = _service(tmp_path)

    nested_id = "upload-44444444444444444444444444444444"
    nested = service.staging_root / nested_id
    nested.mkdir()
    (nested / _STAGING_OWNER).write_text(
        json.dumps({"batch_id": nested_id, "schema": _STAGING_OWNER_SCHEMA}),
        encoding="utf-8",
    )
    staged_directory = nested / "001-55555555555555555555555555555555.uploading"
    staged_directory.mkdir()
    nested_evidence = staged_directory / "operator-note.txt"
    nested_evidence.write_bytes(b"do not remove")

    oversized_id = "upload-66666666666666666666666666666666"
    oversized = service.staging_root / oversized_id
    oversized.mkdir()
    (oversized / _STAGING_OWNER).write_text(
        json.dumps({"batch_id": oversized_id, "schema": _STAGING_OWNER_SCHEMA}),
        encoding="utf-8",
    )
    for index in range(service.max_files + 1):
        (oversized / f"{index + 1:03d}-{index:032x}.uploading").write_bytes(
            b"bounded"
        )

    _service(tmp_path)

    assert nested_evidence.read_bytes() == b"do not remove"
    assert len(tuple(oversized.glob("*.uploading"))) == service.max_files + 1


def test_database_ready_failure_keeps_complete_publication_hidden(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _service(tmp_path)
    original_set_state = service._set_batch_state

    def _fail_ready(batch_id: str, status: str, **values) -> None:
        if status == "ready":
            raise sqlite3.OperationalError("simulated ready commit failure")
        original_set_state(batch_id, status, **values)

    monkeypatch.setattr(service, "_set_batch_state", _fail_ready)
    batch = service.enqueue(
        (
            FileStorage(
                stream=io.BytesIO(b'{"complete":true}\n'),
                filename="data.jsonl",
            ),
        )
    )
    terminal = _wait_for_terminal(service, batch["batch_id"])
    published = service.published_root / batch["batch_id"]

    assert terminal["status"] == "failed"
    assert (published / _IMPORT_MARKER).is_file()
    assert list(iter_jsonl_files(service.published_root)) == []

    recovered = _service(tmp_path)
    assert _wait_for_terminal(recovered, batch["batch_id"])["status"] == "ready"
    assert list(iter_jsonl_files(service.published_root)) == [published / "data.jsonl"]


def test_import_commit_ambiguity_preserves_durable_payload_for_restart(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _service(tmp_path)
    batch_id = "upload-16161616161616161616161616161616"
    payload = b'{"durable":true}\n'
    with sqlite3.connect(service.database) as connection:
        connection.execute(
            """
            INSERT INTO data_upload_batches(
                batch_id,status,file_count,total_bytes,created_at
            ) VALUES(?, 'queued', 1, ?, '2026-08-06T10:00:00Z')
            """,
            (batch_id, len(payload)),
        )
        connection.execute(
            """
            INSERT INTO data_upload_files(
                file_id,batch_id,original_name,published_name,staged_name,
                size_bytes,content_sha256,status
            ) VALUES('file-commit-window',?,'data.jsonl','data.jsonl',
                'data.jsonl.uploading',?,?,'staged')
            """,
            (batch_id, len(payload), hashlib.sha256(payload).hexdigest()),
        )
    staging = service.staging_root / batch_id
    staging.mkdir()
    staged = staging / "data.jsonl.uploading"
    staged.write_bytes(payload)

    original_connect = service._connect
    connection_count = 0

    def _connect():
        nonlocal connection_count
        connection_count += 1
        connection = original_connect()
        if connection_count == 2:
            return _CommitThenRaise(connection)
        return connection

    monkeypatch.setattr(service, "_connect", _connect)

    service._import_batch_serialized(batch_id)

    assert service.batch(batch_id)["status"] == "failed"
    assert staged.read_bytes() == payload

    restarted = _service(tmp_path)

    assert _wait_for_terminal(restarted, batch_id)["status"] == "ready"


def test_restart_finalizes_publication_completed_before_ready_update(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    batch_id = "upload-interrupted"
    published_payload = b'{"complete":true}\n'
    with sqlite3.connect(service.database) as connection:
        connection.execute(
            """
            INSERT INTO data_upload_batches(
                batch_id,status,file_count,total_bytes,created_at
            ) VALUES(?, 'publishing', 1, ?, '2026-08-06T10:00:00Z')
            """,
            (batch_id, len(published_payload)),
        )
        connection.execute(
            """INSERT INTO data_upload_files(
                file_id,batch_id,original_name,published_name,staged_name,
                size_bytes,content_sha256,imported_records,status
            ) VALUES('file-1',?,'data.jsonl','data.jsonl','data.uploading',?,?,1,'imported')""",
            (
                batch_id,
                len(published_payload),
                hashlib.sha256(published_payload).hexdigest(),
            ),
        )
    staging = service.staging_root / batch_id
    published = service.published_root / batch_id
    staging.mkdir(parents=True)
    published.mkdir(parents=True)
    # Every move and marker removal completed before the process died; only the
    # durable DB transition to ready is missing.
    (published / "data.jsonl").write_bytes(published_payload)
    assert not (published / ".fcp-importing").exists()
    assert list(iter_jsonl_files(service.published_root)) == [published / "data.jsonl"]

    restarted = _service(tmp_path)
    recovered = _wait_for_terminal(restarted, batch_id)

    assert recovered["status"] == "ready"
    assert recovered["error_code"] is None
    assert not staging.exists()
    assert (published / "data.jsonl").read_bytes() == published_payload


def test_restart_synchronously_hides_complete_nonready_publication(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _service(tmp_path)
    batch_id = "upload-44444444444444444444444444444444"
    payloads = {"data.jsonl": b'{"complete":true}\n'}
    _insert_batch(
        service,
        batch_id=batch_id,
        status="publishing",
        payloads=payloads,
    )
    published = service.published_root / batch_id
    published.mkdir()
    (published / "data.jsonl").write_bytes(payloads["data.jsonl"])
    assert list(iter_jsonl_files(service.published_root)) == [published / "data.jsonl"]

    monkeypatch.setattr(DataUploadService, "_drain_queued_imports", lambda self: None)
    restarted = _service(tmp_path)

    assert restarted.batch(batch_id)["status"] == "queued"
    assert (published / _IMPORT_MARKER).is_file()
    assert list(iter_jsonl_files(service.published_root)) == []


def test_restart_reveals_verified_database_ready_publication(tmp_path: Path) -> None:
    service = _service(tmp_path)
    batch_id = "upload-55555555555555555555555555555555"
    payloads = {"data.jsonl": b'{"complete":true}\n'}
    _insert_batch(service, batch_id=batch_id, status="ready", payloads=payloads)
    published = service.published_root / batch_id
    published.mkdir()
    (published / _IMPORT_MARKER).write_text(batch_id, encoding="utf-8")
    (published / "data.jsonl").write_bytes(payloads["data.jsonl"])
    assert list(iter_jsonl_files(service.published_root)) == []

    restarted = _service(tmp_path)

    assert restarted.batch(batch_id)["status"] == "ready"
    assert not (published / _IMPORT_MARKER).exists()
    assert list(iter_jsonl_files(service.published_root)) == [published / "data.jsonl"]


def test_restart_preserves_mismatched_publication_marker_on_every_attempt(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    batch_id = "upload-12121212121212121212121212121212"
    payloads = {"data.jsonl": b'{"complete":true}\n'}
    _insert_batch(service, batch_id=batch_id, status="ready", payloads=payloads)
    published = service.published_root / batch_id
    published.mkdir()
    marker = published / _IMPORT_MARKER
    marker.write_text("upload-someone-else", encoding="utf-8")
    (published / "data.jsonl").write_bytes(payloads["data.jsonl"])

    first_restart = _service(tmp_path)

    assert first_restart.batch(batch_id)["status"] == "failed"
    assert first_restart.batch(batch_id)["error_code"] == "upload-publish-marker-invalid"
    assert marker.read_text(encoding="utf-8") == "upload-someone-else"
    assert list(iter_jsonl_files(service.published_root)) == []

    second_restart = _service(tmp_path)

    assert second_restart.batch(batch_id)["status"] == "failed"
    assert second_restart.batch(batch_id)["error_code"] == "upload-publish-marker-invalid"
    assert marker.read_text(encoding="utf-8") == "upload-someone-else"
    assert list(iter_jsonl_files(service.published_root)) == []


def test_restart_repairs_incomplete_database_ready_publication(tmp_path: Path) -> None:
    service = _service(tmp_path, max_pending_imports=1)
    batch_id = "upload-66666666666666666666666666666666"
    payloads = {"first.jsonl": b'{"part":1}\n', "second.jsonl": b'{"part":2}\n'}
    _insert_batch(service, batch_id=batch_id, status="ready", payloads=payloads)
    staging = service.staging_root / batch_id
    published = service.published_root / batch_id
    staging.mkdir()
    published.mkdir()
    (published / _IMPORT_MARKER).write_text(batch_id, encoding="utf-8")
    (published / "first.jsonl").write_bytes(payloads["first.jsonl"])
    (staging / "second.jsonl.uploading").write_bytes(payloads["second.jsonl"])

    restarted = _service(tmp_path, max_pending_imports=1)
    recovered = _wait_for_terminal(restarted, batch_id)

    assert recovered["status"] == "ready"
    assert not (published / _IMPORT_MARKER).exists()
    assert {
        path.name: path.read_bytes() for path in published.glob("*.jsonl")
    } == payloads


def test_recovery_preserves_unexpected_published_content_and_fails_closed(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path, max_pending_imports=1)
    batch_id = "upload-77777777777777777777777777777777"
    payloads = {"data.jsonl": b'{"complete":true}\n'}
    _insert_batch(
        service,
        batch_id=batch_id,
        status="publishing",
        payloads=payloads,
    )
    staging = service.staging_root / batch_id
    published = service.published_root / batch_id
    staging.mkdir()
    published.mkdir()
    (staging / "data.jsonl.uploading").write_bytes(payloads["data.jsonl"])
    (published / "operator-note.txt").write_bytes(b"unrelated")

    restarted = _service(tmp_path, max_pending_imports=1)
    recovered = _wait_for_terminal(restarted, batch_id)

    assert recovered["status"] == "failed"
    assert (published / _IMPORT_MARKER).is_file()
    assert (published / "operator-note.txt").read_bytes() == b"unrelated"
    assert list(iter_jsonl_files(service.published_root)) == []


def test_recovery_preserves_unexpected_staging_content_and_fails_closed(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path, max_pending_imports=1)
    batch_id = "upload-99999999999999999999999999999999"
    payloads = {"data.jsonl": b'{"complete":true}\n'}
    _insert_batch(
        service,
        batch_id=batch_id,
        status="publishing",
        payloads=payloads,
    )
    staging = service.staging_root / batch_id
    staging.mkdir()
    (staging / "data.jsonl.uploading").write_bytes(payloads["data.jsonl"])
    (staging / "operator-note.txt").write_bytes(b"unrelated")

    restarted = _service(tmp_path, max_pending_imports=1)
    recovered = _wait_for_terminal(restarted, batch_id)
    published = service.published_root / batch_id

    assert recovered["status"] == "failed"
    assert recovered["error_code"] == "upload-staging-ambiguous"
    assert (published / _IMPORT_MARKER).is_file()
    assert (staging / "operator-note.txt").read_bytes() == b"unrelated"
    assert list(iter_jsonl_files(service.published_root)) == []


def test_recovery_preserves_expected_name_directory_in_staging(tmp_path: Path) -> None:
    service = _service(tmp_path)
    batch_id = "upload-13131313131313131313131313131313"
    payloads = {"data.jsonl": b'{"complete":true}\n'}
    _insert_batch(service, batch_id=batch_id, status="ready", payloads=payloads)
    staging = service.staging_root / batch_id
    published = service.published_root / batch_id
    staging.mkdir()
    published.mkdir()
    (published / _IMPORT_MARKER).write_text(batch_id, encoding="utf-8")
    (published / "data.jsonl").write_bytes(payloads["data.jsonl"])
    staged_directory = staging / "data.jsonl.uploading"
    staged_directory.mkdir()
    nested_evidence = staged_directory / "operator-note.txt"
    nested_evidence.write_bytes(b"do not remove")

    restarted = _service(tmp_path)
    recovered = restarted.batch(batch_id)

    assert recovered["status"] == "failed"
    assert recovered["error_code"] == "upload-staging-ambiguous"
    assert nested_evidence.read_bytes() == b"do not remove"
    assert (published / _IMPORT_MARKER).is_file()
    assert list(iter_jsonl_files(service.published_root)) == []


def test_recovery_validates_staging_owner_before_cleanup(tmp_path: Path) -> None:
    service = _service(tmp_path)
    batch_id = "upload-14141414141414141414141414141414"
    payloads = {"data.jsonl": b'{"complete":true}\n'}
    _insert_batch(service, batch_id=batch_id, status="ready", payloads=payloads)
    staging = service.staging_root / batch_id
    published = service.published_root / batch_id
    staging.mkdir()
    published.mkdir()
    owner = staging / _STAGING_OWNER
    owner.write_text(
        json.dumps(
            {
                "batch_id": "upload-15151515151515151515151515151515",
                "schema": _STAGING_OWNER_SCHEMA,
            }
        ),
        encoding="utf-8",
    )
    staged = staging / "data.jsonl.uploading"
    staged.write_bytes(payloads["data.jsonl"])
    (published / _IMPORT_MARKER).write_text(batch_id, encoding="utf-8")
    (published / "data.jsonl").write_bytes(payloads["data.jsonl"])

    restarted = _service(tmp_path)
    recovered = restarted.batch(batch_id)

    assert recovered["status"] == "failed"
    assert recovered["error_code"] == "upload-staging-ambiguous"
    assert owner.is_file()
    assert staged.read_bytes() == payloads["data.jsonl"]
    assert (published / _IMPORT_MARKER).is_file()
    assert list(iter_jsonl_files(service.published_root)) == []


def test_recovery_rejects_database_paths_outside_owned_batch_directories(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    batch_id = "upload-88888888888888888888888888888888"
    outside = service.staging_root.parent / "outside.jsonl"
    outside.write_bytes(b"unrelated")
    with sqlite3.connect(service.database) as connection:
        connection.execute(
            """
            INSERT INTO data_upload_batches(
                batch_id,status,file_count,total_bytes,created_at
            ) VALUES(?, 'publishing', 1, 9, '2026-08-06T10:00:00Z')
            """,
            (batch_id,),
        )
        connection.execute(
            """
            INSERT INTO data_upload_files(
                file_id,batch_id,original_name,published_name,staged_name,
                size_bytes,content_sha256,imported_records,status
            ) VALUES('file-unsafe',?,'data.jsonl','data.jsonl',
                '../outside.jsonl',9,?,1,'imported')
            """,
            (batch_id, hashlib.sha256(b"unrelated").hexdigest()),
        )

    restarted = _service(tmp_path)
    recovered = restarted.batch(batch_id)

    assert recovered["status"] == "failed"
    assert recovered["error_code"] == "upload-path-invalid"
    assert outside.read_bytes() == b"unrelated"


def test_restart_resumes_publication_after_only_some_files_were_moved(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path, max_pending_imports=1)
    batch_id = "upload-partial-move"
    payloads = {"first.jsonl": b'{"part":1}\n', "second.jsonl": b'{"part":2}\n'}
    with sqlite3.connect(service.database) as connection:
        connection.execute(
            """INSERT INTO data_upload_batches(
                batch_id,status,file_count,total_bytes,created_at,imported_records
            ) VALUES(?, 'publishing', 2, ?, '2026-08-06T10:00:00Z', 2)""",
            (batch_id, sum(map(len, payloads.values()))),
        )
        for index, (name, payload) in enumerate(payloads.items()):
            connection.execute(
                """INSERT INTO data_upload_files(
                    file_id,batch_id,original_name,published_name,staged_name,
                    size_bytes,content_sha256,imported_records,status
                ) VALUES(?,?,?,?,?,?,?,1,'imported')""",
                (
                    f"file-partial-{index}",
                    batch_id,
                    name,
                    name,
                    f"{name}.uploading",
                    len(payload),
                    hashlib.sha256(payload).hexdigest(),
                ),
            )
    staging = service.staging_root / batch_id
    published = service.published_root / batch_id
    staging.mkdir(parents=True)
    published.mkdir(parents=True)
    (published / ".fcp-importing").write_text(batch_id, encoding="utf-8")
    (published / "first.jsonl").write_bytes(payloads["first.jsonl"])
    # A stale destination with the right size but wrong identity must not be
    # trusted merely because the final path exists.
    (published / "second.jsonl").write_bytes(b'{"part":9}\n')
    (staging / "second.jsonl.uploading").write_bytes(payloads["second.jsonl"])
    assert list(iter_jsonl_files(service.published_root)) == []

    restarted = _service(tmp_path, max_pending_imports=1)
    recovered = _wait_for_terminal(restarted, batch_id)

    assert recovered["status"] == "ready"
    assert not (published / ".fcp-importing").exists()
    assert not staging.exists()
    assert {
        path.name: path.read_bytes() for path in published.glob("*.jsonl")
    } == payloads


def test_restart_drains_more_queued_batches_than_worker_capacity(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path, max_pending_imports=2)
    batch_ids = [f"upload-recovery-{index}" for index in range(5)]
    with sqlite3.connect(service.database) as connection:
        for index, batch_id in enumerate(batch_ids):
            payload = f'{{"batch":{index}}}\n'.encode()
            connection.execute(
                """
                INSERT INTO data_upload_batches(
                    batch_id,status,file_count,total_bytes,created_at
                ) VALUES(?, 'publishing', 1, ?, ?)
                """,
                (batch_id, len(payload), f"2026-08-06T10:00:{index:02d}Z"),
            )
            connection.execute(
                """INSERT INTO data_upload_files(
                    file_id,batch_id,original_name,published_name,staged_name,
                    size_bytes,content_sha256,imported_records,status
                ) VALUES(?,?, 'data.jsonl','data.jsonl','data.uploading',?,?,1,'imported')""",
                (
                    f"file-{index}",
                    batch_id,
                    len(payload),
                    hashlib.sha256(payload).hexdigest(),
                ),
            )
            staging = service.staging_root / batch_id
            published = service.published_root / batch_id
            staging.mkdir(parents=True)
            published.mkdir(parents=True)
            (published / ".fcp-importing").write_text(batch_id, encoding="utf-8")
            (published / "data.jsonl").write_bytes(payload)

    restarted = _service(tmp_path, max_pending_imports=2)

    recovered = [_wait_for_terminal(restarted, batch_id) for batch_id in batch_ids]
    assert [batch["status"] for batch in recovered] == ["ready"] * len(batch_ids)
    assert all(
        not (restarted.staging_root / batch_id).exists() for batch_id in batch_ids
    )
