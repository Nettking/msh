"""Durable multi-file JSONL upload and import service.

Uploads are copied out of the request before a background worker validates them.
Every nonblank line must be a JSON object. Records for one batch are written in a
single SQLite transaction, and the corresponding JSONL files remain hidden from
the supported analysis discovery path until the complete batch is published.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from flask import current_app
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from catalog.orchestrator.pipeline import get_runtime_manager
from catalog.runner.script_catalog import repo_root

_IMPORT_MARKER = ".msh-importing"
_DEFAULT_MAX_FILES = 50
_DEFAULT_MAX_FILE_BYTES = 512 * 1024 * 1024
_DEFAULT_MAX_TOTAL_BYTES = 1024 * 1024 * 1024
_DEFAULT_MAX_LINE_BYTES = 4 * 1024 * 1024
_COPY_CHUNK_BYTES = 1024 * 1024


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class DataUploadError(ValueError):
    """Safe upload/import failure exposed to the HTML and JSON routes."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class StagedUpload:
    file_id: str
    original_name: str
    published_name: str
    staged_name: str
    size_bytes: int
    content_sha256: str


class DataUploadService:
    """Stage uploads, import them transactionally, and request existing analysis."""

    def __init__(
        self,
        *,
        database: Path | str,
        staging_root: Path | str,
        published_root: Path | str,
        runtime_manager: object | None = None,
        max_files: int = _DEFAULT_MAX_FILES,
        max_file_bytes: int = _DEFAULT_MAX_FILE_BYTES,
        max_total_bytes: int = _DEFAULT_MAX_TOTAL_BYTES,
        max_line_bytes: int = _DEFAULT_MAX_LINE_BYTES,
    ) -> None:
        self.database = Path(database)
        self.staging_root = Path(staging_root)
        self.published_root = Path(published_root)
        self.runtime_manager = runtime_manager or get_runtime_manager()
        self.max_files = max(1, int(max_files))
        self.max_file_bytes = max(1, int(max_file_bytes))
        self.max_total_bytes = max(1, int(max_total_bytes))
        self.max_line_bytes = max(1, int(max_line_bytes))
        self._analysis_lock = threading.Lock()
        self._import_lock = threading.Lock()
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.staging_root.mkdir(parents=True, exist_ok=True)
        self.published_root.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        interrupted: tuple[str, ...] = ()
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS data_upload_batches (
                    batch_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    file_count INTEGER NOT NULL CHECK(file_count > 0),
                    total_bytes INTEGER NOT NULL CHECK(total_bytes >= 0),
                    imported_records INTEGER NOT NULL DEFAULT 0 CHECK(imported_records >= 0),
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    published_path TEXT,
                    error_code TEXT,
                    analysis_state TEXT NOT NULL DEFAULT 'not-requested',
                    analysis_requested_at TEXT
                );
                CREATE TABLE IF NOT EXISTS data_upload_files (
                    file_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL REFERENCES data_upload_batches(batch_id),
                    original_name TEXT NOT NULL,
                    published_name TEXT NOT NULL,
                    staged_name TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
                    content_sha256 TEXT NOT NULL,
                    imported_records INTEGER NOT NULL DEFAULT 0 CHECK(imported_records >= 0),
                    status TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS data_upload_files_by_batch
                    ON data_upload_files(batch_id, published_name);
                CREATE TABLE IF NOT EXISTS data_upload_records (
                    batch_id TEXT NOT NULL REFERENCES data_upload_batches(batch_id),
                    file_id TEXT NOT NULL REFERENCES data_upload_files(file_id),
                    line_number INTEGER NOT NULL CHECK(line_number > 0),
                    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
                    PRIMARY KEY(batch_id, file_id, line_number)
                );
                CREATE INDEX IF NOT EXISTS data_upload_records_by_batch
                    ON data_upload_records(batch_id, file_id, line_number);
                """
            )
            interrupted = tuple(
                str(row["batch_id"])
                for row in connection.execute(
                    """
                    SELECT batch_id FROM data_upload_batches
                    WHERE status IN ('queued', 'importing', 'publishing')
                    """
                ).fetchall()
            )
            connection.execute(
                """
                UPDATE data_upload_batches
                SET status='failed', error_code='import-interrupted', completed_at=?
                WHERE status IN ('queued', 'importing', 'publishing')
                """,
                (_utc_now(),),
            )
        for batch_id in interrupted:
            shutil.rmtree(self.staging_root / batch_id, ignore_errors=True)
            shutil.rmtree(self.published_root / batch_id, ignore_errors=True)

    def enqueue(self, files: Iterable[FileStorage]) -> dict[str, Any]:
        selected = tuple(item for item in files if item and item.filename)
        if not selected:
            raise DataUploadError("upload-files-required", "Choose at least one JSONL file.")
        if len(selected) > self.max_files:
            raise DataUploadError(
                "upload-too-many-files",
                f"A single upload may contain at most {self.max_files} files.",
            )

        batch_id = f"upload-{uuid.uuid4().hex}"
        staging_dir = self.staging_root / batch_id
        staging_dir.mkdir(parents=True, exist_ok=False)
        staged: list[StagedUpload] = []
        total_bytes = 0
        used_names: set[str] = set()
        try:
            for index, upload in enumerate(selected, start=1):
                original_name = Path(str(upload.filename)).name
                if Path(original_name).suffix.casefold() != ".jsonl":
                    raise DataUploadError(
                        "upload-jsonl-only",
                        f"{original_name or 'Selected file'} is not a .jsonl file.",
                    )
                safe_name = secure_filename(original_name)
                if not safe_name or Path(safe_name).suffix.casefold() != ".jsonl":
                    safe_name = f"file-{index}.jsonl"
                published_name = self._unique_name(safe_name, used_names)
                staged_name = f"{index:03d}-{uuid.uuid4().hex}.uploading"
                staged_path = staging_dir / staged_name
                digest = hashlib.sha256()
                file_bytes = 0
                with staged_path.open("xb") as target:
                    while True:
                        chunk = upload.stream.read(_COPY_CHUNK_BYTES)
                        if not chunk:
                            break
                        file_bytes += len(chunk)
                        total_bytes += len(chunk)
                        if file_bytes > self.max_file_bytes:
                            raise DataUploadError(
                                "upload-file-too-large",
                                f"{original_name} exceeds the per-file upload limit.",
                            )
                        if total_bytes > self.max_total_bytes:
                            raise DataUploadError(
                                "upload-batch-too-large",
                                "The selected files exceed the total upload limit.",
                            )
                        digest.update(chunk)
                        target.write(chunk)
                    target.flush()
                    os.fsync(target.fileno())
                staged.append(
                    StagedUpload(
                        file_id=f"file-{uuid.uuid4().hex}",
                        original_name=original_name,
                        published_name=published_name,
                        staged_name=staged_name,
                        size_bytes=file_bytes,
                        content_sha256=digest.hexdigest(),
                    )
                )
        except BaseException:
            shutil.rmtree(staging_dir, ignore_errors=True)
            raise

        created_at = _utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO data_upload_batches(
                    batch_id,status,file_count,total_bytes,created_at
                ) VALUES(?,?,?,?,?)
                """,
                (batch_id, "queued", len(staged), total_bytes, created_at),
            )
            connection.executemany(
                """
                INSERT INTO data_upload_files(
                    file_id,batch_id,original_name,published_name,staged_name,
                    size_bytes,content_sha256,status
                ) VALUES(?,?,?,?,?,?,?,'staged')
                """,
                [
                    (
                        item.file_id,
                        batch_id,
                        item.original_name,
                        item.published_name,
                        item.staged_name,
                        item.size_bytes,
                        item.content_sha256,
                    )
                    for item in staged
                ],
            )
            connection.commit()

        worker = threading.Thread(
            target=self._import_batch,
            args=(batch_id,),
            name=f"msh-jsonl-import-{batch_id[-8:]}",
            daemon=True,
        )
        worker.start()
        return self.batch(batch_id)

    @staticmethod
    def _unique_name(name: str, used: set[str]) -> str:
        candidate = name
        stem = Path(name).stem
        suffix = Path(name).suffix
        number = 2
        while candidate.casefold() in used:
            candidate = f"{stem}-{number}{suffix}"
            number += 1
        used.add(candidate.casefold())
        return candidate

    def _import_batch(self, batch_id: str) -> None:
        with self._import_lock:
            self._import_batch_serialized(batch_id)

    def _import_batch_serialized(self, batch_id: str) -> None:
        staging_dir = self.staging_root / batch_id
        try:
            self._set_batch_state(batch_id, "importing", started_at=_utc_now())
            with self._connect() as connection:
                files = connection.execute(
                    """
                    SELECT * FROM data_upload_files
                    WHERE batch_id=? ORDER BY published_name,file_id
                    """,
                    (batch_id,),
                ).fetchall()
                if not files:
                    raise DataUploadError(
                        "upload-batch-empty",
                        "The upload batch does not contain staged files.",
                    )
                connection.execute("BEGIN IMMEDIATE")
                total_records = 0
                for file_row in files:
                    record_count = self._import_file(
                        connection,
                        batch_id=batch_id,
                        file_row=file_row,
                        staging_dir=staging_dir,
                    )
                    if record_count == 0:
                        raise DataUploadError(
                            "upload-empty-jsonl",
                            f"{file_row['original_name']} contains no JSON objects.",
                        )
                    total_records += record_count
                    connection.execute(
                        """
                        UPDATE data_upload_files
                        SET imported_records=?, status='imported'
                        WHERE file_id=?
                        """,
                        (record_count, file_row["file_id"]),
                    )
                connection.execute(
                    """
                    UPDATE data_upload_batches
                    SET status='publishing', imported_records=?, error_code=NULL
                    WHERE batch_id=?
                    """,
                    (total_records, batch_id),
                )
                connection.commit()

            published_dir = self._publish_batch(batch_id, staging_dir)
            self._set_batch_state(
                batch_id,
                "ready",
                completed_at=_utc_now(),
                published_path=str(published_dir),
            )
        except BaseException as exc:  # noqa: BLE001 - worker must persist a safe terminal state
            code = exc.code if isinstance(exc, DataUploadError) else "upload-import-failed"
            self._set_batch_state(
                batch_id,
                "failed",
                completed_at=_utc_now(),
                error_code=code,
            )
            shutil.rmtree(staging_dir, ignore_errors=True)

    def _import_file(
        self,
        connection: sqlite3.Connection,
        *,
        batch_id: str,
        file_row: sqlite3.Row,
        staging_dir: Path,
    ) -> int:
        source = staging_dir / str(file_row["staged_name"])
        count = 0
        try:
            with source.open("r", encoding="utf-8") as handle:
                for line_number, raw_line in enumerate(handle, start=1):
                    if len(raw_line.encode("utf-8")) > self.max_line_bytes:
                        raise DataUploadError(
                            "upload-line-too-large",
                            f"{file_row['original_name']} contains a line that is too large.",
                        )
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        value = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise DataUploadError(
                            "upload-invalid-json",
                            f"{file_row['original_name']} contains invalid JSON at line {line_number}.",
                        ) from exc
                    if not isinstance(value, dict):
                        raise DataUploadError(
                            "upload-json-object-required",
                            f"{file_row['original_name']} line {line_number} is not a JSON object.",
                        )
                    try:
                        payload = json.dumps(
                            value,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                            allow_nan=False,
                        )
                    except (TypeError, ValueError) as exc:
                        raise DataUploadError(
                            "upload-invalid-json-value",
                            f"{file_row['original_name']} line {line_number} contains an unsupported JSON value.",
                        ) from exc
                    connection.execute(
                        """
                        INSERT INTO data_upload_records(
                            batch_id,file_id,line_number,payload_json
                        ) VALUES(?,?,?,?)
                        """,
                        (batch_id, file_row["file_id"], line_number, payload),
                    )
                    count += 1
        except UnicodeDecodeError as exc:
            raise DataUploadError(
                "upload-not-utf8",
                f"{file_row['original_name']} must use UTF-8 encoding.",
            ) from exc
        return count

    def _publish_batch(self, batch_id: str, staging_dir: Path) -> Path:
        final_dir = self.published_root / batch_id
        if final_dir.exists():
            raise DataUploadError(
                "upload-publish-conflict",
                "The upload publication path already exists.",
            )
        final_dir.mkdir(parents=True, exist_ok=False)
        marker = final_dir / _IMPORT_MARKER
        marker.write_text(batch_id, encoding="utf-8")
        # The marker remains if publication fails, so recursive discovery cannot
        # consume a partially published batch.
        with self._connect() as connection:
            files = connection.execute(
                """
                SELECT staged_name,published_name FROM data_upload_files
                WHERE batch_id=? ORDER BY published_name,file_id
                """,
                (batch_id,),
            ).fetchall()
        for item in files:
            os.replace(
                staging_dir / str(item["staged_name"]),
                final_dir / str(item["published_name"]),
            )
        marker.unlink()
        staging_dir.rmdir()
        return final_dir

    def _set_batch_state(
        self,
        batch_id: str,
        status: str,
        *,
        started_at: str | None = None,
        completed_at: str | None = None,
        published_path: str | None = None,
        error_code: str | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE data_upload_batches
                SET status=?,
                    started_at=COALESCE(?,started_at),
                    completed_at=COALESCE(?,completed_at),
                    published_path=COALESCE(?,published_path),
                    error_code=?
                WHERE batch_id=?
                """,
                (
                    status,
                    started_at,
                    completed_at,
                    published_path,
                    error_code,
                    batch_id,
                ),
            )
            if connection.total_changes != 1:
                raise DataUploadError("upload-batch-not-found", "Upload batch not found.")

    def request_analysis(self, batch_id: str) -> dict[str, Any]:
        with self._analysis_lock:
            batch = self.batch(batch_id)
            if batch["status"] != "ready":
                raise DataUploadError(
                    "upload-not-ready",
                    "Wait for the complete upload batch to finish importing.",
                )
            if batch["analysis_state"] == "requested":
                return batch
            request_refresh = getattr(self.runtime_manager, "request_refresh", None)
            if not callable(request_refresh) or not bool(request_refresh()):
                raise DataUploadError(
                    "analysis-busy",
                    "Background analysis is already running or is not ready to start.",
                )
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE data_upload_batches
                    SET analysis_state='requested', analysis_requested_at=?
                    WHERE batch_id=? AND status='ready'
                    """,
                    (_utc_now(), batch_id),
                )
                if connection.total_changes != 1:
                    raise DataUploadError(
                        "upload-analysis-state-changed",
                        "The upload state changed before analysis could be requested.",
                    )
        return self.batch(batch_id)

    def batch(self, batch_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM data_upload_batches WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if row is None:
                raise DataUploadError("upload-batch-not-found", "Upload batch not found.")
            files = connection.execute(
                """
                SELECT original_name,published_name,size_bytes,imported_records,status
                FROM data_upload_files WHERE batch_id=?
                ORDER BY published_name,file_id
                """,
                (batch_id,),
            ).fetchall()
        return {
            "batch_id": str(row["batch_id"]),
            "status": str(row["status"]),
            "file_count": int(row["file_count"]),
            "total_bytes": int(row["total_bytes"]),
            "imported_records": int(row["imported_records"]),
            "created_at": str(row["created_at"]),
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "published_path": row["published_path"],
            "error_code": row["error_code"],
            "analysis_state": str(row["analysis_state"]),
            "analysis_requested_at": row["analysis_requested_at"],
            "ready_for_analysis": (
                row["status"] == "ready" and row["analysis_state"] != "requested"
            ),
            "files": [dict(item) for item in files],
        }

    def list_batches(self, *, limit: int = 20) -> tuple[dict[str, Any], ...]:
        bounded = max(1, min(int(limit), 100))
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT batch_id FROM data_upload_batches
                ORDER BY created_at DESC,batch_id DESC LIMIT ?
                """,
                (bounded,),
            ).fetchall()
        return tuple(self.batch(str(row["batch_id"])) for row in rows)


def _configured_path(key: str, default: str) -> Path:
    value = Path(str(current_app.config.get(key, default)))
    return value if value.is_absolute() else repo_root() / value


def get_data_upload_service() -> DataUploadService:
    existing = current_app.extensions.get("data_upload_service")
    if isinstance(existing, DataUploadService):
        return existing
    service = DataUploadService(
        database=_configured_path(
            "DATA_UPLOAD_DATABASE",
            "data/imports/uploads.sqlite3",
        ),
        staging_root=_configured_path(
            "DATA_UPLOAD_STAGING_DIRECTORY",
            "data/imports/staging",
        ),
        published_root=_configured_path(
            "DATA_UPLOAD_PUBLISHED_DIRECTORY",
            "data/uploads",
        ),
        max_files=int(
            current_app.config.get("DATA_UPLOAD_MAX_FILES", _DEFAULT_MAX_FILES)
        ),
        max_file_bytes=int(
            current_app.config.get(
                "DATA_UPLOAD_MAX_FILE_BYTES",
                _DEFAULT_MAX_FILE_BYTES,
            )
        ),
        max_total_bytes=int(
            current_app.config.get(
                "DATA_UPLOAD_MAX_TOTAL_BYTES",
                _DEFAULT_MAX_TOTAL_BYTES,
            )
        ),
        max_line_bytes=int(
            current_app.config.get(
                "DATA_UPLOAD_MAX_LINE_BYTES",
                _DEFAULT_MAX_LINE_BYTES,
            )
        ),
    )
    current_app.extensions["data_upload_service"] = service
    return service


__all__ = [
    "DataUploadError",
    "DataUploadService",
    "get_data_upload_service",
]
