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
import re
import secrets
import sqlite3
import stat
import threading
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import current_app
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from catalog.orchestrator.pipeline import get_runtime_manager
from catalog.runner.script_catalog import repo_root

_IMPORT_MARKER = ".fcp-importing"
_STAGING_OWNER = ".fcp-upload-owner.json"
_STAGING_OWNER_SCHEMA = "fcp-data-upload-staging-v1"
_GENERATED_BATCH_ID = re.compile(r"upload-[0-9a-f]{32}")
_LEGACY_STAGED_NAME = re.compile(r"[0-9]{3}-[0-9a-f]{32}\.uploading")
_SAFE_BATCH_ID = re.compile(r"upload-[A-Za-z0-9](?:[A-Za-z0-9._-]{0,119}[A-Za-z0-9])?")
_SAFE_FILE_NAME = re.compile(r"[A-Za-z0-9._-]{1,255}")
_DEFAULT_MAX_FILES = 50
_DEFAULT_MAX_FILE_BYTES = 512 * 1024 * 1024
_DEFAULT_MAX_TOTAL_BYTES = 1024 * 1024 * 1024
_DEFAULT_MAX_LINE_BYTES = 4 * 1024 * 1024
_COPY_CHUNK_BYTES = 1024 * 1024
_DEFAULT_MAX_PENDING_IMPORTS = 8
_SERVICE_INITIALIZATION_LOCK = threading.Lock()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _fsync_directory(path: Path) -> None:
    """Persist directory entries where the host exposes directory fsync."""

    # CPython cannot open directory handles with os.open on Windows. File data
    # is still flushed individually there; supported POSIX directory fsync must
    # fail closed so ENOSPC/EIO cannot be mistaken for durable ordering.
    if os.name == "nt":
        return
    descriptor = os.open(
        str(path),
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _is_reparse_point(metadata: os.stat_result) -> bool:
    return bool(
        getattr(metadata, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


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
        max_pending_imports: int = _DEFAULT_MAX_PENDING_IMPORTS,
    ) -> None:
        self.database = Path(database)
        self.staging_root = Path(staging_root)
        self.published_root = Path(published_root)
        self.runtime_manager = runtime_manager or get_runtime_manager()
        self.max_files = max(1, int(max_files))
        self.max_file_bytes = max(1, int(max_file_bytes))
        self.max_total_bytes = max(1, int(max_total_bytes))
        self.max_line_bytes = max(1, int(max_line_bytes))
        self.max_pending_imports = max(1, int(max_pending_imports))
        self._analysis_lock = threading.Lock()
        self._import_lock = threading.Lock()
        self._import_scheduler_lock = threading.Lock()
        self._active_imports: set[str] = set()
        self._import_slots = threading.BoundedSemaphore(self.max_pending_imports)
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

    def _batch_record_exists(self, batch_id: str) -> bool | None:
        """Return None when absence cannot be established safely."""

        try:
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT 1 FROM data_upload_batches WHERE batch_id=?",
                    (batch_id,),
                ).fetchone()
        except Exception:  # noqa: BLE001 - uncertainty must preserve durable staging
            return None
        return row is not None

    def _initialize(self) -> None:
        batches: tuple[tuple[str, str], ...] = ()
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript("""
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
                """)
            batches = tuple(
                (str(row["batch_id"]), str(row["status"]))
                for row in connection.execute(
                    "SELECT batch_id,status FROM data_upload_batches"
                ).fetchall()
            )

        known_batch_ids = {batch_id for batch_id, _status in batches}
        self._reconcile_orphan_staging(known_batch_ids)
        queued = False
        for batch_id, status in batches:
            try:
                if self._reconcile_batch_on_startup(batch_id, status):
                    queued = True
            except DataUploadError as exc:
                self._set_batch_state(
                    batch_id,
                    "failed",
                    completed_at=_utc_now(),
                    error_code=exc.code,
                )
        if queued:
            self._drain_queued_imports()

    @staticmethod
    def _safe_component(batch_id: str) -> bool:
        return _SAFE_BATCH_ID.fullmatch(batch_id) is not None

    @staticmethod
    def _safe_file_name(name: str) -> bool:
        return bool(
            _SAFE_FILE_NAME.fullmatch(name)
            and name not in {".", "..", _IMPORT_MARKER, _STAGING_OWNER}
        )

    def _batch_directory(self, root: Path, batch_id: str) -> Path:
        if not self._safe_component(batch_id):
            raise DataUploadError(
                "upload-path-invalid",
                "The upload batch storage path is not safe.",
            )
        candidate = root / batch_id
        try:
            root_resolved = root.resolve()
            candidate_resolved = candidate.resolve(strict=False)
            try:
                metadata = candidate.lstat()
            except FileNotFoundError:
                metadata = None
        except OSError as exc:
            raise DataUploadError(
                "upload-path-invalid",
                "The upload batch storage path could not be verified safely.",
            ) from exc
        if candidate_resolved.parent != root_resolved or (
            metadata is not None
            and (stat.S_ISLNK(metadata.st_mode) or _is_reparse_point(metadata))
        ):
            raise DataUploadError(
                "upload-path-invalid",
                "The upload batch storage path is not confined to upload storage.",
            )
        return candidate

    def _write_staging_owner(self, staging_dir: Path, batch_id: str) -> None:
        owner = staging_dir / _STAGING_OWNER
        payload = json.dumps(
            {"batch_id": batch_id, "schema": _STAGING_OWNER_SCHEMA},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        with owner.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(staging_dir)

    def _staging_owner_valid(
        self,
        staging_dir: Path,
        batch_id: str,
        *,
        missing_ok: bool = False,
    ) -> bool:
        owner = staging_dir / _STAGING_OWNER
        try:
            owner_metadata = owner.lstat()
        except FileNotFoundError:
            return missing_ok
        except OSError:
            return False
        if (
            not stat.S_ISREG(owner_metadata.st_mode)
            or stat.S_ISLNK(owner_metadata.st_mode)
            or _is_reparse_point(owner_metadata)
            or owner_metadata.st_size > 512
        ):
            return False
        try:
            with owner.open("rb") as handle:
                encoded = handle.read(513)
            if len(encoded) > 512:
                return False
            value = json.loads(encoded.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return False
        return value == {
            "batch_id": batch_id,
            "schema": _STAGING_OWNER_SCHEMA,
        }

    def _owned_orphan(self, candidate: Path) -> bool:
        try:
            metadata = candidate.lstat()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or _is_reparse_point(metadata)
                or _GENERATED_BATCH_ID.fullmatch(candidate.name) is None
                or candidate.resolve().parent != self.staging_root.resolve()
            ):
                return False
            if not self._staging_owner_valid(candidate, candidate.name):
                return False
            file_count = 0
            total_bytes = 0
            for entry in candidate.iterdir():
                if entry.name == _STAGING_OWNER:
                    continue
                file_count += 1
                if file_count > self.max_files:
                    return False
                entry_metadata = entry.lstat()
                total_bytes += entry_metadata.st_size
                if (
                    _LEGACY_STAGED_NAME.fullmatch(entry.name) is None
                    or not stat.S_ISREG(entry_metadata.st_mode)
                    or stat.S_ISLNK(entry_metadata.st_mode)
                    or _is_reparse_point(entry_metadata)
                    or entry_metadata.st_size > self.max_file_bytes
                    or total_bytes > self.max_total_bytes
                ):
                    return False
        except OSError:
            return False
        return True

    def _legacy_owned_orphan(self, candidate: Path) -> bool:
        """Recognize the exact pre-ownership-record staging layout."""

        try:
            metadata = candidate.lstat()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or _is_reparse_point(metadata)
                or _GENERATED_BATCH_ID.fullmatch(candidate.name) is None
                or candidate.resolve().parent != self.staging_root.resolve()
            ):
                return False
            try:
                (candidate / _STAGING_OWNER).lstat()
            except FileNotFoundError:
                pass
            else:
                return False
            file_count = 0
            total_bytes = 0
            for entry in candidate.iterdir():
                file_count += 1
                if file_count > self.max_files:
                    return False
                entry_metadata = entry.lstat()
                total_bytes += entry_metadata.st_size
                if (
                    _LEGACY_STAGED_NAME.fullmatch(entry.name) is None
                    or not stat.S_ISREG(entry_metadata.st_mode)
                    or stat.S_ISLNK(entry_metadata.st_mode)
                    or _is_reparse_point(entry_metadata)
                    or entry_metadata.st_size > self.max_file_bytes
                    or total_bytes > self.max_total_bytes
                ):
                    return False
        except OSError:
            return False
        return file_count > 0

    def _remove_proven_orphan(self, candidate: Path) -> bool:
        """Remove only bounded, regular entries; never recurse through a path."""

        entries: list[Path] = []
        file_count = 0
        try:
            for entry in candidate.iterdir():
                metadata = entry.lstat()
                if entry.name == _STAGING_OWNER:
                    if not self._staging_owner_valid(candidate, candidate.name):
                        return False
                else:
                    file_count += 1
                    if file_count > self.max_files or (
                        _LEGACY_STAGED_NAME.fullmatch(entry.name) is None
                        or not stat.S_ISREG(metadata.st_mode)
                        or stat.S_ISLNK(metadata.st_mode)
                        or _is_reparse_point(metadata)
                    ):
                        return False
                entries.append(entry)
            for entry in entries:
                entry.unlink()
            candidate.rmdir()
            _fsync_directory(self.staging_root)
        except OSError:
            return False
        return True

    def _cleanup_pre_database_staging(self, staging_dir: Path) -> None:
        if self._owned_orphan(staging_dir):
            self._remove_proven_orphan(staging_dir)
            return
        # Before the ownership record exists, only an empty directory is safe
        # to remove. A concurrent or foreign entry makes rmdir fail closed.
        try:
            staging_dir.rmdir()
            _fsync_directory(self.staging_root)
        except OSError:
            pass

    def _reconcile_orphan_staging(self, known_batch_ids: set[str]) -> None:
        """Remove only generated staging proven to be owned and absent from SQLite."""

        try:
            for candidate in self.staging_root.iterdir():
                if candidate.name in known_batch_ids:
                    continue
                if self._owned_orphan(candidate) or self._legacy_owned_orphan(candidate):
                    self._remove_proven_orphan(candidate)
                    continue
                # A crash between mkdir and the ownership record can leave only an
                # empty generated directory. rmdir cannot remove unrelated content.
                if _GENERATED_BATCH_ID.fullmatch(candidate.name):
                    try:
                        metadata = candidate.lstat()
                        if (
                            not stat.S_ISDIR(metadata.st_mode)
                            or stat.S_ISLNK(metadata.st_mode)
                            or _is_reparse_point(metadata)
                            or candidate.resolve().parent
                            != self.staging_root.resolve()
                        ):
                            continue
                        candidate.rmdir()
                        _fsync_directory(self.staging_root)
                    except OSError:
                        pass
        except OSError:
            return

    def _reconcile_batch_on_startup(self, batch_id: str, status: str) -> bool:
        staging_dir = self._batch_directory(self.staging_root, batch_id)
        final_dir = self._batch_directory(self.published_root, batch_id)
        if status == "ready" and not self._import_marker_present(final_dir, batch_id):
            if staging_dir.exists():
                try:
                    self._cleanup_staging(
                        staging_dir,
                        self._publication_files(batch_id),
                    )
                except DataUploadError:
                    # The committed publication remains valid. Ambiguous
                    # staging is retained rather than deleting unknown data.
                    pass
            return False

        active = {"queued", "importing", "publishing"}
        if status == "failed" and not final_dir.exists() and not staging_dir.exists():
            return False
        if status not in active | {"ready", "failed"}:
            if final_dir.exists():
                self._prepare_publication_recovery(batch_id)
            return False

        files = self._publication_files(batch_id)
        complete = self._publication_complete(final_dir, files)

        if status == "ready":
            if complete:
                self._cleanup_staging(staging_dir, files)
                self._reveal_publication(batch_id, final_dir, files)
                return False
            self._prepare_publication_recovery(batch_id)
            self._queue_batch_for_recovery(batch_id)
            return True

        if status in active:
            self._prepare_publication_recovery(batch_id)
            self._queue_batch_for_recovery(batch_id)
            return True

        if (
            status == "failed"
            and files
            and all(str(item["status"]) == "imported" for item in files)
        ):
            self._prepare_publication_recovery(batch_id)
            if complete:
                self._cleanup_staging(staging_dir, files)
                self._set_batch_state(
                    batch_id,
                    "ready",
                    completed_at=_utc_now(),
                    published_path=str(final_dir),
                )
                self._reveal_publication(batch_id, final_dir, files)
                return False
            if self._publication_recoverable(final_dir, staging_dir, files):
                self._queue_batch_for_recovery(batch_id)
                return True

        if final_dir.exists():
            self._prepare_publication_recovery(batch_id)
        return False

    def enqueue(self, files: Iterable[FileStorage]) -> dict[str, Any]:
        selected = tuple(item for item in files if item and item.filename)
        if not selected:
            raise DataUploadError(
                "upload-files-required", "Choose at least one JSONL file."
            )
        if len(selected) > self.max_files:
            raise DataUploadError(
                "upload-too-many-files",
                f"A single upload may contain at most {self.max_files} files.",
            )
        if not self._import_slots.acquire(blocking=False):
            raise DataUploadError(
                "upload-queue-full",
                "The upload import queue is full. Wait for an active import to finish.",
            )

        batch_id = f"upload-{uuid.uuid4().hex}"
        staging_dir = self._batch_directory(self.staging_root, batch_id)
        staged: list[StagedUpload] = []
        total_bytes = 0
        used_names: set[str] = set()
        try:
            staging_dir.mkdir(parents=True, exist_ok=False)
            _fsync_directory(self.staging_root)
            self._write_staging_owner(staging_dir, batch_id)
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
            _fsync_directory(staging_dir)
        except BaseException:
            self._cleanup_pre_database_staging(staging_dir)
            self._import_slots.release()
            raise

        created_at = _utc_now()
        try:
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
        except BaseException:
            # SQLite may report an I/O error after a commit became durable. Only
            # a fresh positive read adopts that durable row; absence permits
            # cleanup, while an unreadable result preserves staging and reraises.
            durable_state = self._batch_record_exists(batch_id)
            if durable_state is True:
                self._start_import(batch_id, slot_acquired=True)
                return self.batch(batch_id)
            if durable_state is False:
                self._cleanup_pre_database_staging(staging_dir)
            self._import_slots.release()
            raise

        self._start_import(batch_id, slot_acquired=True)
        return self.batch(batch_id)

    def _start_import(self, batch_id: str, *, slot_acquired: bool = False) -> bool:
        """Start one queued import, retaining it durably when capacity is full."""
        with self._import_scheduler_lock:
            if batch_id in self._active_imports:
                return True
            if not slot_acquired and not self._import_slots.acquire(blocking=False):
                return False
            self._active_imports.add(batch_id)
        worker = threading.Thread(
            target=self._import_batch_with_slot,
            args=(batch_id,),
            name=f"fcp-jsonl-import-{batch_id[-8:]}",
            daemon=True,
        )
        try:
            worker.start()
        except BaseException:
            with self._import_scheduler_lock:
                self._active_imports.discard(batch_id)
            self._import_slots.release()
            raise
        return True

    def _drain_queued_imports(self) -> None:
        """Fill available worker slots from the durable FIFO queue."""
        with self._connect() as connection:
            queued = tuple(
                str(row["batch_id"])
                for row in connection.execute(
                    """
                    SELECT batch_id FROM data_upload_batches
                    WHERE status='queued' ORDER BY created_at,batch_id
                    """
                ).fetchall()
            )
        for batch_id in queued:
            if not self._start_import(batch_id):
                break

    def _import_batch_with_slot(self, batch_id: str) -> None:
        try:
            self._import_batch(batch_id)
        finally:
            with self._import_scheduler_lock:
                self._active_imports.discard(batch_id)
            self._import_slots.release()
            self._drain_queued_imports()

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
        staging_dir: Path | None = None
        publication_committed = False
        ready_committed = False
        try:
            staging_dir = self._batch_directory(self.staging_root, batch_id)
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
                    if file_row["status"] == "imported":
                        record_count = int(file_row["imported_records"])
                    else:
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
                publication_committed = True

            published_dir = self._publish_batch(batch_id, staging_dir)
            self._set_batch_state(
                batch_id,
                "ready",
                completed_at=_utc_now(),
                published_path=str(published_dir),
            )
            ready_committed = True
            self._reveal_publication(
                batch_id,
                published_dir,
                self._publication_files(batch_id),
            )
        except Exception as exc:  # noqa: BLE001 - worker must persist a safe terminal state
            if ready_committed:
                # Ready is already durable. Marker removal is idempotent and a
                # restart will retry it; never rewrite the DB to failed after
                # the publication commit has crossed that boundary.
                return
            files_for_cleanup: tuple[sqlite3.Row, ...] | None = None
            if not publication_committed:
                try:
                    files_for_cleanup = self._publication_files(batch_id)
                except (DataUploadError, sqlite3.Error, OSError):
                    pass
                else:
                    # A commit can become durable before SQLite reports its
                    # result. Imported rows prove the staged source is still
                    # required for publication/restart recovery.
                    publication_committed = bool(files_for_cleanup) and all(
                        str(item["status"]) == "imported"
                        for item in files_for_cleanup
                    )
            code = (
                exc.code if isinstance(exc, DataUploadError) else "upload-import-failed"
            )
            self._set_batch_state(
                batch_id,
                "failed",
                completed_at=_utc_now(),
                error_code=code,
            )
            if (
                not publication_committed
                and staging_dir is not None
                and files_for_cleanup is not None
            ):
                try:
                    self._cleanup_staging(
                        staging_dir,
                        files_for_cleanup,
                    )
                except DataUploadError:
                    # Ambiguous content is evidence, not upload-owned cleanup.
                    pass

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
                        json.dumps(
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
                    # Validation is intentionally streaming-only.  The staged
                    # JSONL is the durable source of truth; duplicating every
                    # payload in SQLite caused roughly 2x temporary disk use and
                    # held a write transaction for the duration of huge uploads.
                    count += 1
        except UnicodeDecodeError as exc:
            raise DataUploadError(
                "upload-not-utf8",
                f"{file_row['original_name']} must use UTF-8 encoding.",
            ) from exc
        return count

    def _publish_batch(self, batch_id: str, staging_dir: Path) -> Path:
        final_dir = self._batch_directory(self.published_root, batch_id)
        files = self._publication_files(batch_id)
        final_dir.mkdir(parents=True, exist_ok=True)
        _fsync_directory(self.published_root)
        self._write_import_marker(final_dir, batch_id)
        if self._publication_complete(final_dir, files):
            self._cleanup_staging(staging_dir, files)
            return final_dir

        # The marker remains if publication fails, so recursive discovery cannot
        # consume a partially published batch.
        for item in files:
            source = staging_dir / str(item["staged_name"])
            destination = final_dir / str(item["published_name"])
            if self._file_matches(destination, item):
                continue
            elif self._file_matches(source, item):
                os.replace(source, destination)
            else:
                raise DataUploadError(
                    "upload-publish-incomplete",
                    f"{item['published_name']} is missing or does not match the staged upload.",
                )
        _fsync_directory(final_dir)
        if staging_dir.exists():
            _fsync_directory(staging_dir)
        if not self._publication_complete(final_dir, files):
            raise DataUploadError(
                "upload-publish-incomplete",
                "The published upload could not be verified safely.",
            )
        self._cleanup_staging(staging_dir, files)
        return final_dir

    def _publication_files(self, batch_id: str) -> tuple[sqlite3.Row, ...]:
        with self._connect() as connection:
            files = tuple(
                connection.execute(
                    """
                    SELECT staged_name,published_name,size_bytes,content_sha256,status
                    FROM data_upload_files
                    WHERE batch_id=? ORDER BY published_name,file_id
                    """,
                    (batch_id,),
                ).fetchall()
            )
        published_names: set[str] = set()
        staged_names: set[str] = set()
        for item in files:
            published_name = str(item["published_name"])
            staged_name = str(item["staged_name"])
            if (
                not self._safe_file_name(published_name)
                or Path(published_name).suffix.casefold() != ".jsonl"
                or not self._safe_file_name(staged_name)
                or not staged_name.endswith(".uploading")
                or published_name.casefold() in published_names
                or staged_name.casefold() in staged_names
            ):
                raise DataUploadError(
                    "upload-path-invalid",
                    "The upload file storage path is not confined safely.",
                )
            published_names.add(published_name.casefold())
            staged_names.add(staged_name.casefold())
        return files

    @staticmethod
    def _file_matches(path: Path, item: sqlite3.Row) -> bool:
        try:
            metadata = path.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or _is_reparse_point(metadata)
                or metadata.st_size != int(item["size_bytes"])
            ):
                return False
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(_COPY_CHUNK_BYTES), b""):
                    digest.update(chunk)
        except OSError:
            return False
        return secrets.compare_digest(digest.hexdigest(), str(item["content_sha256"]))

    def _publication_complete(
        self, final_dir: Path, files: tuple[sqlite3.Row, ...]
    ) -> bool:
        if not files or not final_dir.is_dir():
            return False
        expected_names = {str(item["published_name"]) for item in files}
        if len(expected_names) != len(files):
            return False
        try:
            actual_names = {
                path.name for path in final_dir.iterdir() if path.name != _IMPORT_MARKER
            }
        except OSError:
            return False
        return actual_names == expected_names and all(
            self._file_matches(final_dir / str(item["published_name"]), item)
            for item in files
        )

    def _publication_recoverable(
        self,
        final_dir: Path,
        staging_dir: Path,
        files: tuple[sqlite3.Row, ...],
    ) -> bool:
        if not files:
            return False
        expected_names = {str(item["published_name"]) for item in files}
        if len(expected_names) != len(files):
            return False
        if final_dir.exists():
            try:
                actual_names = {
                    path.name
                    for path in final_dir.iterdir()
                    if path.name != _IMPORT_MARKER
                }
            except OSError:
                return False
            if not actual_names.issubset(expected_names):
                return False
        if not self._staging_contains_only_owned_entries(staging_dir, files):
            return False
        return all(
            self._file_matches(final_dir / str(item["published_name"]), item)
            or self._file_matches(staging_dir / str(item["staged_name"]), item)
            for item in files
        )

    def _staging_contains_only_owned_entries(
        self,
        staging_dir: Path,
        files: tuple[sqlite3.Row, ...],
    ) -> bool:
        try:
            directory_metadata = staging_dir.lstat()
        except FileNotFoundError:
            return True
        except OSError:
            return False
        if (
            not stat.S_ISDIR(directory_metadata.st_mode)
            or stat.S_ISLNK(directory_metadata.st_mode)
            or _is_reparse_point(directory_metadata)
            or not self._safe_component(staging_dir.name)
        ):
            return False
        try:
            if staging_dir.resolve().parent != self.staging_root.resolve():
                return False
            owner = staging_dir / _STAGING_OWNER
            try:
                owner.lstat()
            except FileNotFoundError:
                pass  # Supported recovery for staging created before owner records.
            else:
                if not self._staging_owner_valid(staging_dir, staging_dir.name):
                    return False
            by_name = {str(item["staged_name"]): item for item in files}
            if len(by_name) != len(files):
                return False
            entry_count = 0
            for path in staging_dir.iterdir():
                entry_count += 1
                if entry_count > len(files) + 1:
                    return False
                if path.name == _STAGING_OWNER:
                    continue
                item = by_name.get(path.name)
                if item is None or not self._file_matches(path, item):
                    return False
        except OSError:
            return False
        return True

    def _cleanup_staging(
        self,
        staging_dir: Path,
        files: tuple[sqlite3.Row, ...],
    ) -> None:
        try:
            staging_dir.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise DataUploadError(
                "upload-staging-ambiguous",
                "The upload staging path could not be verified safely.",
            ) from exc
        if not self._staging_contains_only_owned_entries(staging_dir, files):
            raise DataUploadError(
                "upload-staging-ambiguous",
                "Unexpected content in upload staging was preserved for review.",
            )
        try:
            by_name = {str(item["staged_name"]): item for item in files}
            for entry_count, entry in enumerate(staging_dir.iterdir(), start=1):
                if entry_count > len(files) + 1:
                    raise DataUploadError(
                        "upload-staging-ambiguous",
                        "Unexpected content in upload staging was preserved for review.",
                    )
                if entry.name == _STAGING_OWNER:
                    if not self._staging_owner_valid(staging_dir, staging_dir.name):
                        raise DataUploadError(
                            "upload-staging-ambiguous",
                            "The upload staging owner record was preserved for review.",
                        )
                else:
                    item = by_name.get(entry.name)
                    if item is None or not self._file_matches(entry, item):
                        raise DataUploadError(
                            "upload-staging-ambiguous",
                            "Unexpected content in upload staging was preserved for review.",
                        )
                entry.unlink()
            staging_dir.rmdir()
            _fsync_directory(self.staging_root)
        except DataUploadError:
            raise
        except OSError:
            # A complete verified publication does not become unsafe merely
            # because a redundant owned staging copy is temporarily locked.
            pass

    def _import_marker_present(self, final_dir: Path, batch_id: str) -> bool:
        marker = final_dir / _IMPORT_MARKER
        expected = batch_id.encode("utf-8")
        try:
            metadata = marker.lstat()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise DataUploadError(
                "upload-publish-marker-invalid",
                "The upload publication marker could not be verified safely.",
            ) from exc
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or _is_reparse_point(metadata)
            or metadata.st_size != len(expected)
        ):
            raise DataUploadError(
                "upload-publish-marker-invalid",
                "The upload publication marker is not the owned batch marker.",
            )
        try:
            with marker.open("rb") as handle:
                actual = handle.read(len(expected) + 1)
        except OSError as exc:
            raise DataUploadError(
                "upload-publish-marker-invalid",
                "The upload publication marker could not be read safely.",
            ) from exc
        if actual != expected:
            raise DataUploadError(
                "upload-publish-marker-invalid",
                "The upload publication marker belongs to a different batch.",
            )
        return True

    def _write_import_marker(self, final_dir: Path, batch_id: str) -> None:
        marker = final_dir / _IMPORT_MARKER
        if self._import_marker_present(final_dir, batch_id):
            return
        try:
            with marker.open("xb") as handle:
                handle.write(batch_id.encode("utf-8"))
                handle.flush()
                os.fsync(handle.fileno())
            _fsync_directory(final_dir)
        except OSError as exc:
            raise DataUploadError(
                "upload-publish-marker-failed",
                "The upload could not be hidden safely before publication.",
            ) from exc

    def _reveal_publication(
        self,
        batch_id: str,
        final_dir: Path,
        files: tuple[sqlite3.Row, ...],
    ) -> None:
        if not self._publication_complete(final_dir, files):
            raise DataUploadError(
                "upload-publish-incomplete",
                "The complete upload could not be verified before publication.",
            )
        marker = final_dir / _IMPORT_MARKER
        if not self._import_marker_present(final_dir, batch_id):
            return
        try:
            marker.unlink()
            _fsync_directory(final_dir)
        except OSError as exc:
            try:
                self._write_import_marker(final_dir, batch_id)
            except DataUploadError:
                pass
            raise DataUploadError(
                "upload-publish-marker-failed",
                f"Upload batch {batch_id} could not be made visible safely.",
            ) from exc

    def _prepare_publication_recovery(self, batch_id: str) -> None:
        """Keep every non-ready publication hidden until DB state catches up."""
        final_dir = self._batch_directory(self.published_root, batch_id)
        if not final_dir.exists():
            return
        self._write_import_marker(final_dir, batch_id)

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
                raise DataUploadError(
                    "upload-batch-not-found", "Upload batch not found."
                )

    def _queue_batch_for_recovery(self, batch_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE data_upload_batches
                SET status='queued', started_at=NULL, completed_at=NULL,
                    published_path=NULL, error_code=NULL
                WHERE batch_id=?
                """,
                (batch_id,),
            )
            if connection.total_changes != 1:
                raise DataUploadError(
                    "upload-batch-not-found", "Upload batch not found."
                )

    def request_analysis(
        self, batch_id: str, *, execution_id: str | None = None
    ) -> dict[str, Any]:
        with self._analysis_lock:
            batch = self.batch(batch_id)
            if not batch["ready_for_analysis"]:
                raise DataUploadError(
                    "upload-not-ready",
                    "Wait for the complete upload batch to finish importing.",
                )
            if batch["analysis_state"] == "requested":
                return batch
            request_refresh = getattr(self.runtime_manager, "request_refresh", None)
            if not callable(request_refresh) or not bool(
                request_refresh(execution_id=execution_id)
            ):
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

    def _publication_visible(
        self,
        batch_id: str,
        published_path: object,
    ) -> bool:
        if not published_path:
            return False
        try:
            final_dir = self._batch_directory(self.published_root, batch_id)
            final_resolved = final_dir.resolve()
            recorded = Path(str(published_path)).resolve()
        except (DataUploadError, OSError):
            return False
        try:
            (final_dir / _IMPORT_MARKER).lstat()
        except FileNotFoundError:
            marker_absent = True
        except OSError:
            return False
        else:
            marker_absent = False
        return bool(
            recorded == final_resolved
            and final_dir.is_dir()
            and marker_absent
        )

    def batch(self, batch_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM data_upload_batches WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if row is None:
                raise DataUploadError(
                    "upload-batch-not-found", "Upload batch not found."
                )
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
                row["status"] == "ready"
                and row["analysis_state"] != "requested"
                and self._publication_visible(batch_id, row["published_path"])
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
    with _SERVICE_INITIALIZATION_LOCK:
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
