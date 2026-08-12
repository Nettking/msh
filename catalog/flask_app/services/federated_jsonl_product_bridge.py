"""Federate legacy JSONL source files without changing legacy analysis scripts.

Local JSONL files are deterministically gzip-compressed, split into bounded
logical-storage batches, and committed through the Federation storage authority.
Remote committed chunks are verified, reconstructed below ``data/federation``
and therefore become ordinary recursive JSONL input to the pre-Federation
runner/orchestrator stack.

The active MTConnect recorder JSONL path is excluded because it already has a
stronger sequence-aware Federation publication/mirror contract.
"""

from __future__ import annotations

import gzip
import hashlib
import os
import sqlite3
import tempfile
import threading
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask

from catalog.common.artifact_refresh import request_artifact_catalog_refresh
from catalog.common.data_loading import iter_jsonl_files
from catalog.federation.errors import FederationOperationError, FederationValidationError
from catalog.federation.shared_file_storage import (
    FEDERATED_JSONL_CHUNK_BYTES,
    FEDERATED_JSONL_CONTENT_SCHEMA,
    FEDERATED_JSONL_DATASET_SCHEMA_NAME,
    FEDERATED_JSONL_DATASET_SCHEMA_VERSION,
    FEDERATED_JSONL_ENCODING,
    encode_federated_jsonl_chunk,
    federated_jsonl_batch_id,
    federated_jsonl_dataset_id,
    federated_jsonl_idempotency_key,
    normalize_jsonl_relative_path,
    validate_federated_jsonl_ingest,
)
from catalog.federation.storage_catalog import CommittedBatchPage, CommittedBatchReference
from catalog.federation.storage_protocol import BatchIngestRequest
from catalog.mtconnect_recorder.federation_node import select_storage_authority
from catalog.orchestrator.pipeline import get_runtime_manager
from catalog.runner.script_catalog import repo_root

_PAGE_SIZE = 20
_DEFAULT_MAX_PAGES = 5
_DEFAULT_MAX_REMOTE_BATCHES = 100
_DEFAULT_MAX_PUBLISH_CHUNKS = 64
_HARD_MAX_PAGES = 20
_HARD_MAX_REMOTE_BATCHES = _PAGE_SIZE * _HARD_MAX_PAGES
_HARD_MAX_PUBLISH_CHUNKS = 128
_STORAGE_GROUP_CONFIG_KEYS = (
    "FEDERATED_JSONL_STORAGE_GROUP_ID",
    "FEDERATED_TELEMETRY_STORAGE_GROUP_ID",
    "FEDERATION_STORAGE_GROUP_ID",
    "FCP_FEDERATION_STORAGE_GROUP",
    "RECORDER_FEDERATION_STORAGE_GROUP_ID",
)
_EXCLUDED_LOCAL_PREFIXES = (
    "federation/",
    "sources/mtconnect_recorder/jsonl/",
)

Clock = Callable[[], datetime]
RefreshCallback = Callable[..., bool]
RuntimeRefreshCallback = Callable[[], bool]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise TypeError("clock must return a timezone-aware value")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _safe_node_directory(node_id: str) -> str:
    return hashlib.sha256(node_id.encode("utf-8")).hexdigest()[:24]


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class FederatedJsonlProductBridge:
    """Publish local JSONL and materialize remote JSONL as one logical corpus."""

    def __init__(
        self,
        app: Flask,
        onboarding_service: object,
        *,
        data_root: str | Path | None = None,
        database: str | Path | None = None,
        cache_root: str | Path | None = None,
        mirror_root: str | Path | None = None,
        artifact_refresh_callback: RefreshCallback | None = None,
        runtime_refresh_callback: RuntimeRefreshCallback | None = None,
        clock: Clock = _utc_now,
    ) -> None:
        self.app = app
        self.onboarding_service = onboarding_service
        root = repo_root()
        self.data_root = Path(
            data_root or app.config.get("FEDERATED_JSONL_DATA_ROOT", root / "data")
        ).resolve()
        self.database = Path(
            database
            or app.config.get(
                "FEDERATED_JSONL_SYNC_DATABASE",
                root / "data" / "federation" / "jsonl-sync.sqlite3",
            )
        ).resolve()
        self.cache_root = Path(
            cache_root
            or app.config.get(
                "FEDERATED_JSONL_CACHE_DIRECTORY",
                root / "data" / "federation" / "jsonl-cache",
            )
        ).resolve()
        self.mirror_root = Path(
            mirror_root
            or app.config.get(
                "FEDERATED_JSONL_MIRROR_DIRECTORY",
                root / "data" / "federation" / "shared" / "jsonl-files",
            )
        ).resolve()
        self.artifact_refresh_callback = artifact_refresh_callback
        self.runtime_refresh_callback = runtime_refresh_callback
        self.clock = clock
        self._sync_lock = threading.Lock()
        self._resume_scope: tuple[str, str, str] | None = None
        self._resume_cursor: str | None = None
        self._resume_manifest: tuple[int, str] | None = None
        self._state_lock = threading.RLock()
        self._init_lock = threading.Lock()
        self._initialized = False
        self._state: dict[str, object] = {
            "status": "not-started",
            "authority": None,
            "group": None,
            "published_chunks": 0,
            "remote_discovered": 0,
            "remote_downloaded": 0,
            "materialized_files": 0,
            "last_error": None,
            "last_sync": None,
        }

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            self.data_root.mkdir(parents=True, exist_ok=True)
            self.database.parent.mkdir(parents=True, exist_ok=True)
            self.cache_root.mkdir(parents=True, exist_ok=True)
            self.mirror_root.mkdir(parents=True, exist_ok=True)
            self._initialize_database()
            self._initialized = True

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize_database(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS local_files (
                    relative_path TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    file_sha256 TEXT NOT NULL,
                    encoded_sha256 TEXT NOT NULL,
                    encoded_size INTEGER NOT NULL,
                    dataset_id TEXT NOT NULL,
                    chunk_count INTEGER NOT NULL,
                    next_chunk INTEGER NOT NULL,
                    cache_path TEXT NOT NULL,
                    published_at TEXT
                );
                CREATE TABLE IF NOT EXISTS seen_batches (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    producer_node_id TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    file_sha256 TEXT NOT NULL,
                    encoded_sha256 TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    encoded_size INTEGER NOT NULL,
                    source_mtime_ns INTEGER NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    chunk_count INTEGER NOT NULL,
                    chunk_sha256 TEXT NOT NULL,
                    chunk_path TEXT,
                    committed_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, group_id, dataset_id, batch_id)
                );
                CREATE INDEX IF NOT EXISTS seen_file_version_idx
                    ON seen_batches(
                        session_id,dataset_id,file_sha256,encoded_sha256,chunk_index
                    );
                CREATE TABLE IF NOT EXISTS materialized_files (
                    session_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    producer_node_id TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    file_sha256 TEXT NOT NULL,
                    source_mtime_ns INTEGER NOT NULL,
                    target_path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id,dataset_id)
                );
                """
            )

    def snapshot(self) -> dict[str, object]:
        with self._state_lock:
            return dict(self._state)

    def _set_state(self, **values: object) -> None:
        with self._state_lock:
            state = dict(self._state)
            state.update(values)
            self._state = state

    def _positive_bound(self, key: str, default: int, hard_limit: int) -> int:
        raw = self.app.config.get(key, default)
        if isinstance(raw, bool):
            raise FederationOperationError(
                "invalid-federated-jsonl-config", f"{key} must be a positive integer"
            )
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise FederationOperationError(
                "invalid-federated-jsonl-config", f"{key} must be a positive integer"
            ) from exc
        if value < 1 or value > hard_limit:
            raise FederationOperationError(
                "invalid-federated-jsonl-config",
                f"{key} must be between 1 and {hard_limit}",
            )
        return value

    def _configured_group(self) -> str | None:
        for key in _STORAGE_GROUP_CONFIG_KEYS:
            configured = self.app.config.get(key)
            if configured is not None and (value := str(configured).strip()):
                return value
        return None

    @staticmethod
    def _context_ids(runtime_state: object, context: object) -> tuple[str, str]:
        binding = getattr(context, "binding", None)
        credentials = getattr(context, "credentials", None)
        identity = getattr(credentials, "identity", None)
        session_id = getattr(binding, "internal_session_id", None)
        node_id = getattr(identity, "node_id", None)
        runtime_binding = getattr(runtime_state, "binding", None)
        runtime_session_id = getattr(runtime_binding, "internal_session_id", None)
        if (
            not isinstance(session_id, str)
            or not session_id
            or not isinstance(node_id, str)
            or not node_id
        ):
            raise FederationOperationError(
                "trusted-federation-context-incomplete",
                "trusted Federation context has no session/device identity",
            )
        if runtime_session_id is not None and runtime_session_id != session_id:
            raise FederationOperationError(
                "federation-session-mismatch",
                "connected relay state does not match the trusted context",
            )
        return session_id, node_id

    def _local_candidates(self) -> Iterable[tuple[str, Path]]:
        for path in iter_jsonl_files(self.data_root, recursive=True):
            try:
                relative = path.resolve().relative_to(self.data_root).as_posix()
            except (OSError, ValueError):
                continue
            if any(relative.startswith(prefix) for prefix in _EXCLUDED_LOCAL_PREFIXES):
                continue
            yield normalize_jsonl_relative_path(relative), path

    def _local_row(self, relative_path: str) -> sqlite3.Row | None:
        with self._connect() as connection:
            return connection.execute(
                "SELECT * FROM local_files WHERE relative_path=?", (relative_path,)
            ).fetchone()

    def _prepare_local_file(
        self, node_id: str, relative_path: str, path: Path
    ) -> sqlite3.Row:
        stat_before = path.stat()
        existing = self._local_row(relative_path)
        if (
            existing is not None
            and int(existing["size_bytes"]) == stat_before.st_size
            and int(existing["mtime_ns"]) == stat_before.st_mtime_ns
        ):
            cache_path = Path(str(existing["cache_path"]))
            if existing["published_at"] is not None or cache_path.is_file():
                return existing

        temporary = tempfile.NamedTemporaryFile(
            prefix="fcp-jsonl-",
            suffix=".jsonl.gz",
            dir=self.cache_root,
            delete=False,
        )
        temp_path = Path(temporary.name)
        file_digest = hashlib.sha256()
        try:
            with temporary:
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=temporary,
                    mtime=0,
                ) as compressed:
                    with path.open("rb") as source:
                        for raw in iter(lambda: source.read(1024 * 1024), b""):
                            file_digest.update(raw)
                            compressed.write(raw)
                temporary.flush()
                os.fsync(temporary.fileno())
            stat_after = path.stat()
            if (
                stat_after.st_size != stat_before.st_size
                or stat_after.st_mtime_ns != stat_before.st_mtime_ns
            ):
                raise FederationOperationError(
                    "federated-jsonl-source-changed",
                    f"{relative_path} changed while it was being prepared",
                )
            file_sha256 = f"sha256:{file_digest.hexdigest()}"
            encoded_sha256 = _sha256_path(temp_path)
            encoded_size = temp_path.stat().st_size
            final_cache = self.cache_root / f"{file_sha256[7:]}.jsonl.gz"
            if final_cache.exists():
                if (
                    final_cache.stat().st_size != encoded_size
                    or _sha256_path(final_cache) != encoded_sha256
                ):
                    raise FederationOperationError(
                        "federated-jsonl-cache-conflict",
                        "content-addressed gzip cache contains different bytes",
                    )
                temp_path.unlink(missing_ok=True)
            else:
                os.replace(temp_path, final_cache)
                _fsync_directory(self.cache_root)
            chunk_count = max(
                1,
                (encoded_size + FEDERATED_JSONL_CHUNK_BYTES - 1)
                // FEDERATED_JSONL_CHUNK_BYTES,
            )
            dataset_id = federated_jsonl_dataset_id(node_id, relative_path)
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO local_files(
                        relative_path,size_bytes,mtime_ns,file_sha256,encoded_sha256,
                        encoded_size,dataset_id,chunk_count,next_chunk,cache_path,published_at
                    ) VALUES(?,?,?,?,?,?,?,?,0,?,NULL)
                    ON CONFLICT(relative_path) DO UPDATE SET
                        size_bytes=excluded.size_bytes,
                        mtime_ns=excluded.mtime_ns,
                        file_sha256=excluded.file_sha256,
                        encoded_sha256=excluded.encoded_sha256,
                        encoded_size=excluded.encoded_size,
                        dataset_id=excluded.dataset_id,
                        chunk_count=excluded.chunk_count,
                        next_chunk=0,
                        cache_path=excluded.cache_path,
                        published_at=NULL
                    """,
                    (
                        relative_path,
                        stat_before.st_size,
                        stat_before.st_mtime_ns,
                        file_sha256,
                        encoded_sha256,
                        encoded_size,
                        dataset_id,
                        chunk_count,
                        str(final_cache),
                    ),
                )
            row = self._local_row(relative_path)
            assert row is not None
            return row
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise

    def _prepare_local_rows(self, node_id: str) -> None:
        for relative_path, path in self._local_candidates():
            try:
                self._prepare_local_file(node_id, relative_path, path)
            except FileNotFoundError:
                continue
            except FederationOperationError as exc:
                if exc.code == "federated-jsonl-source-changed":
                    continue
                raise

    def _pending_publish_entries(
        self,
        *,
        session_id: str,
        node_id: str,
        group_id: str,
        maximum: int,
    ) -> list[tuple[dict[str, Any], str, int]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM local_files
                WHERE published_at IS NULL AND next_chunk < chunk_count
                ORDER BY relative_path
                """
            ).fetchall()
        entries: list[tuple[dict[str, Any], str, int]] = []
        for row in rows:
            if len(entries) >= maximum:
                break
            relative_path = str(row["relative_path"])
            source_path = self.data_root / Path(relative_path)
            try:
                stat = source_path.stat()
            except FileNotFoundError:
                continue
            if (
                stat.st_size != int(row["size_bytes"])
                or stat.st_mtime_ns != int(row["mtime_ns"])
            ):
                continue
            cache_path = Path(str(row["cache_path"]))
            if not cache_path.is_file():
                self._prepare_local_file(node_id, relative_path, source_path)
                row = self._local_row(relative_path)
                assert row is not None
                cache_path = Path(str(row["cache_path"]))
            index = int(row["next_chunk"])
            with cache_path.open("rb") as handle:
                handle.seek(index * FEDERATED_JSONL_CHUNK_BYTES)
                data = handle.read(FEDERATED_JSONL_CHUNK_BYTES)
            chunk_sha256 = f"sha256:{hashlib.sha256(data).hexdigest()}"
            batch_id = federated_jsonl_batch_id(
                str(row["file_sha256"]), index, chunk_sha256
            )
            content = {
                "schema": FEDERATED_JSONL_CONTENT_SCHEMA,
                "producer_node_id": node_id,
                "relative_path": relative_path,
                "encoding": FEDERATED_JSONL_ENCODING,
                "file_size": int(row["size_bytes"]),
                "file_sha256": str(row["file_sha256"]),
                "encoded_size": int(row["encoded_size"]),
                "encoded_sha256": str(row["encoded_sha256"]),
                "source_mtime_ns": int(row["mtime_ns"]),
                "chunk_index": index,
                "chunk_count": int(row["chunk_count"]),
                "chunk_offset": index * FEDERATED_JSONL_CHUNK_BYTES,
                "chunk_sha256": chunk_sha256,
                "data": encode_federated_jsonl_chunk(data),
            }
            dataset_id = str(row["dataset_id"])
            entries.append(
                (
                    {
                        "group_id": group_id,
                        "dataset_id": dataset_id,
                        "batch_id": batch_id,
                        "idempotency_key": federated_jsonl_idempotency_key(
                            session_id, dataset_id, batch_id
                        ),
                        "content": content,
                        "created_at": self.clock(),
                        "dataset_schema_name": FEDERATED_JSONL_DATASET_SCHEMA_NAME,
                        "dataset_schema_version": FEDERATED_JSONL_DATASET_SCHEMA_VERSION,
                    },
                    relative_path,
                    index,
                )
            )
        return entries

    def _advance_published(
        self,
        entries: list[tuple[dict[str, Any], str, int]],
        outcomes: tuple[object, ...],
    ) -> int:
        committed = 0
        now = _stamp(self.clock())
        for entry, outcome in zip(entries, outcomes, strict=False):
            _batch, relative_path, index = entry
            if getattr(outcome, "committed", False) is not True:
                continue
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT next_chunk,chunk_count FROM local_files WHERE relative_path=?",
                    (relative_path,),
                ).fetchone()
                if row is None or int(row["next_chunk"]) != index:
                    continue
                next_chunk = index + 1
                connection.execute(
                    """
                    UPDATE local_files
                    SET next_chunk=?, published_at=?
                    WHERE relative_path=?
                    """,
                    (
                        next_chunk,
                        now if next_chunk >= int(row["chunk_count"]) else None,
                        relative_path,
                    ),
                )
                committed += 1
        return committed

    def _publish_local(
        self,
        runtime_state: object,
        *,
        session_id: str,
        node_id: str,
        authority_node_id: str,
        group_id: str,
    ) -> int:
        self._prepare_local_rows(node_id)
        maximum = self._positive_bound(
            "FEDERATED_JSONL_MAX_PUBLISH_CHUNKS_PER_SYNC",
            _DEFAULT_MAX_PUBLISH_CHUNKS,
            _HARD_MAX_PUBLISH_CHUNKS,
        )
        entries = self._pending_publish_entries(
            session_id=session_id,
            node_id=node_id,
            group_id=group_id,
            maximum=maximum,
        )
        if not entries:
            return 0
        runtime = getattr(self.onboarding_service, "relay_runtime", None)
        publish = getattr(runtime, "publish_federated_batches", None)
        if not callable(publish):
            raise FederationOperationError(
                "federated-jsonl-runtime-unavailable",
                "paired runtime does not expose generic Federation batch publication",
            )
        outcomes = publish(
            runtime_state,
            session_id=session_id,
            authority_node_id=authority_node_id,
            batches=[entry[0] for entry in entries],
        )
        if not isinstance(outcomes, tuple) or len(outcomes) != len(entries):
            raise FederationOperationError(
                "federated-jsonl-publication-response-invalid",
                "generic Federation publication returned an invalid result count",
            )
        return self._advance_published(entries, outcomes)

    @staticmethod
    def _validate_page(
        page: object,
        *,
        session_id: str,
        group_id: str,
        requested_limit: int,
    ) -> CommittedBatchPage:
        if not isinstance(page, CommittedBatchPage):
            raise FederationOperationError(
                "storage-catalog-response-invalid",
                "storage authority returned an invalid committed-batch page",
            )
        if (
            page.session_id != session_id
            or page.group_id != group_id
            or page.dataset_id is not None
            or len(page.batches) > requested_limit
        ):
            raise FederationOperationError(
                "storage-catalog-scope-mismatch",
                "storage authority returned a page outside the requested scope",
            )
        return page

    def _seen(self, reference: CommittedBatchReference) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM seen_batches
                WHERE session_id=? AND group_id=? AND dataset_id=? AND batch_id=?
                """,
                (
                    reference.session_id,
                    reference.group_id,
                    reference.dataset_id,
                    reference.batch_id,
                ),
            ).fetchone()
        return row is not None

    def _chunk_path(self, encoded_sha256: str, chunk_index: int) -> Path:
        directory = self.cache_root / "remote" / encoded_sha256[7:]
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{chunk_index:08d}.chunk"

    def _write_chunk(self, path: Path, data: bytes) -> None:
        if path.exists():
            digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
            if path.stat().st_size == len(data) and _sha256_path(path) == digest:
                return
            raise FederationValidationError(
                "federated-jsonl-staging-conflict",
                "content.chunk_index",
                "staged chunk path already contains different bytes",
            )
        with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, path)
        _fsync_directory(path.parent)

    def _record_remote_chunk(
        self,
        reference: CommittedBatchReference,
        content: Mapping[str, Any],
        decoded: bytes,
        *,
        local_node_id: str,
    ) -> bool:
        producer = str(content["producer_node_id"])
        relative_path = normalize_jsonl_relative_path(content["relative_path"])
        file_sha256 = str(content["file_sha256"])
        encoded_sha256 = str(content["encoded_sha256"])
        chunk_index = int(content["chunk_index"])
        chunk_count = int(content["chunk_count"])
        chunk_sha256 = str(content["chunk_sha256"])
        chunk_path: Path | None = None

        with self._connect() as connection:
            version = connection.execute(
                """
                SELECT producer_node_id,relative_path,file_size,encoded_size,
                       source_mtime_ns,chunk_count
                FROM seen_batches
                WHERE session_id=? AND dataset_id=? AND file_sha256=?
                  AND encoded_sha256=?
                LIMIT 1
                """,
                (
                    reference.session_id,
                    reference.dataset_id,
                    file_sha256,
                    encoded_sha256,
                ),
            ).fetchone()
            if version is not None and (
                str(version["producer_node_id"]) != producer
                or str(version["relative_path"]) != relative_path
                or int(version["file_size"]) != int(content["file_size"])
                or int(version["encoded_size"]) != int(content["encoded_size"])
                or int(version["source_mtime_ns"]) != int(content["source_mtime_ns"])
                or int(version["chunk_count"]) != chunk_count
            ):
                raise FederationValidationError(
                    "federated-jsonl-version-conflict",
                    "content",
                    "one file version contains contradictory chunk metadata",
                )
            conflict = connection.execute(
                """
                SELECT chunk_sha256 FROM seen_batches
                WHERE session_id=? AND dataset_id=? AND file_sha256=?
                  AND encoded_sha256=? AND chunk_index=?
                LIMIT 1
                """,
                (
                    reference.session_id,
                    reference.dataset_id,
                    file_sha256,
                    encoded_sha256,
                    chunk_index,
                ),
            ).fetchone()
            if conflict is not None and str(conflict["chunk_sha256"]) != chunk_sha256:
                raise FederationValidationError(
                    "federated-jsonl-version-conflict",
                    "content.chunk_sha256",
                    "one file version contains contradictory chunk content",
                )

        if producer != local_node_id:
            chunk_path = self._chunk_path(encoded_sha256, chunk_index)
            self._write_chunk(chunk_path, decoded)

        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO seen_batches(
                    session_id,group_id,dataset_id,batch_id,producer_node_id,
                    relative_path,file_sha256,encoded_sha256,file_size,encoded_size,
                    source_mtime_ns,chunk_index,chunk_count,chunk_sha256,chunk_path,
                    committed_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    reference.session_id,
                    reference.group_id,
                    reference.dataset_id,
                    reference.batch_id,
                    producer,
                    relative_path,
                    file_sha256,
                    encoded_sha256,
                    int(content["file_size"]),
                    int(content["encoded_size"]),
                    int(content["source_mtime_ns"]),
                    chunk_index,
                    chunk_count,
                    chunk_sha256,
                    None if chunk_path is None else str(chunk_path),
                    reference.committed_at.isoformat(),
                ),
            )
        if producer == local_node_id:
            return False
        return self._try_materialize(
            session_id=reference.session_id,
            dataset_id=reference.dataset_id,
            file_sha256=file_sha256,
            encoded_sha256=encoded_sha256,
        )

    def _target_path(self, producer: str, relative_path: str) -> Path:
        target = self.mirror_root / _safe_node_directory(producer) / Path(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        resolved_root = self.mirror_root.resolve()
        try:
            target.resolve().relative_to(resolved_root)
        except (OSError, ValueError) as exc:
            raise FederationValidationError(
                "unsafe-federated-jsonl-target",
                "content.relative_path",
                "materialized path escaped the managed mirror root",
            ) from exc
        return target

    def _try_materialize(
        self,
        *,
        session_id: str,
        dataset_id: str,
        file_sha256: str,
        encoded_sha256: str,
    ) -> bool:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM seen_batches
                WHERE session_id=? AND dataset_id=? AND file_sha256=?
                  AND encoded_sha256=?
                ORDER BY chunk_index
                """,
                (session_id, dataset_id, file_sha256, encoded_sha256),
            ).fetchall()
        if not rows:
            return False
        with self._connect() as connection:
            local_duplicate = connection.execute(
                "SELECT 1 FROM local_files WHERE file_sha256=? LIMIT 1",
                (file_sha256,),
            ).fetchone()
            remote_duplicate = connection.execute(
                """SELECT 1 FROM materialized_files
                   WHERE session_id=? AND file_sha256=? AND dataset_id<>? LIMIT 1""",
                (session_id, file_sha256, dataset_id),
            ).fetchone()
        if local_duplicate is not None or remote_duplicate is not None:
            for row in rows:
                if row["chunk_path"]:
                    Path(str(row["chunk_path"])).unlink(missing_ok=True)
            return False
        first = rows[0]
        chunk_count = int(first["chunk_count"])
        if len(rows) != chunk_count:
            return False
        if [int(row["chunk_index"]) for row in rows] != list(range(chunk_count)):
            return False
        if any(
            not row["chunk_path"] or not Path(str(row["chunk_path"])).is_file()
            for row in rows
        ):
            return False

        producer = str(first["producer_node_id"])
        relative_path = str(first["relative_path"])
        source_mtime_ns = int(first["source_mtime_ns"])
        with self._connect() as connection:
            current = connection.execute(
                """
                SELECT file_sha256,source_mtime_ns FROM materialized_files
                WHERE session_id=? AND dataset_id=?
                """,
                (session_id, dataset_id),
            ).fetchone()
        if current is not None:
            current_key = (int(current["source_mtime_ns"]), str(current["file_sha256"]))
            candidate_key = (source_mtime_ns, file_sha256)
            if candidate_key <= current_key:
                return False

        target = self._target_path(producer, relative_path)
        with tempfile.NamedTemporaryFile("wb", dir=self.cache_root, delete=False) as encoded:
            encoded_temp = Path(encoded.name)
            for row in rows:
                with Path(str(row["chunk_path"])).open("rb") as chunk:
                    while data := chunk.read(1024 * 1024):
                        encoded.write(data)
            encoded.flush()
            os.fsync(encoded.fileno())
        raw_temp: Path | None = None
        try:
            if encoded_temp.stat().st_size != int(first["encoded_size"]):
                raise FederationValidationError(
                    "federated-jsonl-encoded-size-mismatch",
                    "content.encoded_size",
                    "reconstructed gzip size does not match committed metadata",
                )
            if _sha256_path(encoded_temp) != encoded_sha256:
                raise FederationValidationError(
                    "federated-jsonl-encoded-hash-mismatch",
                    "content.encoded_sha256",
                    "reconstructed gzip hash does not match committed metadata",
                )
            with tempfile.NamedTemporaryFile("wb", dir=target.parent, delete=False) as raw:
                raw_temp = Path(raw.name)
                digest = hashlib.sha256()
                size = 0
                with gzip.open(encoded_temp, "rb") as compressed:
                    while data := compressed.read(1024 * 1024):
                        size += len(data)
                        digest.update(data)
                        raw.write(data)
                raw.flush()
                os.fsync(raw.fileno())
            if size != int(first["file_size"]):
                raise FederationValidationError(
                    "federated-jsonl-file-size-mismatch",
                    "content.file_size",
                    "reconstructed JSONL size does not match committed metadata",
                )
            if f"sha256:{digest.hexdigest()}" != file_sha256:
                raise FederationValidationError(
                    "federated-jsonl-file-hash-mismatch",
                    "content.file_sha256",
                    "reconstructed JSONL hash does not match committed metadata",
                )
            os.replace(raw_temp, target)
            raw_temp = None
            _fsync_directory(target.parent)
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO materialized_files(
                        session_id,dataset_id,producer_node_id,relative_path,
                        file_sha256,source_mtime_ns,target_path,size_bytes,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(session_id,dataset_id) DO UPDATE SET
                        producer_node_id=excluded.producer_node_id,
                        relative_path=excluded.relative_path,
                        file_sha256=excluded.file_sha256,
                        source_mtime_ns=excluded.source_mtime_ns,
                        target_path=excluded.target_path,
                        size_bytes=excluded.size_bytes,
                        updated_at=excluded.updated_at
                    """,
                    (
                        session_id,
                        dataset_id,
                        producer,
                        relative_path,
                        file_sha256,
                        source_mtime_ns,
                        str(target),
                        size,
                        _stamp(self.clock()),
                    ),
                )
            for row in rows:
                Path(str(row["chunk_path"])).unlink(missing_ok=True)
            return True
        finally:
            encoded_temp.unlink(missing_ok=True)
            if raw_temp is not None:
                raw_temp.unlink(missing_ok=True)

    def _ingest_remote(
        self,
        reference: CommittedBatchReference,
        content: object,
        *,
        local_node_id: str,
    ) -> bool:
        if (
            reference.schema_name != FEDERATED_JSONL_DATASET_SCHEMA_NAME
            or reference.schema_version != FEDERATED_JSONL_DATASET_SCHEMA_VERSION
            or not isinstance(content, dict)
        ):
            return False
        if BatchIngestRequest.calculate_content_hash(content) != reference.content_hash:
            raise FederationValidationError(
                "content-hash-mismatch",
                "content_hash",
                "storage content does not match its committed manifest reference",
            )
        producer = content.get("producer_node_id")
        if not isinstance(producer, str) or not producer:
            raise FederationValidationError(
                "invalid-federated-jsonl-producer",
                "content.producer_node_id",
                "must be non-empty text",
            )
        decoded = validate_federated_jsonl_ingest(
            actor_node_id=producer,
            session_id=reference.session_id,
            dataset_id=reference.dataset_id,
            batch_id=reference.batch_id,
            idempotency_key=reference.idempotency_key,
            schema_name=reference.schema_name,
            schema_version=reference.schema_version,
            content=content,
        )
        return self._record_remote_chunk(
            reference,
            content,
            decoded,
            local_node_id=local_node_id,
        )

    def _reset_resume(self) -> None:
        self._resume_scope = None
        self._resume_cursor = None
        self._resume_manifest = None

    def _mirror_remote(
        self,
        runtime_state: object,
        *,
        session_id: str,
        local_node_id: str,
        authority_node_id: str,
        group_id: str,
    ) -> tuple[int, int, int]:
        runtime = getattr(self.onboarding_service, "relay_runtime", None)
        list_batches = getattr(runtime, "list_committed_batches", None)
        read_batches = getattr(runtime, "read_federated_batches", None)
        if not callable(list_batches) or not callable(read_batches):
            raise FederationOperationError(
                "federated-jsonl-runtime-unavailable",
                "paired runtime does not expose Federation batch discovery/read",
            )
        max_pages = self._positive_bound(
            "FEDERATED_JSONL_MAX_PAGES_PER_SYNC",
            _DEFAULT_MAX_PAGES,
            _HARD_MAX_PAGES,
        )
        max_batches = self._positive_bound(
            "FEDERATED_JSONL_MAX_BATCHES_PER_SYNC",
            _DEFAULT_MAX_REMOTE_BATCHES,
            _HARD_MAX_REMOTE_BATCHES,
        )
        scope = (session_id, authority_node_id, group_id)
        if self._resume_scope != scope:
            self._reset_resume()
        cursor = self._resume_cursor
        manifest_identity = self._resume_manifest
        seen_cursors = {cursor} if cursor is not None else set()
        pages = 0
        discovered = 0
        downloaded = 0
        materialized = 0

        while pages < max_pages and discovered < max_batches:
            requested_limit = min(_PAGE_SIZE, max_batches - discovered)
            page = self._validate_page(
                list_batches(
                    runtime_state,
                    authority_node_id=authority_node_id,
                    group_id=group_id,
                    dataset_id=None,
                    limit=requested_limit,
                    cursor=cursor,
                ),
                session_id=session_id,
                group_id=group_id,
                requested_limit=requested_limit,
            )
            current_manifest = (page.manifest_revision, page.manifest_hash)
            if manifest_identity is not None and current_manifest != manifest_identity:
                raise FederationOperationError(
                    "storage-catalog-manifest-changed",
                    "paginated storage catalog changed manifest identity",
                )
            manifest_identity = current_manifest
            pages += 1
            discovered += len(page.batches)
            pending = tuple(
                reference
                for reference in page.batches
                if reference.schema_name == FEDERATED_JSONL_DATASET_SCHEMA_NAME
                and reference.schema_version == FEDERATED_JSONL_DATASET_SCHEMA_VERSION
                and not self._seen(reference)
            )
            if pending:
                contents = read_batches(
                    runtime_state,
                    session_id=session_id,
                    authority_node_id=authority_node_id,
                    references=pending,
                )
                if not isinstance(contents, tuple) or len(contents) != len(pending):
                    raise FederationOperationError(
                        "federated-jsonl-read-response-invalid",
                        "generic Federation read returned an invalid result count",
                    )
                for reference, content in zip(pending, contents, strict=True):
                    downloaded += 1
                    if self._ingest_remote(
                        reference,
                        content,
                        local_node_id=local_node_id,
                    ):
                        materialized += 1
            next_cursor = page.next_cursor
            if next_cursor is None:
                cursor = None
                break
            if next_cursor in seen_cursors:
                raise FederationOperationError(
                    "storage-catalog-cursor-cycle",
                    "storage authority returned a repeated pagination cursor",
                )
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        if cursor is None:
            self._reset_resume()
        else:
            self._resume_scope = scope
            self._resume_cursor = cursor
            self._resume_manifest = manifest_identity
        return discovered, downloaded, materialized

    def _request_refreshes(self) -> None:
        callback = self.artifact_refresh_callback
        if callback is None:
            catalog = self.app.config.get("ARTIFACT_CATALOG")
            start = getattr(catalog, "start_background_rescan_if_idle", None)
            if callable(start):
                callback = start
            else:
                request_artifact_catalog_refresh(reason="federated_jsonl_materialized")
        if callback is not None:
            try:
                callback(reason="federated_jsonl_materialized")
            except Exception:
                pass
        runtime_callback = self.runtime_refresh_callback
        if runtime_callback is None:
            runtime_callback = get_runtime_manager().request_refresh
        try:
            runtime_callback()
        except Exception:
            pass

    def sync(self, runtime_state: object, context: object) -> dict[str, object]:
        """Publish and mirror one bounded Federation JSONL synchronization pass."""

        self._ensure_initialized()
        if not self._sync_lock.acquire(blocking=False):
            return self.snapshot()
        authority_node_id: str | None = None
        group_id: str | None = None
        try:
            self._set_state(status="syncing", last_error=None)
            session_id, node_id = self._context_ids(runtime_state, context)
            runtime = getattr(self.onboarding_service, "relay_runtime", None)
            if runtime is None:
                raise FederationOperationError(
                    "pairing-relay-runtime-unavailable",
                    "the authenticated relay runtime is unavailable",
                )
            status = runtime.coordinator_status()
            if not isinstance(status, dict):
                raise FederationOperationError(
                    "coordinator-status-invalid",
                    "coordinator status must be an object",
                )
            selection = select_storage_authority(
                status,
                session_id=session_id,
                requested_group=self._configured_group(),
            )
            authority_node_id = selection.authority_node_id
            group_id = selection.group_id
            if (
                selection.state != "ready"
                or authority_node_id is None
                or group_id is None
            ):
                self._reset_resume()
                self._set_state(
                    status=selection.state,
                    authority=authority_node_id,
                    group=group_id,
                    last_error=selection.state,
                )
                return self.snapshot()

            published = self._publish_local(
                runtime_state,
                session_id=session_id,
                node_id=node_id,
                authority_node_id=authority_node_id,
                group_id=group_id,
            )
            discovered, downloaded, materialized = self._mirror_remote(
                runtime_state,
                session_id=session_id,
                local_node_id=node_id,
                authority_node_id=authority_node_id,
                group_id=group_id,
            )
            if materialized:
                self._request_refreshes()
            self._set_state(
                status="up-to-date" if self._resume_cursor is None else "syncing",
                authority=authority_node_id,
                group=group_id,
                published_chunks=published,
                remote_discovered=discovered,
                remote_downloaded=downloaded,
                materialized_files=materialized,
                last_error=None,
                last_sync=_stamp(self.clock()),
            )
            return self.snapshot()
        except Exception as exc:
            self._reset_resume()
            self._set_state(
                status="error",
                authority=authority_node_id,
                group=group_id,
                last_error=str(getattr(exc, "code", type(exc).__name__)),
            )
            raise
        finally:
            self._sync_lock.release()


__all__ = ["FederatedJsonlProductBridge"]
