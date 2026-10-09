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
import stat
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask

from catalog.common.artifact_refresh import request_artifact_catalog_refresh
from catalog.common.data_loading import iter_jsonl_files
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.host_resources import (
    HostResourceRefused,
    ProcessResourceAdmission,
)
from catalog.federation.process_resource_admission import PROCESS_RESOURCE_ADMISSION
from catalog.federation.shared_file_storage import (
    FEDERATED_JSONL_CHUNK_BYTES,
    FEDERATED_JSONL_CONTENT_SCHEMA,
    FEDERATED_JSONL_DATASET_SCHEMA_NAME,
    FEDERATED_JSONL_DATASET_SCHEMA_VERSION,
    FEDERATED_JSONL_ENCODING,
    FEDERATED_JSONL_MAX_ENCODED_BYTES,
    FEDERATED_JSONL_MAX_FILE_BYTES,
    encode_federated_jsonl_chunk,
    federated_jsonl_batch_id,
    federated_jsonl_dataset_id,
    federated_jsonl_idempotency_key,
    normalize_jsonl_relative_path,
    validate_federated_jsonl_ingest,
)
from catalog.federation.stable_filesystem import (
    TEMPORARY_OWNER_SUFFIX,
    StableDirectory,
    StableFilesystemError,
    stable_directory,
)
from catalog.federation.storage_catalog import (
    CommittedBatchPage,
    CommittedBatchReference,
)
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
# Remote JSONL lands on local disk and is never evicted, so the mirror needs the
# same kind of ceiling the recorder telemetry mirror already enforces. Reaching
# it stops further materialization instead of failing the pass, so local
# publication and discovery keep working and the condition stays visible.
_DEFAULT_MAX_MIRROR_BYTES = 2 * 1024 * 1024 * 1024
_HARD_MAX_MIRROR_BYTES = 64 * 1024 * 1024 * 1024
_DEFAULT_MAX_STAGED_BYTES = 512 * 1024 * 1024
_HARD_MAX_STAGED_BYTES = 16 * 1024 * 1024 * 1024
_DEFAULT_MAX_STAGED_FILES = 8192
_HARD_MAX_STAGED_FILES = 1_000_000
_STAGED_CACHE_SCAN_MULTIPLIER = 4
_SQLITE_WRITE_RESERVE_BYTES = 2 * 1024 * 1024
_SQLITE_WRITE_INODES = 3
_SQLITE_BOOTSTRAP_RESERVE_BYTES = 4 * 1024 * 1024
_SQLITE_WAL_AUTOCHECKPOINT_PAGES = 64
# Each managed temporary now has a durable owner record. The first use of a
# directory also creates its authenticated temporary-root marker, so reserve
# temp + owner + marker rather than pretending the owner metadata is free.
_JSONL_CACHE_INODES = 3
_JSONL_CHUNK_INODES = 3
_JSONL_MATERIALIZATION_INODES = 3
_STAGED_CACHE_LOCK = threading.RLock()
_TEMPORARY_SCAVENGE_MAX_ENTRIES = 4096
_OWNED_TEMPORARY_PREFIXES = (
    "fcp-chunk-",
    "fcp-encoded-",
    "fcp-raw-",
    "fcp-jsonl-",
)
_STORAGE_GROUP_CONFIG_KEYS = (
    "FEDERATED_JSONL_STORAGE_GROUP_ID",
    "FEDERATED_TELEMETRY_STORAGE_GROUP_ID",
    "FEDERATION_STORAGE_GROUP_ID",
    "FCP_FEDERATION_STORAGE_GROUP",
    "RECORDER_FEDERATION_STORAGE_GROUP_ID",
)
_EXCLUDED_LOCAL_PREFIXES = (
    # Already mirrored from the Federation; republishing would loop.
    "federation/",
    # Has a stronger sequence-aware publication and mirror contract of its own.
    "sources/mtconnect_recorder/jsonl/",
    # Raw batches have a separate sequence-aware outbox. Walking this tree for
    # generic JSONL files needlessly enumerates every raw envelope and manifest
    # on installations where the Recorder data root is a slow host bind mount.
    "sources/mtconnect_recorder/raw/",
)
_PRUNED_LOCAL_DIRECTORY_PREFIXES = ("sources/mtconnect_recorder/raw/",)
#: Prefixes that are published by default but that a deployment may withhold.
#: Browser uploads are the case that matters: they are shared like any other
#: local JSONL, and an installation that treats uploaded files as device-local
#: material sets ``FEDERATED_JSONL_PUBLISH_UPLOADS`` to false to keep them off
#: the Federation. Files already committed stay committed; the log is
#: append-only, so this governs what is published from now on.
_OPTIONAL_LOCAL_PREFIXES = {"uploads/": "FEDERATED_JSONL_PUBLISH_UPLOADS"}

Clock = Callable[[], datetime]
RefreshCallback = Callable[..., bool]
RuntimeRefreshCallback = Callable[[], bool]


@dataclass(frozen=True)
class FederatedJsonlPublishResult:
    """Result of one bounded, local-only JSONL publication pass."""

    session_id: str
    node_id: str
    authority_node_id: str
    group_id: str
    published_chunks: int


class _BoundedWriter:
    """Refuse a temp-file write before its declared byte ceiling is crossed."""

    def __init__(self, handle: Any, max_bytes: int) -> None:
        self._handle = handle
        self._max_bytes = max(int(max_bytes), 0)
        self._written = 0

    def write(self, data: bytes | bytearray | memoryview) -> int:
        length = len(data)
        if self._written + length > self._max_bytes:
            raise FederationValidationError(
                "federated-jsonl-size-limit",
                "size_bytes",
                "Federated JSONL temporary output must not exceed "
                f"{self._max_bytes} bytes",
            )
        written = self._handle.write(data)
        self._written += written
        return written

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)


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


def _missing_directory_count(path: Path) -> int:
    """Count directory inodes one transaction may have to create."""

    count = 0
    current = path
    while not current.exists():
        count += 1
        parent = current.parent
        if parent == current:
            break
        current = parent
    return count


def _local_gzip_requirement(source_bytes: int) -> int:
    """Conservative upper bound for one new gzip temp/cache publication."""

    bounded = max(int(source_bytes), 0)
    return min(FEDERATED_JSONL_MAX_ENCODED_BYTES, (2 * bounded) + 1024)


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
        resource_admission: ProcessResourceAdmission | None = None,
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
        self.resource_admission = resource_admission or PROCESS_RESOURCE_ADMISSION
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
            "mirror_bytes": 0,
            "mirror_quota_bytes": None,
            "mirror_quota_reached": False,
            "last_error": None,
            "last_sync": None,
        }

    @contextmanager
    def _reserve(
        self,
        path: Path,
        *,
        bytes_required: int,
        inodes_required: int,
    ) -> Iterator[object]:
        try:
            with self.resource_admission.reserve(
                path,
                bytes_required=bytes_required,
                inodes_required=inodes_required,
            ) as reservation:
                yield reservation
        except HostResourceRefused as exc:
            raise FederationOperationError(
                "federated-jsonl-resource-pressure",
                "host resource pressure refused a Federated JSONL disk write",
            ) from exc

    @contextmanager
    def _reserve_many(
        self, requirements: tuple[tuple[Path, int, int], ...]
    ) -> Iterator[tuple[object, ...]]:
        reserve_many = getattr(self.resource_admission, "reserve_many", None)
        if not callable(reserve_many):
            raise FederationOperationError(
                "federated-jsonl-resource-admission-incompatible",
                "resource admission does not support atomic multi-resource reservations",
            )
        try:
            with reserve_many(requirements) as reservations:
                yield tuple(reservations)
        except HostResourceRefused as exc:
            raise FederationOperationError(
                "federated-jsonl-resource-pressure",
                "host resource pressure refused a Federated JSONL disk write",
            ) from exc

    def _assert_reserved_resource(self, path: Path, reservation: object) -> None:
        """Fail closed if a missing-path reservation moved to another filesystem."""

        expected = getattr(reservation, "resource_id", None)
        if not isinstance(expected, str) or not expected:
            raise FederationOperationError(
                "federated-jsonl-resource-identity-unavailable",
                "resource admission did not expose the reserved backing resource",
            )
        actual = self.resource_admission.assessment(path).resource_id
        if actual != expected:
            raise FederationOperationError(
                "federated-jsonl-resource-changed",
                "Federated JSONL destination changed backing resource after admission",
            )

    @contextmanager
    def _stable_directory(
        self, root: Path, relative: Path = Path("."), *, create: bool = False
    ) -> Iterator[StableDirectory]:
        try:
            with stable_directory(root, relative, create=create) as boundary:
                yield boundary
        except FileNotFoundError:
            raise
        except (OSError, StableFilesystemError) as exc:
            raise FederationOperationError(
                "federated-jsonl-filesystem-boundary",
                "Federated JSONL managed path could not be held behind a stable filesystem boundary",
            ) from exc

    def _assert_stable_reserved_resource(
        self, boundary: StableDirectory, reservation: object
    ) -> None:
        expected = getattr(reservation, "resource_id", None)
        if not isinstance(expected, str) or not expected:
            raise FederationOperationError(
                "federated-jsonl-resource-identity-unavailable",
                "resource admission did not expose the reserved backing resource",
            )
        actual = boundary.resource_id
        if expected == actual:
            return
        # Test/adaptor admissions may expose synthetic identities. Production
        # reservations use device:/volume: identities and must match the pinned
        # directory handle directly; never fall back to a path snapshot there.
        if expected.startswith(("device:", "volume:", "volume-anchor:")):
            raise FederationOperationError(
                "federated-jsonl-resource-changed",
                "Federated JSONL destination changed backing resource after admission",
            )
        measured = self.resource_admission.assessment(boundary.path).resource_id
        if measured != expected:
            raise FederationOperationError(
                "federated-jsonl-resource-changed",
                "Federated JSONL destination changed backing resource after admission",
            )

    def _reservation_for_boundary(
        self, boundary: StableDirectory, reservations: tuple[object, ...]
    ) -> object:
        """Select the reservation proved against a pinned directory.

        Testing every candidate through the same identity assertion keeps this
        helper correct when ``reserve_many`` coalesces paths on one resource,
        while also allowing injectable test controllers with synthetic IDs.
        Multiple candidates that claim the same boundary are rejected rather
        than guessed.
        """

        matches: list[object] = []
        failures: list[FederationOperationError] = []
        for reservation in reservations:
            try:
                self._assert_stable_reserved_resource(boundary, reservation)
            except FederationOperationError as exc:
                failures.append(exc)
                continue
            matches.append(reservation)
        if len(matches) == 1:
            return matches[0]
        if len(reservations) == 1 and len(failures) == 1:
            # Preserve a proved TOCTOU/resource-identity failure instead of
            # hiding it behind the generic no-match error.
            raise failures[0]
        raise FederationOperationError(
            "federated-jsonl-resource-identity-unavailable",
            "atomic resource admission did not expose the reservation for a pinned destination",
        )

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            bootstrap_dirs = (self.data_root, self.database.parent, self.cache_root, self.mirror_root)
            requirements = tuple(
                (directory, 0, _missing_directory_count(directory))
                for directory in bootstrap_dirs
            ) + (
                (self.cache_root, 0, 1),
                (self.mirror_root, 0, 1),
                (
                    self.database.parent,
                    _SQLITE_BOOTSTRAP_RESERVE_BYTES,
                    _SQLITE_WRITE_INODES,
                ),
            )
            with self._reserve_many(requirements):
                for directory in bootstrap_dirs:
                    directory.mkdir(parents=True, exist_ok=True)
                self._initialize_database()
                self._scavenge_owned_temporaries()
            self._initialized = True

    def _scavenge_owned_temporaries(self) -> None:
        """Reclaim authenticated, unlocked FCP temp files on re-entry.

        Lexical traversal only discovers candidate owner records. Each candidate
        is reopened through the pinned stable directory boundary, where the
        durable root token, owner proof, exact file identity, and cross-process
        lock are checked before deletion. Prefixes are only a traversal filter.
        """

        scanned = 0
        for root in (self.cache_root, self.mirror_root):
            if not root.is_dir():
                continue
            for owner_path in root.rglob(f".*{TEMPORARY_OWNER_SUFFIX}"):
                scanned += 1
                if scanned > _TEMPORARY_SCAVENGE_MAX_ENTRIES:
                    raise FederationOperationError(
                        "federated-jsonl-temporary-scan-bounded",
                        "owned temporary cleanup traversal exceeded its bound",
                    )
                try:
                    relative_parent = owner_path.parent.relative_to(root)
                except ValueError:
                    continue
                try:
                    with self._stable_directory(root, relative_parent) as directory:
                        directory.scavenge_temporary_files(
                            prefixes=_OWNED_TEMPORARY_PREFIXES,
                            max_entries=_TEMPORARY_SCAVENGE_MAX_ENTRIES,
                        )
                except FileNotFoundError:
                    continue

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute(f"PRAGMA wal_autocheckpoint={_SQLITE_WAL_AUTOCHECKPOINT_PAGES}")
        return connection

    @contextmanager
    def _write_connection(
        self, *, admitted_reservation: object | None = None
    ) -> Iterator[sqlite3.Connection]:
        if admitted_reservation is not None:
            # The caller owns an enclosing atomic reservation. Re-admitting
            # here would reject valid completion when that reservation leaves
            # the resource at PRESSURE. Pin the SQLite parent before opening
            # the connection so the already-held reservation still has an
            # identity check at this boundary.
            with self._stable_directory(self.database.parent) as database_directory:
                self._assert_stable_reserved_resource(
                    database_directory, admitted_reservation
                )
                with self._connect() as connection:
                    yield connection
            return
        missing_directories = _missing_directory_count(self.database.parent)
        with self._reserve(
            self.database.parent,
            bytes_required=_SQLITE_WRITE_RESERVE_BYTES,
            inodes_required=_SQLITE_WRITE_INODES + missing_directories,
        ) as reservation:
            self.database.parent.mkdir(parents=True, exist_ok=True)
            self._assert_reserved_resource(self.database.parent, reservation)
            with self._connect() as connection:
                yield connection

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

    def _mirror_quota_bytes(self) -> int:
        return self._positive_bound(
            "FEDERATED_JSONL_MAX_MIRROR_BYTES",
            _DEFAULT_MAX_MIRROR_BYTES,
            _HARD_MAX_MIRROR_BYTES,
        )

    def _staged_quota_bytes(self) -> int:
        return self._positive_bound(
            "FEDERATED_JSONL_MAX_STAGED_BYTES", _DEFAULT_MAX_STAGED_BYTES, _HARD_MAX_STAGED_BYTES
        )

    def _staged_file_quota(self) -> int:
        return self._positive_bound(
            "FEDERATED_JSONL_MAX_STAGED_FILES",
            _DEFAULT_MAX_STAGED_FILES,
            _HARD_MAX_STAGED_FILES,
        )

    def _physical_staged_usage(self) -> tuple[int, int]:
        remote = self.cache_root / "remote"
        if not remote.is_dir():
            return 0, 0
        total = 0
        files = 0
        for staged in remote.rglob("*.chunk"):
            try:
                total += staged.stat().st_size
            except FileNotFoundError:
                continue
            files += 1
            if total >= self._staged_quota_bytes() or files >= self._staged_file_quota():
                break
        return total, files

    def _cleanup_orphaned_staged_chunks(self) -> int:
        with _STAGED_CACHE_LOCK:
            remote = self.cache_root / "remote"
            if not remote.is_dir():
                return 0
            maximum = self._positive_bound(
                "FEDERATED_JSONL_MAX_BATCHES_PER_SYNC",
                _DEFAULT_MAX_REMOTE_BATCHES,
                _HARD_MAX_REMOTE_BATCHES,
            )
            scan_limit = min(
                self._staged_file_quota(), maximum * _STAGED_CACHE_SCAN_MULTIPLIER
            )
            removed = 0
            scanned = 0
            with self._connect() as connection:
                for staged in remote.rglob("*.chunk"):
                    if removed >= maximum or scanned >= scan_limit:
                        break
                    scanned += 1
                    referenced = connection.execute(
                        "SELECT 1 FROM seen_batches WHERE chunk_path=? LIMIT 1",
                        (str(staged),),
                    ).fetchone()
                    if referenced is not None:
                        continue
                    try:
                        staged.unlink()
                        removed += 1
                    except FileNotFoundError:
                        continue
            return removed

    def _mirrored_bytes(self, *, excluding: tuple[str, str] | None = None) -> int:
        """Total bytes this device holds from other members' JSONL files."""

        with self._connect() as connection:
            if excluding is None:
                row = connection.execute(
                    "SELECT COALESCE(SUM(size_bytes),0) AS total FROM materialized_files"
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT COALESCE(SUM(size_bytes),0) AS total
                    FROM materialized_files
                    WHERE NOT (session_id=? AND dataset_id=?)
                    """,
                    excluding,
                ).fetchone()
        return int(row["total"]) if row is not None else 0

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

    def _excluded_prefixes(self) -> tuple[str, ...]:
        withheld = tuple(
            prefix
            for prefix, key in _OPTIONAL_LOCAL_PREFIXES.items()
            if not bool(self.app.config.get(key, True))
        )
        return _EXCLUDED_LOCAL_PREFIXES + withheld

    def _local_candidates(self) -> Iterable[tuple[str, Path]]:
        excluded = self._excluded_prefixes()
        last_parent: Path | None = None
        last_resolved_parent: Path | None = None

        def include_directory(entry: os.DirEntry[str]) -> bool:
            # Keep the established alias traversal for other excluded trees.
            # The ordinary Recorder raw root is a dedicated sequence-aware
            # archive boundary, so generic JSONL discovery can skip it before
            # enumerating the many raw envelopes and manifests beneath it.
            # Reparse directories still take the existing full traversal path.
            try:
                if os.name == "nt" and (
                    getattr(entry.stat(follow_symlinks=False), "st_file_attributes", 0)
                    & stat.FILE_ATTRIBUTE_REPARSE_POINT
                ):
                    return True
                relative = (
                    Path(entry.path)
                    .relative_to(self.data_root)
                    .as_posix()
                    .rstrip("/")
                    + "/"
                )
            except (OSError, ValueError):
                return True  # Preserve discovery if pruning cannot be proven safe.
            return not any(
                relative.startswith(prefix)
                for prefix in _PRUNED_LOCAL_DIRECTORY_PREFIXES
            )

        def include_entry(entry: os.DirEntry[str]) -> bool:
            nonlocal last_parent, last_resolved_parent
            # Readdir already knows ordinary leaf types (and Windows attributes).
            # Avoid Path.stat/lstat RPCs for the large excluded Recorder store.
            # Aliases/reparse entries still take the unchanged full-path checks;
            # an alias inside an excluded tree may target an eligible source.
            try:
                if not entry.is_file(follow_symlinks=False) or entry.is_symlink():
                    return True
                if os.name == "nt" and (
                    getattr(entry.stat(follow_symlinks=False), "st_file_attributes", 0)
                    & stat.FILE_ATTRIBUTE_REPARSE_POINT
                ):
                    return True
                parent = Path(entry.path).parent
                if parent != last_parent or last_resolved_parent is None:
                    last_resolved_parent = parent.resolve()
                    last_parent = parent
                relative = (last_resolved_parent / entry.name).relative_to(
                    self.data_root
                ).as_posix()
            except (OSError, ValueError):
                return True  # Retain the existing fail-closed full check below.
            return not any(relative.startswith(prefix) for prefix in excluded)

        def include_file(path: Path) -> bool:
            nonlocal last_parent, last_resolved_parent
            # Resolve before excluding: a lexical alias below an excluded
            # directory can still refer to an eligible local source. Files
            # already excluded need no ancestor upload-marker inspection.
            try:
                metadata = path.lstat()
                if stat.S_ISREG(metadata.st_mode) and not (
                    getattr(metadata, "st_file_attributes", 0)
                    & stat.FILE_ATTRIBUTE_REPARSE_POINT
                ):
                    # Sorted siblings share a parent. Retain only one successful
                    # resolution for this pass; never cache upload-marker state.
                    # Selected paths still get the fresh full check below.
                    parent = path.parent
                    if parent != last_parent or last_resolved_parent is None:
                        resolved_parent = parent.resolve()
                        last_parent, last_resolved_parent = parent, resolved_parent
                    resolved = last_resolved_parent / path.name
                else:
                    resolved = path.resolve()
                relative = resolved.relative_to(self.data_root).as_posix()
            except (OSError, ValueError):
                return False
            return not any(relative.startswith(prefix) for prefix in excluded)

        for path in iter_jsonl_files(
            self.data_root, recursive=True, file_filter=include_file,
            entry_filter=include_entry, directory_filter=include_directory,
        ):
            try:
                relative = path.resolve().relative_to(self.data_root).as_posix()
            except (OSError, ValueError):
                continue
            if any(relative.startswith(prefix) for prefix in excluded):
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
        if stat_before.st_size > FEDERATED_JSONL_MAX_FILE_BYTES:
            raise FederationValidationError(
                "federated-jsonl-file-too-large",
                "file_size",
                "Federated JSONL source must not exceed "
                f"{FEDERATED_JSONL_MAX_FILE_BYTES} bytes",
            )
        existing = self._local_row(relative_path)
        if (
            existing is not None
            and int(existing["size_bytes"]) == stat_before.st_size
            and int(existing["mtime_ns"]) == stat_before.st_mtime_ns
        ):
            cache_path = Path(str(existing["cache_path"]))
            if existing["published_at"] is not None or cache_path.is_file():
                return existing

        file_digest = hashlib.sha256()
        requirement = _local_gzip_requirement(stat_before.st_size)
        sqlite_missing_directories = _missing_directory_count(self.database.parent)
        requirements = (
            (self.cache_root, requirement, _JSONL_CACHE_INODES),
            (
                self.database.parent,
                _SQLITE_WRITE_RESERVE_BYTES,
                _SQLITE_WRITE_INODES + sqlite_missing_directories,
            ),
        )
        with self._reserve_many(requirements) as reservations, self._stable_directory(
            self.cache_root
        ) as cache_directory, self._stable_directory(
            self.database.parent
        ) as database_directory:
            self._reservation_for_boundary(cache_directory, reservations)
            database_reservation = self._reservation_for_boundary(
                database_directory, reservations
            )
            with cache_directory.temporary_file(
                prefix="fcp-jsonl-", suffix=".jsonl.gz"
            ) as (temp_name, temporary):
                bounded = _BoundedWriter(temporary, FEDERATED_JSONL_MAX_ENCODED_BYTES)
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=bounded,
                    mtime=0,
                ) as compressed, path.open("rb") as source:
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
                encoded_sha256 = cache_directory.sha256(temp_name)
                encoded_size = cache_directory.stat(temp_name).st_size
                final_name = f"{file_sha256[7:]}.jsonl.gz"
                final_cache = self.cache_root / final_name
                if cache_directory.is_file(final_name):
                    if (
                        cache_directory.stat(final_name).st_size != encoded_size
                        or cache_directory.sha256(final_name) != encoded_sha256
                    ):
                        raise FederationOperationError(
                            "federated-jsonl-cache-conflict",
                            "content-addressed gzip cache contains different bytes",
                        )
                else:
                    cache_directory.replace(temp_name, final_name)
                    cache_directory.fsync()
                chunk_count = max(
                    1,
                    (encoded_size + FEDERATED_JSONL_CHUNK_BYTES - 1)
                    // FEDERATED_JSONL_CHUNK_BYTES,
                )
                dataset_id = federated_jsonl_dataset_id(node_id, relative_path)
                with self._write_connection(
                    admitted_reservation=database_reservation
                ) as connection:
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
            with self._write_connection() as connection:
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

    def _publish_local_progress(
        self,
        runtime_state: object,
        *,
        session_id: str,
        node_id: str,
        authority_node_id: str,
        group_id: str,
    ) -> tuple[int, int]:
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
            return 0, 0
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
        return self._advance_published(entries, outcomes), len(entries)

    def _publish_local(
        self,
        runtime_state: object,
        *,
        session_id: str,
        node_id: str,
        authority_node_id: str,
        group_id: str,
    ) -> int:
        committed, _attempted = self._publish_local_progress(
            runtime_state,
            session_id=session_id,
            node_id=node_id,
            authority_node_id=authority_node_id,
            group_id=group_id,
        )
        return committed

    def publish_local_once(
        self,
        runtime_state: object,
        context: object,
        *,
        authority_node_id: str,
        group_id: str,
    ) -> FederatedJsonlPublishResult:
        """Run one bounded publisher-only pass over local ``data/**/*.jsonl``.

        This entry point is suitable for a headless lifecycle that already has
        a trusted Federation context and a selected logical-storage route. It
        deliberately does not discover, read, materialize, or refresh remote
        data. Candidate filtering, durable progress, chunk schemas and
        idempotency are shared with :meth:`sync`.
        """

        self._ensure_initialized()
        if not self._sync_lock.acquire(blocking=False):
            raise FederationOperationError(
                "federated-jsonl-publication-busy",
                "another Federation JSONL pass is already running",
            )
        try:
            session_id, node_id = self._context_ids(runtime_state, context)
            if not isinstance(authority_node_id, str) or not authority_node_id:
                raise FederationValidationError(
                    "invalid-federated-jsonl-authority",
                    "authority_node_id",
                    "must be non-empty text",
                )
            if not isinstance(group_id, str) or not group_id:
                raise FederationValidationError(
                    "invalid-federated-jsonl-group",
                    "group_id",
                    "must be non-empty text",
                )
            published, attempted = self._publish_local_progress(
                runtime_state,
                session_id=session_id,
                node_id=node_id,
                authority_node_id=authority_node_id,
                group_id=group_id,
            )
            if published != attempted:
                raise FederationOperationError(
                    "federated-jsonl-publication-pending",
                    "logical storage did not commit every pending JSONL chunk",
                )
            return FederatedJsonlPublishResult(
                session_id=session_id,
                node_id=node_id,
                authority_node_id=authority_node_id,
                group_id=group_id,
                published_chunks=published,
            )
        finally:
            self._sync_lock.release()

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
        return directory / f"{chunk_index:08d}.chunk"

    def _write_chunk(
        self,
        path: Path,
        data: bytes,
        *,
        admitted_reservation: object | None = None,
    ) -> None:
        with _STAGED_CACHE_LOCK:
            self._write_chunk_locked(
                path, data, admitted_reservation=admitted_reservation
            )

    def _write_chunk_locked(
        self,
        path: Path,
        data: bytes,
        *,
        admitted_reservation: object | None = None,
    ) -> None:
        try:
            relative_parent = path.parent.relative_to(self.cache_root)
        except ValueError as exc:
            raise FederationValidationError(
                "unsafe-federated-jsonl-staging-target",
                "content.chunk_index",
                "staged chunk path escaped the managed cache root",
            ) from exc
        try:
            with self._stable_directory(self.cache_root, relative_parent) as directory:
                if directory.is_file(path.name):
                    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
                    if (
                        directory.stat(path.name).st_size == len(data)
                        and directory.sha256(path.name) == digest
                    ):
                        return
                    raise FederationValidationError(
                        "federated-jsonl-staging-conflict",
                        "content.chunk_index",
                        "staged chunk path already contains different bytes",
                    )
        except FileNotFoundError:
            pass

        staged_bytes, staged_files = self._physical_staged_usage()
        if (
            staged_bytes + len(data) > self._staged_quota_bytes()
            or staged_files + 1 > self._staged_file_quota()
        ):
            raise FederationOperationError(
                "federated-jsonl-staged-cache-full",
                "remote Federated JSONL staged-cache quota is exhausted",
            )
        reservation_context = (
            self._reserve(
                path.parent,
                bytes_required=len(data),
                inodes_required=_JSONL_CHUNK_INODES + len(relative_parent.parts),
            )
            if admitted_reservation is None
            else nullcontext(admitted_reservation)
        )
        with reservation_context as reservation, self._stable_directory(
            self.cache_root, relative_parent, create=True
        ) as directory:
            self._assert_stable_reserved_resource(directory, reservation)
            if directory.is_file(path.name):
                digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
                if (
                    directory.stat(path.name).st_size == len(data)
                    and directory.sha256(path.name) == digest
                ):
                    return
                raise FederationValidationError(
                    "federated-jsonl-staging-conflict",
                    "content.chunk_index",
                    "staged chunk path already contains different bytes",
                )
            with directory.temporary_file(prefix="fcp-chunk-") as (temp_name, handle):
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
                directory.replace(temp_name, path.name)
                directory.fsync()

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
        requirements: list[tuple[Path, int, int]] = []
        if chunk_path is not None:
            relative_chunk_parent = chunk_path.parent.relative_to(self.cache_root)
            requirements.append(
                (
                    chunk_path.parent,
                    len(decoded),
                    _JSONL_CHUNK_INODES + len(relative_chunk_parent.parts),
                )
            )
        requirements.append(
            (
                self.database.parent,
                _SQLITE_WRITE_RESERVE_BYTES,
                _SQLITE_WRITE_INODES + _missing_directory_count(self.database.parent),
            )
        )
        with self._reserve_many(requirements) as reservations:
            if not reservations:
                raise FederationOperationError(
                    "federated-jsonl-resource-identity-unavailable",
                    "atomic resource admission returned no backing resource",
                )
            with self._stable_directory(self.database.parent) as database_directory:
                database_reservation = self._reservation_for_boundary(
                    database_directory, reservations
                )
                chunk_reservation = None
                if chunk_path is not None:
                    with self._stable_directory(
                        self.cache_root, relative_chunk_parent, create=True
                    ) as chunk_directory:
                        chunk_reservation = self._reservation_for_boundary(
                            chunk_directory, reservations
                        )
                if chunk_path is not None:
                    self._write_chunk(
                        chunk_path,
                        decoded,
                        admitted_reservation=chunk_reservation,
                    )

                with self._write_connection(
                    admitted_reservation=database_reservation
                ) as connection:
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

    def _consume_staged_rows(self, rows: Iterable[sqlite3.Row]) -> None:
        rows = tuple(rows)
        if not rows:
            return
        with self._write_connection() as connection:
            connection.executemany(
                """
                UPDATE seen_batches SET chunk_path=NULL
                WHERE session_id=? AND group_id=? AND dataset_id=? AND batch_id=?
                """,
                (
                    (
                        str(row["session_id"]),
                        str(row["group_id"]),
                        str(row["dataset_id"]),
                        str(row["batch_id"]),
                    )
                    for row in rows
                ),
            )
        for row in rows:
            if row["chunk_path"]:
                Path(str(row["chunk_path"])).unlink(missing_ok=True)

    def _record_materialized_file(
        self,
        *,
        session_id: str,
        dataset_id: str,
        producer: str,
        relative_path: str,
        file_sha256: str,
        source_mtime_ns: int,
        target: Path,
        size: int,
        rows: Iterable[sqlite3.Row],
        admitted_reservation: object | None = None,
    ) -> None:
        staged_rows = tuple(rows)
        with self._write_connection(
            admitted_reservation=admitted_reservation
        ) as connection:
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
            connection.executemany(
                """
                UPDATE seen_batches SET chunk_path=NULL
                WHERE session_id=? AND group_id=? AND dataset_id=? AND batch_id=?
                """,
                (
                    (
                        str(row["session_id"]),
                        str(row["group_id"]),
                        str(row["dataset_id"]),
                        str(row["batch_id"]),
                    )
                    for row in staged_rows
                ),
            )
        for row in staged_rows:
            if row["chunk_path"]:
                Path(str(row["chunk_path"])).unlink(missing_ok=True)

    def _retry_staged_materializations(self, *, session_id: str) -> int:
        max_versions = self._positive_bound(
            "FEDERATED_JSONL_MAX_BATCHES_PER_SYNC",
            _DEFAULT_MAX_REMOTE_BATCHES,
            _HARD_MAX_REMOTE_BATCHES,
        )
        with self._connect() as connection:
            versions = connection.execute(
                """
                SELECT dataset_id,file_sha256,encoded_sha256
                FROM seen_batches
                WHERE session_id=? AND chunk_path IS NOT NULL
                GROUP BY dataset_id,file_sha256,encoded_sha256
                HAVING COUNT(*)=MAX(chunk_count)
                   AND COUNT(DISTINCT chunk_index)=MAX(chunk_count)
                ORDER BY MAX(committed_at),dataset_id,file_sha256,encoded_sha256
                LIMIT ?
                """,
                (session_id, max_versions),
            ).fetchall()
        materialized = 0
        for version in versions:
            if self._try_materialize(
                session_id=session_id,
                dataset_id=str(version["dataset_id"]),
                file_sha256=str(version["file_sha256"]),
                encoded_sha256=str(version["encoded_sha256"]),
            ):
                materialized += 1
        return materialized

    def _target_path(self, producer: str, relative_path: str) -> Path:
        relative = Path(relative_path)
        if relative.is_absolute() or any(part == ".." for part in relative.parts):
            raise FederationValidationError(
                "unsafe-federated-jsonl-target",
                "content.relative_path",
                "materialized path escaped the managed mirror root",
            )
        target = self.mirror_root / _safe_node_directory(producer) / relative
        try:
            target.relative_to(self.mirror_root)
        except ValueError as exc:
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
            self._consume_staged_rows(rows)
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
        declared_encoded_size = int(first["encoded_size"])
        declared_file_size = int(first["file_size"])
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

        quota = self._mirror_quota_bytes()
        retained = self._mirrored_bytes(excluding=(session_id, dataset_id))
        if retained + declared_file_size > quota:
            self._set_state(
                mirror_bytes=retained,
                mirror_quota_bytes=quota,
                mirror_quota_reached=True,
            )
            return False

        staged_size = sum(Path(str(row["chunk_path"])).stat().st_size for row in rows)
        if staged_size != declared_encoded_size:
            raise FederationValidationError(
                "federated-jsonl-encoded-size-mismatch",
                "content.encoded_size",
                "staged gzip size does not match committed metadata",
            )

        target = self._target_path(producer, relative_path)
        relative_parent = target.parent.relative_to(self.mirror_root)
        try:
            with self._stable_directory(self.mirror_root, relative_parent) as target_directory:
                if (
                    target_directory.is_file(target.name)
                    and target_directory.stat(target.name).st_size == declared_file_size
                    and target_directory.sha256(target.name) == file_sha256
                ):
                    self._record_materialized_file(
                        session_id=session_id,
                        dataset_id=dataset_id,
                        producer=producer,
                        relative_path=relative_path,
                        file_sha256=file_sha256,
                        source_mtime_ns=source_mtime_ns,
                        target=target,
                        size=declared_file_size,
                        rows=rows,
                    )
                    return True
        except FileNotFoundError:
            pass

        requirements = (
            (
                self.cache_root,
                declared_encoded_size,
                _JSONL_MATERIALIZATION_INODES,
            ),
            (
                target.parent,
                declared_file_size,
                _JSONL_MATERIALIZATION_INODES + len(relative_parent.parts),
            ),
            (
                self.database.parent,
                _SQLITE_WRITE_RESERVE_BYTES,
                _SQLITE_WRITE_INODES + _missing_directory_count(self.database.parent),
            ),
        )
        with self._reserve_many(requirements) as materialization_reservations:
            if not materialization_reservations:
                raise FederationOperationError(
                    "federated-jsonl-resource-identity-unavailable",
                    "atomic resource admission returned no backing resource",
                )
            with self._stable_directory(self.cache_root) as cache_directory, self._stable_directory(
                self.mirror_root, relative_parent, create=True
            ) as target_directory, self._stable_directory(
                self.database.parent
            ) as database_directory:
                self._reservation_for_boundary(
                    cache_directory, materialization_reservations
                )
                self._reservation_for_boundary(
                    target_directory, materialization_reservations
                )
                database_reservation = self._reservation_for_boundary(
                    database_directory, materialization_reservations
                )
                with cache_directory.temporary_file(prefix="fcp-encoded-") as (
                    encoded_name,
                    encoded,
                ):
                    bounded_encoded = _BoundedWriter(encoded, declared_encoded_size)
                    for row in rows:
                        with Path(str(row["chunk_path"])).open("rb") as chunk:
                            while data := chunk.read(1024 * 1024):
                                bounded_encoded.write(data)
                    encoded.flush()
                    os.fsync(encoded.fileno())
                    if cache_directory.stat(encoded_name).st_size != declared_encoded_size:
                        raise FederationValidationError(
                            "federated-jsonl-encoded-size-mismatch",
                            "content.encoded_size",
                            "reconstructed gzip size does not match committed metadata",
                        )
                    if cache_directory.sha256(encoded_name) != encoded_sha256:
                        raise FederationValidationError(
                            "federated-jsonl-encoded-hash-mismatch",
                            "content.encoded_sha256",
                            "reconstructed gzip hash does not match committed metadata",
                        )
                    with target_directory.temporary_file(prefix="fcp-raw-") as (
                        raw_name,
                        raw,
                    ):
                        bounded_raw = _BoundedWriter(raw, declared_file_size)
                        digest = hashlib.sha256()
                        size = 0
                        with cache_directory.open_read(encoded_name) as encoded_source, gzip.GzipFile(
                            fileobj=encoded_source, mode="rb"
                        ) as compressed:
                            while data := compressed.read(1024 * 1024):
                                size += len(data)
                                digest.update(data)
                                bounded_raw.write(data)
                        raw.flush()
                        os.fsync(raw.fileno())
                        if size != declared_file_size:
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
                        target_directory.replace(raw_name, target.name)
                        target_directory.fsync()
                self._record_materialized_file(
                    session_id=session_id,
                    dataset_id=dataset_id,
                    producer=producer,
                    relative_path=relative_path,
                    file_sha256=file_sha256,
                    source_mtime_ns=source_mtime_ns,
                    target=target,
                    size=size,
                    rows=rows,
                    admitted_reservation=database_reservation,
                )
                return True

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
            except Exception as exc:  # noqa: BLE001 - refresh is best effort
                self.app.logger.info(
                    "Federated JSONL artifact refresh unavailable (%s)",
                    type(exc).__name__,
                )
        runtime_callback = self.runtime_refresh_callback
        if runtime_callback is None:
            runtime_callback = get_runtime_manager().request_refresh
        try:
            runtime_callback()
        except Exception as exc:  # noqa: BLE001 - runtime polling remains fallback
            self.app.logger.info(
                "Federated JSONL runtime refresh unavailable (%s)",
                type(exc).__name__,
            )

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
            self._cleanup_orphaned_staged_chunks()
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
            materialized = self._retry_staged_materializations(session_id=session_id)
            discovered, downloaded, newly_materialized = self._mirror_remote(
                runtime_state,
                session_id=session_id,
                local_node_id=node_id,
                authority_node_id=authority_node_id,
                group_id=group_id,
            )
            materialized += newly_materialized
            if materialized:
                self._request_refreshes()
            quota = self._mirror_quota_bytes()
            retained = self._mirrored_bytes()
            self._set_state(
                status="up-to-date" if self._resume_cursor is None else "syncing",
                authority=authority_node_id,
                group=group_id,
                published_chunks=published,
                remote_discovered=discovered,
                remote_downloaded=downloaded,
                materialized_files=materialized,
                mirror_bytes=retained,
                mirror_quota_bytes=quota,
                mirror_quota_reached=retained >= quota,
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


__all__ = ["FederatedJsonlProductBridge", "FederatedJsonlPublishResult"]
