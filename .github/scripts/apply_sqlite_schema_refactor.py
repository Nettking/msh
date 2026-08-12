from __future__ import annotations

from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if text.count(old) != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {text.count(old)}")
    return text.replace(old, new, 1)


def replace_between(text: str, start: str, end: str, replacement: str, label: str) -> str:
    if text.count(start) != 1 or text.count(end) != 1:
        raise RuntimeError(
            f"{label}: expected unique boundaries, found start={text.count(start)} end={text.count(end)}"
        )
    start_index = text.index(start)
    end_index = text.index(end, start_index)
    return text[:start_index] + replacement + text[end_index:]


local_path = Path("catalog/federation/local_storage.py")
local = local_path.read_text(encoding="utf-8")
local = replace_once(
    local,
    "from .errors import FederationValidationError\nfrom .storage_protocol import (",
    "from .errors import FederationValidationError\nfrom .sqlite_schema import SQLiteMigration, ensure_sqlite_schema\nfrom .storage_protocol import (",
    "local_storage import",
)
local_helpers = '''

STORAGE_INDEX_SCHEMA_NAME = "federation.filesystem_batch_storage"
STORAGE_INDEX_SCHEMA_VERSION = 2
_STORAGE_INDEX_BASE_COLUMNS = frozenset(
    {
        "session_id",
        "group_id",
        "dataset_id",
        "batch_id",
        "idempotency_key",
        "content_hash",
        "relative_path",
        "created_at",
    }
)
_STORAGE_INDEX_SCHEMA_COLUMNS = frozenset(
    {"dataset_schema_name", "dataset_schema_version"}
)


def _storage_index_table_exists(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='committed_batches'"
        ).fetchone()
        is not None
    )


def _storage_index_columns(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(committed_batches)")
    }


def _detect_storage_index_version(connection: sqlite3.Connection) -> int:
    if not _storage_index_table_exists(connection):
        return 0
    columns = _storage_index_columns(connection)
    missing_base = _STORAGE_INDEX_BASE_COLUMNS.difference(columns)
    if missing_base:
        raise FederationValidationError(
            "unsupported-storage-index-schema",
            "schema",
            f"legacy storage index is missing required columns: {sorted(missing_base)!r}",
        )
    return (
        STORAGE_INDEX_SCHEMA_VERSION
        if _STORAGE_INDEX_SCHEMA_COLUMNS.issubset(columns)
        else 1
    )


def _create_storage_index_v1(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE committed_batches (
            session_id TEXT NOT NULL,
            group_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (session_id, group_id, batch_id),
            UNIQUE (session_id, group_id, idempotency_key)
        )
        """
    )


def _upgrade_storage_index_v2(connection: sqlite3.Connection) -> None:
    columns = _storage_index_columns(connection)
    if "dataset_schema_name" not in columns:
        connection.execute(
            """ALTER TABLE committed_batches
               ADD COLUMN dataset_schema_name TEXT NOT NULL
               DEFAULT 'fcp.storage.dataset.opaque'"""
        )
    if "dataset_schema_version" not in columns:
        connection.execute(
            """ALTER TABLE committed_batches
               ADD COLUMN dataset_schema_version INTEGER NOT NULL
               DEFAULT 1 CHECK(dataset_schema_version > 0)"""
        )


def _validate_storage_index(connection: sqlite3.Connection) -> None:
    if not _storage_index_table_exists(connection):
        raise FederationValidationError(
            "unsupported-storage-index-schema",
            "schema",
            "committed_batches table is missing",
        )
    missing = (
        _STORAGE_INDEX_BASE_COLUMNS | _STORAGE_INDEX_SCHEMA_COLUMNS
    ).difference(_storage_index_columns(connection))
    if missing:
        raise FederationValidationError(
            "unsupported-storage-index-schema",
            "schema",
            f"storage index is missing required columns: {sorted(missing)!r}",
        )
'''
local = replace_once(
    local,
    "\n\nclass FilesystemBatchStorageProvider:",
    local_helpers + "\n\nclass FilesystemBatchStorageProvider:",
    "local_storage helpers",
)
local_initialize = '''    def _initialize(self) -> None:
        with self._connect() as connection:
            ensure_sqlite_schema(
                connection,
                schema_name=STORAGE_INDEX_SCHEMA_NAME,
                target_version=STORAGE_INDEX_SCHEMA_VERSION,
                migrations=(
                    SQLiteMigration(1, _create_storage_index_v1),
                    SQLiteMigration(2, _upgrade_storage_index_v2),
                ),
                detect_legacy_version=_detect_storage_index_version,
                validate=_validate_storage_index,
            )

'''
local = replace_between(
    local,
    "    def _initialize(self) -> None:\n",
    "    def _relative_path(self, request: BatchIngestRequest) -> Path:\n",
    local_initialize,
    "local_storage initialize",
)
local_path.write_text(local, encoding="utf-8")


commit_path = Path("catalog/federation/commit_tracking.py")
commit = commit_path.read_text(encoding="utf-8")
commit = replace_once(
    commit,
    "from .errors import FederationValidationError\nfrom .storage_protocol import BatchIngestRequest",
    "from .errors import FederationValidationError\nfrom .sqlite_schema import SQLiteMigration, ensure_sqlite_schema\nfrom .storage_protocol import BatchIngestRequest",
    "commit_tracking import",
)
commit_helpers = '''

ACKNOWLEDGEMENT_SCHEMA_NAME = "federation.storage_acknowledgements"
ACKNOWLEDGEMENT_SCHEMA_VERSION = 2
_ACKNOWLEDGEMENT_BASE_COLUMNS = frozenset(
    {
        "session_id",
        "group_id",
        "batch_id",
        "idempotency_key",
        "content_hash",
        "required_replica_acks",
        "primary_committed",
        "created_at",
        "updated_at",
    }
)
_ACKNOWLEDGEMENT_IDENTITY_COLUMNS = frozenset(
    {"dataset_id", "dataset_schema_name", "dataset_schema_version"}
)
_ACKNOWLEDGEMENT_REPLICA_COLUMNS = frozenset(
    {
        "session_id",
        "group_id",
        "batch_id",
        "provider_id",
        "acknowledged",
        "acknowledged_at",
    }
)
_ACKNOWLEDGEMENT_PENDING_INDEX = "storage_commit_replica_pending"


def _ack_table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def _ack_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    if table == "storage_commits":
        pragma = "PRAGMA table_info(storage_commits)"
    elif table == "storage_commit_replicas":
        pragma = "PRAGMA table_info(storage_commit_replicas)"
    else:
        raise ValueError("unsupported acknowledgement table")
    return {str(row[1]) for row in connection.execute(pragma)}


def _ack_pending_index_exists(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
            (_ACKNOWLEDGEMENT_PENDING_INDEX,),
        ).fetchone()
        is not None
    )


def _detect_acknowledgement_version(connection: sqlite3.Connection) -> int:
    commits = _ack_table_exists(connection, "storage_commits")
    replicas = _ack_table_exists(connection, "storage_commit_replicas")
    if not commits and not replicas:
        return 0
    if not commits or not replicas:
        raise FederationValidationError(
            "unsupported-acknowledgement-schema",
            "schema",
            "legacy acknowledgement database is missing one of its required tables",
        )
    commit_columns = _ack_columns(connection, "storage_commits")
    replica_columns = _ack_columns(connection, "storage_commit_replicas")
    missing_commits = _ACKNOWLEDGEMENT_BASE_COLUMNS.difference(commit_columns)
    missing_replicas = _ACKNOWLEDGEMENT_REPLICA_COLUMNS.difference(replica_columns)
    if missing_commits or missing_replicas:
        raise FederationValidationError(
            "unsupported-acknowledgement-schema",
            "schema",
            "legacy acknowledgement database is missing required base columns",
        )
    return (
        ACKNOWLEDGEMENT_SCHEMA_VERSION
        if _ACKNOWLEDGEMENT_IDENTITY_COLUMNS.issubset(commit_columns)
        and _ack_pending_index_exists(connection)
        else 1
    )


def _create_acknowledgement_v1(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE storage_commits (
            session_id TEXT NOT NULL,
            group_id TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            required_replica_acks INTEGER NOT NULL CHECK(required_replica_acks >= 0),
            primary_committed INTEGER NOT NULL DEFAULT 0 CHECK(primary_committed IN (0,1)),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(session_id, group_id, batch_id),
            UNIQUE(session_id, group_id, idempotency_key)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE storage_commit_replicas (
            session_id TEXT NOT NULL,
            group_id TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            provider_id TEXT NOT NULL,
            acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0,1)),
            acknowledged_at TEXT,
            PRIMARY KEY(session_id, group_id, batch_id, provider_id),
            FOREIGN KEY(session_id, group_id, batch_id)
                REFERENCES storage_commits(session_id, group_id, batch_id)
                ON DELETE CASCADE
        )
        """
    )
    connection.execute(
        """CREATE INDEX storage_commit_replica_pending
           ON storage_commit_replicas(
               provider_id, acknowledged, session_id, group_id, batch_id
           )"""
    )


def _upgrade_acknowledgement_v2(connection: sqlite3.Connection) -> None:
    columns = _ack_columns(connection, "storage_commits")
    if "dataset_id" not in columns:
        connection.execute(
            """ALTER TABLE storage_commits
               ADD COLUMN dataset_id TEXT NOT NULL
               DEFAULT 'legacy-unknown-dataset'"""
        )
    if "dataset_schema_name" not in columns:
        connection.execute(
            """ALTER TABLE storage_commits
               ADD COLUMN dataset_schema_name TEXT NOT NULL
               DEFAULT 'fcp.storage.dataset.opaque'"""
        )
    if "dataset_schema_version" not in columns:
        connection.execute(
            """ALTER TABLE storage_commits
               ADD COLUMN dataset_schema_version INTEGER NOT NULL
               DEFAULT 1 CHECK(dataset_schema_version > 0)"""
        )
    connection.execute(
        """CREATE INDEX IF NOT EXISTS storage_commit_replica_pending
           ON storage_commit_replicas(
               provider_id, acknowledged, session_id, group_id, batch_id
           )"""
    )


def _validate_acknowledgement_schema(connection: sqlite3.Connection) -> None:
    if not _ack_table_exists(connection, "storage_commits") or not _ack_table_exists(
        connection, "storage_commit_replicas"
    ):
        raise FederationValidationError(
            "unsupported-acknowledgement-schema",
            "schema",
            "acknowledgement tables are missing",
        )
    missing_commits = (
        _ACKNOWLEDGEMENT_BASE_COLUMNS | _ACKNOWLEDGEMENT_IDENTITY_COLUMNS
    ).difference(_ack_columns(connection, "storage_commits"))
    missing_replicas = _ACKNOWLEDGEMENT_REPLICA_COLUMNS.difference(
        _ack_columns(connection, "storage_commit_replicas")
    )
    if missing_commits or missing_replicas or not _ack_pending_index_exists(connection):
        raise FederationValidationError(
            "unsupported-acknowledgement-schema",
            "schema",
            "acknowledgement schema is incomplete",
        )
'''
commit = replace_once(
    commit,
    "\n\nclass DurableAcknowledgementStore:",
    commit_helpers + "\n\nclass DurableAcknowledgementStore:",
    "commit_tracking helpers",
)
commit_initialize = '''    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            ensure_sqlite_schema(
                connection,
                schema_name=ACKNOWLEDGEMENT_SCHEMA_NAME,
                target_version=ACKNOWLEDGEMENT_SCHEMA_VERSION,
                migrations=(
                    SQLiteMigration(1, _create_acknowledgement_v1),
                    SQLiteMigration(2, _upgrade_acknowledgement_v2),
                ),
                detect_legacy_version=_detect_acknowledgement_version,
                validate=_validate_acknowledgement_schema,
            )

'''
commit = replace_between(
    commit,
    "    def __init__(self, database: Path | str) -> None:\n",
    "    def _connect(self) -> sqlite3.Connection:\n",
    commit_initialize,
    "commit_tracking initialize",
)
commit_path.write_text(commit, encoding="utf-8")
