"""Durable coordinator-owned authoritative storage manifests.

The store owns one append-only hash chain per ``(session_id, group_id)``.  A
storage provider may supply commit evidence, but only the coordinator calls
``commit`` after validating authority and the configured acknowledgement
policy.  Provider storage and this SQLite database are separate resources; the
Phase E runtime bridges that boundary with a durable, idempotent local intent.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import FederationValidationError
from .manifest import (
    AuthoritativeStorageManifest,
    DatasetManifest,
    ManifestItem,
    ManifestItemKind,
)
from .models import CommitState
from .storage_control_plane import STORAGE_GROUP_CREATED


def _text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-id",
            field,
            "must be non-empty opaque text",
        )
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    return value


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp",
                field,
                "must be RFC 3339",
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise FederationValidationError(
            "invalid-timestamp",
            field,
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value, "now").isoformat()


def _json(value: dict[str, Any]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


@dataclass(frozen=True)
class ManifestItemProposal:
    """Coordinator-approved evidence for one immutable committed item."""

    session_id: str
    group_id: str
    item_id: str
    kind: ManifestItemKind
    dataset_id: str
    idempotency_key: str | None
    content_hash: str
    size_bytes: int
    schema_name: str
    schema_version: int
    source_id: str | None
    first_sequence: int | None
    last_sequence: int | None
    dataset_required: bool
    acknowledged_provider_ids: tuple[str, ...]
    term: int
    expected_control_revision: int
    committed_at: datetime

    def __post_init__(self) -> None:
        for field in (
            "session_id",
            "group_id",
            "item_id",
            "dataset_id",
            "schema_name",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(self, "term", _uint(self.term, "term"))
        object.__setattr__(
            self,
            "expected_control_revision",
            _uint(self.expected_control_revision, "expected_control_revision"),
        )
        if not isinstance(self.dataset_required, bool):
            raise FederationValidationError(
                "invalid-boolean",
                "dataset_required",
                "must be boolean",
            )
        object.__setattr__(
            self,
            "acknowledged_provider_ids",
            tuple(sorted(self.acknowledged_provider_ids)),
        )
        object.__setattr__(
            self,
            "committed_at",
            _utc(self.committed_at, "committed_at"),
        )
        try:
            object.__setattr__(self, "kind", ManifestItemKind(self.kind))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-enum",
                "kind",
                "unknown manifest item kind",
            ) from exc
        # Reuse the public contracts for all item-level validation.
        self.to_item()

    def to_item(self) -> ManifestItem:
        return ManifestItem(
            item_id=self.item_id,
            kind=self.kind,
            dataset_id=self.dataset_id,
            idempotency_key=self.idempotency_key,
            content_hash=self.content_hash,
            size_bytes=self.size_bytes,
            schema_name=self.schema_name,
            schema_version=self.schema_version,
            source_id=self.source_id,
            first_sequence=self.first_sequence,
            last_sequence=self.last_sequence,
            commit_state=CommitState.COMMITTED,
            acknowledged_provider_ids=self.acknowledged_provider_ids,
            committed_at=self.committed_at,
        )

    def to_dataset(self) -> DatasetManifest:
        return DatasetManifest(
            dataset_id=self.dataset_id,
            schema_name=self.schema_name,
            schema_version=self.schema_version,
            required=self.dataset_required,
            source_id=self.source_id,
            last_contiguous_sequence=None,
            missing_ranges=(),
        )


@dataclass(frozen=True)
class ManifestCommitResult:
    manifest: AuthoritativeStorageManifest
    item_revision: int
    created: bool


@dataclass(frozen=True)
class ManifestCommitIntent:
    """Coordinator-retained immutable metadata prepared before provider write."""

    session_id: str
    group_id: str
    item_id: str
    kind: ManifestItemKind
    dataset_id: str
    idempotency_key: str | None
    content_hash: str
    size_bytes: int
    schema_name: str
    schema_version: int
    source_id: str | None
    first_sequence: int | None
    last_sequence: int | None
    dataset_required: bool
    primary_provider_id: str
    assigned_replica_ids: tuple[str, ...]
    required_replica_acks: int
    grant_id: str
    term: int
    fencing_token: int
    lease_expires_at: datetime
    prepared_control_revision: int
    prepared_at: datetime
    committed_revision: int | None = None

    def __post_init__(self) -> None:
        for field in (
            "session_id",
            "group_id",
            "item_id",
            "dataset_id",
            "schema_name",
            "primary_provider_id",
            "grant_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        try:
            object.__setattr__(self, "kind", ManifestItemKind(self.kind))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-enum",
                "kind",
                "unknown manifest item kind",
            ) from exc
        replicas = tuple(sorted(_text(value, "assigned_replica_ids") for value in self.assigned_replica_ids))
        if len(replicas) != len(set(replicas)) or self.primary_provider_id in replicas:
            raise FederationValidationError(
                "invalid-assignment",
                "assigned_replica_ids",
                "replicas must be unique and exclude the primary",
            )
        object.__setattr__(self, "assigned_replica_ids", replicas)
        for field in (
            "required_replica_acks",
            "term",
            "fencing_token",
            "prepared_control_revision",
        ):
            object.__setattr__(self, field, _uint(getattr(self, field), field))
        if self.required_replica_acks > len(replicas):
            raise FederationValidationError(
                "invalid-ack-count",
                "required_replica_acks",
                "cannot exceed the frozen replica count",
            )
        if self.committed_revision is not None:
            object.__setattr__(
                self,
                "committed_revision",
                _uint(self.committed_revision, "committed_revision"),
            )
            if self.committed_revision == 0:
                raise FederationValidationError(
                    "invalid-manifest-revision",
                    "committed_revision",
                    "a committed item cannot be introduced by genesis",
                )
        object.__setattr__(
            self,
            "lease_expires_at",
            _utc(self.lease_expires_at, "lease_expires_at"),
        )
        object.__setattr__(
            self,
            "prepared_at",
            _utc(self.prepared_at, "prepared_at"),
        )
        # Validate all immutable item and dataset fields through the contracts.
        self.to_proposal(
            acknowledged_provider_ids=(self.primary_provider_id,),
            expected_control_revision=self.prepared_control_revision,
            committed_at=self.prepared_at,
        )

    def to_proposal(
        self,
        *,
        acknowledged_provider_ids: tuple[str, ...],
        expected_control_revision: int,
        committed_at: datetime,
    ) -> ManifestItemProposal:
        return ManifestItemProposal(
            session_id=self.session_id,
            group_id=self.group_id,
            item_id=self.item_id,
            kind=self.kind,
            dataset_id=self.dataset_id,
            idempotency_key=self.idempotency_key,
            content_hash=self.content_hash,
            size_bytes=self.size_bytes,
            schema_name=self.schema_name,
            schema_version=self.schema_version,
            source_id=self.source_id,
            first_sequence=self.first_sequence,
            last_sequence=self.last_sequence,
            dataset_required=self.dataset_required,
            acknowledged_provider_ids=acknowledged_provider_ids,
            term=self.term,
            expected_control_revision=expected_control_revision,
            committed_at=committed_at,
        )

    def immutable_item_identity(self) -> tuple[Any, ...]:
        return (
            self.session_id,
            self.group_id,
            self.item_id,
            self.kind,
            self.dataset_id,
            self.idempotency_key,
            self.content_hash,
            self.size_bytes,
            self.schema_name,
            self.schema_version,
            self.source_id,
            self.first_sequence,
            self.last_sequence,
            self.dataset_required,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "group_id": self.group_id,
            "item_id": self.item_id,
            "kind": self.kind.value,
            "dataset_id": self.dataset_id,
            "idempotency_key": self.idempotency_key,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "source_id": self.source_id,
            "first_sequence": self.first_sequence,
            "last_sequence": self.last_sequence,
            "dataset_required": self.dataset_required,
            "primary_provider_id": self.primary_provider_id,
            "assigned_replica_ids": list(self.assigned_replica_ids),
            "required_replica_acks": self.required_replica_acks,
            "grant_id": self.grant_id,
            "term": self.term,
            "fencing_token": self.fencing_token,
            "lease_expires_at": _timestamp(self.lease_expires_at),
            "prepared_control_revision": self.prepared_control_revision,
            "prepared_at": _timestamp(self.prepared_at),
            "committed_revision": self.committed_revision,
        }

    @classmethod
    def from_dict(cls, value: Any) -> ManifestCommitIntent:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object",
                "manifest_intent",
                "must be an object",
            )
        required = {
            "session_id",
            "group_id",
            "item_id",
            "kind",
            "dataset_id",
            "idempotency_key",
            "content_hash",
            "size_bytes",
            "schema_name",
            "schema_version",
            "source_id",
            "first_sequence",
            "last_sequence",
            "dataset_required",
            "primary_provider_id",
            "assigned_replica_ids",
            "required_replica_acks",
            "grant_id",
            "term",
            "fencing_token",
            "lease_expires_at",
            "prepared_control_revision",
            "prepared_at",
            "committed_revision",
        }
        missing = sorted(required - value.keys())
        if missing:
            raise FederationValidationError(
                "missing-field",
                missing[0],
                "is required",
            )
        return cls(
            session_id=value["session_id"],
            group_id=value["group_id"],
            item_id=value["item_id"],
            kind=value["kind"],
            dataset_id=value["dataset_id"],
            idempotency_key=value["idempotency_key"],
            content_hash=value["content_hash"],
            size_bytes=value["size_bytes"],
            schema_name=value["schema_name"],
            schema_version=value["schema_version"],
            source_id=value["source_id"],
            first_sequence=value["first_sequence"],
            last_sequence=value["last_sequence"],
            dataset_required=value["dataset_required"],
            primary_provider_id=value["primary_provider_id"],
            assigned_replica_ids=tuple(value["assigned_replica_ids"]),
            required_replica_acks=value["required_replica_acks"],
            grant_id=value["grant_id"],
            term=value["term"],
            fencing_token=value["fencing_token"],
            lease_expires_at=value["lease_expires_at"],
            prepared_control_revision=value["prepared_control_revision"],
            prepared_at=value["prepared_at"],
            committed_revision=value["committed_revision"],
        )


class AuthoritativeManifestStore:
    """Transactional SQLite persistence for immutable manifest revisions."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            manifest_schema_existed = (
                connection.execute(
                    """SELECT 1 FROM sqlite_master
                       WHERE type='table'
                         AND name='storage_manifest_revisions'"""
                ).fetchone()
                is not None
            )
            phase_d_metadata_existed = (
                connection.execute(
                    """SELECT 1 FROM sqlite_master
                       WHERE type='table'
                         AND name='storage_fencing_counters'"""
                ).fetchone()
                is not None
            )
            legacy_groups: tuple[tuple[str, str], ...] = ()
            if not manifest_schema_existed and phase_d_metadata_existed:
                rows = connection.execute(
                    """SELECT session_id, payload_json
                       FROM storage_control_events
                       WHERE event_type=?""",
                    (STORAGE_GROUP_CREATED,),
                ).fetchall()
                legacy_groups = tuple(
                    (
                        str(row["session_id"]),
                        str(json.loads(row["payload_json"])["group_id"]),
                    )
                    for row in rows
                )
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS storage_manifest_group_states (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    state TEXT NOT NULL
                        CHECK(state IN ('ready','inventory-required')),
                    reason TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, group_id)
                );
                CREATE TABLE IF NOT EXISTS storage_manifest_revisions (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK(revision >= 0),
                    term INTEGER NOT NULL CHECK(term >= 0),
                    previous_manifest_hash TEXT,
                    manifest_hash TEXT NOT NULL,
                    commit_state TEXT NOT NULL CHECK(commit_state = 'committed'),
                    manifest_json TEXT NOT NULL CHECK(json_valid(manifest_json)),
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, group_id, revision),
                    UNIQUE(session_id, group_id, manifest_hash)
                );
                CREATE TABLE IF NOT EXISTS storage_manifest_heads (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK(revision >= 0),
                    manifest_hash TEXT NOT NULL,
                    PRIMARY KEY(session_id, group_id),
                    FOREIGN KEY(session_id, group_id, revision)
                        REFERENCES storage_manifest_revisions(
                            session_id, group_id, revision
                        )
                );
                CREATE TABLE IF NOT EXISTS storage_manifest_datasets (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    dataset_json TEXT NOT NULL CHECK(json_valid(dataset_json)),
                    introduced_revision INTEGER NOT NULL CHECK(introduced_revision > 0),
                    PRIMARY KEY(session_id, group_id, dataset_id)
                );
                CREATE TABLE IF NOT EXISTS storage_manifest_items (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    idempotency_key TEXT,
                    item_json TEXT NOT NULL CHECK(json_valid(item_json)),
                    introduced_revision INTEGER NOT NULL CHECK(introduced_revision > 0),
                    PRIMARY KEY(session_id, group_id, item_id),
                    UNIQUE(session_id, group_id, idempotency_key)
                );
                CREATE TABLE IF NOT EXISTS storage_manifest_intents (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    idempotency_key TEXT,
                    intent_json TEXT NOT NULL CHECK(json_valid(intent_json)),
                    state TEXT NOT NULL CHECK(state IN ('prepared','committed')),
                    prepared_control_revision INTEGER NOT NULL
                        CHECK(prepared_control_revision >= 0),
                    committed_revision INTEGER,
                    PRIMARY KEY(session_id, group_id, item_id),
                    UNIQUE(session_id, group_id, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS storage_manifest_intents_pending
                    ON storage_manifest_intents(
                        state, session_id, group_id, item_id
                    );
                CREATE INDEX IF NOT EXISTS storage_manifest_revision_chain
                    ON storage_manifest_revisions(
                        session_id, group_id, revision, manifest_hash
                    );
                """
            )
            migration_stamp = datetime.now(timezone.utc).isoformat()
            for session_id, group_id in legacy_groups:
                connection.execute(
                    """INSERT OR IGNORE INTO storage_manifest_group_states
                       (session_id, group_id, state, reason, updated_at)
                       VALUES (?, ?, 'inventory-required', ?, ?)""",
                    (
                        session_id,
                        group_id,
                        (
                            "pre-E1 storage inventory must be verified before "
                            "authoritative completeness can be established"
                        ),
                        migration_stamp,
                    ),
                )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _control_revision(
        connection: sqlite3.Connection,
        session_id: str,
    ) -> int:
        row = connection.execute(
            """SELECT COALESCE(MAX(revision), 0) AS revision
               FROM storage_control_events WHERE session_id=?""",
            (session_id,),
        ).fetchone()
        return int(row["revision"])

    @staticmethod
    def _require_group(
        connection: sqlite3.Connection,
        session_id: str,
        group_id: str,
    ) -> None:
        rows = connection.execute(
            """SELECT payload_json FROM storage_control_events
               WHERE session_id=? AND event_type=?""",
            (session_id, STORAGE_GROUP_CREATED),
        ).fetchall()
        if not any(
            json.loads(row["payload_json"]).get("group_id") == group_id
            for row in rows
        ):
            raise FederationValidationError(
                "unknown-storage-group",
                "group_id",
                "storage group is not registered",
            )

    @staticmethod
    def _check_control_revision(
        connection: sqlite3.Connection,
        session_id: str,
        expected: int,
    ) -> None:
        actual = AuthoritativeManifestStore._control_revision(
            connection,
            session_id,
        )
        if actual != expected:
            raise FederationValidationError(
                "concurrent-control-change",
                "expected_control_revision",
                f"expected control revision {expected}, found {actual}",
            )

    @staticmethod
    def _decode_revision(row: sqlite3.Row) -> AuthoritativeStorageManifest:
        try:
            manifest = AuthoritativeStorageManifest.from_dict(
                json.loads(row["manifest_json"])
            )
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
            FederationValidationError,
        ) as exc:
            if isinstance(exc, FederationValidationError):
                raise
            raise FederationValidationError(
                "malformed-manifest-row",
                "manifest_json",
                str(exc),
            ) from exc
        expected = {
            "session_id": manifest.session_id,
            "group_id": manifest.group_id,
            "revision": manifest.revision,
            "term": manifest.term,
            "previous_manifest_hash": manifest.previous_manifest_hash,
            "manifest_hash": manifest.manifest_hash,
            "commit_state": manifest.commit_state.value,
        }
        for field, value in expected.items():
            if row[field] != value:
                raise FederationValidationError(
                    "manifest-row-mismatch",
                    field,
                    "indexed manifest metadata does not match canonical JSON",
                )
        try:
            stored_time = datetime.fromisoformat(str(row["updated_at"]))
        except ValueError as exc:
            raise FederationValidationError(
                "malformed-manifest-row",
                "updated_at",
                "must be RFC 3339",
            ) from exc
        if manifest.updated_at != _utc(stored_time, "updated_at"):
            raise FederationValidationError(
                "manifest-row-mismatch",
                "updated_at",
                "indexed timestamp does not match canonical JSON",
            )
        return manifest

    def _history(
        self,
        connection: sqlite3.Connection,
        session_id: str,
        group_id: str,
    ) -> tuple[AuthoritativeStorageManifest, ...]:
        rows = connection.execute(
            """SELECT * FROM storage_manifest_revisions
               WHERE session_id=? AND group_id=? ORDER BY revision""",
            (session_id, group_id),
        ).fetchall()
        manifests = tuple(self._decode_revision(row) for row in rows)
        for expected_revision, manifest in enumerate(manifests):
            if manifest.revision != expected_revision:
                raise FederationValidationError(
                    "manifest-revision-gap",
                    "revision",
                    "manifest revisions must be contiguous from genesis",
                )
            predecessor = (
                None
                if expected_revision == 0
                else manifests[expected_revision - 1].manifest_hash
            )
            if manifest.previous_manifest_hash != predecessor:
                raise FederationValidationError(
                    "manifest-chain-mismatch",
                    "previous_manifest_hash",
                    "manifest predecessor does not match the previous revision",
                )
        return manifests

    def _head(
        self,
        connection: sqlite3.Connection,
        session_id: str,
        group_id: str,
    ) -> AuthoritativeStorageManifest:
        head_row = connection.execute(
            """SELECT revision, manifest_hash FROM storage_manifest_heads
               WHERE session_id=? AND group_id=?""",
            (session_id, group_id),
        ).fetchone()
        if head_row is None:
            raise FederationValidationError(
                "manifest-not-found",
                "group_id",
                "storage group has no authoritative manifest",
            )
        history = self._history(connection, session_id, group_id)
        if not history:
            raise FederationValidationError(
                "manifest-head-corrupt",
                "revision",
                "manifest head has no revision row",
            )
        manifest = history[-1]
        if (
            int(head_row["revision"]) != manifest.revision
            or head_row["manifest_hash"] != manifest.manifest_hash
        ):
            raise FederationValidationError(
                "manifest-head-corrupt",
                "manifest_hash",
                "head pointer does not match the verified chain",
            )
        return manifest

    def ensure_genesis(
        self,
        session_id: str,
        group_id: str,
        *,
        term: int,
        expected_control_revision: int,
        now: datetime,
    ) -> AuthoritativeStorageManifest:
        session_id = _text(session_id, "session_id")
        group_id = _text(group_id, "group_id")
        term = _uint(term, "term")
        expected_control_revision = _uint(
            expected_control_revision,
            "expected_control_revision",
        )
        if term != 0:
            raise FederationValidationError(
                "invalid-genesis-term",
                "term",
                "genesis manifest term must be zero",
            )
        now = _utc(now, "now")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    """SELECT 1 FROM storage_manifest_heads
                       WHERE session_id=? AND group_id=?""",
                    (session_id, group_id),
                ).fetchone()
                if existing is not None:
                    manifest = self._head(connection, session_id, group_id)
                    connection.commit()
                    return manifest
                group_state = connection.execute(
                    """SELECT state, reason
                       FROM storage_manifest_group_states
                       WHERE session_id=? AND group_id=?""",
                    (session_id, group_id),
                ).fetchone()
                if (
                    group_state is not None
                    and group_state["state"] == "inventory-required"
                ):
                    raise FederationValidationError(
                        "manifest-inventory-required",
                        "group_id",
                        str(group_state["reason"]),
                    )
                self._check_control_revision(
                    connection,
                    session_id,
                    expected_control_revision,
                )
                self._require_group(connection, session_id, group_id)
                manifest = AuthoritativeStorageManifest.build(
                    session_id=session_id,
                    group_id=group_id,
                    revision=0,
                    term=0,
                    previous_manifest_hash=None,
                    datasets=(),
                    items=(),
                    updated_at=now,
                )
                connection.execute(
                    """INSERT INTO storage_manifest_revisions
                       (session_id, group_id, revision, term,
                        previous_manifest_hash, manifest_hash, commit_state,
                        manifest_json, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        session_id,
                        group_id,
                        0,
                        0,
                        None,
                        manifest.manifest_hash,
                        manifest.commit_state.value,
                        _json(manifest.to_dict()),
                        _timestamp(manifest.updated_at),
                    ),
                )
                connection.execute(
                    """INSERT INTO storage_manifest_heads
                       (session_id, group_id, revision, manifest_hash)
                       VALUES (?, ?, 0, ?)""",
                    (session_id, group_id, manifest.manifest_hash),
                )
                connection.execute(
                    """INSERT INTO storage_manifest_group_states
                       (session_id, group_id, state, reason, updated_at)
                       VALUES (?, ?, 'ready', NULL, ?)
                       ON CONFLICT(session_id, group_id) DO UPDATE SET
                           state='ready', reason=NULL, updated_at=excluded.updated_at""",
                    (session_id, group_id, _timestamp(now)),
                )
                connection.commit()
                return manifest
            except Exception:
                connection.rollback()
                raise

    def head(
        self,
        session_id: str,
        group_id: str,
    ) -> AuthoritativeStorageManifest:
        session_id = _text(session_id, "session_id")
        group_id = _text(group_id, "group_id")
        with self._connect() as connection:
            return self._head(connection, session_id, group_id)

    def history(
        self,
        session_id: str,
        group_id: str,
    ) -> tuple[AuthoritativeStorageManifest, ...]:
        session_id = _text(session_id, "session_id")
        group_id = _text(group_id, "group_id")
        with self._connect() as connection:
            manifests = self._history(connection, session_id, group_id)
            if not manifests:
                raise FederationValidationError(
                    "manifest-not-found",
                    "group_id",
                    "storage group has no authoritative manifest",
                )
            self._head(connection, session_id, group_id)
            return manifests

    def at_revision(
        self,
        session_id: str,
        group_id: str,
        revision: int,
    ) -> AuthoritativeStorageManifest:
        session_id = _text(session_id, "session_id")
        group_id = _text(group_id, "group_id")
        revision = _uint(revision, "revision")
        with self._connect() as connection:
            self._head(connection, session_id, group_id)
            history = self._history(connection, session_id, group_id)
            if revision >= len(history):
                raise FederationValidationError(
                    "manifest-revision-not-found",
                    "revision",
                    "manifest revision does not exist",
                )
            return history[revision]

    @staticmethod
    def _decode_intent(row: sqlite3.Row) -> ManifestCommitIntent:
        try:
            intent = ManifestCommitIntent.from_dict(
                json.loads(row["intent_json"])
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "malformed-manifest-intent",
                "intent_json",
                str(exc),
            ) from exc
        expected_state = (
            "committed"
            if intent.committed_revision is not None
            else "prepared"
        )
        if (
            row["session_id"] != intent.session_id
            or row["group_id"] != intent.group_id
            or row["item_id"] != intent.item_id
            or row["idempotency_key"] != intent.idempotency_key
            or row["state"] != expected_state
            or int(row["prepared_control_revision"])
            != intent.prepared_control_revision
            or row["committed_revision"] != intent.committed_revision
        ):
            raise FederationValidationError(
                "manifest-intent-row-mismatch",
                "intent",
                "indexed intent metadata does not match canonical JSON",
            )
        return intent

    def prepare_intent(
        self,
        intent: ManifestCommitIntent,
    ) -> ManifestCommitIntent:
        if not isinstance(intent, ManifestCommitIntent):
            raise FederationValidationError(
                "invalid-object",
                "intent",
                "must be ManifestCommitIntent",
            )
        if intent.committed_revision is not None:
            raise FederationValidationError(
                "manifest-intent-already-committed",
                "committed_revision",
                "a new intent must not already be committed",
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """SELECT * FROM storage_manifest_intents
                       WHERE session_id=? AND group_id=? AND item_id=?""",
                    (intent.session_id, intent.group_id, intent.item_id),
                ).fetchone()
                if row is not None:
                    existing = self._decode_intent(row)
                    if (
                        existing.immutable_item_identity()
                        != intent.immutable_item_identity()
                    ):
                        raise FederationValidationError(
                            "manifest-intent-conflict",
                            "item_id",
                            "prepared item identity conflicts with existing intent",
                        )
                    connection.commit()
                    return existing
                self._check_control_revision(
                    connection,
                    intent.session_id,
                    intent.prepared_control_revision,
                )
                self._require_group(
                    connection,
                    intent.session_id,
                    intent.group_id,
                )
                if intent.idempotency_key is not None:
                    by_key = connection.execute(
                        """SELECT item_id FROM storage_manifest_intents
                           WHERE session_id=? AND group_id=?
                             AND idempotency_key=?""",
                        (
                            intent.session_id,
                            intent.group_id,
                            intent.idempotency_key,
                        ),
                    ).fetchone()
                    if by_key is not None:
                        raise FederationValidationError(
                            "manifest-intent-idempotency-conflict",
                            "idempotency_key",
                            "idempotency identity belongs to another intent",
                        )
                connection.execute(
                    """INSERT INTO storage_manifest_intents
                       (session_id, group_id, item_id, idempotency_key,
                        intent_json, state, prepared_control_revision,
                        committed_revision)
                       VALUES (?, ?, ?, ?, ?, 'prepared', ?, NULL)""",
                    (
                        intent.session_id,
                        intent.group_id,
                        intent.item_id,
                        intent.idempotency_key,
                        _json(intent.to_dict()),
                        intent.prepared_control_revision,
                    ),
                )
                connection.commit()
                return intent
            except Exception:
                connection.rollback()
                raise

    def intent(
        self,
        session_id: str,
        group_id: str,
        item_id: str,
    ) -> ManifestCommitIntent:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM storage_manifest_intents
                   WHERE session_id=? AND group_id=? AND item_id=?""",
                (
                    _text(session_id, "session_id"),
                    _text(group_id, "group_id"),
                    _text(item_id, "item_id"),
                ),
            ).fetchone()
        if row is None:
            raise FederationValidationError(
                "manifest-intent-not-found",
                "item_id",
                "manifest commit intent does not exist",
            )
        return self._decode_intent(row)

    def pending_intents(
        self,
        *,
        primary_provider_id: str | None = None,
    ) -> tuple[ManifestCommitIntent, ...]:
        query = (
            "SELECT * FROM storage_manifest_intents "
            "WHERE state='prepared' ORDER BY session_id,group_id,item_id"
        )
        with self._connect() as connection:
            intents = tuple(
                self._decode_intent(row)
                for row in connection.execute(query).fetchall()
            )
        if primary_provider_id is None:
            return intents
        provider_id = _text(primary_provider_id, "primary_provider_id")
        return tuple(
            intent
            for intent in intents
            if intent.primary_provider_id == provider_id
        )

    def finalize_intent(
        self,
        intent: ManifestCommitIntent,
        *,
        acknowledged_provider_ids: tuple[str, ...],
        expected_control_revision: int,
        manifest_term: int,
        now: datetime,
    ) -> ManifestCommitResult:
        providers = tuple(
            sorted(
                _text(value, "acknowledged_provider_ids")
                for value in acknowledged_provider_ids
            )
        )
        expected_control_revision = _uint(
            expected_control_revision,
            "expected_control_revision",
        )
        manifest_term = _uint(manifest_term, "manifest_term")
        now = _utc(now, "now")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """SELECT * FROM storage_manifest_intents
                       WHERE session_id=? AND group_id=? AND item_id=?""",
                    (intent.session_id, intent.group_id, intent.item_id),
                ).fetchone()
                if row is None:
                    raise FederationValidationError(
                        "manifest-intent-not-found",
                        "item_id",
                        "manifest commit intent does not exist",
                    )
                persisted = self._decode_intent(row)
                if (
                    persisted.immutable_item_identity()
                    != intent.immutable_item_identity()
                ):
                    raise FederationValidationError(
                        "manifest-intent-conflict",
                        "intent",
                        "finalization identity differs from the prepared intent",
                    )
                if (
                    len(providers) != len(set(providers))
                    or persisted.primary_provider_id not in providers
                ):
                    raise FederationValidationError(
                        "invalid-manifest-acknowledgements",
                        "acknowledged_provider_ids",
                        "evidence must uniquely include the prepared primary",
                    )
                replica_evidence = set(providers) - {
                    persisted.primary_provider_id
                }
                if (
                    not replica_evidence.issubset(
                        persisted.assigned_replica_ids
                    )
                    or len(replica_evidence)
                    < persisted.required_replica_acks
                ):
                    raise FederationValidationError(
                        "manifest-commit-not-ready",
                        "acknowledged_provider_ids",
                        "frozen acknowledgement policy is not satisfied",
                    )
                proposal = persisted.to_proposal(
                acknowledged_provider_ids=providers,
                expected_control_revision=expected_control_revision,
                committed_at=now,
                )
                result = self._commit_in_transaction(
                    connection,
                    replace(
                        proposal,
                        term=max(proposal.term, manifest_term),
                    ),
                )
                if (
                    persisted.committed_revision is not None
                    and persisted.committed_revision
                    != result.item_revision
                ):
                    raise FederationValidationError(
                        "manifest-intent-result-conflict",
                        "committed_revision",
                        "intent was finalized at another revision",
                    )
                completed = replace(
                    persisted,
                    committed_revision=result.item_revision,
                )
                connection.execute(
                    """UPDATE storage_manifest_intents
                       SET intent_json=?, state='committed',
                           committed_revision=?
                       WHERE session_id=? AND group_id=? AND item_id=?""",
                    (
                        _json(completed.to_dict()),
                        result.item_revision,
                        persisted.session_id,
                        persisted.group_id,
                        persisted.item_id,
                    ),
                )
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    @staticmethod
    def _dataset_contract(dataset: DatasetManifest) -> tuple[Any, ...]:
        return (
            dataset.dataset_id,
            dataset.schema_name,
            dataset.schema_version,
            dataset.required,
            dataset.source_id,
        )

    @staticmethod
    def _verify_projections(
        connection: sqlite3.Connection,
        manifest: AuthoritativeStorageManifest,
    ) -> None:
        dataset_rows = connection.execute(
            """SELECT dataset_json FROM storage_manifest_datasets
               WHERE session_id=? AND group_id=? ORDER BY dataset_id""",
            (manifest.session_id, manifest.group_id),
        ).fetchall()
        projected_datasets = tuple(
            DatasetManifest.from_dict(json.loads(row["dataset_json"]))
            for row in dataset_rows
        )
        if projected_datasets != manifest.datasets:
            raise FederationValidationError(
                "manifest-projection-mismatch",
                "datasets",
                "dataset projection does not match the authoritative head",
            )
        item_rows = connection.execute(
            """SELECT item_json FROM storage_manifest_items
               WHERE session_id=? AND group_id=? ORDER BY item_id""",
            (manifest.session_id, manifest.group_id),
        ).fetchall()
        projected_items = tuple(
            sorted(
                (
                    ManifestItem.from_dict(json.loads(row["item_json"]))
                    for row in item_rows
                ),
                key=lambda value: (
                    value.dataset_id,
                    value.kind.value,
                    value.item_id,
                ),
            )
        )
        if projected_items != manifest.items:
            raise FederationValidationError(
                "manifest-projection-mismatch",
                "items",
                "item projection does not match the authoritative head",
            )

    @staticmethod
    def _same_item_identity(
        existing: ManifestItem,
        proposed: ManifestItem,
    ) -> bool:
        return (
            existing.item_id == proposed.item_id
            and existing.kind is proposed.kind
            and existing.dataset_id == proposed.dataset_id
            and existing.idempotency_key == proposed.idempotency_key
            and existing.content_hash == proposed.content_hash
            and existing.size_bytes == proposed.size_bytes
            and existing.schema_name == proposed.schema_name
            and existing.schema_version == proposed.schema_version
            and existing.source_id == proposed.source_id
            and existing.first_sequence == proposed.first_sequence
            and existing.last_sequence == proposed.last_sequence
            and set(existing.acknowledged_provider_ids).issubset(
                proposed.acknowledged_provider_ids
            )
        )

    def _commit_in_transaction(
        self,
        connection: sqlite3.Connection,
        proposal: ManifestItemProposal,
    ) -> ManifestCommitResult:
        if not isinstance(proposal, ManifestItemProposal):
            raise FederationValidationError(
                "invalid-object",
                "proposal",
                "must be ManifestItemProposal",
            )
        proposed_item = proposal.to_item()
        proposed_dataset = proposal.to_dataset()
        self._check_control_revision(
            connection,
            proposal.session_id,
            proposal.expected_control_revision,
        )
        self._require_group(
            connection,
            proposal.session_id,
            proposal.group_id,
        )
        head = self._head(
            connection,
            proposal.session_id,
            proposal.group_id,
        )
        self._verify_projections(connection, head)

        dataset_row = connection.execute(
            """SELECT dataset_json FROM storage_manifest_datasets
               WHERE session_id=? AND group_id=? AND dataset_id=?""",
            (
                proposal.session_id,
                proposal.group_id,
                proposal.dataset_id,
            ),
        ).fetchone()
        if dataset_row is not None:
            existing_dataset = DatasetManifest.from_dict(
                json.loads(dataset_row["dataset_json"])
            )
            if self._dataset_contract(
                existing_dataset
            ) != self._dataset_contract(proposed_dataset):
                raise FederationValidationError(
                    "manifest-dataset-conflict",
                    "dataset_id",
                    "dataset schema, source, or required status changed",
                )

        item_row = connection.execute(
            """SELECT item_json, introduced_revision
               FROM storage_manifest_items
               WHERE session_id=? AND group_id=? AND item_id=?""",
            (
                proposal.session_id,
                proposal.group_id,
                proposal.item_id,
            ),
        ).fetchone()
        if item_row is not None:
            existing_item = ManifestItem.from_dict(
                json.loads(item_row["item_json"])
            )
            if not self._same_item_identity(existing_item, proposed_item):
                raise FederationValidationError(
                    "manifest-item-conflict",
                    "item_id",
                    "item identity was reused with conflicting metadata",
                )
            return ManifestCommitResult(
                head,
                int(item_row["introduced_revision"]),
                False,
            )

        if proposal.idempotency_key is not None:
            by_key = connection.execute(
                """SELECT item_id FROM storage_manifest_items
                   WHERE session_id=? AND group_id=?
                     AND idempotency_key=?""",
                (
                    proposal.session_id,
                    proposal.group_id,
                    proposal.idempotency_key,
                ),
            ).fetchone()
            if by_key is not None:
                raise FederationValidationError(
                    "manifest-idempotency-conflict",
                    "idempotency_key",
                    "idempotency identity belongs to another item",
                )

        revision = head.revision + 1
        datasets = head.datasets
        if dataset_row is None:
            datasets = (*datasets, proposed_dataset)
        manifest = AuthoritativeStorageManifest.build(
            session_id=proposal.session_id,
            group_id=proposal.group_id,
            revision=revision,
            term=max(proposal.term, head.term),
            previous_manifest_hash=head.manifest_hash,
            datasets=datasets,
            items=(*head.items, proposed_item),
            updated_at=proposal.committed_at,
        )
        connection.execute(
            """INSERT INTO storage_manifest_revisions
               (session_id, group_id, revision, term,
                previous_manifest_hash, manifest_hash, commit_state,
                manifest_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                proposal.session_id,
                proposal.group_id,
                manifest.revision,
                manifest.term,
                manifest.previous_manifest_hash,
                manifest.manifest_hash,
                manifest.commit_state.value,
                _json(manifest.to_dict()),
                _timestamp(manifest.updated_at),
            ),
        )
        if dataset_row is None:
            connection.execute(
                """INSERT INTO storage_manifest_datasets
                   (session_id, group_id, dataset_id, dataset_json,
                    introduced_revision)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    proposal.session_id,
                    proposal.group_id,
                    proposal.dataset_id,
                    _json(proposed_dataset.to_dict()),
                    revision,
                ),
            )
        connection.execute(
            """INSERT INTO storage_manifest_items
               (session_id, group_id, item_id, idempotency_key,
                item_json, introduced_revision)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                proposal.session_id,
                proposal.group_id,
                proposal.item_id,
                proposal.idempotency_key,
                _json(proposed_item.to_dict()),
                revision,
            ),
        )
        updated = connection.execute(
            """UPDATE storage_manifest_heads
               SET revision=?, manifest_hash=?
               WHERE session_id=? AND group_id=? AND revision=?
                 AND manifest_hash=?""",
            (
                revision,
                manifest.manifest_hash,
                proposal.session_id,
                proposal.group_id,
                head.revision,
                head.manifest_hash,
            ),
        )
        if updated.rowcount != 1:
            raise FederationValidationError(
                "concurrent-manifest-change",
                "revision",
                "manifest head changed while commit was being prepared",
            )
        return ManifestCommitResult(manifest, revision, True)

    def commit(self, proposal: ManifestItemProposal) -> ManifestCommitResult:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = self._commit_in_transaction(connection, proposal)
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise
