"""Restart-safe F8.6 reconstruction of trusted provider runtime bindings.

The coordinator event log, F8.1 enrollment, F8.2 health, F8.3 remote AI
binding, and F8.4 compute activation remain authoritative. This module stores
only a safe replay cursor and immutable binding evidence. It never revives
health, approves providers, installs handlers, dispatches work, invokes models,
or grants artifact or storage authority.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Self

from catalog.ai.remote_provider import (
    RemoteLanguageModelProvider,
    TrustedRemoteLanguageModelBinder,
)
from catalog.ai.runtime_manager import ConfiguredLanguageModelRuntimeManager
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.redaction import (
    contains_nonpublic_location,
    contains_secret_material,
)

from .relay_dispatch import RelayDispatchEndpoint
from .worker_activation import (
    ActivatedComputeWorker,
    ComputeWorkerActivationSnapshot,
    TrustedComputeWorkerBinder,
)

PROVIDER_RECONCILIATION_CHECKPOINT_SCHEMA = (
    "msh.provider-reconciliation-checkpoint.v1"
)
PROVIDER_RECONCILIATION_BINDING_SCHEMA = "msh.provider-reconciliation-binding.v1"
PROVIDER_RECONCILIATION_RESULT_SCHEMA = "msh.provider-reconciliation-result.v1"
PROVIDER_RECONCILIATION_STORE_SCHEMA_VERSION = 1
MAX_RECONCILIATION_TEXT_BYTES = 512
MAX_RECONCILIATION_BINDINGS = 512
MAX_RECONCILIATION_REPLAY_EVENTS = 4096
DEFAULT_RECONCILIATION_REPLAY_PAGE = 256

_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")


class ReconciledBindingKind(str, Enum):
    """Runtime binding types reconstructed by F8.6."""

    REMOTE_AI = "remote-ai"
    COMPUTE = "compute"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _text(value: Any, field: str, *, maximum: int = MAX_RECONCILIATION_TEXT_BYTES) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-reconciliation-text",
            field,
            "must be non-empty trimmed text without control characters",
        )
    if len(value.encode("utf-8")) > maximum:
        raise FederationValidationError(
            "reconciliation-text-too-large",
            field,
            f"must not exceed {maximum} UTF-8 bytes",
        )
    if contains_secret_material(value):
        raise FederationValidationError(
            "secret-reconciliation-text",
            field,
            "must not contain credentials or secret material",
        )
    if contains_nonpublic_location(value):
        raise FederationValidationError(
            "nonpublic-reconciliation-text",
            field,
            "must not expose endpoints, addresses, or backend paths",
        )
    return value


def _optional_text(value: Any, field: str) -> str | None:
    return None if value is None else _text(value, field)


def _non_negative(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    return value


def _positive(value: Any, field: str) -> int:
    value = _non_negative(value, field)
    if value == 0:
        raise FederationValidationError(
            "invalid-positive-integer",
            field,
            "must be a positive integer",
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
        or value.utcoffset().total_seconds() != 0
    ):
        raise FederationValidationError(
            "invalid-timestamp",
            field,
            "must be a UTC timestamp",
        )
    return value.astimezone(timezone.utc)


def _stamp(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


def _schema(value: Any, expected: str) -> None:
    actual = _text(value, "schema")
    if ".v" not in actual:
        raise ProtocolCompatibilityError(
            "unsupported-reconciliation-schema",
            "schema",
            f"expected {expected}",
        )
    name, major = actual.rsplit(".v", 1)
    expected_name, expected_major = expected.rsplit(".v", 1)
    if name != expected_name or not major.isdigit():
        raise ProtocolCompatibilityError(
            "unsupported-reconciliation-schema",
            "schema",
            f"expected {expected}, received {actual!r}",
        )
    if int(major) != int(expected_major):
        raise ProtocolCompatibilityError(
            "unsupported-reconciliation-schema-major",
            "schema",
            f"supported major is {expected_major}, received {major}",
        )


def _fingerprint(value: Any, field: str) -> str:
    value = _text(value, field)
    if _FINGERPRINT.fullmatch(value) is None:
        raise FederationValidationError(
            "invalid-reconciliation-fingerprint",
            field,
            "must be a lowercase SHA-256 identifier",
        )
    return value


def _canonical(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-reconciliation-json",
            "value",
            "must contain JSON-compatible finite values",
        ) from exc


@dataclass(frozen=True)
class ReconciledProviderBinding:
    """Safe immutable evidence for one reconstructed runtime binding."""

    SCHEMA: ClassVar[str] = PROVIDER_RECONCILIATION_BINDING_SCHEMA

    kind: ReconciledBindingKind
    capability_id: str
    node_id: str
    provider_generation: int
    report_revision: int
    enrollment_revision: int
    binding_id: str | None = None
    binding_revision: int | None = None
    descriptor_fingerprint: str | None = None

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "kind", ReconciledBindingKind(self.kind))
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-reconciliation-binding-kind",
                "kind",
                "unknown runtime binding kind",
            ) from exc
        for field in ("capability_id", "node_id"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(
            self,
            "provider_generation",
            _positive(self.provider_generation, "provider_generation"),
        )
        object.__setattr__(
            self,
            "report_revision",
            _non_negative(self.report_revision, "report_revision"),
        )
        object.__setattr__(
            self,
            "enrollment_revision",
            _positive(self.enrollment_revision, "enrollment_revision"),
        )
        object.__setattr__(
            self,
            "binding_id",
            _optional_text(self.binding_id, "binding_id"),
        )
        if self.binding_revision is not None:
            object.__setattr__(
                self,
                "binding_revision",
                _positive(self.binding_revision, "binding_revision"),
            )
        if self.descriptor_fingerprint is not None:
            object.__setattr__(
                self,
                "descriptor_fingerprint",
                _fingerprint(
                    self.descriptor_fingerprint,
                    "descriptor_fingerprint",
                ),
            )
        compute_fields = (
            self.binding_id,
            self.binding_revision,
            self.descriptor_fingerprint,
        )
        if self.kind is ReconciledBindingKind.REMOTE_AI and any(
            value is not None for value in compute_fields
        ):
            raise FederationValidationError(
                "unexpected-ai-binding-fields",
                "binding",
                "remote AI evidence must not contain compute binding fields",
            )
        if self.kind is ReconciledBindingKind.COMPUTE and any(
            value is None for value in compute_fields
        ):
            raise FederationValidationError(
                "missing-compute-binding-fields",
                "binding",
                "compute evidence requires binding and descriptor fencing",
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "kind": self.kind.value,
            "capability_id": self.capability_id,
            "node_id": self.node_id,
            "provider_generation": self.provider_generation,
            "report_revision": self.report_revision,
            "enrollment_revision": self.enrollment_revision,
            "binding_id": self.binding_id,
            "binding_revision": self.binding_revision,
            "descriptor_fingerprint": self.descriptor_fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-reconciliation-binding",
                "binding",
                "must be an object",
            )
        _schema(value.get("schema"), cls.SCHEMA)
        names = (
            "kind",
            "capability_id",
            "node_id",
            "provider_generation",
            "report_revision",
            "enrollment_revision",
            "binding_id",
            "binding_revision",
            "descriptor_fingerprint",
        )
        for name in names:
            if name not in value:
                raise FederationValidationError("missing-field", name, "is required")
        return cls(**{name: value[name] for name in names})


@dataclass(frozen=True)
class ProviderReconciliationCheckpoint:
    """Restart-safe cursor and exact runtime evidence for one local actor."""

    SCHEMA: ClassVar[str] = PROVIDER_RECONCILIATION_CHECKPOINT_SCHEMA

    session_id: str
    actor_node_id: str
    checkpoint_revision: int
    last_applied_revision: int
    bindings: tuple[ReconciledProviderBinding, ...]
    updated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "session_id", _text(self.session_id, "session_id"))
        object.__setattr__(
            self,
            "actor_node_id",
            _text(self.actor_node_id, "actor_node_id"),
        )
        object.__setattr__(
            self,
            "checkpoint_revision",
            _non_negative(self.checkpoint_revision, "checkpoint_revision"),
        )
        object.__setattr__(
            self,
            "last_applied_revision",
            _non_negative(self.last_applied_revision, "last_applied_revision"),
        )
        bindings = tuple(
            item
            if isinstance(item, ReconciledProviderBinding)
            else ReconciledProviderBinding.from_dict(item)
            for item in self.bindings
        )
        if len(bindings) > MAX_RECONCILIATION_BINDINGS:
            raise FederationValidationError(
                "too-many-reconciliation-bindings",
                "bindings",
                f"must not exceed {MAX_RECONCILIATION_BINDINGS}",
            )
        ordered = tuple(
            sorted(bindings, key=lambda item: (item.kind.value, item.capability_id))
        )
        if bindings != ordered:
            raise FederationValidationError(
                "unordered-reconciliation-bindings",
                "bindings",
                "must be deterministically sorted by kind and capability ID",
            )
        keys = tuple((item.kind, item.capability_id) for item in bindings)
        if len(keys) != len(set(keys)):
            raise FederationValidationError(
                "duplicate-reconciliation-binding",
                "bindings",
                "contains duplicate runtime binding identities",
            )
        object.__setattr__(self, "bindings", bindings)
        object.__setattr__(self, "updated_at", _utc(self.updated_at, "updated_at"))

    @property
    def ai_bindings(self) -> tuple[ReconciledProviderBinding, ...]:
        return tuple(
            item
            for item in self.bindings
            if item.kind is ReconciledBindingKind.REMOTE_AI
        )

    @property
    def compute_bindings(self) -> tuple[ReconciledProviderBinding, ...]:
        return tuple(
            item
            for item in self.bindings
            if item.kind is ReconciledBindingKind.COMPUTE
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "session_id": self.session_id,
            "actor_node_id": self.actor_node_id,
            "checkpoint_revision": self.checkpoint_revision,
            "last_applied_revision": self.last_applied_revision,
            "bindings": [item.to_dict() for item in self.bindings],
            "updated_at": _stamp(self.updated_at),
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-reconciliation-checkpoint",
                "checkpoint",
                "must be an object",
            )
        _schema(value.get("schema"), cls.SCHEMA)
        names = (
            "session_id",
            "actor_node_id",
            "checkpoint_revision",
            "last_applied_revision",
            "bindings",
            "updated_at",
        )
        for name in names:
            if name not in value:
                raise FederationValidationError("missing-field", name, "is required")
        if not isinstance(value["bindings"], list):
            raise FederationValidationError(
                "invalid-reconciliation-bindings",
                "bindings",
                "must be an array",
            )
        return cls(
            session_id=value["session_id"],
            actor_node_id=value["actor_node_id"],
            checkpoint_revision=value["checkpoint_revision"],
            last_applied_revision=value["last_applied_revision"],
            bindings=tuple(
                ReconciledProviderBinding.from_dict(item)
                for item in value["bindings"]
            ),
            updated_at=value["updated_at"],
        )


@dataclass(frozen=True)
class ProviderReconciliationResult:
    """Safe outcome of one bounded reconciliation pass."""

    SCHEMA: ClassVar[str] = PROVIDER_RECONCILIATION_RESULT_SCHEMA

    session_id: str
    actor_node_id: str
    checkpoint_revision: int
    previous_event_revision: int
    applied_event_revision: int
    replayed_event_count: int
    ai_provider_ids: tuple[str, ...]
    compute_provider_ids: tuple[str, ...]
    changed: bool
    reason_code: str

    def __post_init__(self) -> None:
        for field in ("session_id", "actor_node_id", "reason_code"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        for field in (
            "checkpoint_revision",
            "previous_event_revision",
            "applied_event_revision",
            "replayed_event_count",
        ):
            object.__setattr__(
                self,
                field,
                _non_negative(getattr(self, field), field),
            )
        if self.applied_event_revision < self.previous_event_revision:
            raise FederationValidationError(
                "reconciliation-revision-regressed",
                "applied_event_revision",
                "must not precede the previous event revision",
            )
        for field in ("ai_provider_ids", "compute_provider_ids"):
            values = tuple(_text(item, field) for item in getattr(self, field))
            if values != tuple(sorted(values)) or len(values) != len(set(values)):
                raise FederationValidationError(
                    "invalid-reconciliation-provider-ids",
                    field,
                    "must be unique and deterministically sorted",
                )
            object.__setattr__(self, field, values)
        if not isinstance(self.changed, bool):
            raise FederationValidationError(
                "invalid-reconciliation-changed",
                "changed",
                "must be boolean",
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "session_id": self.session_id,
            "actor_node_id": self.actor_node_id,
            "checkpoint_revision": self.checkpoint_revision,
            "previous_event_revision": self.previous_event_revision,
            "applied_event_revision": self.applied_event_revision,
            "replayed_event_count": self.replayed_event_count,
            "ai_provider_ids": list(self.ai_provider_ids),
            "compute_provider_ids": list(self.compute_provider_ids),
            "changed": self.changed,
            "reason_code": self.reason_code,
        }


class SQLiteProviderReconciliationStore:
    """Transactional checkpoint storage with optimistic revision fencing."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        database = self._connect()
        try:
            database.execute("BEGIN IMMEDIATE")
            yield database
            database.commit()
        except BaseException:
            database.rollback()
            raise
        finally:
            database.close()

    def initialize(self) -> None:
        with self.transaction() as database:
            database.executescript(
                """
                CREATE TABLE IF NOT EXISTS provider_reconciliation_meta(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS provider_reconciliation_checkpoints(
                    session_id TEXT NOT NULL,
                    actor_node_id TEXT NOT NULL,
                    checkpoint_revision INTEGER NOT NULL,
                    last_applied_revision INTEGER NOT NULL,
                    checkpoint_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id,actor_node_id)
                );
                """
            )
            row = database.execute(
                """
                SELECT value FROM provider_reconciliation_meta
                WHERE key='schema_version'
                """
            ).fetchone()
            if row is None:
                database.execute(
                    """
                    INSERT INTO provider_reconciliation_meta(key,value)
                    VALUES('schema_version',?)
                    """,
                    (str(PROVIDER_RECONCILIATION_STORE_SCHEMA_VERSION),),
                )
            elif row["value"] != str(PROVIDER_RECONCILIATION_STORE_SCHEMA_VERSION):
                raise FederationOperationError(
                    "unsupported-reconciliation-store-schema",
                    "provider reconciliation database uses an unsupported schema",
                )

    def get(
        self,
        *,
        session_id: str,
        actor_node_id: str,
    ) -> ProviderReconciliationCheckpoint | None:
        session_id = _text(session_id, "session_id")
        actor_node_id = _text(actor_node_id, "actor_node_id")
        with self._connect() as database:
            row = database.execute(
                """
                SELECT checkpoint_json
                FROM provider_reconciliation_checkpoints
                WHERE session_id=? AND actor_node_id=?
                """,
                (session_id, actor_node_id),
            ).fetchone()
        if row is None:
            return None
        checkpoint = ProviderReconciliationCheckpoint.from_dict(
            json.loads(row["checkpoint_json"])
        )
        if (
            checkpoint.session_id != session_id
            or checkpoint.actor_node_id != actor_node_id
        ):
            raise FederationOperationError(
                "reconciliation-checkpoint-identity-conflict",
                "stored checkpoint identity differs from its database key",
            )
        return checkpoint

    def save(
        self,
        checkpoint: ProviderReconciliationCheckpoint,
        *,
        expected_checkpoint_revision: int,
    ) -> ProviderReconciliationCheckpoint:
        if not isinstance(checkpoint, ProviderReconciliationCheckpoint):
            raise FederationValidationError(
                "invalid-reconciliation-checkpoint",
                "checkpoint",
                "must be a ProviderReconciliationCheckpoint",
            )
        expected_checkpoint_revision = _non_negative(
            expected_checkpoint_revision,
            "expected_checkpoint_revision",
        )
        if checkpoint.checkpoint_revision != expected_checkpoint_revision + 1:
            raise FederationValidationError(
                "invalid-reconciliation-checkpoint-revision",
                "checkpoint_revision",
                "must advance exactly once from the expected revision",
            )
        with self.transaction() as database:
            row = database.execute(
                """
                SELECT checkpoint_revision,last_applied_revision
                FROM provider_reconciliation_checkpoints
                WHERE session_id=? AND actor_node_id=?
                """,
                (checkpoint.session_id, checkpoint.actor_node_id),
            ).fetchone()
            current_revision = 0 if row is None else row["checkpoint_revision"]
            if current_revision != expected_checkpoint_revision:
                raise FederationOperationError(
                    "stale-reconciliation-checkpoint",
                    "checkpoint changed since reconciliation started",
                    "expected_checkpoint_revision",
                )
            if (
                row is not None
                and checkpoint.last_applied_revision < row["last_applied_revision"]
            ):
                raise FederationOperationError(
                    "reconciliation-event-revision-regressed",
                    "checkpoint event cursor cannot move backward",
                    "last_applied_revision",
                )
            encoded = _canonical(checkpoint.to_dict())
            database.execute(
                """
                INSERT INTO provider_reconciliation_checkpoints(
                    session_id,actor_node_id,checkpoint_revision,
                    last_applied_revision,checkpoint_json,updated_at
                ) VALUES(?,?,?,?,?,?)
                ON CONFLICT(session_id,actor_node_id) DO UPDATE SET
                    checkpoint_revision=excluded.checkpoint_revision,
                    last_applied_revision=excluded.last_applied_revision,
                    checkpoint_json=excluded.checkpoint_json,
                    updated_at=excluded.updated_at
                """,
                (
                    checkpoint.session_id,
                    checkpoint.actor_node_id,
                    checkpoint.checkpoint_revision,
                    checkpoint.last_applied_revision,
                    encoded,
                    _stamp(checkpoint.updated_at),
                ),
            )
        return checkpoint


class TrustedProviderRuntimeReconciler:
    """Rebuild only current F8.3 and F8.4 runtime bindings after restart."""

    def __init__(
        self,
        coordinator: SessionCoordinator,
        checkpoint_store: SQLiteProviderReconciliationStore,
        *,
        session_id: str,
        actor_node_id: str,
        ai_binder: TrustedRemoteLanguageModelBinder | None = None,
        ai_runtime_manager: ConfiguredLanguageModelRuntimeManager | None = None,
        compute_binder: TrustedComputeWorkerBinder | None = None,
        compute_endpoint: RelayDispatchEndpoint | None = None,
        replay_page_size: int = DEFAULT_RECONCILIATION_REPLAY_PAGE,
        maximum_replay_events: int = MAX_RECONCILIATION_REPLAY_EVENTS,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not isinstance(coordinator, SessionCoordinator):
            raise FederationValidationError(
                "invalid-reconciliation-coordinator",
                "coordinator",
                "must be a SessionCoordinator",
            )
        if not isinstance(checkpoint_store, SQLiteProviderReconciliationStore):
            raise FederationValidationError(
                "invalid-reconciliation-store",
                "checkpoint_store",
                "must be a SQLiteProviderReconciliationStore",
            )
        if (ai_binder is None) != (ai_runtime_manager is None):
            raise FederationValidationError(
                "incomplete-ai-reconciliation-binding",
                "ai_binder",
                "AI binder and runtime manager must be supplied together",
            )
        if (compute_binder is None) != (compute_endpoint is None):
            raise FederationValidationError(
                "incomplete-compute-reconciliation-binding",
                "compute_binder",
                "compute binder and relay endpoint must be supplied together",
            )
        if ai_binder is None and compute_binder is None:
            raise FederationValidationError(
                "empty-reconciliation-runtime",
                "runtime",
                "at least one AI or compute runtime binding is required",
            )
        if (
            isinstance(replay_page_size, bool)
            or not isinstance(replay_page_size, int)
            or replay_page_size <= 0
        ):
            raise FederationValidationError(
                "invalid-reconciliation-replay-page",
                "replay_page_size",
                "must be a positive integer",
            )
        if (
            isinstance(maximum_replay_events, bool)
            or not isinstance(maximum_replay_events, int)
            or maximum_replay_events <= 0
        ):
            raise FederationValidationError(
                "invalid-reconciliation-replay-limit",
                "maximum_replay_events",
                "must be a positive integer",
            )
        self.coordinator = coordinator
        self.checkpoint_store = checkpoint_store
        self.session_id = _text(session_id, "session_id")
        self.actor_node_id = _text(actor_node_id, "actor_node_id")
        self.ai_binder = ai_binder
        self.ai_runtime_manager = ai_runtime_manager
        self.compute_binder = compute_binder
        self.compute_endpoint = compute_endpoint
        self.replay_page_size = replay_page_size
        self.maximum_replay_events = maximum_replay_events
        self._clock = clock
        self._validate_component_identity()

    def _validate_component_identity(self) -> None:
        if self.ai_binder is not None and self.ai_runtime_manager is not None:
            authority = self.ai_binder.authority
            if (
                authority.session_id != self.session_id
                or authority.actor_node_id != self.actor_node_id
                or self.ai_runtime_manager.session_id != self.session_id
            ):
                raise FederationValidationError(
                    "ai-reconciliation-identity-mismatch",
                    "ai_binder",
                    "AI authority and runtime manager must match session and actor",
                )
        if self.compute_binder is not None and self.compute_endpoint is not None:
            authority = self.compute_binder.authority
            endpoint_node = getattr(self.compute_endpoint.relay_client, "node_id", None)
            if (
                authority.session_id != self.session_id
                or authority.local_node_id != self.actor_node_id
                or endpoint_node != self.actor_node_id
            ):
                raise FederationValidationError(
                    "compute-reconciliation-identity-mismatch",
                    "compute_binder",
                    "compute authority and relay endpoint must match session and actor",
                )

    def _now(self) -> datetime:
        return _utc(self._clock(), "clock")

    def _empty_checkpoint(self) -> ProviderReconciliationCheckpoint:
        return ProviderReconciliationCheckpoint(
            session_id=self.session_id,
            actor_node_id=self.actor_node_id,
            checkpoint_revision=0,
            last_applied_revision=0,
            bindings=(),
            updated_at=self._now(),
        )

    def _checkpoint(self) -> ProviderReconciliationCheckpoint:
        return self.checkpoint_store.get(
            session_id=self.session_id,
            actor_node_id=self.actor_node_id,
        ) or self._empty_checkpoint()

    @staticmethod
    def _binding_ids(
        checkpoint: ProviderReconciliationCheckpoint,
        kind: ReconciledBindingKind,
    ) -> tuple[str, ...]:
        return tuple(
            item.capability_id for item in checkpoint.bindings if item.kind is kind
        )

    def _clear_owned(self, checkpoint: ProviderReconciliationCheckpoint) -> None:
        if self.ai_runtime_manager is not None:
            self.ai_runtime_manager.replace_registered(
                (),
                replace_capability_ids=self._binding_ids(
                    checkpoint,
                    ReconciledBindingKind.REMOTE_AI,
                ),
            )
        if self.compute_endpoint is not None:
            self.compute_endpoint.replace_workers(
                {},
                replace_provider_ids=self._binding_ids(
                    checkpoint,
                    ReconciledBindingKind.COMPUTE,
                ),
            )

    def _replay(
        self,
        start_revision: int,
    ) -> tuple[int, int]:
        cursor = _non_negative(start_revision, "last_applied_revision")
        replayed = 0
        while True:
            page, current_revision = self.coordinator.replay_page(
                session_id=self.session_id,
                actor_node_id=self.actor_node_id,
                last_applied_revision=cursor,
                limit=self.replay_page_size,
            )
            if page:
                expected = cursor + 1
                for event in page:
                    if event.session_id != self.session_id or event.revision != expected:
                        raise FederationOperationError(
                            "reconciliation-event-order-invalid",
                            "coordinator replay did not return exact contiguous session events",
                            "revision",
                        )
                    expected += 1
                cursor = page[-1].revision
                replayed += len(page)
                if replayed > self.maximum_replay_events:
                    raise FederationOperationError(
                        "reconciliation-replay-too-large",
                        "bounded reconciliation replay limit was exceeded",
                        "last_applied_revision",
                    )
            if cursor == current_revision:
                return cursor, replayed
            if not page or cursor > current_revision:
                raise FederationOperationError(
                    "reconciliation-event-gap",
                    "coordinator replay could not advance to its current revision",
                    "revision",
                )

    @staticmethod
    def _ai_evidence(
        provider: RemoteLanguageModelProvider,
    ) -> ReconciledProviderBinding:
        return ReconciledProviderBinding(
            kind=ReconciledBindingKind.REMOTE_AI,
            capability_id=provider.capability_id,
            node_id=provider.node_id,
            provider_generation=provider.provider_generation,
            report_revision=provider.health_report_revision,
            enrollment_revision=provider.enrollment_revision,
        )

    @staticmethod
    def _compute_evidence(
        snapshot: ComputeWorkerActivationSnapshot,
    ) -> ReconciledProviderBinding:
        return ReconciledProviderBinding(
            kind=ReconciledBindingKind.COMPUTE,
            capability_id=snapshot.provider_id,
            node_id=snapshot.node_id,
            provider_generation=snapshot.provider_generation,
            report_revision=snapshot.report_revision,
            enrollment_revision=snapshot.record.enrollment_revision,
            binding_id=snapshot.binding_id,
            binding_revision=snapshot.binding_revision,
            descriptor_fingerprint=snapshot.descriptor.descriptor_fingerprint,
        )

    def _desired_ai(
        self,
    ) -> tuple[tuple[RemoteLanguageModelProvider, ...], tuple[ReconciledProviderBinding, ...]]:
        if self.ai_binder is None:
            return (), ()
        providers = self.ai_binder.bind_all()
        evidence = tuple(self._ai_evidence(provider) for provider in providers)
        return providers, evidence

    def _desired_compute(
        self,
    ) -> tuple[tuple[ActivatedComputeWorker, ...], tuple[ReconciledProviderBinding, ...]]:
        if self.compute_binder is None:
            return (), ()
        workers = self.compute_binder.activate_all()
        evidence = tuple(self._compute_evidence(worker.snapshot) for worker in workers)
        return workers, evidence

    def _runtime_matches(
        self,
        bindings: tuple[ReconciledProviderBinding, ...],
    ) -> bool:
        ai_expected = tuple(
            item
            for item in bindings
            if item.kind is ReconciledBindingKind.REMOTE_AI
        )
        compute_expected = tuple(
            item for item in bindings if item.kind is ReconciledBindingKind.COMPUTE
        )
        if self.ai_runtime_manager is not None:
            actual = tuple(
                self._ai_evidence(provider)
                for provider in self.ai_runtime_manager.additional_providers()
                if provider.capability_id
                in {item.capability_id for item in ai_expected}
            )
            if actual != ai_expected:
                return False
        elif ai_expected:
            return False
        if self.compute_endpoint is not None:
            actual_compute: list[ReconciledProviderBinding] = []
            for expected in compute_expected:
                worker = self.compute_endpoint.workers.get(expected.capability_id)
                if not isinstance(worker, ActivatedComputeWorker):
                    return False
                actual_compute.append(self._compute_evidence(worker.snapshot))
            if tuple(actual_compute) != compute_expected:
                return False
        elif compute_expected:
            return False
        return True

    def _validate_desired(
        self,
        bindings: tuple[ReconciledProviderBinding, ...],
    ) -> None:
        for item in bindings:
            if item.kind is ReconciledBindingKind.REMOTE_AI:
                if self.ai_binder is None:
                    raise FederationOperationError(
                        "missing-ai-reconciliation-authority",
                        "AI binding exists without an AI authority",
                    )
                record = self.ai_binder.authority.current_record(
                    item.capability_id,
                    provider_generation=item.provider_generation,
                    report_revision=item.report_revision,
                )
                if (
                    record.node_id != item.node_id
                    or record.enrollment_revision != item.enrollment_revision
                ):
                    raise FederationOperationError(
                        "ai-reconciliation-evidence-changed",
                        "remote AI authority changed during reconciliation",
                        "capability_id",
                    )
            else:
                if self.compute_binder is None:
                    raise FederationOperationError(
                        "missing-compute-reconciliation-authority",
                        "compute binding exists without a compute authority",
                    )
                snapshot = self.compute_binder.authority.current_snapshot(
                    item.capability_id,
                    provider_generation=item.provider_generation,
                    report_revision=item.report_revision,
                    expected_binding_id=item.binding_id,
                )
                current = self._compute_evidence(snapshot)
                if current != item:
                    raise FederationOperationError(
                        "compute-reconciliation-evidence-changed",
                        "compute activation changed during reconciliation",
                        "capability_id",
                    )

    def reconcile(self) -> ProviderReconciliationResult:
        checkpoint = self._checkpoint()
        try:
            self.coordinator.store.require_membership(
                session_id=self.session_id,
                node_id=self.actor_node_id,
            )
        except Exception:
            self._clear_owned(checkpoint)
            raise

        applied_revision, replayed_count = self._replay(
            checkpoint.last_applied_revision
        )
        self.coordinator.store.require_membership(
            session_id=self.session_id,
            node_id=self.actor_node_id,
        )

        ai_providers, ai_bindings = self._desired_ai()
        compute_workers, compute_bindings = self._desired_compute()
        bindings = tuple(
            sorted(
                (*ai_bindings, *compute_bindings),
                key=lambda item: (item.kind.value, item.capability_id),
            )
        )
        same_authority = (
            applied_revision == checkpoint.last_applied_revision
            and bindings == checkpoint.bindings
        )
        if same_authority and self._runtime_matches(bindings):
            return ProviderReconciliationResult(
                session_id=self.session_id,
                actor_node_id=self.actor_node_id,
                checkpoint_revision=checkpoint.checkpoint_revision,
                previous_event_revision=checkpoint.last_applied_revision,
                applied_event_revision=applied_revision,
                replayed_event_count=replayed_count,
                ai_provider_ids=tuple(item.capability_id for item in ai_bindings),
                compute_provider_ids=tuple(
                    item.capability_id for item in compute_bindings
                ),
                changed=False,
                reason_code="reconciliation-unchanged",
            )

        prior_ai: tuple[Any, ...] = ()
        prior_compute: dict[str, Any] = {}
        previous_ai_ids = self._binding_ids(
            checkpoint,
            ReconciledBindingKind.REMOTE_AI,
        )
        previous_compute_ids = self._binding_ids(
            checkpoint,
            ReconciledBindingKind.COMPUTE,
        )
        desired_ai_ids = tuple(item.capability_id for item in ai_bindings)
        desired_compute_ids = tuple(item.capability_id for item in compute_bindings)
        ai_replace_ids = tuple(sorted({*previous_ai_ids, *desired_ai_ids}))
        compute_replace_ids = tuple(
            sorted({*previous_compute_ids, *desired_compute_ids})
        )
        try:
            if self.ai_runtime_manager is not None:
                prior_ai = self.ai_runtime_manager.replace_registered(
                    ai_providers,
                    replace_capability_ids=ai_replace_ids,
                )
            if self.compute_endpoint is not None:
                prior_compute = self.compute_endpoint.replace_workers(
                    {
                        worker.snapshot.provider_id: worker
                        for worker in compute_workers
                    },
                    replace_provider_ids=compute_replace_ids,
                )
            self._validate_desired(bindings)
            self.coordinator.store.require_membership(
                session_id=self.session_id,
                node_id=self.actor_node_id,
            )
            next_checkpoint = ProviderReconciliationCheckpoint(
                session_id=self.session_id,
                actor_node_id=self.actor_node_id,
                checkpoint_revision=checkpoint.checkpoint_revision + 1,
                last_applied_revision=applied_revision,
                bindings=bindings,
                updated_at=self._now(),
            )
            self.checkpoint_store.save(
                next_checkpoint,
                expected_checkpoint_revision=checkpoint.checkpoint_revision,
            )
        except BaseException:
            if self.compute_endpoint is not None:
                self.compute_endpoint.replace_workers(
                    prior_compute,
                    replace_provider_ids=compute_replace_ids,
                )
            if self.ai_runtime_manager is not None:
                self.ai_runtime_manager.replace_registered(
                    prior_ai,
                    replace_capability_ids=ai_replace_ids,
                )
            raise

        reason_code = (
            "runtime-rebuilt"
            if replayed_count == 0 and bindings == checkpoint.bindings
            else "events-replayed-and-runtime-rebuilt"
            if replayed_count
            else "runtime-authority-rebuilt"
        )
        return ProviderReconciliationResult(
            session_id=self.session_id,
            actor_node_id=self.actor_node_id,
            checkpoint_revision=checkpoint.checkpoint_revision + 1,
            previous_event_revision=checkpoint.last_applied_revision,
            applied_event_revision=applied_revision,
            replayed_event_count=replayed_count,
            ai_provider_ids=desired_ai_ids,
            compute_provider_ids=desired_compute_ids,
            changed=True,
            reason_code=reason_code,
        )
