"""Provider-enforced durable fencing for Phase E storage promotion."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import FederationValidationError

PROVIDER_FENCE_SCHEMA = "msh.storage_provider_fence.v1"


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(char) < 32 for char in value):
        raise FederationValidationError("invalid-id", field, "must be non-empty opaque text")
    return value


def _term(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError("invalid-term", field, "must be a non-negative integer")
    return value


def _utc(value: datetime | str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError("invalid-timestamp", "persisted_at", "must be RFC 3339") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError("invalid-timestamp", "persisted_at", "must be timezone-aware")
    return value.astimezone(timezone.utc)


def _identity(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class StorageFenceCommand:
    promotion_id: str
    session_id: str
    group_id: str
    provider_id: str
    previous_term: int
    fencing_high_water_term: int
    idempotency_key: str

    def __post_init__(self) -> None:
        for field in ("promotion_id", "session_id", "group_id", "provider_id", "idempotency_key"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(self, "previous_term", _term(self.previous_term, "previous_term"))
        object.__setattr__(
            self,
            "fencing_high_water_term",
            _term(self.fencing_high_water_term, "fencing_high_water_term"),
        )
        if self.fencing_high_water_term < self.previous_term:
            raise FederationValidationError(
                "stale-fencing-value",
                "fencing_high_water_term",
                "must fence the previous authority term",
            )

    def binding(self) -> dict[str, Any]:
        return {
            "promotion_id": self.promotion_id,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "provider_id": self.provider_id,
            "previous_term": self.previous_term,
            "fencing_high_water_term": self.fencing_high_water_term,
            "idempotency_key": self.idempotency_key,
        }


@dataclass(frozen=True)
class StorageFenceAcknowledgement:
    schema: str
    promotion_id: str
    session_id: str
    group_id: str
    provider_id: str
    previous_term: int
    fencing_high_water_term: int
    idempotency_key: str
    acknowledgement_id: str
    persisted_at: datetime

    def __post_init__(self) -> None:
        if self.schema != PROVIDER_FENCE_SCHEMA:
            raise FederationValidationError("unsupported-schema", "schema", f"expected {PROVIDER_FENCE_SCHEMA}")
        for field in (
            "promotion_id",
            "session_id",
            "group_id",
            "provider_id",
            "idempotency_key",
            "acknowledgement_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(self, "previous_term", _term(self.previous_term, "previous_term"))
        object.__setattr__(
            self,
            "fencing_high_water_term",
            _term(self.fencing_high_water_term, "fencing_high_water_term"),
        )
        object.__setattr__(self, "persisted_at", _utc(self.persisted_at))


class ProviderFenceStore:
    """Provider-local fence ledger; acknowledgement follows durable persistence."""

    def __init__(self, database: Path | str, *, session_id: str, provider_id: str) -> None:
        self.database = str(database)
        self.session_id = _text(session_id, "session_id")
        self.provider_id = _text(provider_id, "provider_id")
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS provider_storage_fences (
                       session_id TEXT NOT NULL,
                       group_id TEXT NOT NULL,
                       provider_id TEXT NOT NULL,
                       promotion_id TEXT NOT NULL,
                       previous_term INTEGER NOT NULL CHECK(previous_term >= 0),
                       fencing_high_water_term INTEGER NOT NULL CHECK(fencing_high_water_term >= previous_term),
                       idempotency_key TEXT NOT NULL,
                       acknowledgement_id TEXT NOT NULL,
                       persisted_at TEXT NOT NULL,
                       PRIMARY KEY(session_id, group_id, provider_id),
                       UNIQUE(session_id, group_id, provider_id, promotion_id)
                   )"""
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def acknowledgement_identity(command: StorageFenceCommand) -> str:
        return _identity({"schema": PROVIDER_FENCE_SCHEMA, **command.binding()})

    def apply(self, command: StorageFenceCommand, *, now: datetime | None = None) -> StorageFenceAcknowledgement:
        if command.session_id != self.session_id or command.provider_id != self.provider_id:
            raise FederationValidationError("fence-target-mismatch", "provider_id", "command is not for this provider")
        persisted_at = _utc(now or datetime.now(timezone.utc))
        acknowledgement_id = self.acknowledgement_identity(command)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """SELECT * FROM provider_storage_fences
                       WHERE session_id=? AND group_id=? AND provider_id=?""",
                    (self.session_id, command.group_id, self.provider_id),
                ).fetchone()
                if row is not None:
                    existing = self._from_row(row)
                    if (
                        existing.promotion_id == command.promotion_id
                        and existing.previous_term == command.previous_term
                        and existing.fencing_high_water_term == command.fencing_high_water_term
                        and existing.idempotency_key == command.idempotency_key
                    ):
                        connection.commit()
                        return existing
                    if command.fencing_high_water_term <= existing.fencing_high_water_term:
                        raise FederationValidationError(
                            "stale-fencing-value",
                            "fencing_high_water_term",
                            "must exceed the provider's durable fencing high-water mark",
                        )
                connection.execute(
                    """INSERT INTO provider_storage_fences
                       (session_id, group_id, provider_id, promotion_id, previous_term,
                        fencing_high_water_term, idempotency_key, acknowledgement_id, persisted_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(session_id, group_id, provider_id) DO UPDATE SET
                           promotion_id=excluded.promotion_id,
                           previous_term=excluded.previous_term,
                           fencing_high_water_term=excluded.fencing_high_water_term,
                           idempotency_key=excluded.idempotency_key,
                           acknowledgement_id=excluded.acknowledgement_id,
                           persisted_at=excluded.persisted_at""",
                    (
                        self.session_id,
                        command.group_id,
                        self.provider_id,
                        command.promotion_id,
                        command.previous_term,
                        command.fencing_high_water_term,
                        command.idempotency_key,
                        acknowledgement_id,
                        persisted_at.isoformat().replace("+00:00", "Z"),
                    ),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return StorageFenceAcknowledgement(
            schema=PROVIDER_FENCE_SCHEMA,
            acknowledgement_id=acknowledgement_id,
            persisted_at=persisted_at,
            **command.binding(),
        )

    def high_water_term(self, group_id: str) -> int | None:
        acknowledgement = self.acknowledgement(group_id)
        return None if acknowledgement is None else acknowledgement.fencing_high_water_term

    def acknowledgement(self, group_id: str) -> StorageFenceAcknowledgement | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM provider_storage_fences
                   WHERE session_id=? AND group_id=? AND provider_id=?""",
                (self.session_id, group_id, self.provider_id),
            ).fetchone()
        return None if row is None else self._from_row(row)

    def validate_grant_term(self, group_id: str, term: int) -> None:
        term = _term(term, "term")
        high_water = self.high_water_term(group_id)
        if high_water is not None and term <= high_water:
            raise FederationValidationError(
                "stale-term",
                "term",
                "provider has durably fenced this term or a later term",
            )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> StorageFenceAcknowledgement:
        try:
            acknowledgement = StorageFenceAcknowledgement(
                schema=PROVIDER_FENCE_SCHEMA,
                promotion_id=row["promotion_id"],
                session_id=row["session_id"],
                group_id=row["group_id"],
                provider_id=row["provider_id"],
                previous_term=row["previous_term"],
                fencing_high_water_term=row["fencing_high_water_term"],
                idempotency_key=row["idempotency_key"],
                acknowledgement_id=row["acknowledgement_id"],
                persisted_at=row["persisted_at"],
            )
        except (IndexError, KeyError, TypeError, ValueError, FederationValidationError) as exc:
            raise FederationValidationError(
                "corrupt-provider-fencing-state",
                "provider_storage_fences",
                "persisted provider fence is malformed",
            ) from exc
        expected = _identity(
            {
                "schema": PROVIDER_FENCE_SCHEMA,
                "promotion_id": acknowledgement.promotion_id,
                "session_id": acknowledgement.session_id,
                "group_id": acknowledgement.group_id,
                "provider_id": acknowledgement.provider_id,
                "previous_term": acknowledgement.previous_term,
                "fencing_high_water_term": acknowledgement.fencing_high_water_term,
                "idempotency_key": acknowledgement.idempotency_key,
            }
        )
        if acknowledgement.acknowledgement_id != expected:
            raise FederationValidationError(
                "corrupt-provider-fencing-state",
                "acknowledgement_id",
                "persisted fence acknowledgement does not match its immutable binding",
            )
        return acknowledgement
