"""Durable Phase E promotion transaction state."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Self

from .errors import FederationValidationError

PROMOTION_TRANSACTION_SCHEMA = "msh.storage_promotion_transaction.v1"
PROMOTION_TRANSACTION_STAGES = (
    "pending",
    "validated",
    "fenced-old-authority",
    "allocated-term",
    "granted-new-authority",
    "finalized",
    "failed",
)
_SHA256_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(char) < 32 for char in value):
        raise FederationValidationError("invalid-id", field, "must be non-empty opaque text")
    return value


def _sha256(value: Any, field: str) -> str:
    value = _required_text(value, field)
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise FederationValidationError("invalid-content-hash", field, "must be a lowercase sha256 digest")
    return value


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError("invalid-timestamp", field, "must be RFC 3339") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError("invalid-timestamp", field, "must be timezone-aware UTC")
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class PromotionTransactionRecord:
    schema: str
    promotion_id: str
    session_id: str
    group_id: str
    selected_provider_id: str
    selected_report_hash: str
    selected_report_revision: int
    selected_manifest_revision: int
    selected_manifest_hash: str
    previous_provider_id: str | None
    previous_term: int | None
    reserved_term: int | None
    fencing_status: str
    fencing_acknowledged: bool
    fencing_ack_identity: str | None
    grant_id: str | None
    grant_status: str
    grant_acknowledged: bool
    grant_ack_identity: str | None
    state: str
    failure_code: str | None
    failure_reason: str | None
    updated_at: datetime

    def __post_init__(self) -> None:
        if self.schema != PROMOTION_TRANSACTION_SCHEMA:
            raise FederationValidationError("unsupported-schema", "schema", f"expected {PROMOTION_TRANSACTION_SCHEMA}")
        for field in (
            "promotion_id",
            "session_id",
            "group_id",
            "selected_provider_id",
            "fencing_status",
            "grant_status",
            "state",
        ):
            object.__setattr__(self, field, _required_text(getattr(self, field), field))
        object.__setattr__(self, "selected_report_hash", _sha256(self.selected_report_hash, "selected_report_hash"))
        object.__setattr__(self, "selected_manifest_hash", _sha256(self.selected_manifest_hash, "selected_manifest_hash"))
        object.__setattr__(self, "selected_report_revision", int(self.selected_report_revision))
        object.__setattr__(self, "selected_manifest_revision", int(self.selected_manifest_revision))
        if self.previous_provider_id is not None:
            object.__setattr__(self, "previous_provider_id", _required_text(self.previous_provider_id, "previous_provider_id"))
        if self.previous_term is not None:
            object.__setattr__(self, "previous_term", int(self.previous_term))
        if self.reserved_term is not None:
            object.__setattr__(self, "reserved_term", int(self.reserved_term))
        if not isinstance(self.fencing_acknowledged, bool):
            raise FederationValidationError("invalid-boolean", "fencing_acknowledged", "must be boolean")
        if self.fencing_ack_identity is not None:
            object.__setattr__(self, "fencing_ack_identity", _required_text(self.fencing_ack_identity, "fencing_ack_identity"))
        if self.grant_id is not None:
            object.__setattr__(self, "grant_id", _required_text(self.grant_id, "grant_id"))
        if not isinstance(self.grant_acknowledged, bool):
            raise FederationValidationError("invalid-boolean", "grant_acknowledged", "must be boolean")
        if self.grant_ack_identity is not None:
            object.__setattr__(self, "grant_ack_identity", _required_text(self.grant_ack_identity, "grant_ack_identity"))
        if self.failure_code is not None:
            object.__setattr__(self, "failure_code", _required_text(self.failure_code, "failure_code"))
        if self.failure_reason is not None:
            object.__setattr__(self, "failure_reason", _required_text(self.failure_reason, "failure_reason"))
        object.__setattr__(self, "updated_at", _utc(self.updated_at, "updated_at"))
        if self.state not in PROMOTION_TRANSACTION_STAGES:
            raise FederationValidationError("invalid-enum", "state", "unknown promotion transaction stage")

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "promotion_transaction", "must be an object")
        required = {
            "schema",
            "promotion_id",
            "session_id",
            "group_id",
            "selected_provider_id",
            "selected_report_hash",
            "selected_report_revision",
            "selected_manifest_revision",
            "selected_manifest_hash",
            "previous_provider_id",
            "previous_term",
            "reserved_term",
            "fencing_status",
            "fencing_acknowledged",
            "fencing_ack_identity",
            "grant_id",
            "grant_status",
            "grant_acknowledged",
            "grant_ack_identity",
            "state",
            "failure_code",
            "failure_reason",
            "updated_at",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(
            schema=value["schema"],
            promotion_id=value["promotion_id"],
            session_id=value["session_id"],
            group_id=value["group_id"],
            selected_provider_id=value["selected_provider_id"],
            selected_report_hash=value["selected_report_hash"],
            selected_report_revision=value["selected_report_revision"],
            selected_manifest_revision=value["selected_manifest_revision"],
            selected_manifest_hash=value["selected_manifest_hash"],
            previous_provider_id=value["previous_provider_id"],
            previous_term=value["previous_term"],
            reserved_term=value["reserved_term"],
            fencing_status=value["fencing_status"],
            fencing_acknowledged=value["fencing_acknowledged"],
            fencing_ack_identity=value["fencing_ack_identity"],
            grant_id=value["grant_id"],
            grant_status=value["grant_status"],
            grant_acknowledged=value["grant_acknowledged"],
            grant_ack_identity=value["grant_ack_identity"],
            state=value["state"],
            failure_code=value["failure_code"],
            failure_reason=value["failure_reason"],
            updated_at=value["updated_at"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "promotion_id": self.promotion_id,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "selected_provider_id": self.selected_provider_id,
            "selected_report_hash": self.selected_report_hash,
            "selected_report_revision": self.selected_report_revision,
            "selected_manifest_revision": self.selected_manifest_revision,
            "selected_manifest_hash": self.selected_manifest_hash,
            "previous_provider_id": self.previous_provider_id,
            "previous_term": self.previous_term,
            "reserved_term": self.reserved_term,
            "fencing_status": self.fencing_status,
            "fencing_acknowledged": self.fencing_acknowledged,
            "fencing_ack_identity": self.fencing_ack_identity,
            "grant_id": self.grant_id,
            "grant_status": self.grant_status,
            "grant_acknowledged": self.grant_acknowledged,
            "grant_ack_identity": self.grant_ack_identity,
            "state": self.state,
            "failure_code": self.failure_code,
            "failure_reason": self.failure_reason,
            "updated_at": _timestamp(self.updated_at),
        }

    def json(self) -> str:
        return _json(self.to_dict())
