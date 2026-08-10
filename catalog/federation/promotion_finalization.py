"""Durable Phase E promotion finalization proof."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .errors import FederationValidationError

PROMOTION_FINALIZATION_SCHEMA = "fcp.storage_promotion_finalization.v1"


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(char) < 32 for char in value):
        raise FederationValidationError("invalid-id", field, "must be non-empty opaque text")
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError("invalid-non-negative-integer", field, "must be a non-negative integer")
    return value


def _utc(value: datetime | str, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError("invalid-timestamp", field, "must be RFC 3339") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError("invalid-timestamp", field, "must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class PromotionFinalizationRecord:
    schema: str
    promotion_id: str
    session_id: str
    group_id: str
    selected_provider_id: str
    new_authority_term: int
    manifest_revision: int
    manifest_hash: str
    report_revision: int
    report_hash: str
    fencing_ack_identity: str
    grant_ack_identity: str
    final_authority_identity: str
    degraded_state_hash: str
    missing_ranges: tuple[dict[str, Any], ...]
    recovery_obligations: dict[str, Any]
    completed_at: datetime
    degraded_cleared: bool

    def __post_init__(self) -> None:
        if self.schema != PROMOTION_FINALIZATION_SCHEMA:
            raise FederationValidationError(
                "unsupported-schema",
                "schema",
                f"expected {PROMOTION_FINALIZATION_SCHEMA}",
            )
        for field in (
            "promotion_id",
            "session_id",
            "group_id",
            "selected_provider_id",
            "manifest_hash",
            "report_hash",
            "fencing_ack_identity",
            "grant_ack_identity",
            "final_authority_identity",
            "degraded_state_hash",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        for field in ("new_authority_term", "manifest_revision", "report_revision"):
            object.__setattr__(self, field, _uint(getattr(self, field), field))
        if not isinstance(self.missing_ranges, tuple) or any(not isinstance(item, dict) for item in self.missing_ranges):
            raise FederationValidationError("invalid-array", "missing_ranges", "must contain objects")
        if not isinstance(self.recovery_obligations, dict):
            raise FederationValidationError("invalid-object", "recovery_obligations", "must be an object")
        if not isinstance(self.degraded_cleared, bool):
            raise FederationValidationError("invalid-boolean", "degraded_cleared", "must be boolean")
        object.__setattr__(self, "completed_at", _utc(self.completed_at, "completed_at"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "promotion_id": self.promotion_id,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "selected_provider_id": self.selected_provider_id,
            "new_authority_term": self.new_authority_term,
            "manifest_revision": self.manifest_revision,
            "manifest_hash": self.manifest_hash,
            "report_revision": self.report_revision,
            "report_hash": self.report_hash,
            "fencing_ack_identity": self.fencing_ack_identity,
            "grant_ack_identity": self.grant_ack_identity,
            "final_authority_identity": self.final_authority_identity,
            "degraded_state_hash": self.degraded_state_hash,
            "missing_ranges": list(self.missing_ranges),
            "recovery_obligations": self.recovery_obligations,
            "completed_at": self.completed_at.isoformat().replace("+00:00", "Z"),
            "degraded_cleared": self.degraded_cleared,
        }

    def json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)
