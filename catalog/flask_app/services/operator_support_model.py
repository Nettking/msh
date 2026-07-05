from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


CONFIRMATION_REQUIRED_ACTIONS = {
    "change_tool",
    "change_offset",
    "reduce_feed",
    "pause_production",
    "inspect_part",
    "call_supervisor",
}


@dataclass(frozen=True)
class ProblemObservation:
    issue: str
    context: str = ""
    signal: str = ""
    severity: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CauseHypothesis:
    cause: str
    confidence: str = "unknown"
    evidence: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RecommendedAction:
    action: str
    action_type: str = "manual_check"
    rationale: str = ""
    requires_confirmation: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ActionRisk:
    risk: str
    mitigation: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AlternativeAction:
    action: str
    tradeoff: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EvidenceItem:
    source: str
    summary: str
    reference_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OperatorConfirmation:
    support_card_id: str
    status: str
    comment: str = ""
    confirmed_by: str = "operator"
    confirmed_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if not payload["confirmed_at"]:
            payload["confirmed_at"] = _now_utc()
        return payload


@dataclass(frozen=True)
class QualityOutcome:
    operator_note_id: str
    result: str
    measurement_type: str = ""
    measurement_value: str = ""
    tolerance: str = ""
    surface_finish_note: str = ""
    conclusion: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SupportCard:
    issue: str
    observation: str
    suggested_action: str
    possible_causes: list[str] = field(default_factory=list)
    rationale: str = ""
    risk: str = ""
    alternative_action: str = ""
    evidence: list[str] = field(default_factory=list)
    mode: str = "assist"
    requires_confirmation: bool = True
    action_type: str = "manual_check"
    id: str = ""

    def __post_init__(self) -> None:
        if not self.id:
            object.__setattr__(self, "id", uuid4().hex[:12])
        if self.action_type in CONFIRMATION_REQUIRED_ACTIONS and not self.requires_confirmation:
            object.__setattr__(self, "requires_confirmation", True)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def action_requires_confirmation(action_type: str) -> bool:
    return action_type in CONFIRMATION_REQUIRED_ACTIONS


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
