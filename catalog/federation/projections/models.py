"""Framework-neutral Federation product view-model contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar

from catalog.federation.errors import FederationValidationError

from .safety import assert_public_projection, safe_route, safe_text

JSON = dict[str, Any]


class FederationPage(str, Enum):
    OVERVIEW = "overview"
    THIS_DEVICE = "this-device"
    DEVICES = "devices"
    SERVICES = "services"
    BENCHMARKS = "benchmarks"
    STORAGE = "storage"
    JOBS = "jobs"
    ACTIVITY = "activity"
    SETTINGS = "settings"


class ProjectionState(str, Enum):
    CONNECTED = "connected"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    EMPTY = "empty"
    REPAIR = "repair"
    UNAVAILABLE = "unavailable"


class NoticeKind(str, Enum):
    EMPTY = "empty"
    DEGRADED = "degraded"
    REPAIR = "repair"
    INFORMATION = "information"


def _utc(value: datetime, field_name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise FederationValidationError(
            "invalid-projection-timestamp",
            field_name,
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


def _enum(value: object, enum_type: type[Enum], field_name: str) -> Enum:
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-projection-enum",
            field_name,
            f"unknown {field_name}",
        ) from exc


def _stamp(value: datetime) -> str:
    return _utc(value, "generated_at").isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class SectionLink:
    key: str
    label: str
    url: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", safe_text(self.key, "section.key"))
        object.__setattr__(self, "label", safe_text(self.label, "section.label"))
        object.__setattr__(self, "url", safe_route(self.url, "section.url"))

    def to_dict(self) -> JSON:
        return {"key": self.key, "label": self.label, "url": self.url}


@dataclass(frozen=True)
class RecommendedAction:
    title: str
    message: str
    label: str
    url: str

    def __post_init__(self) -> None:
        for name in ("title", "message", "label"):
            object.__setattr__(
                self,
                name,
                safe_text(getattr(self, name), f"recommended_action.{name}"),
            )
        object.__setattr__(
            self,
            "url",
            safe_route(self.url, "recommended_action.url"),
        )

    def to_dict(self) -> JSON:
        return {
            "title": self.title,
            "message": self.message,
            "label": self.label,
            "url": self.url,
        }


@dataclass(frozen=True)
class ProjectionNotice:
    kind: NoticeKind
    title: str
    message: str
    action: RecommendedAction | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", _enum(self.kind, NoticeKind, "notice.kind"))
        object.__setattr__(self, "title", safe_text(self.title, "notice.title"))
        object.__setattr__(self, "message", safe_text(self.message, "notice.message"))
        if self.action is not None and not isinstance(self.action, RecommendedAction):
            raise FederationValidationError(
                "invalid-projection-action",
                "notice.action",
                "must be a RecommendedAction",
            )

    def to_dict(self) -> JSON:
        return {
            "kind": self.kind.value,
            "title": self.title,
            "message": self.message,
            "action": None if self.action is None else self.action.to_dict(),
        }


@dataclass(frozen=True)
class SummaryCard:
    label: str
    value: str
    detail: str
    state: str | None = None
    state_label: str | None = None

    def __post_init__(self) -> None:
        for name in ("label", "value", "detail"):
            object.__setattr__(
                self,
                name,
                safe_text(getattr(self, name), f"summary_card.{name}"),
            )
        if self.state is not None:
            object.__setattr__(
                self,
                "state",
                safe_text(self.state, "summary_card.state"),
            )
        if self.state_label is not None:
            object.__setattr__(
                self,
                "state_label",
                safe_text(self.state_label, "summary_card.state_label"),
            )
        if (self.state is None) != (self.state_label is None):
            raise FederationValidationError(
                "incomplete-projection-status",
                "summary_card.state",
                "state and state_label must be provided together",
            )

    def to_dict(self) -> JSON:
        return {
            "label": self.label,
            "value": self.value,
            "detail": self.detail,
            "state": self.state,
            "state_label": self.state_label,
        }


@dataclass(frozen=True)
class ProjectionItem:
    key: str
    label: str
    detail: str
    state: str
    state_label: str
    details: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        for name in ("key", "label", "detail", "state", "state_label"):
            object.__setattr__(
                self,
                name,
                safe_text(getattr(self, name), f"item.{name}"),
            )
        normalized: list[tuple[str, str]] = []
        for index, pair in enumerate(self.details):
            if not isinstance(pair, tuple) or len(pair) != 2:
                raise FederationValidationError(
                    "invalid-projection-detail",
                    f"item.details[{index}]",
                    "must be a label/value pair",
                )
            normalized.append(
                (
                    safe_text(pair[0], f"item.details[{index}].label"),
                    safe_text(pair[1], f"item.details[{index}].value"),
                )
            )
        object.__setattr__(self, "details", tuple(normalized))

    def to_dict(self) -> JSON:
        return {
            "key": self.key,
            "label": self.label,
            "detail": self.detail,
            "state": self.state,
            "state_label": self.state_label,
            "details": [
                {"label": label, "value": value}
                for label, value in self.details
            ],
        }


@dataclass(frozen=True)
class TechnicalDetail:
    label: str
    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "label", safe_text(self.label, "technical.label"))
        object.__setattr__(self, "value", safe_text(self.value, "technical.value"))

    def to_dict(self) -> JSON:
        return {"label": self.label, "value": self.value}


@dataclass(frozen=True)
class FederationViewModel:
    """One complete safe page projection with at most one next action."""

    SCHEMA: ClassVar[str] = "fcp.federation-product-view.v1"

    page: FederationPage
    title: str
    intro: str
    state: ProjectionState
    state_label: str
    generated_at: datetime
    sections: tuple[SectionLink, ...]
    recommended_action: RecommendedAction | None = None
    notice: ProjectionNotice | None = None
    summary_cards: tuple[SummaryCard, ...] = ()
    items: tuple[ProjectionItem, ...] = ()
    technical_details: tuple[TechnicalDetail, ...] = ()
    content: JSON = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "page", _enum(self.page, FederationPage, "page"))
        object.__setattr__(self, "state", _enum(self.state, ProjectionState, "state"))
        object.__setattr__(self, "title", safe_text(self.title, "title"))
        object.__setattr__(self, "intro", safe_text(self.intro, "intro"))
        object.__setattr__(
            self,
            "state_label",
            safe_text(self.state_label, "state_label"),
        )
        object.__setattr__(
            self,
            "generated_at",
            _utc(self.generated_at, "generated_at"),
        )
        if not self.sections:
            raise FederationValidationError(
                "missing-projection-sections",
                "sections",
                "must contain the Federation section navigation",
            )
        if len({section.key for section in self.sections}) != len(self.sections):
            raise FederationValidationError(
                "duplicate-projection-section",
                "sections",
                "section keys must be unique",
            )
        if self.page.value not in {section.key for section in self.sections}:
            raise FederationValidationError(
                "missing-active-section",
                "sections",
                "must contain the active page",
            )
        if not isinstance(self.content, dict):
            raise FederationValidationError(
                "invalid-projection-content",
                "content",
                "must be an object",
            )
        if (
            self.recommended_action is not None
            and self.notice is not None
            and self.notice.action is not None
            and self.notice.kind in {NoticeKind.DEGRADED, NoticeKind.REPAIR}
        ):
            raise FederationValidationError(
                "multiple-recommended-actions",
                "recommended_action",
                "the projection may expose only one recommended next action",
            )
        reserved = {
            "schema",
            "page",
            "active_section",
            "title",
            "intro",
            "state",
            "state_label",
            "generated_at",
            "updated_label",
            "sections",
            "recommended_action",
            "degraded",
            "notice",
            "summary_cards",
            "items",
            "technical_details",
        }
        overlap = reserved & set(self.content)
        if overlap:
            raise FederationValidationError(
                "reserved-projection-key",
                f"content.{min(overlap)}",
                "is owned by the common projection envelope",
            )
        assert_public_projection(self.content, "content")

    def to_dict(self) -> JSON:
        notice = None if self.notice is None else self.notice.to_dict()
        if notice is not None:
            notice["action"] = None
        degraded = {
            "visible": self.notice is not None
            and self.notice.kind in {NoticeKind.DEGRADED, NoticeKind.REPAIR},
            "title": "No current issue" if self.notice is None else self.notice.title,
            "message": (
                "This projection has no degraded or repair state."
                if self.notice is None
                else self.notice.message
            ),
            "action": (
                {"label": "Open overview", "url": "/federation"}
                if self.notice is None or self.notice.action is None
                else {
                    "label": self.notice.action.label,
                    "url": self.notice.action.url,
                }
            ),
        }
        payload: JSON = {
            "schema": self.SCHEMA,
            "page": self.page.value,
            "active_section": self.page.value,
            "title": self.title,
            "intro": self.intro,
            "state": self.state.value,
            "state_label": self.state_label,
            "generated_at": _stamp(self.generated_at),
            "updated_label": "Updated just now",
            "sections": [section.to_dict() for section in self.sections],
            "recommended_action": (
                None
                if self.recommended_action is None
                else self.recommended_action.to_dict()
            ),
            "degraded": degraded,
            "notice": notice,
            "summary_cards": [card.to_dict() for card in self.summary_cards],
            "items": [item.to_dict() for item in self.items],
            "technical_details": [
                detail.to_dict() for detail in self.technical_details
            ],
            **self.content,
        }
        return assert_public_projection(payload)
