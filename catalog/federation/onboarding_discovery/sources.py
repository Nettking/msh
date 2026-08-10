"""Bounded discovery inputs for capability-first federation onboarding."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.onboarding_compat import federation_id_from_session_id
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationDiscoveryResult,
)

from .authority import VerifiedJoinMaterial

MAX_SOURCE_CANDIDATES = 32


def _utc(value: datetime, field_name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset().total_seconds() != 0
    ):
        raise FederationValidationError(
            "invalid-discovery-time",
            field_name,
            "must be a UTC timestamp",
        )
    return value.astimezone(timezone.utc)


def _positive_bound(value: int, field_name: str, *, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise FederationValidationError(
            "invalid-discovery-bound",
            field_name,
            f"must be between 1 and {maximum}",
        )
    return value


def _discovery_id(*, federation_fingerprint: str, internal_session_id: str) -> str:
    digest = hashlib.sha256(
        (
            "fcp-onboarding-discovery-v1\0"
            + federation_fingerprint
            + "\0"
            + internal_session_id
        ).encode("utf-8")
    ).hexdigest()
    return f"discovery-{digest[:32]}"


@dataclass(frozen=True)
class FederationDiscoveryCandidate:
    """Internal candidate whose authority material never enters safe output."""

    federation_label: str
    internal_session_id: str
    federation_fingerprint: str
    transport_kind: str
    verification_code: str
    discovered_at: datetime
    expires_at: datetime
    prior_trust: bool = False
    available: bool = True
    unavailable_reason: str | None = None
    auto_accept_authorized: bool = False
    enrollment_token: str | None = field(default=None, repr=False, compare=False)
    invitation_token: str | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "discovered_at",
            _utc(self.discovered_at, "discovered_at"),
        )
        object.__setattr__(
            self,
            "expires_at",
            _utc(self.expires_at, "expires_at"),
        )
        for field_name in ("prior_trust", "available", "auto_accept_authorized"):
            if not isinstance(getattr(self, field_name), bool):
                raise FederationValidationError(
                    "invalid-boolean",
                    field_name,
                    "must be a boolean",
                )
        if self.available and self.unavailable_reason is not None:
            raise FederationValidationError(
                "unexpected-reason",
                "unavailable_reason",
                "is only valid for unavailable candidates",
            )
        if not self.available and self.unavailable_reason is None:
            raise FederationValidationError(
                "missing-reason",
                "unavailable_reason",
                "is required for unavailable candidates",
            )
        self.to_result()

    @property
    def discovery_id(self) -> str:
        return _discovery_id(
            federation_fingerprint=self.federation_fingerprint,
            internal_session_id=self.internal_session_id,
        )

    @property
    def federation_id(self) -> str:
        return federation_id_from_session_id(self.internal_session_id)

    @property
    def join_material(self) -> VerifiedJoinMaterial:
        return VerifiedJoinMaterial(
            internal_session_id=self.internal_session_id,
            enrollment_token=self.enrollment_token,
            invitation_token=self.invitation_token,
        )

    def to_result(
        self,
        *,
        now: datetime | None = None,
    ) -> FederationDiscoveryResult:
        effective_now = self.discovered_at if now is None else _utc(now, "now")
        available = self.available and self.expires_at > effective_now
        reason = self.unavailable_reason
        if self.available and not available:
            reason = "Discovery candidate expired."
        state = (
            FederationConnectionState.UNAVAILABLE
            if not available
            else FederationConnectionState.DISCOVERED
            if self.prior_trust
            else FederationConnectionState.VERIFICATION_REQUIRED
        )
        return FederationDiscoveryResult(
            discovery_id=self.discovery_id,
            federation_label=self.federation_label,
            federation_fingerprint=self.federation_fingerprint,
            transport_kind=self.transport_kind,
            verification_code=self.verification_code,
            prior_trust=self.prior_trust,
            state=state,
            discovered_at=self.discovered_at,
            expires_at=self.expires_at,
            unavailable_reason=reason if state is FederationConnectionState.UNAVAILABLE else None,
        )


class FederationDiscoverySource(Protocol):
    """Synchronous source bounded by the caller's deadline and item limit."""

    source_id: str

    def discover(
        self,
        *,
        now: datetime,
        deadline: datetime,
    ) -> tuple[FederationDiscoveryCandidate, ...]: ...


class ConfiguredFederationDiscoveryAdapter:
    """Adapt already configured federation join material to safe discovery."""

    def __init__(
        self,
        candidates: Iterable[FederationDiscoveryCandidate],
        *,
        source_id: str = "configured",
        max_candidates: int = MAX_SOURCE_CANDIDATES,
    ) -> None:
        self.source_id = str(source_id)
        self.max_candidates = _positive_bound(
            max_candidates,
            "max_candidates",
            maximum=MAX_SOURCE_CANDIDATES,
        )
        values = tuple(candidates)
        if len(values) > self.max_candidates:
            raise FederationValidationError(
                "too-many-discovery-candidates",
                "candidates",
                "configured discovery exceeds its candidate bound",
            )
        self._candidates = values

    def discover(
        self,
        *,
        now: datetime,
        deadline: datetime,
    ) -> tuple[FederationDiscoveryCandidate, ...]:
        now = _utc(now, "now")
        deadline = _utc(deadline, "deadline")
        if deadline < now:
            raise FederationOperationError(
                "discovery-deadline-expired",
                "the configured discovery deadline has expired",
                "deadline",
            )
        return self._candidates


class RelayFederationDiscoveryAdapter:
    """Narrow adapter for existing relay/configuration resolvers.

    The resolver receives only the bounded deadline. Any endpoint, credential,
    enrollment token, or invitation token stays inside internal candidates.
    """

    def __init__(
        self,
        resolver: Callable[[datetime], Iterable[FederationDiscoveryCandidate]],
        *,
        source_id: str = "relay",
        max_candidates: int = MAX_SOURCE_CANDIDATES,
    ) -> None:
        self.source_id = str(source_id)
        self.max_candidates = _positive_bound(
            max_candidates,
            "max_candidates",
            maximum=MAX_SOURCE_CANDIDATES,
        )
        self._resolver = resolver

    def discover(
        self,
        *,
        now: datetime,
        deadline: datetime,
    ) -> tuple[FederationDiscoveryCandidate, ...]:
        now = _utc(now, "now")
        deadline = _utc(deadline, "deadline")
        if deadline < now:
            raise FederationOperationError(
                "discovery-deadline-expired",
                "the relay discovery deadline has expired",
                "deadline",
            )
        try:
            values = tuple(self._resolver(deadline))
        except (FederationValidationError, FederationOperationError):
            raise
        except Exception as exc:
            raise FederationOperationError(
                "relay-discovery-unavailable",
                "relay discovery is temporarily unavailable",
                "source",
            ) from exc
        if len(values) > self.max_candidates:
            raise FederationOperationError(
                "relay-discovery-limit-exceeded",
                "relay discovery exceeded its candidate bound",
                "source",
            )
        if any(not isinstance(value, FederationDiscoveryCandidate) for value in values):
            raise FederationValidationError(
                "invalid-discovery-candidate",
                "candidate",
                "relay discovery returned an unsupported candidate",
            )
        return values
