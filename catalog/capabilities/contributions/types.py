"""Framework-neutral contribution recommendation and activation types."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

from catalog.federation.onboarding_models import (
    BenchmarkResult,
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    DeviceInspectionSnapshot,
)


@dataclass(frozen=True)
class LocalContributionDescriptor:
    """Private local descriptor used to derive one public candidate contract."""

    logical_service_id: str
    capability_type: str
    capability_protocol: str
    display_label: str
    capacity_envelope: dict[str, Any] = field(default_factory=dict)
    missing_prerequisites: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in (
            "logical_service_id",
            "capability_type",
            "capability_protocol",
            "display_label",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty text")
            object.__setattr__(self, name, value.strip())
        try:
            encoded = json.dumps(
                self.capacity_envelope,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("capacity_envelope must be finite JSON") from exc
        if len(encoded.encode("utf-8")) > 32_768:
            raise ValueError("capacity_envelope is too large")
        prerequisites = tuple(str(item).strip() for item in self.missing_prerequisites)
        if any(not item for item in prerequisites):
            raise ValueError("missing_prerequisites must contain non-empty text")
        if len(prerequisites) != len(set(prerequisites)):
            raise ValueError("missing_prerequisites must be unique")
        object.__setattr__(self, "missing_prerequisites", tuple(sorted(prerequisites)))


@dataclass(frozen=True)
class CandidateRecommendation:
    """One CF1 candidate plus the CF2 evidence used to recommend it."""

    candidate: ContributionCandidate
    logical_service_id: str
    benchmark_results: tuple[BenchmarkResult, ...]


@dataclass(frozen=True)
class AdapterOutcome:
    """Safe state returned by one authority adapter."""

    activation_state: ContributionActivationState
    reason: str | None = None
    authority_confirmed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "activation_state",
            ContributionActivationState(self.activation_state),
        )
        if self.reason is not None:
            reason = str(self.reason).strip()
            if not reason:
                raise ValueError("reason must be non-empty when supplied")
            object.__setattr__(self, "reason", reason)
        if not isinstance(self.authority_confirmed, bool):
            raise ValueError("authority_confirmed must be boolean")


class CandidateSource(Protocol):
    """Produce private descriptors from one bounded CF2 inspection snapshot."""

    def descriptors(
        self,
        inspection: DeviceInspectionSnapshot,
    ) -> tuple[LocalContributionDescriptor, ...]: ...


class ContributionActivationAdapter(Protocol):
    """Narrow bridge to one existing authority boundary."""

    candidate_only: bool

    def supports(self, candidate: ContributionCandidate) -> bool: ...

    def enable(self, candidate: ContributionCandidate) -> AdapterOutcome: ...

    def disable(self, candidate: ContributionCandidate) -> AdapterOutcome: ...

    def suspend(
        self,
        candidate: ContributionCandidate,
        *,
        reason: str,
    ) -> AdapterOutcome: ...

    def reconcile(
        self,
        candidate: ContributionCandidate,
        *,
        desired_state: ContributionDesiredState,
    ) -> AdapterOutcome: ...
