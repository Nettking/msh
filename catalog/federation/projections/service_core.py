"""Framework-neutral Federation product projection service."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .adapters import (
    BenchmarkRecord,
    BenchmarkResultsAdapter,
    BenchmarkSnapshot,
    ContributionRecord,
    FederationAuthorityAdapter,
    FederationAuthoritySnapshot,
    JobAuthorityAdapter,
    JobAuthoritySnapshot,
    OnboardingContractsAdapter,
    OnboardingSnapshot,
    ProviderOperatorAdapter,
    ProviderRecord,
    ProviderSnapshot,
    StorageAuthorityAdapter,
    StorageAuthoritySnapshot,
)
from .models import (
    NoticeKind,
    ProjectionNotice,
    ProjectionState,
    RecommendedAction,
    SectionLink,
    TechnicalDetail,
)
from .safety import safe_text


@dataclass(frozen=True)
class ProjectionAdapters:
    onboarding: OnboardingContractsAdapter | None = None
    providers: ProviderOperatorAdapter | None = None
    benchmarks: BenchmarkResultsAdapter | None = None
    federation: FederationAuthorityAdapter | None = None
    storage: StorageAuthorityAdapter | None = None
    jobs: JobAuthorityAdapter | None = None


@dataclass(frozen=True)
class ProjectionSnapshot:
    onboarding: OnboardingSnapshot
    providers: ProviderSnapshot
    benchmarks: BenchmarkSnapshot
    federation: FederationAuthoritySnapshot
    storage: StorageAuthoritySnapshot
    jobs: JobAuthoritySnapshot


_SECTION_LINKS = (
    SectionLink("overview", "Overview", "/federation"),
    SectionLink("this-device", "This device", "/federation/device"),
    SectionLink("devices", "Devices", "/federation/devices"),
    SectionLink("services", "Services", "/federation/services"),
    SectionLink("benchmarks", "Benchmarks", "/federation/benchmarks"),
    SectionLink("storage", "Storage", "/federation/storage"),
    SectionLink("jobs", "Jobs", "/federation/jobs"),
    SectionLink("activity", "Activity", "/federation/activity"),
    SectionLink("settings", "Settings", "/federation/settings"),
)

_ACTIVE_PROVIDER_STATES = {"eligible", "active"}
_CURRENT_HEALTH_STATES = {"current", "healthy", "online"}
_ONLINE_DEVICE_STATES = {"connected", "online", "current"}
_RUNNING_JOB_STATES = {"accepted", "running", "cancellation-requested"}
_FAILED_JOB_STATES = {"failed", "timed-out", "lost", "blocked"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime | None) -> str:
    if value is None:
        return "Not available"
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _title(value: str) -> str:
    return value.replace("-", " ").replace("_", " ").title()


def _count(value: int, singular: str, plural: str | None = None) -> str:
    return f"{value} {singular if value == 1 else (plural or singular + 's')}"


def _flatten_public_mapping(value: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    rows: list[tuple[str, str]] = []
    for key, item in sorted(value.items(), key=lambda pair: str(pair[0])):
        label = _title(str(key))
        if isinstance(item, Mapping):
            summary = ", ".join(
                f"{_title(str(child_key))}: {child_value}"
                for child_key, child_value in sorted(
                    item.items(), key=lambda pair: str(pair[0])
                )
                if isinstance(child_value, (str, int, float, bool))
            )
            if summary:
                rows.append((label, safe_text(summary, "resource.summary")))
        elif isinstance(item, (str, int, float, bool)):
            rows.append((label, safe_text(str(item), "resource.value")))
    return tuple(rows)


def _provider_state(provider: ProviderRecord) -> tuple[str, str]:
    if provider.activation_state in _ACTIVE_PROVIDER_STATES:
        return "active", "Available"
    if provider.health_state not in _CURRENT_HEALTH_STATES:
        return "degraded", "Needs attention"
    if provider.enrollment_state in {"pending", "suspended", "revoked"}:
        return provider.enrollment_state, _title(provider.enrollment_state)
    return "unavailable", "Unavailable"


def _contribution_state(contribution: ContributionRecord) -> tuple[str, str]:
    if contribution.activation_state == "active":
        return "active", "Active"
    if contribution.activation_state in {"blocked", "suspended"}:
        return contribution.activation_state, _title(contribution.activation_state)
    if contribution.policy_state == "allowed":
        return "available", "Available"
    if contribution.missing_prerequisites:
        return "degraded", "Setup needed"
    return "inactive", "Not enabled"


def _benchmark_state(benchmark: BenchmarkRecord) -> tuple[str, str]:
    if not benchmark.current:
        return "expired", "Rerun"
    if benchmark.state == "passed":
        if benchmark.recommendation == "recommended":
            return "passed", "Recommended"
        return "passed", "Current"
    if benchmark.state == "failed":
        return "failed", "Failed"
    return benchmark.state, _title(benchmark.state)


class FederationProjectionCore:
    """Build safe product view models without granting or changing authority."""

    def __init__(
        self,
        adapters: ProjectionAdapters,
        *,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._adapters = adapters
        self._now = now

    def _snapshot(self) -> ProjectionSnapshot:
        return ProjectionSnapshot(
            onboarding=(
                OnboardingSnapshot(False, "onboarding-state-unavailable")
                if self._adapters.onboarding is None
                else self._adapters.onboarding.snapshot()
            ),
            providers=(
                ProviderSnapshot(False, "provider-surface-unavailable")
                if self._adapters.providers is None
                else self._adapters.providers.snapshot()
            ),
            benchmarks=(
                BenchmarkSnapshot(False, "benchmark-store-unavailable")
                if self._adapters.benchmarks is None
                else self._adapters.benchmarks.snapshot()
            ),
            federation=(
                FederationAuthoritySnapshot(
                    False,
                    "federation-authority-unavailable",
                )
                if self._adapters.federation is None
                else self._adapters.federation.snapshot()
            ),
            storage=(
                StorageAuthoritySnapshot(False, "storage-authority-unavailable")
                if self._adapters.storage is None
                else self._adapters.storage.snapshot()
            ),
            jobs=(
                JobAuthoritySnapshot(False, "job-authority-unavailable")
                if self._adapters.jobs is None
                else self._adapters.jobs.snapshot()
            ),
        )

    @staticmethod
    def _repair_action() -> RecommendedAction:
        return RecommendedAction(
            "Repair the federation connection",
            "Your saved identity can be reused. Follow the guided checks to reconnect safely.",
            "Open guided repair",
            "/onboarding?repair=1",
        )

    @staticmethod
    def _setup_action() -> RecommendedAction:
        return RecommendedAction(
            "Complete federation setup",
            "Connect this device before federation status can be shown.",
            "Open onboarding",
            "/onboarding",
        )

    @staticmethod
    def _next_action(snapshot: ProjectionSnapshot) -> RecommendedAction | None:
        onboarding = snapshot.onboarding
        if not onboarding.available or onboarding.federation_id is None:
            return FederationProjectionCore._setup_action()
        if onboarding.connection_state != "connected" or not onboarding.trusted:
            return FederationProjectionCore._repair_action()
        expired = [result for result in snapshot.benchmarks.results if not result.current]
        if expired:
            return RecommendedAction(
                "Rerun expired benchmarks",
                "Suitability evidence has expired or changed and should be refreshed before relying on it.",
                "Open benchmarks",
                "/federation/benchmarks",
            )
        unhealthy = [
            provider
            for provider in snapshot.providers.providers
            if provider.health_state not in _CURRENT_HEALTH_STATES
        ]
        if snapshot.providers.available and unhealthy:
            return RecommendedAction(
                "Review service health",
                "At least one contributed service is not reporting current health.",
                "Open services",
                "/federation/services",
            )
        suitable = [
            contribution
            for contribution in onboarding.contributions
            if contribution.policy_state == "allowed"
            and contribution.activation_state != "active"
        ]
        if suitable:
            return RecommendedAction(
                "Review available contributions",
                "This device has a suitable service that is not currently enabled.",
                "Review services",
                "/onboarding?step=contributions",
            )
        return None

    @staticmethod
    def _selected_action(
        snapshot: ProjectionSnapshot,
        notice: ProjectionNotice | None,
    ) -> RecommendedAction | None:
        if (
            notice is not None
            and notice.action is not None
            and notice.kind in {NoticeKind.DEGRADED, NoticeKind.REPAIR}
        ):
            return None
        return FederationProjectionCore._next_action(snapshot)

    @staticmethod
    def _notice(snapshot: ProjectionSnapshot) -> ProjectionNotice | None:
        onboarding = snapshot.onboarding
        if not onboarding.available or onboarding.federation_id is None:
            return ProjectionNotice(
                NoticeKind.EMPTY,
                "Federation setup is not complete",
                "Complete onboarding to create or join a federation and load safe status projections.",
                FederationProjectionCore._setup_action(),
            )
        if onboarding.connection_state != "connected" or not onboarding.trusted:
            return ProjectionNotice(
                NoticeKind.REPAIR,
                "This device could not reconnect",
                "The saved identity is intact, but authenticated federation context is not currently available.",
                FederationProjectionCore._repair_action(),
            )
        unavailable = [
            available
            for available in (
                snapshot.providers.available,
                snapshot.benchmarks.available,
                snapshot.federation.available,
                snapshot.storage.available,
                snapshot.jobs.available,
            )
            if not available
        ]
        if unavailable:
            return ProjectionNotice(
                NoticeKind.DEGRADED,
                "Some federation details are temporarily unavailable",
                "The connected state is preserved. Missing projections can be retried without changing authority.",
                RecommendedAction(
                    "Review federation status",
                    "Open the overview again after the local authorities have recovered.",
                    "Refresh overview",
                    "/federation",
                ),
            )
        return None

    @staticmethod
    def _state(snapshot: ProjectionSnapshot) -> tuple[ProjectionState, str]:
        if (
            not snapshot.onboarding.available
            or snapshot.onboarding.federation_id is None
        ):
            return ProjectionState.EMPTY, "Setup needed"
        if (
            snapshot.onboarding.connection_state != "connected"
            or not snapshot.onboarding.trusted
        ):
            return ProjectionState.REPAIR, "Reconnection needed"
        if FederationProjectionCore._notice(snapshot) is not None:
            return ProjectionState.DEGRADED, "Limited status"
        if any(not result.current for result in snapshot.benchmarks.results):
            return ProjectionState.DEGRADED, "Review needed"
        return ProjectionState.CONNECTED, "Connected"

    @staticmethod
    def _contribution_card(contribution: ContributionRecord) -> dict[str, Any]:
        state, label = _contribution_state(contribution)
        capacity = [
            {"label": _title(str(key)), "value": str(value)}
            for key, value in sorted(
                contribution.capacity.items(), key=lambda pair: str(pair[0])
            )
            if isinstance(value, (str, int, float, bool))
        ]
        return {
            "candidate_id": contribution.candidate_id,
            "type_label": _title(contribution.capability_type),
            "label": contribution.label,
            "description": "A bounded service contribution from this device.",
            "recommendation": (
                "recommended" if contribution.policy_state == "allowed" else "review"
            ),
            "recommendation_label": (
                "Suitable" if contribution.policy_state == "allowed" else "Review"
            ),
            "capacity": capacity,
            "missing_prerequisites": list(contribution.missing_prerequisites),
            "policy_state": contribution.policy_state,
            "activation_state": contribution.activation_state,
            "activation_label": label,
            "state": state,
            "detail_url": "/federation/services",
        }

    @staticmethod
    def _technical_details(
        snapshot: ProjectionSnapshot,
        *,
        device_only: bool = False,
        authority_only: bool = False,
        providers_only: bool = False,
        settings_only: bool = False,
    ) -> tuple[TechnicalDetail, ...]:
        details: list[TechnicalDetail] = []
        if snapshot.onboarding.federation_id and not device_only and not providers_only:
            details.append(
                TechnicalDetail("Federation ID", snapshot.onboarding.federation_id)
            )
        if snapshot.onboarding.device_id and not authority_only and not providers_only:
            details.append(TechnicalDetail("Device ID", snapshot.onboarding.device_id))
        if (
            snapshot.onboarding.inspection_revision is not None
            and not authority_only
            and not providers_only
            and not settings_only
        ):
            details.append(
                TechnicalDetail(
                    "Inspection revision",
                    str(snapshot.onboarding.inspection_revision),
                )
            )
        if (
            snapshot.federation.revision is not None
            and not device_only
            and not providers_only
        ):
            details.append(
                TechnicalDetail(
                    "Federation revision",
                    str(snapshot.federation.revision),
                )
            )
        if providers_only:
            details.append(
                TechnicalDetail(
                    "Visible providers",
                    str(len(snapshot.providers.providers)),
                )
            )
        return tuple(details)
