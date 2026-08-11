"""Framework-neutral Federation product projection service."""

from __future__ import annotations

from .detail_pages import DetailProjectionMixin
from .models import (
    FederationPage,
    FederationViewModel,
    NoticeKind,
    ProjectionItem,
    ProjectionNotice,
    ProjectionState,
    RecommendedAction,
    TechnicalDetail,
)
from .overview_page import OverviewProjectionMixin
from .service_core import FederationProjectionCore, ProjectionAdapters, _title


class FederationProjectionService(
    OverviewProjectionMixin,
    DetailProjectionMixin,
    FederationProjectionCore,
):
    """Build safe product view models without granting or changing authority."""

    @staticmethod
    def _storage_candidate_provider_id(candidate: object) -> str | None:
        capacity = getattr(candidate, "capacity", None)
        if not isinstance(capacity, dict):
            return None
        provider_id = capacity.get("provider_id")
        return provider_id if isinstance(provider_id, str) and provider_id else None

    def storage(self, *, include_technical: bool = False) -> FederationViewModel:
        """Project storage without conflating candidate and provider identities.

        Capability-first ``candidate_id`` values are device-scoped decision IDs;
        storage-control-plane ``provider_id`` values are authority IDs. Suppress
        a local candidate-only row only when its explicit provider identity is
        already represented by the authoritative storage snapshot.
        """

        snapshot = self._snapshot()
        device_labels = {
            device.node_id: device.label for device in snapshot.federation.devices
        }
        items = [
            ProjectionItem(
                group.group_id,
                f"Storage group {group.group_id}",
                f"Primary: {group.primary_provider_id or 'Not assigned'}",
                "active" if group.leader_active else "degraded",
                (
                    "Write authority active"
                    if group.leader_active
                    else "No active authority"
                ),
                (("Replicas", ", ".join(group.replica_provider_ids) or "None"),),
            )
            for group in snapshot.storage.groups
        ]
        items.extend(
            ProjectionItem(
                provider.provider_id,
                f"Storage provider {provider.provider_id}",
                ", ".join(_title(role) for role in provider.roles) or "Unassigned",
                "active" if provider.assignable else "unavailable",
                (
                    "Assignable"
                    if provider.assignable
                    else "Candidate or unavailable"
                ),
                (
                    ("Device", device_labels.get(provider.node_id, provider.node_id)),
                    ("Device ID", provider.node_id),
                ),
            )
            for provider in snapshot.storage.providers
        )
        authoritative = {
            provider.provider_id for provider in snapshot.storage.providers
        }
        items.extend(
            ProjectionItem(
                candidate.candidate_id,
                candidate.label,
                "Candidate only; the control plane has not assigned authority.",
                "candidate",
                "Candidate only",
            )
            for candidate in snapshot.onboarding.contributions
            if candidate.capability_type == "storage"
            and (
                (provider_id := self._storage_candidate_provider_id(candidate)) is None
                or provider_id not in authoritative
            )
        )
        notice = self._notice(snapshot)
        state = state_label = None
        if snapshot.storage.available and not items:
            state, state_label = ProjectionState.EMPTY, "No storage authority"
            notice = ProjectionNotice(
                NoticeKind.EMPTY,
                "No storage providers or groups are assigned",
                "Benchmark success remains evidence until control-plane assignment.",
                RecommendedAction(
                    "Review storage candidates",
                    "Review candidate readiness without assigning authority.",
                    "Open services",
                    "/federation/services",
                ),
            )
        technical = ()
        if include_technical and snapshot.storage.revision is not None:
            technical = (
                TechnicalDetail(
                    "Control-plane revision",
                    str(snapshot.storage.revision),
                ),
            )
        return self._page(
            FederationPage.STORAGE,
            "Storage",
            "Review candidate readiness and control-plane assignments separately.",
            snapshot,
            items=tuple(items),
            notice=notice,
            state=state,
            state_label=state_label,
            technical_details=technical,
        )

    def project(
        self,
        page: FederationPage | str,
        *,
        include_technical: bool = False,
    ) -> FederationViewModel:
        page = FederationPage(page)
        builders = {
            FederationPage.OVERVIEW: self.overview,
            FederationPage.THIS_DEVICE: self.this_device,
            FederationPage.DEVICES: self.devices,
            FederationPage.SERVICES: self.services,
            FederationPage.BENCHMARKS: self.benchmarks,
            FederationPage.STORAGE: self.storage,
            FederationPage.JOBS: self.jobs,
            FederationPage.ACTIVITY: self.activity,
            FederationPage.SETTINGS: self.settings,
        }
        return builders[page](include_technical=include_technical)


__all__ = ["FederationProjectionService", "ProjectionAdapters"]
