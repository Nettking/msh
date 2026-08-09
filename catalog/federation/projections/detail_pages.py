"""Detail-page projection builders for the Federation product surface."""

from __future__ import annotations

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
from .service_core import (
    _FAILED_JOB_STATES,
    _ONLINE_DEVICE_STATES,
    _SECTION_LINKS,
    _benchmark_state,
    _contribution_state,
    _count,
    _flatten_public_mapping,
    _provider_state,
    _stamp,
    _title,
)


class DetailProjectionMixin:
    def _page(
        self,
        page: FederationPage,
        title: str,
        intro: str,
        snapshot: object,
        *,
        items: tuple[ProjectionItem, ...] = (),
        notice: ProjectionNotice | None = None,
        state: ProjectionState | None = None,
        state_label: str | None = None,
        technical_details: tuple[TechnicalDetail, ...] = (),
        content: dict[str, object] | None = None,
    ) -> FederationViewModel:
        default_state, default_label = self._state(snapshot)
        notice = self._notice(snapshot) if notice is None else notice
        return FederationViewModel(
            page=page,
            title=title,
            intro=intro,
            state=state or default_state,
            state_label=state_label or default_label,
            generated_at=self._now(),
            sections=_SECTION_LINKS,
            recommended_action=self._selected_action(snapshot, notice),
            notice=notice,
            items=items,
            technical_details=technical_details,
            content={} if content is None else content,
        )

    def this_device(self, *, include_technical: bool = False) -> FederationViewModel:
        snapshot = self._snapshot()
        onboarding = snapshot.onboarding
        items = [
            ProjectionItem(
                f"resource-{index}",
                label,
                value,
                "current",
                "Observed",
            )
            for index, (label, value) in enumerate(
                _flatten_public_mapping(onboarding.resource_observations)
            )
        ]
        for kind, label, values in (
            ("service", "Detected service", onboarding.detected_services),
            ("handler", "Registered handler", onboarding.registered_handlers),
            ("source", "Detected data source", onboarding.detected_data_sources),
        ):
            items.extend(
                ProjectionItem(
                    f"{kind}-{index}",
                    value,
                    label,
                    "available",
                    "Detected",
                )
                for index, value in enumerate(values)
            )
        notice = self._notice(snapshot)
        state = state_label = None
        if onboarding.available and onboarding.inspection_revision is None:
            state, state_label = ProjectionState.EMPTY, "No inspection data"
            notice = ProjectionNotice(
                NoticeKind.EMPTY,
                "This device has not been inspected",
                "Run the bounded local inspection before reviewing suitability.",
                RecommendedAction(
                    "Inspect this device",
                    "Start a safe local inspection from onboarding.",
                    "Open inspection",
                    "/onboarding?step=inspect",
                ),
            )
        return self._page(
            FederationPage.THIS_DEVICE,
            "This device",
            "Review the safe local inspection and possible contributions.",
            snapshot,
            items=tuple(items),
            notice=notice,
            state=state,
            state_label=state_label,
            technical_details=(
                self._technical_details(snapshot, device_only=True)
                if include_technical
                else ()
            ),
            content={
                "device_id": onboarding.device_id or "Not available",
                "operating_system": onboarding.os_family or "Not available",
                "architecture": onboarding.architecture or "Not available",
                "inspection_revision": onboarding.inspection_revision,
                "inspection_expires_at": _stamp(onboarding.inspection_expires_at),
                "warnings": list(onboarding.warnings),
            },
        )

    def devices(self, *, include_technical: bool = False) -> FederationViewModel:
        snapshot = self._snapshot()
        items = tuple(
            ProjectionItem(
                device.node_id,
                device.label,
                _count(device.capability_count, "service"),
                "connected" if device.state in _ONLINE_DEVICE_STATES else "unavailable",
                "Online" if device.state in _ONLINE_DEVICE_STATES else "Offline",
                (("Last seen", _stamp(device.last_seen_at)),),
            )
            for device in snapshot.federation.devices
        )
        notice = self._notice(snapshot)
        state = state_label = None
        if snapshot.federation.available and not items:
            state, state_label = ProjectionState.EMPTY, "No trusted devices"
            notice = ProjectionNotice(
                NoticeKind.EMPTY,
                "No trusted devices are visible",
                "Join a device through verified onboarding before it appears here.",
                RecommendedAction(
                    "Add a trusted device",
                    "Open onboarding to verify and join another device.",
                    "Open onboarding",
                    "/onboarding?step=federation",
                ),
            )
        return self._page(
            FederationPage.DEVICES,
            "Devices",
            "See trusted members without exposing transport addresses.",
            snapshot,
            items=items,
            notice=notice,
            state=state,
            state_label=state_label,
            technical_details=(
                self._technical_details(snapshot, authority_only=True)
                if include_technical
                else ()
            ),
        )

    @staticmethod
    def _federated_capability_state(status: str) -> tuple[str, str]:
        return {
            "ready": ("active", "Active"),
            "registering": ("available", "Pending"),
            "disabled": ("unavailable", "Disabled"),
            "unavailable": ("degraded", "Unavailable"),
            "draining": ("degraded", "Draining"),
            "revoked": ("unavailable", "Revoked"),
        }.get(status, ("information", _title(status)))

    def services(self, *, include_technical: bool = False) -> FederationViewModel:
        snapshot = self._snapshot()
        items: list[ProjectionItem] = []
        for contribution in snapshot.onboarding.contributions:
            state, label = _contribution_state(contribution)
            items.append(
                ProjectionItem(
                    contribution.candidate_id,
                    contribution.label,
                    _title(contribution.capability_type),
                    state,
                    label,
                    (
                        ("Policy", _title(contribution.policy_state)),
                        ("Desired state", _title(contribution.desired_state)),
                    ),
                )
            )
        visible_ids = {item.key for item in items}
        for provider in snapshot.providers.providers:
            if provider.capability_id in visible_ids:
                continue
            state, label = _provider_state(provider)
            items.append(
                ProjectionItem(
                    provider.capability_id,
                    _title(provider.capability_type),
                    f"{provider.protocol} {provider.protocol_version}",
                    state,
                    label,
                    (
                        ("Enrollment", _title(provider.enrollment_state)),
                        ("Health", _title(provider.health_state)),
                    ),
                )
            )
            visible_ids.add(provider.capability_id)

        # Coordinator-announced capabilities are shared metadata only. They let
        # every trusted member see the same service inventory without importing
        # provider authority or exposing transport endpoints. Local contribution
        # cards above remain richer and win on duplicate IDs.
        device_labels = {
            device.node_id: device.label for device in snapshot.federation.devices
        }
        for capability in snapshot.federation.capabilities:
            if capability.capability_id in visible_ids:
                continue
            state, label = self._federated_capability_state(capability.status)
            items.append(
                ProjectionItem(
                    capability.capability_id,
                    _title(capability.capability_type),
                    f"{capability.protocol} {capability.protocol_version}",
                    state,
                    label,
                    (
                        (
                            "Device",
                            device_labels.get(
                                capability.node_id,
                                "Trusted MSH device",
                            ),
                        ),
                        ("Source", "Federation authority"),
                    ),
                )
            )
            visible_ids.add(capability.capability_id)

        notice = self._notice(snapshot)
        state = state_label = None
        if snapshot.federation.available and not items:
            state, state_label = ProjectionState.EMPTY, "No services"
            notice = ProjectionNotice(
                NoticeKind.EMPTY,
                "No services are currently visible",
                "Inspect this device before enabling an independent contribution.",
                RecommendedAction(
                    "Review contribution candidates",
                    "Open the contribution step to choose suitable services.",
                    "Open contributions",
                    "/onboarding?step=contributions",
                ),
            )
        return self._page(
            FederationPage.SERVICES,
            "Services",
            "Review shared service metadata and local provider health without granting authority.",
            snapshot,
            items=tuple(items),
            notice=notice,
            state=state,
            state_label=state_label,
            technical_details=(
                self._technical_details(snapshot, providers_only=True)
                if include_technical
                else ()
            ),
        )

    def benchmarks(self, *, include_technical: bool = False) -> FederationViewModel:
        snapshot = self._snapshot()
        items = tuple(
            ProjectionItem(
                result.run_id,
                _title(result.benchmark_id),
                (
                    "Current suitability evidence"
                    if result.current
                    else f"Rerun required: {_title(result.validity_reason)}"
                ),
                *_benchmark_state(result),
                (
                    ("Target", result.target_service_id),
                    ("Finished", _stamp(result.finished_at)),
                    ("Expires", _stamp(result.expires_at)),
                ),
            )
            for result in snapshot.benchmarks.results
        )
        notice = self._notice(snapshot)
        state = state_label = None
        if snapshot.benchmarks.available and not items:
            state, state_label = ProjectionState.EMPTY, "No results"
            notice = ProjectionNotice(
                NoticeKind.EMPTY,
                "No benchmark results yet",
                "Run recommended bounded checks to create suitability evidence.",
                RecommendedAction(
                    "Run recommended benchmarks",
                    "Open onboarding to run bounded local checks.",
                    "Open benchmarks",
                    "/onboarding?step=benchmarks",
                ),
            )
        elif any(not result.current for result in snapshot.benchmarks.results):
            count = sum(not result.current for result in snapshot.benchmarks.results)
            state, state_label = ProjectionState.DEGRADED, f"{count} need rerun"
        technical = ()
        if include_technical and snapshot.benchmarks.results:
            technical = (
                TechnicalDetail(
                    "Latest benchmark run",
                    snapshot.benchmarks.results[0].run_id,
                ),
            )
        return self._page(
            FederationPage.BENCHMARKS,
            "Benchmarks",
            "Benchmark results are evidence only and never grant authority.",
            snapshot,
            items=items,
            notice=notice,
            state=state,
            state_label=state_label,
            technical_details=technical,
        )

    def storage(self, *, include_technical: bool = False) -> FederationViewModel:
        snapshot = self._snapshot()
        items = [
            ProjectionItem(
                group.group_id,
                f"Storage group {group.group_id}",
                f"Primary: {group.primary_provider_id or 'Not assigned'}",
                "active" if group.leader_active else "degraded",
                "Write authority active" if group.leader_active else "No active authority",
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
                "Assignable" if provider.assignable else "Candidate or unavailable",
                (("Device", provider.node_id),),
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
            and candidate.candidate_id not in authoritative
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

    def jobs(self, *, include_technical: bool = False) -> FederationViewModel:
        del include_technical
        snapshot = self._snapshot()
        items = tuple(
            ProjectionItem(
                job.job_id,
                f"Job {job.job_id}",
                _title(job.capability_type),
                "degraded" if job.status in _FAILED_JOB_STATES else job.status,
                (
                    "Needs attention"
                    if job.status in _FAILED_JOB_STATES
                    else _title(job.status)
                ),
                (
                    ("Provider", job.provider_id or "Not assigned"),
                    ("Attempts", str(job.attempt_count)),
                    ("Updated", _stamp(job.updated_at)),
                ),
            )
            for job in snapshot.jobs.jobs
        )
        notice = self._notice(snapshot)
        state = state_label = None
        if snapshot.jobs.available and not items:
            state, state_label = ProjectionState.EMPTY, "No jobs"
            notice = ProjectionNotice(
                NoticeKind.EMPTY,
                "No federation jobs are active",
                "Jobs appear after an authorized service accepts work.",
            )
        elif any(job.status in _FAILED_JOB_STATES for job in snapshot.jobs.jobs):
            state, state_label = ProjectionState.DEGRADED, "Jobs need attention"
        return self._page(
            FederationPage.JOBS,
            "Jobs",
            "See lifecycle status without leases, artifacts, or worker addresses.",
            snapshot,
            items=items,
            notice=notice,
            state=state,
            state_label=state_label,
        )

    def activity(self, *, include_technical: bool = False) -> FederationViewModel:
        snapshot = self._snapshot()
        items = tuple(
            ProjectionItem(
                f"event-{event.revision if event.revision is not None else index}",
                event.title,
                event.message,
                "information",
                _stamp(event.occurred_at),
                (("Event type", event.event_type),),
            )
            for index, event in enumerate(snapshot.federation.activity[::-1])
        )
        notice = self._notice(snapshot)
        state = state_label = None
        if snapshot.federation.available and not items:
            state, state_label = ProjectionState.EMPTY, "No recent activity"
            notice = ProjectionNotice(
                NoticeKind.EMPTY,
                "No recent federation activity",
                "Authenticated membership and authority changes appear here.",
            )
        technical = ()
        if include_technical and snapshot.federation.revision is not None:
            technical = (
                TechnicalDetail(
                    "Authority revision",
                    str(snapshot.federation.revision),
                ),
            )
        return self._page(
            FederationPage.ACTIVITY,
            "Activity",
            "Review user-language summaries of authenticated changes.",
            snapshot,
            items=items,
            notice=notice,
            state=state,
            state_label=state_label,
            technical_details=technical,
        )

    def settings(self, *, include_technical: bool = False) -> FederationViewModel:
        snapshot = self._snapshot()
        onboarding = snapshot.onboarding
        items = (
            ProjectionItem(
                "connection",
                "Federation connection",
                _title(onboarding.connection_state),
                "connected" if onboarding.connection_state == "connected" else "repair",
                "Trusted" if onboarding.trusted else "Verification needed",
            ),
            ProjectionItem(
                "service-management",
                "Service management",
                "Controls use the existing authenticated operator authority.",
                "available" if snapshot.providers.is_owner else "information",
                "Manage" if snapshot.providers.is_owner else "View only",
            ),
            ProjectionItem(
                "privacy",
                "Projection privacy",
                "Private transport and local implementation details are hidden.",
                "healthy",
                "Protected",
            ),
        )
        notice = self._notice(snapshot)
        return self._page(
            FederationPage.SETTINGS,
            "Settings",
            "Review safe federation identity and management capabilities.",
            snapshot,
            items=items,
            notice=notice,
            technical_details=(
                self._technical_details(snapshot, settings_only=True)
                if include_technical
                else ()
            ),
            content={
                "federation_id": onboarding.federation_id or "Not available",
                "trusted": onboarding.trusted,
                "can_manage_services": snapshot.providers.is_owner,
            },
        )
