"""Overview page projection builder."""

from __future__ import annotations

from .models import (
    FederationPage,
    FederationViewModel,
    RecommendedAction,
    SummaryCard,
)
from .service_core import (
    _ACTIVE_PROVIDER_STATES,
    _FAILED_JOB_STATES,
    _ONLINE_DEVICE_STATES,
    _RUNNING_JOB_STATES,
    _SECTION_LINKS,
    _benchmark_state,
    _count,
    _stamp,
    _title,
)


class OverviewProjectionMixin:
    @staticmethod
    def _device_connected(snapshot: object, device: object) -> bool:
        del snapshot
        return device.state in _ONLINE_DEVICE_STATES

    def overview(self, *, include_technical: bool = False) -> FederationViewModel:
        snapshot = self._snapshot()
        state, state_label = self._state(snapshot)
        notice = self._notice(snapshot)
        next_action = self._selected_action(snapshot, notice)
        if (
            next_action is None
            and snapshot.onboarding.available
            and snapshot.onboarding.connection_state == "connected"
            and snapshot.onboarding.trusted
            and snapshot.onboarding.inspection_revision is None
        ):
            next_action = RecommendedAction(
                "Inspect this device",
                "Run the safe local inspection before reviewing benchmarks or contributions.",
                "Open inspection",
                "/onboarding?step=inspect",
            )

        devices = snapshot.federation.devices
        connected_devices = sum(
            self._device_connected(snapshot, device) for device in devices
        )
        shared_capabilities = snapshot.federation.capabilities
        if shared_capabilities:
            active_services = sum(
                capability.status == "ready"
                for capability in shared_capabilities
            )
        else:
            # Compatibility fallback for an older authority snapshot that has
            # not published shared capability metadata yet.
            active_services = sum(
                provider.activation_state in _ACTIVE_PROVIDER_STATES
                for provider in snapshot.providers.providers
            )
            if active_services == 0:
                active_services = sum(
                    contribution.activation_state == "active"
                    for contribution in snapshot.onboarding.contributions
                )
        benchmark_count = len(snapshot.benchmarks.results)
        current_benchmarks = sum(
            result.current for result in snapshot.benchmarks.results
        )
        running_jobs = sum(
            job.status in _RUNNING_JOB_STATES for job in snapshot.jobs.jobs
        )
        failed_jobs = sum(
            job.status in _FAILED_JOB_STATES for job in snapshot.jobs.jobs
        )

        if benchmark_count == 0:
            benchmark_card = SummaryCard(
                "Benchmark freshness",
                "0 results",
                "Run inspection and review the recommended checks.",
                "empty",
                "Not run",
            )
        else:
            benchmark_card = SummaryCard(
                "Benchmark freshness",
                f"{current_benchmarks} of {benchmark_count}",
                (
                    "All evidence is current"
                    if current_benchmarks == benchmark_count
                    else (
                        f"{benchmark_count - current_benchmarks} "
                        "result(s) need review"
                    )
                ),
                (
                    "passed"
                    if current_benchmarks == benchmark_count
                    else "expired"
                ),
                (
                    "Current"
                    if current_benchmarks == benchmark_count
                    else "Review"
                ),
            )

        cards = (
            SummaryCard(
                "Trusted devices",
                str(len(devices)),
                f"{connected_devices} connected now",
                (
                    "connected"
                    if devices and connected_devices == len(devices)
                    else "degraded"
                ),
                (
                    "Healthy"
                    if devices and connected_devices == len(devices)
                    else "Review"
                ),
            ),
            SummaryCard(
                "Active services",
                str(active_services),
                "Independent contributions across trusted devices",
                "active" if active_services else "empty",
                "Available" if active_services else "None active",
            ),
            benchmark_card,
            SummaryCard(
                "Jobs",
                f"{running_jobs} running",
                (
                    "No blocked work"
                    if failed_jobs == 0
                    else f"{failed_jobs} need attention"
                ),
                (
                    "running"
                    if running_jobs
                    else ("degraded" if failed_jobs else "empty")
                ),
                (
                    "In progress"
                    if running_jobs
                    else ("Review" if failed_jobs else "Idle")
                ),
            ),
        )

        device_id = snapshot.onboarding.device_id or "This device"
        own_device = next(
            (
                device
                for device in devices
                if device.node_id == snapshot.onboarding.device_id
            ),
            None,
        )
        device_label = own_device.label if own_device is not None else "This device"
        own_contributions = [
            contribution
            for contribution in snapshot.onboarding.contributions
            if contribution.device_id == snapshot.onboarding.device_id
        ]
        active_types = [
            _title(contribution.capability_type)
            for contribution in own_contributions
            if contribution.activation_state == "active"
        ]
        device_summary = "Connected device" + (
            f" with {', '.join(active_types)} contributions."
            if active_types
            else "."
        )
        device_connected = (
            snapshot.onboarding.connection_state == "connected"
            and snapshot.onboarding.trusted
        )
        device = {
            "label": device_label,
            "summary": device_summary,
            "state": "connected" if device_connected else "unavailable",
            "state_label": "Connected" if device_connected else "Reconnect",
            "detail_url": "/federation/device",
            "details": [
                {"label": "Device ID", "value": device_id},
                {
                    "label": "Inspection",
                    "value": (
                        f"Revision {snapshot.onboarding.inspection_revision}"
                        if snapshot.onboarding.inspection_revision is not None
                        else "Not available"
                    ),
                },
                {
                    "label": "Connection",
                    "value": _title(snapshot.onboarding.connection_state),
                },
            ],
        }

        benchmark_items = []
        for result in snapshot.benchmarks.results[:5]:
            item_state, item_label = _benchmark_state(result)
            benchmark_items.append(
                {
                    "label": _title(result.benchmark_id),
                    "detail": (
                        f"Valid until {_stamp(result.expires_at)}"
                        if result.current
                        else f"Rerun required: {_title(result.validity_reason)}"
                    ),
                    "state": item_state,
                    "state_label": item_label,
                }
            )
        expired_count = sum(
            not result.current for result in snapshot.benchmarks.results
        )
        benchmarks = {
            "title": "Suitability evidence",
            "summary": (
                "Current benchmark results support contribution decisions."
                if snapshot.benchmarks.results and expired_count == 0
                else (
                    "One or more results need a bounded rerun."
                    if expired_count
                    else "No benchmark evidence is available yet."
                )
            ),
            "state": (
                "expired" if expired_count else ("passed" if benchmark_items else "empty")
            ),
            "state_label": (
                f"{expired_count} expired"
                if expired_count
                else ("Current" if benchmark_items else "No results")
            ),
            "detail_url": "/federation/benchmarks",
            "items": benchmark_items,
            "empty_action": {
                "label": "Run benchmarks",
                "url": "/onboarding?step=benchmarks",
            },
        }

        contributions = [
            self._contribution_card(contribution)
            for contribution in snapshot.onboarding.contributions
        ]
        device_items = []
        for item in devices[:5]:
            is_connected = self._device_connected(snapshot, item)
            device_items.append(
                {
                    "label": item.label,
                    "detail": _count(item.capability_count, "service"),
                    "state": "connected" if is_connected else "unavailable",
                    "state_label": "Connected" if is_connected else "Offline",
                }
            )
        activity_items = [
            {
                "time_label": _stamp(event.occurred_at),
                "title": event.title,
                "message": event.message,
            }
            for event in snapshot.federation.activity[-5:][::-1]
        ]
        technical = self._technical_details(snapshot) if include_technical else ()
        return FederationViewModel(
            page=FederationPage.OVERVIEW,
            title=snapshot.federation.label,
            intro=(
                "See trusted devices, available services, benchmark freshness, "
                "and the next useful action."
            ),
            state=state,
            state_label=state_label,
            generated_at=self._now(),
            sections=_SECTION_LINKS,
            recommended_action=next_action,
            notice=notice,
            summary_cards=cards,
            technical_details=technical,
            content={
                "device": device,
                "benchmarks": benchmarks,
                "contributions": contributions,
                "services_url": "/federation/services",
                "empty_contributions_action": {
                    "label": "Review contributions",
                    "url": "/onboarding?step=contributions",
                },
                "devices": {
                    "title": _count(len(devices), "trusted device"),
                    "summary": "Trusted devices reconnect with their saved identities.",
                    "detail_url": "/federation/devices",
                    "items": device_items,
                },
                "activity": {
                    "detail_url": "/federation/activity",
                    "items": activity_items,
                },
                "docs_url": "/docs/federation",
            },
        )
