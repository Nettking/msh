"""Installed product composition for explicit, run-once capability evidence.

The frozen CF2/CF4 kernels retain temporal expiry semantics for isolated contract
and safety tests. The installed FCP product treats inspection and benchmark work
as explicit operator evidence: age alone never forces a rerun. Structural
invalidation remains fail-closed through benchmark identity, implementation
version, and dependency fingerprints.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import datetime, timezone

from flask import Flask
from flask.signals import appcontext_pushed

from catalog.capabilities.benchmarking.policy import evaluate_run_once_result
from catalog.capabilities.contributions import (
    ContributionCandidateGenerator,
    ContributionService,
)
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.onboarding_models import (
    BenchmarkResult,
    BenchmarkState,
    ContributionCandidate,
    ContributionIntent,
    DeviceInspectionSnapshot,
)

from .capability_benchmark_service import (
    BenchmarkPlanItem,
    CapabilityBenchmarkService,
)
from .capability_config_service import load_capability_config
from .capability_contribution_service import CapabilityContributionService
from .capability_inspection_service import CapabilityInspectionService
from .capability_onboarding_service import get_capability_onboarding_service


class RunOnceCapabilityInspectionService(CapabilityInspectionService):
    """Reuse a valid persisted inspection regardless of legacy expiry metadata."""

    def state(self, snapshot: DeviceInspectionSnapshot | None) -> str:
        if snapshot is None:
            return "empty"
        return "current"


class RunOnceCapabilityBenchmarkService(CapabilityBenchmarkService):
    """Reuse saved benchmark evidence until its capability inputs actually change."""

    def accepted_results(
        self,
        snapshot: DeviceInspectionSnapshot,
    ) -> tuple[BenchmarkResult, ...]:
        plan = self.plan(snapshot)
        latest = self._latest_results(
            self.list_results(),
            device_id=snapshot.device_id,
        )
        accepted: list[BenchmarkResult] = []
        for item in plan:
            if not item.runnable:
                continue
            result = latest.get(item.key)
            if result is None:
                continue
            validity = evaluate_run_once_result(
                result,
                item.definition,
                item.dependency_inputs,
            )
            if validity.current:
                accepted.append(result)
        return tuple(
            sorted(
                accepted,
                key=lambda result: (result.finished_at, result.run_id),
            )
        )

    def _card_model(
        self,
        *,
        item: BenchmarkPlanItem,
        result: BenchmarkResult | None,
        skipped: bool,
        active: bool,
        inspection_current: bool,
    ) -> dict[str, object]:
        state = "pending"
        state_label = "Ready"
        summary = "This bounded local check has not been run."
        diagnostic = item.unavailable_reason
        metrics: list[dict[str, str]] = []
        expires_label = None
        action_label = "Run check"

        if not item.runnable:
            state = "blocked"
            state_label = "Inspect again"
            summary = item.unavailable_reason or "The benchmark target is unavailable."
        elif result is not None:
            validity = evaluate_run_once_result(
                result,
                item.definition,
                item.dependency_inputs,
            )
            if not validity.current:
                state = "stale"
                state_label = "Changed"
                summary = "The benchmark definition or local dependency changed."
                diagnostic = "Rerun this check before using its recommendation."
                action_label = "Rerun check"
            else:
                state = result.state.value
                state_label = self._recommendation_label(result)
                summaries = {
                    BenchmarkState.PASSED: "The bounded local check completed successfully.",
                    BenchmarkState.FAILED: "The bounded local check did not support activation.",
                    BenchmarkState.SKIPPED: "The runner skipped this check safely.",
                    BenchmarkState.CANCELLED: "The benchmark was cancelled safely.",
                    BenchmarkState.EXPIRED: "The saved result requires explicit review.",
                }
                summary = summaries[result.state]
                diagnostic = result.diagnostics[0] if result.diagnostics else None
                metrics = [
                    {
                        "label": self._humanize(str(key)),
                        "value": self._format_value(value),
                    }
                    for key, value in sorted(result.metrics.items())
                ]
                expires_label = (
                    "Saved evidence · collected "
                    + result.finished_at.astimezone(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M UTC"
                    )
                )
                action_label = "Run again"
        elif skipped:
            state = "skipped"
            state_label = "Skipped"
            summary = (
                "This optional benchmark was skipped. No contribution was enabled."
            )
            action_label = "Run check"

        if active:
            state = "running"
            state_label = "Running"
            summary = "The bounded local check is currently running."
            action_label = "Running"

        digest = hashlib.sha256(
            f"{item.benchmark_id}\0{item.target_service_id}".encode()
        ).hexdigest()[:12]
        return {
            "id": f"{item.benchmark_id}-{digest}",
            "benchmark_id": item.benchmark_id,
            "target_service_id": item.target_service_id,
            "capability_label": self._capability_label(item.definition),
            "label": self._benchmark_label(item.definition),
            "target_label": self._humanize(item.target_service_id),
            "state": state,
            "state_label": state_label,
            "summary": summary,
            "metrics": metrics,
            "diagnostic": diagnostic,
            "action_url": "/onboarding/benchmarks/run",
            "cancel_url": "/onboarding/benchmarks/cancel",
            "action_label": action_label,
            "can_run": item.runnable and inspection_current and not active,
            "can_cancel": active,
            "expires_label": expires_label,
        }


class RunOnceCapabilityContributionService(CapabilityContributionService):
    """Keep explicit contribution intent active across time-only evidence expiry."""

    def _build_service(
        self,
        *,
        generator_clock: Callable[[], datetime],
    ) -> ContributionService:
        sources, adapters = self._components()
        registry, _benchmark_adapters = self.benchmark_service._components()
        return ContributionService(
            generator=ContributionCandidateGenerator(
                sources=sources,
                benchmark_definitions=registry.definitions(),
                candidate_ttl_seconds=self._candidate_ttl_seconds,
                now=generator_clock,
                enforce_evidence_expiry=False,
            ),
            store=self._store(),
            policy=self._policy(),
            adapters=adapters,
            now=self._clock,
            enforce_candidate_expiry=False,
        )

    def _accepted_benchmark_results(
        self,
        snapshot: DeviceInspectionSnapshot,
    ) -> tuple[BenchmarkResult, ...]:
        supplier = getattr(self.benchmark_service, "accepted_results", None)
        if callable(supplier):
            return tuple(supplier(snapshot))
        return self.benchmark_service.list_results()

    def recommend(
        self,
        *,
        require_benchmark_review: bool = True,
    ) -> tuple[ContributionCandidate, ...]:
        snapshot, _complete = self._authorized_snapshot(
            require_benchmark_review=require_benchmark_review
        )
        return self._service().recommend(
            snapshot,
            self._accepted_benchmark_results(snapshot),
        )

    def _service_for_reconciliation(self) -> ContributionService:
        context = self.onboarding_service.authorized_context()
        if context is None:
            raise FederationOperationError(
                "contribution-federation-required",
                "a trusted federation connection is required before reconciliation",
                "binding",
            )
        snapshot = self.inspection_service.load()
        if snapshot is None:
            raise FederationOperationError(
                "contribution-inspection-required",
                "saved inspection evidence is required for reconciliation",
                "inspection",
            )
        if snapshot.device_id != context.credentials.identity.node_id:
            raise FederationValidationError(
                "contribution-device-mismatch",
                "device_id",
                "inspection evidence belongs to another device identity",
            )
        service = self._service()
        service.recommend(
            snapshot,
            self._accepted_benchmark_results(snapshot),
        )
        return service

    def reconcile(self) -> tuple[ContributionIntent, ...]:
        if not self.has_persisted_intents():
            return ()
        self._authorized_snapshot(require_benchmark_review=True)
        return self._service_for_reconciliation().reconcile()


def _install_services_for_context(app: Flask) -> None:
    """Resolve configured paths/services only when the first app context exists."""

    onboarding = get_capability_onboarding_service()

    configured_inspection = app.config.get("CAPABILITY_INSPECTION_SERVICE")
    if isinstance(configured_inspection, CapabilityInspectionService):
        inspection = configured_inspection
    else:
        inspection = RunOnceCapabilityInspectionService(
            onboarding_service=onboarding,
            state_database=app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
            adapters=app.config.get("CAPABILITY_ONBOARDING_INSPECTION_ADAPTERS"),
            inspection_ttl_seconds=int(
                app.config.get("CAPABILITY_ONBOARDING_INSPECTION_TTL_SECONDS", 900)
            ),
            clock=onboarding._clock,
            system_observer=app.config.get("CAPABILITY_ONBOARDING_SYSTEM_OBSERVER"),
        )
        app.config["CAPABILITY_INSPECTION_SERVICE"] = inspection

    configured_benchmarks = app.config.get("CAPABILITY_BENCHMARK_SERVICE")
    if isinstance(configured_benchmarks, CapabilityBenchmarkService):
        benchmarks = configured_benchmarks
    else:
        benchmarks = RunOnceCapabilityBenchmarkService(
            onboarding_service=onboarding,
            inspection_service=inspection,
            state_database=app.config.get(
                "CAPABILITY_ONBOARDING_BENCHMARK_DATABASE",
                app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
            ),
            clock=onboarding._clock,
        )
        app.config["CAPABILITY_BENCHMARK_SERVICE"] = benchmarks

    configured_contributions = app.config.get("CAPABILITY_CONTRIBUTION_SERVICE")
    if isinstance(configured_contributions, CapabilityContributionService):
        return
    app.config["CAPABILITY_CONTRIBUTION_SERVICE"] = (
        RunOnceCapabilityContributionService(
            onboarding_service=onboarding,
            inspection_service=inspection,
            benchmark_service=benchmarks,
            state_database=app.config.get(
                "CAPABILITY_ONBOARDING_CONTRIBUTION_DATABASE",
                app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
            ),
            sources=app.config.get("CAPABILITY_ONBOARDING_CONTRIBUTION_SOURCES"),
            adapters=app.config.get("CAPABILITY_ONBOARDING_CONTRIBUTION_ADAPTERS"),
            config_loader=app.config.get(
                "CAPABILITY_CONFIG_LOADER",
                load_capability_config,
            ),
            clock=onboarding._clock,
            candidate_ttl_seconds=int(
                app.config.get("CAPABILITY_ONBOARDING_CONTRIBUTION_TTL_SECONDS", 900)
            ),
        )
    )


def install_run_once_capability_evidence(app: Flask) -> None:
    """Install product-only services lazily without rewriting saved evidence."""

    def install_on_context(sender: Flask, **_extra: object) -> None:
        if sender is not app:
            return
        _install_services_for_context(app)

    appcontext_pushed.connect(install_on_context, sender=app, weak=False)
    app.extensions["run_once_capability_evidence_installer"] = install_on_context


__all__ = [
    "RunOnceCapabilityBenchmarkService",
    "RunOnceCapabilityContributionService",
    "RunOnceCapabilityInspectionService",
    "install_run_once_capability_evidence",
]
